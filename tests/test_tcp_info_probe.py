"""Смоук-тесты диагностического зонда.

Зонд не встроен в приложение и запускается вручную, но запускается он один раз
и по продовому кластеру через VPN. Падение там стоит целого окна для замера,
поэтому минимум — раскладка структуры, жизненный цикл потока опроса и то, что
отчёт не роняется ни на каких данных.

Ловят конкретный случившийся дефект: поле `_stop` в наследнике `threading.Thread`
перекрывало метод `Thread._stop`, и `join()` падал с TypeError уже ПОСЛЕ того,
как замеры были собраны.
"""

from __future__ import annotations

import ctypes
import socket
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))

import tcp_info_probe as probe  # noqa: E402


windows_only = pytest.mark.skipif(sys.platform != "win32", reason="SIO_TCP_INFO есть только на Windows")


def make_sample(**overrides) -> probe.Sample:
    values = {
        "at_s": 0.0,
        "rtt_us": 40_000,
        "min_rtt_us": 40_000,
        "bytes_in_flight": 200_000,
        "cwnd": 1_000_000,
        "snd_wnd": 2_000_000,
        "mss": 1228,
        "bytes_out": 0,
        "bytes_retrans": 0,
        "timeout_episodes": 0,
    }
    values.update(overrides)
    return probe.Sample(**values)


def transfer(total_bytes: int, steps: int = 6, **overrides) -> list[probe.Sample]:
    return [
        make_sample(at_s=index * 0.25, bytes_out=total_bytes * index // (steps - 1), **overrides)
        for index in range(steps)
    ]


EXPECTED_TCP_INFO_OFFSETS = {
    "State": 0,
    "Mss": 4,
    "ConnectionTimeMs": 8,
    "TimestampsEnabled": 16,
    "RttUs": 20,
    "MinRttUs": 24,
    "BytesInFlight": 28,
    "Cwnd": 32,
    "SndWnd": 36,
    "RcvWnd": 40,
    "RcvBuf": 44,
    "BytesOut": 48,
    "BytesIn": 56,
    "BytesReordered": 64,
    "BytesRetrans": 68,
    "FastRetrans": 72,
    "DupAcksIn": 76,
    "TimeoutEpisodes": 80,
    "SynRetrans": 84,
}


def test_tcp_info_struct_matches_the_documented_layout() -> None:
    """Проверяются ВСЕ смещения, а не размер и первые четыре имени: перестановка
    любых двух полей одного типа сохраняет 88 байт и молча подменяет числа, по
    которым выносится вердикт (например Cwnd и SndWnd лежат рядом)."""
    assert ctypes.sizeof(probe.TCP_INFO_v0) == 88

    actual = {
        name: getattr(probe.TCP_INFO_v0, name).offset
        for name, _ in probe.TCP_INFO_v0._fields_
    }
    assert actual == EXPECTED_TCP_INFO_OFFSETS


def test_poller_is_not_a_thread_subclass() -> None:
    """Структурный пин на исправление, а не на симптом.

    `threading.Thread` владеет именами `_stop`, `_target`, `_args`, `_started`,
    `_tstate_lock`. Поле `_stop` в наследнике уже роняло `join()` с TypeError
    после того, как все замеры были собраны. Композиция убирает весь класс
    коллизий, и вернуть наследование молча не должно получиться.
    """
    assert not issubclass(probe.Poller, threading.Thread)


@windows_only
def test_poller_collects_samples_and_stops_without_raising() -> None:
    """`Poller` не наследует `threading.Thread` намеренно: у того есть приватный
    метод `_stop`, и одноимённое поле роняло `join()` уже после сбора замеров."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    threading.Thread(target=lambda: server.accept(), daemon=True).start()
    client = socket.create_connection(server.getsockname())

    poller = probe.Poller(client, 0.02)
    try:
        poller.start()
        client.sendall(b"x" * 100_000)
        time.sleep(0.12)
        poller.stop()
    finally:
        client.close()
        server.close()

    assert poller.errors == []
    assert len(poller.samples) >= 2
    assert poller.samples[-1].bytes_out >= 100_000


@windows_only
def test_poller_actually_stops_sampling() -> None:
    """`stop()` без выставленного события отрабатывал бы «успешно», оставив
    поток опроса жить до конца процесса."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    threading.Thread(target=lambda: server.accept(), daemon=True).start()
    client = socket.create_connection(server.getsockname())

    poller = probe.Poller(client, 0.01)
    try:
        poller.start()
        time.sleep(0.08)
        poller.stop()
        after_stop = len(poller.samples)
        time.sleep(0.1)
    finally:
        client.close()
        server.close()

    assert after_stop >= 2
    assert len(poller.samples) == after_stop, "поток опроса продолжил работу после stop()"


def test_print_report_survives_degenerate_input(capsys: pytest.CaptureFixture[str]) -> None:
    for samples in ([], [make_sample()], transfer(0)):
        probe.print_report(samples, [], wall_s=1.0, payload_bytes=14 * 1024 * 1024)

    output = capsys.readouterr().out
    assert "Ни одного успешного замера" in output
    assert output.count("ОТКАЗ ОТ ВЫВОДА") == 2


def test_print_report_always_prints_the_sample_table(capsys: pytest.CaptureFixture[str]) -> None:
    """Когда ни один признак не выражен, оператору сказано приложить таблицу
    целиком — значит таблица обязана быть напечатана."""
    payload = 14 * 1024 * 1024
    probe.print_report(transfer(payload), [], wall_s=6.0, payload_bytes=payload)

    output = capsys.readouterr().out
    assert "inflight" in output and "sndwnd" in output and "retrans" in output
    assert output.count("\n") > len(transfer(payload))


def test_print_report_measures_throughput_from_the_socket_not_the_payload(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Драйвер может молча перезалить тело, и тогда payload/wall занижает
    скорость вдвое. Считать надо по счётчику BytesOut самого сокета."""
    payload = 10 * 1024 * 1024
    carried = 9 * 1024 * 1024
    probe.print_report(transfer(carried), [], wall_s=9.0, payload_bytes=payload)

    output = capsys.readouterr().out
    assert "1.00 MB/s по счётчику сокета" in output
    assert "1.11 MB/s" not in output  # это payload/wall, то есть неверное число


def test_print_report_names_the_so_sndbuf_pin_when_it_is_the_binding_ceiling(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Главная гипотеза, ради которой зонд и написан."""
    payload = 14 * 1024 * 1024
    probe.print_report(
        transfer(
            payload,
            bytes_in_flight=probe.SO_SNDBUF_PIN_BYTES,
            cwnd=4 * probe.SO_SNDBUF_PIN_BYTES,
            snd_wnd=8 * probe.SO_SNDBUF_PIN_BYTES,
            rtt_us=40_000,
        ),
        [],
        wall_s=6.0,
        payload_bytes=payload,
    )

    output = capsys.readouterr().out
    assert "Пин SO_SNDBUF связывает" in output
    assert "Application-limited" not in output


def test_a_run_pinned_just_under_the_buffer_is_not_blamed_on_the_producer(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Потолков на BytesInFlight три, и локальный пин — один из них. Без него в
    min() прогон с 200 КБ в полёте при пине 256 КиБ объявлялся «виноват
    продюсер», хотя упирается он ровно в пин."""
    payload = 14 * 1024 * 1024
    probe.print_report(
        transfer(payload, bytes_in_flight=200_000, cwnd=1_000_000, snd_wnd=2_000_000),
        [],
        wall_s=6.0,
        payload_bytes=payload,
    )

    output = capsys.readouterr().out
    assert "Application-limited" not in output


def test_a_peer_window_just_above_the_pin_is_not_blamed_on_the_gateway(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Когда окно пира и наш пин в пределах 20% друг от друга, связывают оба, и
    честный ответ — «нет однозначного признака». Обвинение шлюза здесь дороже
    остальных: именно оно ставит под сомнение всю фазу сжатия."""
    payload = 14 * 1024 * 1024
    probe.print_report(
        transfer(
            payload,
            bytes_in_flight=probe.SO_SNDBUF_PIN_BYTES,
            cwnd=1_000_000,
            snd_wnd=280_000,
        ),
        [],
        wall_s=6.0,
        payload_bytes=payload,
    )

    output = capsys.readouterr().out
    assert "регулирует шлюз" not in output


def test_print_report_refuses_to_conclude_when_the_socket_carried_nothing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    probe.print_report(transfer(1024), [], wall_s=6.0, payload_bytes=14 * 1024 * 1024)

    output = capsys.readouterr().out
    assert "ОТКАЗ ОТ ВЫВОДА" in output
    assert "Признаки:" not in output


def test_print_report_refuses_to_conclude_when_the_body_went_out_twice(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Драйвер молча перезаливает тело на ECONNRESET. Пока это не исключено,
    любая гипотеза про транспорт строилась бы на удвоенных байтах."""
    payload = 7 * 1024 * 1024
    probe.print_report(transfer(2 * payload), [], wall_s=6.0, payload_bytes=payload)

    output = capsys.readouterr().out
    assert "ОТКАЗ ОТ ВЫВОДА" in output
    assert "перезалил тело" in output
    assert "Признаки:" not in output


def test_print_report_does_not_blame_the_gateway_when_the_producer_is_the_limit(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Окно пира в 200 КБ меньше локального пина в 256 КиБ, но в полёте всего
    20 КБ — то есть окно ни при чём. Сравнение объявленного пиром окна с нашим
    SO_SNDBUF объявляло виноватым шлюз именно на таких данных."""
    payload = 14 * 1024 * 1024
    probe.print_report(
        transfer(payload, bytes_in_flight=20_000, cwnd=2_000_000, snd_wnd=200_000),
        [],
        wall_s=6.0,
        payload_bytes=payload,
    )

    output = capsys.readouterr().out
    assert "Application-limited" in output
    assert "регулирует шлюз" not in output


def test_print_report_names_the_peer_window_only_when_the_sender_stalls_against_it(
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = 14 * 1024 * 1024
    probe.print_report(
        transfer(payload, bytes_in_flight=118_000, cwnd=2_000_000, snd_wnd=120_000),
        [],
        wall_s=6.0,
        payload_bytes=payload,
    )

    output = capsys.readouterr().out
    assert "регулирует шлюз" in output


def run_probe_main(monkeypatch: pytest.MonkeyPatch, *, fail: bool, argv: list[str]) -> int:
    """Гоняет probe.main() целиком поверх настоящего loopback-сокета."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    drained: list[socket.socket] = []

    def drain() -> None:
        connection, _ = server.accept()
        drained.append(connection)
        while True:
            try:
                if not connection.recv(1 << 20):
                    return
            except OSError:
                return

    threading.Thread(target=drain, daemon=True).start()
    client_socket = socket.create_connection(server.getsockname())

    class FakeClickHouse:
        def __init__(self) -> None:
            pool = SimpleNamespace(
                pool=SimpleNamespace(queue=[SimpleNamespace(sock=client_socket)])
            )
            self.http = SimpleNamespace(pools={"key": pool})

        def query(self, sql):
            return SimpleNamespace(result_rows=[[1]])

        def raw_insert(self, **kwargs):
            if fail:
                raise RuntimeError("HTTP status 500: the read limit is reached")
            client_socket.sendall(kwargs["insert_block"])
            time.sleep(0.2)
            return SimpleNamespace(summary={"elapsed_ns": "1000000"})

    monkeypatch.setattr(probe, "get_client", lambda config: FakeClickHouse())
    monkeypatch.setattr(sys, "argv", ["tcp_info_probe.py", *argv])
    try:
        return probe.main()
    finally:
        client_socket.close()
        server.close()


@windows_only
def test_probe_main_runs_end_to_end_and_prints_a_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Сквозной прогон: settings -> клиент -> прогрев -> захват сокета -> поток
    опроса -> настоящий WSAIoctl -> отчёт. Именно этот путь пропустил падение
    `Poller.join()`, которое прогон одних функций форматирования не показывал."""
    code = run_probe_main(monkeypatch, fail=False, argv=["--payload-mb", "1", "--interval-ms", "50"])

    output = capsys.readouterr().out
    assert code == 0
    assert "Захвачен сокет" in output
    assert "Traceback" not in output
    assert "Сокет пронёс за прогон" in output


@windows_only
def test_probe_main_keeps_the_report_when_the_insert_fails(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Сбой вставки — самый интересный случай для единственного прогона по
    проду: замеры показывают, на чём всё встало, и терять их нельзя."""
    code = run_probe_main(monkeypatch, fail=True, argv=["--payload-mb", "1", "--interval-ms", "50"])

    output = capsys.readouterr().out
    assert code == 1
    assert "ВСТАВКА УПАЛА" in output
    assert "read limit is reached" in output
    assert "Инсерт:" in output, "отчёт потерян при сбое вставки"


def test_build_payload_uses_the_requested_column() -> None:
    payload = probe.build_payload(0.01, "acct")

    assert payload.startswith(b'{"acct": "')
    assert payload.endswith(b"\n")
    assert len(payload) > 0
