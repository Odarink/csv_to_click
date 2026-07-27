"""Зонд Windows SIO_TCP_INFO: кто на самом деле ограничивает скорость вставки.

Диагностический скрипт, запускается вручную на машине с доступом к кластеру.
В приложение НЕ встраивается и им не импортируется.

Зачем. Наблюдаемые ~380 КБ/с объясняются четырьмя разными механизмами, и
эксперимент «load_workers=1 против 4» их не различает: рост скорости одинаково
совместим с тремя из них. Windows отдаёт нужные счётчики по одному ioctl без
всяких привилегий, и один инсерт (~6 с) закрывает вопрос:

    BytesInFlight упирается в ~262144, Cwnd и SndWnd заметно больше
        -> связывает пин SO_SNDBUF в драйвере (httputil.py:40-41)
    Cwnd мал, BytesRetrans/BytesOut заметно выше нуля
        -> потери на одном потоке, помогут несколько соединений
    BytesInFlight существенно ниже и Cwnd, и 262144
        -> application-limited: продюсер не наполняет сокет
    SndWnd мал или проседает
        -> регулятор потока это mTLS-шлюз, а не наш канал; тогда лимит не в
           байтах, и оценки выигрыша от сжатия неверны

Последнюю гипотезу не видит ни один Python API и ни одна из 118 находок
диагностического прогона: объявленное peer'ом окно приёма нигде не всплывает.

Запуск (из корня репозитория):

    .venv\\Scripts\\python.exe benchmarks\\tcp_info_probe.py

По умолчанию пишет в табличную функцию ``null()``: никаких прав на DDL не нужно
и никакие данные никуда не попадают. Чтобы измерить настоящий путь записи,
укажите реальную таблицу через ``--table sandbox.my_table``.
"""

from __future__ import annotations

import argparse
import ctypes
import os
import socket
import sys
import threading
import time
from ctypes import wintypes
from dataclasses import dataclass
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from csv_click.clickhouse import ClickHouseConfig, get_client  # noqa: E402
from csv_click.load_stats import first_pooled_socket  # noqa: E402
from csv_click.settings import load_app_settings  # noqa: E402


SIO_TCP_INFO = 0xD800_0027
TCP_INFO_VERSION_0 = 0
SO_SNDBUF_PIN_BYTES = 256 * 1024  # httputil.py:40-41


class TCP_INFO_v0(ctypes.Structure):
    """mstcpip.h, TCP_INFO_v0. Ожидаемый размер — 88 байт."""

    _fields_ = [
        ("State", ctypes.c_uint32),
        ("Mss", ctypes.c_uint32),
        ("ConnectionTimeMs", ctypes.c_uint64),
        ("TimestampsEnabled", ctypes.c_uint8),
        ("RttUs", ctypes.c_uint32),
        ("MinRttUs", ctypes.c_uint32),
        ("BytesInFlight", ctypes.c_uint32),
        ("Cwnd", ctypes.c_uint32),
        ("SndWnd", ctypes.c_uint32),
        ("RcvWnd", ctypes.c_uint32),
        ("RcvBuf", ctypes.c_uint32),
        ("BytesOut", ctypes.c_uint64),
        ("BytesIn", ctypes.c_uint64),
        ("BytesReordered", ctypes.c_uint32),
        ("BytesRetrans", ctypes.c_uint32),
        ("FastRetrans", ctypes.c_uint32),
        ("DupAcksIn", ctypes.c_uint32),
        ("TimeoutEpisodes", ctypes.c_uint32),
        ("SynRetrans", ctypes.c_uint8),
    ]


@dataclass(frozen=True)
class Sample:
    at_s: float
    rtt_us: int
    min_rtt_us: int
    bytes_in_flight: int
    cwnd: int
    snd_wnd: int
    mss: int
    bytes_out: int
    bytes_retrans: int
    timeout_episodes: int


def read_tcp_info(sock: socket.socket) -> TCP_INFO_v0:
    ws2 = ctypes.WinDLL("ws2_32", use_last_error=True)
    ws2.WSAIoctl.argtypes = [
        ctypes.c_size_t,  # SOCKET (UINT_PTR)
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    ws2.WSAIoctl.restype = ctypes.c_int

    version = wintypes.DWORD(TCP_INFO_VERSION_0)
    info = TCP_INFO_v0()
    returned = wintypes.DWORD(0)
    result = ws2.WSAIoctl(
        sock.fileno(),
        SIO_TCP_INFO,
        ctypes.byref(version),
        ctypes.sizeof(version),
        ctypes.byref(info),
        ctypes.sizeof(info),
        ctypes.byref(returned),
        None,
        None,
    )
    if result != 0:
        raise OSError(
            f"WSAIoctl(SIO_TCP_INFO) failed, WSAGetLastError={ctypes.get_last_error()}"
        )
    return info


def find_pooled_socket(client) -> socket.socket:
    """Достаёт живой сокет из пула urllib3 внутри клиента clickhouse-connect.

    Соединение доступно в очереди пула только пока оно простаивает: во время
    запроса urllib3 забирает его из очереди. Поэтому сокет надо захватить до
    инсерта — пул LIFO, и следующий запрос из того же потока переиспользует
    именно его (urllib3/connectionpool.py:79, :201).
    """
    sock = first_pooled_socket(client)
    if sock is None:
        raise RuntimeError(
            "В пуле нет ни одного открытого сокета. Прогрев не сработал: "
            "проверьте, что подключение к ClickHouse вообще устанавливается."
        )
    return sock


class Poller:
    """Опрашивает SIO_TCP_INFO в фоновом потоке.

    Намеренно НЕ наследник ``threading.Thread``: у него уже есть приватный метод
    ``_stop``, который вызывают ``join()`` и ``is_alive()``, и любое поле с таким
    же именем в подклассе роняет их с ``TypeError``. Композиция вместо
    наследования убирает весь этот класс коллизий разом.
    """

    def __init__(self, sock: socket.socket, interval_s: float) -> None:
        self._sock = sock
        self._interval_s = interval_s
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self.samples: list[Sample] = []
        self.errors: list[str] = []

    def _poll(self) -> None:
        started = time.perf_counter()
        while not self._stop_event.is_set():
            # Windows переиспользует номера дескрипторов. Если соединение закрыли,
            # опрос по старому номеру начал бы отдавать счётчики чужого сокета
            # как будто это наша вставка — лучше остановиться.
            if self._sock.fileno() == -1:
                self.errors.append("сокет закрыт во время прогона, опрос остановлен")
                return
            try:
                info = read_tcp_info(self._sock)
            except OSError as exc:
                # Зонд не имеет права уронить измерение: фиксируем и продолжаем.
                self.errors.append(str(exc))
            else:
                self.samples.append(
                    Sample(
                        at_s=time.perf_counter() - started,
                        rtt_us=info.RttUs,
                        min_rtt_us=info.MinRttUs,
                        bytes_in_flight=info.BytesInFlight,
                        cwnd=info.Cwnd,
                        snd_wnd=info.SndWnd,
                        mss=info.Mss,
                        bytes_out=info.BytesOut,
                        bytes_retrans=info.BytesRetrans,
                        timeout_episodes=info.TimeoutEpisodes,
                    )
                )
            self._stop_event.wait(self._interval_s)

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout_s: float = 2.0) -> None:
        self._stop_event.set()
        self._thread.join(timeout=timeout_s)


def build_payload(payload_mb: float, column: str) -> bytes:
    target_bytes = int(payload_mb * 1024 * 1024)
    line = b'{"' + column.encode() + b'": "' + b"probe-payload-0123456789" * 4 + b'"}\n'
    repeats = max(1, target_bytes // len(line))
    return line * repeats


def print_report(samples: list[Sample], errors: list[str], wall_s: float, payload_bytes: int) -> None:
    print()
    print(f"Инсерт: {payload_bytes / 1024 / 1024:.2f} MB payload за {wall_s:.2f} s")
    if errors:
        print(f"Ошибок ioctl: {len(errors)}, первая: {errors[0]}")
    if not samples:
        print("Ни одного успешного замера — гипотезы не различить.")
        return

    print()
    header = f"{'t,s':>6} {'RTT,ms':>8} {'minRTT':>8} {'inflight':>10} {'cwnd':>10} {'sndwnd':>10} {'out,MB':>8} {'retrans':>8}"
    print(header)
    print("-" * len(header))
    for sample in samples:
        print(
            f"{sample.at_s:>6.2f} {sample.rtt_us / 1000:>8.1f} {sample.min_rtt_us / 1000:>8.1f} "
            f"{sample.bytes_in_flight:>10} {sample.cwnd:>10} {sample.snd_wnd:>10} "
            f"{sample.bytes_out / 1024 / 1024:>8.2f} {sample.bytes_retrans:>8}"
        )

    # Первая проверка — вёз ли опрошенный сокет вообще эту вставку. Без неё
    # зонд, промахнувшийся мимо соединения, всё равно печатал бы уверенный
    # вердикт по чужим или простаивающим счётчикам.
    carried = samples[-1].bytes_out - samples[0].bytes_out
    # Скорость считаем по тому, что сокет реально пронёс, а не по размеру
    # payload: драйвер молча перезаливает тело целиком на ECONNRESET
    # (httpclient.py:602-617), и тогда payload/wall занижает скорость вдвое.
    observed_bytes_per_s = carried / max(wall_s, 1e-9)
    print()
    print(f"Сокет пронёс за прогон: {carried / 1024 / 1024:.2f} MB из "
          f"{payload_bytes / 1024 / 1024:.2f} MB payload = "
          f"{observed_bytes_per_s / 1024 / 1024:.2f} MB/s по счётчику сокета")
    if carried < payload_bytes * 0.5:
        print("ОТКАЗ ОТ ВЫВОДА: опрошенный сокет не нёс эту вставку (или почти не нёс). "
              "Скорее всего запрос ушёл по другому соединению пула. Повторите прогон; "
              "если повторяется — захват сокета для этой версии urllib3 не работает.")
        return
    if carried > payload_bytes * 1.2:
        print(f"ОТКАЗ ОТ ВЫВОДА: сокет пронёс в {carried / payload_bytes:.2f} раза больше, чем "
              "было в payload. Самое вероятное — драйвер молча перезалил тело целиком "
              "(httpclient.py:602-617). Пока это не исключено, любая гипотеза про транспорт "
              "строится на удвоенных байтах. Включите DEBUG на "
              "clickhouse_connect.driver.httpclient и повторите.")
        return

    # Окно и cwnd считаем только по тем замерам, в которых передача реально шла:
    # в первом и последнем сэмплах окно ещё или уже не раскрыто, и минимум по
    # всем сэмплам систематически занижает SndWnd, выдавая ложный вердикт
    # «регулирует шлюз» почти на любом здоровом соединении.
    active = [
        current
        for previous, current in zip(samples, samples[1:])
        if current.bytes_out > previous.bytes_out
    ]
    if not active:
        print("ОТКАЗ ОТ ВЫВОДА: ни в одном интервале между замерами байты не уходили.")
        return

    peak_inflight = max(sample.bytes_in_flight for sample in active)
    min_snd_wnd = min(sample.snd_wnd for sample in active)
    peak_cwnd = max(sample.cwnd for sample in active)
    retrans = samples[-1].bytes_retrans - samples[0].bytes_retrans
    mss = samples[-1].mss
    rtt_values = [sample.rtt_us for sample in active if sample.rtt_us > 0]
    typical_rtt_s = (sum(rtt_values) / len(rtt_values) / 1_000_000) if rtt_values else 0.0
    # Без RTT размер окна в скорость не превращается, и ни один вердикт про
    # окно не обоснован. None здесь — это «не знаем», а не «бесконечно много».
    window_allows = SO_SNDBUF_PIN_BYTES / typical_rtt_s if typical_rtt_s else None

    print()
    print("Итоги (по интервалам, в которых шла передача):")
    print(f"  MSS: {mss} (проверка истории про MTU 1280)")
    print(f"  RTT: типичный {typical_rtt_s * 1000:.1f} ms, "
          f"min {min(s.min_rtt_us for s in active) / 1000:.1f} ms, "
          f"max {max(s.rtt_us for s in active) / 1000:.1f} ms")
    print(f"  BytesInFlight peak: {peak_inflight} против пина SO_SNDBUF {SO_SNDBUF_PIN_BYTES}")
    print(f"  Cwnd peak: {peak_cwnd}, SndWnd min: {min_snd_wnd}")
    print(f"  BytesRetrans за прогон: {retrans}"
          + (f" = {retrans / carried * 100:.3f}% от пронесённого" if carried else ""))
    if window_allows is None:
        print("  Окно 256 КиБ при этом RTT разрешает: неизвестно, RTT не измерен "
              f"(наблюдаем {observed_bytes_per_s / 1024 / 1024:.2f} MB/s)")
    else:
        print(f"  Окно 256 КиБ при этом RTT разрешает: {window_allows / 1024 / 1024:.2f} MB/s, "
              f"наблюдаем {observed_bytes_per_s / 1024 / 1024:.2f} MB/s")

    loss_ratio = retrans / carried if carried else 0.0

    # Все признаки РЕЛЯЦИОННЫЕ — «что из трёх лимитов реально держит отправку»,
    # а не сравнение с константой. Прежняя версия сравнивала объявленное ПИРОМ
    # окно с нашим локальным пином SO_SNDBUF и на этом основании объявляла
    # виноватым шлюз даже там, где в полёте было в десять раз меньше окна.
    # Потолков на BytesInFlight ТРИ: cwnd, объявленное пиром окно и наш
    # локальный пин SO_SNDBUF. В min() должны входить все три — иначе прогон, где
    # в полёте 200 КБ при пине 256 КиБ и огромных cwnd/окне, объявляется
    # «виноват продюсер», хотя упирается он ровно в пин.
    ceiling = min(peak_cwnd, min_snd_wnd, SO_SNDBUF_PIN_BYTES)
    pin_binds = (
        peak_inflight >= SO_SNDBUF_PIN_BYTES * 0.9
        and min_snd_wnd > peak_inflight * 1.2
        and peak_cwnd > peak_inflight * 1.2
    )
    # Винить пира можно только когда его окно меньше нашего пина: если они в
    # пределах 20% друг от друга, связывают оба, и честный ответ — «нет
    # однозначного признака», а не «регулирует шлюз».
    peer_window_limits = (
        min_snd_wnd < SO_SNDBUF_PIN_BYTES
        and peak_inflight >= min_snd_wnd * 0.8
        and min_snd_wnd < peak_cwnd
    )
    loss_limits = loss_ratio > 0.003
    application_limits = peak_inflight < ceiling * 0.5

    print()
    print(f"Признаки: пин SO_SNDBUF={pin_binds}, окно пира={peer_window_limits}, "
          f"потери={loss_limits}, продюсер={application_limits}")
    print("Чтение:")
    if pin_binds:
        print("  Пин SO_SNDBUF связывает: в полёте держится ровно 256 КиБ, при том что и "
              "cwnd, и объявленное пиром окно заметно больше. Поднимать буфер, а не число "
              "воркеров.")
        if window_allows is not None and observed_bytes_per_s < window_allows * 0.5:
            print("  Осторожно: наблюдаемая скорость всё равно вдвое ниже того, что это окно "
                  "разрешает при измеренном RTT, — одним буфером дело не ограничится.")
    elif peer_window_limits:
        print("  Отправка упирается в объявленное ПИРОМ окно, и оно меньше cwnd: поток "
              "регулирует шлюз, а не наш канал. Лимит тогда не в байтах, и оценки выигрыша "
              "от сжатия под вопросом.")
    elif loss_limits:
        print("  Потери выше 0.3%: одиночный поток loss-limited, помогут несколько "
              "соединений — каждое получает собственный бюджет 1/sqrt(p).")
    elif application_limits:
        print("  Application-limited: в полёте вдвое меньше и cwnd, и окна пира — узкое "
              "место в продюсере, а не в сети.")
    elif window_allows is not None and observed_bytes_per_s < window_allows * 0.2:
        print("  Ни одна из четырёх гипотез не выражена, но скорость сильно ниже того, что "
              "разрешает окно при измеренном RTT: смотрите на серверное время инсерта "
              "(server % из LoadStats), а не на транспорт.")
    else:
        print("  Однозначного признака нет — приложите таблицу выше целиком.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--table",
        default="FUNCTION null('c String')",
        help="Куда вставлять. По умолчанию табличная функция null(): без прав на DDL, данные никуда не попадают.",
    )
    parser.add_argument(
        "--column",
        default="c",
        help="Имя единственной колонки в payload. Для --table на реальную таблицу укажите её колонку типа String.",
    )
    parser.add_argument("--payload-mb", type=float, default=14.0)
    parser.add_argument("--interval-ms", type=int, default=250)
    parser.add_argument("--inserts", type=int, default=1)
    args = parser.parse_args()

    if sys.platform != "win32":
        print("SIO_TCP_INFO есть только на Windows.", file=sys.stderr)
        return 2

    settings = load_app_settings()
    # Те же переменные окружения, что документирует README для loader.bat.
    config = ClickHouseConfig(
        host=settings.host,
        port=settings.port,
        username=os.environ.get("CLICKHOUSE_USER") or settings.username,
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        secure=settings.secure,
        verify=settings.verify,
        client_cert=settings.client_cert,
        client_key=settings.client_key,
        database=settings.database,
        cluster=settings.cluster,
    )
    print(f"Подключение к {config.host}:{config.port}, база {config.database}, "
          f"пользователь {config.username or '(пустой)'}")
    client = get_client(config)

    client.query("SELECT 1")  # прогрев: сокет должен вернуться в пул
    sock = find_pooled_socket(client)
    print(f"Захвачен сокет {sock.getsockname()} -> {sock.getpeername()}")

    payload = build_payload(args.payload_mb, args.column)
    poller = Poller(sock, args.interval_ms / 1000)
    poller.start()
    started = time.perf_counter()
    insert_error: Exception | None = None
    try:
        for _ in range(args.inserts):
            client.raw_insert(
                table=args.table,
                column_names=[args.column],
                insert_block=payload,
                fmt="JSONEachRow",
            )
    except Exception as exc:
        insert_error = exc
    finally:
        wall_s = time.perf_counter() - started
        poller.stop()

    # Отчёт печатается и при сбое вставки. Именно сбой — самый интересный
    # случай для единственного прогона по проду: замеры показывают, на чём
    # именно всё встало, и терять их из-за исключения нельзя.
    if insert_error is not None:
        print()
        print(f"ВСТАВКА УПАЛА: {type(insert_error).__name__}: {insert_error}")
        print("Замеры ниже относятся к тому, что успело уйти до сбоя.")
    print_report(poller.samples, poller.errors, wall_s, len(payload) * args.inserts)
    return 1 if insert_error is not None else 0


if __name__ == "__main__":
    raise SystemExit(main())
