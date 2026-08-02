"""Поведенческие тесты на _create_and_load.

Остальные проверки этой функции в `test_app_state.py` — сопоставления исходника
с литералами: они ловят переименования, но не ловят ни одной ошибки в потоке
данных. Фаза 0 перевела функцию на LoadStats и запись о прогоне, поэтому здесь
проверяется собственно поведение: часы, счётчики и файл прогона.
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import csv_click.app as app
from csv_click.load_stats import DRIVER_LOGGER_NAME, DRIVER_RETRY_MESSAGE
from csv_click.clickhouse import ClickHouseConfig
from csv_click.errors import CsvSchemaError
from csv_click.pandas_loader import (
    ReadOptions,
    SchemaMapping,
    mappings_to_editor_rows,
    mappings_to_schema,
)


BLOCK_SERVER_NS = 3_000_000


class FakeRawClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def raw_insert(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(summary={"elapsed_ns": str(BLOCK_SERVER_NS)})


class FakeSlot:
    """Заменяет st.progress()/st.empty().

    Методы перечислены явно, а не через __getattr__: опечатка в имени вызова
    должна падать AttributeError, а не молча проглатываться фейком.
    """

    def __init__(self) -> None:
        self.messages: list[tuple[str, str]] = []
        self.progress_values: list[float] = []
        self.metrics: list[tuple[str, object]] = []

    def progress(self, value: float) -> None:
        self.progress_values.append(value)

    def info(self, text: str) -> None:
        self.messages.append(("info", text))

    def success(self, text: str) -> None:
        self.messages.append(("success", text))

    def error(self, text: str) -> None:
        self.messages.append(("error", text))

    def code(self, text: str, language: str | None = None) -> None:
        self.messages.append(("code", text))

    def metric(self, label: str, value: object) -> None:
        self.metrics.append((label, value))


class FakeStreamlit:
    def __init__(self, session_state: dict[str, object]) -> None:
        self.session_state = session_state
        self.slots: list[FakeSlot] = []
        self.warnings: list[str] = []

    def progress(self, value: float) -> FakeSlot:
        return self._new_slot()

    def empty(self) -> FakeSlot:
        return self._new_slot()

    def warning(self, text: str) -> None:
        self.warnings.append(text)

    def _new_slot(self) -> FakeSlot:
        slot = FakeSlot()
        self.slots.append(slot)
        return slot

    @property
    def rendered_log(self) -> str:
        return "\n".join(
            text
            for slot in self.slots
            for kind, text in slot.messages
            if kind == "code"
        )

    def statuses(self, kind: str) -> list[str]:
        return [text for slot in self.slots for message_kind, text in slot.messages if message_kind == kind]


@pytest.fixture
def load_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    csv_path = tmp_path / "load.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]
    fake_st = FakeStreamlit({"type_rows": mappings_to_editor_rows(mappings)})
    monkeypatch.setattr(app, "st", fake_st)
    # Заметная пауза, чтобы connect_s был отличим от нуля: иначе тест прошёл бы
    # и на захардкоженном нуле.
    monkeypatch.setattr(app, "test_connection", lambda client: time.sleep(0.05))
    monkeypatch.setattr(app, "create_tables", lambda **kwargs: None)

    records: list[Path] = []
    real_write_run_record = app.write_run_record

    def write_run_record_to_tmp(**kwargs):
        record_path = real_write_run_record(**{**kwargs, "directory": tmp_path / "runs"})
        records.append(record_path)
        return record_path

    monkeypatch.setattr(app, "write_run_record", write_run_record_to_tmp)

    def run(client, insert_compression: str = "off") -> None:
        monkeypatch.setattr(app, "get_client", lambda config: client)
        app._create_and_load(
            insert_compression=insert_compression,
            config=ClickHouseConfig(database="sandbox", cluster="clickhouse"),
            csv_path=str(csv_path),
            read_options=ReadOptions(batch_size=2),
            schema=mappings_to_schema(mappings),
            distributed_table="orders",
            order_by="ID",
            partition_by=None,
            batch_size=2,
            max_insert_payload_mb=16,
            load_workers=1,
            strict_preflight=True,
            sharding_key="ID",
        )

    return SimpleNamespace(run=run, streamlit=fake_st, records=records, csv_path=csv_path)


def read_single_record(records: list[Path]) -> dict:
    assert len(records) == 1
    return json.loads(records[0].read_text(encoding="utf-8"))


def test_successful_load_reports_every_clock_and_persists_the_run_record(load_environment) -> None:
    client = FakeRawClient()

    load_environment.run(client)

    log = load_environment.streamlit.rendered_log
    assert "Load finished: 3 rows" in log
    assert "preflight" in log and "insert wall" in log
    assert "Server reported" in log
    assert len(client.calls) == 2

    record = read_single_record(load_environment.records)
    assert record["outcome"] == "ok"
    assert record["error"] is None
    assert record["stats"]["rows"] == 3
    assert record["stats"]["blocks"] == 2
    assert record["stats"]["blocks_without_server_time"] == 0
    assert record["stats"]["server_ns"] == 2 * BLOCK_SERVER_NS
    assert record["stats"]["insert_wall_s"] > 0
    assert record["stats"]["preflight_s"] > 0
    assert record["stats"]["source_fully_read"] is True
    assert record["stats"]["connect_s"] >= 0.05
    assert record["stats"]["total_s"] > 0
    assert record["stats"]["src_bytes"] == load_environment.csv_path.stat().st_size
    assert record["stats"]["server_share"] is not None
    assert record["stats"]["worker_count"] == 1
    assert record["config"]["batch_size"] == 2
    assert record["config"]["table"] == "orders"
    assert record["config"]["load_workers"] == 1
    # Обе отметки пула PyArrow сняты — иначе «прирост за загрузку» не посчитать.
    assert isinstance(record["stats"]["arrow_bytes_at_start"], int)
    assert isinstance(record["stats"]["arrow_bytes"], int)
    assert record["stats"]["arrow_bytes"] >= record["stats"]["arrow_bytes_at_start"]


def test_the_chosen_codec_reaches_the_wire_and_the_record(load_environment, monkeypatch) -> None:
    """Настройка, которая не доезжает до провода, — это ложное ускорение.

    Оператор увидит `zstd` в записи, прежнюю скорость и не поймёт, почему.
    Поэтому проверяется и то, что уехало в клиент, и то, что записано.
    """
    sent: list[str | None] = []
    original = app.load_csv_via_raw_insert

    def spy(**kwargs):
        sent.append(kwargs.get("compression"))
        return original(**kwargs)

    monkeypatch.setattr(app, "load_csv_via_raw_insert", spy)
    load_environment.run(FakeRawClient(), insert_compression="zstd")

    record = read_single_record(load_environment.records)
    assert sent == ["zstd"], "кодек не доехал до загрузчика"
    assert record["config"]["insert_compression"] == "zstd"
    # Тело обязано отличаться от исходного. МЕНЬШЕ оно тут не будет: три строки
    # в 61 байт дают 71 из-за заголовка кадра zstd. Выигрыш появляется на
    # мегабайтных блоках (замерено 3,76x на 9,5 МБ), а порога по размеру нет
    # намеренно: десяток лишних байт на блок против 3,76x — не та цена, за
    # которую стоит заводить ещё одну ручку.
    assert record["stats"]["wire_bytes"] != record["stats"]["raw_bytes"]
    assert record["stats"]["compress_s"] > 0
    assert "compressed" in load_environment.streamlit.rendered_log


def test_the_default_setting_does_not_put_off_into_the_header(load_environment) -> None:
    """Путь ПО УМОЛЧАНИЮ обязан работать.

    Реальный прогон 2026-07-27 23:54 упал на первом блоке: приложение отдало
    кодек строкой `off`, драйвер сделал из неё `Content-Encoding`, и прокси
    ответил `unsupported compression method off`. Фейк здесь ведёт себя как тот
    прокси — принимает только настоящие кодеки.
    """

    class ProxyLikeClient(FakeRawClient):
        def raw_insert(self, **kwargs):
            codec = kwargs.get("compression")
            if codec is not None and codec not in {"zstd", "lz4", "gzip"}:
                raise RuntimeError(
                    "HTTP driver received HTTP status 500, server response: "
                    f"clickHouse engine unsupported compression method {codec}"
                )
            return super().raw_insert(**kwargs)

    load_environment.run(ProxyLikeClient())

    record = read_single_record(load_environment.records)
    assert record["outcome"] == "ok", record["error"]
    assert record["stats"]["rows"] == 3


def test_without_compression_the_wire_carries_the_raw_payload(load_environment) -> None:
    load_environment.run(FakeRawClient())

    record = read_single_record(load_environment.records)
    assert record["config"]["insert_compression"] == "off"
    assert record["stats"]["wire_bytes"] == record["stats"]["raw_bytes"]
    assert record["stats"]["compress_s"] == 0.0


def test_the_record_answers_who_was_the_bottleneck(load_environment) -> None:
    """Прогон на 500 млн строк оставил 85% времени необъяснёнными.

    Теперь запись отвечает на это сама: сколько продюсер стоял, сколько длилась
    вставка и какую долю в ней занял сервер. Занятость считается только здесь —
    `insert_wall_s` ставит приложение, загрузчик его не знает.
    """
    load_environment.run(FakeRawClient())

    record = read_single_record(load_environment.records)
    stats = record["stats"]

    assert stats["insert_busy_s"] > 0, "время самой вставки не замерено"
    assert stats["producer_stall_s"] >= 0
    assert stats["insert_queue_s"] >= 0

    log = load_environment.streamlit.rendered_log
    assert "Who waited for whom" in log
    assert "workers were busy" in log
    assert "Unattributed producer time" in log


def test_the_app_arms_the_driver_retry_counter_around_the_load(load_environment) -> None:
    """Ничто иначе не связывает счётчик с настоящей загрузкой: без этого теста
    его можно вообще не вызывать из app.py, и все тесты останутся зелёными."""
    driver_logger = logging.getLogger(DRIVER_LOGGER_NAME)

    class ReconnectingClient(FakeRawClient):
        def raw_insert(self, **kwargs):
            driver_logger.debug("%s (attempt %s/%s)", DRIVER_RETRY_MESSAGE, 1, 2)
            return super().raw_insert(**kwargs)

    load_environment.run(ReconnectingClient())

    record = read_single_record(load_environment.records)
    assert record["stats"]["driver_retries"] == 2
    log = load_environment.streamlit.rendered_log
    assert "re-uploaded a request body 2 time(s)" in log


def test_insert_wall_time_excludes_preflight_connect_and_ddl(load_environment, monkeypatch) -> None:
    """server % считается от insert_wall_s именно потому, что раньше в тот же
    счётчик попадали preflight, connect и DDL — до 2.5% времени, списанного на провод."""
    import time as time_module

    monkeypatch.setattr(
        app,
        "create_tables",
        lambda **kwargs: time_module.sleep(0.5),
    )

    load_environment.run(FakeRawClient())

    record = read_single_record(load_environment.records)
    assert record["stats"]["ddl_s"] >= 0.5
    # Строго больше нуля: односторонняя проверка прошла бы и если бы
    # insert_wall_s вообще не присвоили.
    assert record["stats"]["insert_wall_s"] > 0
    assert record["stats"]["insert_wall_s"] < 0.5
    assert record["stats"]["total_s"] > record["stats"]["ddl_s"]


def test_failed_load_still_persists_the_run_record_with_partial_counters(
    load_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cleanups: list[str] = []
    monkeypatch.setattr(
        app,
        "_cleanup_after_failed_load",
        lambda *args, **kwargs: cleanups.append(kwargs.get("distributed_table", args[2])),
    )

    class FailsOnSecondBlock(FakeRawClient):
        def raw_insert(self, **kwargs):
            if b'"ID":3' in kwargs["insert_block"]:
                raise RuntimeError("HTTP status 500: the read limit is reached")
            return super().raw_insert(**kwargs)

    load_environment.run(FailsOnSecondBlock())

    # Первый блок уже в таблице: удалять её из-за сбоя на втором - значит
    # уничтожить залитое. Один транзиентный 5xx на 900-м блоке из 1000 стоил бы
    # всей работы.
    assert cleanups == [], "таблицы с залитыми строками удалены из-за сбоя загрузки"
    assert "kept" in load_environment.streamlit.rendered_log.lower()
    record = read_single_record(load_environment.records)
    assert record["outcome"] == "failed"
    assert "HTTP/proxy read limit" in record["error"]
    assert record["stats"]["rows"] == 2
    assert record["stats"]["blocks"] == 1
    assert record["stats"]["insert_wall_s"] > 0
    assert record["config"]["batch_size"] == 2


def test_failure_before_the_first_block_still_drops_the_empty_tables(
    load_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Пока в таблицах ничего нет, чистка - это откат создания, и она нужна.

    Иначе после неудачной попытки остаётся пустая пара таблиц, и следующая
    загрузка того же имени упирается в `ExistingTableError`.
    """
    cleanups: list[str] = []
    monkeypatch.setattr(
        app,
        "_cleanup_after_failed_load",
        lambda *args, **kwargs: cleanups.append(kwargs.get("distributed_table", args[2])),
    )

    def fails_before_sending_anything(**kwargs):
        raise CsvSchemaError("Cannot convert chunk 1, column 'ID', value 'x' to UInt64")

    monkeypatch.setattr(app, "load_csv_via_raw_insert", fails_before_sending_anything)

    load_environment.run(FakeRawClient())

    assert cleanups == ["orders"]
    record = read_single_record(load_environment.records)
    assert record["stats"]["blocks"] == 0
    assert record["stats"]["blocks_unconfirmed"] == 0


def test_an_unconfirmed_block_without_a_confirmed_one_still_drops(
    load_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ни один блок не подтверждён - таблицы считаются пустыми и удаляются.

    `blocks_unconfirmed` не отвечает на вопрос «есть ли данные»: туда попадают
    и блоки, которые не отправлялись вовсе. Держаться за него значило бы
    оставлять пустую пару после каждого отказа кодека.
    """
    cleanups: list[str] = []
    monkeypatch.setattr(
        app,
        "_cleanup_after_failed_load",
        lambda *args, **kwargs: cleanups.append(kwargs.get("distributed_table", args[2])),
    )

    class LosesTheAnswer(FakeRawClient):
        def raw_insert(self, **kwargs):
            super().raw_insert(**kwargs)
            raise RuntimeError("Connection aborted before the answer arrived")

    load_environment.run(LosesTheAnswer())

    record = read_single_record(load_environment.records)
    assert record["stats"]["blocks"] == 0
    assert record["stats"]["blocks_unconfirmed"] >= 1
    assert cleanups == ["orders"]
    # Оператор обязан узнать про размен: долетевший блок ушёл вместе с таблицей.
    assert "never came back confirmed" in load_environment.streamlit.rendered_log


class _RecordingClient:
    """Считает DROP-запросы. `drop_target_tables` ходит в клиент только через `query`."""

    def __init__(self) -> None:
        self.queries: list[str] = []

    def query(self, sql: str):  # noqa: ANN201
        self.queries.append(sql)
        return None


@pytest.mark.parametrize(
    ("stats_kwargs", "want_drops"),
    [
        # Ничего не ушло: чистка - это откат создания, и она нужна.
        ({}, 2),
        # Подтверждённые блоки: данные в таблице, и терять их нельзя.
        ({"rows": 449_000_000, "blocks": 900}, 0),
        ({"rows": 100, "blocks": 2, "blocks_unconfirmed": 3}, 0),
        # Ни одного подтверждённого: в этот счётчик попадают и блоки, которые
        # не отправлялись, поэтому таблицы считаются пустыми.
        ({"blocks_unconfirmed": 1}, 2),
        ({"blocks_unconfirmed": 7}, 2),
    ],
)
def test_tables_are_dropped_until_a_block_is_confirmed(
    stats_kwargs: dict[str, int], want_drops: int
) -> None:
    from csv_click.load_stats import LoadStats

    client = _RecordingClient()

    app._handle_tables_after_failed_load(
        client,
        ClickHouseConfig(database="sandbox", cluster="clickhouse"),
        "orders",
        lambda _line: None,
        LoadStats(**stats_kwargs),
    )

    assert len(client.queries) == want_drops


def test_kept_tables_message_names_both_tables_and_what_is_inside() -> None:
    from csv_click.load_stats import LoadStats

    lines: list[str] = []

    app._handle_tables_after_failed_load(
        _RecordingClient(),
        ClickHouseConfig(database="sandbox", cluster="clickhouse"),
        "orders",
        lines.append,
        LoadStats(rows=4_000, blocks=2),
    )

    message = "\n".join(lines)
    assert "sandbox.orders" in message
    assert "sandbox.orders_local" in message
    assert "4000 rows" in message
    assert "2 confirmed" in message


def test_dropping_after_unconfirmed_blocks_names_the_tradeoff() -> None:
    """Удаляя пару, надо сказать: долетевший блок ушёл вместе с ней.

    Сообщение не должно утверждать, что сервер не ответил: драйвер бросает один
    и тот же `OperationalError` и на отказ, и на обрыв, а `blocks_unconfirmed`
    считает ещё и блоки, которые не отправлялись.
    """
    from csv_click.load_stats import LoadStats

    lines: list[str] = []

    app._handle_tables_after_failed_load(
        _RecordingClient(),
        ClickHouseConfig(database="sandbox", cluster="clickhouse"),
        "orders",
        lines.append,
        LoadStats(blocks_unconfirmed=1),
    )

    message = "\n".join(lines)
    assert "0 rows" not in message
    assert "got no answer from the server" not in message
    assert "reload from the start" in message


def test_a_stripped_summary_header_is_reported_instead_of_a_zero_server_share(
    load_environment,
) -> None:
    """Фейк повторяет то, что реально отдаёт драйвер при срезанном заголовке:
    сводку с одним query_id, а не None и не пустой словарь (httpclient.py:444)."""

    class StrippedSummaryClient(FakeRawClient):
        def raw_insert(self, **kwargs):
            super().raw_insert(**kwargs)
            return SimpleNamespace(summary={"query_id": "01234567-89ab-cdef"})

    load_environment.run(StrippedSummaryClient())

    log = load_environment.streamlit.rendered_log
    assert "Server time is not computable" in log
    assert "2 of 2 blocks returned no elapsed_ns" in log
    assert "% of insert wall time" not in log

    record = read_single_record(load_environment.records)
    assert record["outcome"] == "ok"
    assert record["stats"]["blocks_without_server_time"] == 2
    assert record["stats"]["server_share"] is None


def test_a_streamlit_rerun_is_recorded_as_interrupted_and_not_as_a_silent_failure(
    load_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RerunException наследуется от BaseException и проходит мимо
    `except Exception`. Без отдельной ветки запись утверждала бы «failed» без
    причины, то есть выглядела бы как настоящий сбой загрузки."""

    class RerunException(BaseException):
        pass

    monkeypatch.setattr(
        app,
        "create_tables",
        lambda **kwargs: (_ for _ in ()).throw(RerunException("rerun")),
    )

    with pytest.raises(RerunException):
        load_environment.run(FakeRawClient())

    record = read_single_record(load_environment.records)
    assert record["outcome"] == "interrupted"
    assert "interrupted" in record["error"]
    assert record["stats"]["rows"] == 0


class RerunException(BaseException):
    """Форма исключения Streamlit: наследник BaseException, мимо `except Exception`."""


class InterruptingSlot(FakeSlot):
    """Слот, который на N-м вызове названного метода бросает RerunException.

    Бросает ДО записи вызова: настоящий `st.*` тоже не успевает ничего показать.
    """

    def __init__(self, method: str, call_number: int) -> None:
        super().__init__()
        self._method = method
        self._call_number = call_number
        self._calls = 0

    def _tick(self, method: str) -> None:
        if method != self._method:
            return
        self._calls += 1
        if self._calls == self._call_number:
            raise RerunException("rerun")

    def progress(self, value: float) -> None:
        self._tick("progress")
        super().progress(value)

    def success(self, text: str) -> None:
        self._tick("success")
        super().success(text)

    def metric(self, label: str, value: object) -> None:
        self._tick("metric")
        super().metric(label, value)


class StreamlitWithInterruptingSlot(FakeStreamlit):
    """Отдаёт подготовленный слот на нужной позиции.

    Порядок создания в `_create_and_load`: progress, status, metrics, log.
    """

    def __init__(self, session_state: dict[str, object], slot_index: int, slot: FakeSlot) -> None:
        super().__init__(session_state)
        self._slot_index = slot_index
        self._interrupting = slot

    def _new_slot(self) -> FakeSlot:
        if len(self.slots) == self._slot_index:
            self.slots.append(self._interrupting)
            return self._interrupting
        return super()._new_slot()


def test_a_rerun_while_reporting_a_finished_load_is_recorded_as_ok(
    load_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Загрузка прошла, строки в ClickHouse — запись обязана это сказать.

    Прерывание ловим на ПЕРВОМ `st.*`-вызове после загрузки: это
    `progress.progress(1.0)`, третий вызов слота (два блока до него). Отметка
    успеха должна стоять раньше него, иначе запись объявит провал загрузки,
    которая прошла.
    """
    cleanups: list[str] = []
    monkeypatch.setattr(
        app,
        "_cleanup_after_failed_load",
        lambda client, config, distributed_table, log: cleanups.append(distributed_table),
    )
    # Нулевой интервал, чтобы троттлинг не менял нумерацию вызовов: два блока
    # дают два вызова progress, и третий — уже тот, что после загрузки.
    monkeypatch.setattr(app, "LOAD_UI_MIN_INTERVAL_S", 0.0)
    monkeypatch.setattr(
        app,
        "st",
        StreamlitWithInterruptingSlot(
            load_environment.streamlit.session_state,
            slot_index=0,
            slot=InterruptingSlot("progress", 3),
        ),
    )

    with pytest.raises(RerunException):
        load_environment.run(FakeRawClient())

    record = read_single_record(load_environment.records)
    assert record["outcome"] == "ok"
    assert record["stats"]["rows"] == 3
    assert record["stats"]["source_fully_read"] is True
    assert cleanups == [], "залитые таблицы нельзя удалять из-за сбоя в отчёте"


def test_a_rerun_during_the_load_says_the_source_was_not_read_to_the_end(
    load_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Прерывание внутри загрузки: конец файла НЕ подтверждён.

    Здесь прерывание приходит на последнем блоке трёхстрочного файла, то есть
    строки в таблице как раз все. Поле про это и не говорит: оно утверждает
    ровно одно — итератор чанков не был исчерпан, значит право заявить «файл
    прочитан целиком» не заработано. Настоящее прерывание на середине файла
    проверяется на уровне загрузчика, в tests/test_pandas_loader.py.
    """
    monkeypatch.setattr(app, "LOAD_UI_MIN_INTERVAL_S", 0.0)
    monkeypatch.setattr(
        app,
        "st",
        StreamlitWithInterruptingSlot(
            load_environment.streamlit.session_state,
            slot_index=2,
            slot=InterruptingSlot("metric", 2),
        ),
    )

    with pytest.raises(RerunException):
        load_environment.run(FakeRawClient())

    record = read_single_record(load_environment.records)
    assert record["outcome"] == "interrupted"
    assert record["stats"]["source_fully_read"] is False


def _renders_ending_on_a_block_line(fake_st: FakeStreamlit) -> int:
    """Сколько отрисовок лога случилось сразу после строки о блоке.

    Строка про блок оказывается ПОСЛЕДНЕЙ в отрисованном тексте только тогда,
    когда отрисовка произошла прямо за ней. Придержанная строка попадёт в лог
    позже, уже не последней, — так и отличается «шлём каждый блок» от «шлём не
    чаще интервала».
    """
    return sum(
        1
        for slot in fake_st.slots
        for kind, text in slot.messages
        if kind == "code" and text.splitlines() and "Loaded chunk" in text.splitlines()[-1]
    )


class SlotFailingOnSuccess(FakeSlot):
    """Streamlit роняет обычное исключение, когда сообщение слишком велико."""

    def success(self, text: str) -> None:
        raise RuntimeError("ForwardMsg is too large")


def test_a_reporting_error_after_a_finished_load_does_not_drop_the_tables(
    load_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Обычное исключение из `st.*` уходит в `except Exception`, а тот удалял
    ОБЕ таблицы. Строки уже в ClickHouse: сбой отчёта не повод их уничтожать.

    Проверяется именно обычным исключением, а не RerunException: последняя
    наследует BaseException и мимо `except Exception` проходит, то есть чистку
    не проверяет вообще.
    """
    cleanups: list[str] = []
    monkeypatch.setattr(
        app,
        "_cleanup_after_failed_load",
        lambda client, config, distributed_table, log: cleanups.append(distributed_table),
    )
    monkeypatch.setattr(
        app,
        "st",
        StreamlitWithInterruptingSlot(
            load_environment.streamlit.session_state,
            slot_index=1,
            slot=SlotFailingOnSuccess(),
        ),
    )

    load_environment.run(FakeRawClient())

    record = read_single_record(load_environment.records)
    assert cleanups == [], "таблицы с залитыми строками удалены из-за сбоя отчёта"
    assert record["outcome"] == "ok"
    assert record["stats"]["rows"] == 3
    assert record["error"] is not None, "причина сбоя отчёта не должна пропадать"
    # Текст обязан говорить про отчёт, а не про загрузку: `_format_load_error`
    # рассказывает про сбой ЗАГРУЗКИ и на этом пути врал бы.
    assert "load error" not in record["error"].lower(), record["error"]
    assert "report" in record["error"].lower(), record["error"]


def test_progress_updates_are_throttled_instead_of_firing_on_every_block(
    load_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """381 блок прошлого прогона давал 381 обновление и лог, растущий O(n²)."""
    monkeypatch.setattr(app, "LOAD_UI_MIN_INTERVAL_S", 3600.0)

    load_environment.run(FakeRawClient())

    metrics = [metric for slot in load_environment.streamlit.slots for metric in slot.metrics]
    assert _renders_ending_on_a_block_line(load_environment.streamlit) == 1, (
        "лог тоже не должен уходить в сокет на каждый блок"
    )
    # Придержать промежуточные обновления можно, оставить плитку с устаревшим
    # числом навсегда — нет: после загрузки она обязана показать итог.
    assert metrics[-1] == ("Inserted rows", 3), metrics


def test_a_zero_interval_still_updates_on_every_block(
    load_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Обратный конец: троттлинг не должен ПРОПУСКАТЬ обновления навсегда."""
    monkeypatch.setattr(app, "LOAD_UI_MIN_INTERVAL_S", 0.0)

    load_environment.run(FakeRawClient())

    metrics = [metric for slot in load_environment.streamlit.slots for metric in slot.metrics]
    assert len(metrics) == 3, "два блока плюс итоговое обновление"
    assert _renders_ending_on_a_block_line(load_environment.streamlit) == 2


def test_the_throttle_rearms_after_the_interval_passes(
    load_environment,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Неградусный интервал, а рабочий: обновления обязаны ВОЗОБНОВЛЯТЬСЯ.

    Оба прежних теста стояли на крайностях (0 и 3600), и троттлинг, который
    замолкает навсегда после первого блока либо считает интервал вдвое, проходил
    бы их. Здесь вставка спит дольше интервала, поэтому оба блока попадают в
    интерфейс, а придержать их можно только сломанными часами.
    """
    monkeypatch.setattr(app, "LOAD_UI_MIN_INTERVAL_S", 0.02)

    class SlowClient(FakeRawClient):
        def raw_insert(self, **kwargs):
            time.sleep(0.05)
            return super().raw_insert(**kwargs)

    load_environment.run(SlowClient())

    assert _renders_ending_on_a_block_line(load_environment.streamlit) == 2


def test_the_rendered_log_is_bounded_instead_of_resending_everything() -> None:
    """Лечение O(n²): в сокет уходит хвост, а не весь накопленный лог."""
    slot = FakeSlot()
    messages = [f"line {index}" for index in range(app.LOAD_LOG_TAIL_LINES + 50)]

    app._render_load_log(slot, messages)

    text = slot.messages[-1][1]
    lines = text.splitlines()
    assert f"line {len(messages) - 1}" in text
    assert "line 0" not in text
    # Хвост плюс ровно одна строка о скрытом: без неё оператор не узнает, что
    # часть лога обрезана, и решит, что загрузка началась с середины.
    assert len(lines) == app.LOAD_LOG_TAIL_LINES + 1
    assert not lines[0].startswith("line "), lines[0]
    # Именно число, а не подстрока: `"50" in ...` выполнялось бы и на строке
    # `line 50`, то есть держало бы неверный счётчик скрытых.
    assert re.findall(r"\d+", lines[0]) == ["50"], lines[0]


def test_the_run_record_names_the_socket_the_load_actually_used(load_environment) -> None:
    """Отвечает на «прогон шёл через туннель или напрямую» — без этого сравнение
    двух прогонов имеет неконтролируемый конфаундер."""

    class ClientWithPool(FakeRawClient):
        def __init__(self) -> None:
            super().__init__()
            sock = SimpleNamespace(
                fileno=lambda: 7,
                getsockname=lambda: ("192.0.2.220", 51515),
                getpeername=lambda: ("10.1.2.3", 443),
            )
            pool = SimpleNamespace(pool=SimpleNamespace(queue=[SimpleNamespace(sock=sock)]))
            self.http = SimpleNamespace(pools={"key": pool})

    load_environment.run(ClientWithPool())

    record = read_single_record(load_environment.records)
    assert record["stats"]["connection_path"] == {
        "local_network": "192.0.0.0/16",
        "local_is_private": True,
        "remote_network": "10.1.0.0/16",
        "remote_is_private": True,
    }
    # Ни один адрес хоста не должен попасть в файл, который прикладывают к issue.
    assert "192.0.2.220" not in json.dumps(record)
    assert "10.1.2.3" not in json.dumps(record)
