"""Поведенческие тесты на LoadJob — загрузку в фоновом потоке.

Сюда переехало поведение `_create_and_load`: часы, счётчики, запись прогона,
судьба таблиц. Новое против app-версии: отмена, исход `cancelled`, реестр
одной задачи на процесс и полная свобода от Streamlit.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import csv_click.load_job as load_job
from csv_click.clickhouse import ClickHouseConfig
from csv_click.errors import ClickHouseConnectionError, CsvLoadCancelled, CsvLoadError
from csv_click.load_job import (
    TABLES_CLEANUP_FAILED,
    TABLES_CREATED,
    TABLES_DROPPED_AS_EMPTY,
    TABLES_KEPT_WITH_DATA,
    TABLES_NOT_CREATED,
    LoadJob,
    current_load_job,
    reset_load_job_registry,
    start_load_job,
)
from csv_click.pandas_loader import (
    ReadOptions,
    SchemaMapping,
    mappings_to_schema,
)

BLOCK_SERVER_NS = 3_000_000


class FakeRawClient:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def raw_insert(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(summary={"elapsed_ns": str(BLOCK_SERVER_NS)})


@pytest.fixture(autouse=True)
def clean_registry():
    reset_load_job_registry()
    yield
    reset_load_job_registry()


@pytest.fixture
def job_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    csv_path = tmp_path / "load.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]
    monkeypatch.setattr(load_job, "test_connection", lambda client: None)
    monkeypatch.setattr(load_job, "create_tables", lambda **kwargs: None)

    records: list[Path] = []
    real_write_run_record = load_job.write_run_record

    def write_run_record_to_tmp(**kwargs):
        record_path = real_write_run_record(**{**kwargs, "directory": tmp_path / "runs"})
        records.append(record_path)
        return record_path

    monkeypatch.setattr(load_job, "write_run_record", write_run_record_to_tmp)

    dropped: list[dict] = []
    monkeypatch.setattr(load_job, "drop_target_tables", lambda **kwargs: dropped.append(kwargs))

    def make_job(client, **overrides) -> LoadJob:
        monkeypatch.setattr(load_job, "get_client", lambda config: client)
        params = dict(
            config=ClickHouseConfig(database="sandbox", cluster="clickhouse"),
            csv_path=str(csv_path),
            read_options=ReadOptions(batch_size=2),
            schema=mappings_to_schema(mappings),
            mappings=mappings,
            distributed_table="orders",
            order_by="ID",
            partition_by=None,
            sharding_key="ID",
            max_insert_payload_mb=16,
            load_workers=1,
            insert_compression="off",
            strict_preflight=True,
            schema_inference_mode="Fast sample, 100000 rows",
        )
        params.update(overrides)
        return LoadJob(**params)

    return SimpleNamespace(
        make_job=make_job,
        records=records,
        dropped=dropped,
        csv_path=csv_path,
        mappings=mappings,
    )


def read_single_record(records: list[Path]) -> dict:
    assert len(records) == 1
    return json.loads(records[0].read_text(encoding="utf-8"))


def test_load_job_module_is_streamlit_free() -> None:
    """Тело загрузки не имеет права трогать Streamlit: st.* из чужого потока
    молча не работает, а RerunException убивал часовую заливку."""
    code = (
        "import sys\n"
        "import csv_click.load_job\n"
        "sys.exit(1 if 'streamlit' in sys.modules else 0)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        encoding="utf-8",
    )
    assert result.returncode == 0, result.stderr


def test_successful_job_reports_ok_created_tables_and_the_record(job_environment) -> None:
    client = FakeRawClient()
    job = job_environment.make_job(client)

    job.run()

    assert job.outcome == "ok"
    assert job.error_message is None
    assert job.tables_fate == TABLES_CREATED
    assert job.stats.rows == 3
    assert job.stats.source_fully_read is True
    assert job.record_path is not None

    record = read_single_record(job_environment.records)
    assert record["outcome"] == "ok"
    assert record["tables"] == {
        "distributed": "orders",
        "local": "orders_local",
        "fate": "created",
    }
    assert record["stats"]["rows"] == 3
    assert record["stats"]["src_read_bytes"] == job_environment.csv_path.stat().st_size

    log = "\n".join(job.log_lines())
    assert "Load finished: 3 rows" in log
    assert "Run record saved to" in log


def test_cancel_before_run_leaves_no_tables_and_records_cancelled(job_environment) -> None:
    client = FakeRawClient()
    job = job_environment.make_job(client)

    job.request_cancel()
    job.run()

    assert job.outcome == "cancelled"
    assert job.tables_fate == TABLES_NOT_CREATED
    assert client.calls == []
    assert job_environment.dropped == []

    record = read_single_record(job_environment.records)
    assert record["outcome"] == "cancelled"
    assert record["tables"]["fate"] == "not_created"
    assert record["error"]


def test_cancel_after_a_confirmed_block_keeps_the_tables(job_environment) -> None:
    """Отмена после подтверждённых блоков идёт по логике 514466c: данные уже в
    таблицах, и удалять их — уничтожение работы, а не откат."""
    holder: dict[str, LoadJob] = {}

    class CancelsAfterFirstBlock(FakeRawClient):
        def raw_insert(self, **kwargs):
            result = super().raw_insert(**kwargs)
            holder["job"].request_cancel()
            return result

    client = CancelsAfterFirstBlock()
    job = job_environment.make_job(client, read_options=ReadOptions(batch_size=1))
    holder["job"] = job

    job.run()

    assert job.outcome == "cancelled"
    assert job.tables_fate == TABLES_KEPT_WITH_DATA
    assert job.stats.blocks >= 1
    assert job_environment.dropped == [], "таблицы с данными нельзя удалять при отмене"

    record = read_single_record(job_environment.records)
    assert record["outcome"] == "cancelled"
    assert record["tables"]["fate"] == "kept_with_data"

    log = "\n".join(job.log_lines())
    assert "KEPT" in log


def test_cancel_before_the_first_block_drops_the_empty_tables(
    job_environment, monkeypatch: pytest.MonkeyPatch
) -> None:
    def cancelled_immediately(**kwargs):
        raise CsvLoadCancelled("The load was cancelled by the operator")

    monkeypatch.setattr(load_job, "load_csv_via_raw_insert", cancelled_immediately)
    job = job_environment.make_job(FakeRawClient())

    job.run()

    assert job.outcome == "cancelled"
    assert job.tables_fate == TABLES_DROPPED_AS_EMPTY
    assert len(job_environment.dropped) == 1

    record = read_single_record(job_environment.records)
    assert record["tables"]["fate"] == "dropped_as_empty"


def test_failure_before_ddl_reports_not_created(job_environment, monkeypatch) -> None:
    def refuses_connection(config):
        raise ClickHouseConnectionError("no route to host")

    # После make_job: фикстура сама патчит get_client на возврат клиента.
    job = job_environment.make_job(FakeRawClient())
    monkeypatch.setattr(load_job, "get_client", refuses_connection)

    job.run()

    assert job.outcome == "failed"
    assert job.tables_fate == TABLES_NOT_CREATED
    assert "no route to host" in (job.error_message or "")
    assert job_environment.dropped == []

    record = read_single_record(job_environment.records)
    assert record["outcome"] == "failed"
    assert record["tables"]["fate"] == "not_created"


def test_failed_load_without_blocks_drops_and_records_it(job_environment, monkeypatch) -> None:
    def fails_before_sending_anything(**kwargs):
        raise CsvLoadError("boom before the first block")

    monkeypatch.setattr(load_job, "load_csv_via_raw_insert", fails_before_sending_anything)
    job = job_environment.make_job(FakeRawClient())

    job.run()

    assert job.outcome == "failed"
    assert job.tables_fate == TABLES_DROPPED_AS_EMPTY
    assert len(job_environment.dropped) == 1

    record = read_single_record(job_environment.records)
    assert record["tables"]["fate"] == "dropped_as_empty"


def test_a_failed_cleanup_is_reported_not_swallowed(job_environment, monkeypatch) -> None:
    def fails_before_sending_anything(**kwargs):
        raise CsvLoadError("boom before the first block")

    def drop_refuses(**kwargs):
        raise RuntimeError("DROP timed out")

    monkeypatch.setattr(load_job, "load_csv_via_raw_insert", fails_before_sending_anything)
    monkeypatch.setattr(load_job, "drop_target_tables", drop_refuses)
    job = job_environment.make_job(FakeRawClient())

    job.run()

    assert job.outcome == "failed"
    assert job.tables_fate == TABLES_CLEANUP_FAILED

    record = read_single_record(job_environment.records)
    assert record["tables"]["fate"] == "cleanup_failed"
    assert "Cleanup error" in "\n".join(job.log_lines())


def test_the_job_exposes_progress_while_running(job_environment, monkeypatch) -> None:
    """Интерфейс читает фазу, счётчики и лог, пока задача работает."""
    seen = SimpleNamespace(phase=None, running=None)
    release = threading.Event()
    holder: dict[str, LoadJob] = {}

    def slow_load(**kwargs):
        seen.phase = holder["job"].phase
        seen.running = holder["job"].is_running
        release.wait(5)
        kwargs["stats"].source_fully_read = True
        return kwargs["stats"]

    monkeypatch.setattr(load_job, "load_csv_via_raw_insert", slow_load)
    job = job_environment.make_job(FakeRawClient())
    holder["job"] = job

    job.start()
    try:
        deadline = time.monotonic() + 5
        while seen.running is None and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        release.set()
    assert job.wait(5)

    assert seen.running is True
    assert seen.phase == "Loading CSV chunks through JSONEachRow..."
    assert job.is_running is False
    assert job.outcome == "ok"


def test_registry_holds_one_running_job_per_process(job_environment, monkeypatch) -> None:
    """Двойной клик сегодня может запустить две заливки в одну таблицу; реестр
    разрешает одну живую задачу, а после завершения — замену."""
    release = threading.Event()
    started = threading.Event()

    def slow_load(**kwargs):
        started.set()
        release.wait(5)
        kwargs["stats"].source_fully_read = True
        return kwargs["stats"]

    monkeypatch.setattr(load_job, "load_csv_via_raw_insert", slow_load)
    first = job_environment.make_job(FakeRawClient())
    second = job_environment.make_job(FakeRawClient())

    assert start_load_job(first) is True
    assert started.wait(5)
    assert start_load_job(second) is False, "вторая загрузка при живой первой не стартует"
    assert current_load_job() is first

    release.set()
    assert first.wait(5)

    assert start_load_job(second) is True
    assert second.wait(5)
    assert current_load_job() is second


# --- поведение, переехавшее из test_create_and_load.py -------------------------------


def test_successful_load_reports_every_clock_and_persists_the_run_record(
    job_environment, monkeypatch
) -> None:
    # Заметная пауза, чтобы connect_s был отличим от нуля: иначе тест прошёл бы
    # и на захардкоженном нуле.
    monkeypatch.setattr(load_job, "test_connection", lambda client: time.sleep(0.05))
    client = FakeRawClient()
    job = job_environment.make_job(client)

    job.run()

    log = "\n".join(job.log_lines())
    assert "Load finished: 3 rows" in log
    assert "preflight" in log and "insert wall" in log
    assert "Server reported" in log
    assert len(client.calls) == 2

    record = read_single_record(job_environment.records)
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
    assert record["stats"]["src_bytes"] == job_environment.csv_path.stat().st_size
    assert record["stats"]["server_share"] is not None
    assert record["stats"]["worker_count"] == 1
    assert record["config"]["batch_size"] == 2
    assert record["config"]["table"] == "orders"
    assert record["config"]["load_workers"] == 1
    # Обе отметки пула PyArrow сняты — иначе «прирост за загрузку» не посчитать.
    assert isinstance(record["stats"]["arrow_bytes_at_start"], int)
    assert isinstance(record["stats"]["arrow_bytes"], int)
    assert record["stats"]["arrow_bytes"] >= record["stats"]["arrow_bytes_at_start"]


def test_the_chosen_codec_reaches_the_wire_and_the_record(job_environment, monkeypatch) -> None:
    """Настройка, которая не доезжает до провода, — это ложное ускорение.

    Оператор увидит `zstd` в записи, прежнюю скорость и не поймёт, почему.
    Поэтому проверяется и то, что уехало в клиент, и то, что записано.
    """
    sent: list[str | None] = []
    original = load_job.load_csv_via_raw_insert

    def spy(**kwargs):
        sent.append(kwargs.get("compression"))
        return original(**kwargs)

    monkeypatch.setattr(load_job, "load_csv_via_raw_insert", spy)
    job = job_environment.make_job(FakeRawClient(), insert_compression="zstd")

    job.run()

    record = read_single_record(job_environment.records)
    assert sent == ["zstd"], "кодек не доехал до загрузчика"
    assert record["config"]["insert_compression"] == "zstd"
    # Тело обязано отличаться от исходного. МЕНЬШЕ оно тут не будет: три строки
    # в 61 байт дают 71 из-за заголовка кадра zstd. Выигрыш появляется на
    # мегабайтных блоках (замерено 3,76x на 9,5 МБ).
    assert record["stats"]["wire_bytes"] != record["stats"]["raw_bytes"]
    assert record["stats"]["compress_s"] > 0
    assert "compressed" in "\n".join(job.log_lines())


def test_the_default_setting_does_not_put_off_into_the_header(job_environment) -> None:
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

    job = job_environment.make_job(ProxyLikeClient())
    job.run()

    record = read_single_record(job_environment.records)
    assert record["outcome"] == "ok", record["error"]
    assert record["stats"]["rows"] == 3


def test_without_compression_the_wire_carries_the_raw_payload(job_environment) -> None:
    job = job_environment.make_job(FakeRawClient())
    job.run()

    record = read_single_record(job_environment.records)
    assert record["config"]["insert_compression"] == "off"
    assert record["stats"]["wire_bytes"] == record["stats"]["raw_bytes"]
    assert record["stats"]["compress_s"] == 0.0


def test_the_record_answers_who_was_the_bottleneck(job_environment) -> None:
    """Прогон на 500 млн строк оставил 85% времени необъяснёнными.

    Теперь запись отвечает на это сама: сколько продюсер стоял, сколько длилась
    вставка и какую долю в ней занял сервер. Занятость считается только здесь —
    `insert_wall_s` ставит задача, загрузчик его не знает.
    """
    job = job_environment.make_job(FakeRawClient())
    job.run()

    record = read_single_record(job_environment.records)
    stats = record["stats"]

    assert stats["insert_busy_s"] > 0, "время самой вставки не замерено"
    assert stats["producer_stall_s"] >= 0
    assert stats["insert_queue_s"] >= 0

    log = "\n".join(job.log_lines())
    assert "Who waited for whom" in log
    assert "workers were busy" in log
    assert "Unattributed producer time" in log


def test_the_job_arms_the_driver_retry_counter_around_the_load(job_environment) -> None:
    """Ничто иначе не связывает счётчик с настоящей загрузкой: без этого теста
    его можно вообще не вызывать, и все тесты останутся зелёными."""
    import logging

    from csv_click.load_stats import DRIVER_LOGGER_NAME, DRIVER_RETRY_MESSAGE

    driver_logger = logging.getLogger(DRIVER_LOGGER_NAME)

    class ReconnectingClient(FakeRawClient):
        def raw_insert(self, **kwargs):
            driver_logger.debug("%s (attempt %s/%s)", DRIVER_RETRY_MESSAGE, 1, 2)
            return super().raw_insert(**kwargs)

    job = job_environment.make_job(ReconnectingClient())
    job.run()

    record = read_single_record(job_environment.records)
    assert record["stats"]["driver_retries"] == 2
    assert "re-uploaded a request body 2 time(s)" in "\n".join(job.log_lines())


def test_insert_wall_time_excludes_preflight_connect_and_ddl(job_environment, monkeypatch) -> None:
    """server % считается от insert_wall_s именно потому, что раньше в тот же
    счётчик попадали preflight, connect и DDL — до 2.5% времени, списанного на провод."""
    monkeypatch.setattr(load_job, "create_tables", lambda **kwargs: time.sleep(0.5))

    job = job_environment.make_job(FakeRawClient())
    job.run()

    record = read_single_record(job_environment.records)
    assert record["stats"]["ddl_s"] >= 0.5
    # Строго больше нуля: односторонняя проверка прошла бы и если бы
    # insert_wall_s вообще не присвоили.
    assert record["stats"]["insert_wall_s"] > 0
    assert record["stats"]["insert_wall_s"] < 0.5
    assert record["stats"]["total_s"] > record["stats"]["ddl_s"]


def test_failed_load_still_persists_the_run_record_with_partial_counters(job_environment) -> None:
    class FailsOnSecondBlock(FakeRawClient):
        def raw_insert(self, **kwargs):
            if b'"ID":3' in kwargs["insert_block"]:
                raise RuntimeError("HTTP status 500: the read limit is reached")
            return super().raw_insert(**kwargs)

    job = job_environment.make_job(FailsOnSecondBlock())
    job.run()

    # Первый блок уже в таблице: удалять её из-за сбоя на втором - значит
    # уничтожить залитое. Один транзиентный 5xx на 900-м блоке из 1000 стоил бы
    # всей работы.
    assert job_environment.dropped == [], "таблицы с залитыми строками удалены из-за сбоя загрузки"
    assert job.tables_fate == TABLES_KEPT_WITH_DATA
    assert "kept" in "\n".join(job.log_lines()).lower()

    record = read_single_record(job_environment.records)
    assert record["outcome"] == "failed"
    assert "HTTP/proxy read limit" in record["error"]
    assert record["tables"]["fate"] == "kept_with_data"
    assert record["stats"]["rows"] == 2
    assert record["stats"]["blocks"] == 1
    assert record["stats"]["insert_wall_s"] > 0
    assert record["config"]["batch_size"] == 2


def test_failure_before_the_first_block_still_drops_the_empty_tables(
    job_environment, monkeypatch
) -> None:
    """Пока в таблицах ничего нет, чистка - это откат создания, и она нужна.

    Иначе после неудачной попытки остаётся пустая пара таблиц, и следующая
    загрузка того же имени упирается в `ExistingTableError`.
    """
    from csv_click.errors import CsvSchemaError

    def fails_before_sending_anything(**kwargs):
        raise CsvSchemaError("Cannot convert chunk 1, column 'ID', value 'x' to UInt64")

    monkeypatch.setattr(load_job, "load_csv_via_raw_insert", fails_before_sending_anything)

    job = job_environment.make_job(FakeRawClient())
    job.run()

    assert len(job_environment.dropped) == 1
    assert job.tables_fate == TABLES_DROPPED_AS_EMPTY
    record = read_single_record(job_environment.records)
    assert record["stats"]["blocks"] == 0
    assert record["stats"]["blocks_unconfirmed"] == 0


def test_an_unconfirmed_block_without_a_confirmed_one_still_drops(job_environment) -> None:
    """Ни один блок не подтверждён - таблицы считаются пустыми и удаляются.

    `blocks_unconfirmed` не отвечает на вопрос «есть ли данные»: туда попадают
    и блоки, которые не отправлялись вовсе. Держаться за него значило бы
    оставлять пустую пару после каждого отказа кодека.
    """

    class LosesTheAnswer(FakeRawClient):
        def raw_insert(self, **kwargs):
            super().raw_insert(**kwargs)
            raise RuntimeError("Connection aborted before the answer arrived")

    job = job_environment.make_job(LosesTheAnswer())
    job.run()

    record = read_single_record(job_environment.records)
    assert record["stats"]["blocks"] == 0
    assert record["stats"]["blocks_unconfirmed"] >= 1
    assert len(job_environment.dropped) == 1
    assert record["tables"]["fate"] == "dropped_as_empty"
    # Оператор обязан узнать про размен: долетевший блок ушёл вместе с таблицей.
    assert "never came back confirmed" in "\n".join(job.log_lines())


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

    load_job._handle_tables_after_stopped_load(
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

    fate = load_job._handle_tables_after_stopped_load(
        _RecordingClient(),
        ClickHouseConfig(database="sandbox", cluster="clickhouse"),
        "orders",
        lines.append,
        LoadStats(rows=4_000, blocks=2),
    )

    message = "\n".join(lines)
    assert fate == TABLES_KEPT_WITH_DATA
    assert "sandbox.orders" in message
    assert "sandbox.orders_local" in message
    assert "4000 rows" in message
    assert "2 confirmed" in message


def test_a_cancelled_load_says_cancelled_not_failed_in_the_kept_message() -> None:
    """Оператор отменил сам — сообщение не имеет права называть это сбоем."""
    from csv_click.load_stats import LoadStats

    lines: list[str] = []

    load_job._handle_tables_after_stopped_load(
        _RecordingClient(),
        ClickHouseConfig(database="sandbox", cluster="clickhouse"),
        "orders",
        lines.append,
        LoadStats(rows=100, blocks=1),
        reason="cancelled",
    )

    message = "\n".join(lines)
    assert "Load cancelled" in message
    assert "Load failed" not in message


def test_dropping_after_unconfirmed_blocks_names_the_tradeoff() -> None:
    """Удаляя пару, надо сказать: долетевший блок ушёл вместе с ней.

    Сообщение не должно утверждать, что сервер не ответил: драйвер бросает один
    и тот же `OperationalError` и на отказ, и на обрыв, а `blocks_unconfirmed`
    считает ещё и блоки, которые не отправлялись.
    """
    from csv_click.load_stats import LoadStats

    lines: list[str] = []

    load_job._handle_tables_after_stopped_load(
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
    job_environment,
) -> None:
    """Фейк повторяет то, что реально отдаёт драйвер при срезанном заголовке:
    сводку с одним query_id, а не None и не пустой словарь (httpclient.py:444)."""

    class StrippedSummaryClient(FakeRawClient):
        def raw_insert(self, **kwargs):
            super().raw_insert(**kwargs)
            return SimpleNamespace(summary={"query_id": "01234567-89ab-cdef"})

    job = job_environment.make_job(StrippedSummaryClient())
    job.run()

    log = "\n".join(job.log_lines())
    assert "Server time is not computable" in log
    assert "2 of 2 blocks returned no elapsed_ns" in log
    assert "% of insert wall time" not in log

    record = read_single_record(job_environment.records)
    assert record["outcome"] == "ok"
    assert record["stats"]["blocks_without_server_time"] == 2
    assert record["stats"]["server_share"] is None


def test_an_unexpected_base_exception_is_recorded_as_interrupted(
    job_environment, monkeypatch
) -> None:
    """Страховка от BaseException мимо `except Exception`: запись не имеет права
    утверждать «failed» с пустой причиной. Штатно ветка недостижима — в теле
    задачи нет st.*, бросать RerunException некому."""

    class HardStop(BaseException):
        pass

    monkeypatch.setattr(
        load_job,
        "create_tables",
        lambda **kwargs: (_ for _ in ()).throw(HardStop("stop")),
    )

    job = job_environment.make_job(FakeRawClient())
    with pytest.raises(HardStop):
        job.run()

    record = read_single_record(job_environment.records)
    assert record["outcome"] == "interrupted"
    assert "interrupted" in record["error"]
    assert record["stats"]["rows"] == 0
    assert job.outcome == "interrupted"


def test_the_run_record_names_the_socket_the_load_actually_used(job_environment) -> None:
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

    job = job_environment.make_job(ClientWithPool())
    job.run()

    record = read_single_record(job_environment.records)
    assert record["stats"]["connection_path"] == {
        "local_network": "192.0.0.0/16",
        "local_is_private": True,
        "remote_network": "10.1.0.0/16",
        "remote_is_private": True,
    }
    # Ни один адрес хоста не должен попасть в файл, который прикладывают к issue.
    assert "192.0.2.220" not in json.dumps(record)
    assert "10.1.2.3" not in json.dumps(record)


def test_reporting_failure_after_a_finished_load_keeps_outcome_ok(job_environment, monkeypatch) -> None:
    """Загрузка прошла, сломался отчёт: строки в таблицах, запись говорит ok
    с текстом про отчёт, таблицы никто не трогает."""

    def format_refuses(stats):
        raise RuntimeError("no format for you")

    monkeypatch.setattr(load_job, "format_load_stats_lines", format_refuses)
    job = job_environment.make_job(FakeRawClient())

    job.run()

    assert job.outcome == "ok"
    assert job.tables_fate == TABLES_CREATED
    assert "reporting it failed" in (job.error_message or "")
    assert job_environment.dropped == []

    record = read_single_record(job_environment.records)
    assert record["outcome"] == "ok"
    assert record["error"]
    assert record["tables"]["fate"] == "created"
