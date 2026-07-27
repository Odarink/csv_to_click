"""Поведенческие тесты на _create_and_load.

Остальные проверки этой функции в `test_app_state.py` — сопоставления исходника
с литералами: они ловят переименования, но не ловят ни одной ошибки в потоке
данных. Фаза 0 перевела функцию на LoadStats и запись о прогоне, поэтому здесь
проверяется собственно поведение: часы, счётчики и файл прогона.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

import csv_click.app as app
from csv_click.load_stats import DRIVER_LOGGER_NAME, DRIVER_RETRY_MESSAGE
from csv_click.clickhouse import ClickHouseConfig
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

    def run(client) -> None:
        monkeypatch.setattr(app, "get_client", lambda config: client)
        app._create_and_load(
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
        lambda client, config, distributed_table, log: cleanups.append(distributed_table),
    )

    class FailsOnSecondBlock(FakeRawClient):
        def raw_insert(self, **kwargs):
            if b'"ID":3' in kwargs["insert_block"]:
                raise RuntimeError("HTTP status 500: the read limit is reached")
            return super().raw_insert(**kwargs)

    load_environment.run(FailsOnSecondBlock())

    assert cleanups == ["orders"]
    record = read_single_record(load_environment.records)
    assert record["outcome"] == "failed"
    assert "HTTP/proxy read limit" in record["error"]
    assert record["stats"]["rows"] == 2
    assert record["stats"]["blocks"] == 1
    assert record["stats"]["insert_wall_s"] > 0
    assert record["config"]["batch_size"] == 2


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
