"""Панель загрузки: живой прогресс, отмена, итог, переживающий перерисовку.

Сама загрузка проверяется в test_load_job.py; здесь — контракт «состояние
задачи → экран»: задачи в тестах собираются состоянием, без потоков.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from streamlit.testing.v1 import AppTest

import csv_click.app as app
import csv_click.load_job as load_job
from csv_click.clickhouse import ClickHouseConfig
from csv_click.load_job import LoadJob
from csv_click.load_stats import LoadStats
from csv_click.pandas_loader import ReadOptions, SchemaMapping, mappings_to_schema


@pytest.fixture(autouse=True)
def clean_registry():
    load_job.reset_load_job_registry()
    yield
    load_job.reset_load_job_registry()


def _make_job(tmp_path: Path) -> LoadJob:
    csv_path = tmp_path / "ui.csv"
    csv_path.write_text("ID\n1\n", encoding="utf_8")
    mappings = [SchemaMapping("ID", "ID", True, "UInt64", False)]
    return LoadJob(
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
        strict_preflight=False,
        schema_inference_mode="Fast sample, 100000 rows",
    )


def _running_job(tmp_path: Path) -> LoadJob:
    """Задача в состоянии «идёт загрузка» — без настоящего потока."""
    job = _make_job(tmp_path)
    job._started = True
    job.phase = "Loading CSV chunks through JSONEachRow..."
    job.stats.src_bytes = 100 * 1024 * 1024
    job.stats.src_read_bytes = 25 * 1024 * 1024
    job.stats.rows = 123_456
    job.log("Loading CSV chunks through JSONEachRow.")
    return job


def _finished_job(
    tmp_path: Path,
    outcome: str = "ok",
    fate: str = load_job.TABLES_CREATED,
    error: str | None = None,
) -> LoadJob:
    job = _make_job(tmp_path)
    job._started = True
    job.outcome = outcome
    job.error_message = error
    job.tables_fate = fate
    job.stats.rows = 3
    job.stats.blocks = 2
    job.stats.total_s = 1.5
    job.record_path = tmp_path / "runs" / "record.json"
    job.log("Load finished: 3 rows in 1.50 sec.")
    job._finished.set()
    return job


def _install(job: LoadJob) -> None:
    """Кладёт задачу в реестр процесса, не запуская поток."""
    load_job._current_job = job


def _app() -> None:
    import streamlit as st

    from csv_click.app import main

    st.session_state.setdefault("app_settings_loaded", False)
    main()


def _texts(elements) -> str:
    return "\n".join(str(block.value) for block in elements)


def test_a_running_load_shows_phase_progress_and_cancel(tmp_path: Path) -> None:
    _install(_running_job(tmp_path))

    at = AppTest.from_function(_app, default_timeout=30)
    at.run()

    assert not len(at.exception), at.exception
    assert "Loading CSV chunks through JSONEachRow..." in _texts(at.info)
    captions = _texts(at.caption)
    assert "Read 25 of 100 MB" in captions
    assert "ahead of confirmed inserts" in captions
    assert any(button.label == "Cancel load" for button in at.button)
    # Хвост лога на экране: без него отмену не с чем соотнести.
    assert "JSONEachRow" in _texts(at.code)


def test_clicking_cancel_asks_the_job_to_stop(tmp_path: Path) -> None:
    job = _running_job(tmp_path)
    _install(job)

    at = AppTest.from_function(_app, default_timeout=30)
    at.run()
    at.button(key="cancel_load_button").click().run()

    assert job.cancel_requested is True
    assert "Cancelling" in _texts(at.warning)
    assert "Cancel requested" in "\n".join(job.log_lines())


def test_the_result_survives_an_extra_rerun(tmp_path: Path) -> None:
    """Раньше итог жил в st.empty() и исчезал после первой же перерисовки;
    поля 0b/8a оставались только в JSON-записи."""
    _install(_finished_job(tmp_path))

    at = AppTest.from_function(_app, default_timeout=30)
    at.run()
    assert "Load finished: 3 rows" in _texts(at.success)

    at.run()  # дополнительная перерисовка — та самая, что раньше стирала итог

    assert "Load finished: 3 rows" in _texts(at.success)
    captions = _texts(at.caption)
    assert "loaded rows" in captions, "судьба таблиц не показана"
    assert "record.json" in captions, "путь записи прогона не показан"


def test_a_cancelled_result_warns_and_names_the_tables_fate(tmp_path: Path) -> None:
    _install(
        _finished_job(tmp_path, outcome="cancelled", fate=load_job.TABLES_KEPT_WITH_DATA)
    )

    at = AppTest.from_function(_app, default_timeout=30)
    at.run()

    warnings = _texts(at.warning)
    assert "Load cancelled" in warnings
    assert "3 rows" in warnings
    assert "KEPT" in _texts(at.caption)


def test_a_failed_result_shows_the_error_and_opens_the_log(tmp_path: Path) -> None:
    _install(
        _finished_job(
            tmp_path,
            outcome="failed",
            fate=load_job.TABLES_DROPPED_AS_EMPTY,
            error="ClickHouse raw insert failed for sandbox.orders",
        )
    )

    at = AppTest.from_function(_app, default_timeout=30)
    at.run()

    assert "raw insert failed" in _texts(at.error)
    assert "dropped as empty" in _texts(at.caption)
    log_expanders = [
        node
        for node in at.main
        if type(node).__name__ == "Expander" and node.proto.label == "Load log"
    ]
    assert log_expanders, "лог завершённой загрузки не найден"
    assert log_expanders[0].proto.expanded, "после сбоя лог должен быть раскрыт"


def test_a_second_start_warns_instead_of_replacing_the_running_job(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    running = _running_job(tmp_path)
    _install(running)

    class RecorderSt:
        def __init__(self) -> None:
            self.session_state: dict[str, object] = {}
            self.warnings: list[str] = []
            self.errors: list[str] = []

        def warning(self, text: str) -> None:
            self.warnings.append(text)

        def error(self, text: str) -> None:
            self.errors.append(text)

    recorder = RecorderSt()
    monkeypatch.setattr(app, "st", recorder)

    app._start_load(
        config=ClickHouseConfig(database="sandbox", cluster="clickhouse"),
        csv_path=str(tmp_path / "ui.csv"),
        read_options=ReadOptions(batch_size=2),
        schema=mappings_to_schema([SchemaMapping("ID", "ID", True, "UInt64", False)]),
        distributed_table="orders",
        order_by="ID",
        partition_by=None,
        sharding_key="ID",
        max_insert_payload_mb=16,
        load_workers=1,
        insert_compression="off",
        strict_preflight=False,
    )

    assert any("already running" in warning for warning in recorder.warnings)
    assert load_job.current_load_job() is running, "живую задачу заменили"


def test_a_vanished_csv_shows_an_error_instead_of_a_raw_traceback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Файл удалили между «Read CSV» и кликом загрузки: раньше это ронял прогон
    сырым FileNotFoundError (регрессия против main, где был except Exception)."""

    class RecorderSt:
        def __init__(self) -> None:
            self.session_state: dict[str, object] = {
                "type_rows": [
                    {
                        "source_name": "ID",
                        "target_name": "ID",
                        "include": True,
                        "inferred_type": "UInt64",
                        "final_type": "UInt64",
                        "custom_type": "",
                        "nullable": False,
                        "sample_values": "",
                        "notes": "",
                    }
                ]
            }
            self.warnings: list[str] = []
            self.errors: list[str] = []

        def warning(self, text: str) -> None:
            self.warnings.append(text)

        def error(self, text: str) -> None:
            self.errors.append(text)

    recorder = RecorderSt()
    monkeypatch.setattr(app, "st", recorder)

    app._start_load(
        config=ClickHouseConfig(database="sandbox", cluster="clickhouse"),
        csv_path=str(tmp_path / "vanished.csv"),
        read_options=ReadOptions(batch_size=2),
        schema=mappings_to_schema([SchemaMapping("ID", "ID", True, "UInt64", False)]),
        distributed_table="orders",
        order_by="ID",
        partition_by=None,
        sharding_key="ID",
        max_insert_payload_mb=16,
        load_workers=1,
        insert_compression="off",
        strict_preflight=False,
    )

    assert recorder.errors, "исчезнувший файл должен дать внятную ошибку, а не трейсбек"
    assert load_job.current_load_job() is None, "задача не должна была создаться"


def test_the_finished_log_is_bounded_on_screen(tmp_path: Path) -> None:
    """Итоговый экран резал лог так же, как живой: полный лог в сокет на каждом
    прогоне — та самая O(n²), против которой введён LOAD_LOG_TAIL_LINES."""
    job = _finished_job(tmp_path)
    for index in range(app.LOAD_LOG_TAIL_LINES + 50):
        job.log(f"line {index}")
    _install(job)

    at = AppTest.from_function(_app, default_timeout=30)
    at.run()

    log_blocks = [str(block.value) for block in at.code]
    assert log_blocks, "лог завершённой загрузки не найден"
    rendered = max(log_blocks, key=len)
    lines = rendered.splitlines()
    assert len(lines) <= app.LOAD_LOG_TAIL_LINES + 1, "полный лог уехал в сокет"
    assert "omitted" in lines[0], "оператор не узнает, что лог обрезан"


def test_load_progress_line_reports_bytes_not_rows() -> None:
    stats = LoadStats(src_bytes=100 * 1024 * 1024, src_read_bytes=25 * 1024 * 1024)

    fraction, caption = app.load_progress_line(stats)

    assert fraction == pytest.approx(0.25)
    assert "Read 25 of 100 MB" in caption
    assert "(25%)" in caption
    # Подпись обязана честно говорить, что чтение опережает вставку.
    assert "ahead of confirmed inserts" in caption


def test_load_progress_line_survives_zero_and_overflow() -> None:
    assert app.load_progress_line(LoadStats()) == (0.0, "Reading the source file...")

    # Счётчик чтения может на волосок обогнать размер (упреждающий буфер);
    # доля не имеет права вылезать за 1.0 — st.progress на этом падает.
    fraction, _ = app.load_progress_line(LoadStats(src_bytes=10, src_read_bytes=11))
    assert fraction == 1.0


def test_every_tables_fate_has_a_human_caption() -> None:
    fates = (
        load_job.TABLES_CREATED,
        load_job.TABLES_NOT_CREATED,
        load_job.TABLES_KEPT_WITH_DATA,
        load_job.TABLES_DROPPED_AS_EMPTY,
        load_job.TABLES_CLEANUP_FAILED,
    )

    captions = [app.tables_fate_caption(fate) for fate in fates]

    assert len(set(captions)) == len(fates), "судьбы таблиц слились в одну подпись"
    # Незнакомый fate не прячется за молчанием и не падает.
    assert "mystery" in app.tables_fate_caption("mystery")


class _FakeSlot:
    def __init__(self) -> None:
        self.rendered: list[str] = []

    def code(self, text: str, language: str | None = None) -> None:
        self.rendered.append(text)


def test_the_rendered_log_is_bounded_instead_of_resending_everything() -> None:
    """Лечение O(n²): в сокет уходит хвост, а не весь накопленный лог."""
    import re

    slot = _FakeSlot()
    messages = [f"line {index}" for index in range(app.LOAD_LOG_TAIL_LINES + 50)]

    app._render_load_log(slot, messages)

    text = slot.rendered[-1]
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
