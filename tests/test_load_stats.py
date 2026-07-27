from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from csv_click.load_stats import (
    DRIVER_LOGGER_NAME,
    BlockProgress,
    DriverRetryCounter,
    LoadStats,
    RunConfig,
    describe_connection_path,
    format_load_stats_lines,
    write_run_record,
)


RUN_TIMESTAMP = datetime(2026, 7, 26, 14, 30, 12, tzinfo=timezone.utc)


def make_block(**overrides) -> BlockProgress:
    values: dict[str, object] = {
        "chunk_number": 1,
        "block_number": 1,
        "block_rows": 10,
        "rows_total": 10,
        "raw_bytes": 1000,
        "wire_bytes": 1000,
        "server_ns": 2_000_000,
        "server_time_reported": True,
    }
    values.update(overrides)
    return BlockProgress(**values)


def make_run_config() -> RunConfig:
    return RunConfig(
        batch_size=100_000,
        max_insert_payload_mb=16,
        effective_insert_payload_bytes=15_099_494,
        load_workers=4,
        insert_compression="zstd",
        strict_preflight=True,
        schema_inference_mode="Fast sample, 100000 rows",
        separator=",",
        encoding="utf_8",
        database="sandbox",
        table="orders",
        cluster="clickhouse",
        order_by="id",
        partition_by="toYYYYMM(dt)",
        sharding_key="id",
    )


def test_add_block_accumulates_rows_bytes_and_server_time() -> None:
    stats = LoadStats()

    stats.add_block(make_block(rows_total=10, raw_bytes=1000, wire_bytes=400, server_ns=2_000_000))
    stats.add_block(
        make_block(
            block_number=2,
            block_rows=5,
            rows_total=15,
            raw_bytes=500,
            wire_bytes=200,
            server_ns=1_000_000,
        )
    )

    assert stats.rows == 15
    assert stats.blocks == 2
    assert stats.raw_bytes == 1500
    assert stats.wire_bytes == 600
    assert stats.server_ns == 3_000_000
    assert stats.blocks_without_server_time == 0


def test_server_share_is_measured_against_insert_wall_time() -> None:
    stats = LoadStats(insert_wall_s=4.0)

    stats.add_block(make_block(server_ns=1_000_000_000))

    assert stats.server_share == pytest.approx(0.25)


def test_server_share_is_none_when_the_server_time_was_not_reported() -> None:
    stats = LoadStats(insert_wall_s=10.0)

    stats.add_block(make_block(server_ns=0, server_time_reported=False))

    assert stats.blocks_without_server_time == 1
    assert stats.server_share is None


def test_server_share_is_none_before_any_insert_wall_time_is_known() -> None:
    stats = LoadStats()

    stats.add_block(make_block())

    assert stats.server_share is None


def test_server_share_is_none_when_nothing_was_inserted() -> None:
    """Ноль блоков — это «нечего измерять», а не «сервер потратил 0%»."""
    stats = LoadStats(insert_wall_s=10.0)

    assert stats.blocks == 0
    assert stats.server_share is None


def test_server_share_is_none_on_the_parallel_path() -> None:
    """server_ns — это СУММА по одновременным запросам. Делить её на одни
    стенные часы нельзя: при шести воркерах честные 50 мс на блок дают «514%»."""
    stats = LoadStats(insert_wall_s=0.2, worker_count=6)
    for index in range(6):
        stats.add_block(make_block(block_number=index + 1, rows_total=10 * (index + 1), server_ns=200_000_000))

    naive_ratio = (stats.server_ns / 1_000_000_000) / stats.insert_wall_s
    assert naive_ratio > 1.0, "тест не воспроизвёл перекрытие запросов"
    assert stats.server_share is None


def test_the_report_states_the_compression_ratio_it_actually_got() -> None:
    """Коэффициент — единственное число, по которому судят о новой настройке.

    Без него оператор видит два размера и считает в уме, а решение «оставить
    сжатие или нет» упирается ровно в это отношение.
    """
    stats = LoadStats(insert_wall_s=10.0, compress_s=1.5)
    stats.add_block(make_block(raw_bytes=9_500_000, wire_bytes=2_520_000))

    report = " ".join(format_load_stats_lines(stats))

    assert "compressed 3.77x" in report
    assert "compress 1.50 s" in report


def test_the_report_says_nothing_about_a_ratio_when_nothing_was_compressed() -> None:
    stats = LoadStats(insert_wall_s=10.0)
    stats.add_block(make_block(raw_bytes=1000, wire_bytes=1000))

    report = " ".join(format_load_stats_lines(stats))

    # Именно коэффициента быть не должно; фраза «not compressed on this path»
    # рядом законна, поэтому проверяется шаблон, а не слово.
    assert re.search(r"compressed \d", report) is None, report
    assert "not compressed on this path" in report


def test_who_was_the_bottleneck_is_readable_from_the_record() -> None:
    """Прогон 5 оставил 85% времени в графе «прочее» — так больше нельзя.

    Три поля разделяют то, что раньше было слито в `insert_wall_s`: сколько
    продюсер стоял из-за очереди, сколько длилась вставка в воркере и сколько
    из неё занял сервер. Числа взяты с прогона 5: 1000 блоков, 5 воркеров,
    insert_wall 1948,55 с.
    """
    stats = LoadStats(
        insert_wall_s=1948.55,
        worker_count=5,
        read_s=123.92,
        convert_s=59.83,
        serialize_s=105.35,
        producer_stall_s=1659.46,
        insert_busy_s=9743.0,
    )
    for index in range(1000):
        stats.add_block(make_block(block_number=index + 1, rows_total=index + 1, server_ns=215_578_604))

    # Воркеры были заняты почти всё время: простаивал не пул, а провод.
    assert 0.98 < stats.worker_occupancy < 1.02
    # Из времени вставки на сервер приходятся считаные проценты.
    assert 0.02 < stats.server_share_of_insert < 0.03
    # Продюсерский поток не потерялся: остаток мал и объясним.
    assert abs(stats.producer_unattributed_s) < 1.0


def test_shares_are_none_when_they_cannot_be_computed() -> None:
    """Ноль вместо «не знаю» уже один раз стоил неверного вывода."""
    empty = LoadStats(insert_wall_s=10.0, worker_count=5)

    assert empty.worker_occupancy is None, "блоков не было — занятости нет"
    assert empty.server_share_of_insert is None
    assert empty.producer_unattributed_s is None

    stripped = LoadStats(insert_wall_s=10.0, worker_count=5, insert_busy_s=40.0)
    stripped.add_block(make_block(server_ns=0, server_time_reported=False))

    assert stripped.worker_occupancy is not None
    assert stripped.server_share_of_insert is None, "сервер не сообщил время — доли нет"


def test_format_load_stats_lines_names_the_bottleneck() -> None:
    stats = LoadStats(
        insert_wall_s=1948.55,
        worker_count=5,
        producer_stall_s=1659.46,
        insert_busy_s=9743.0,
        insert_queue_s=12.0,
    )
    for index in range(1000):
        stats.add_block(make_block(block_number=index + 1, rows_total=index + 1, server_ns=215_578_604))

    report = " ".join(format_load_stats_lines(stats))

    assert "producer stalled" in report
    assert "9.74 s" in report, "среднее время вставки на блок — главное число этого прогона"
    assert "2.2%" in report, "доля сервера внутри вставки"


def test_format_load_stats_lines_reports_every_clock_and_byte_count() -> None:
    stats = LoadStats(
        preflight_s=6.0,
        connect_s=1.0,
        ddl_s=58.0,
        insert_wall_s=100.0,
        read_s=3.0,
        convert_s=4.0,
        serialize_s=5.0,
        compress_s=7.0,
        src_bytes=3 * 1024 * 1024,
    )
    stats.add_block(
        make_block(server_ns=20_000_000_000, raw_bytes=2 * 1024 * 1024, wire_bytes=1024 * 1024)
    )

    report = " ".join(format_load_stats_lines(stats))

    assert "preflight 6.00 s" in report
    assert "connect 1.00 s" in report
    assert "DDL 58.00 s" in report
    assert "insert wall 100.00 s" in report
    assert "read 3.00 s" in report
    assert "convert 4.00 s" in report
    assert "serialize 5.00 s" in report
    assert "compress 7.00 s" in report
    assert "not compressed on this path" not in report
    assert "source 3.0 MB" in report
    assert "raw payload 2.0 MB" in report
    assert "on wire 1.0 MB in 1 blocks" in report
    assert "20.00 s = 20.0% of insert wall time" in report
    assert "covers the initiator" in report


def test_format_load_stats_lines_does_not_report_a_compress_stage_that_never_ran() -> None:
    """«compress 0.00 s» неотличимо от измерения «сжатие ничего не стоит», а
    сжатия в пути ещё нет вовсе — до фазы 2 об этом надо говорить словами."""
    stats = LoadStats(insert_wall_s=10.0, read_s=1.0, convert_s=2.0, serialize_s=3.0)
    stats.add_block(make_block())

    report = " ".join(format_load_stats_lines(stats))

    assert re.search(r"compress \d", report) is None
    assert "The body is not compressed on this path" in report
    assert "serialize 3.00 s" in report


def test_format_load_stats_lines_refuses_to_print_a_share_it_cannot_trust() -> None:
    stats = LoadStats(insert_wall_s=100.0)
    stats.add_block(make_block(server_ns=0, server_time_reported=False))

    report = " ".join(format_load_stats_lines(stats))

    assert "Server time is not computable" in report
    assert "1 of 1 blocks returned no elapsed_ns" in report
    assert "% of insert wall time" not in report


def test_format_load_stats_lines_never_turns_overlapping_requests_into_a_percentage() -> None:
    stats = LoadStats(insert_wall_s=0.2, worker_count=6)
    for index in range(6):
        stats.add_block(make_block(block_number=index + 1, rows_total=index + 1, server_ns=200_000_000))

    report = " ".join(format_load_stats_lines(stats))

    assert "summed over 6 concurrent connections" in report
    assert "load_workers=1" in report
    assert "% of insert wall time" not in report


def test_format_load_stats_lines_says_nothing_was_inserted_when_no_block_landed() -> None:
    report = " ".join(format_load_stats_lines(LoadStats(insert_wall_s=5.0)))

    assert "no blocks were inserted" in report
    assert "% of insert wall time" not in report


def test_format_load_stats_lines_points_driver_reuploads_in_the_right_direction() -> None:
    """wire_bytes перезалитые копии НЕ содержит, значит канал пронёс больше, чем
    там записано, и настоящая пропускная способность ВЫШЕ, а не ниже."""
    stats = LoadStats(insert_wall_s=10.0, driver_retries=3)
    stats.add_block(make_block())

    report = " ".join(format_load_stats_lines(stats))

    assert "re-uploaded a request body 3 time(s)" in report
    assert "HIGHER than these bytes imply" in report
    assert "Subtract" not in report


def test_format_load_stats_lines_marks_the_arrow_mark_as_process_wide() -> None:
    stats = LoadStats(
        insert_wall_s=10.0,
        arrow_bytes_at_start=100 * 1024 * 1024,
        arrow_bytes=180 * 1024 * 1024,
    )
    stats.add_block(make_block())

    report = " ".join(format_load_stats_lines(stats))

    assert "180.0 MB at the end" in report
    assert "100.0 MB before the load" in report
    assert "only the growth is attributable to this run" in report


def test_driver_retry_counter_counts_silent_body_reuploads() -> None:
    logger = logging.getLogger(DRIVER_LOGGER_NAME)

    with DriverRetryCounter() as counter:
        logger.debug("Retrying remotely closed connection (attempt %s/%s)", 1, 2)
        logger.debug("Successfully retrieved the response")

    assert counter.count == 1


def test_driver_retry_counter_restores_the_logger_it_borrowed() -> None:
    logger = logging.getLogger(DRIVER_LOGGER_NAME)
    level_before = logger.level
    propagate_before = logger.propagate
    handlers_before = list(logger.handlers)

    with DriverRetryCounter():
        assert logger.level == logging.DEBUG
        assert logger.propagate is False

    assert logger.level == level_before
    assert logger.propagate == propagate_before
    assert logger.handlers == handlers_before


def test_driver_retry_counter_still_lets_driver_warnings_out(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger(DRIVER_LOGGER_NAME)

    with caplog.at_level(logging.WARNING):
        with DriverRetryCounter():
            logger.warning("driver could not reuse the connection")

    assert "driver could not reuse the connection" in caplog.text


def test_nested_driver_retry_counters_each_count_their_own_window() -> None:
    logger = logging.getLogger(DRIVER_LOGGER_NAME)
    level_before = logger.level
    propagate_before = logger.propagate

    with DriverRetryCounter() as outer:
        with DriverRetryCounter() as inner:
            logger.debug("Retrying remotely closed connection (attempt 1/2)")
        # Внутренний вышел — состояние всё ещё принадлежит внешнему.
        assert logger.level == logging.DEBUG
        assert logger.propagate is False
        logger.debug("Retrying remotely closed connection (attempt 1/2)")

    assert inner.count == 1
    assert outer.count == 2
    assert logger.level == level_before
    assert logger.propagate == propagate_before
    assert not any(isinstance(handler, DriverRetryCounter) for handler in logger.handlers)


def test_a_counter_keeps_counting_after_an_overlapping_one_exits_first() -> None:
    """Восстанавливать состояние логгера обязан ПОСЛЕДНИЙ отцепившийся.

    Если это делает тот, кто вошёл первым, то на его выходе логгер возвращается
    из DEBUG, а всё ещё живой второй счётчик молча перестаёт что-либо считать:
    записи уровня DEBUG до него уже не доходят.
    """
    logger = logging.getLogger(DRIVER_LOGGER_NAME)
    level_before = logger.level
    propagate_before = logger.propagate

    first = DriverRetryCounter()
    second = DriverRetryCounter()
    try:
        first.__enter__()
        second.__enter__()
        first.__exit__(None, None, None)
        logger.debug("Retrying remotely closed connection (attempt 1/2)")
    finally:
        second.__exit__(None, None, None)

    assert second.count == 1
    assert logger.level == level_before
    assert logger.propagate == propagate_before


def test_overlapping_driver_retry_counters_do_not_leak_the_logger_state() -> None:
    """Настоящий режим утечки — не вложенность, а ПЕРЕСЕЧЕНИЕ.

    Streamlit может начать вторую загрузку, пока первая жива, и выйти они могут
    в любом порядке. Если второй экземпляр сохранит уже изменённое состояние, то
    после выхода первого он вернёт логгер не в исходное, а в DEBUG с
    propagate=False — навсегда, на весь процесс.
    """
    logger = logging.getLogger(DRIVER_LOGGER_NAME)
    level_before = logger.level
    propagate_before = logger.propagate

    first = DriverRetryCounter()
    second = DriverRetryCounter()
    try:
        first.__enter__()
        second.__enter__()
        first.__exit__(None, None, None)
    finally:
        second.__exit__(None, None, None)

    assert logger.level == level_before
    assert logger.propagate == propagate_before
    assert not any(isinstance(handler, DriverRetryCounter) for handler in logger.handlers)


def test_overlapping_counters_do_not_duplicate_forwarded_warnings(
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger = logging.getLogger(DRIVER_LOGGER_NAME)

    with caplog.at_level(logging.WARNING):
        with DriverRetryCounter():
            with DriverRetryCounter():
                logger.warning("driver could not reuse the connection")

    assert caplog.text.count("driver could not reuse the connection") == 1


def test_write_run_record_persists_knob_positions_and_stats(tmp_path: Path) -> None:
    csv_path = tmp_path / "source.csv"
    csv_path.write_text("ID\n1\n", encoding="utf_8")
    stats = LoadStats(
        rows=42,
        blocks=3,
        insert_wall_s=1.5,
        server_ns=500_000_000,
        preflight_s=0.5,
        connect_s=0.25,
        ddl_s=2.0,
        total_s=12.5,
        driver_retries=2,
        arrow_bytes_at_start=1024 * 1024,
        arrow_bytes=7 * 1024 * 1024,
    )

    record_path = write_run_record(
        config=make_run_config(),
        stats=stats,
        csv_path=csv_path,
        outcome="ok",
        timestamp=RUN_TIMESTAMP,
        directory=tmp_path / "runs",
    )

    assert record_path.name == "20260726T143012Z-orders.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["outcome"] == "ok"
    assert record["error"] is None
    assert record["config"]["batch_size"] == 100_000
    assert record["config"]["load_workers"] == 4
    assert record["config"]["partition_by"] == "toYYYYMM(dt)"
    assert record["config"]["schema_inference_mode"] == "Fast sample, 100000 rows"
    assert record["stats"]["rows"] == 42
    assert record["stats"]["driver_retries"] == 2
    assert record["stats"]["ddl_s"] == 2.0
    assert record["stats"]["total_s"] == 12.5
    assert record["stats"]["arrow_bytes"] == 7 * 1024 * 1024
    assert record["stats"]["arrow_bytes_at_start"] == 1024 * 1024
    assert record["source"]["size_bytes"] == csv_path.stat().st_size
    assert record["finished_at"] == RUN_TIMESTAMP.isoformat()
    assert record["platform"]
    # Версии всех библиотек, которые влияют на скорость загрузки: без них два
    # прогона сравнивать нельзя, а «not installed» здесь означал бы, что запись
    # сделана в чужом окружении.
    for package in ("pandas", "clickhouse-connect", "urllib3", "streamlit", "numpy"):
        assert record["libraries"][package] not in (None, "", "not installed"), package


def test_write_run_record_keeps_failure_details_and_survives_missing_source(tmp_path: Path) -> None:
    record_path = write_run_record(
        config=make_run_config(),
        stats=LoadStats(rows=7, blocks=1),
        csv_path=tmp_path / "gone.csv",
        outcome="failed",
        error="read limit is reached",
        timestamp=RUN_TIMESTAMP,
        directory=tmp_path / "runs",
    )

    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["outcome"] == "failed"
    assert record["error"] == "read limit is reached"
    assert record["stats"]["rows"] == 7
    assert record["source"]["size_bytes"] is None
    # Ошибка stat попадает в запись значением поля, а не исчезает.
    assert "stat_error" in record["source"]
    assert record["source"]["stat_error"]


def test_write_run_record_sanitizes_the_table_name_used_for_the_file(tmp_path: Path) -> None:
    config = RunConfig(**{**make_run_config().__dict__, "table": "orders 2024/q1"})

    record_path = write_run_record(
        config=config,
        stats=LoadStats(),
        csv_path=tmp_path / "gone.csv",
        outcome="failed",
        timestamp=RUN_TIMESTAMP,
        directory=tmp_path / "runs",
    )

    assert record_path.name == "20260726T143012Z-orders_2024_q1.json"


def test_two_records_in_the_same_second_do_not_overwrite_each_other(tmp_path: Path) -> None:
    """Имя состоит из секунды и таблицы, а быстрое падение до вставки укладывается
    в ту же секунду, что и предыдущее. Потерять запись о падении дороже суффикса."""
    arguments = {
        "config": make_run_config(),
        "stats": LoadStats(),
        "csv_path": tmp_path / "gone.csv",
        "outcome": "failed",
        "timestamp": RUN_TIMESTAMP,
        "directory": tmp_path / "runs",
    }

    first = write_run_record(**arguments)
    second = write_run_record(**arguments)

    assert first != second
    assert first.exists() and second.exists()
    assert second.name == "20260726T143012Z-orders-2.json"


def test_describe_connection_path_records_the_socket_addresses(tmp_path: Path) -> None:
    class FakeSocket:
        def fileno(self) -> int:
            return 7

        def getsockname(self):
            return ("192.0.2.220", 51515)

        def getpeername(self):
            return ("10.1.2.3", 443)

    connection = SimpleNamespace(sock=FakeSocket())
    pool = SimpleNamespace(pool=SimpleNamespace(queue=[connection]))
    client = SimpleNamespace(http=SimpleNamespace(pools={"key": pool}))

    path = describe_connection_path(client)

    # Сети, а не адреса: запись о прогоне уходит в issue, и ни адрес машины, ни
    # адрес узла кластера туда попадать не должны.
    # 192.0.2.0/24 — TEST-NET-1 из RFC 5737, ipaddress относит его к
    # специальным диапазонам, поэтому is_private здесь True.
    assert path == {
        "local_network": "192.0.0.0/16",
        "local_is_private": True,
        "remote_network": "10.1.0.0/16",
        "remote_is_private": True,
    }
    assert "192.0.2.220" not in str(path)
    assert "10.1.2.3" not in str(path)


def test_describe_connection_path_records_the_failure_instead_of_raising() -> None:
    """Копание во внутренностях urllib3 не имеет права уронить загрузку, но и
    исчезнуть молча не должно."""
    broken = SimpleNamespace(http=SimpleNamespace(pools=None))
    assert "unavailable" in describe_connection_path(broken)

    class ExplodingPools:
        def keys(self):
            raise RuntimeError("urllib3 internals changed")

    exploding = SimpleNamespace(http=SimpleNamespace(pools=ExplodingPools()))
    result = describe_connection_path(exploding)
    assert "RuntimeError" in str(result["unavailable"])
    assert "urllib3 internals changed" in str(result["unavailable"])


def test_first_pooled_socket_takes_the_connection_lifo_will_hand_out_next() -> None:
    """urllib3 держит пул в LifoQueue: следующий запрос получит ПОСЛЕДНИЙ элемент
    списка. Взяв первый, зонд следил бы за простаивающим соединением."""

    def connection(name: str):
        return SimpleNamespace(
            sock=SimpleNamespace(fileno=lambda: 7, getsockname=lambda: (name, 1), getpeername=lambda: (name, 2))
        )

    pool = SimpleNamespace(pool=SimpleNamespace(queue=[connection("10.0.0.1"), connection("172.31.0.1")]))
    client = SimpleNamespace(http=SimpleNamespace(pools={"key": pool}))

    assert describe_connection_path(client)["local_network"] == "172.31.0.0/16"
