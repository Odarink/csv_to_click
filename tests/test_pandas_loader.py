import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pandas as pd
import pytest

from csv_click.errors import CsvLoadError, CsvReadCancelled, CsvSchemaError
from csv_click.load_stats import LoadStats
from csv_click.pandas_loader import (
    ENCODING_SUGGESTIONS,
    DEFAULT_MAX_INSERT_PAYLOAD_BYTES,
    ReadOptions,
    SchemaMapping,
    analyze_csv_with_pandas_sample,
    analyze_csv_with_pandas_chunks,
    choose_read_options_for_preview,
    chunk_to_json_lines,
    convert_chunk_to_schema,
    detect_mojibake,
    iter_pandas_chunks,
    iter_json_each_row_payloads,
    load_csv_via_raw_insert,
    mappings_from_editor_rows,
    mappings_to_schema,
    preview_csv_rows,
    validate_csv_sample_with_pandas_chunks,
)


BLOCK_SERVER_NS = 1_000_000


class FakeRawClient:
    def __init__(self) -> None:
        self.calls = []

    def raw_insert(self, **kwargs):
        self.calls.append(kwargs)
        return SimpleNamespace(summary={"elapsed_ns": str(BLOCK_SERVER_NS)})


class SlowRawClient(FakeRawClient):
    def raw_insert(self, **kwargs):
        time.sleep(0.02)
        return super().raw_insert(**kwargs)


class StrippedSummaryRawClient(FakeRawClient):
    """Прокси срезал X-ClickHouse-Summary.

    Драйвер при этом возвращает НЕ пустую сводку, а сводку с одним query_id:
    httpclient.py:444 дописывает его безусловно. Фейк обязан повторять именно
    эту форму, иначе тест зеленит проверку, которой в бою не существует.
    """

    def raw_insert(self, **kwargs):
        super().raw_insert(**kwargs)
        return SimpleNamespace(summary={"query_id": "01234567-89ab-cdef"})


def test_read_options_defaults_to_utf8_and_comma() -> None:
    options = ReadOptions()

    assert options.encoding == "utf_8"
    assert options.separator == ","
    assert options.batch_size == 100_000


def test_iter_pandas_chunks_uses_selected_separator_and_encoding(tmp_path: Path) -> None:
    csv_path = tmp_path / "cp1251.csv"
    csv_path.write_text("ID;NAME\n1;тест\n", encoding="cp1251")
    options = ReadOptions(separator=";", encoding="cp1251", batch_size=1)

    chunks = list(iter_pandas_chunks(csv_path, options))

    assert len(chunks) == 1
    assert chunks[0].columns.tolist() == ["ID", "NAME"]
    # Строка, а не число: путь загрузки теперь читает файл ровно так же, как
    # превью и инференс. Раньше `007` здесь становилось числом 7 и уезжало в
    # String-колонку как "7" — см. tests/test_read_consistency.py.
    assert chunks[0].iloc[0].to_dict() == {"ID": "1", "NAME": "тест"}


def test_preview_csv_rows_uses_selected_separator_and_encoding(tmp_path: Path) -> None:
    csv_path = tmp_path / "preview_cp1251.csv"
    csv_path.write_text("ID;NAME\n1;тест\n", encoding="cp1251")
    options = ReadOptions(separator=";", encoding="cp1251", batch_size=1)

    preview = preview_csv_rows(csv_path, options, nrows=20)

    assert preview.columns.tolist() == ["ID", "NAME"]
    assert preview.iloc[0].to_dict() == {"ID": "1", "NAME": "тест"}


def test_preview_csv_rows_keeps_empty_values_as_strings(tmp_path: Path) -> None:
    csv_path = tmp_path / "preview_strings.csv"
    csv_path.write_text("ID;NAME\n001;\n", encoding="utf_8")
    options = ReadOptions(separator=";", encoding="utf_8", batch_size=1)

    preview = preview_csv_rows(csv_path, options, nrows=20)

    assert preview.iloc[0].to_dict() == {"ID": "001", "NAME": ""}


def test_detect_mojibake_suggests_encoding_candidates() -> None:
    preview = pd.DataFrame({"NAME": ["С‚РµСЃС‚"]})

    warning = detect_mojibake(preview)

    assert warning is not None
    assert "mojibake" in warning.message
    assert warning.suggested_encodings == ENCODING_SUGGESTIONS


def test_detect_mojibake_catches_replacement_character_mojibake() -> None:
    preview = pd.DataFrame({"NAME": ["пїЅпїЅпїЅпїЅ"]})

    warning = detect_mojibake(preview)

    assert warning is not None
    assert "mojibake" in warning.message


def test_choose_read_options_prefers_encoding_with_lower_mojibake_score(tmp_path: Path) -> None:
    csv_path = tmp_path / "utf8.csv"
    csv_path.write_text("ID;NAME\n1;тест\n", encoding="utf_8")
    selected_options = ReadOptions(separator=";", encoding="cp1251", batch_size=1)

    effective_options, preview, warning = choose_read_options_for_preview(csv_path, selected_options)

    assert effective_options.encoding == "utf_8"
    assert preview.iloc[0].to_dict() == {"ID": "1", "NAME": "тест"}
    assert warning is not None
    assert "Auto-selected encoding utf_8" in warning.message


def test_choose_read_options_rejects_real_csv_with_replacement_characters() -> None:
    csv_path = Path("tests/test_csv.csv")
    selected_options = ReadOptions(separator=";", encoding="utf_8", batch_size=1)

    with pytest.raises(CsvSchemaError, match="replacement characters"):
        choose_read_options_for_preview(csv_path, selected_options)


def test_analyze_csv_with_pandas_chunks_strips_and_normalizes_target_names(tmp_path: Path) -> None:
    csv_path = tmp_path / "source.csv"
    csv_path.write_text(" ID ,C_SPOSOB_KVIT#0,skip_me\n1,10,x\n", encoding="utf_8")

    schema = analyze_csv_with_pandas_chunks(csv_path, ReadOptions(batch_size=1))

    assert [column.source_name for column in schema.columns] == [
        "ID",
        "C_SPOSOB_KVIT#0",
        "skip_me",
    ]
    assert [column.column_name for column in schema.columns] == [
        "id",
        "c_sposob_kvit_0",
        "skip_me",
    ]


def test_analyze_csv_with_pandas_sample_infers_schema_from_limited_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "source.csv"
    csv_path.write_text("ID,AMOUNT\n1,10\n2,abc\n", encoding="utf_8")

    schema = analyze_csv_with_pandas_sample(
        csv_path,
        ReadOptions(batch_size=1),
        nrows=1,
    )

    amount_column = next(column for column in schema.columns if column.source_name == "AMOUNT")
    assert amount_column.inferred_type == "UInt64"
    assert amount_column.final_type == "UInt64"
    assert amount_column.sample_values == ["10"]


def test_pandas_sample_inference_keeps_leading_zero_identifiers_as_string(tmp_path: Path) -> None:
    """Выборочный инференс не имеет права типизировать счёт числом.

    Тест держит и чтение: если из `preview_csv_rows` уйдёт `dtype=str`, нули
    пропадут ещё до статистики, и колонка снова станет UInt64.
    """
    csv_path = tmp_path / "accounts.csv"
    csv_path.write_text("ACCOUNT,AMOUNT\n00123456789,10\n00987654321,20\n", encoding="utf_8")

    schema = analyze_csv_with_pandas_sample(csv_path, ReadOptions(batch_size=1), nrows=2)

    assert [(column.source_name, column.final_type) for column in schema.columns] == [
        ("ACCOUNT", "String"),
        ("AMOUNT", "UInt64"),
    ]


def test_pandas_chunk_inference_keeps_leading_zero_identifiers_as_string(tmp_path: Path) -> None:
    """Ведущий ноль во ВТОРОМ чанке обязан решать судьбу колонки так же."""
    csv_path = tmp_path / "accounts_full.csv"
    csv_path.write_text("ACCOUNT,AMOUNT\n42,10\n00987654321,20\n", encoding="utf_8")

    schema = analyze_csv_with_pandas_chunks(csv_path, ReadOptions(batch_size=1))

    assert [(column.source_name, column.final_type) for column in schema.columns] == [
        ("ACCOUNT", "String"),
        ("AMOUNT", "UInt64"),
    ]


def test_analyze_csv_with_pandas_chunks_still_uses_full_file(tmp_path: Path) -> None:
    csv_path = tmp_path / "source.csv"
    csv_path.write_text("ID,NAME\n1,Alice\n2,\n", encoding="utf_8")

    schema = analyze_csv_with_pandas_chunks(csv_path, ReadOptions(batch_size=1))

    name_column = next(column for column in schema.columns if column.source_name == "NAME")
    assert name_column.final_type == "Nullable(String)"


def test_analyze_csv_with_pandas_chunks_can_be_cancelled_between_chunks(tmp_path: Path) -> None:
    csv_path = tmp_path / "source.csv"
    csv_path.write_text("ID,NAME\n1,Alice\n2,Bob\n", encoding="utf_8")

    with pytest.raises(CsvReadCancelled, match="CSV read was stopped"):
        analyze_csv_with_pandas_chunks(
            csv_path,
            ReadOptions(batch_size=1),
            cancel_callback=lambda: True,
        )


def test_iter_pandas_chunks_wraps_parser_errors_with_csv_schema_error(tmp_path: Path) -> None:
    csv_path = tmp_path / "bad_delimiter.csv"
    csv_path.write_text('ID,NAME\n1,Alice\n2,"Bob\n', encoding="utf_8")

    with pytest.raises(CsvSchemaError) as exc_info:
        list(iter_pandas_chunks(csv_path, ReadOptions(separator=",", batch_size=1)))

    message = str(exc_info.value)
    assert "Cannot parse CSV" in message
    assert "separator" in message


def test_convert_chunk_supports_include_rename_and_nullable_int() -> None:
    chunk = pd.DataFrame(
        {
            "ID": [1, 2],
            "C_SPOSOB_KVIT#0": [10, None],
            "skip_me": ["x", "y"],
        }
    )
    mappings = [
        SchemaMapping("ID", "ID", True, "Int64", False),
        SchemaMapping("C_SPOSOB_KVIT#0", "C_SPOSOB_KVIT_0", True, "Nullable(Int64)", True),
        SchemaMapping("skip_me", "skip_me", False, "String", False),
    ]

    converted = convert_chunk_to_schema(chunk, mappings, chunk_number=1)

    assert converted.columns.tolist() == ["ID", "C_SPOSOB_KVIT_0"]
    # Проверяются отправляемые байты, а не промежуточные Python-типы: `.map()`
    # по маскированной Int64-серии отдаёт float, и такой пин мерил бы поведение
    # pandas, а не наши данные.
    assert [
        json.loads(line)
        for line in chunk_to_json_lines(converted, ["ID", "C_SPOSOB_KVIT_0"]).decode("utf-8").splitlines()
    ] == [
        {"ID": 1, "C_SPOSOB_KVIT_0": 10},
        {"ID": 2, "C_SPOSOB_KVIT_0": None},
    ]


def test_chunk_to_json_lines_has_no_nan() -> None:
    chunk = pd.DataFrame({"ID": [1], "VALUE": [None]})

    payload = chunk_to_json_lines(chunk, ["ID", "VALUE"])

    decoded = payload.decode("utf-8").strip()
    assert "NaN" not in decoded
    assert json.loads(decoded) == {"ID": 1, "VALUE": None}


def test_json_each_row_payloads_are_split_by_byte_limit() -> None:
    chunk = pd.DataFrame(
        {
            "ID": [1, 2, 3],
            "VALUE": ["a" * 20, "b" * 20, "c" * 20],
        }
    )
    first_row_payload = chunk_to_json_lines(chunk.iloc[:1].reset_index(drop=True), ["ID", "VALUE"])
    max_payload_bytes = len(first_row_payload) + 1

    payloads = list(iter_json_each_row_payloads(chunk, ["ID", "VALUE"], max_payload_bytes=max_payload_bytes))

    assert [rows for _, rows in payloads] == [1, 1, 1]
    assert all(len(payload) <= max_payload_bytes for payload, _ in payloads)
    decoded_rows = [
        json.loads(line)
        for payload, _ in payloads
        for line in payload.decode("utf-8").splitlines()
    ]
    assert [row["ID"] for row in decoded_rows] == [1, 2, 3]


def test_json_each_row_payload_reports_single_row_larger_than_limit() -> None:
    chunk = pd.DataFrame({"ID": [1], "VALUE": ["x" * 100]})

    with pytest.raises(CsvLoadError, match="single JSONEachRow row"):
        list(iter_json_each_row_payloads(chunk, ["ID", "VALUE"], max_payload_bytes=10))


def test_validate_csv_sample_with_pandas_chunks_reads_only_first_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("ID\n1\n2\nbad\n", encoding="utf_8")
    mappings = [SchemaMapping("ID", "ID", True, "UInt64", False)]

    rows = validate_csv_sample_with_pandas_chunks(
        csv_path,
        ReadOptions(batch_size=1),
        mappings,
        sample_rows=2,
    )

    assert rows == 2


def test_validate_csv_sample_with_pandas_chunks_validates_types_and_payload(tmp_path: Path) -> None:
    csv_path = tmp_path / "sample_bad.csv"
    csv_path.write_text("ID,VALUE\n1,xxxxxxxxxxxxxxxxxxxx\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]

    with pytest.raises(CsvLoadError, match="single JSONEachRow row"):
        validate_csv_sample_with_pandas_chunks(
            csv_path,
            ReadOptions(batch_size=1),
            mappings,
            max_insert_payload_bytes=10,
            sample_rows=1,
        )


def test_invalid_manual_type_reports_chunk_column_and_value() -> None:
    chunk = pd.DataFrame({"ID": ["abc"]})
    mappings = [SchemaMapping("ID", "ID", True, "UInt64", False)]

    with pytest.raises(CsvSchemaError) as exc_info:
        convert_chunk_to_schema(chunk, mappings, chunk_number=3)

    message = str(exc_info.value)
    assert "chunk 3" in message
    assert "ID" in message
    assert "abc" in message


def test_load_csv_via_raw_insert_uses_json_each_row_chunks(tmp_path: Path) -> None:
    csv_path = tmp_path / "load.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]
    client = FakeRawClient()
    progress_events = []

    stats = load_csv_via_raw_insert(
        client=client,
        csv_path=csv_path,
        read_options=ReadOptions(batch_size=2),
        database="sandbox",
        table="target_table",
        mappings=mappings,
        progress_callback=progress_events.append,
    )

    assert stats.rows == 3
    assert stats.blocks == 2
    assert len(client.calls) == 2
    assert client.calls[0]["table"] == "sandbox.target_table"
    assert client.calls[0]["column_names"] == ["ID", "VALUE"]
    assert client.calls[0]["fmt"] == "JSONEachRow"
    first = progress_events[0]
    assert (first.chunk_number, first.block_number, first.block_rows, first.rows_total) == (1, 1, 2, 2)
    assert first.raw_bytes == len(client.calls[0]["insert_block"])
    assert first.wire_bytes == first.raw_bytes


def test_load_csv_via_raw_insert_splits_one_chunk_into_bounded_payloads(tmp_path: Path) -> None:
    csv_path = tmp_path / "load.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n4,d\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]
    client = FakeRawClient()
    progress_events = []

    stats = load_csv_via_raw_insert(
        client=client,
        csv_path=csv_path,
        read_options=ReadOptions(batch_size=4),
        database="sandbox",
        table="target_table",
        mappings=mappings,
        max_insert_payload_bytes=30,
        progress_callback=progress_events.append,
    )

    assert stats.rows == 4
    assert len(client.calls) == 4
    assert all(len(call["insert_block"]) <= 30 for call in client.calls)
    assert [
        (event.chunk_number, event.block_number, event.block_rows, event.rows_total)
        for event in progress_events
    ] == [
        (1, 1, 1, 1),
        (1, 2, 1, 2),
        (1, 3, 1, 3),
        (1, 4, 1, 4),
    ]
    assert all(event.wire_bytes <= 30 for event in progress_events)


def test_load_csv_via_raw_insert_parallel_uses_client_factory(tmp_path: Path) -> None:
    csv_path = tmp_path / "parallel.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n4,d\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]
    shared_client = FakeRawClient()
    worker_clients: list[FakeRawClient] = []
    progress_events = []

    def client_factory() -> FakeRawClient:
        client = SlowRawClient()
        worker_clients.append(client)
        return client

    stats = load_csv_via_raw_insert(
        client=shared_client,
        csv_path=csv_path,
        read_options=ReadOptions(batch_size=1),
        database="sandbox",
        table="target_table",
        mappings=mappings,
        worker_count=3,
        client_factory=client_factory,
        progress_callback=progress_events.append,
    )

    worker_calls = [call for worker_client in worker_clients for call in worker_client.calls]
    assert stats.rows == 4
    assert stats.blocks == 4
    assert stats.server_ns == 4 * BLOCK_SERVER_NS
    assert shared_client.calls == []
    assert len(worker_calls) == 4
    assert len(worker_clients) > 1
    assert sorted(event.rows_total for event in progress_events) == [1, 2, 3, 4]
    assert stats.source_fully_read is True
    assert stats.blocks_unconfirmed == 0


def test_load_csv_via_raw_insert_parallel_requires_client_factory(tmp_path: Path) -> None:
    csv_path = tmp_path / "parallel.csv"
    csv_path.write_text("ID\n1\n", encoding="utf_8")
    mappings = [SchemaMapping("ID", "ID", True, "UInt64", False)]

    with pytest.raises(ValueError, match="client_factory is required"):
        load_csv_via_raw_insert(
            client=FakeRawClient(),
            csv_path=csv_path,
            read_options=ReadOptions(batch_size=1),
            database="sandbox",
            table="target_table",
            mappings=mappings,
            worker_count=2,
        )


def test_load_csv_via_raw_insert_parallel_wraps_insert_error_with_context(tmp_path: Path) -> None:
    csv_path = tmp_path / "parallel_bad.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]

    class FailingRawClient(FakeRawClient):
        def raw_insert(self, **kwargs):
            if b'"ID":2' in kwargs["insert_block"]:
                raise RuntimeError("HTTP status 500: the read limit is reached")
            super().raw_insert(**kwargs)

    with pytest.raises(CsvLoadError) as exc_info:
        load_csv_via_raw_insert(
            client=FakeRawClient(),
            csv_path=csv_path,
            read_options=ReadOptions(batch_size=1),
            database="sandbox",
            table="target_table",
            mappings=mappings,
            worker_count=2,
            client_factory=FailingRawClient,
        )

    message = str(exc_info.value)
    assert "chunk 2" in message
    assert "block 1" in message
    assert "HTTP/proxy read limit" in message


def test_load_csv_via_raw_insert_uses_default_payload_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    csv_path = tmp_path / "load.csv"
    csv_path.write_text("ID,VALUE\n1,a\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]
    observed_limits = []

    def fake_payloads(chunk, columns, max_payload_bytes):
        observed_limits.append(max_payload_bytes)
        yield b'{"ID":1,"VALUE":"a"}', 1

    monkeypatch.setattr("csv_click.pandas_loader.iter_json_each_row_payloads", fake_payloads)

    load_csv_via_raw_insert(
        client=FakeRawClient(),
        csv_path=csv_path,
        read_options=ReadOptions(batch_size=1),
        database="sandbox",
        table="target_table",
        mappings=mappings,
    )

    assert observed_limits == [DEFAULT_MAX_INSERT_PAYLOAD_BYTES]


def test_load_csv_via_raw_insert_wraps_read_limit_error_with_payload_context(tmp_path: Path) -> None:
    csv_path = tmp_path / "load.csv"
    csv_path.write_text("ID,VALUE\n1,a\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]

    class FailingRawClient(FakeRawClient):
        def raw_insert(self, **kwargs):
            raise RuntimeError("HTTP status 500: the read limit is reached")

    with pytest.raises(CsvLoadError, match="HTTP/proxy read limit"):
        load_csv_via_raw_insert(
            client=FailingRawClient(),
            csv_path=csv_path,
            read_options=ReadOptions(batch_size=1),
            database="sandbox",
            table="target_table",
            mappings=mappings,
        )


def test_load_csv_via_raw_insert_records_server_time_and_stage_clocks(tmp_path: Path) -> None:
    csv_path = tmp_path / "load.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]

    stats = load_csv_via_raw_insert(
        client=FakeRawClient(),
        csv_path=csv_path,
        read_options=ReadOptions(batch_size=2),
        database="sandbox",
        table="target_table",
        mappings=mappings,
    )

    assert stats.blocks == 2
    assert stats.server_ns == 2 * BLOCK_SERVER_NS
    assert stats.blocks_without_server_time == 0
    assert stats.worker_count == 1
    assert stats.raw_bytes > 0
    assert stats.wire_bytes == stats.raw_bytes
    assert stats.read_s > 0
    assert stats.convert_s > 0
    assert stats.serialize_s > 0


def test_load_csv_via_raw_insert_counts_blocks_with_a_stripped_summary_header(tmp_path: Path) -> None:
    csv_path = tmp_path / "load.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]

    stats = load_csv_via_raw_insert(
        client=StrippedSummaryRawClient(),
        csv_path=csv_path,
        read_options=ReadOptions(batch_size=2),
        database="sandbox",
        table="target_table",
        mappings=mappings,
    )
    stats.insert_wall_s = 5.0

    assert stats.blocks == 2
    assert stats.blocks_without_server_time == 2
    assert stats.server_ns == 0
    assert stats.server_share is None


def test_load_csv_via_raw_insert_keeps_partial_stats_of_a_failed_load(tmp_path: Path) -> None:
    csv_path = tmp_path / "load.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]
    stats = LoadStats()

    class FailingOnSecondBlock(FakeRawClient):
        def raw_insert(self, **kwargs):
            if b'"ID":2' in kwargs["insert_block"]:
                raise RuntimeError("HTTP status 500: the read limit is reached")
            return super().raw_insert(**kwargs)

    with pytest.raises(CsvLoadError):
        load_csv_via_raw_insert(
            client=FailingOnSecondBlock(),
            csv_path=csv_path,
            read_options=ReadOptions(batch_size=1),
            database="sandbox",
            table="target_table",
            mappings=mappings,
            stats=stats,
        )

    assert stats.rows == 1
    assert stats.blocks == 1
    assert stats.server_ns == BLOCK_SERVER_NS


def test_parallel_load_accumulates_correctly_when_blocks_finish_out_of_order(tmp_path: Path) -> None:
    """Блоки завершаются в любом порядке, а накопление идёт в порядке завершения
    и в одном потоке. Здесь порядок завершения принудительно обратный порядку
    отправки — итог от этого зависеть не должен."""
    csv_path = tmp_path / "reverse.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n4,d\n5,e\n6,f\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]
    order_lock = threading.Lock()
    submitted: list[bytes] = []
    completed: list[bytes] = []

    class ReverseOrderClient(FakeRawClient):
        def raw_insert(self, **kwargs):
            with order_lock:
                index = len(submitted)
                submitted.append(kwargs["insert_block"])
            time.sleep(0.30 - index * 0.04)
            with order_lock:
                completed.append(kwargs["insert_block"])
            return super().raw_insert(**kwargs)

    events: list = []
    stats = load_csv_via_raw_insert(
        client=FakeRawClient(),
        csv_path=csv_path,
        read_options=ReadOptions(batch_size=1),
        database="sandbox",
        table="target_table",
        mappings=mappings,
        worker_count=6,
        client_factory=ReverseOrderClient,
        progress_callback=events.append,
    )

    assert completed != submitted, "порядок завершения не отличался — тест ничего не проверил"
    assert stats.rows == 6
    assert stats.blocks == 6
    assert stats.server_ns == 6 * BLOCK_SERVER_NS
    assert stats.worker_count == 6
    assert sorted(event.rows_total for event in events) == [1, 2, 3, 4, 5, 6]
    # Сумма серверных времён по одновременным запросам долей стенных часов не является.
    stats.insert_wall_s = 1.0
    assert stats.server_share is None


def test_failed_parallel_load_still_counts_blocks_the_server_already_accepted(tmp_path: Path) -> None:
    """При сбое остальные задачи гасятся, но те, что успели дойти до сервера,
    уже лежат в ClickHouse. Если их не засчитать, запись о падении покажет
    меньше строк, чем реально загружено, и фаза 4 будет считать по заниженным."""
    csv_path = tmp_path / "partial.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n4,d\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]
    stats = LoadStats()
    # Барьер вместо sleep: он ГАРАНТИРУЕТ, что к моменту падения все четыре
    # блока уже внутри raw_insert. Со sleep тест проверял бы скорость запуска
    # потоков пула — на загруженной машине часть задач ещё не стартовала бы,
    # cancel_pending успешно их отменял, и счёт падал ниже трёх при полностью
    # исправном коде.
    all_blocks_in_flight = threading.Barrier(4, timeout=10)

    class FailsOnceEveryBlockIsInFlight(FakeRawClient):
        def raw_insert(self, **kwargs):
            all_blocks_in_flight.wait()
            if b'"ID":1' in kwargs["insert_block"]:
                raise RuntimeError("HTTP status 500: the read limit is reached")
            # Пауза ПОСЛЕ барьера: гонки со стартом потоков она уже не создаёт,
            # но задаёт порядок — упавший блок завершается первым, поэтому все
            # успешные достижимы только через cancel_pending.
            time.sleep(0.2)
            return super().raw_insert(**kwargs)

    events: list = []

    with pytest.raises(CsvLoadError):
        load_csv_via_raw_insert(
            client=FakeRawClient(),
            csv_path=csv_path,
            read_options=ReadOptions(batch_size=1),
            database="sandbox",
            table="target_table",
            mappings=mappings,
            worker_count=4,
            client_factory=FailsOnceEveryBlockIsInFlight,
            progress_callback=events.append,
            stats=stats,
        )

    assert stats.blocks == 3, "успешные блоки потеряны при отмене оставшихся задач"
    assert stats.rows == 3
    assert stats.server_ns == 3 * BLOCK_SERVER_NS
    assert stats.blocks_unconfirmed >= 1, "упавший блок обязан быть посчитан неподтверждённым"
    # Засчитаны, но НЕ отправлены в progress_callback: загрузка уже падает, а
    # callback ходит в Streamlit и может подменить исходную ошибку своей.
    assert events == []


def test_a_streamlit_rerun_mid_load_still_counts_what_the_server_accepted(tmp_path: Path) -> None:
    """RerunException наследуется от BaseException и может прилететь из любого
    st.*-вызова внутри progress_callback. `except Exception` её не ловил, отмена
    не срабатывала, но пул всё равно дожидался отправленных блоков — сервер их
    принимал, а запись о прогоне их теряла."""
    csv_path = tmp_path / "rerun.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n4,d\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]
    stats = LoadStats()
    accepted = []
    accepted_lock = threading.Lock()
    all_blocks_in_flight = threading.Barrier(4, timeout=10)

    class RerunException(BaseException):
        pass

    class CountingClient(FakeRawClient):
        def raw_insert(self, **kwargs):
            all_blocks_in_flight.wait()
            with accepted_lock:
                accepted.append(kwargs["insert_block"])
            return super().raw_insert(**kwargs)

    def rerun_on_first_progress(block) -> None:
        raise RerunException("streamlit rerun")

    with pytest.raises(RerunException):
        load_csv_via_raw_insert(
            client=FakeRawClient(),
            csv_path=csv_path,
            read_options=ReadOptions(batch_size=1),
            database="sandbox",
            table="target_table",
            mappings=mappings,
            worker_count=4,
            client_factory=CountingClient,
            progress_callback=rerun_on_first_progress,
            stats=stats,
        )

    assert len(accepted) == 4, "фейковый сервер должен был принять все четыре блока"
    assert stats.blocks == len(accepted), (
        f"сервер принял {len(accepted)} блоков, а записано {stats.blocks}"
    )
    assert stats.rows == 4
    # Файл из четырёх строк успел прочитаться целиком до первого callback: все
    # блоки были отданы воркерам, умер только отчёт.
    assert stats.source_fully_read is True


def test_a_rerun_before_the_producer_finished_says_the_source_is_not_fully_read(tmp_path: Path) -> None:
    """Прерывание НА СЕРЕДИНЕ файла: в таблице останется префикс.

    Этот путь — с воркерами — тот, которым грузит оператор, и именно он должен
    честно сказать, что дочитать файл не успели. 10 блоков против max_pending=4
    гарантируют, что callback дёрнется, пока итератор чанков ещё не исчерпан.

    Отменённые блоки обязаны быть посчитаны: сервер их не видел, и без счётчика
    запись утверждала бы, что отправлено всё прочитанное.
    """
    csv_path = tmp_path / "rerun_midfile.csv"
    csv_path.write_text("ID\n" + "".join(f"{index}\n" for index in range(10)), encoding="utf_8")
    mappings = [SchemaMapping("ID", "ID", True, "UInt64", False)]
    stats = LoadStats()

    class RerunException(BaseException):
        pass

    def rerun_on_first_progress(block) -> None:
        raise RerunException("streamlit rerun")

    with pytest.raises(RerunException):
        load_csv_via_raw_insert(
            client=FakeRawClient(),
            csv_path=csv_path,
            read_options=ReadOptions(batch_size=1),
            database="sandbox",
            table="target_table",
            mappings=mappings,
            worker_count=2,
            client_factory=FakeRawClient,
            progress_callback=rerun_on_first_progress,
            stats=stats,
        )

    assert stats.source_fully_read is False
    assert stats.rows < 10, "префикс файла, а не весь файл"
    assert stats.blocks_unconfirmed > 0, "отменённые блоки не посчитаны"


def test_mappings_from_editor_rows_prefers_custom_type_override() -> None:
    rows = [
        {
            "source_name": "NAME",
            "target_name": "name",
            "include": True,
            "inferred_type": "String",
            "final_type": "String",
            "custom_type": "LowCardinality(String)",
            "nullable": False,
            "sample_values": "alice",
            "notes": "",
        }
    ]

    mappings = mappings_from_editor_rows(rows)
    schema = mappings_to_schema(mappings)

    assert mappings[0].final_type == "LowCardinality(String)"
    assert schema.columns[0].final_type == "LowCardinality(String)"


def test_convert_chunk_passes_custom_type_values_as_strings() -> None:
    chunk = pd.DataFrame({"NAME": ["alice", "bob"]})
    mappings = [
        SchemaMapping(
            source_name="NAME",
            target_name="name",
            include=True,
            final_type="LowCardinality(String)",
            nullable=False,
        )
    ]

    converted = convert_chunk_to_schema(chunk, mappings, chunk_number=1)

    assert converted["name"].tolist() == ["alice", "bob"]
