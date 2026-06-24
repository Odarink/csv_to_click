import json
from pathlib import Path

import pandas as pd
import pytest

from csv_click.errors import CsvLoadError, CsvReadCancelled, CsvSchemaError
from csv_click.pandas_loader import (
    ENCODING_SUGGESTIONS,
    DEFAULT_MAX_INSERT_PAYLOAD_BYTES,
    ReadOptions,
    SchemaMapping,
    analyze_csv_with_pandas_sample,
    analyze_csv_with_pandas_chunks,
    choose_read_options_for_preview,
    chunk_to_json_each_row_payload,
    convert_chunk_to_schema,
    detect_mojibake,
    iter_pandas_chunks,
    iter_json_each_row_payloads,
    load_csv_via_raw_insert,
    mappings_from_editor_rows,
    mappings_to_schema,
    preview_csv_rows,
)


class FakeRawClient:
    def __init__(self) -> None:
        self.calls = []

    def raw_insert(self, **kwargs):
        self.calls.append(kwargs)


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
    assert chunks[0].iloc[0].to_dict() == {"ID": 1, "NAME": "тест"}


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
    assert converted["C_SPOSOB_KVIT_0"].map(lambda value: type(value).__name__).tolist() == [
        "int",
        "NoneType",
    ]


def test_chunk_to_json_each_row_payload_has_no_nan() -> None:
    chunk = pd.DataFrame({"ID": [1], "VALUE": [None]})

    payload = chunk_to_json_each_row_payload(chunk, ["ID", "VALUE"])

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
    first_row_payload = chunk_to_json_each_row_payload(chunk.iloc[:1].reset_index(drop=True), ["ID", "VALUE"])
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

    rows = load_csv_via_raw_insert(
        client=client,
        csv_path=csv_path,
        read_options=ReadOptions(batch_size=2),
        database="sandbox",
        table="target_table",
        mappings=mappings,
        progress_callback=lambda *args: progress_events.append(args),
    )

    assert rows == 3
    assert len(client.calls) == 2
    assert client.calls[0]["table"] == "sandbox.target_table"
    assert client.calls[0]["column_names"] == ["ID", "VALUE"]
    assert client.calls[0]["fmt"] == "JSONEachRow"
    assert progress_events[0] == (1, 1, 2, 2, len(client.calls[0]["insert_block"]))


def test_load_csv_via_raw_insert_splits_one_chunk_into_bounded_payloads(tmp_path: Path) -> None:
    csv_path = tmp_path / "load.csv"
    csv_path.write_text("ID,VALUE\n1,a\n2,b\n3,c\n4,d\n", encoding="utf_8")
    mappings = [
        SchemaMapping("ID", "ID", True, "UInt64", False),
        SchemaMapping("VALUE", "VALUE", True, "String", False),
    ]
    client = FakeRawClient()
    progress_events = []

    rows = load_csv_via_raw_insert(
        client=client,
        csv_path=csv_path,
        read_options=ReadOptions(batch_size=4),
        database="sandbox",
        table="target_table",
        mappings=mappings,
        max_insert_payload_bytes=30,
        progress_callback=lambda *args: progress_events.append(args),
    )

    assert rows == 4
    assert len(client.calls) == 4
    assert all(len(call["insert_block"]) <= 30 for call in client.calls)
    assert [event[:4] for event in progress_events] == [
        (1, 1, 1, 1),
        (1, 2, 1, 2),
        (1, 3, 1, 3),
        (1, 4, 1, 4),
    ]
    assert all(event[4] <= 30 for event in progress_events)


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

    with pytest.raises(CsvLoadError, match="Max insert payload"):
        load_csv_via_raw_insert(
            client=FailingRawClient(),
            csv_path=csv_path,
            read_options=ReadOptions(batch_size=1),
            database="sandbox",
            table="target_table",
            mappings=mappings,
        )


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
