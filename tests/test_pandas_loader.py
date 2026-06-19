import json
from pathlib import Path

import pandas as pd
import pytest

from csv_click.errors import CsvSchemaError
from csv_click.pandas_loader import (
    ReadOptions,
    SchemaMapping,
    analyze_csv_with_pandas_chunks,
    chunk_to_json_each_row_payload,
    convert_chunk_to_schema,
    iter_pandas_chunks,
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
    assert options.batch_size == 1_000_000


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
    assert preview.iloc[0].to_dict() == {"ID": 1, "NAME": "тест"}


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

    rows = load_csv_via_raw_insert(
        client=client,
        csv_path=csv_path,
        read_options=ReadOptions(batch_size=2),
        database="sandbox",
        table="target_table",
        mappings=mappings,
    )

    assert rows == 3
    assert len(client.calls) == 2
    assert client.calls[0]["table"] == "sandbox.target_table"
    assert client.calls[0]["column_names"] == ["ID", "VALUE"]
    assert client.calls[0]["fmt"] == "JSONEachRow"


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
