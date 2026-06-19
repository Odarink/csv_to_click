from pathlib import Path

import pytest

from csv_click.schema import (
    CLICKHOUSE_TYPE_OPTIONS,
    CsvSchemaError,
    analyze_csv_schema,
    normalize_identifier,
    validate_csv_against_schema,
)


def write_csv(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_analyze_csv_schema_requires_header(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "empty.csv", "")

    with pytest.raises(CsvSchemaError, match="header"):
        analyze_csv_schema(csv_path)


def test_analyze_csv_schema_infers_types_from_full_file(tmp_path: Path) -> None:
    csv_path = write_csv(
        tmp_path / "sample.csv",
        "\n".join(
            [
                "id,amount,created_dt,flag,comment",
                "1,10.50,2026-06-18,true,ok",
                "2,0.01,2026-06-19,false,",
                "3,999999999999.12,2026-06-20,true,late value",
            ]
        ),
    )

    result = analyze_csv_schema(csv_path)

    assert [column.column_name for column in result.columns] == [
        "id",
        "amount",
        "created_dt",
        "flag",
        "comment",
    ]
    assert [column.final_type for column in result.columns] == [
        "UInt64",
        "Decimal(18, 2)",
        "Date",
        "Bool",
        "Nullable(String)",
    ]


def test_mixed_column_falls_back_to_string_with_note(tmp_path: Path) -> None:
    csv_path = write_csv(
        tmp_path / "mixed.csv",
        "id,value\n1,10\n2,abc\n",
    )

    result = analyze_csv_schema(csv_path)

    value_column = result.columns[1]
    assert value_column.final_type == "String"
    assert "fallback" in value_column.notes.lower()


def test_validate_csv_against_manual_type_reports_row_column_and_value(tmp_path: Path) -> None:
    csv_path = write_csv(
        tmp_path / "bad_manual_type.csv",
        "id,value\n1,10\n2,abc\n",
    )
    schema = analyze_csv_schema(csv_path)
    schema.columns[1].final_type = "UInt64"

    with pytest.raises(CsvSchemaError) as exc_info:
        validate_csv_against_schema(csv_path, schema)

    message = str(exc_info.value)
    assert "row 3" in message
    assert "value" in message
    assert "abc" in message


def test_validate_csv_against_schema_returns_row_count(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "valid.csv", "id,name\n1,Alice\n2,Bob\n")
    schema = analyze_csv_schema(csv_path)

    assert validate_csv_against_schema(csv_path, schema) == 2


def test_schema_from_editor_rows_applies_manual_nullable_type(tmp_path: Path) -> None:
    from csv_click.schema import schema_from_editor_rows, schema_to_editor_rows

    csv_path = write_csv(tmp_path / "manual.csv", "id,value\n1,10\n2,\n")
    schema = analyze_csv_schema(csv_path)
    rows = schema_to_editor_rows(schema)
    rows[1]["final_type"] = "Nullable(String)"

    edited_schema = schema_from_editor_rows(rows)

    assert edited_schema.columns[1].final_type == "Nullable(String)"
    assert validate_csv_against_schema(csv_path, edited_schema) == 2


def test_numeric_zero_one_column_infers_uint_not_bool(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "numeric_flags.csv", "id,flag\n1,0\n2,1\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[1].final_type == "UInt64"


def test_clickhouse_type_options_include_nullable_dropdown_values() -> None:
    assert "String" in CLICKHOUSE_TYPE_OPTIONS
    assert "Nullable(Decimal(38, 10))" in CLICKHOUSE_TYPE_OPTIONS


def test_normalize_identifier_rejects_duplicate_columns_after_normalization() -> None:
    assert normalize_identifier("Order ID") == "order_id"
    assert normalize_identifier("123") == "col_123"
