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


@pytest.mark.parametrize(
    ("values", "want_type"),
    [
        # Разбор съедает ведущий ноль и ведущий плюс, а текст обратно не вернуть.
        # В банковской выгрузке это счета, БИК, ИНН, КПП, телефоны и индексы.
        (["00123456789", "00987654321"], "String"),
        (["044525225", "045004641"], "String"),
        (["+79001234567", "+79001234568"], "String"),
        (["007", "42"], "String"),
        (["00.5", "1.25"], "String"),
        (["-007", "-42"], "String"),
        # Граница длины: двузначные коды с нулём впереди - месяцы, регионы,
        # коды операций. `01` уехало бы единицей.
        (["01", "07"], "String"),
        # Не-ASCII цифры разбор перепишет целиком, а не только префикс:
        # `١٢٣٤` уедет как `1234`.
        (["٠١٢٣", "١٢٣٤"], "String"),
        (["１２３", "４５６"], "String"),
        # Числа, из которых разбор ничего не выкусывает, обязаны остаться числами.
        (["0", "1", "2"], "UInt64"),
        (["-7", "42"], "Int64"),
        (["0.5", "1.25"], "Decimal(18, 2)"),
        (["1e5", "2e5"], "Decimal(18, 2)"),
        (["2024-01-05", "2024-02-06"], "Date"),
        # Год с ведущими нулями - сентинел `DateTime.MinValue` из выгрузок .NET.
        # Разбор даты ничего не теряет, и она обязана остаться датой.
        (["0001-01-01", "0999-12-31"], "Date"),
        (["true", "false"], "Bool"),
    ],
)
def test_numeric_inference_refuses_types_that_would_eat_a_leading_zero_or_plus(
    tmp_path: Path, values: list[str], want_type: str
) -> None:
    csv_path = write_csv(tmp_path / "column.csv", "code\n" + "".join(f"{value}\n" for value in values))

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == want_type


def test_leading_zero_column_says_in_notes_why_it_stayed_string(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "accounts.csv", "account\n00123456789\n00987654321\n")

    schema = analyze_csv_schema(csv_path)

    notes = schema.columns[0].notes
    assert "zero" in notes.lower(), notes


def test_mixed_column_with_a_leading_zero_still_blames_the_mix(tmp_path: Path) -> None:
    """Колонка не числовая вовсе, и объяснение обязано быть про смесь.

    Пометка про ведущие нули здесь послала бы оператора искать не ту причину.
    """
    csv_path = write_csv(tmp_path / "mixed_zero.csv", "code\n007\nabc\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "String"
    assert "mixed" in schema.columns[0].notes.lower(), schema.columns[0].notes


def test_leading_zero_column_with_empty_cells_becomes_nullable_string(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "accounts_nullable.csv", "account,name\n00123456789,a\n,b\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "Nullable(String)"


def test_clickhouse_type_options_include_nullable_dropdown_values() -> None:
    assert "String" in CLICKHOUSE_TYPE_OPTIONS
    assert "Nullable(Decimal(38, 10))" in CLICKHOUSE_TYPE_OPTIONS


def test_schema_from_editor_rows_accepts_datetime64_timezone_custom_type(tmp_path: Path) -> None:
    from csv_click.schema import schema_from_editor_rows, schema_to_editor_rows

    csv_path = write_csv(tmp_path / "manual_datetime64.csv", "dt\n2026-06-19 10:00:00\n")
    schema = analyze_csv_schema(csv_path)
    rows = schema_to_editor_rows(schema)
    rows[0]["custom_type"] = "DateTime64(3, 'Europe/Moscow')"

    edited_schema = schema_from_editor_rows(rows)

    assert edited_schema.columns[0].final_type == "DateTime64(3, 'Europe/Moscow')"


def test_normalize_identifier_rejects_duplicate_columns_after_normalization() -> None:
    assert normalize_identifier("Order ID") == "order_id"
    assert normalize_identifier("123") == "col_123"
