"""Путь чтения при загрузке обязан совпадать с путём инференса.

Фаза 3b. Превью и обе схемы инференса читают файл с `dtype=str,
keep_default_na=False`, а `iter_pandas_chunks` — без них. Из-за этого то, что
пользователь видит в интерфейсе, и то, что уезжает в ClickHouse, — разные
данные, и расходятся они молча.

Измерено на текущем коде до правки:
  `007` в String-колонке   -> в базу уходит `7`
  `NA` в String-колонке    -> в базу уходит текст `nan`
  пустая ячейка в String   -> в базу уходит текст `nan`
  заголовок `id, code ,amt` -> ValueError: Usecols do not match columns

Для банковского домена первое — это счета, БИК, ИНН и индексы: данные портятся
и выглядят правдоподобно.

Проверять надо СЕРИАЛИЗОВАННЫЕ БАЙТЫ, а не `Series.tolist()`: pandas 3.0
приводит `None` обратно к `NaN`, и очевидное `out.tolist() == ['a', None]`
провалится на корректном коде.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from csv_click.pandas_loader import (
    ReadOptions,
    SchemaMapping,
    analyze_csv_with_pandas_sample,
    chunk_to_json_lines,
    convert_chunk_to_schema,
    iter_pandas_chunks,
    preview_csv_rows,
)


OPTIONS = ReadOptions(batch_size=10)


def load(csv_path: Path, types: dict[str, str], usecols: list[str] | None = None) -> list[dict[str, object]]:
    """Гоняет файл по реальному пути загрузки и отдаёт распарсенный JSON."""
    columns = usecols if usecols is not None else list(types)
    chunks = list(iter_pandas_chunks(csv_path, OPTIONS, columns))
    assert chunks, "чанков не получилось"
    mappings = [
        SchemaMapping(name, name, True, types[name], types[name].startswith("Nullable("))
        for name in columns
    ]
    converted = convert_chunk_to_schema(chunks[0], mappings, chunk_number=1)
    payload = chunk_to_json_lines(converted, list(converted.columns)).decode("utf-8")
    return [json.loads(line) for line in payload.splitlines()]


def write(tmp_path: Path, text: str) -> Path:
    csv_path = tmp_path / "source.csv"
    csv_path.write_text(text, encoding="utf_8")
    return csv_path


def test_leading_zeros_survive_into_a_string_column(tmp_path: Path) -> None:
    """Счета, БИК, ИНН, индексы. Сейчас `007` уезжает как `7`."""
    csv_path = write(tmp_path, "code,name\n007,a\n042,b\n")

    assert load(csv_path, {"code": "String", "name": "String"}) == [
        {"code": "007", "name": "a"},
        {"code": "042", "name": "b"},
    ]


def test_na_like_literals_stay_strings_in_a_string_column(tmp_path: Path) -> None:
    """`NA`, `null`, `N/A` — обычные значения, а не отсутствие значения.
    Сейчас они превращаются в NaN, а в не-nullable String — в текст `nan`."""
    csv_path = write(tmp_path, "id,v\n1,NA\n2,null\n3,N/A\n4,ok\n")

    assert load(csv_path, {"id": "UInt64", "v": "String"}) == [
        {"id": 1, "v": "NA"},
        {"id": 2, "v": "null"},
        {"id": 3, "v": "N/A"},
        {"id": 4, "v": "ok"},
    ]


def test_empty_cell_becomes_null_in_a_nullable_string_column(tmp_path: Path) -> None:
    csv_path = write(tmp_path, "id,v\n1,a\n2,\n3,b\n")

    assert load(csv_path, {"id": "UInt64", "v": "Nullable(String)"}) == [
        {"id": 1, "v": "a"},
        {"id": 2, "v": None},
        {"id": 3, "v": "b"},
    ]


def test_empty_cell_becomes_an_empty_string_in_a_plain_string_column(tmp_path: Path) -> None:
    """Сейчас сюда попадает текст `nan` — значение, которого в файле не было."""
    csv_path = write(tmp_path, "id,v\n1,a\n2,\n3,b\n")

    assert load(csv_path, {"id": "UInt64", "v": "String"}) == [
        {"id": 1, "v": "a"},
        {"id": 2, "v": ""},
        {"id": 3, "v": "b"},
    ]


@pytest.mark.parametrize(
    "clickhouse_type, values, expected",
    [
        ("Nullable(Int64)", "1\n\n3", [1, None, 3]),
        ("Nullable(UInt64)", "1\n\n3", [1, None, 3]),
        ("Nullable(Float64)", "1.5\n\n3.5", [1.5, None, 3.5]),
        ("Nullable(Date)", "2024-01-02\n\n2024-01-03", ["2024-01-02", None, "2024-01-03"]),
        (
            "Nullable(DateTime)",
            "2024-01-02 03:04:05\n\n2024-01-03 00:00:00",
            ["2024-01-02 03:04:05", None, "2024-01-03 00:00:00"],
        ),
        ("Nullable(Decimal(18, 2))", "1.50\n\n3.25", ["1.50", None, "3.25"]),
    ],
)
def test_empty_cell_is_null_for_every_nullable_type(
    tmp_path: Path, clickhouse_type: str, values: str, expected: list[object]
) -> None:
    rows = "\n".join(f"{index},{value}" for index, value in enumerate(values.split("\n"), start=1))
    csv_path = write(tmp_path, f"id,v\n{rows}\n")

    loaded = load(csv_path, {"id": "UInt64", "v": clickhouse_type})

    assert [row["v"] for row in loaded] == expected


@pytest.mark.parametrize("marker", ["NA", "N/A", "null", "NULL", "NaN", "nan", "None", "<NA>", "#N/A"])
def test_na_markers_are_still_missing_values_outside_string_columns(tmp_path: Path, marker: str) -> None:
    """`keep_default_na=False` действует на весь файл, но смысл маркера зависит
    от типа колонки. В String `NA` — это значение. В числовой или временной
    колонке это по-прежнему отсутствие значения, и загрузка, которая раньше
    работала, не должна начать падать."""
    csv_path = write(tmp_path, f"id,v\n1,10\n2,{marker}\n3,30\n")

    assert load(csv_path, {"id": "UInt64", "v": "Nullable(Int64)"}) == [
        {"id": 1, "v": 10},
        {"id": 2, "v": None},
        {"id": 3, "v": 30},
    ]


def test_na_marker_in_a_non_nullable_number_still_fails_loudly(tmp_path: Path) -> None:
    csv_path = write(tmp_path, "id,v\n1,10\n2,NA\n")

    with pytest.raises(Exception, match="non-nullable"):
        load(csv_path, {"id": "UInt64", "v": "Int64"})


def test_na_marker_in_a_datetime_column_is_missing_not_a_parse_error(tmp_path: Path) -> None:
    csv_path = write(tmp_path, "id,v\n1,2024-01-02 03:04:05\n2,NA\n")

    assert load(csv_path, {"id": "UInt64", "v": "Nullable(DateTime)"}) == [
        {"id": 1, "v": "2024-01-02 03:04:05"},
        {"id": 2, "v": None},
    ]


def test_our_na_marker_set_matches_what_pandas_would_have_used() -> None:
    """Иначе наш список молча разойдётся с поведением pandas при обновлении."""
    from pandas._libs.parsers import STR_NA_VALUES

    from csv_click.pandas_loader import NA_MARKERS

    assert NA_MARKERS == set(STR_NA_VALUES)


def test_a_header_with_spaces_around_a_name_loads(tmp_path: Path) -> None:
    """`usecols` сопоставляется с СЫРЫМ заголовком, а strip делается строкой
    позже. Заголовок `id, code ,amt` из Excel-выгрузки роняет и загрузку, и
    preflight, называя колонку, которую интерфейс показывает иначе."""
    csv_path = write(tmp_path, "id, code ,amt\n1,x,2\n")

    assert load(csv_path, {"id": "UInt64", "code": "String", "amt": "UInt64"}) == [
        {"id": 1, "code": "x", "amt": 2}
    ]


def test_a_header_with_spaces_gives_the_same_names_everywhere(tmp_path: Path) -> None:
    """Регрессионная стража: имена колонок из превью, из инференса и из чтения
    должны совпадать, иначе интерфейс показывает одно, а грузится другое."""
    csv_path = write(tmp_path, "id, code ,amt\n1,x,2\n")

    preview_names = list(preview_csv_rows(csv_path, OPTIONS).columns)
    schema_names = [column.source_name for column in analyze_csv_with_pandas_sample(csv_path, OPTIONS).columns]
    chunk_names = list(next(iter(iter_pandas_chunks(csv_path, OPTIONS))).columns)

    assert preview_names == schema_names == chunk_names == ["id", "code", "amt"]


def test_selecting_a_subset_of_a_padded_header_works(tmp_path: Path) -> None:
    csv_path = write(tmp_path, "id, code ,amt\n1,x,2\n")

    assert load(csv_path, {"id": "UInt64", "code": "String", "amt": "UInt64"}, usecols=["code", "id"]) == [
        {"code": "x", "id": 1}
    ]


def test_what_the_preview_shows_is_what_gets_loaded(tmp_path: Path) -> None:
    """Одно утверждение, ради которого делается вся фаза: строковое
    представление ячейки в превью и в чтении совпадает."""
    csv_path = write(tmp_path, "code,v,n\n007,NA,1\n042,,2\n")

    preview = preview_csv_rows(csv_path, OPTIONS)
    chunk = next(iter(iter_pandas_chunks(csv_path, OPTIONS)))

    assert preview.to_dict("records") == chunk.to_dict("records")
