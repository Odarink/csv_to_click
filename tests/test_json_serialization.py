"""Контракт сериализации в JSONEachRow.

Фаза 3 заменяет построчный `json.dumps` на векторный `DataFrame.to_json`. На
замеренном профиле (одна колонка, 107 млн строк) сериализация занимала 90,1%
времени вставки, поэтому это главный рычаг — но менять она обязана только
скорость, не байты.

Ожидания ниже сняты с построчного сериализатора выполнением кода, а не написаны
по памяти. Единственное намеренное расхождение — разделитель в `DateTime`:
построчный путь слал `2024-01-02T03:04:05`, векторный шлёт
`2024-01-02 03:04:05`. Это канонический `basic`-формат ClickHouse, и он же
открывает дорогу к `date_time_input_format=basic`, который в 2–5 раз дешевле
`best_effort` на значение.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

import csv_click.pandas_loader as pandas_loader
from csv_click.errors import CsvLoadError, CsvSchemaError
from csv_click.pandas_loader import (
    SchemaMapping,
    chunk_to_json_lines,
    convert_chunk_to_schema,
    iter_json_each_row_payloads,
)


# тип -> (исходные значения, ожидаемые распарсенные JSON-объекты)
CONTRACT: dict[str, tuple[list[str], list[dict[str, object]]]] = {
    "String": (
        ["abc", "007", "", "тест"],
        [{"col": "abc"}, {"col": "007"}, {"col": ""}, {"col": "тест"}],
    ),
    "Nullable(String)": (
        ["abc", "007", "", "тест"],
        [{"col": "abc"}, {"col": "007"}, {"col": ""}, {"col": "тест"}],
    ),
    "Int64": (
        ["42", "-7", "0", "007"],
        [{"col": 42}, {"col": -7}, {"col": 0}, {"col": 7}],
    ),
    "Nullable(Int64)": (
        ["42", "", "0", "007"],
        [{"col": 42}, {"col": None}, {"col": 0}, {"col": 7}],
    ),
    "UInt64": (
        ["42", "0", "18446744073709551615", "007"],
        [{"col": 42}, {"col": 0}, {"col": 18446744073709551615}, {"col": 7}],
    ),
    "Nullable(UInt64)": (
        ["42", "", "0", "007"],
        [{"col": 42}, {"col": None}, {"col": 0}, {"col": 7}],
    ),
    "Float64": (
        ["1.5", "-0.25", "0", "3.141592653589793"],
        [{"col": 1.5}, {"col": -0.25}, {"col": 0.0}, {"col": 3.141592653589793}],
    ),
    "Nullable(Float64)": (
        ["1.5", "", "0", "2.0"],
        [{"col": 1.5}, {"col": None}, {"col": 0.0}, {"col": 2.0}],
    ),
    "Decimal(18, 2)": (
        ["1.50", "-3.25", "0", "12345678901234.99"],
        [{"col": "1.50"}, {"col": "-3.25"}, {"col": "0"}, {"col": "12345678901234.99"}],
    ),
    "Nullable(Decimal(18, 2))": (
        ["1.50", "", "0", "7.10"],
        [{"col": "1.50"}, {"col": None}, {"col": "0"}, {"col": "7.10"}],
    ),
    "Decimal(38, 10)": (
        ["1.5000000000", "0", "-2.25", "3.1415926536"],
        [{"col": "1.5000000000"}, {"col": "0"}, {"col": "-2.25"}, {"col": "3.1415926536"}],
    ),
    "Date": (
        ["2024-01-02", "2024-12-31", "1999-06-15", "2000-02-29"],
        [{"col": "2024-01-02"}, {"col": "2024-12-31"}, {"col": "1999-06-15"}, {"col": "2000-02-29"}],
    ),
    "Nullable(Date)": (
        ["2024-01-02", "", "1999-06-15", "2000-02-29"],
        [{"col": "2024-01-02"}, {"col": None}, {"col": "1999-06-15"}, {"col": "2000-02-29"}],
    ),
    "DateTime": (
        ["2024-01-02 03:04:05", "2024-12-31 23:59:59", "1999-06-15 00:00:00", "2000-02-29 12:00:00"],
        [
            {"col": "2024-01-02 03:04:05"},
            {"col": "2024-12-31 23:59:59"},
            {"col": "1999-06-15 00:00:00"},
            {"col": "2000-02-29 12:00:00"},
        ],
    ),
    "Nullable(DateTime)": (
        ["2024-01-02 03:04:05", "", "1999-06-15 00:00:00", "2000-02-29 12:00:00"],
        [
            {"col": "2024-01-02 03:04:05"},
            {"col": None},
            {"col": "1999-06-15 00:00:00"},
            {"col": "2000-02-29 12:00:00"},
        ],
    ),
    "Bool": (
        ["true", "false", "1", "0"],
        [{"col": True}, {"col": False}, {"col": True}, {"col": False}],
    ),
    "Nullable(Bool)": (
        ["true", "", "1", "0"],
        [{"col": True}, {"col": None}, {"col": True}, {"col": False}],
    ),
}


def serialize(values: list[str], clickhouse_type: str) -> list[dict[str, object]]:
    frame = pd.DataFrame({"col": values}, dtype="object")
    mapping = [
        SchemaMapping("col", "col", True, clickhouse_type, clickhouse_type.startswith("Nullable("))
    ]
    converted = convert_chunk_to_schema(frame, mapping, chunk_number=1)
    payload = chunk_to_json_lines(converted, ["col"])
    return [json.loads(line) for line in payload.decode("utf-8").splitlines()]


@pytest.mark.parametrize("clickhouse_type", sorted(CONTRACT))
def test_vectorized_serializer_matches_the_contract_for_every_ui_type(clickhouse_type: str) -> None:
    values, expected = CONTRACT[clickhouse_type]

    assert serialize(values, clickhouse_type) == expected


def test_datetime_uses_the_clickhouse_basic_separator_not_iso_t() -> None:
    """Единственное намеренное расхождение с построчным путём.

    `best_effort` терпел `T` и микросекунды, `basic` их не примет — а перейти на
    `basic` стоит, он в 2–5 раз дешевле на значение.
    """
    payload = chunk_to_json_lines(
        convert_chunk_to_schema(
            pd.DataFrame({"col": ["2024-01-02 03:04:05"]}, dtype="object"),
            [SchemaMapping("col", "col", True, "DateTime", False)],
            chunk_number=1,
        ),
        ["col"],
    ).decode("utf-8")

    assert payload == '{"col":"2024-01-02 03:04:05"}'
    assert "T" not in payload
    assert "." not in payload, "дробные секунды basic-парсер отвергнет"


def test_serializer_emits_one_json_object_per_line_without_a_trailing_newline() -> None:
    frame = pd.DataFrame({"a": ["1", "2"], "b": ["x", "y"]}, dtype="object")
    mapping = [
        SchemaMapping("a", "a", True, "Int64", False),
        SchemaMapping("b", "b", True, "String", False),
    ]

    payload = chunk_to_json_lines(convert_chunk_to_schema(frame, mapping, chunk_number=1), ["a", "b"])

    assert payload == b'{"a":1,"b":"x"}\n{"a":2,"b":"y"}'


def test_serializer_keeps_column_order_and_ignores_columns_not_asked_for() -> None:
    frame = pd.DataFrame({"a": ["1"], "b": ["x"], "c": ["skip"]}, dtype="object")
    mapping = [
        SchemaMapping("a", "a", True, "Int64", False),
        SchemaMapping("b", "b", True, "String", False),
        SchemaMapping("c", "c", True, "String", False),
    ]
    converted = convert_chunk_to_schema(frame, mapping, chunk_number=1)

    assert chunk_to_json_lines(converted, ["b", "a"]) == b'{"b":"x","a":1}'


def test_serializer_does_not_escape_non_ascii() -> None:
    frame = pd.DataFrame({"col": ["Тест ĄŽ 中文"]}, dtype="object")
    mapping = [SchemaMapping("col", "col", True, "String", False)]

    payload = chunk_to_json_lines(convert_chunk_to_schema(frame, mapping, chunk_number=1), ["col"])

    assert payload.decode("utf-8") == '{"col":"Тест ĄŽ 中文"}'


def test_serializer_escapes_characters_that_would_break_jsoneachrow() -> None:
    frame = pd.DataFrame({"col": ['a"b\\c\nd\te']}, dtype="object")
    mapping = [SchemaMapping("col", "col", True, "String", False)]

    payload = chunk_to_json_lines(convert_chunk_to_schema(frame, mapping, chunk_number=1), ["col"])

    assert payload.count(b"\n") == 0, "перевод строки внутри значения разорвал бы JSONEachRow"
    assert json.loads(payload.decode("utf-8")) == {"col": 'a"b\\c\nd\te'}


def test_float_precision_is_capped_at_fifteen_decimals() -> None:
    """Задокументированное ограничение векторного пути.

    `to_json` принимает не больше 15 знаков после запятой (это потолок
    параметра `double_precision`), а построчный `json.dumps` писал repr со
    всеми значащими. Значения, которым нужно 16–17 значащих цифр, теряют
    последнюю: относительная погрешность ~1e-16, то есть на границе
    представимости самого float64. Для чисел из CSV это ниже значимости, но
    поведение должно быть выбранным, а не случайным.
    """
    frame = pd.DataFrame({"col": [1 / 3, 1.2345678901234567, 3.141592653589793, 0.1]}, dtype="object")

    values = [
        json.loads(line)["col"]
        for line in chunk_to_json_lines(frame, ["col"]).decode("utf-8").splitlines()
    ]

    assert values[0] == 0.333333333333333, "потеряна 16-я значащая цифра"
    assert values[1] == 1.234567890123457
    # 15 знаков хватает — эти два значения проходят без потерь.
    assert values[2] == 3.141592653589793
    assert values[3] == 0.1


def test_negative_zero_loses_its_sign() -> None:
    """Тоже следствие `to_json`. Для ClickHouse Float64 -0.0 и 0.0 равны при
    сравнении, различает их только копирование знака — на данных из CSV этого
    не бывает, но записать факт надо."""
    frame = pd.DataFrame({"col": [-0.0]}, dtype="object")

    assert chunk_to_json_lines(frame, ["col"]) == b'{"col":0.0}'


def test_non_finite_float_fails_loudly_instead_of_becoming_null() -> None:
    """Построчный путь звал `json.dumps(allow_nan=False)` и падал на inf.
    У `to_json` такого рычага нет, он молча пишет null — то есть значение,
    переполнившее double, тихо легло бы в NOT NULL колонку как пустое, а прогон
    отчитался бы об успехе. Достижимо без ручного выбора типа: колонка со
    слишком большим scale сама сваливается в Float64.
    """
    frame = pd.DataFrame({"col": ["1.5", "9e999"]}, dtype="object")
    mapping = [SchemaMapping("col", "col", True, "Float64", False)]

    with pytest.raises(CsvSchemaError, match="col"):
        convert_chunk_to_schema(frame, mapping, chunk_number=1)


def test_negative_infinity_fails_loudly_too() -> None:
    frame = pd.DataFrame({"col": ["-9e999"]}, dtype="object")
    mapping = [SchemaMapping("col", "col", True, "Nullable(Float64)", True)]

    with pytest.raises(CsvSchemaError, match="col"):
        convert_chunk_to_schema(frame, mapping, chunk_number=1)


def test_timezone_aware_datetime_fails_loudly_instead_of_shifting_the_instant() -> None:
    """`%Y-%m-%d %H:%M:%S` печатает локальное время стены и молча теряет офсет.

    Построчный путь слал `.isoformat()` с офсетом, а `best_effort` на сервере
    его учитывал, поэтому момент сохранялся верно. Уронить офсет — значит
    сдвинуть КАЖДУЮ строку, ничего не сообщив. Целевая колонка `DateTime`
    таймзоны не несёт, так что выбрать интерпретацию за пользователя нельзя.
    """
    frame = pd.DataFrame({"col": ["2024-01-02T03:04:05+03:00"]}, dtype="object")
    mapping = [SchemaMapping("col", "col", True, "DateTime", False)]

    with pytest.raises(CsvSchemaError) as exc_info:
        convert_chunk_to_schema(frame, mapping, chunk_number=1)

    message = str(exc_info.value)
    assert "col" in message
    assert "timezone" in message.lower() or "часов" in message.lower()


def test_naive_datetime_still_passes_after_the_timezone_check() -> None:
    frame = pd.DataFrame({"col": ["2024-01-02 03:04:05"]}, dtype="object")
    mapping = [SchemaMapping("col", "col", True, "DateTime", False)]

    payload = chunk_to_json_lines(convert_chunk_to_schema(frame, mapping, chunk_number=1), ["col"])

    assert payload == b'{"col":"2024-01-02 03:04:05"}'


def test_years_below_1000_keep_four_digits() -> None:
    """`.dt.strftime` в pandas не дополняет год нулями: 1-й год уезжает как
    `1-01-01`, и ClickHouse получает мусор вместо даты."""
    frame = pd.DataFrame({"col": ["0001-01-01", "0999-12-31", "2024-01-02"]}, dtype="object")
    mapping = [SchemaMapping("col", "col", True, "Date", False)]

    values = [
        json.loads(line)["col"]
        for line in chunk_to_json_lines(
            convert_chunk_to_schema(frame, mapping, chunk_number=1), ["col"]
        ).decode("utf-8").splitlines()
    ]

    assert values == ["0001-01-01", "0999-12-31", "2024-01-02"]


def greedy_block_count(lines: list[bytes], limit: int) -> int:
    """Сколько блоков дал бы жадный упаковщик, видящий каждую строку.

    Это то, что делал построчный путь до фазы 3, и нижняя планка, ниже которой
    нарезка по срезам опускаться не обязана, но и заметно выше быть не должна:
    каждый лишний блок - это лишний HTTP-запрос.
    """
    blocks, used = 1, 0
    for line in lines:
        extra = len(line) + (1 if used else 0)
        if used + extra > limit:
            blocks += 1
            used = len(line)
        else:
            used += extra
    return blocks


@pytest.mark.parametrize(
    "name, values",
    [
        ("толстые в начале", ["F" * 5000] * 10 + ["t" * 10] * 1000),
        ("толстые в конце", ["t" * 10] * 1000 + ["F" * 5000] * 10),
        ("вперемешку", [("F" * 5000 if index % 100 == 0 else "t" * 10) for index in range(1010)]),
        ("один толстяк среди тонких", ["t" * 10] * 500 + ["F" * 15000] + ["t" * 10] * 500),
    ],
)
def test_skewed_rows_are_packed_about_as_tightly_as_a_greedy_packer(name: str, values: list[str]) -> None:
    """Размер блока оценивается по срезам, а не по каждой строке. Оценка обязана
    и расти, и падать: иначе, пройдя пачку толстых строк, цикл продолжит резать
    тонкие такими же мелкими кусками, и запросов станет кратно больше."""
    frame = pd.DataFrame({"col": values}, dtype="object")
    converted = convert_chunk_to_schema(
        frame, [SchemaMapping("col", "col", True, "String", False)], chunk_number=1
    )
    limit = 20_000

    blocks = list(iter_json_each_row_payloads(converted, ["col"], max_payload_bytes=limit))

    assert sum(rows for _, rows in blocks) == len(values)
    assert all(len(payload) <= limit for payload, _ in blocks)
    restored = [
        json.loads(line)["col"]
        for payload, _ in blocks
        for line in payload.decode("utf-8").splitlines()
    ]
    assert restored == values

    reference = greedy_block_count(
        chunk_to_json_lines(converted, ["col"]).split(b"\n"), limit
    )
    assert len(blocks) <= reference + 1, (
        f"{name}: {len(blocks)} блоков против {reference} у жадного упаковщика; "
        f"заполнение {[round(len(p) / limit * 100) for p, _ in blocks]}"
    )


def test_empty_chunk_serializes_to_empty_bytes() -> None:
    frame = pd.DataFrame({"col": []}, dtype="object")
    mapping = [SchemaMapping("col", "col", True, "String", False)]

    assert chunk_to_json_lines(convert_chunk_to_schema(frame, mapping, chunk_number=1), ["col"]) == b""


def build_rows(count: int, width: int = 20) -> pd.DataFrame:
    frame = pd.DataFrame({"col": ["x" * width] * count}, dtype="object")
    return convert_chunk_to_schema(
        frame, [SchemaMapping("col", "col", True, "String", False)], chunk_number=1
    )


def test_payloads_never_exceed_the_byte_limit_and_lose_no_rows() -> None:
    converted = build_rows(50)
    limit = 200

    blocks = list(iter_json_each_row_payloads(converted, ["col"], max_payload_bytes=limit))

    assert sum(rows for _, rows in blocks) == 50
    assert all(len(payload) <= limit for payload, _ in blocks)
    assert len(blocks) > 1, "лимит должен был вынудить разбиение"
    restored = [
        json.loads(line)
        for payload, _ in blocks
        for line in payload.decode("utf-8").splitlines()
    ]
    assert restored == [{"col": "x" * 20}] * 50


def test_row_counts_reported_per_block_match_the_lines_in_it() -> None:
    converted = build_rows(37)

    for payload, rows in iter_json_each_row_payloads(converted, ["col"], max_payload_bytes=150):
        assert len(payload.decode("utf-8").splitlines()) == rows


def test_a_single_row_larger_than_the_limit_fails_loudly() -> None:
    converted = build_rows(1, width=500)

    with pytest.raises(CsvLoadError, match="larger than Max insert payload"):
        list(iter_json_each_row_payloads(converted, ["col"], max_payload_bytes=100))


def test_uneven_rows_force_the_slice_to_shrink_below_the_estimate() -> None:
    """Размер среза оценивается по средней строке всего чанка. На разнородных
    данных оценка промахивается, и без ужимания блок уехал бы за лимит."""
    values = ["x" * 10] * 10 + ["y" * 400] * 10
    frame = pd.DataFrame({"col": values}, dtype="object")
    converted = convert_chunk_to_schema(
        frame, [SchemaMapping("col", "col", True, "String", False)], chunk_number=1
    )
    limit = 1000

    blocks = list(iter_json_each_row_payloads(converted, ["col"], max_payload_bytes=limit))

    assert all(len(payload) <= limit for payload, _ in blocks), "срез не ужался под лимит"
    assert sum(rows for _, rows in blocks) == 20
    restored = [
        json.loads(line)["col"]
        for payload, _ in blocks
        for line in payload.decode("utf-8").splitlines()
    ]
    assert restored == values


def test_the_block_estimate_grows_back_after_a_run_of_fat_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Считается не результат, а цена: сколько проходов сериализации сделано.

    Число блоков здесь одинаковое в обоих случаях, поэтому по нему дефект не
    виден. Но если оценка строк-на-блок не растёт при дозаполнении, то, пройдя
    пачку толстых строк, цикл читает тонкие такими же мелкими порциями: 42
    вызова `to_json` вместо 23 на этих данных, и разрыв растёт с размером чанка.
    """
    values = ["F" * 4000] * 5 + ["t" * 10] * 5000
    frame = pd.DataFrame({"col": values}, dtype="object")
    converted = convert_chunk_to_schema(
        frame, [SchemaMapping("col", "col", True, "String", False)], chunk_number=1
    )

    calls: list[int] = []
    original = pandas_loader.chunk_to_json_lines

    def counted(chunk: pd.DataFrame, columns: list[str]) -> bytes:
        calls.append(len(chunk))
        return original(chunk, columns)

    monkeypatch.setattr(pandas_loader, "chunk_to_json_lines", counted)
    blocks = list(
        pandas_loader.iter_json_each_row_payloads(converted, ["col"], max_payload_bytes=5000)
    )

    assert sum(rows for _, rows in blocks) == len(values)
    assert len(calls) <= 30, f"проходов сериализации {len(calls)}, оценка не восстанавливается"


def test_an_empty_chunk_yields_no_blocks_at_all() -> None:
    """Пустой блок отправился бы в ClickHouse как пустой INSERT."""
    frame = pd.DataFrame({"col": []}, dtype="object")
    converted = convert_chunk_to_schema(
        frame, [SchemaMapping("col", "col", True, "String", False)], chunk_number=1
    )

    assert list(iter_json_each_row_payloads(converted, ["col"], max_payload_bytes=1000)) == []


def test_a_chunk_that_fits_is_emitted_as_one_block() -> None:
    converted = build_rows(10)

    blocks = list(iter_json_each_row_payloads(converted, ["col"], max_payload_bytes=1_000_000))

    assert len(blocks) == 1
    assert blocks[0][1] == 10
