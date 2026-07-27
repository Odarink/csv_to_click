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
import random
from decimal import Decimal

import numpy as np
import pandas as pd
import pyarrow as pa
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
    # Пустая ячейка в Nullable-колонке — это NULL, а не пустая строка: в CSV
    # `,,` и `,"",` неразличимы, а Nullable выбирается инференсом именно из-за
    # пустых значений. Для обычного String пустая остаётся пустой (ниже).
    "Nullable(String)": (
        ["abc", "007", "", "тест"],
        [{"col": "abc"}, {"col": "007"}, {"col": None}, {"col": "тест"}],
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


def test_a_chunk_whose_head_looks_thin_still_respects_the_limit() -> None:
    """Оценка размера берётся с ГОЛОВЫ чанка, и она умеет соврать.

    1024 тонкие строки, дальше толстые: по голове чанк «влезает целиком», на
    деле нет. Решение «отдать одним блоком» обязано опираться на фактическую
    длину, а не на оценку, иначе в ClickHouse уедет блок сверх лимита.
    """
    values = ["t" * 10] * 1024 + ["F" * 5000] * 200
    frame = pd.DataFrame({"col": values}, dtype="object")
    converted = convert_chunk_to_schema(
        frame, [SchemaMapping("col", "col", True, "String", False)], chunk_number=1
    )
    limit = 100_000

    blocks = list(iter_json_each_row_payloads(converted, ["col"], max_payload_bytes=limit))

    assert sum(rows for _, rows in blocks) == len(values)
    assert all(len(payload) <= limit for payload, _ in blocks)
    restored = [
        json.loads(line)["col"]
        for payload, _ in blocks
        for line in payload.decode("utf-8").splitlines()
    ]
    assert restored == values


def test_a_fitting_chunk_is_serialized_in_one_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    """Чанк, который влезает целиком, сериализуется одним проходом.

    Тест держит именно это: срезов по `rows_per_block` быть не должно. Сколько
    работы делается ВНУТРИ прохода, снаружи не видно — построчная упаковка
    проверяется замером и эквивалентной мутацией, а не этим тестом.
    """
    converted = build_rows(5_000)
    serialized_rows: list[int] = []
    original = pandas_loader.chunk_to_json_lines

    def counted(chunk: pd.DataFrame, columns: list[str]) -> bytes:
        serialized_rows.append(len(chunk))
        return original(chunk, columns)

    monkeypatch.setattr(pandas_loader, "chunk_to_json_lines", counted)

    blocks = list(
        pandas_loader.iter_json_each_row_payloads(converted, ["col"], max_payload_bytes=1_000_000)
    )

    assert len(blocks) == 1
    assert blocks[0][1] == 5_000
    assert blocks[0][0] == pandas_loader.chunk_to_json_lines(converted, ["col"])
    assert max(serialized_rows) == 5_000, (
        "чанк обязан сериализоваться одним куском, а не срезами: "
        f"размеры проходов {serialized_rows}"
    )


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


# --- фаза 3c: тот же JSONEachRow, собранный через Arrow -----------------------
#
# Быстрый путь имеет право существовать только при БАЙТ-В-БАЙТ совпадении с
# `to_json`. Поэтому главная проверка тут дифференциальная: эталон против Arrow
# на значениях, которые обычно и расходятся. Расхождения `to_json`, снятые
# выполнением: прямой слэш экранируется (`a/b` -> `a\/b`), не-ASCII уезжает как
# есть (`force_ascii=False`), управляющие символы становятся `\u0001`.

ARROW_CASES: dict[str, pd.Series] = {
    "UInt64": pd.Series(pd.array([0, 7, 18446744073709551615], dtype="UInt64")),
    "UInt64 с пропуском": pd.Series(pd.array([7, None, 42], dtype="UInt64")),
    "Int64 отрицательные": pd.Series(pd.array([-9223372036854775808, 0, 42], dtype="Int64")),
    "Int64 с пропуском": pd.Series(pd.array([-7, None], dtype="Int64")),
    "boolean": pd.Series(pd.array([True, False], dtype="boolean")),
    "boolean с пропуском": pd.Series(pd.array([True, None, False], dtype="boolean")),
    "строки обычные": pd.Series(["Иванов", "ok", ""], dtype="object"),
    "строки с кавычкой": pd.Series(['a"b', 'both "x" and "y"'], dtype="object"),
    "строки с обратным слэшем": pd.Series(["a\\b", "C:\\Users\\x"], dtype="object"),
    "строки с прямым слэшем": pd.Series(["01/02/2024", "a/b/c"], dtype="object"),
    "строки со всеми тремя": pd.Series(['a"b\\c/d'], dtype="object"),
    "строки не-ASCII": pd.Series(["Иванов Иван", "ок 🚀", "über"], dtype="object"),
    "строки с пропуском": pd.Series(["a", None], dtype="object"),
    "строки dtype string": pd.Series(pd.array(["a", None, 'q"q'], dtype="string")),
    "строки, похожие на JSON": pd.Series(['{"a": 1}', "[1,2]", "null"], dtype="object"),
    "одна строка": pd.Series(pd.array([1], dtype="Int64")),
}


@pytest.mark.parametrize("case", sorted(ARROW_CASES))
def test_the_arrow_path_matches_to_json_byte_for_byte(case: str) -> None:
    frame = pd.DataFrame({"col": ARROW_CASES[case]})

    fast = pandas_loader._arrow_json_lines(frame, ["col"])

    assert fast is not None, "быстрый путь обязан браться на этих типах"
    assert fast == pandas_loader._pandas_json_lines(frame, ["col"])


COLUMN_NAME_CASES: dict[str, str] = {
    "кавычка в имени": 'q"q',
    "обратный слэш в имени": "back\\slash",
    "прямой слэш в имени": "с/слэшем",
    "не-ASCII в имени": "имя_колонки",
    "точка в имени": "a.b",
}


@pytest.mark.parametrize("case", sorted(COLUMN_NAME_CASES))
def test_the_arrow_path_escapes_column_names_like_to_json(case: str) -> None:
    """`to_json` экранирует и КЛЮЧ, не только значение.

    Найдено дифференциальным фаззером: имя `q"q` давало `{"q"q":1}` — сломанный
    JSON. Целевое имя колонки приходит из редактора типов свободным текстом,
    `normalize_identifier` к нему не применяется, так что вход достижим.
    """
    column = COLUMN_NAME_CASES[case]
    frame = pd.DataFrame({column: pd.array([1, None], dtype="Int64")})

    fast = pandas_loader._arrow_json_lines(frame, [column])

    assert fast is not None
    assert fast == pandas_loader._pandas_json_lines(frame, [column])


def test_a_control_character_in_a_column_name_falls_back() -> None:
    column = "col\x01name"
    frame = pd.DataFrame({column: pd.array([1], dtype="Int64")})

    assert pandas_loader._arrow_json_lines(frame, [column]) is None
    assert chunk_to_json_lines(frame, [column]) == pandas_loader._pandas_json_lines(frame, [column])


def test_the_arrow_path_matches_to_json_on_many_columns_at_once() -> None:
    """Порядок колонок, запятые и кавычки в ключах — там же, где у эталона."""
    frame = pd.DataFrame(
        {
            "nmid": pd.array([1, None], dtype="UInt64"),
            "name": pd.Series(['Иванов "И"', None], dtype="object"),
            "flag": pd.array([True, None], dtype="boolean"),
            "dt": pd.Series(["2024-01-02 03:04:05", "0001-01-01 00:00:00"], dtype="object"),
        }
    )
    columns = ["nmid", "name", "flag", "dt"]

    fast = pandas_loader._arrow_json_lines(frame, columns)

    assert fast is not None
    assert fast == pandas_loader._pandas_json_lines(frame, columns)


def test_safe_columns_never_reach_the_reference_serializer(monkeypatch: pytest.MonkeyPatch) -> None:
    """Быстрый путь обязан действительно БРАТЬСЯ, а не просто существовать.

    Без этого теста удаление быстрого пути оставило бы весь набор зелёным: байты
    совпадают, меняется только скорость. Эталон здесь заминирован, а ожидаемые
    байты выписаны руками, а не получены тем же кодом.
    """

    def must_not_run(chunk: pd.DataFrame, columns: list[str]) -> bytes:
        raise AssertionError("эталон `to_json` не должен вызываться на безопасных колонках")

    monkeypatch.setattr(pandas_loader, "_pandas_json_lines", must_not_run)
    frame = pd.DataFrame(
        {
            "nmid": pd.array([7, None], dtype="UInt64"),
            "name": pd.Series(["Иванов", 'q"q'], dtype="object"),
            # dtype "string" даёт large_string: он тоже обязан идти быстрым путём,
            # а не откатываться на эталон из-за несовпадения типов в ядре Arrow.
            "code": pd.array(["01", None], dtype="string"),
        }
    )

    payload = chunk_to_json_lines(frame, ["nmid", "name", "code"])

    assert payload == (
        '{"nmid":7,"name":"Иванов","code":"01"}\n'
        '{"nmid":null,"name":"q\\"q","code":null}'
    ).encode("utf-8")


# Временные dtype здесь намеренно отсутствуют: `_convert_series` приводит их к
# строкам до сериализации, а сам эталон на них предупреждает об устаревшем
# формате даты — проверять чужую депрекацию тут нечего.
EXOTIC_DTYPES: dict[str, pd.Series] = {
    "categorical": pd.Series(pd.Categorical(["a", "b", "a"])),
    "arrow large_string": pd.Series(["a", None], dtype=pd.ArrowDtype(pa.large_string())),
    "arrow int32": pd.Series([1, None], dtype=pd.ArrowDtype(pa.int32())),
    "arrow decimal": pd.Series([Decimal("1.50")], dtype=pd.ArrowDtype(pa.decimal128(18, 2))),
    "numpy int32": pd.Series([1, 2], dtype="int32"),
    "numpy bool": pd.Series([True, False], dtype="bool"),
    "пустая object": pd.Series([], dtype="object"),
}


@pytest.mark.parametrize("case", sorted(EXOTIC_DTYPES))
def test_no_dtype_makes_the_arrow_path_raise_instead_of_refusing(case: str) -> None:
    """Контракт: либо байт-в-байт как эталон, либо отказ. Исключения — нельзя.

    Через `custom_type` в редакторе типов до сериализации доезжают колонки,
    которых обычный путь не создаёт. Исключение здесь обменяло бы 14 минут
    загрузки на незнакомое ядро Arrow.
    """
    frame = pd.DataFrame({"col": EXOTIC_DTYPES[case]})

    assert chunk_to_json_lines(frame, ["col"]) == pandas_loader._pandas_json_lines(frame, ["col"])


_FUZZ_CHARS = [
    "a", "Z", "0", " ", "", '"', "\\", "/", "'", "`", " ", " ",
    "Иванов", "über", "中文", "🚀", "{", "}", "[", "]", ":", ",", "null", "true",
    "-", "+", "e", ".", "\\u0041", "\\\\", '\\"',
]
_FUZZ_NAMES = ["col", "nmid", "имя", "a b", "a.b", 'q"q', "back\\slash", "с/слэшем"]
_FUZZ_INTS = [0, 1, -1, 2**31 - 1, 2**63 - 1, -(2**63), 2**64 - 1, 10**18]


def _fuzz_series(rng: random.Random, rows: int) -> pd.Series:
    kind = rng.choice(["uint", "int", "bool", "object", "string"])
    if kind == "uint":
        values = [rng.choice([None, *_FUZZ_INTS]) for _ in range(rows)]
        return pd.Series(pd.array([None if v is None or v < 0 else v for v in values], dtype="UInt64"))
    if kind == "int":
        values = [rng.choice([None, *_FUZZ_INTS]) for _ in range(rows)]
        return pd.Series(pd.array([None if v is None or v >= 2**63 else v for v in values], dtype="Int64"))
    if kind == "bool":
        return pd.Series(pd.array([rng.choice([True, False, None]) for _ in range(rows)], dtype="boolean"))
    texts = [
        rng.choice([None, "".join(rng.choice(_FUZZ_CHARS) for _ in range(rng.randint(0, 5)))])
        for _ in range(rows)
    ]
    return pd.Series(pd.array(texts, dtype="string") if kind == "string" else texts, dtype=None if kind == "string" else "object")


def test_the_arrow_path_survives_a_seeded_differential_fuzz() -> None:
    """Перебор случайных кадров против эталона. Семя фиксировано.

    Этот перебор нашёл настоящий дефект — неэкранированные ИМЕНА колонок, из-за
    которых имя `q"q` давало сломанный JSON. Ручные случаи его не поймали, потому
    что имя колонки никто не подозревал. 20 тыс. итераций того же генератора
    расхождений больше не дают; здесь их 400, чтобы набор оставался быстрым.
    """
    rng = random.Random(20260727)
    fast_taken = 0

    for _ in range(400):
        rows = rng.randint(1, 5)
        names = rng.sample(_FUZZ_NAMES, rng.randint(1, 4))
        frame = pd.DataFrame({name: _fuzz_series(rng, rows) for name in names})
        columns = list(frame.columns)
        rng.shuffle(columns)

        fast = pandas_loader._arrow_json_lines(frame, columns)
        if fast is None:
            continue
        fast_taken += 1
        assert fast == pandas_loader._pandas_json_lines(frame, columns), (
            f"расхождение на кадре {({name: list(frame[name]) for name in columns})}, колонки {columns}"
        )

    assert fast_taken > 100, f"быстрый путь брался всего {fast_taken} раз — перебор ничего не проверил"


def test_a_chunked_arrow_column_falls_back_instead_of_crashing() -> None:
    """Строковая колонка больше 2 ГиБ приезжает `ChunkedArray`, а не `Array`.

    Найдено ревью. Такой массив проходит все проверки типа, ядра Arrow его
    принимают, а `ListArray.from_arrays` бросает ОБЫЧНЫЙ `TypeError` — не
    наследник `ArrowException`. Загрузка падала вместо отката на эталон, который
    с этими данными работает.
    """
    chunked = pa.chunked_array([pa.array(["a", None]), pa.array(['q"q'])])
    frame = pd.DataFrame({"col": pd.Series(pd.arrays.ArrowExtensionArray(chunked))})

    assert pandas_loader._arrow_json_lines(frame, ["col"]) is None
    assert chunk_to_json_lines(frame, ["col"]) == pandas_loader._pandas_json_lines(frame, ["col"])


def test_an_object_column_of_huge_ints_falls_back_instead_of_crashing() -> None:
    """`from_pandas` на int больше int64 бросает `OverflowError`."""
    frame = pd.DataFrame({"col": pd.Series([2**70, 1], dtype="object")})

    assert pandas_loader._arrow_json_lines(frame, ["col"]) is None
    assert chunk_to_json_lines(frame, ["col"]) == pandas_loader._pandas_json_lines(frame, ["col"])


@pytest.mark.parametrize("code", list(range(0x20)))
def test_every_control_character_sends_the_chunk_to_the_reference(code: int) -> None:
    """Все 32 управляющих символа, а не три штучных.

    Ревью показало: мутация границы класса `[\\x00-\\x1f]` на `[\\x00-\\x1e]`
    выживала, потому что 0x1f не проверялся ни одним тестом.
    """
    frame = pd.DataFrame({"col": [f"a{chr(code)}b"]}, dtype="object")

    assert pandas_loader._arrow_json_lines(frame, ["col"]) is None
    assert chunk_to_json_lines(frame, ["col"]) == pandas_loader._pandas_json_lines(frame, ["col"])


@pytest.mark.parametrize("code", list(range(0x20)))
def test_every_control_character_in_a_column_name_sends_the_chunk_to_the_reference(code: int) -> None:
    column = f"a{chr(code)}b"
    frame = pd.DataFrame({column: pd.array([1], dtype="Int64")})

    assert pandas_loader._arrow_json_lines(frame, [column]) is None
    assert chunk_to_json_lines(frame, [column]) == pandas_loader._pandas_json_lines(frame, [column])


#: Тип из редактора -> берётся ли быстрый путь ПОСЛЕ реальной конвертации.
#: Таблица снята выполнением и держит границу выигрыша: если путь перестанет
#: браться на String или числах, ускорение исчезнет молча.
FAST_PATH_BY_TYPE: dict[str, bool] = {
    "String": True,
    "Int64": True,
    "UInt64": True,
    "Float64": False,
    "Decimal(18, 2)": False,
    "Decimal(38, 10)": False,
    "Date": True,
    "DateTime": True,
    "Bool": True,
}


@pytest.mark.parametrize("clickhouse_type", sorted(FAST_PATH_BY_TYPE))
@pytest.mark.parametrize("nullable", [False, True])
def test_the_fast_path_covers_the_types_it_claims(clickhouse_type: str, nullable: bool) -> None:
    """Ровно на каких типах живёт ускорение — и что байты при этом те же."""
    samples = {
        "String": ["abc", "007"],
        "Int64": ["42", "-7"],
        "UInt64": ["42", "0"],
        "Float64": ["1.5", "0"],
        "Decimal(18, 2)": ["1.50", "0"],
        "Decimal(38, 10)": ["1.5", "0"],
        "Date": ["2024-01-02", "2024-01-03"],
        "DateTime": ["2024-01-02 03:04:05", "2024-01-03 00:00:00"],
        "Bool": ["true", "false"],
    }
    values = list(samples[clickhouse_type])
    final_type = f"Nullable({clickhouse_type})" if nullable else clickhouse_type
    if nullable:
        values[-1] = ""
    converted = convert_chunk_to_schema(
        pd.DataFrame({"col": values}, dtype="object"),
        [SchemaMapping("col", "col", True, final_type, nullable)],
        chunk_number=1,
    )

    fast = pandas_loader._arrow_json_lines(converted, ["col"])

    assert (fast is not None) is FAST_PATH_BY_TYPE[clickhouse_type]
    assert chunk_to_json_lines(converted, ["col"]) == pandas_loader._pandas_json_lines(converted, ["col"])


OBJECT_PEEK_CASES: dict[str, tuple[list[object], bool]] = {
    "строки": (["a", "b"], True),
    "строки с пропусками впереди": ([None, None, "a"], True),
    "python int": ([1, 2], True),
    "python bool": ([True, False], True),
    "numpy bool": ([np.True_, np.False_], True),
    "Decimal": ([Decimal("1.50")], False),
    "float": ([1.5], False),
    "только пропуски": ([None, None], True),
    "bytes": ([b"a"], False),
    "вложенный список": ([["a"]], False),
}


@pytest.mark.parametrize("case", sorted(OBJECT_PEEK_CASES))
def test_object_columns_are_refused_by_value_type_before_any_conversion(case: str) -> None:
    """Отказ обязан быть ДЕШЁВЫМ, иначе быстрый путь хуже своего отсутствия.

    Замерено: `pa.Array.from_pandas` на Decimal-объектах стоит 0,855 мкс/строку,
    и кадр с одной такой колонкой был в 2,15 раза МЕДЛЕННЕЕ, чем до правки —
    конвертация делалась только чтобы её выбросить. Решение принимается по типу
    первых значений; `True` здесь ничего не обещает, тип проверяется и после.
    """
    values, supported = OBJECT_PEEK_CASES[case]

    assert pandas_loader._arrow_supports_object_values(pd.Series(values, dtype="object")) is supported


def test_a_decimal_column_does_not_drag_the_whole_chunk_below_the_reference() -> None:
    """Кадр, который быстрый путь отказывает, не должен стать медленнее эталона.

    Проверяется поведением, а не секундами: Decimal-колонка обязана отсеяться
    решением по типу, то есть до конвертации всего кадра в Arrow.
    """
    frame = pd.DataFrame(
        {
            "nmid": pd.array([1, 2], dtype="UInt64"),
            "amt": pd.Series([Decimal("1.50"), Decimal("2")], dtype="object"),
        }
    )

    assert pandas_loader._arrow_supports_object_values(frame["amt"]) is False
    assert pandas_loader._arrow_json_lines(frame, ["nmid", "amt"]) is None
    assert chunk_to_json_lines(frame, ["nmid", "amt"]) == pandas_loader._pandas_json_lines(
        frame, ["nmid", "amt"]
    )


FALLBACK_CASES: dict[str, pd.Series] = {
    "float — формат не доказан": pd.Series(pd.array([1.5, 1e20], dtype="Float64")),
    "Decimal — объекты": pd.Series([Decimal("1.50"), Decimal("2")], dtype="object"),
    "управляющий символ": pd.Series(["a\x01b"], dtype="object"),
    "перевод строки": pd.Series(["a\nb"], dtype="object"),
    "табуляция": pd.Series(["a\tb"], dtype="object"),
    "смешанные объекты": pd.Series(["a", 1], dtype="object"),
}


@pytest.mark.parametrize("case", sorted(FALLBACK_CASES))
def test_the_arrow_path_refuses_what_it_cannot_promise(case: str) -> None:
    """Отказ, а не приблизительный ответ: молча разойтись с эталоном нельзя."""
    frame = pd.DataFrame({"col": FALLBACK_CASES[case]})

    assert pandas_loader._arrow_json_lines(frame, ["col"]) is None


@pytest.mark.parametrize("case", sorted(FALLBACK_CASES))
def test_a_refused_chunk_still_serializes_through_to_json(case: str) -> None:
    frame = pd.DataFrame({"col": FALLBACK_CASES[case]})

    assert chunk_to_json_lines(frame, ["col"]) == pandas_loader._pandas_json_lines(frame, ["col"])


def test_a_mixed_frame_falls_back_as_a_whole_when_one_column_is_unsafe() -> None:
    """Одна колонка вне быстрого пути — весь чанк идёт эталоном.

    Собирать строку из частей разных путей нельзя: расхождение в одной колонке
    испортило бы весь блок, а не одно поле.
    """
    frame = pd.DataFrame(
        {
            "nmid": pd.array([1, 2], dtype="UInt64"),
            "amount": pd.array([1.5, 2.5], dtype="Float64"),
        }
    )
    columns = ["nmid", "amount"]

    assert pandas_loader._arrow_json_lines(frame, columns) is None
    assert chunk_to_json_lines(frame, columns) == pandas_loader._pandas_json_lines(frame, columns)
