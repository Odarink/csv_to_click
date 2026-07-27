"""Бенчмарк сериализации в JSONEachRow: построчный путь против векторного.

Меряется то, что реально вызывает приложение, — `iter_json_each_row_payloads`
на настройках по умолчанию (batch_size 100 000, лимит 16 МБ x 0.9 = 14.4 МБ).
Сравнивать только `chunk_to_json_lines` нельзя: он совпадает с рабочим путём
лишь когда чанк целиком влезает в лимит, а любая таблица шире ~150 байт/строку
при этих настройках идёт через нарезку.

Критерий приёмки фазы 3: векторный путь **не менее чем в 3 раза быстрее**
построчного на профиле оператора И даёт идентичный набор распарсенных
JSON-объектов на всех профилях. Сравниваются распарсенные объекты, а не байты:
`to_json` не ставит пробел после двоеточия, и это не расхождение по данным.

Построчные реализации ниже — копии удалённого из `pandas_loader` кода, включая
преобразование временных колонок, которое фаза 3 тоже переписала. Иначе работа,
переехавшая между стадиями, выпала бы из измерения и завысила бы результат.

Запуск из корня репозитория:

    .venv\\Scripts\\python.exe benchmarks\\serialize_bench.py [строк] [повторов]
"""

from __future__ import annotations

import json
import sys
import time
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

from csv_click.pandas_loader import (  # noqa: E402
    SchemaMapping,
    convert_chunk_to_schema,
    iter_json_each_row_payloads,
)
from csv_click.schema import unwrap_nullable  # noqa: E402


APP_DEFAULT_LIMIT_BYTES = int(16 * 1024 * 1024 * 0.9)
APP_DEFAULT_BATCH_ROWS = 100_000


# --------------------------------------------------------------------------
# Копии удалённого кода: путь до фазы 3, целиком.
# --------------------------------------------------------------------------

def legacy_clean_json_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        as_float = float(value)
        return int(as_float) if as_float.is_integer() else as_float
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def legacy_convert_temporal(series: pd.Series, clickhouse_type: str) -> pd.Series | None:
    """Как HEAD готовил Date/DateTime: объекты, а не строки."""
    _, inner = unwrap_nullable(clickhouse_type)
    if inner == "Date":
        converted = pd.to_datetime(series, errors="raise").dt.date
        return converted.map(lambda value: None if pd.isna(value) else value).astype("object")
    if inner == "DateTime":
        converted = pd.to_datetime(series, errors="raise")
        return converted.map(
            lambda value: None if pd.isna(value) else value.to_pydatetime()
        ).astype("object")
    return None


def legacy_convert(frame: pd.DataFrame, mappings: list[SchemaMapping]) -> pd.DataFrame:
    converted = convert_chunk_to_schema(frame, mappings, chunk_number=1)
    for mapping in mappings:
        temporal = legacy_convert_temporal(frame[mapping.source_name], mapping.final_type)
        if temporal is not None:
            converted[mapping.target_name] = temporal
    return converted


def legacy_pack(
    chunk: pd.DataFrame, columns: list[str], max_payload_bytes: int
) -> Iterator[tuple[bytes, int]]:
    """Жадный построчный упаковщик из HEAD."""
    lines: list[bytes] = []
    payload_bytes = 0
    rows_count = 0
    for values in chunk[columns].itertuples(index=False, name=None):
        row = dict(zip(columns, values))
        cleaned = {column: legacy_clean_json_value(row[column]) for column in columns}
        line = json.dumps(cleaned, ensure_ascii=False, allow_nan=False).encode("utf-8")
        size = len(line)
        if size > max_payload_bytes:
            raise ValueError(f"single row is {size} bytes")
        separator = 1 if rows_count else 0
        if rows_count and payload_bytes + separator + size > max_payload_bytes:
            yield b"\n".join(lines), rows_count
            lines, payload_bytes, rows_count, separator = [], 0, 0, 0
        lines.append(line)
        payload_bytes += separator + size
        rows_count += 1
    if rows_count:
        yield b"\n".join(lines), rows_count


# --------------------------------------------------------------------------
# Профили
# --------------------------------------------------------------------------

def build_profiles(rows: int) -> dict[str, tuple[list[SchemaMapping], dict[str, list[str]]]]:
    profiles: dict[str, tuple[list[SchemaMapping], dict[str, list[str]]]] = {}

    profiles["1 колонка, 11 символов (профиль оператора)"] = (
        [SchemaMapping("nmid", "nmid", True, "String", False)],
        {"nmid": [f"{index % 100000000000:011d}" for index in range(rows)]},
    )
    profiles["1 колонка UInt64"] = (
        [SchemaMapping("nmid", "nmid", True, "UInt64", False)],
        {"nmid": [str(index) for index in range(rows)]},
    )

    wide_mappings, wide_columns = [], {}
    for index in range(20):
        name = f"c{index:02d}"
        wide_mappings.append(SchemaMapping(name, name, True, "String", False))
        wide_columns[name] = [f"value-{index}-{row}" for row in range(rows)]
    profiles["20 колонок String (~450 Б/строку)"] = (wide_mappings, wide_columns)

    profiles["смешанный: String, UInt64, Float64, DateTime, Decimal"] = (
        [
            SchemaMapping("name", "name", True, "String", False),
            SchemaMapping("qty", "qty", True, "UInt64", False),
            SchemaMapping("price", "price", True, "Float64", False),
            SchemaMapping("at", "at", True, "DateTime", False),
            SchemaMapping("amount", "amount", True, "Decimal(18, 2)", False),
        ],
        {
            "name": [f"наименование-{row}" for row in range(rows)],
            "qty": [str(row % 1000) for row in range(rows)],
            "price": [f"{row % 10000}.25" for row in range(rows)],
            "at": ["2024-01-02 03:04:05"] * rows,
            "amount": [f"{row % 100000}.99" for row in range(rows)],
        },
    )

    # Неблагоприятный для вектора профиль: широкие значения, где выигрыш от
    # снятия построчных расходов размывается объёмом самих байт.
    profiles["широкие значения, 2000 символов"] = (
        [SchemaMapping("blob", "blob", True, "String", False)],
        {"blob": ["b" * 2000] * rows},
    )
    # Разнородные строки: здесь оценка размера блока обязана и расти, и падать.
    profiles["разнородные: 99% по 12 Б, 1% по 100 КБ"] = (
        [SchemaMapping("col", "col", True, "String", False)],
        {"col": [("F" * 100_000 if index % 100 == 0 else "t" * 12) for index in range(rows)]},
    )
    return profiles


# --------------------------------------------------------------------------
# Измерение и сверка
# --------------------------------------------------------------------------

def measure(run: Callable[[], list[tuple[bytes, int]]], repeats: int) -> tuple[float, list[tuple[bytes, int]]]:
    best = float("inf")
    blocks: list[tuple[bytes, int]] = []
    for _ in range(repeats):
        started = time.perf_counter()
        blocks = run()
        best = min(best, time.perf_counter() - started)
    return best, blocks


def parsed_objects(blocks: list[tuple[bytes, int]]) -> list[dict[str, object]]:
    """Разбор по РЕАЛЬНОМУ разделителю записей.

    `str.splitlines()` здесь нельзя: он делит ещё и по U+2028, U+2029, \\x0b,
    \\x0c и \\x85, которых ни один из путей не экранирует, и сверка падала бы на
    данных, которые оба сериализатора кодируют одинаково.
    """
    objects: list[dict[str, object]] = []
    for payload, _ in blocks:
        if not payload:
            continue
        for line in payload.split(b"\n"):
            objects.append(json.loads(line.decode("utf-8")))
    return objects


def normalise_legacy(objects: list[dict[str, object]], temporal_columns: set[str]) -> list[dict[str, object]]:
    """Единственное задокументированное расхождение: 'T' против пробела.

    Правится ТОЛЬКО в колонках, объявленных как Date/DateTime, и только в
    позиции 10. Иначе строковая колонка с настоящей ISO-меткой, которую оба
    пути кодируют одинаково, объявлялась бы расхождением.
    """
    if not temporal_columns:
        return objects
    fixed = []
    for row in objects:
        row = dict(row)
        for column in temporal_columns:
            value = row.get(column)
            if isinstance(value, str) and len(value) > 10 and value[10] == "T":
                row[column] = value[:10] + " " + value[11:]
        fixed.append(row)
    return fixed


def canonical(objects: list[dict[str, object]]) -> str:
    """Строгая сверка: json.dumps различает -0.0 и 0.0, а == нет."""
    return "\n".join(json.dumps(row, sort_keys=True, ensure_ascii=False) for row in objects)


def main() -> int:
    rows = int(sys.argv[1]) if len(sys.argv) > 1 else APP_DEFAULT_BATCH_ROWS
    repeats = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    limit = APP_DEFAULT_LIMIT_BYTES

    print(f"Строк на профиль: {rows:,}, повторов: {repeats} (берётся лучшее время)")
    print(f"Лимит блока: {limit / 1024 / 1024:.2f} МБ (значение приложения по умолчанию)")
    print("Меряется iter_json_each_row_payloads — то, что вызывает приложение.")
    print()
    header = (
        f"{'профиль':<48} {'построчно':>11} {'вектор':>11} {'ускор':>7} "
        f"{'блоков':>13} {'данные':>9}"
    )
    print(header)
    print("-" * len(header))

    operator_speedup = 0.0
    all_identical = True

    for name, (mappings, columns) in build_profiles(rows).items():
        frame = pd.DataFrame(columns, dtype="object")
        temporal = {
            mapping.target_name
            for mapping in mappings
            if unwrap_nullable(mapping.final_type)[1] in {"Date", "DateTime"}
        }
        new_frame = convert_chunk_to_schema(frame, mappings, chunk_number=1)
        old_frame = legacy_convert(frame, mappings)
        names = list(new_frame.columns)

        legacy_s, legacy_blocks = measure(lambda: list(legacy_pack(old_frame, names, limit)), repeats)
        vector_s, vector_blocks = measure(
            lambda: list(iter_json_each_row_payloads(new_frame, names, max_payload_bytes=limit)),
            repeats,
        )

        identical = canonical(normalise_legacy(parsed_objects(legacy_blocks), temporal)) == canonical(
            parsed_objects(vector_blocks)
        )
        all_identical = all_identical and identical
        speedup = legacy_s / vector_s if vector_s else float("inf")
        if name.startswith("1 колонка, 11"):
            operator_speedup = speedup

        print(
            f"{name:<48} {legacy_s:>10.3f}s {vector_s:>10.3f}s {speedup:>6.2f}x "
            f"{len(legacy_blocks):>5} -> {len(vector_blocks):<4} "
            f"{'совпали' if identical else 'РАЗОШЛИСЬ':>9}"
        )

    print()
    print(f"Профиль оператора: {operator_speedup:.2f}x")
    print(f"Распарсенные объекты идентичны везде: {'да' if all_identical else 'НЕТ'}")
    print()
    print("Замечание: выигрыш падает с ростом ширины значения — построчные расходы,")
    print("которые снимает вектор, постоянны на строку, а объём байт растёт. На")
    print("значениях в тысячи символов пути сходятся; профиль оператора - 20 Б/строку.")
    print()

    accepted = all_identical and operator_speedup >= 3.0
    print("КРИТЕРИЙ ПРИЁМКИ ФАЗЫ 3:", "ВЫПОЛНЕН" if accepted else "НЕ ВЫПОЛНЕН")
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
