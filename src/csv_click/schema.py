from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable

from csv_click.errors import CsvSchemaError


CLICKHOUSE_TYPE_OPTIONS = [
    "String",
    "Int64",
    "UInt64",
    "Float64",
    "Decimal(18, 2)",
    "Decimal(38, 10)",
    "Date",
    "DateTime",
    "Bool",
    "Nullable(String)",
    "Nullable(Int64)",
    "Nullable(UInt64)",
    "Nullable(Float64)",
    "Nullable(Decimal(18, 2))",
    "Nullable(Decimal(38, 10))",
    "Nullable(Date)",
    "Nullable(DateTime)",
    "Nullable(Bool)",
]


def validate_clickhouse_type_expression(clickhouse_type: str) -> str:
    normalized = clickhouse_type.strip()
    if not normalized:
        raise CsvSchemaError("ClickHouse type cannot be empty")
    if any(token in normalized for token in [";", "--", "/*", "*/", "\n", "\r"]):
        raise CsvSchemaError(f"Unsafe ClickHouse type expression: {clickhouse_type}")
    if not re.match(r"[A-Za-z]", normalized):
        raise CsvSchemaError(f"Unsafe ClickHouse type expression: {clickhouse_type}")
    if _has_top_level_comma(normalized):
        raise CsvSchemaError(f"Unsafe ClickHouse type expression: {clickhouse_type}")
    if not _has_balanced_parentheses(normalized):
        raise CsvSchemaError(f"Unbalanced ClickHouse type expression: {clickhouse_type}")
    return normalized


def _has_balanced_parentheses(value: str) -> bool:
    depth = 0
    quote: str | None = None
    for char in value:
        if char in {"'", '"'}:
            quote = None if quote == char else char if quote is None else quote
            continue
        if quote:
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0 and quote is None


def _has_top_level_comma(value: str) -> bool:
    depth = 0
    quote: str | None = None
    for char in value:
        if char in {"'", '"'}:
            quote = None if quote == char else char if quote is None else quote
            continue
        if quote:
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            return True
    return False


@dataclass
class CsvColumn:
    column_name: str
    source_name: str
    inferred_type: str
    final_type: str
    nullable: bool
    sample_values: list[str]
    notes: str = ""


@dataclass
class CsvSchema:
    columns: list[CsvColumn]

    @property
    def source_names(self) -> list[str]:
        return [column.source_name for column in self.columns]

    @property
    def column_names(self) -> list[str]:
        return [column.column_name for column in self.columns]


@dataclass
class _ColumnStats:
    has_empty: bool = False
    total_non_empty: int = 0
    all_bool: bool = True
    has_explicit_bool_literal: bool = False
    all_int: bool = True
    all_uint: bool = True
    all_decimal: bool = True
    all_float: bool = True
    all_date: bool = True
    all_datetime: bool = True
    max_decimal_scale: int = 0
    sample_values: list[str] | None = None

    def add_value(self, value: str) -> None:
        raw = value.strip()
        if self.sample_values is None:
            self.sample_values = []
        if raw and len(self.sample_values) < 5 and raw not in self.sample_values:
            self.sample_values.append(raw)
        if raw == "":
            self.has_empty = True
            return

        self.total_non_empty += 1
        self.all_bool = self.all_bool and _is_bool(raw)
        self.has_explicit_bool_literal = self.has_explicit_bool_literal or raw.lower() in {
            "true",
            "false",
            "yes",
            "no",
            "y",
            "n",
        }
        self.all_int = self.all_int and _is_int(raw)
        self.all_uint = self.all_uint and _is_uint(raw)
        decimal_scale = _decimal_scale(raw)
        self.all_decimal = self.all_decimal and decimal_scale is not None
        self.all_float = self.all_float and _is_float(raw)
        self.all_date = self.all_date and _is_date(raw)
        self.all_datetime = self.all_datetime and _is_datetime(raw)
        if decimal_scale is not None:
            self.max_decimal_scale = max(self.max_decimal_scale, decimal_scale)


def normalize_identifier(value: str) -> str:
    normalized = re.sub(r"[^0-9a-zA-Z_]+", "_", value.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        raise CsvSchemaError("CSV header contains an empty column name")
    if normalized[0].isdigit():
        normalized = f"col_{normalized}"
    return normalized


def analyze_csv_schema(csv_path: str | Path, delimiter: str | None = None) -> CsvSchema:
    path = Path(csv_path)
    dialect = _detect_dialect(path, delimiter)
    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file, dialect=dialect)
        if not reader.fieldnames:
            raise CsvSchemaError("CSV header is required")

        normalized_names = [normalize_identifier(name) for name in reader.fieldnames]
        duplicates = _duplicates(normalized_names)
        if duplicates:
            raise CsvSchemaError(
                "CSV header contains duplicate column names after normalization: "
                + ", ".join(sorted(duplicates))
            )

        stats = {name: _ColumnStats() for name in reader.fieldnames}
        for row in reader:
            for source_name in reader.fieldnames:
                stats[source_name].add_value(row.get(source_name, ""))

    columns: list[CsvColumn] = []
    for source_name, column_name in zip(reader.fieldnames, normalized_names, strict=True):
        inferred_type, notes = _infer_type(stats[source_name])
        final_type = _with_nullable(inferred_type, stats[source_name].has_empty)
        columns.append(
            CsvColumn(
                column_name=column_name,
                source_name=source_name,
                inferred_type=inferred_type,
                final_type=final_type,
                nullable=stats[source_name].has_empty,
                sample_values=stats[source_name].sample_values or [],
                notes=notes,
            )
        )
    return CsvSchema(columns=columns)


def validate_csv_against_schema(
    csv_path: str | Path,
    schema: CsvSchema,
    delimiter: str | None = None,
) -> int:
    path = Path(csv_path)
    dialect = _detect_dialect(path, delimiter)
    rows_count = 0
    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file, dialect=dialect)
        if reader.fieldnames != schema.source_names:
            raise CsvSchemaError("CSV header changed after schema analysis")
        for row_num, row in enumerate(reader, start=2):
            rows_count += 1
            for column in schema.columns:
                raw_value = row.get(column.source_name, "")
                try:
                    convert_value(raw_value, column.final_type)
                except CsvSchemaError as exc:
                    raise CsvSchemaError(
                        f"Cannot convert row {row_num}, column '{column.source_name}', "
                        f"value '{raw_value}' to {column.final_type}: {exc}"
                    ) from exc
    return rows_count


def convert_value(value: str, clickhouse_type: str) -> object:
    raw = value.strip()
    nullable, inner_type = unwrap_nullable(clickhouse_type)
    if raw == "":
        if nullable:
            return None
        if inner_type == "String":
            return ""
        raise CsvSchemaError("empty value is not allowed for non-nullable type")

    if inner_type == "String":
        return value
    if inner_type == "Int64":
        if not _is_int(raw):
            raise CsvSchemaError("expected Int64")
        return int(raw)
    if inner_type == "UInt64":
        if not _is_uint(raw):
            raise CsvSchemaError("expected UInt64")
        return int(raw)
    if inner_type == "Float64":
        if not _is_float(raw):
            raise CsvSchemaError("expected Float64")
        return float(raw)
    if inner_type.startswith("Decimal("):
        try:
            return Decimal(raw)
        except InvalidOperation as exc:
            raise CsvSchemaError("expected Decimal") from exc
    if inner_type == "Date":
        try:
            return date.fromisoformat(raw)
        except ValueError as exc:
            raise CsvSchemaError("expected Date in YYYY-MM-DD format") from exc
    if inner_type == "DateTime":
        try:
            return datetime.fromisoformat(raw)
        except ValueError as exc:
            raise CsvSchemaError("expected DateTime") from exc
    if inner_type == "Bool":
        if not _is_bool(raw):
            raise CsvSchemaError("expected Bool")
        return raw.lower() in {"true", "1", "yes", "y"}
    validate_clickhouse_type_expression(clickhouse_type)
    return value


def unwrap_nullable(clickhouse_type: str) -> tuple[bool, str]:
    if clickhouse_type.startswith("Nullable(") and clickhouse_type.endswith(")"):
        return True, clickhouse_type.removeprefix("Nullable(").removesuffix(")")
    return False, clickhouse_type


def schema_from_editor_rows(rows: Iterable[dict[str, object]]) -> CsvSchema:
    columns = []
    for row in rows:
        custom_type = str(row.get("custom_type") or "").strip()
        final_type = custom_type or str(row["final_type"])
        nullable = bool(row.get("nullable", final_type.startswith("Nullable(")))
        if nullable and not final_type.startswith("Nullable("):
            final_type = f"Nullable({final_type})"
        if not nullable and final_type.startswith("Nullable("):
            final_type = final_type.removeprefix("Nullable(").removesuffix(")")
        final_type = validate_clickhouse_type_expression(final_type)
        columns.append(
            CsvColumn(
                column_name=str(row["column_name"]),
                source_name=str(row.get("source_name") or row["column_name"]),
                inferred_type=str(row["inferred_type"]),
                final_type=final_type,
                nullable=final_type.startswith("Nullable("),
                sample_values=_split_sample_values(row.get("sample_values", "")),
                notes=str(row.get("notes", "")),
            )
        )
    return CsvSchema(columns=columns)


def schema_to_editor_rows(schema: CsvSchema) -> list[dict[str, object]]:
    return [
        {
            "column_name": column.column_name,
            "source_name": column.source_name,
            "inferred_type": column.inferred_type,
            "final_type": column.final_type,
            "custom_type": "",
            "nullable": column.final_type.startswith("Nullable("),
            "sample_values": ", ".join(column.sample_values),
            "notes": column.notes,
        }
        for column in schema.columns
    ]


def _infer_type(stats: _ColumnStats) -> tuple[str, str]:
    if stats.total_non_empty == 0:
        return "String", "All values are empty; fallback to String"
    if stats.all_bool and stats.has_explicit_bool_literal:
        return "Bool", ""
    if stats.all_uint:
        return "UInt64", ""
    if stats.all_int:
        return "Int64", ""
    if stats.all_decimal:
        if stats.max_decimal_scale <= 2:
            return "Decimal(18, 2)", ""
        if stats.max_decimal_scale <= 10:
            return "Decimal(38, 10)", ""
        return "Float64", "Decimal scale is too high; fallback to Float64"
    if stats.all_date:
        return "Date", ""
    if stats.all_datetime:
        return "DateTime", ""
    if stats.all_float:
        return "Float64", ""
    return "String", "Mixed or unsupported values; fallback to String"


def _with_nullable(clickhouse_type: str, nullable: bool) -> str:
    return f"Nullable({clickhouse_type})" if nullable else clickhouse_type


def _detect_dialect(path: Path, delimiter: str | None = None) -> csv.Dialect:
    if delimiter:
        return _dialect_for_delimiter(delimiter)
    sample = path.read_text(encoding="utf-8-sig")[:8192]
    if not sample:
        raise CsvSchemaError("CSV header is required")
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        return csv.get_dialect("excel")


def _dialect_for_delimiter(delimiter: str) -> csv.Dialect:
    class CustomDialect(csv.excel):
        pass

    CustomDialect.delimiter = delimiter
    return CustomDialect


def _duplicates(values: list[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return duplicates


def _is_bool(value: str) -> bool:
    return value.lower() in {"true", "false", "1", "0", "yes", "no", "y", "n"}


def _is_int(value: str) -> bool:
    return re.fullmatch(r"[+-]?\d+", value) is not None


def _is_uint(value: str) -> bool:
    return re.fullmatch(r"\+?\d+", value) is not None


def _decimal_scale(value: str) -> int | None:
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        return None
    exponent = parsed.as_tuple().exponent
    return abs(exponent) if exponent < 0 else 0


def _is_float(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True


def _is_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def _is_datetime(value: str) -> bool:
    try:
        datetime.fromisoformat(value)
    except ValueError:
        return False
    return True


def _split_sample_values(value: object) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]
