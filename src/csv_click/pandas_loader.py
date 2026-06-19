from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd

from csv_click.clickhouse import raw_insert_batch
from csv_click.errors import CsvSchemaError
from csv_click.schema import (
    CsvColumn,
    CsvSchema,
    _ColumnStats,
    _infer_type,
    convert_value,
    normalize_identifier,
    unwrap_nullable,
    validate_clickhouse_type_expression,
)


@dataclass(frozen=True)
class ReadOptions:
    separator: str = ","
    encoding: str = "utf_8"
    batch_size: int = 1_000_000


ENCODING_SUGGESTIONS: tuple[str, ...] = ("utf_8", "utf-8-sig", "cp1251", "windows-1251")
MOJIBAKE_MARKERS: tuple[str, ...] = ("С‚", "Рµ", "Р°", "Рё", "Рѕ", "РЅ", "�")


@dataclass(frozen=True)
class MojibakeWarning:
    message: str
    suggested_encodings: tuple[str, ...]


@dataclass(frozen=True)
class SchemaMapping:
    source_name: str
    target_name: str
    include: bool
    final_type: str
    nullable: bool
    inferred_type: str | None = None
    sample_values: tuple[str, ...] = ()
    notes: str = ""


def iter_pandas_chunks(
    csv_path: str | Path,
    read_options: ReadOptions,
    usecols: list[str] | None = None,
) -> Iterator[pd.DataFrame]:
    if read_options.batch_size <= 0:
        raise ValueError("batch_size must be positive")

    try:
        reader = pd.read_csv(
            csv_path,
            sep=read_options.separator,
            encoding=read_options.encoding,
            chunksize=read_options.batch_size,
            usecols=usecols,
        )
        for chunk in reader:
            chunk.columns = chunk.columns.str.strip()
            yield chunk
    except pd.errors.ParserError as exc:
        raise CsvSchemaError(
            "Cannot parse CSV with "
            f"separator {read_options.separator!r} and encoding {read_options.encoding}: {exc}"
        ) from exc


def preview_csv_rows(
    csv_path: str | Path,
    read_options: ReadOptions,
    nrows: int = 20,
) -> pd.DataFrame:
    try:
        preview = pd.read_csv(
            csv_path,
            sep=read_options.separator,
            encoding=read_options.encoding,
            nrows=nrows,
            dtype=str,
            keep_default_na=False,
        )
    except pd.errors.ParserError as exc:
        raise CsvSchemaError(
            "Cannot parse CSV preview with "
            f"separator {read_options.separator!r} and encoding {read_options.encoding}: {exc}"
        ) from exc
    except UnicodeDecodeError as exc:
        raise CsvSchemaError(
            f"Cannot decode CSV preview with encoding {read_options.encoding}: {exc}. "
            "Try cp1251 or windows-1251 for Windows Cyrillic CSV files."
        ) from exc
    preview.columns = preview.columns.str.strip()
    return preview


def detect_mojibake(preview: pd.DataFrame) -> MojibakeWarning | None:
    for value in preview.astype(str).to_numpy().ravel().tolist():
        if any(marker in value for marker in MOJIBAKE_MARKERS):
            return MojibakeWarning(
                message=(
                    "CSV preview may contain mojibake. Try another encoding: "
                    + ", ".join(ENCODING_SUGGESTIONS)
                ),
                suggested_encodings=ENCODING_SUGGESTIONS,
            )
    return None


def analyze_csv_with_pandas_chunks(
    csv_path: str | Path,
    read_options: ReadOptions,
) -> CsvSchema:
    stats: dict[str, _ColumnStats] = {}
    source_names: list[str] | None = None

    try:
        for chunk in iter_pandas_chunks(csv_path, read_options):
            if source_names is None:
                source_names = list(chunk.columns)
                target_names = [normalize_identifier(name) for name in source_names]
                duplicates = _duplicates(target_names)
                if duplicates:
                    raise CsvSchemaError(
                        "CSV header contains duplicate column names after normalization: "
                        + ", ".join(sorted(duplicates))
                    )
                stats = {name: _ColumnStats() for name in source_names}

            for source_name in source_names:
                for value in chunk[source_name].tolist():
                    stats[source_name].add_value(_value_to_string(value))
    except pd.errors.EmptyDataError as exc:
        raise CsvSchemaError("CSV header is required") from exc
    except UnicodeDecodeError as exc:
        raise CsvSchemaError(f"Cannot decode CSV with encoding {read_options.encoding}: {exc}") from exc

    if source_names is None:
        raise CsvSchemaError("CSV header is required")

    columns = []
    for source_name in source_names:
        inferred_type, notes = _infer_type(stats[source_name])
        final_type = f"Nullable({inferred_type})" if stats[source_name].has_empty else inferred_type
        columns.append(
            CsvColumn(
                column_name=normalize_identifier(source_name),
                source_name=source_name,
                inferred_type=inferred_type,
                final_type=final_type,
                nullable=final_type.startswith("Nullable("),
                sample_values=stats[source_name].sample_values or [],
                notes=notes,
            )
        )
    return CsvSchema(columns=columns)


def schema_to_mappings(schema: CsvSchema) -> list[SchemaMapping]:
    return [
        SchemaMapping(
            source_name=column.source_name,
            target_name=column.column_name,
            include=True,
            final_type=column.final_type,
            nullable=column.final_type.startswith("Nullable("),
            inferred_type=column.inferred_type,
            sample_values=tuple(column.sample_values),
            notes=column.notes,
        )
        for column in schema.columns
    ]


def mappings_to_schema(mappings: list[SchemaMapping]) -> CsvSchema:
    columns = []
    target_names = [mapping.target_name for mapping in mappings if mapping.include]
    duplicates = _duplicates(target_names)
    if duplicates:
        raise CsvSchemaError("Duplicate target column names: " + ", ".join(sorted(duplicates)))

    for mapping in mappings:
        if not mapping.include:
            continue
        target_name = mapping.target_name.strip()
        if not target_name:
            raise CsvSchemaError("Target column name cannot be empty")
        final_type = _normalize_nullable_type(mapping.final_type, mapping.nullable)
        final_type = validate_clickhouse_type_expression(final_type)
        columns.append(
            CsvColumn(
                column_name=target_name,
                source_name=mapping.source_name,
                inferred_type=mapping.inferred_type or final_type,
                final_type=final_type,
                nullable=final_type.startswith("Nullable("),
                sample_values=list(mapping.sample_values),
                notes=mapping.notes,
            )
        )
    if not columns:
        raise CsvSchemaError("At least one column must be included")
    return CsvSchema(columns=columns)


def mappings_to_editor_rows(mappings: list[SchemaMapping]) -> list[dict[str, object]]:
    return [
        {
            "source_name": mapping.source_name,
            "target_name": mapping.target_name,
            "include": mapping.include,
            "inferred_type": mapping.inferred_type or mapping.final_type,
            "final_type": mapping.final_type,
            "custom_type": "",
            "nullable": mapping.final_type.startswith("Nullable("),
            "sample_values": ", ".join(mapping.sample_values),
            "notes": mapping.notes,
        }
        for mapping in mappings
    ]


def mappings_from_editor_rows(rows: list[dict[str, object]]) -> list[SchemaMapping]:
    mappings = []
    for row in rows:
        custom_type = str(row.get("custom_type") or "").strip()
        selected_type = custom_type or str(row["final_type"])
        final_type = _normalize_nullable_type(selected_type, bool(row.get("nullable", False)))
        final_type = validate_clickhouse_type_expression(final_type)
        mappings.append(
            SchemaMapping(
                source_name=str(row["source_name"]),
                target_name=str(row["target_name"]),
                include=bool(row.get("include", True)),
                final_type=final_type,
                nullable=final_type.startswith("Nullable("),
                inferred_type=str(row.get("inferred_type") or final_type),
                sample_values=tuple(_split_sample_values(row.get("sample_values", ""))),
                notes=str(row.get("notes", "")),
            )
        )
    return mappings


def convert_chunk_to_schema(
    chunk: pd.DataFrame,
    mappings: list[SchemaMapping],
    chunk_number: int,
) -> pd.DataFrame:
    output = pd.DataFrame(index=chunk.index)
    for mapping in mappings:
        if not mapping.include:
            continue
        if mapping.source_name not in chunk.columns:
            raise CsvSchemaError(f"Column '{mapping.source_name}' not found in chunk {chunk_number}")
        target_name = mapping.target_name.strip()
        if not target_name:
            raise CsvSchemaError("Target column name cannot be empty")
        try:
            output[target_name] = _convert_series(chunk[mapping.source_name], mapping.final_type)
        except (CsvSchemaError, ValueError, TypeError) as exc:
            bad_value = _first_bad_value(chunk[mapping.source_name], mapping.final_type)
            raise CsvSchemaError(
                f"Cannot convert chunk {chunk_number}, column '{mapping.source_name}', "
                f"value '{bad_value}' to {mapping.final_type}: {exc}"
            ) from exc
    return output.reset_index(drop=True)


def validate_csv_with_pandas_chunks(
    csv_path: str | Path,
    read_options: ReadOptions,
    mappings: list[SchemaMapping],
) -> int:
    rows_count = 0
    usecols = [mapping.source_name for mapping in mappings if mapping.include]
    for chunk_number, chunk in enumerate(iter_pandas_chunks(csv_path, read_options, usecols), start=1):
        converted = convert_chunk_to_schema(chunk, mappings, chunk_number)
        chunk_to_json_each_row_payload(converted, list(converted.columns))
        rows_count += len(converted)
    return rows_count


def chunk_to_json_each_row_payload(chunk: pd.DataFrame, columns: list[str]) -> bytes:
    lines = []
    for row in chunk[columns].to_dict(orient="records"):
        cleaned = {column: _clean_json_value(row[column]) for column in columns}
        lines.append(json.dumps(cleaned, ensure_ascii=False, allow_nan=False))
    return "\n".join(lines).encode("utf-8")


def load_csv_via_raw_insert(
    client,
    csv_path: str | Path,
    read_options: ReadOptions,
    database: str,
    table: str,
    mappings: list[SchemaMapping],
    progress_callback=None,
) -> int:
    rows_count = 0
    usecols = [mapping.source_name for mapping in mappings if mapping.include]
    for chunk_number, chunk in enumerate(iter_pandas_chunks(csv_path, read_options, usecols), start=1):
        converted = convert_chunk_to_schema(chunk, mappings, chunk_number)
        columns = list(converted.columns)
        payload = chunk_to_json_each_row_payload(converted, columns)
        raw_insert_batch(client, database, table, columns, payload)
        rows_count += len(converted)
        if progress_callback:
            progress_callback(chunk_number, len(converted), rows_count)
    return rows_count


def _convert_series(series: pd.Series, clickhouse_type: str) -> pd.Series:
    nullable, inner_type = unwrap_nullable(clickhouse_type)
    if inner_type == "String":
        return series.map(lambda value: None if pd.isna(value) and nullable else str(value))
    if inner_type in {"Int64", "UInt64"}:
        converted = pd.to_numeric(series, errors="raise")
        if not nullable and converted.isna().any():
            raise CsvSchemaError("empty value is not allowed for non-nullable integer")
        return pd.Series(
            [None if pd.isna(value) else int(value) for value in converted.tolist()],
            index=series.index,
            dtype="object",
        )
    if inner_type == "Float64":
        converted = pd.to_numeric(series, errors="raise")
        if not nullable and converted.isna().any():
            raise CsvSchemaError("empty value is not allowed for non-nullable float")
        return pd.Series(
            [None if pd.isna(value) else float(value) for value in converted.tolist()],
            index=series.index,
            dtype="object",
        )
    if inner_type.startswith("Decimal("):
        return series.map(lambda value: convert_value(_value_to_string(value), clickhouse_type)).astype("object")
    if inner_type == "Date":
        converted = pd.to_datetime(series, errors="raise").dt.date
        if not nullable and converted.isna().any():
            raise CsvSchemaError("empty value is not allowed for non-nullable date")
        return converted.map(lambda value: None if pd.isna(value) else value).astype("object")
    if inner_type == "DateTime":
        converted = pd.to_datetime(series, errors="raise")
        if not nullable and converted.isna().any():
            raise CsvSchemaError("empty value is not allowed for non-nullable datetime")
        return converted.map(lambda value: None if pd.isna(value) else value.to_pydatetime()).astype("object")
    if inner_type == "Bool":
        return series.map(lambda value: convert_value(_value_to_string(value), clickhouse_type)).astype("object")
    validate_clickhouse_type_expression(clickhouse_type)
    if not nullable and series.isna().any():
        raise CsvSchemaError(f"empty value is not allowed for non-nullable {clickhouse_type}")
    return series.map(lambda value: None if pd.isna(value) else _value_to_string(value)).astype("object")


def _first_bad_value(series: pd.Series, clickhouse_type: str) -> object:
    for value in series.tolist():
        try:
            convert_value(_value_to_string(value), clickhouse_type)
        except CsvSchemaError:
            return value
    return series.iloc[0] if len(series) else ""


def _clean_json_value(value):
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


def _value_to_string(value) -> str:
    return "" if pd.isna(value) else str(value).strip()


def _normalize_nullable_type(clickhouse_type: str, nullable: bool) -> str:
    if nullable and not clickhouse_type.startswith("Nullable("):
        return f"Nullable({clickhouse_type})"
    if not nullable and clickhouse_type.startswith("Nullable("):
        return clickhouse_type.removeprefix("Nullable(").removesuffix(")")
    return clickhouse_type


def _split_sample_values(value: object) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def _duplicates(values: list[str]) -> set[str]:
    seen = set()
    duplicates = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return duplicates
