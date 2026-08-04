from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
import io
from pathlib import Path
import threading
import time
from typing import Iterator, TypeVar
import zlib

import lz4.frame
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import zstandard
from clickhouse_connect.driver.compression import get_compressor

from csv_click.clickhouse import raw_insert_batch, summary_elapsed_ns
from csv_click.errors import CsvLoadCancelled, CsvLoadError, CsvReadCancelled, CsvSchemaError
from csv_click.load_stats import BlockProgress, LoadStats
from csv_click.schema import (
    NA_MARKERS,
    CsvColumn,
    CsvSchema,
    _ColumnStats,
    _infer_type,
    _needs_nullable,
    convert_value,
    normalize_identifier,
    unwrap_nullable,
    validate_clickhouse_type_expression,
)


@dataclass(frozen=True)
class ReadOptions:
    separator: str = ","
    encoding: str = "utf_8"
    batch_size: int = 100_000


DEFAULT_MAX_INSERT_PAYLOAD_BYTES = 16 * 1024 * 1024
#: Сколько строк сериализовать, чтобы оценить размер строки перед нарезкой.
_BLOCK_ESTIMATE_SAMPLE_ROWS = 1024
#: Во сколько раз оценка строк-на-блок может вырасти за один блок. Ограничение
#: нужно, чтобы после одного маленького блока не сериализовать заведомо
#: огромный срез только ради того, чтобы его обрезать.
_BLOCK_ESTIMATE_GROWTH_LIMIT = 16
DEFAULT_SCHEMA_SAMPLE_ROWS = 100_000
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


#: Типы, которые pandas разбирает при чтении быстрее и без потерь. Всё
#: остальное читается текстом: там значима исходная запись — ведущие нули в
#: String, хвостовой ноль в `1.50` для Decimal, литералы вроде `NA`.
NATIVELY_PARSED_TYPES: frozenset[str] = frozenset({"Int64", "UInt64", "Float64"})

#: Что `to_json` экранирует обратным слэшем внутри строки. Прямой слэш — не
#: описка, а наследие ujson: `a/b` уезжает как `a\/b`. Снято выполнением, не
#: по документации, и закреплено дифференциальным тестом.
_JSON_ESCAPES: tuple[tuple[str, str], ...] = (("\\", "\\\\"), ('"', '\\"'), ("/", "\\/"))
#: Управляющие символы `to_json` пишет escape-последовательностью. Быстрый путь так не умеет и
#: на них отказывается в пользу эталона.
_JSON_UNSUPPORTED_RE = r"[\x00-\x1f]"
#: Один проход вместо трёх замен: если ни одного особого символа в колонке нет,
#: экранировать нечего. На тексте без слэшей и кавычек это обычный случай.
_JSON_ESCAPE_NEEDED_RE = r'["\\/\x00-\x1f]'
#: Сколько значений object-колонки хватает, чтобы отказать по типу без полной
#: конвертации в Arrow. Смотрится голова, потому что колонка однородна по типу.
_OBJECT_PEEK_ROWS = 8


def text_columns_for(mappings: list[SchemaMapping]) -> set[str]:
    """Колонки, которые обязаны приехать сырым текстом."""
    return {
        mapping.source_name
        for mapping in mappings
        if mapping.include and unwrap_nullable(mapping.final_type)[1] not in NATIVELY_PARSED_TYPES
    }


class _CountingBinaryFile(io.RawIOBase):
    """Бинарный файл, считающий отданные pandas байты — прогресс по файлу.

    Снято зондом на pandas 3.0.3: парсер читает из объекта постепенно
    (упреждающий буфер ~3,4 МБ), счётчик растёт монотонно, а после последнего
    чанка равен размеру файла байт в байт.
    """

    def __init__(self, path: str | Path) -> None:
        self._file = open(path, "rb")
        self.bytes_read = 0

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        data = self._file.read(size)
        self.bytes_read += len(data)
        return data

    def readinto(self, buffer) -> int | None:
        count = self._file.readinto(buffer)
        self.bytes_read += count or 0
        return count

    def close(self) -> None:
        self._file.close()
        super().close()


def iter_pandas_chunks(
    csv_path: str | Path,
    read_options: ReadOptions,
    usecols: list[str] | None = None,
    text_columns: set[str] | None = None,
    on_bytes_read: Callable[[int], None] | None = None,
) -> Iterator[pd.DataFrame]:
    """Читает CSV чанками так же, как его читают превью и инференс.

    ``text_columns`` — какие колонки нужны сырым текстом; остальные pandas
    разбирает сам, что и быстрее, и точнее для чисел. ``None`` означает «все
    текстом» и используется путями инференса, которые типов ещё не знают.

    ``on_bytes_read`` получает после каждого чанка накопленное число байт,
    прочитанных из файла. Общее число строк большого CSV неизвестно, а размер
    в байтах известен всегда — это единственная честная основа прогресса.
    Из-за упреждающего буфера счётчик чуть опережает разобранные строки.
    """
    if read_options.batch_size <= 0:
        raise ValueError("batch_size must be positive")

    try:
        raw_usecols = _raw_header_names(csv_path, read_options, usecols)
        counter = _CountingBinaryFile(csv_path) if on_bytes_read is not None else None
        try:
            reader = pd.read_csv(
                counter if counter is not None else csv_path,
                sep=read_options.separator,
                encoding=read_options.encoding,
                chunksize=read_options.batch_size,
                usecols=raw_usecols,
                # Ровно то же, что читают превью и инференс. Без этого путь загрузки
                # видит другие данные, чем интерфейс: `007` становится числом 7,
                # литералы NA/null/N/A - пропусками, а `1.50` в Decimal теряет ноль.
                **_read_type_options(csv_path, read_options, raw_usecols, text_columns),
            )
            for chunk in reader:
                chunk.columns = chunk.columns.str.strip()
                if counter is not None and on_bytes_read is not None:
                    on_bytes_read(counter.bytes_read)
                yield chunk
        finally:
            if counter is not None:
                counter.close()
    except pd.errors.ParserError as exc:
        raise CsvSchemaError(
            "Cannot parse CSV with "
            f"separator {read_options.separator!r} and encoding {read_options.encoding}: {exc}"
        ) from exc


def _read_type_options(
    csv_path: str | Path,
    read_options: ReadOptions,
    raw_usecols: list[str] | None,
    text_columns: set[str] | None,
) -> dict[str, object]:
    """Аргументы `read_csv`, отвечающие за типы и пропуски.

    `keep_default_na` — переключатель на весь файл, а смысл маркера пропуска
    зависит от колонки: `NA` в String это значение, в числовой — пропуск.
    Поэтому маркеры выключаются глобально и возвращаются точечно через
    `na_values` тем колонкам, которые pandas разбирает сам.
    """
    if text_columns is None:
        return {"dtype": str, "keep_default_na": False}

    names = raw_usecols if raw_usecols is not None else _header_names(csv_path, read_options)
    stripped = {name: str(name).strip() for name in names}
    dtype = {name: str for name, clean in stripped.items() if clean in text_columns}
    na_values = {
        name: list(NA_MARKERS) for name, clean in stripped.items() if clean not in text_columns
    }
    return {"dtype": dtype, "keep_default_na": False, "na_values": na_values}


def _header_names(csv_path: str | Path, read_options: ReadOptions) -> list[str]:
    header = pd.read_csv(
        csv_path,
        sep=read_options.separator,
        encoding=read_options.encoding,
        nrows=0,
    )
    return list(header.columns)


def _raw_header_names(
    csv_path: str | Path,
    read_options: ReadOptions,
    usecols: list[str] | None,
) -> list[str] | None:
    """Переводит обрезанные имена колонок обратно в сырые имена заголовка.

    `pd.read_csv` сопоставляет `usecols` с СЫРЫМ заголовком, а `.str.strip()`
    выполняется строкой позже. Поэтому заголовок вида `id, code ,amt`, обычный
    для выгрузок из Excel, ронял и загрузку, и preflight с
    `ValueError: Usecols do not match columns`, называя колонку, которую
    интерфейс показывает без пробелов.
    """
    if usecols is None:
        return None
    raw_by_stripped = {str(name).strip(): name for name in _header_names(csv_path, read_options)}
    return [raw_by_stripped.get(name, name) for name in usecols]


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


def choose_read_options_for_preview(
    csv_path: str | Path,
    read_options: ReadOptions,
    nrows: int = 20,
) -> tuple[ReadOptions, pd.DataFrame, MojibakeWarning | None]:
    candidates = (read_options.encoding,) + tuple(
        encoding for encoding in ENCODING_SUGGESTIONS if encoding != read_options.encoding
    )
    best_options: ReadOptions | None = None
    best_preview: pd.DataFrame | None = None
    best_score: int | None = None
    first_error: CsvSchemaError | None = None

    for encoding in candidates:
        candidate_options = ReadOptions(
            separator=read_options.separator,
            encoding=encoding,
            batch_size=read_options.batch_size,
        )
        try:
            candidate_preview = preview_csv_rows(csv_path, candidate_options, nrows=nrows)
        except CsvSchemaError as exc:
            if first_error is None:
                first_error = exc
            continue
        candidate_score = _mojibake_score(candidate_preview)
        if best_score is None or candidate_score < best_score:
            best_options = candidate_options
            best_preview = candidate_preview
            best_score = candidate_score
        if candidate_score == 0:
            break

    if best_options is None or best_preview is None:
        if first_error is not None:
            raise first_error
        raise CsvSchemaError("Cannot decode CSV preview with configured encodings")
    if best_score:
        raise CsvSchemaError(
            "CSV preview still contains replacement characters or mojibake after trying "
            + ", ".join(candidates)
            + ". The source file is likely already corrupted or was exported with a wrong encoding."
        )

    warning = detect_mojibake(best_preview)
    if best_options.encoding != read_options.encoding:
        warning = MojibakeWarning(
            message=(
                f"Auto-selected encoding {best_options.encoding} because "
                f"{read_options.encoding} produced mojibake in CSV preview."
            ),
            suggested_encodings=ENCODING_SUGGESTIONS,
        )
    return best_options, best_preview, warning


def detect_mojibake(preview: pd.DataFrame) -> MojibakeWarning | None:
    if not _mojibake_score(preview):
        return None
    return MojibakeWarning(
        message=(
            "CSV preview may contain mojibake. Try another encoding: "
            + ", ".join(ENCODING_SUGGESTIONS)
        ),
        suggested_encodings=ENCODING_SUGGESTIONS,
    )


def _mojibake_score(preview: pd.DataFrame) -> int:
    score = 0
    for value in preview.astype(str).to_numpy().ravel().tolist():
        score += value.count("пїЅ") * 10
        score += value.count("�") * 10
        for marker in MOJIBAKE_MARKERS:
            if marker not in {"пїЅ", "�"}:
                score += value.count(marker)
    return score


def analyze_csv_with_pandas_sample(
    csv_path: str | Path,
    read_options: ReadOptions,
    nrows: int = DEFAULT_SCHEMA_SAMPLE_ROWS,
) -> CsvSchema:
    if nrows <= 0:
        raise ValueError("nrows must be positive")

    try:
        sample = preview_csv_rows(csv_path, read_options, nrows=nrows)
    except pd.errors.EmptyDataError as exc:
        raise CsvSchemaError("CSV header is required") from exc

    source_names = list(sample.columns)
    stats = _init_column_stats(source_names)
    for source_name in source_names:
        for value in sample[source_name].tolist():
            stats[source_name].add_value(_value_to_string(value))

    return _schema_from_stats(source_names, stats)


def analyze_csv_with_pandas_chunks(
    csv_path: str | Path,
    read_options: ReadOptions,
    cancel_callback: Callable[[], bool] | None = None,
) -> CsvSchema:
    stats: dict[str, _ColumnStats] = {}
    source_names: list[str] | None = None

    try:
        for chunk in iter_pandas_chunks(csv_path, read_options):
            if cancel_callback and cancel_callback():
                raise CsvReadCancelled("CSV read was stopped")
            if source_names is None:
                source_names = list(chunk.columns)
                stats = _init_column_stats(source_names)

            for source_name in source_names:
                for value in chunk[source_name].tolist():
                    stats[source_name].add_value(_value_to_string(value))
            if cancel_callback and cancel_callback():
                raise CsvReadCancelled("CSV read was stopped")
    except pd.errors.EmptyDataError as exc:
        raise CsvSchemaError("CSV header is required") from exc
    except UnicodeDecodeError as exc:
        raise CsvSchemaError(f"Cannot decode CSV with encoding {read_options.encoding}: {exc}") from exc

    if source_names is None:
        raise CsvSchemaError("CSV header is required")

    return _schema_from_stats(source_names, stats)


def _init_column_stats(source_names: list[str]) -> dict[str, _ColumnStats]:
    target_names = [normalize_identifier(name) for name in source_names]
    duplicates = _duplicates(target_names)
    if duplicates:
        raise CsvSchemaError(
            "CSV header contains duplicate column names after normalization: "
            + ", ".join(sorted(duplicates))
        )
    return {name: _ColumnStats() for name in source_names}


def _schema_from_stats(source_names: list[str], stats: dict[str, _ColumnStats]) -> CsvSchema:
    columns = []
    for source_name in source_names:
        inferred_type, notes = _infer_type(stats[source_name])
        nullable = _needs_nullable(stats[source_name], inferred_type)
        final_type = f"Nullable({inferred_type})" if nullable else inferred_type
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
    max_insert_payload_bytes: int = DEFAULT_MAX_INSERT_PAYLOAD_BYTES,
) -> int:
    rows_count = 0
    usecols = [mapping.source_name for mapping in mappings if mapping.include]
    text_columns = text_columns_for(mappings)
    for chunk_number, chunk in enumerate(
        iter_pandas_chunks(csv_path, read_options, usecols, text_columns), start=1
    ):
        converted = convert_chunk_to_schema(chunk, mappings, chunk_number)
        for _payload, _payload_rows in iter_json_each_row_payloads(
            converted,
            list(converted.columns),
            max_payload_bytes=max_insert_payload_bytes,
        ):
            pass
        rows_count += len(converted)
    return rows_count


def validate_csv_sample_with_pandas_chunks(
    csv_path: str | Path,
    read_options: ReadOptions,
    mappings: list[SchemaMapping],
    max_insert_payload_bytes: int = DEFAULT_MAX_INSERT_PAYLOAD_BYTES,
    sample_rows: int = 200_000,
) -> int:
    if sample_rows <= 0:
        raise ValueError("sample_rows must be positive")

    rows_count = 0
    usecols = [mapping.source_name for mapping in mappings if mapping.include]
    text_columns = text_columns_for(mappings)
    for chunk_number, chunk in enumerate(
        iter_pandas_chunks(csv_path, read_options, usecols, text_columns), start=1
    ):
        remaining_rows = sample_rows - rows_count
        if remaining_rows <= 0:
            break
        sample_chunk = chunk.head(remaining_rows).reset_index(drop=True)
        converted = convert_chunk_to_schema(sample_chunk, mappings, chunk_number)
        for _payload, _payload_rows in iter_json_each_row_payloads(
            converted,
            list(converted.columns),
            max_payload_bytes=max_insert_payload_bytes,
        ):
            pass
        rows_count += len(converted)
        if rows_count >= sample_rows:
            break
    return rows_count


#: Кодеки, которые умеет и драйвер, и ClickHouse на `Content-Encoding`.
#: Замерено на профиле выгрузки (блок 9,49 МБ, одна колонка UInt64):
#:   zstd  3,76x, 493 МБ/с — 19 с процессора на весь файл в 9,5 ГБ
#:   gzip  3,32x,  34 МБ/с — 283 с, слишком дорого
#:   lz4   1,93x, 878 МБ/с — на цифровом JSON жмёт вдвое хуже zstd
COMPRESSION_CODECS: tuple[str, ...] = ("zstd", "lz4", "gzip")
COMPRESSION_OFF = "off"


def wire_codec(compression: str | None) -> str | None:
    """Что отдать драйверу как кодек: настоящий кодек либо `None`.

    Драйвер ставит `Content-Encoding` из ЛЮБОЙ непустой строки
    (`httpclient.py:417-418`), поэтому `off` обязан превратиться в `None`
    ЗДЕСЬ, а не «пониматься» дальше по пути. Прогон 2026-07-27 23:54 упал на
    первом же блоке с ответом прокси `unsupported compression method off`:
    выключатель уехал в заголовок как имя кодека.
    """
    if not compression or compression == COMPRESSION_OFF:
        return None
    return compression


def compress_payload(payload: bytes, codec: str | None) -> bytes:
    """Сжимает тело блока перед отправкой. `None`/`off` — вернуть как есть.

    Тело сжимает ВЫЗЫВАЮЩИЙ: `raw_insert` только ставит `Content-Encoding` и
    переносит сам запрос в параметры URL (`httpclient.py:417-427`).

    Компрессор создаётся на каждый вызов намеренно: `Lz4Compressor` и
    `GzipCompressor` помечены в драйвере как НЕ потокобезопасные
    (`compression.py`, `thread_safe=False`), а жмут пять воркеров сразу.
    """
    if not codec or codec == COMPRESSION_OFF:
        return payload
    if codec not in COMPRESSION_CODECS:
        raise CsvSchemaError(
            f"Unknown insert compression {codec!r}. "
            f"Supported: {COMPRESSION_OFF}, {', '.join(COMPRESSION_CODECS)}."
        )
    compressor = get_compressor(codec)
    compressed = compressor.compress_block(payload)
    tail = compressor.flush()
    return bytes(compressed + tail) if tail else bytes(compressed)


def _compress_block(payload: bytes, compression: str | None, stats: LoadStats) -> bytes:
    """Сжимает тело блока и записывает потраченное время в счётчики.

    Только для ПОСЛЕДОВАТЕЛЬНОГО пути, где поток один и распараллеливать нечего.
    На пути с воркерами сжатие делает сам воркер: zlib отпускает GIL (4,5× на
    пяти потоках), и на прогоне с gzip продюсер был занят 99,5% времени, из них
    53% — сжатие, при простое воркеров 74%.
    """
    if not compression or compression == COMPRESSION_OFF:
        return payload
    started = time.perf_counter()
    body = compress_payload(payload, compression)
    stats.compress_s += time.perf_counter() - started
    return body


def _decompress_for_tests(payload: bytes, codec: str) -> bytes:
    """Обратная сторона :func:`compress_payload`, нужна только проверкам.

    Живёт здесь, а не в тестах, чтобы кодеки распаковывались тем же списком,
    каким сжимаются: разъехавшись, они дали бы зелёный тест на битых байтах.
    """
    if codec == "zstd":
        return zstandard.decompress(payload)
    if codec == "lz4":
        return lz4.frame.decompress(payload)
    if codec == "gzip":
        return zlib.decompress(payload, wbits=31)
    raise CsvSchemaError(f"Unknown insert compression {codec!r}")


def chunk_to_json_lines(chunk: pd.DataFrame, columns: list[str]) -> bytes:
    """Весь чанк в JSONEachRow: сначала Arrow, при отказе — `to_json`.

    Замерено на профиле выгрузки (одна колонка, 500 тыс. строк на чанк):
    `to_json` даёт 0,288 мкс/строку, сборка через Arrow — 0,083, то есть 3,5×.
    Это главная стадия: на прогоне в 500 млн строк она занимала 70,5% времени
    вставки, а сервер — 4,9%.

    Быстрый путь берётся ТОЛЬКО когда обещает байт-в-байт тот же результат.
    Иначе возвращается `None`, и чанк идёт эталоном: расхождение здесь означало
    бы порчу данных, а не замедление.

    Все значения к этому моменту уже приведены `convert_chunk_to_schema` к
    типам, которые `to_json` кодирует напрямую: str, int, float, bool, None и
    Decimal — последний он кодирует строкой, ровно как это делал `str()` в
    построчном пути. Временные колонки приведены к строкам там же, вектором.
    """
    fast = _arrow_json_lines(chunk, columns)
    if fast is not None:
        return fast
    return _pandas_json_lines(chunk, columns)


def _pandas_json_lines(chunk: pd.DataFrame, columns: list[str]) -> bytes:
    """Эталон, с которым обязан совпадать быстрый путь."""
    payload = chunk[columns].to_json(
        orient="records",
        lines=True,
        force_ascii=False,
        double_precision=15,
    )
    return payload.rstrip("\n").encode("utf-8")


def _arrow_json_lines(chunk: pd.DataFrame, columns: list[str]) -> bytes | None:
    """Тот же JSONEachRow, собранный вычислениями Arrow. `None` — отказ.

    Строка собирается ОДНИМ вызовом `binary_join_element_wise`: литералы
    (`{"имя":`, запятые, `}`) передаются скалярами и размножаются сами, поэтому
    на строку не остаётся ни одного действия на Python.
    """
    if not columns or len(chunk) == 0:
        return None

    # Сначала ДЕШЁВАЯ проверка всех колонок и только потом сборка. Отказ решается
    # по dtype и по типу первых значений, без конвертации: иначе кадр, у которого
    # непригодна ПОСЛЕДНЯЯ колонка, успевал полностью собраться через Arrow и
    # выброситься. Замерено на пяти колонках с Decimal в конце: 1,28× медленнее
    # эталона против 1,01× после этой проверки.
    for column in columns:
        if not _arrow_may_support(chunk[column]):
            return None

    # Контракт функции — либо байты, либо `None`; бросать нельзя. Несовпадение
    # типов в ядрах Arrow приходит именно исключением, и уронить им загрузку
    # значило бы обменять 14 минут работы на неизвестное ядро. Откат при этом
    # выдаёт ТЕ ЖЕ байты, так что тише не становится ничего, кроме скорости.
    try:
        parts: list[object] = []
        for index, column in enumerate(columns):
            fragment = _arrow_json_values(chunk[column])
            if fragment is None:
                # Отказ хотя бы одной колонки — отказ всего чанка. Собирать
                # строку из частей двух путей нельзя: разойдётся весь блок.
                return None
            key = _arrow_json_key(column)
            if key is None:
                return None
            parts.append(pa.scalar(('{' if index == 0 else ',') + f'"{key}":'))
            parts.append(fragment)
        parts.append(pa.scalar("}"))

        lines = pc.binary_join_element_wise(*parts, pa.scalar(""))
        offsets = pa.array([0, len(lines)], type=pa.int32())
        joined = pc.binary_join(pa.ListArray.from_arrays(offsets, lines), "\n")
    except pa.ArrowException:
        return None
    return joined[0].as_py().encode("utf-8")


def _arrow_may_support(series: pd.Series) -> bool:
    """Может ли колонка идти быстрым путём — решается БЕЗ конвертации в Arrow.

    Не окончательный ответ: `_arrow_json_values` проверяет ещё и тип Arrow после
    конвертации. Смысл этой функции — отказать всему кадру раньше, чем на него
    потратятся вычисления Arrow.
    """
    if pd.api.types.is_float_dtype(series):
        return False
    if pd.api.types.is_object_dtype(series):
        return _arrow_supports_object_values(series)
    return True


def _arrow_supports_object_values(series: pd.Series) -> bool:
    """Годится ли object-колонка быстрому пути, судя по первым значениям.

    Нужна только для ДЕШЁВОГО отказа. `True` ничего не гарантирует: тип всё
    равно проверяется после конвертации. Замерено, зачем это: `from_pandas` на
    Decimal-объектах стоит 0,855 мкс/строку, и кадр с одной такой колонкой был
    в 2,15 раза медленнее, чем до правки — конвертация делалась и выбрасывалась.

    Пропуски пропускаются: колонка из одних пропусков решается уже в Arrow.
    """
    for value in series.head(_OBJECT_PEEK_ROWS):
        if value is None or value is pd.NA:
            continue
        if isinstance(value, float) and value != value:
            continue
        return isinstance(value, (str, bool, int, np.bool_, np.integer))
    return True


def _arrow_json_key(column: str) -> str | None:
    """Имя колонки, экранированное как это делает `to_json`. `None` — отказ.

    Ключ экранируется по тем же правилам, что значение: имя `q"q` без этого
    давало `{"q"q":1}`, то есть сломанный JSON. Найдено дифференциальным
    фаззером; целевое имя приходит из редактора типов свободным текстом, и
    `normalize_identifier` к нему не применяется.
    """
    if any(ord(char) < 0x20 for char in column):
        return None
    for needle, replacement in _JSON_ESCAPES:
        column = column.replace(needle, replacement)
    return column


def _arrow_json_values(series: pd.Series) -> pa.Array | None:
    """Колонка в готовые фрагменты значения: число как есть, строка в кавычках.

    `None` — когда байт-в-байт совпадение с `to_json` не гарантируется. Фрагмент
    НИКОГДА не содержит null: пропуск превращается в литерал `null`, иначе
    `binary_join_element_wise` обнулил бы всю строку.
    """
    if pd.api.types.is_float_dtype(series):
        # Самый дешёвый отказ: у `to_json` свой формат float (`1e+20` при
        # double_precision=15), и совпадение с ним не доказано.
        return None
    if pd.api.types.is_object_dtype(series) and not _arrow_supports_object_values(series):
        return None

    try:
        values = pa.Array.from_pandas(series)
    except (pa.ArrowException, OverflowError, TypeError, ValueError):
        # Смешанная object-колонка либо int вне int64 (`OverflowError`). Эталон
        # их кодирует, быстрый путь — нет.
        return None

    if not isinstance(values, pa.Array):
        # Колонка, чей UTF-8 не влез в 2 ГиБ, приезжает `ChunkedArray`. Ядра
        # Arrow его принимают, а `ListArray.from_arrays` бросает обычный
        # `TypeError` мимо `ArrowException` — и загрузка падала вместо отката.
        return None

    # Решает ТИП ARROW, а не dtype pandas: `convert_chunk_to_schema` отдаёт Bool
    # и целые в object-колонке, и по dtype они выглядели текстом.
    if pa.types.is_integer(values.type) or pa.types.is_boolean(values.type):
        text = pc.cast(values, pa.string())
        return pc.fill_null(text, "null") if values.null_count else text

    if not (pa.types.is_string(values.type) or pa.types.is_large_string(values.type)):
        # Decimal и прочие объекты приезжают своим типом Arrow, а не строкой.
        return None

    if pa.types.is_large_string(values.type):
        # dtype "string" в pandas даёт large_string, а его ядра
        # `binary_join_element_wise` со скалярами string не смешивают.
        try:
            values = pc.cast(values, pa.string())
        except pa.ArrowInvalid:
            return None

    if pc.any(pc.match_substring_regex(values, _JSON_ESCAPE_NEEDED_RE)).as_py():
        if pc.any(pc.match_substring_regex(values, _JSON_UNSUPPORTED_RE)).as_py():
            # Управляющие символы to_json пишет escape-последовательностью, чего
            # этого не умеет, а угадывать здесь нельзя.
            return None
        for needle, replacement in _JSON_ESCAPES:
            values = pc.replace_substring(values, needle, replacement)

    quoted = pc.binary_join_element_wise(pa.scalar('"'), values, pa.scalar('"'), pa.scalar(""))
    return pc.fill_null(quoted, "null") if values.null_count else quoted


def iter_json_each_row_payloads(
    chunk: pd.DataFrame,
    columns: list[str],
    max_payload_bytes: int = DEFAULT_MAX_INSERT_PAYLOAD_BYTES,
) -> Iterator[tuple[bytes, int]]:
    if max_payload_bytes <= 0:
        raise ValueError("max_payload_bytes must be positive")
    total_rows = len(chunk)
    if total_rows == 0:
        return

    # Размер строки оценивается по ОГРАНИЧЕННОЙ головной выборке, а не по всему
    # чанку. Сериализовать чанк целиком только чтобы узнать его длину - значит
    # платить лишний полный проход и держать весь payload в памяти на каждом
    # чанке, который в лимит не влез, а это любая таблица шире ~150 байт/строку
    # при настройках по умолчанию.
    sample_rows = min(_BLOCK_ESTIMATE_SAMPLE_ROWS, total_rows)
    sample = chunk_to_json_lines(chunk.iloc[:sample_rows], columns)
    if sample_rows == total_rows and len(sample) <= max_payload_bytes:
        yield sample, total_rows
        return

    bytes_per_row = max(1.0, len(sample) / sample_rows)
    rows_per_block = max(1, int(max_payload_bytes / bytes_per_row))

    # Чанк, который по оценке влезает целиком, уходит одним куском. Иначе цикл
    # ниже платит по одному bytes-объекту и одной итерации упаковки на КАЖДУЮ
    # строку: на профиле выгрузки это 67% времени сериализации при блоке 9,5 МБ
    # против лимита 28,3 МБ, то есть при работе, которой не требовалось.
    # Решение принимается по ФАКТИЧЕСКОЙ длине: оценка снята с головы чанка и
    # умеет соврать, а блок сверх лимита ClickHouse отвергнет. Риск памяти тот
    # же, что у цикла ниже: он режет срезами ровно по этой же оценке.
    # Уже сериализованные, но ещё не отправленные строки. Благодаря буферу
    # перебравший срез не выбрасывается: лишние строки уходят в следующий блок,
    # и каждая строка сериализуется РОВНО ОДИН раз. Без него нарезка на
    # разнородных данных проигрывала построчному упаковщику вдвое.
    # Разбиение по b"\n" корректно: JSON экранирует переводы строк в значениях.
    pending: list[bytes] = []
    pending_bytes = 0
    start = 0

    if bytes_per_row * total_rows <= max_payload_bytes:
        whole = chunk_to_json_lines(chunk, columns)
        if len(whole) <= max_payload_bytes:
            yield whole, total_rows
            return
        # Оценка соврала. Строки из собранного payload — ровно то, что цикл ниже
        # собирался сериализовать заново, поэтому он их забирает, а не
        # выбрасывает: иначе чанк сериализовался бы дважды.
        pending = whole.split(b"\n")
        pending_bytes = sum(map(len, pending))
        start = total_rows
        del whole
    while pending or start < total_rows:
        while start < total_rows and pending_bytes + max(0, len(pending) - 1) <= max_payload_bytes:
            stop = min(start + rows_per_block, total_rows)
            more = chunk_to_json_lines(chunk.iloc[start:stop], columns).split(b"\n")
            pending.extend(more)
            read_bytes = sum(map(len, more))
            pending_bytes += read_bytes
            start = stop
            # Оценка растёт и здесь, а не только после отправки блока: иначе
            # после пачки толстых строк дозаполнение читало бы тонкие такими же
            # мелкими порциями и делало бы тысячи вызовов to_json на один блок.
            # Сверху всё равно ограничено остатком чанка.
            if read_bytes:
                grown = int(len(more) * max_payload_bytes / read_bytes)
                rows_per_block = max(
                    rows_per_block,
                    min(grown, len(more) * _BLOCK_ESTIMATE_GROWTH_LIMIT),
                )

        taken = 0
        used = 0
        for line in pending:
            extra = len(line) + (1 if taken else 0)
            if used + extra > max_payload_bytes:
                break
            used += extra
            taken += 1
        if taken == 0:
            raise CsvLoadError(
                "A single JSONEachRow row is "
                f"{len(pending[0])} bytes, which is larger than Max insert payload "
                f"{max_payload_bytes} bytes. Reduce column width or increase Max insert payload, MB."
            )

        yield b"\n".join(pending[:taken]), taken
        pending_bytes -= sum(map(len, pending[:taken]))
        del pending[:taken]


def load_csv_via_raw_insert(
    client,
    csv_path: str | Path,
    read_options: ReadOptions,
    database: str,
    table: str,
    mappings: list[SchemaMapping],
    max_insert_payload_bytes: int = DEFAULT_MAX_INSERT_PAYLOAD_BYTES,
    worker_count: int = 1,
    client_factory: Callable[[], object] | None = None,
    progress_callback=None,
    compression: str | None = None,
    stats: LoadStats | None = None,
    cancel_callback: Callable[[], bool] | None = None,
) -> LoadStats:
    """Грузит CSV блоками JSONEachRow и возвращает счётчики прогона.

    ``stats`` можно передать снаружи, чтобы частичные счётчики уцелели, если
    загрузка упадёт на середине: исключение уносит возвращаемое значение, но не
    переданный объект.

    ``cancel_callback`` опрашивается перед каждым блоком (первый блок нового
    чанка покрывает и границу чанков) — контракт как у
    :func:`analyze_csv_with_pandas_chunks`. Ответ ``True`` поднимает
    :class:`CsvLoadCancelled`; блоки, уже отданные воркерам, дорабатывают и
    попадают либо в подтверждённые, либо в ``blocks_unconfirmed`` — новые на
    сервер не уходят.
    """
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")
    if stats is None:
        stats = LoadStats()
    # Ниже по пути ходит только настоящий кодек либо None: `off` в заголовке
    # роняет загрузку на первом блоке ответом прокси.
    compression = wire_codec(compression)
    # Нужен самой статистике: при worker_count > 1 запросы идут одновременно, и
    # сумма серверных времён перестаёт быть долей стенных часов.
    stats.worker_count = worker_count
    if worker_count > 1:
        if client_factory is None:
            raise ValueError("client_factory is required when worker_count is greater than 1")
        return _load_csv_via_raw_insert_parallel(
            client_factory=client_factory,
            csv_path=csv_path,
            read_options=read_options,
            database=database,
            table=table,
            mappings=mappings,
            max_insert_payload_bytes=max_insert_payload_bytes,
            worker_count=worker_count,
            progress_callback=progress_callback,
            compression=compression,
            stats=stats,
            cancel_callback=cancel_callback,
        )

    usecols = [mapping.source_name for mapping in mappings if mapping.include]
    chunks = _iter_timed(
        iter_pandas_chunks(
            csv_path,
            read_options,
            usecols,
            text_columns_for(mappings),
            on_bytes_read=_note_read_bytes(stats),
        )
    )
    for chunk_number, (chunk, read_s) in enumerate(chunks, start=1):
        stats.read_s += read_s
        convert_started = time.perf_counter()
        converted = convert_chunk_to_schema(chunk, mappings, chunk_number)
        stats.convert_s += time.perf_counter() - convert_started
        columns = list(converted.columns)
        payloads = _iter_timed(
            iter_json_each_row_payloads(
                converted,
                columns,
                max_payload_bytes=max_insert_payload_bytes,
            )
        )
        for block_number, ((payload, block_rows), serialize_s) in enumerate(payloads, start=1):
            _raise_if_load_cancelled(cancel_callback)
            stats.serialize_s += serialize_s
            body = _compress_block(payload, compression, stats)
            insert_started = time.perf_counter()
            try:
                summary = raw_insert_batch(client, database, table, columns, body, compression)
            except Exception as exc:
                # Тот же учёт, что на параллельном пути: блок не подтверждён.
                stats.insert_busy_s += time.perf_counter() - insert_started
                stats.blocks_unconfirmed += 1
                raise _raw_insert_error(
                    exc=exc,
                    database=database,
                    table=table,
                    chunk_number=chunk_number,
                    block_number=block_number,
                    block_rows=block_rows,
                    payload_bytes=len(payload),
                ) from exc
            stats.insert_busy_s += time.perf_counter() - insert_started
            progress = _block_progress(
                chunk_number=chunk_number,
                block_number=block_number,
                block_rows=block_rows,
                rows_total=stats.rows + block_rows,
                payload_bytes=len(payload),
                wire_bytes=len(body),
                summary=summary,
            )
            stats.add_block(progress)
            if progress_callback:
                progress_callback(progress)
    # Итератор чанков исчерпан: файл прочитан до конца, блоков больше не будет.
    stats.source_fully_read = True
    return stats


def _load_csv_via_raw_insert_parallel(
    *,
    client_factory: Callable[[], object],
    csv_path: str | Path,
    read_options: ReadOptions,
    database: str,
    table: str,
    mappings: list[SchemaMapping],
    max_insert_payload_bytes: int,
    worker_count: int,
    progress_callback,
    compression: str | None,
    stats: LoadStats,
    cancel_callback: Callable[[], bool] | None,
) -> LoadStats:
    usecols = [mapping.source_name for mapping in mappings if mapping.include]
    max_pending = worker_count * 2
    worker_state = threading.local()

    def worker_client():
        client = getattr(worker_state, "client", None)
        if client is None:
            client = client_factory()
            worker_state.client = client
        return client

    def insert_payload(
        *,
        chunk_number: int,
        block_number: int,
        block_rows: int,
        columns: list[str],
        payload: bytes,
        submitted_at: float,
    ) -> _InsertedBlock:
        # Сжатие делает ВОРКЕР, а не продюсер: zlib отпускает GIL (замерено
        # 4,5x на пяти потоках), а на прогоне с gzip продюсер был занят 99,5%
        # времени, из них 53% — сжатие в одном потоке, при простое воркеров 74%.
        compress_started = time.perf_counter()
        body = compress_payload(payload, compression)
        compress_s = time.perf_counter() - compress_started

        # Часы вставки начинаются ПОСЛЕ сжатия: `insert_busy_s` — единственный
        # измеритель провода, и процессор в него подмешивать нельзя.
        started = time.perf_counter()
        try:
            summary = raw_insert_batch(
                worker_client(), database, table, columns, body, compression
            )
        except Exception as exc:
            raise _raw_insert_error(
                exc=exc,
                database=database,
                table=table,
                chunk_number=chunk_number,
                block_number=block_number,
                block_rows=block_rows,
                payload_bytes=len(payload),
            ) from exc
        return _InsertedBlock(
            chunk_number=chunk_number,
            block_number=block_number,
            block_rows=block_rows,
            payload_bytes=len(payload),
            wire_bytes=len(body),
            summary=summary,
            # Время сжатия возвращается вместе с блоком и складывается в главном
            # потоке: `float +=` из пяти воркеров теряет слагаемые молча.
            compress_s=compress_s,
            # Разность отметок из РАЗНЫХ потоков законна: perf_counter на Windows
            # это QueryPerformanceCounter, он общесистемный, а не потоковый.
            queue_s=started - submitted_at,
            insert_s=time.perf_counter() - started,
        )

    def block_progress_for(inserted: _InsertedBlock) -> BlockProgress:
        # rows_total — это «сколько строк принято на момент завершения ЭТОГО
        # блока», а не порядковый номер отправки: блоки завершаются в любом
        # порядке, а накопление идёт в порядке завершения и в одном потоке,
        # поэтому итог от порядка не зависит.
        #
        # Часы воркера складываются ЗДЕСЬ, в главном потоке: сами воркеры
        # общих счётчиков не трогают, поэтому лок не нужен.
        stats.insert_busy_s += inserted.insert_s
        stats.insert_queue_s += inserted.queue_s
        stats.compress_s += inserted.compress_s
        return _block_progress(
            chunk_number=inserted.chunk_number,
            block_number=inserted.block_number,
            block_rows=inserted.block_rows,
            rows_total=stats.rows + inserted.block_rows,
            payload_bytes=inserted.payload_bytes,
            wire_bytes=inserted.wire_bytes,
            summary=inserted.summary,
        )

    def cancel_pending(pending: set[Future]) -> None:
        """Гасит оставшиеся задачи после сбоя.

        Блоки, которые всё-таки успели дойти до сервера, засчитываются в stats:
        эти строки уже лежат в ClickHouse, и запись о падении обязана их
        показывать — иначе диагностика фазы 4 пойдёт по заниженным числам.
        Progress callback при этом не дёргается: загрузка уже падает, а он
        ходит в Streamlit и может подменить исходную ошибку своей.

        Обработанные задачи вынимаются из набора: эту функцию зовут дважды на
        одном и том же наборе — из collect_completed и из внешнего except, — и
        без изъятия каждый успевший блок засчитался бы по два раза.
        """
        for future in pending:
            future.cancel()
        while pending:
            future = pending.pop()
            if future.cancelled():
                # Блок не был отправлен вообще: его строк в таблице нет.
                stats.blocks_unconfirmed += 1
                continue
            try:
                inserted = future.result()
            except Exception:
                # Ошибку упавшего блока поднимает collect_completed; остальные
                # производны от того же сбоя и контекста не добавляют.
                stats.blocks_unconfirmed += 1
                continue
            stats.add_block(block_progress_for(inserted))

    def collect_completed(pending: set[Future]) -> None:
        # Вызывается только из главного потока, поэтому stats мутируется без лока.
        #
        # Замеряется РОВНО ожидание готового блока, а не тело функции: дальше
        # идут `future.result()`, учёт и `progress_callback`, который ходит в
        # Streamlit и стоянкой на очереди не является. Если блок уже готов,
        # `wait` возвращается сразу и прибавляет почти ноль — поле считает
        # настоящее блокирование, а не число вызовов.
        stall_started = time.perf_counter()
        done, _pending = wait(pending, return_when=FIRST_COMPLETED)
        stats.producer_stall_s += time.perf_counter() - stall_started
        for future in done:
            pending.remove(future)
            try:
                inserted = future.result()
            except BaseException:
                # Сюда попадает только сбой самой вставки: `future.result()` не
                # зовёт progress_callback. Значит блок не подтверждён.
                stats.blocks_unconfirmed += 1
                cancel_pending(pending)
                raise
            progress = block_progress_for(inserted)
            stats.add_block(progress)
            if progress_callback:
                progress_callback(progress)

    pending: set[Future] = set()
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        try:
            chunks = _iter_timed(
                iter_pandas_chunks(
                    csv_path,
                    read_options,
                    usecols,
                    text_columns_for(mappings),
                    on_bytes_read=_note_read_bytes(stats),
                )
            )
            for chunk_number, (chunk, read_s) in enumerate(chunks, start=1):
                stats.read_s += read_s
                convert_started = time.perf_counter()
                converted = convert_chunk_to_schema(chunk, mappings, chunk_number)
                stats.convert_s += time.perf_counter() - convert_started
                columns = list(converted.columns)
                payloads = _iter_timed(
                    iter_json_each_row_payloads(
                        converted,
                        columns,
                        max_payload_bytes=max_insert_payload_bytes,
                    )
                )
                for block_number, ((payload, block_rows), serialize_s) in enumerate(payloads, start=1):
                    _raise_if_load_cancelled(cancel_callback)
                    stats.serialize_s += serialize_s
                    pending.add(
                        executor.submit(
                            insert_payload,
                            chunk_number=chunk_number,
                            block_number=block_number,
                            block_rows=block_rows,
                            columns=columns,
                            payload=payload,
                            submitted_at=time.perf_counter(),
                        )
                    )
                    if len(pending) >= max_pending:
                        collect_completed(pending)

            # Файл прочитан до конца и все блоки отданы воркерам. Долетели ли
            # они — отдельный вопрос, на него отвечает `blocks_unconfirmed`.
            stats.source_fully_read = True
            while pending:
                collect_completed(pending)
        except BaseException:
            # Именно BaseException: RerunException и StopException в Streamlit
            # наследуются от него, а бросить их может любой st.*-вызов внутри
            # progress_callback. При except Exception отмена не срабатывала, но
            # ThreadPoolExecutor.__exit__ всё равно дожидался уже отправленных
            # блоков — сервер их принимал, а запись о прогоне их теряла.
            cancel_pending(pending)
            raise
    return stats


@dataclass(frozen=True)
class _InsertedBlock:
    """Результат одной вставки, возвращаемый воркером в главный поток."""

    chunk_number: int
    block_number: int
    block_rows: int
    payload_bytes: int
    #: Сколько байт реально ушло в провод: после сжатия меньше `payload_bytes`.
    wire_bytes: int
    summary: dict[str, str]
    #: Сколько блок пролежал в очереди пула: от `submit` до начала отправки.
    queue_s: float = 0.0
    #: Сколько заняло сжатие тела в этом воркере.
    compress_s: float = 0.0
    #: Сколько длилась сама вставка в воркере, стенные часы.
    insert_s: float = 0.0


def _note_read_bytes(stats: LoadStats) -> Callable[[int], None]:
    """Пишет накопленные прочитанные байты в счётчики прогона.

    Писатель один — поток продюсера; интерфейс это поле только читает.
    """

    def note(count: int) -> None:
        stats.src_read_bytes = count

    return note


def _raise_if_load_cancelled(cancel_callback: Callable[[], bool] | None) -> None:
    """Проверка отмены перед каждым блоком.

    Отдельной проверки на границе чанков нет намеренно: первый блок нового
    чанка проходит эту же, а мутация, убиравшая границу чанков, выживала —
    поведение неотличимо. Хвост тоже не проверяется: когда файл дочитан и все
    блоки отданы воркерам, гашение только подождало бы те же вставки и
    записало доехавшие блоки в неподтверждённые.
    """
    if cancel_callback and cancel_callback():
        raise CsvLoadCancelled("The load was cancelled by the operator")


_TimedItem = TypeVar("_TimedItem")


def _iter_timed(iterator: Iterator[_TimedItem]) -> Iterator[tuple[_TimedItem, float]]:
    """Отдаёт элементы вместе со временем, потраченным на получение каждого."""
    while True:
        started = time.perf_counter()
        try:
            item = next(iterator)
        except StopIteration:
            return
        yield item, time.perf_counter() - started


def _block_progress(
    *,
    chunk_number: int,
    block_number: int,
    block_rows: int,
    rows_total: int,
    payload_bytes: int,
    summary: dict[str, str],
    wire_bytes: int | None = None,
) -> BlockProgress:
    # `raw_bytes` — размер ДО сжатия, `wire_bytes` — то, что реально ушло. Без
    # сжатия они равны; коэффициент читается как их отношение.
    #
    # Признак «сервер сообщил своё время» берётся из наличия elapsed_ns, а НЕ из
    # непустоты сводки: драйвер всегда дописывает в неё query_id
    # (httpclient.py:444), поэтому пустой она не бывает даже когда прокси срезал
    # заголовок целиком, и проверка на пустоту была бы мёртвой.
    elapsed_ns = summary_elapsed_ns(summary)
    return BlockProgress(
        chunk_number=chunk_number,
        block_number=block_number,
        block_rows=block_rows,
        rows_total=rows_total,
        raw_bytes=payload_bytes,
        wire_bytes=payload_bytes if wire_bytes is None else wire_bytes,
        server_ns=elapsed_ns or 0,
        server_time_reported=elapsed_ns is not None,
    )


def _raw_insert_error(
    *,
    exc: Exception,
    database: str,
    table: str,
    chunk_number: int,
    block_number: int,
    block_rows: int,
    payload_bytes: int,
) -> CsvLoadError:
    message = str(exc)
    hint = ""
    if "read limit is reached" in message.lower():
        hint = (
            " The insert request exceeded the ClickHouse HTTP/proxy read limit. "
            "Reduce Max insert payload, MB or Batch size and retry."
        )
    elif "unsupported compression method" in message.lower():
        # Так ответил контур на `Content-Encoding: zstd` 2026-07-27 23:53.
        # Отвергает не ClickHouse, а то, что стоит перед ним, поэтому список
        # поддерживаемых кодеков угадать нельзя — его можно только перебрать.
        hint = (
            " The path in front of ClickHouse refused this Content-Encoding. "
            "Try another Insert compression codec or set it to off; the load fails "
            "on the first block, so each attempt costs seconds."
        )
    payload_mb = payload_bytes / 1024 / 1024
    return CsvLoadError(
        "ClickHouse raw insert failed for "
        f"{database}.{table}, chunk {chunk_number}, block {block_number}, "
        f"{block_rows} rows, {payload_mb:.2f} MB payload.{hint} Original error: {message}"
    )


def _missing_mask(series: pd.Series, na_markers: bool) -> pd.Series:
    """Где значения нет.

    С `keep_default_na=False` пустая ячейка приезжает пустой строкой, а не NaN,
    поэтому одной `isna()` больше не хватает. Пустая строка и отсутствие
    значения в CSV неразличимы: `,,` и `,"",` читаются одинаково.

    ``na_markers`` включает распознавание текстовых маркеров вроде ``NA`` и
    ``null``. Смысл маркера зависит от типа колонки: в String это значение, в
    числовой или временной - отсутствие значения. Флаг `keep_default_na`
    действует на весь файл и такого различия сделать не может.
    """
    if not (pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)):
        # Числовую колонку pandas разобрал сам, текстовых маркеров в ней уже
        # нет, а astype("object") боксил бы каждое значение в Python-объект.
        return series.isna()
    values = series.astype("object")
    missing = series.isna() | values.eq("")
    if na_markers:
        missing = missing | values.isin(NA_MARKERS)
    return missing


def _convert_series(series: pd.Series, clickhouse_type: str) -> pd.Series:
    nullable, inner_type = unwrap_nullable(clickhouse_type)
    # В String-колонке `NA` и `null` - обычный текст, ради чего фаза 3b и
    # делалась; во всех остальных типах это по-прежнему пропуск.
    missing = _missing_mask(series, na_markers=inner_type != "String")
    if inner_type == "String":
        # Для Nullable пустая ячейка это NULL, для обычного String - пустая
        # строка. Раньше сюда доезжал NaN, и не-nullable колонка получала текст
        # "nan" - значение, которого в файле не было.
        filler = None if nullable else ""
        values = series.astype("object")
        if not pd.api.types.is_string_dtype(series):
            # Обычный путь сюда не попадает: String-колонки читаются текстом.
            # Ветка нужна для кадров, собранных в коде, а не прочитанных из CSV.
            values = values.map(lambda value: value if value is None else str(value))
        return values.mask(missing, filler)
    # Остальным типам пустая ячейка это отсутствие значения; ниже её ждут как NaN.
    series = series.astype("object").mask(missing, None)
    if inner_type in {"Int64", "UInt64"}:
        converted = pd.to_numeric(series, errors="raise")
        if not nullable and converted.isna().any():
            raise CsvSchemaError("empty value is not allowed for non-nullable integer")
        # Целочисленный dtype с поддержкой пропусков: to_json печатает из него
        # и целые, и null сам, без построчного боксинга в Python int.
        try:
            return converted.astype("UInt64" if inner_type == "UInt64" else "Int64")
        except (TypeError, ValueError) as exc:
            # Раньше здесь стоял int(value), который молча ОБРЕЗАЛ дробную
            # часть: 1.7 в Int64-колонке уезжало единицей.
            raise CsvSchemaError(
                f"value is not a whole number and cannot be stored as {inner_type}: {exc}"
            ) from exc
    if inner_type == "Float64":
        converted = pd.to_numeric(series, errors="raise")
        if not nullable and converted.isna().any():
            raise CsvSchemaError("empty value is not allowed for non-nullable float")
        # Построчный путь звал json.dumps(allow_nan=False) и падал на inf.
        # У to_json такого рычага нет, он молча пишет null - переполнившее
        # double значение тихо легло бы пустым, а прогон отчитался бы успехом.
        if converted.abs().eq(float("inf")).any():
            raise CsvSchemaError(
                "value does not fit into Float64 and became infinity; "
                "use String or Decimal for this column"
            )
        # Float-dtype с поддержкой пропусков: to_json печатает из него и числа,
        # и null сам, без построчного боксинга.
        return converted.astype("Float64")
    if inner_type.startswith("Decimal("):
        # Decimal остаётся Decimal: to_json кодирует его как строку "1.50", то
        # есть ровно так же, как это делал построчный путь через str().
        return series.map(lambda value: convert_value(_value_to_string(value), clickhouse_type)).astype("object")
    if inner_type in {"Date", "Date32"}:
        # `Date32` отличается от `Date` только диапазоном, а формат у них общий:
        # без этой ветки колонка уходила сырым текстом, и `19600102` уезжало
        # так же, как записано, вместо канонического `1960-01-02`.
        return _format_temporal(series, nullable, "D", "T", "date")
    if inner_type == "DateTime":
        # Разделитель пробел, а не 'T', и без дробных секунд: это канонический
        # basic-формат ClickHouse. Драйвер сейчас навязывает каждому запросу
        # date_time_input_format=best_effort, который в 2-5 раз дороже basic на
        # значение; отдавать basic-совместимые строки - предпосылка к переходу.
        return _format_temporal(series, nullable, "s", " ", "datetime")
    if inner_type == "Bool":
        return series.map(lambda value: convert_value(_value_to_string(value), clickhouse_type)).astype("object")
    validate_clickhouse_type_expression(clickhouse_type)
    if not nullable and series.isna().any():
        raise CsvSchemaError(f"empty value is not allowed for non-nullable {clickhouse_type}")
    return series.map(lambda value: None if pd.isna(value) else _value_to_string(value)).astype("object")


def _format_temporal(
    series: pd.Series,
    nullable: bool,
    numpy_unit: str,
    separator: str,
    what: str,
) -> pd.Series:
    """Приводит временную колонку к строкам ОДНИМ векторным вызовом.

    Строки, а не объекты `date`/`datetime`, потому что дальше их сериализует
    `DataFrame.to_json`, а он временные типы кодирует по-своему. Заодно это
    убирает построчный `.isoformat()` из горячего пути.
    """
    converted = pd.to_datetime(series, errors="raise")
    if not nullable and converted.isna().any():
        raise CsvSchemaError(f"empty value is not allowed for non-nullable {what}")
    if isinstance(converted.dtype, pd.DatetimeTZDtype):
        # Форматирование напечатало бы локальное время стены и молча потеряло
        # офсет, сдвинув КАЖДУЮ строку. Выбрать интерпретацию за пользователя
        # нельзя: целевая колонка DateTime таймзоны не несёт.
        raise CsvSchemaError(
            "value carries a timezone offset, and the target DateTime column has no timezone; "
            "strip the offset in the source or convert the column to UTC first"
        )
    # np.datetime_as_string, а не .dt.strftime: последний не дополняет год
    # нулями, и 1-й год уехал бы как "1-01-01".
    formatted = pd.Series(
        np.datetime_as_string(converted.to_numpy(), unit=numpy_unit),
        index=series.index,
        dtype="object",
    )
    if separator != "T":
        formatted = formatted.str.replace("T", separator, n=1, regex=False)
    return formatted.where(converted.notna(), None).astype("object")


def _first_bad_value(series: pd.Series, clickhouse_type: str) -> object:
    for value in series.tolist():
        try:
            convert_value(_value_to_string(value), clickhouse_type)
        except CsvSchemaError:
            return value
    return series.iloc[0] if len(series) else ""


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
