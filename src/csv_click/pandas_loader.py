from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import threading
import time
from typing import Iterator, TypeVar

import numpy as np
import pandas as pd

from csv_click.clickhouse import raw_insert_batch, summary_elapsed_ns
from csv_click.errors import CsvLoadError, CsvReadCancelled, CsvSchemaError
from csv_click.load_stats import BlockProgress, LoadStats
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
    batch_size: int = 100_000


DEFAULT_MAX_INSERT_PAYLOAD_BYTES = 16 * 1024 * 1024
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
    max_insert_payload_bytes: int = DEFAULT_MAX_INSERT_PAYLOAD_BYTES,
) -> int:
    rows_count = 0
    usecols = [mapping.source_name for mapping in mappings if mapping.include]
    for chunk_number, chunk in enumerate(iter_pandas_chunks(csv_path, read_options, usecols), start=1):
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
    for chunk_number, chunk in enumerate(iter_pandas_chunks(csv_path, read_options, usecols), start=1):
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


def chunk_to_json_each_row_payload(chunk: pd.DataFrame, columns: list[str]) -> bytes:
    return b"\n".join(_json_each_row_line(row, columns) for row in _iter_rows(chunk, columns))


def iter_json_each_row_payloads(
    chunk: pd.DataFrame,
    columns: list[str],
    max_payload_bytes: int = DEFAULT_MAX_INSERT_PAYLOAD_BYTES,
) -> Iterator[tuple[bytes, int]]:
    if max_payload_bytes <= 0:
        raise ValueError("max_payload_bytes must be positive")

    lines: list[bytes] = []
    payload_bytes = 0
    rows_count = 0

    for row in _iter_rows(chunk, columns):
        line = _json_each_row_line(row, columns)
        line_size = len(line)
        if line_size > max_payload_bytes:
            raise CsvLoadError(
                "A single JSONEachRow row is "
                f"{line_size} bytes, which is larger than Max insert payload "
                f"{max_payload_bytes} bytes. Reduce column width or increase Max insert payload, MB."
            )

        separator_bytes = 1 if rows_count else 0
        if rows_count and payload_bytes + separator_bytes + line_size > max_payload_bytes:
            yield b"\n".join(lines), rows_count
            lines = []
            payload_bytes = 0
            rows_count = 0
            separator_bytes = 0

        lines.append(line)
        payload_bytes += separator_bytes + line_size
        rows_count += 1

    if rows_count:
        yield b"\n".join(lines), rows_count


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
    stats: LoadStats | None = None,
) -> LoadStats:
    """Грузит CSV блоками JSONEachRow и возвращает счётчики прогона.

    ``stats`` можно передать снаружи, чтобы частичные счётчики уцелели, если
    загрузка упадёт на середине: исключение уносит возвращаемое значение, но не
    переданный объект.
    """
    if worker_count <= 0:
        raise ValueError("worker_count must be positive")
    if stats is None:
        stats = LoadStats()
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
            stats=stats,
        )

    usecols = [mapping.source_name for mapping in mappings if mapping.include]
    chunks = _iter_timed(iter_pandas_chunks(csv_path, read_options, usecols))
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
            stats.serialize_s += serialize_s
            try:
                summary = raw_insert_batch(client, database, table, columns, payload)
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
            progress = _block_progress(
                chunk_number=chunk_number,
                block_number=block_number,
                block_rows=block_rows,
                rows_total=stats.rows + block_rows,
                payload_bytes=len(payload),
                summary=summary,
            )
            stats.add_block(progress)
            if progress_callback:
                progress_callback(progress)
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
    stats: LoadStats,
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
    ) -> _InsertedBlock:
        try:
            summary = raw_insert_batch(worker_client(), database, table, columns, payload)
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
            summary=summary,
        )

    def block_progress_for(inserted: _InsertedBlock) -> BlockProgress:
        # rows_total — это «сколько строк принято на момент завершения ЭТОГО
        # блока», а не порядковый номер отправки: блоки завершаются в любом
        # порядке, а накопление идёт в порядке завершения и в одном потоке,
        # поэтому итог от порядка не зависит.
        return _block_progress(
            chunk_number=inserted.chunk_number,
            block_number=inserted.block_number,
            block_rows=inserted.block_rows,
            rows_total=stats.rows + inserted.block_rows,
            payload_bytes=inserted.payload_bytes,
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
                continue
            try:
                inserted = future.result()
            except Exception:
                # Ошибку упавшего блока поднимает collect_completed; остальные
                # производны от того же сбоя и контекста не добавляют.
                continue
            stats.add_block(block_progress_for(inserted))

    def collect_completed(pending: set[Future]) -> None:
        # Вызывается только из главного потока, поэтому stats мутируется без лока.
        done, _pending = wait(pending, return_when=FIRST_COMPLETED)
        for future in done:
            pending.remove(future)
            try:
                inserted = future.result()
            except BaseException:
                cancel_pending(pending)
                raise
            progress = block_progress_for(inserted)
            stats.add_block(progress)
            if progress_callback:
                progress_callback(progress)

    pending: set[Future] = set()
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        try:
            chunks = _iter_timed(iter_pandas_chunks(csv_path, read_options, usecols))
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
                    stats.serialize_s += serialize_s
                    pending.add(
                        executor.submit(
                            insert_payload,
                            chunk_number=chunk_number,
                            block_number=block_number,
                            block_rows=block_rows,
                            columns=columns,
                            payload=payload,
                        )
                    )
                    if len(pending) >= max_pending:
                        collect_completed(pending)

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
    summary: dict[str, str]


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
) -> BlockProgress:
    # raw_bytes == wire_bytes, пока тело не сжимается: сжатие приходит в фазе 2.
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
        wire_bytes=payload_bytes,
        server_ns=elapsed_ns or 0,
        server_time_reported=elapsed_ns is not None,
    )


def _iter_rows(chunk: pd.DataFrame, columns: list[str]) -> Iterator[dict[str, object]]:
    for values in chunk[columns].itertuples(index=False, name=None):
        yield dict(zip(columns, values))


def _json_each_row_line(row: dict[str, object], columns: list[str]) -> bytes:
    cleaned = {column: _clean_json_value(row[column]) for column in columns}
    return json.dumps(cleaned, ensure_ascii=False, allow_nan=False).encode("utf-8")


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
    payload_mb = payload_bytes / 1024 / 1024
    return CsvLoadError(
        "ClickHouse raw insert failed for "
        f"{database}.{table}, chunk {chunk_number}, block {block_number}, "
        f"{block_rows} rows, {payload_mb:.2f} MB payload.{hint} Original error: {message}"
    )


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
