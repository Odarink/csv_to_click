from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator

from csv_click.schema import CsvSchema, convert_value


def detect_dialect(csv_path: str | Path, delimiter: str | None = None) -> csv.Dialect:
    path = Path(csv_path)
    if delimiter:
        return _dialect_for_delimiter(delimiter)
    sample = path.read_text(encoding="utf-8-sig")[:8192]
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        return csv.get_dialect("excel")


def iter_csv_batches(
    csv_path: str | Path,
    schema: CsvSchema,
    batch_size: int,
    delimiter: str | None = None,
) -> Iterator[list[list[object]]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    dialect = detect_dialect(csv_path, delimiter)
    with Path(csv_path).open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file, dialect=dialect)
        batch: list[list[object]] = []
        for row in reader:
            batch.append(
                [
                    convert_value(row.get(column.source_name, ""), column.final_type)
                    for column in schema.columns
                ]
            )
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def _dialect_for_delimiter(delimiter: str) -> csv.Dialect:
    class CustomDialect(csv.excel):
        pass

    CustomDialect.delimiter = delimiter
    return CustomDialect
