from pathlib import Path

from csv_click.csv_reader import detect_dialect, iter_csv_batches
from csv_click.schema import analyze_csv_schema


def test_detect_dialect_handles_semicolon(tmp_path: Path) -> None:
    csv_path = tmp_path / "semicolon.csv"
    csv_path.write_text("id;name\n1;alice\n", encoding="utf-8")

    assert detect_dialect(csv_path).delimiter == ";"


def test_iter_csv_batches_preserves_order_and_converts_values(tmp_path: Path) -> None:
    csv_path = tmp_path / "rows.csv"
    csv_path.write_text("id,name\n1,Alice\n2,\n3,Bob\n", encoding="utf-8")
    schema = analyze_csv_schema(csv_path)

    batches = list(iter_csv_batches(csv_path, schema, batch_size=2))

    assert batches == [
        [[1, "Alice"], [2, None]],
        [[3, "Bob"]],
    ]
