from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any

from csv_click.clickhouse import (
    DEFAULT_CLIENT_CERT,
    DEFAULT_CLIENT_KEY,
    DEFAULT_HOST,
    DEFAULT_PORT,
)


DEFAULT_SETTINGS_PATH = Path.home() / ".csv_click" / "settings.json"


@dataclass(frozen=True)
class AppSettings:
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    username: str = ""
    secure: bool = True
    verify: bool = False
    client_cert: str = DEFAULT_CLIENT_CERT
    client_key: str = DEFAULT_CLIENT_KEY
    database: str = "sandbox"
    cluster: str = "clickhouse"
    batch_size: int = 100_000
    max_insert_payload_mb: int = 16
    load_workers: int = 1
    strict_preflight: bool = True
    separator: str = ","
    encoding: str = "utf_8"


def load_app_settings(settings_path: Path = DEFAULT_SETTINGS_PATH) -> AppSettings:
    if not settings_path.exists():
        return AppSettings()
    try:
        raw = json.loads(settings_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return AppSettings()

    allowed_fields = {field.name for field in fields(AppSettings)}
    values: dict[str, Any] = {
        key: value
        for key, value in raw.items()
        if key in allowed_fields
    }
    if "max_insert_payload_mb" not in raw and values.get("batch_size") == 1_000_000:
        values["batch_size"] = 100_000
    return AppSettings(**values)


def save_app_settings(
    settings: AppSettings,
    settings_path: Path = DEFAULT_SETTINGS_PATH,
) -> None:
    settings_path.parent.mkdir(parents=True, exist_ok=True)
    settings_path.write_text(
        json.dumps(asdict(settings), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
