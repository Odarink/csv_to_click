from pathlib import Path

from csv_click.clickhouse import DEFAULT_CLIENT_CERT, DEFAULT_CLIENT_KEY
from csv_click.settings import AppSettings, load_app_settings, save_app_settings


def test_load_app_settings_uses_notebook_defaults_when_file_is_missing(tmp_path: Path) -> None:
    settings = load_app_settings(tmp_path / "missing.json")

    assert settings.client_cert == DEFAULT_CLIENT_CERT
    assert settings.client_key == DEFAULT_CLIENT_KEY
    assert settings.host == "tp17.wb-bank.ru"
    assert settings.database == "sandbox"
    assert settings.cluster == "clickhouse"
    assert settings.batch_size == 100_000
    assert settings.max_insert_payload_mb == 16
    assert settings.load_workers == 1


def test_save_and_load_app_settings_persists_static_ui_defaults(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    settings = AppSettings(
        host="custom.host",
        port=9440,
        username="user.name",
        secure=False,
        verify=True,
        client_cert="/tmp/client.crt",
        client_key="/tmp/client.key",
        database="analytics",
        cluster="custom_cluster",
        batch_size=5000,
        max_insert_payload_mb=8,
        load_workers=4,
        strict_preflight=False,
        separator=";",
        encoding="cp1251",
    )

    save_app_settings(settings, settings_path)
    loaded = load_app_settings(settings_path)

    assert loaded == settings


def test_load_app_settings_migrates_old_default_batch_size(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"
    settings_path.write_text('{"batch_size": 1000000}', encoding="utf-8")

    loaded = load_app_settings(settings_path)

    assert loaded.batch_size == 100_000
    assert loaded.max_insert_payload_mb == 16


def test_saved_app_settings_do_not_include_table_specific_fields(tmp_path: Path) -> None:
    settings_path = tmp_path / "settings.json"

    save_app_settings(AppSettings(username="user.name"), settings_path)

    saved = settings_path.read_text(encoding="utf-8")
    assert "csv_path" not in saved
    assert "distributed_table" not in saved
    assert "order_by" not in saved
    assert "partition_by" not in saved
    assert "sharding_key" not in saved
