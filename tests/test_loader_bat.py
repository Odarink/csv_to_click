from __future__ import annotations

from pathlib import Path


def test_loader_bat_bootstraps_windows_runtime() -> None:
    loader = Path("loader.bat").read_text(encoding="utf-8")

    expected_snippets = [
        "@echo off",
        'cd /d "%~dp0"',
        "py -3.12",
        "py -3.11",
        "python",
        r".venv\Scripts\python.exe",
        "python -m venv .venv",
        "python -m pip install --upgrade pip",
        "python -m pip install -r requirements.txt",
        "import streamlit, pandas, clickhouse_connect",
        r'set "PYTHONPATH=%PROJECT_DIR%src"',
        r"python -m streamlit run src\csv_click\app.py --server.address 127.0.0.1 --server.port 8501",
        "winget install -e --id Python.Python.3.12",
    ]

    for snippet in expected_snippets:
        assert snippet in loader


def test_readme_makes_loader_bat_the_primary_windows_launch_path() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "запустите `loader.bat`" in readme
    assert "## Ручной запуск и диагностика" in readme
    assert readme.index("запустите `loader.bat`") < readme.index("## Ручной запуск и диагностика")
