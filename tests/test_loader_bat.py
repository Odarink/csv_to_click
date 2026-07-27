from __future__ import annotations

from pathlib import Path


def loader_lines() -> list[str]:
    """Строки loader.bat без отступов.

    Проверять вхождение подстроки в весь файл нельзя: рядом с каждой командой
    стоит `echo Running: <та же команда>`, и такой пин остаётся зелёным после
    удаления самой команды.
    """
    return [line.strip() for line in Path("loader.bat").read_text(encoding="utf-8").splitlines()]


def test_loader_bat_installs_the_locked_environment() -> None:
    lines = loader_lines()

    assert "call :find_uv" in lines
    assert '"%UV_CMD%" sync --locked --extra dev' in lines


def test_loader_bat_finds_uv_outside_path() -> None:
    """На рабочей машине без прав администратора единственный проходящий способ
    установки — `pip install --user uv`, а он кладёт uv.exe в каталог
    пользовательских скриптов Python, которого на PATH обычно нет. Поиск только
    по PATH превращал рабочий uv в «uv не найден»."""
    loader = Path("loader.bat").read_text(encoding="utf-8")
    lines = loader_lines()

    assert ":find_uv" in loader
    assert "for /f \"delims=\" %%U in ('where uv 2^>nul') do (" in lines
    assert "sysconfig.get_path('scripts','nt_user')" in loader
    assert 'set "UV_CMD=%%D\\uv.exe"' in lines


def test_loader_bat_offers_an_install_route_without_admin_rights() -> None:
    loader = Path("loader.bat").read_text(encoding="utf-8")

    assert "py -3.12 -m pip install --user uv" in loader
    assert loader.index("Without administrator rights") < loader.index("With administrator rights")


def test_loader_bat_launches_streamlit_from_the_project_venv() -> None:
    lines = loader_lines()

    launch = [line for line in lines if line.startswith('"%VENV_PYTHON%" -m streamlit run')]
    assert len(launch) == 1
    assert '"%APP_PATH%"' in launch[0]
    assert "--server.address 127.0.0.1" in launch[0]
    assert "--server.port 8501" in launch[0]
    assert 'set "PYTHONPATH=%PROJECT_DIR%src"' in lines


def test_loader_bat_tells_the_operator_how_to_install_uv() -> None:
    loader = Path("loader.bat").read_text(encoding="utf-8")

    # --source winget обязателен: без него winget падает на источнике msstore.
    assert "winget install --source winget -e --id astral-sh.uv" in loader
    assert "import streamlit, pandas, clickhouse_connect" in loader


def test_loader_bat_no_longer_resolves_dependencies_at_launch() -> None:
    """Иначе pandas, clickhouse-connect и urllib3 могут смениться между базовым
    и контрольным прогоном без единой правки в репозитории, и сравнение
    «до/после» перестаёт что-либо значить."""
    loader = Path("loader.bat").read_text(encoding="utf-8")

    assert "pip install -r" not in loader
    assert "python -m venv .venv" not in loader
    assert not Path("requirements.txt").exists()
    # Единственная исполняемая строка с pip — установка самого uv; ставить им
    # зависимости проекта нельзя, иначе версии снова поплывут между прогонами.
    pip_commands = [
        line for line in loader_lines()
        if "pip install" in line and not line.startswith("rem ")
    ]
    assert len(pip_commands) == 1
    assert pip_commands[0].endswith("pip install --user uv")


def test_loader_bat_uses_locked_rather_than_frozen() -> None:
    """`--frozen` берёт локфайл, вообще не сверяя его с pyproject.toml, поэтому
    молча запускается на разъехавшемся окружении. `--locked` на этом падает —
    ради этого он и выбран."""
    loader = Path("loader.bat").read_text(encoding="utf-8")

    assert "--frozen" not in loader


def test_python_version_is_pinned_for_uv() -> None:
    """Поиск `py -3.12` удалён вместе с pip-бутстрапом, а requires-python
    принимает любую 3.11+, так что без этого файла две машины получили бы разные
    минорные версии CPython из одного и того же коммита."""
    assert Path(".python-version").read_text(encoding="utf-8").strip() == "3.12"


def test_readme_makes_loader_bat_the_primary_windows_launch_path() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "запустите `loader.bat`" in readme
    assert "## Ручной запуск и диагностика" in readme
    assert readme.index("запустите `loader.bat`") < readme.index("## Ручной запуск и диагностика")


def test_readme_does_not_document_commands_the_loader_no_longer_runs() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")

    assert "requirements.txt" not in readme
    assert "uv sync --frozen" not in readme
    assert "python -m pip install" not in readme
