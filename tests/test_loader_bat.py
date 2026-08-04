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


def test_loader_bat_hands_the_launch_to_the_python_module() -> None:
    """Всё, что можно проверить исполнением, живёт в `csv_click.launcher`.

    В `.bat` осталось только то, что обязано работать ДО появления `.venv`, и
    проверяется оно чтением текста. Выбор порта, версия и запуск Streamlit ушли
    в модуль, у которого есть настоящие тесты.
    """
    lines = loader_lines()

    launch = [line for line in lines if line.startswith('"%VENV_PYTHON%" -m csv_click.launcher')]
    assert len(launch) == 1
    assert 'set "PYTHONPATH=%PROJECT_DIR%src"' in lines
    # Порт и адрес выбирает модуль: захардкоженные здесь, они разошлись бы с ним.
    assert not [line for line in lines if "--server.port" in line]


def test_loader_bat_does_not_blame_python_when_python_worked() -> None:
    """Установка uv может провалиться при живом Python: индекс недоступен.

    На целевых машинах есть корпоративный перехват TLS - README сам приводит
    падение winget с `0x8a15005e`. Тогда `pip install` возвращает ненулевой код,
    а загрузчик печатал «no working Python either», хотя тремя строками выше сам
    сообщал, что Python нашёл. И предлагал ту же команду, которая только что не
    сработала.
    """
    loader = Path("loader.bat").read_text(encoding="utf-8")
    lines = loader_lines()

    # Факт «рабочий Python найден» запоминается там же, где он установлен.
    assert any(line.startswith('set "PYTHON_FOUND=') for line in lines)
    not_found_block = loader[loader.index("\n:uv_not_found") :]
    assert "no working Python either" not in not_found_block
    # Ветка обязана различать два исхода, а не печатать один текст на оба.
    assert "if defined PYTHON_FOUND" in not_found_block


def test_loader_bat_checks_whether_pip_succeeded() -> None:
    install_block_start = Path("loader.bat").read_text(encoding="utf-8").index("\n:install_uv")
    loader = Path("loader.bat").read_text(encoding="utf-8")
    install_block = loader[install_block_start : loader.index("\n:uv_not_found")]

    after_pip = install_block[install_block.index("pip install --user uv") :]
    # Именно `errorlevel 1`: в cmd проверка means «код >= указанного», поэтому
    # порог 9009 выглядит как проверка, но пропускает обычный провал pip.
    assert "if errorlevel 1 (" in after_pip, (
        "код возврата pip не проверяется на единице, и провал выглядит как успех"
    )


def test_loader_bat_does_not_claim_streamlit_failed_before_it_started() -> None:
    """Модуль может выйти с ошибкой ДО запуска Streamlit - например, когда все
    порты заняты. Утверждать в этом случае, что упал Streamlit, значит послать
    искать не там; совет про 8501 устарел вместе с фиксированным портом."""
    loader = Path("loader.bat").read_text(encoding="utf-8")
    # Метка переименована вместе со смыслом: падает не Streamlit, а запуск.
    assert ":streamlit_failed" not in loader
    failure_block = loader[loader.index("\n:launch_failed") :]

    assert "Streamlit stopped with an error" not in failure_block
    assert "8501" not in failure_block


def test_loader_bat_has_no_dead_variables() -> None:
    """`APP_PATH` осиротел, когда запуск переехал в модуль."""
    loader = Path("loader.bat").read_text(encoding="utf-8")

    assert "APP_PATH" not in loader


def test_loader_bat_installs_uv_itself_when_python_is_available() -> None:
    """Раньше загрузчик печатал команду и выходил, хотя мог выполнить её сам.

    Пользователь без прав администратора - аналитик, а не инженер: копирование
    команды в консоль это лишний шаг, на котором он останавливается и идёт
    спрашивать.
    """
    loader = Path("loader.bat").read_text(encoding="utf-8")
    lines = loader_lines()

    assert ":install_uv" in loader
    assert "call :install_uv" in lines[lines.index("call :find_uv") + 1]
    executed = [
        line for line in lines
        if "pip install --user uv" in line and not line.startswith(("rem ", "echo"))
    ]
    assert executed, "нет строки, которая РЕАЛЬНО ставит uv, а не печатает совет"
    # Метки ищутся с начала строки: `:install_uv` встречается и внутри
    # `call :install_uv`, а `:uv_not_found` - внутри `goto :uv_not_found`, и по
    # первому вхождению «блок» вырождался в две строки основного потока.
    install_block = loader[loader.index("\n:install_uv") : loader.index("\n:uv_not_found")]
    # После установки uv.exe лежит вне PATH, поэтому поиск повторяется.
    assert "call :find_uv" in install_block


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
    # pip разрешён РОВНО для установки самого uv; ставить им зависимости проекта
    # нельзя, иначе версии снова поплывут между прогонами. `echo` с той же
    # командой - подсказка пользователю, а не исполнение, и в счёт не идёт.
    pip_commands = [
        line for line in loader_lines()
        if "pip install" in line and not line.startswith(("rem ", "echo"))
    ]
    assert pip_commands, "загрузчик больше не ставит uv"
    for command in pip_commands:
        assert command.endswith("pip install --user uv"), command


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
