"""Запуск приложения после того, как окружение уже поставлено.

`loader.bat` отвечает только за то, что обязано работать ДО появления `.venv`:
найти или поставить `uv` и выполнить `uv sync`. Всё остальное живёт здесь,
потому что `.bat` можно проверить лишь чтением его текста, а этот модуль
проверяется исполнением.

Пользователи - аналитики без прав администратора, для которых трассировка
Python это тупик. Поэтому свои ошибки печатаются одной понятной строкой, а
`main` возвращает код возврата вместо того, чтобы бросать наружу.
"""

from __future__ import annotations

import socket
import subprocess
import sys
from pathlib import Path

#: Порт по умолчанию. Занят - будет взят следующий свободный.
DEFAULT_PORT = 8501
#: Сколько портов пробовать, прежде чем сдаться.
PORT_ATTEMPTS = 10


class LauncherError(Exception):
    """Ошибка запуска, которую можно объяснить пользователю одной строкой."""


def choose_free_port(preferred: int = DEFAULT_PORT, attempts: int = PORT_ATTEMPTS) -> int:
    """Первый свободный порт, начиная с `preferred`.

    Занятый порт - обычное дело: вторая копия приложения или чужой процесс.
    Раньше это выяснялось только после старта Streamlit, и пользователь получал
    совет закрыть процесс и запустить заново.

    `SO_REUSEADDR` намеренно НЕ ставится: с ним привязка к занятому порту на
    некоторых системах проходит, и проверка стала бы бессмысленной.
    """
    for offset in range(attempts):
        port = preferred + offset
        with socket.socket() as probe:
            try:
                probe.bind(("127.0.0.1", port))
            except OSError:
                continue
        return port
    raise LauncherError(
        f"No free port found in {preferred}..{preferred + attempts - 1}. "
        "Close the application that holds them and start again."
    )


def describe_version(project_dir: Path) -> str | None:
    """Короткий хеш и дата коммита, или ``None`` - если версию не узнать.

    Сеть не трогается: запуск не должен зависеть от доступности GitHub. Именно
    поэтому здесь нет сравнения с `origin` - `@{upstream}` знает лишь состояние
    последнего `git fetch`, так что у пользователя, который fetch не делал,
    сравнение молчало бы всегда, а сразу после `git pull` копия свежа по
    определению.

    ``None`` возвращается и когда папку скопировали без `.git`, и когда самого
    `git` на машине нет. Выдумывать версию нельзя: по ней будут искать причину
    чужой ошибки.
    """
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%h %cs"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    version = result.stdout.strip()
    return version or None


def streamlit_command(port: int, app_path: Path) -> list[str]:
    """Команда запуска Streamlit тем же интерпретатором, что запустил модуль.

    `sys.executable` - это python из `.venv`, потому что `loader.bat` зовёт
    модуль именно им. Так исключён запуск приложения чужим интерпретатором, где
    зависимостей нет.
    """
    return [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.address",
        "127.0.0.1",
        "--server.port",
        str(port),
    ]


def main() -> int:
    project_dir = Path(__file__).resolve().parents[2]
    app_path = Path(__file__).with_name("app.py")

    version = describe_version(project_dir)
    print(f"Version: {version}" if version else "Version: unknown (no git data in this folder)")

    try:
        port = choose_free_port()
    except LauncherError as exc:
        print(f"ERROR: {exc}")
        return 1

    if port != DEFAULT_PORT:
        print(f"Port {DEFAULT_PORT} is busy, using {port} instead.")
    print(f"Open http://127.0.0.1:{port} when the browser does not do it for you.")

    return subprocess.call(streamlit_command(port, app_path))


if __name__ == "__main__":
    sys.exit(main())
