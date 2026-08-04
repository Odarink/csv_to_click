from __future__ import annotations

import re
import socket
import subprocess
import sys
from pathlib import Path

import pytest

from csv_click.launcher import (
    LauncherError,
    choose_free_port,
    describe_version,
    streamlit_command,
)


def _git(args: list[str], cwd: Path) -> None:
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
    )


def test_free_port_is_taken_as_is() -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        free_port = probe.getsockname()[1]

    assert choose_free_port(free_port) == free_port


def test_a_busy_port_moves_the_app_to_the_next_one() -> None:
    """Занятый 8501 - обычное дело: вторая копия приложения или чужой процесс.

    Раньше это обнаруживалось только после старта Streamlit, и оператор получал
    совет закрыть процесс и запустить заново.
    """
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen(1)
        busy_port = taken.getsockname()[1]

        chosen = choose_free_port(busy_port)

    assert chosen != busy_port
    assert busy_port < chosen <= busy_port + 10


def test_no_free_port_fails_loudly() -> None:
    sockets = []
    try:
        with socket.socket() as first:
            first.bind(("127.0.0.1", 0))
            start = first.getsockname()[1]
        for offset in range(3):
            held = socket.socket()
            held.bind(("127.0.0.1", start + offset))
            held.listen(1)
            sockets.append(held)

        with pytest.raises(LauncherError, match="port"):
            choose_free_port(start, attempts=3)
    finally:
        for held in sockets:
            held.close()


def test_version_reads_the_commit_from_git(tmp_path: Path) -> None:
    _git(["init", "-q"], cwd=tmp_path)
    (tmp_path / "a.txt").write_text("a", encoding="utf-8")
    _git(["add", "."], cwd=tmp_path)
    _git(["commit", "-qm", "first"], cwd=tmp_path)
    expected = subprocess.run(
        ["git", "log", "-1", "--format=%h"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()

    version = describe_version(tmp_path)

    assert version is not None
    assert expected in version
    # Дата обязательна: по одному хешу нельзя понять, насколько копия старая, а
    # именно за этим версию и показывают.
    assert re.fullmatch(r"\S+ \d{4}-\d{2}-\d{2}", version), version


def test_version_is_silent_outside_a_repository(tmp_path: Path) -> None:
    """Папку могли скопировать без `.git`. Выдумывать версию нельзя."""
    assert describe_version(tmp_path) is None


def test_a_failing_git_is_not_mistaken_for_a_version(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ненулевой код возврата решает всё, что бы git ни напечатал в stdout.

    Проверять только пустоту вывода недостаточно: git пишет диагностику в
    stderr, но полагаться на это - значит однажды показать пользователю
    сообщение об ошибке вместо номера версии.
    """
    import csv_click.launcher as launcher

    def failing_git(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        return subprocess.CompletedProcess(args=args, returncode=128, stdout="fatal: whatever", stderr="")

    monkeypatch.setattr(launcher.subprocess, "run", failing_git)

    assert describe_version(Path(".")) is None


def test_streamlit_runs_from_the_project_interpreter() -> None:
    command = streamlit_command(port=8502, app_path=Path("src/csv_click/app.py"))

    assert command[0] == sys.executable
    assert command[1:4] == ["-m", "streamlit", "run"]
    assert "--server.port" in command
    assert "8502" in command
    assert "127.0.0.1" in command
