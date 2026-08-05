# -*- coding: utf-8 -*-
"""Сверка docs/codemap/codemap.lock с рабочим деревом для SessionStart-хука.

Печатает короткий статус карты кода (свежа / какие модули устарели) — stdout
хука попадает в контекст сессии. Всегда завершается кодом 0: проверка не
должна блокировать старт сессии. Алгоритм отпечатков — sha256-v1, тот же,
что записан в codemap.lock.

Ручной запуск: python docs/codemap/tools/check_codemap.py [путь-к-lock]
"""
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

MAX_LISTED = 8

# Хук читает stdout байтами; консольная кодировка Windows (cp1251) ломает
# кириллицу и не знает части символов — печатаем всегда в UTF-8.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except (AttributeError, OSError):
    pass


def fingerprint(root: Path, files: list[str]) -> str | None:
    lines = []
    for f in sorted(files):
        p = root / f
        if not p.is_file():
            return None
        lines.append(f"{f}:{hashlib.sha256(p.read_bytes()).hexdigest()}")
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def head_commit(root: Path) -> str | None:
    try:
        r = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root,
                           capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    return r.stdout.strip() if r.returncode == 0 else None


def main() -> int:
    root = Path(os.environ.get("CLAUDE_PROJECT_DIR") or Path.cwd())
    lock_path = Path(sys.argv[1]) if len(sys.argv) > 1 else root / "docs" / "codemap" / "codemap.lock"
    if not lock_path.is_file():
        print("Карта кода: docs/codemap/ отсутствует. Перед крупными правками "
              "сгенерируй её по правилу из AGENTS.md (codemap.html+json+lock вместе).")
        return 0
    try:
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Карта кода: codemap.lock не читается ({exc}) — перегенерируй docs/codemap/.")
        return 0

    stale = []
    for mid, entry in sorted(lock.get("modules", {}).items()):
        if fingerprint(root, entry.get("files", [])) != entry.get("fingerprint"):
            stale.append(mid)

    commit = head_commit(root)
    lock_commit = lock.get("commit", "")
    same_commit = commit is None or commit == lock_commit

    if not stale and same_commit:
        print("Карта кода docs/codemap/ актуальна (коммит "
              f"{lock_commit[:7]}). Перед правкой модуля смотри в codemap.json: "
              "кто его вызывает, на что он влияет, какие тесты его покрывают; "
              "потоки и ограничения — там же (codemap.html — то же интерактивно).")
        return 0

    parts = []
    if stale:
        listed = ", ".join(stale[:MAX_LISTED]) + ("…" if len(stale) > MAX_LISTED else "")
        parts.append(f"изменённые модули: {listed}")
    if not same_commit:
        parts.append(f"коммит ушёл: {lock_commit[:7]} -> {commit[:7]}")
    print("Карта кода docs/codemap/ УСТАРЕЛА (" + "; ".join(parts) + "). "
          "Для перечисленных модулей codemap.json может врать — проверяй по исходникам. "
          "По правилу AGENTS.md перегенерируй codemap.html+json+lock вместе "
          "(при правке границ модулей/зависимостей/потоков — в том же коммите).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
