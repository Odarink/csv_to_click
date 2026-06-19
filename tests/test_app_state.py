from __future__ import annotations

import ast
from pathlib import Path


def test_streamlit_widget_keys_are_not_reused_for_custom_session_state() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    widget_keys: set[str] = set()
    custom_state_keys: set[str] = set()

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "form"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "st"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            widget_keys.add(node.args[0].value)

        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.value, ast.Attribute)
            and node.value.attr == "session_state"
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == "st"
            and isinstance(node.slice, ast.Constant)
            and isinstance(node.slice.value, str)
        ):
            custom_state_keys.add(node.slice.value)

    assert widget_keys.isdisjoint(custom_state_keys)
