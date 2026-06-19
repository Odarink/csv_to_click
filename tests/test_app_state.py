from __future__ import annotations

import ast
import re
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


def test_main_renders_csv_mapping_types_before_database_settings() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    main_func = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "main"
    )

    calls = [
        node.func.id
        for node in ast.walk(main_func)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    ]

    assert calls.index("_render_csv_path_step") < calls.index("_render_column_mapping_editor")
    assert calls.index("_render_column_mapping_editor") < calls.index("_render_type_editor")
    assert calls.index("_render_type_editor") < calls.index("_render_connection_and_load_form")


def test_connection_form_no_longer_collects_csv_path_or_read_options() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _render_connection_and_load_form\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    form_source = match.group(0)
    assert '"CSV path"' not in form_source
    assert '"Separator"' not in form_source
    assert '"Encoding"' not in form_source


def test_create_and_load_uses_confirmed_type_rows_for_mappings() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _create_and_load\(.*?\n(?=if __name__)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    create_and_load_source = match.group(0)
    assert 'st.session_state["type_rows"]' in create_and_load_source
    assert 'st.session_state["schema_rows"]' not in create_and_load_source


def test_connection_form_uses_persisted_settings_and_no_table_specific_defaults() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _render_connection_and_load_form\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    form_source = match.group(0)
    assert "load_app_settings" in source
    assert "save_app_settings" in source
    assert 'st.text_input("ORDER BY")' in form_source
    assert '"Distributed sharding key"' in form_source
    assert 'value="rand()"' not in form_source
    assert 'value="sipHash64(ID)"' not in form_source
    assert '"sharding_key": sharding_key or "rand()"' not in source
    assert "Sharding example: sipHash64(<column>)" in form_source
    assert "Distributed sharding key is required" in form_source
