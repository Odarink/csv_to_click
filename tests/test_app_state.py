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

    assert "_render_csv_read_options_step" not in calls
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


def test_create_and_load_renders_progress_and_accumulated_log() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _create_and_load\(.*?\n(?=if __name__)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    create_and_load_source = match.group(0)
    assert "st.progress(0)" in create_and_load_source
    assert "log_container = st.empty()" in create_and_load_source
    assert "_append_load_log" in create_and_load_source
    assert "_render_load_log" in create_and_load_source


def test_create_and_load_rechecks_effective_encoding_before_insert() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _create_and_load\(.*?\n(?=if __name__)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    create_and_load_source = match.group(0)
    assert "choose_read_options_for_preview" in create_and_load_source
    assert "effective_read_options" in create_and_load_source
    assert "validate_csv_with_pandas_chunks(csv_path, effective_read_options, mappings)" in create_and_load_source
    assert "read_options=effective_read_options" in create_and_load_source


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
    assert 'st.text_input("ORDER BY")' not in form_source
    assert 'st.selectbox("ORDER BY"' in form_source
    assert '"Distributed sharding key"' in form_source
    assert 'st.text_input("Distributed sharding key"' not in form_source
    assert 'st.selectbox("Distributed sharding key"' in form_source
    assert 'st.text_input("PARTITION BY (optional)")' in form_source
    assert 'value="rand()"' not in form_source
    assert 'value="sipHash64(ID)"' not in form_source
    assert '"sharding_key": sharding_key or "rand()"' not in source
    assert "Distributed sharding key is required" in form_source


def test_connection_form_uses_schema_target_columns_for_sorting_and_sharding() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _render_connection_and_load_form\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    form_source = match.group(0)
    assert "_schema_target_names(schema)" in form_source
    assert "options=target_names" in form_source
    assert '"sharding_key": sharding_column' in form_source


def test_csv_path_step_renders_read_settings_before_first_analysis() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _render_csv_path_step\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    csv_path_step_source = match.group(0)
    assert '"CSV path"' in csv_path_step_source
    assert '"CSV read settings"' in csv_path_step_source
    assert '"Separator"' in csv_path_step_source
    assert '"Encoding"' in csv_path_step_source
    assert '"Read CSV"' in csv_path_step_source
    assert "st.session_state[\"csv_read_options\"] = read_options" in csv_path_step_source


def test_apply_csv_path_does_not_reset_selected_read_options() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _apply_csv_path\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    apply_source = match.group(0)
    assert "_read_options_from_settings" not in apply_source
    assert "read_options:" in apply_source


def test_connection_form_has_apply_and_test_connection_submit_buttons() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _render_connection_and_load_form\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    form_source = match.group(0)
    assert 'form_submit_button("Apply parameters"' in form_source
    assert 'form_submit_button("Test connection"' in form_source
    assert "if test_submitted:" in form_source
    assert form_source.index("if test_submitted:") < form_source.index("if not submitted")
