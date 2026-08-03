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
    """Поток шагов живёт в `_render_load_flow`: `main` оборачивает его, чтобы
    строка пути заполнялась ПОСЛЕ блоков, которые меняют состояние."""
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    main_func = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "_render_load_flow"
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
    assert "validate_csv_with_pandas_chunks(" in create_and_load_source
    assert "max_insert_payload_bytes = _effective_insert_payload_bytes(max_insert_payload_mb)" in create_and_load_source
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
    assert '_remembered_selectbox("ORDER BY"' in form_source
    assert '"Distributed sharding key"' in form_source
    assert 'st.text_input("Distributed sharding key"' not in form_source
    assert '"Distributed sharding key", target_names' in form_source
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
    assert '_remembered_selectbox("ORDER BY", target_names, "order_by_choice")' in form_source
    assert '"Distributed sharding key", target_names, "sharding_column_choice"' in form_source
    assert '"sharding_key": sharding_column' in form_source


def test_connection_form_exposes_bounded_insert_payload_setting() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _render_connection_and_load_form\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    form_source = match.group(0)
    assert '"Batch size"' in form_source
    assert '"Max insert payload, MB"' in form_source
    assert "settings.max_insert_payload_mb" in form_source
    assert '"max_insert_payload_mb": int(max_insert_payload_mb)' in form_source
    assert "max_insert_payload_mb=int(max_insert_payload_mb)" in form_source


def test_connection_form_exposes_bounded_load_workers_setting() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _render_connection_and_load_form\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    form_source = match.group(0)
    assert '"Load workers"' in form_source
    assert "min_value=1" in form_source
    assert "max_value=6" in form_source
    assert "settings.load_workers" in form_source
    assert '"load_workers": int(load_workers)' in form_source
    assert "load_workers=int(load_workers)" in form_source


def test_connection_form_uses_live_widget_values_without_stale_form_state() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _render_connection_and_load_form\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    form_source = match.group(0)
    assert 'st.form("load_params_form")' not in form_source
    assert "form_submit_button" not in form_source
    assert 'return st.session_state["load_params"]' not in form_source
    assert 'st.session_state["load_params"] = params' in form_source


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


def test_csv_path_step_defaults_to_sample_schema_inference_with_full_scan_opt_in() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _render_csv_path_step\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    csv_path_step_source = match.group(0)
    assert 'SCHEMA_INFERENCE_SAMPLE = "Fast sample, 100000 rows"' in source
    assert 'SCHEMA_INFERENCE_FULL_SCAN = "Full scan"' in source
    assert '"Schema inference mode"' in csv_path_step_source
    assert "SCHEMA_INFERENCE_OPTIONS" in csv_path_step_source
    assert "index=0" in csv_path_step_source
    assert "schema_analysis_mode" in csv_path_step_source
    assert "_apply_csv_path(csv_path, read_options, schema_analysis_mode)" in csv_path_step_source

    apply_match = re.search(
        r"def _apply_csv_path\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )
    assert apply_match is not None
    apply_source = apply_match.group(0)
    assert "_analyze_csv(csv_path, read_options, schema_analysis_mode)" in apply_source


def test_csv_path_step_renders_stop_button_after_csv_is_selected() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _render_csv_path_step\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    csv_path_step_source = match.group(0)
    assert '"Stop read CSV / choose another file"' in csv_path_step_source
    assert "_request_stop_csv_read()" in csv_path_step_source
    assert "st.rerun()" in csv_path_step_source
    assert '"csv_path" not in st.session_state' in csv_path_step_source


def test_stop_csv_read_clears_downstream_csv_state() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _clear_csv_read_state\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    clear_source = match.group(0)
    assert "CSV_READ_STATE_KEYS = [" in source
    for key in [
        "csv_path",
        "schema_rows",
        "mapping_rows",
        "type_rows",
        "mapping_confirmed",
        "types_confirmed",
        "load_params",
        "csv_preview_rows",
        "csv_preview_warning",
    ]:
        assert f'"{key}"' in source
    assert "for key in CSV_READ_STATE_KEYS" in clear_source
    assert 'st.session_state["csv_read_cancel_requested"] = True' in source


def test_analyze_csv_uses_sample_inference_unless_full_scan_is_selected() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _analyze_csv\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    analyze_source = match.group(0)
    assert "analyze_csv_with_pandas_sample" in analyze_source
    assert "analyze_csv_with_pandas_chunks" in analyze_source
    assert "SCHEMA_INFERENCE_FULL_SCAN" in analyze_source
    assert "st.warning" in analyze_source
    assert "_csv_read_cancel_requested" in analyze_source
    assert "cancel_callback=_csv_read_cancel_requested" in analyze_source


def test_create_and_load_passes_max_insert_payload_to_loader() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _create_and_load\(.*?\n(?=if __name__)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    create_and_load_source = match.group(0)
    assert "max_insert_payload_mb: int" in create_and_load_source
    assert "max_insert_payload_bytes = _effective_insert_payload_bytes(max_insert_payload_mb)" in create_and_load_source
    assert "block.wire_bytes" in create_and_load_source

    # Именно аргументы вызова загрузчика: тот же kwarg передаётся и в две
    # функции preflight-валидации, поэтому пин на подстроку по всей функции
    # оставался бы зелёным даже после его пропажи из вызова загрузчика.
    loader_call = re.search(
        r"load_csv_via_raw_insert\(\n(.*?)\n\s*\)\n",
        create_and_load_source,
        flags=re.DOTALL,
    )
    assert loader_call is not None
    loader_arguments = loader_call.group(1)
    assert "max_insert_payload_bytes=max_insert_payload_bytes" in loader_arguments
    assert "progress_callback=on_progress" in loader_arguments
    assert "stats=stats" in loader_arguments


def test_create_and_load_uses_payload_safety_headroom() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _effective_insert_payload_bytes\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )
    create_match = re.search(
        r"def _create_and_load\(.*?\n(?=if __name__)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    assert create_match is not None
    helper_source = match.group(0)
    create_and_load_source = create_match.group(0)
    assert "INSERT_PAYLOAD_SAFETY_RATIO = 0.9" in source
    assert "max_insert_payload_mb * 1024 * 1024" in helper_source
    assert "INSERT_PAYLOAD_SAFETY_RATIO" in helper_source
    assert "Effective insert payload limit" in create_and_load_source
    assert "Load settings: batch size" in create_and_load_source


def test_create_and_load_times_preflight_connect_ddl_and_insert_separately() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _create_and_load\(.*?\n(?=if __name__)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    create_and_load_source = match.group(0)
    assert "stats.preflight_s = " in create_and_load_source
    assert "stats.connect_s = " in create_and_load_source
    assert "stats.ddl_s = " in create_and_load_source
    assert "stats.insert_wall_s = time.perf_counter() - insert_started" in create_and_load_source
    assert "stats.driver_retries = driver_retries.count" in create_and_load_source
    # insert_started ставится строго перед загрузкой, иначе server % считался бы
    # от времени, в которое входят preflight, connect и DDL.
    assert create_and_load_source.index("insert_started = time.perf_counter()") > create_and_load_source.index(
        "stats.ddl_s = "
    )


def test_create_and_load_persists_the_run_record_even_when_the_load_fails() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _create_and_load\(.*?\n(?=if __name__)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    create_and_load_source = match.group(0)
    assert "run_config = RunConfig(" in create_and_load_source
    assert "write_run_record(" in create_and_load_source
    # Запись обязана уходить из finally самой функции (отступ ровно 4 пробела,
    # в отличие от внутреннего finally вокруг вставки): конфигурация упавшего
    # прогона нужна именно для диагностики падения.
    finally_index = create_and_load_source.index("\n    finally:")
    assert create_and_load_source.index("write_run_record(") > finally_index


def test_create_and_load_passes_load_workers_to_loader() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    # Поток шагов - в `_render_load_flow`; `main` теперь только обёртка вокруг
    # него ради строки пути, которая заполняется после блоков.
    main_match = re.search(
        r"def _render_load_flow\(.*?\n(?=def _)",
        source,
        flags=re.DOTALL,
    )
    create_match = re.search(
        r"def _create_and_load\(.*?\n(?=if __name__)",
        source,
        flags=re.DOTALL,
    )

    assert main_match is not None
    assert create_match is not None
    main_source = main_match.group(0)
    create_and_load_source = create_match.group(0)
    assert 'load_workers=params["load_workers"]' in main_source
    assert "load_workers: int" in create_and_load_source
    assert "worker_count=load_workers" in create_and_load_source
    assert "client_factory=lambda: get_client(config)" in create_and_load_source


def test_create_and_load_uses_sample_validation_for_large_files() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")
    match = re.search(
        r"def _create_and_load\(.*?\n(?=if __name__)",
        source,
        flags=re.DOTALL,
    )

    assert match is not None
    create_and_load_source = match.group(0)
    assert "LARGE_CSV_PRECHECK_THRESHOLD_BYTES = 50 * 1024 * 1024" in source
    assert "SAMPLE_PRECHECK_ROWS = 200_000" in source
    assert "Path(csv_path).stat().st_size" in create_and_load_source
    assert "validate_csv_with_pandas_chunks(" in create_and_load_source
    assert "validate_csv_sample_with_pandas_chunks(" in create_and_load_source
    assert "Strict validation finished:" in create_and_load_source
    assert "Sample validation finished: first" in create_and_load_source
    assert "File is larger than 50 MB; using sample validation" in create_and_load_source


def test_app_renders_inline_help_for_each_ui_step() -> None:
    source = Path("src/csv_click/app.py").read_text(encoding="utf-8")

    expected_help_texts = [
        "Как указать CSV файл",
        r"C:\Users\<user>\Downloads\data.csv",
        "/home/jovyan/data/data.csv",
        "Как настроить колонки",
        "target_name станет именем колонки в ClickHouse",
        "Как проверить типы",
        "custom_type переопределяет final_type",
        "Decimal(18, 2)",
        "Как заполнить параметры ClickHouse",
        "для my_table будет создана локальная таблица my_table_local",
        "ORDER BY = customer_id",
        "Порядок финальных действий",
        "Preview DDL только показывает SQL",
        "если distributed или local таблица уже существует",
    ]

    for text in expected_help_texts:
        assert text in source


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
    assert "st.button(" in form_source
    assert '"Apply parameters"' in form_source
    assert '"Test connection"' in form_source
    assert 'key="apply_load_params_button"' in form_source
    assert 'key="test_load_params_connection_button"' in form_source
    assert "if test_submitted:" in form_source
    assert form_source.index("if test_submitted:") < form_source.index("params = {")
