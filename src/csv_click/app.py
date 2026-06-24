from __future__ import annotations

import os
import sys
import time
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import streamlit as st

from csv_click.clickhouse import (
    ClickHouseConfig,
    build_create_distributed_table_sql,
    build_create_local_table_sql,
    build_table_names,
    create_tables,
    drop_target_tables,
    get_client,
    test_connection,
)
from csv_click.errors import (
    CertificateError,
    ClickHouseConnectionError,
    CsvClickError,
    CsvReadCancelled,
    CsvSchemaError,
    ExistingTableError,
)
from csv_click.pandas_loader import (
    DEFAULT_SCHEMA_SAMPLE_ROWS,
    ReadOptions,
    analyze_csv_with_pandas_sample,
    analyze_csv_with_pandas_chunks,
    choose_read_options_for_preview,
    load_csv_via_raw_insert,
    mappings_from_editor_rows,
    mappings_to_editor_rows,
    mappings_to_schema,
    schema_to_mappings,
    validate_csv_with_pandas_chunks,
)
from csv_click.schema import (
    CLICKHOUSE_TYPE_OPTIONS,
    CsvSchema,
)
from csv_click.settings import AppSettings, load_app_settings, save_app_settings


SCHEMA_INFERENCE_SAMPLE = "Fast sample, 100000 rows"
SCHEMA_INFERENCE_FULL_SCAN = "Full scan"
SCHEMA_INFERENCE_OPTIONS = [SCHEMA_INFERENCE_SAMPLE, SCHEMA_INFERENCE_FULL_SCAN]
CSV_READ_STATE_KEYS = [
    "csv_path",
    "schema_rows",
    "mapping_rows",
    "type_rows",
    "mapping_confirmed",
    "types_confirmed",
    "load_params",
    "csv_preview_rows",
    "csv_preview_warning",
]


def main() -> None:
    st.set_page_config(page_title="CSV to ClickHouse", layout="wide")
    st.title("CSV to ClickHouse")

    csv_context = _render_csv_path_step()
    if not csv_context:
        return

    mappings = _render_column_mapping_editor()
    if mappings is None:
        return

    schema = _render_type_editor()
    if schema is None:
        return

    params = _render_connection_and_load_form(schema)
    if not params:
        return

    config = params["config"]
    csv_path = csv_context["csv_path"]
    read_options = params["read_options"]
    distributed_table = params["distributed_table"]
    table_names = build_table_names(distributed_table)

    st.info(f"Local table: `{table_names.local}`")

    col_test, _ = st.columns(2)
    with col_test:
        if st.button("Test connection", type="secondary", use_container_width=True):
            _test_connection(config)

    try:
        ddl_local = build_create_local_table_sql(
            database=config.database,
            table=table_names.local,
            cluster=config.cluster,
            schema=schema,
            order_by=params["order_by"],
            partition_by=params["partition_by"],
        )
        ddl_distributed = build_create_distributed_table_sql(
            database=config.database,
            distributed_table=table_names.distributed,
            local_table=table_names.local,
            cluster=config.cluster,
            sharding_key=params["sharding_key"],
        )
    except ValueError as exc:
        st.error(f"DDL parameter error: {exc}")
        return

    _render_final_actions_help()

    if st.button("Preview DDL", use_container_width=True):
        st.subheader("Local table DDL")
        st.code(ddl_local, language="sql")
        st.subheader("Distributed table DDL")
        st.code(ddl_distributed, language="sql")

    if st.button("Create tables and load", type="primary", use_container_width=True):
        _create_and_load(
            config=config,
            csv_path=csv_path,
            read_options=read_options,
            schema=schema,
            distributed_table=distributed_table,
            order_by=params["order_by"],
            partition_by=params["partition_by"],
            batch_size=params["batch_size"],
            max_insert_payload_mb=params["max_insert_payload_mb"],
            strict_preflight=params["strict_preflight"],
            sharding_key=params["sharding_key"],
        )


def _render_step_help(title: str, body: str) -> None:
    with st.expander(title, expanded=False):
        st.markdown(body)


def _render_csv_path_help() -> None:
    _render_step_help(
        "Как указать CSV файл",
        r"""
Укажите путь к CSV файлу на той машине, где запущен Streamlit. Если приложение
запущено на сервере или в JupyterHub, путь должен быть серверным, а не путем с
вашего локального компьютера.

Примеры:

- Windows: `C:\Users\<user>\Downloads\data.csv`
- Linux/JupyterHub: `/home/jovyan/data/data.csv`

`Separator` задает разделитель колонок: `,`, `;`, `\t`, `|` или свое значение
через `custom`. `Encoding` задает кодировку файла: обычно `utf_8`, для русских
CSV из Windows часто подходит `cp1251` или `windows-1251`.

После `Read CSV` приложение прочитает preview, подберет эффективные настройки
чтения и определит начальную схему колонок.
""",
    )


def _render_column_mapping_help() -> None:
    _render_step_help(
        "Как настроить колонки",
        """
Проверьте, какие колонки из CSV попадут в ClickHouse:

- `source_name` - исходное имя колонки в CSV;
- target_name станет именем колонки в ClickHouse;
- `include` определяет, загружать колонку или исключить ее.

Пустые `target_name` и дублирующиеся целевые имена не допускаются. После проверки
нажмите `Apply column mapping`, чтобы перейти к типам.
""",
    )


def _render_type_review_help() -> None:
    _render_step_help(
        "Как проверить типы",
        """
Проверьте типы перед созданием таблицы:

- `inferred_type` - тип, который приложение определило по CSV;
- `final_type` - итоговый тип ClickHouse для загрузки;
- custom_type переопределяет final_type, если поле заполнено;
- `nullable` оборачивает тип в `Nullable(...)`;
- `sample_values` помогает сверить тип с реальными значениями.

Примеры: `Decimal(18, 2)` для денежных сумм, `Nullable(DateTime)` для дат и
времени с пропусками.
""",
    )


def _render_clickhouse_params_help() -> None:
    _render_step_help(
        "Как заполнить параметры ClickHouse",
        """
Заполните параметры целевой таблицы и подключения:

- `Database` - база ClickHouse, куда будет создана таблица;
- `Distributed table name` - имя распределенной таблицы;
- `Cluster` - кластер для `ON CLUSTER`;
- для my_table будет создана локальная таблица my_table_local;
- `ORDER BY` - ключ сортировки локальной `ReplicatedMergeTree`;
- `PARTITION BY` - необязательное выражение партиционирования;
- `Distributed sharding key` - колонка для `sipHash64(...)` в `Distributed`;
- `Batch size` - размер CSV чанка при проверке и загрузке;
- `Max insert payload, MB` - максимальный размер одного HTTP `JSONEachRow` insert request;
- `Strict preflight validation` заранее проверяет конвертацию CSV в выбранные
  типы ClickHouse.

Пример: ORDER BY = customer_id, `PARTITION BY = toYYYYMM(dt)`,
`sharding key = customer_id`.
""",
    )


def _render_final_actions_help() -> None:
    _render_step_help(
        "Порядок финальных действий",
        """
Рекомендуемый порядок:

1. Нажмите `Test connection`, чтобы проверить подключение через `SELECT 1`.
2. Нажмите `Preview DDL`, если нужно посмотреть SQL перед созданием таблиц.
3. Нажмите `Create tables and load`, чтобы создать таблицы и загрузить CSV.

Preview DDL только показывает SQL и ничего не создает. `Create tables and load`
создает local и distributed таблицы, затем загружает CSV чанками через
`JSONEachRow`.

Загрузка блокируется, если distributed или local таблица уже существует. Для
`my_table` проверяются `my_table` и `my_table_local`.
""",
    )


def _render_connection_and_load_form(schema: CsvSchema) -> dict[str, object] | None:
    settings = _get_app_settings()
    target_names = _schema_target_names(schema)
    with st.form("load_params_form"):
        st.subheader("ClickHouse and load parameters")
        _render_clickhouse_params_help()
        left, right = st.columns(2)
        with left:
            database = st.text_input("Database", value=settings.database)
            distributed_table = st.text_input("Distributed table name")
            cluster = st.text_input("Cluster", value=settings.cluster)
            order_by = st.selectbox("ORDER BY", options=target_names)
            partition_by = st.text_input("PARTITION BY (optional)")
            sharding_column = st.selectbox("Distributed sharding key", options=target_names)
            batch_size = st.number_input(
                "Batch size",
                min_value=1,
                value=settings.batch_size,
                step=10_000,
            )
            max_insert_payload_mb = st.number_input(
                "Max insert payload, MB",
                min_value=1,
                value=settings.max_insert_payload_mb,
                step=1,
                help="Upper bound for one HTTP JSONEachRow insert request.",
            )
            strict_preflight = st.checkbox(
                "Strict preflight validation",
                value=settings.strict_preflight,
            )
        with right:
            host = st.text_input("Host", value=settings.host)
            port = st.number_input("Port", min_value=1, max_value=65535, value=settings.port)
            username = st.text_input(
                "Username",
                value=settings.username or os.getenv("CLICKHOUSE_USER", ""),
            )
            password = st.text_input(
                "Password",
                value=os.getenv("CLICKHOUSE_PASSWORD", ""),
                type="password",
            )
            secure = st.checkbox("Secure", value=settings.secure)
            verify = st.checkbox("Verify TLS", value=settings.verify)
            client_cert = st.text_input("Client cert path", value=settings.client_cert)
            client_key = st.text_input("Client key path", value=settings.client_key)

        apply_col, test_col = st.columns(2)
        with apply_col:
            submitted = st.form_submit_button("Apply parameters", use_container_width=True)
        with test_col:
            test_submitted = st.form_submit_button("Test connection", use_container_width=True)

    config = ClickHouseConfig(
        database=database,
        cluster=cluster,
        host=host,
        port=int(port),
        username=username,
        password=password,
        secure=secure,
        verify=verify,
        client_cert=client_cert,
        client_key=client_key,
    )

    if test_submitted:
        _test_connection(config)

    if not submitted and "load_params" not in st.session_state:
        return None
    if not submitted:
        return st.session_state["load_params"]

    errors = []
    if not database:
        errors.append("Database is required")
    if not distributed_table:
        errors.append("Distributed table name is required")
    if not order_by:
        errors.append("ORDER BY is required")
    if not sharding_column:
        errors.append("Distributed sharding key is required")

    if errors:
        for error in errors:
            st.error(error)
        return None

    csv_read_options = st.session_state.get("csv_read_options", ReadOptions())
    read_options = ReadOptions(
        separator=csv_read_options.separator,
        encoding=csv_read_options.encoding,
        batch_size=int(batch_size),
    )
    params = {
        "read_options": read_options,
        "distributed_table": distributed_table,
        "order_by": order_by,
        "partition_by": partition_by or None,
        "sharding_key": sharding_column,
        "batch_size": int(batch_size),
        "max_insert_payload_mb": int(max_insert_payload_mb),
        "strict_preflight": strict_preflight,
        "config": config,
    }
    st.session_state["load_params"] = params
    _save_app_settings(
        AppSettings(
            host=host,
            port=int(port),
            username=username,
            secure=secure,
            verify=verify,
            client_cert=client_cert,
            client_key=client_key,
            database=database,
            cluster=cluster,
            batch_size=int(batch_size),
            max_insert_payload_mb=int(max_insert_payload_mb),
            strict_preflight=strict_preflight,
            separator=read_options.separator,
            encoding=read_options.encoding,
        )
    )
    return params


def _get_app_settings() -> AppSettings:
    if "app_settings" not in st.session_state:
        st.session_state["app_settings"] = load_app_settings()
    return st.session_state["app_settings"]


def _save_app_settings(settings: AppSettings) -> None:
    try:
        save_app_settings(settings)
    except OSError as exc:
        st.warning(f"Could not save UI settings: {exc}")
    else:
        st.session_state["app_settings"] = settings


def _render_csv_path_step() -> dict[str, object] | None:
    current_options = st.session_state.get(
        "csv_read_options",
        _read_options_from_settings(_get_app_settings()),
    )
    _render_csv_path_help()
    with st.form("csv_path_form"):
        csv_col, button_col = st.columns([5, 1])
        with csv_col:
            csv_path = st.text_input("CSV path", value=st.session_state.get("csv_path", ""))
        with button_col:
            st.write("")
            submitted = st.form_submit_button("Read CSV", use_container_width=True)

        st.subheader("CSV read settings")
        settings_left, settings_right = st.columns(2)
        with settings_left:
            separator_choice = st.selectbox(
                "Separator",
                options=[",", ";", "\\t", "|", "custom"],
                index=_separator_index(current_options.separator),
            )
            custom_separator = st.text_input(
                "Custom separator",
                value="" if current_options.separator in {",", ";", "\t", "|"} else current_options.separator,
            )
        with settings_right:
            encoding_choice = st.selectbox(
                "Encoding",
                options=["utf_8", "cp1251", "windows-1251", "utf-8-sig", "custom"],
                index=_encoding_index(current_options.encoding),
            )
            custom_encoding = st.text_input(
                "Custom encoding",
                value="" if current_options.encoding in {"utf_8", "cp1251", "windows-1251", "utf-8-sig"} else current_options.encoding,
            )
        current_schema_analysis_mode = st.session_state.get(
            "schema_analysis_mode",
            SCHEMA_INFERENCE_SAMPLE,
        )
        schema_analysis_mode = st.radio(
            "Schema inference mode",
            options=SCHEMA_INFERENCE_OPTIONS,
            index=0
            if current_schema_analysis_mode not in SCHEMA_INFERENCE_OPTIONS
            else SCHEMA_INFERENCE_OPTIONS.index(current_schema_analysis_mode),
            horizontal=True,
            help=(
                "Fast sample reads only the first 100000 rows for draft types. "
                "Full scan reads the entire CSV before type review."
            ),
        )

    if submitted:
        read_options = _read_options_from_form(
            separator_choice=separator_choice,
            custom_separator=custom_separator,
            encoding_choice=encoding_choice,
            custom_encoding=custom_encoding,
            current_options=current_options,
        )
        if read_options is not None:
            st.session_state["csv_read_options"] = read_options
            st.session_state["schema_analysis_mode"] = schema_analysis_mode
            _apply_csv_path(csv_path, read_options, schema_analysis_mode)

    if "csv_path" not in st.session_state:
        return None

    if st.button("Stop read CSV / choose another file", type="secondary", use_container_width=True):
        _request_stop_csv_read()
        st.rerun()

    st.caption(f"CSV path: `{st.session_state['csv_path']}`")
    _render_csv_preview()
    return {
        "csv_path": st.session_state["csv_path"],
        "read_options": st.session_state.get(
            "csv_read_options",
            _read_options_from_settings(_get_app_settings()),
        ),
    }


def _read_options_from_form(
    *,
    separator_choice: str,
    custom_separator: str,
    encoding_choice: str,
    custom_encoding: str,
    current_options: ReadOptions,
) -> ReadOptions | None:
    separator = custom_separator if separator_choice == "custom" else separator_choice
    separator = "\t" if separator == "\\t" else separator
    encoding = custom_encoding if encoding_choice == "custom" else encoding_choice
    if not separator:
        st.error("Separator is required")
        return None
    if not encoding:
        st.error("Encoding is required")
        return None
    return ReadOptions(
        separator=separator,
        encoding=encoding,
        batch_size=current_options.batch_size,
    )


def _render_csv_preview() -> None:
    preview = st.session_state.get("csv_preview_rows")
    if preview is None:
        return
    st.subheader("CSV preview")
    warning = st.session_state.get("csv_preview_warning")
    if warning:
        st.warning(str(warning))
    st.dataframe(preview, hide_index=True, use_container_width=True)


def _schema_target_names(schema: CsvSchema) -> list[str]:
    return [column.column_name for column in schema.columns]


def _separator_index(separator: str) -> int:
    normalized = "\\t" if separator == "\t" else separator
    options = [",", ";", "\\t", "|", "custom"]
    return options.index(normalized) if normalized in options else options.index("custom")


def _encoding_index(encoding: str) -> int:
    options = ["utf_8", "cp1251", "windows-1251", "utf-8-sig", "custom"]
    return options.index(encoding) if encoding in options else options.index("custom")


def _apply_csv_path(
    csv_path: str,
    read_options: ReadOptions,
    schema_analysis_mode: str = SCHEMA_INFERENCE_SAMPLE,
) -> None:
    if not csv_path:
        st.error("CSV path is required")
        return
    if not Path(csv_path).exists():
        st.error(f"CSV path does not exist: {csv_path}")
        return

    st.session_state["csv_read_cancel_requested"] = False
    st.session_state["csv_path"] = csv_path
    st.session_state["csv_read_options"] = read_options
    st.session_state["schema_analysis_mode"] = schema_analysis_mode
    _clear_csv_read_state(include_path=False)
    _analyze_csv(csv_path, read_options, schema_analysis_mode)


def _clear_csv_read_state(include_path: bool = True) -> None:
    for key in CSV_READ_STATE_KEYS:
        if key == "csv_path" and not include_path:
            continue
        st.session_state.pop(key, None)


def _request_stop_csv_read() -> None:
    st.session_state["csv_read_cancel_requested"] = True
    _clear_csv_read_state()


def _csv_read_cancel_requested() -> bool:
    return bool(st.session_state.get("csv_read_cancel_requested", False))


def _read_options_from_settings(settings: AppSettings) -> ReadOptions:
    return ReadOptions(
        separator=settings.separator,
        encoding=settings.encoding,
        batch_size=settings.batch_size,
    )


def _test_connection(config: ClickHouseConfig) -> None:
    try:
        client = get_client(config)
        test_connection(client)
    except CertificateError as exc:
        st.error(f"Certificate error: {exc}")
    except ClickHouseConnectionError as exc:
        st.error(f"ClickHouse connection error: {exc}")
    else:
        st.success("Connection OK")


def _analyze_csv(
    csv_path: str,
    read_options: ReadOptions,
    schema_analysis_mode: str = SCHEMA_INFERENCE_SAMPLE,
) -> None:
    try:
        spinner_text = (
            "Scanning full CSV chunks to infer schema..."
            if schema_analysis_mode == SCHEMA_INFERENCE_FULL_SCAN
            else f"Scanning first {DEFAULT_SCHEMA_SAMPLE_ROWS} rows to infer draft schema..."
        )
        with st.spinner(spinner_text):
            if _csv_read_cancel_requested():
                raise CsvReadCancelled("CSV read was stopped")
            effective_options, preview, warning = choose_read_options_for_preview(
                csv_path,
                read_options,
                nrows=20,
            )
            if _csv_read_cancel_requested():
                raise CsvReadCancelled("CSV read was stopped")
            if schema_analysis_mode == SCHEMA_INFERENCE_FULL_SCAN:
                schema = analyze_csv_with_pandas_chunks(
                    csv_path,
                    effective_options,
                    cancel_callback=_csv_read_cancel_requested,
                )
            else:
                schema = analyze_csv_with_pandas_sample(
                    csv_path,
                    effective_options,
                    nrows=DEFAULT_SCHEMA_SAMPLE_ROWS,
                )
            if _csv_read_cancel_requested():
                raise CsvReadCancelled("CSV read was stopped")
    except CsvReadCancelled:
        _clear_csv_read_state()
        st.warning("CSV read was stopped. Choose another file and press Read CSV.")
        return
    except CsvSchemaError as exc:
        st.error(f"CSV schema error: {exc}")
        return

    st.session_state["csv_read_options"] = effective_options
    st.session_state["schema_rows"] = mappings_to_editor_rows(schema_to_mappings(schema))
    st.session_state["mapping_rows"] = _schema_rows_to_mapping_rows(st.session_state["schema_rows"])
    st.session_state["csv_preview_rows"] = preview
    st.session_state["csv_preview_warning"] = warning.message if warning else None
    if schema_analysis_mode != SCHEMA_INFERENCE_FULL_SCAN:
        st.warning(
            "Schema was inferred from the first 100000 rows only. Review Type review carefully; "
            "use Full scan for exact inference or Strict preflight validation before loading."
        )
    st.success(f"Schema inferred for {len(schema.columns)} columns")


def _render_column_mapping_editor() -> list[dict[str, object]] | None:
    schema_rows = st.session_state.get("schema_rows")
    if not schema_rows:
        return None

    st.subheader("Column mapping")
    _render_column_mapping_help()
    mapping_rows = st.session_state.get("mapping_rows") or _schema_rows_to_mapping_rows(schema_rows)
    edited = st.data_editor(
        pd.DataFrame(mapping_rows),
        hide_index=True,
        use_container_width=True,
        disabled=["source_name"],
        column_config={
            "include": st.column_config.CheckboxColumn("include"),
            "target_name": st.column_config.TextColumn("target_name", required=True),
        },
        key="mapping_editor",
    )
    edited_rows = edited.to_dict(orient="records")
    st.session_state["mapping_rows"] = edited_rows

    if st.button("Apply column mapping", type="primary", use_container_width=True):
        try:
            _validate_mapping_rows(edited_rows)
        except CsvSchemaError as exc:
            st.error(f"Column mapping error: {exc}")
            return None
        st.session_state["mapping_confirmed"] = True
        st.session_state["types_confirmed"] = False
        st.session_state.pop("load_params", None)
        st.session_state["type_rows"] = _type_rows_from_mapping(schema_rows, edited_rows)

    if not st.session_state.get("mapping_confirmed"):
        st.info("Apply column mapping to continue to type review.")
        return None

    return st.session_state["mapping_rows"]


def _render_type_editor() -> CsvSchema | None:
    rows = st.session_state.get("type_rows")
    if not rows:
        return None

    st.subheader("Type review")
    _render_type_review_help()
    edited = st.data_editor(
        pd.DataFrame(rows),
        hide_index=True,
        use_container_width=True,
        disabled=["source_name", "target_name", "inferred_type", "sample_values", "notes"],
        column_config={
            "final_type": st.column_config.SelectboxColumn(
                "final_type",
                options=CLICKHOUSE_TYPE_OPTIONS,
                required=True,
            ),
            "custom_type": st.column_config.TextColumn(
                "custom_type",
                help="Optional manual ClickHouse type. If filled, it overrides final_type.",
            ),
            "nullable": st.column_config.CheckboxColumn("nullable"),
        },
        key="type_editor",
    )
    edited_rows = edited.to_dict(orient="records")
    st.session_state["type_rows"] = edited_rows
    try:
        schema = mappings_to_schema(mappings_from_editor_rows(edited_rows))
    except CsvSchemaError as exc:
        st.error(f"Type editor error: {exc}")
        return None

    if st.button("Apply types", type="primary", use_container_width=True):
        st.session_state["types_confirmed"] = True
        st.session_state.pop("load_params", None)

    if not st.session_state.get("types_confirmed"):
        st.info("Apply types to continue to ClickHouse settings.")
        return None

    return schema


def _schema_rows_to_mapping_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "source_name": row["source_name"],
            "target_name": row["target_name"],
            "include": row.get("include", True),
        }
        for row in rows
    ]


def _type_rows_from_mapping(
    schema_rows: list[dict[str, object]],
    mapping_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    schema_by_source = {str(row["source_name"]): row for row in schema_rows}
    type_rows = []
    for mapping in mapping_rows:
        if not bool(mapping.get("include", True)):
            continue
        source_name = str(mapping["source_name"])
        source_row = schema_by_source[source_name]
        type_rows.append(
            {
                "source_name": source_name,
                "target_name": str(mapping["target_name"]),
                "include": True,
                "inferred_type": source_row["inferred_type"],
                "final_type": source_row["final_type"],
                "custom_type": "",
                "nullable": source_row["nullable"],
                "sample_values": source_row["sample_values"],
                "notes": source_row["notes"],
            }
        )
    return type_rows


def _validate_mapping_rows(rows: list[dict[str, object]]) -> None:
    target_names = [
        str(row.get("target_name", "")).strip()
        for row in rows
        if bool(row.get("include", True))
    ]
    if not target_names:
        raise CsvSchemaError("At least one column must be included")
    empty_names = [name for name in target_names if not name]
    if empty_names:
        raise CsvSchemaError("Target column name cannot be empty")
    duplicates = sorted({name for name in target_names if target_names.count(name) > 1})
    if duplicates:
        raise CsvSchemaError("Duplicate target column names: " + ", ".join(duplicates))


def _append_load_log(log_messages: list[str], message: str) -> None:
    log_messages.append(f"{time.strftime('%H:%M:%S')} {message}")


def _render_load_log(log_container, log_messages: list[str]) -> None:
    log_container.code("\n".join(log_messages), language="text")


def _format_load_error(exc: Exception) -> str:
    message = str(exc)
    normalized = message.lower()
    if "unknown_table" in normalized or "does not exist" in normalized:
        return (
            "ClickHouse load failed during load step because the target table is not visible "
            f"after DDL creation: {message}"
        )
    return f"Unexpected load error: {message}"


def _cleanup_after_failed_load(
    client,
    config: ClickHouseConfig,
    distributed_table: str,
    log_callback,
) -> None:
    table_names = build_table_names(distributed_table)
    log_callback("Load failed after table creation. Dropping target tables.")
    drop_target_tables(
        client=client,
        config=config,
        distributed_table=table_names.distributed,
        local_table=table_names.local,
        log_callback=log_callback,
    )


def _create_and_load(
    config: ClickHouseConfig,
    csv_path: str,
    read_options: ReadOptions,
    schema: CsvSchema,
    distributed_table: str,
    order_by: str,
    partition_by: str | None,
    batch_size: int,
    max_insert_payload_mb: int,
    strict_preflight: bool,
    sharding_key: str,
) -> None:
    progress = st.progress(0)
    status = st.empty()
    metrics = st.empty()
    log_container = st.empty()
    log_messages: list[str] = []
    start = time.time()
    inserted_rows = 0
    client = None
    tables_created = False

    def log(message: str) -> None:
        _append_load_log(log_messages, message)
        _render_load_log(log_container, log_messages)

    try:
        mappings = mappings_from_editor_rows(st.session_state["type_rows"])
        effective_read_options, _, encoding_warning = choose_read_options_for_preview(
            csv_path,
            read_options,
            nrows=20,
        )
        if encoding_warning:
            log(encoding_warning.message)
        st.session_state["csv_read_options"] = effective_read_options
        total_rows = 0
        if strict_preflight:
            log("Validating CSV chunks against selected types.")
            status.info("Validating CSV chunks against selected types...")
            total_rows = validate_csv_with_pandas_chunks(
                csv_path,
                effective_read_options,
                mappings,
                max_insert_payload_bytes=max_insert_payload_mb * 1024 * 1024,
            )
            log(f"CSV validation finished: {total_rows} rows.")

        log("Connecting to ClickHouse.")
        status.info("Connecting to ClickHouse...")
        client = get_client(config)
        test_connection(client)
        log("ClickHouse connection OK.")

        log("Checking existing tables and creating DDL.")
        status.info("Checking existing tables and creating DDL...")
        create_tables(
            client=client,
            config=config,
            schema=schema,
            distributed_table=distributed_table,
            order_by=order_by,
            partition_by=partition_by,
            sharding_key=sharding_key,
            log_callback=log,
        )
        tables_created = True
        log("Target tables are created and visible on cluster.")

        log("Loading CSV chunks through JSONEachRow.")
        status.info("Loading CSV chunks through JSONEachRow...")

        def on_progress(
            chunk_number: int,
            block_number: int,
            block_rows: int,
            rows_total: int,
            payload_bytes: int,
        ) -> None:
            nonlocal inserted_rows
            inserted_rows = rows_total
            if total_rows:
                progress.progress(min(1.0, inserted_rows / total_rows))
            metrics.metric("Inserted rows", inserted_rows)
            payload_mb = payload_bytes / 1024 / 1024
            status.info(f"Loaded chunk {chunk_number}, block {block_number}: {block_rows} rows")
            log(
                f"Loaded chunk {chunk_number}, block {block_number}: "
                f"{block_rows} rows, {payload_mb:.2f} MB, total {rows_total}."
            )

        inserted_rows = load_csv_via_raw_insert(
            client=client,
            csv_path=csv_path,
            read_options=effective_read_options,
            database=config.database,
            table=distributed_table,
            mappings=mappings,
            max_insert_payload_bytes=max_insert_payload_mb * 1024 * 1024,
            progress_callback=on_progress,
        )

        progress.progress(1.0)
        elapsed = time.time() - start
        log(f"Load finished: {inserted_rows} rows in {elapsed:.2f} sec.")
        status.success(f"Load finished: {inserted_rows} rows in {elapsed:.2f} sec")
    except CertificateError as exc:
        log(f"Certificate error: {exc}")
        status.error(f"Certificate error: {exc}")
    except ExistingTableError as exc:
        log(f"Existing table error: {exc}")
        status.error(f"Existing table error: {exc}")
    except (CsvSchemaError, ClickHouseConnectionError, CsvClickError) as exc:
        if tables_created and client is not None:
            try:
                _cleanup_after_failed_load(client, config, distributed_table, log)
            except Exception as cleanup_exc:
                log(f"Cleanup error: {cleanup_exc}")
        log(str(exc))
        status.error(str(exc))
    except Exception as exc:
        if tables_created and client is not None:
            try:
                _cleanup_after_failed_load(client, config, distributed_table, log)
            except Exception as cleanup_exc:
                log(f"Cleanup error: {cleanup_exc}")
        message = _format_load_error(exc)
        log(message)
        status.error(message)


if __name__ == "__main__":
    main()
