from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from csv_click.clickhouse import (
    DEFAULT_CLIENT_CERT,
    DEFAULT_CLIENT_KEY,
    DEFAULT_HOST,
    DEFAULT_PORT,
    ClickHouseConfig,
    build_create_distributed_table_sql,
    build_create_local_table_sql,
    build_table_names,
    create_tables,
    get_client,
    insert_batch,
    test_connection,
)
from csv_click.csv_reader import iter_csv_batches
from csv_click.errors import (
    CertificateError,
    ClickHouseConnectionError,
    CsvClickError,
    CsvSchemaError,
    ExistingTableError,
)
from csv_click.schema import (
    CLICKHOUSE_TYPE_OPTIONS,
    CsvSchema,
    analyze_csv_schema,
    schema_from_editor_rows,
    schema_to_editor_rows,
    validate_csv_against_schema,
)


def main() -> None:
    st.set_page_config(page_title="CSV to ClickHouse", layout="wide")
    st.title("CSV to ClickHouse")

    params = _render_connection_and_load_form()
    if not params:
        return

    config = params["config"]
    csv_path = params["csv_path"]
    delimiter = params["delimiter"]
    distributed_table = params["distributed_table"]
    table_names = build_table_names(distributed_table)

    st.info(f"Local table: `{table_names.local}`")

    col_test, col_analyze = st.columns(2)
    with col_test:
        if st.button("Test connection", type="secondary", use_container_width=True):
            _test_connection(config)

    with col_analyze:
        if st.button("Analyze CSV", type="primary", use_container_width=True):
            _analyze_csv(csv_path, delimiter)

    schema = _render_schema_editor()
    if schema is None:
        return

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
    )

    if st.button("Preview DDL", use_container_width=True):
        st.subheader("Local table DDL")
        st.code(ddl_local, language="sql")
        st.subheader("Distributed table DDL")
        st.code(ddl_distributed, language="sql")

    if st.button("Create tables and load", type="primary", use_container_width=True):
        _create_and_load(
            config=config,
            csv_path=csv_path,
            delimiter=delimiter,
            schema=schema,
            distributed_table=distributed_table,
            order_by=params["order_by"],
            partition_by=params["partition_by"],
            batch_size=params["batch_size"],
        )


def _render_connection_and_load_form() -> dict[str, object] | None:
    with st.form("load_params"):
        st.subheader("Load parameters")
        left, right = st.columns(2)
        with left:
            csv_path = st.text_input("CSV path")
            database = st.text_input("Database", value="sandbox")
            distributed_table = st.text_input("Distributed table name")
            cluster = st.text_input("Cluster", value="clickhouse")
            order_by = st.text_input("ORDER BY")
            partition_by = st.text_input("PARTITION BY (optional)")
            batch_size = st.number_input("Batch size", min_value=1, value=1_000_000, step=10_000)
            delimiter = st.text_input("Delimiter (optional)", value="")
        with right:
            host = st.text_input("Host", value=DEFAULT_HOST)
            port = st.number_input("Port", min_value=1, max_value=65535, value=DEFAULT_PORT)
            username = st.text_input("Username", value=os.getenv("CLICKHOUSE_USER", ""))
            password = st.text_input(
                "Password",
                value=os.getenv("CLICKHOUSE_PASSWORD", ""),
                type="password",
            )
            secure = st.checkbox("Secure", value=True)
            verify = st.checkbox("Verify TLS", value=False)
            client_cert = st.text_input("Client cert path", value=DEFAULT_CLIENT_CERT)
            client_key = st.text_input("Client key path", value=DEFAULT_CLIENT_KEY)

        submitted = st.form_submit_button("Apply parameters", use_container_width=True)

    if not submitted and "load_params" not in st.session_state:
        return None

    errors = []
    if not csv_path:
        errors.append("CSV path is required")
    elif not Path(csv_path).exists():
        errors.append(f"CSV path does not exist: {csv_path}")
    if not database:
        errors.append("Database is required")
    if not distributed_table:
        errors.append("Distributed table name is required")
    if not order_by:
        errors.append("ORDER BY is required")

    if errors:
        for error in errors:
            st.error(error)
        return None

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
    params = {
        "csv_path": csv_path,
        "delimiter": delimiter or None,
        "distributed_table": distributed_table,
        "order_by": order_by,
        "partition_by": partition_by or None,
        "batch_size": int(batch_size),
        "config": config,
    }
    st.session_state["load_params"] = params
    return params


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


def _analyze_csv(csv_path: str, delimiter: str | None) -> None:
    try:
        with st.spinner("Scanning full CSV to infer schema..."):
            schema = analyze_csv_schema(csv_path, delimiter)
    except CsvSchemaError as exc:
        st.error(f"CSV schema error: {exc}")
        return

    st.session_state["schema_rows"] = schema_to_editor_rows(schema)
    st.success(f"Schema inferred for {len(schema.columns)} columns")


def _render_schema_editor() -> CsvSchema | None:
    rows = st.session_state.get("schema_rows")
    if not rows:
        st.warning("Analyze CSV before previewing DDL or loading data.")
        return None

    st.subheader("Schema")
    edited = st.data_editor(
        pd.DataFrame(rows),
        hide_index=True,
        use_container_width=True,
        disabled=["column_name", "source_name", "inferred_type", "sample_values", "notes"],
        column_config={
            "final_type": st.column_config.SelectboxColumn(
                "final_type",
                options=CLICKHOUSE_TYPE_OPTIONS,
                required=True,
            ),
            "nullable": st.column_config.CheckboxColumn("nullable"),
        },
        key="schema_editor",
    )
    edited_rows = edited.to_dict(orient="records")
    st.session_state["schema_rows"] = edited_rows
    try:
        return schema_from_editor_rows(edited_rows)
    except CsvSchemaError as exc:
        st.error(f"Schema editor error: {exc}")
        return None


def _create_and_load(
    config: ClickHouseConfig,
    csv_path: str,
    delimiter: str | None,
    schema: CsvSchema,
    distributed_table: str,
    order_by: str,
    partition_by: str | None,
    batch_size: int,
) -> None:
    progress = st.progress(0)
    status = st.empty()
    metrics = st.empty()
    start = time.time()
    inserted_rows = 0

    try:
        status.info("Validating CSV against selected types...")
        total_rows = validate_csv_against_schema(csv_path, schema, delimiter)

        status.info("Connecting to ClickHouse...")
        client = get_client(config)
        test_connection(client)

        status.info("Checking existing tables and creating DDL...")
        create_tables(
            client=client,
            config=config,
            schema=schema,
            distributed_table=distributed_table,
            order_by=order_by,
            partition_by=partition_by,
        )

        status.info("Loading CSV batches...")
        batch_number = 0
        for batch in iter_csv_batches(csv_path, schema, batch_size, delimiter):
            batch_number += 1
            insert_batch(client, config, distributed_table, schema, batch)
            inserted_rows += len(batch)
            progress.progress(min(1.0, inserted_rows / total_rows if total_rows else 1.0))
            metrics.metric("Inserted rows", inserted_rows)
            status.info(f"Loaded batch {batch_number}: {len(batch)} rows")

        progress.progress(1.0)
        elapsed = time.time() - start
        status.success(f"Load finished: {inserted_rows} rows in {elapsed:.2f} sec")
    except CertificateError as exc:
        status.error(f"Certificate error: {exc}")
    except ExistingTableError as exc:
        status.error(f"Existing table error: {exc}")
    except (CsvSchemaError, ClickHouseConnectionError, CsvClickError) as exc:
        status.error(str(exc))
    except Exception as exc:
        status.error(f"Unexpected load error: {exc}")


if __name__ == "__main__":
    main()
