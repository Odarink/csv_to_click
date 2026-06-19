# CSV to ClickHouse

Streamlit UI for loading server-side CSV files into new ClickHouse local and
distributed tables from a JupyterHub terminal.

## Run

```bash
uv run streamlit run src/csv_click/app.py --server.address 0.0.0.0 --server.port 8501
```

Open through JupyterHub proxy:

```text
https://<jupyterhub-host>/user/<username>/proxy/8501/
```

## Behavior

- The CSV file must have a header.
- The app scans the full CSV to infer ClickHouse types.
- The inferred schema can be edited before DDL generation and load.
- Only the distributed table name is entered manually.
- The local table name is generated as `{distributed_table}_local`.
- If either target table already exists on the cluster, the load is blocked before DDL or insert.
- Data is inserted sequentially in batches, default `1_000_000` rows.

Default ClickHouse certificate paths match the source notebook:

- `/home/jovyan/tsh/clickhouse-prod.crt`
- `/home/jovyan/tsh/clickhouse-prod.key`
