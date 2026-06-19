# CSV to ClickHouse

Streamlit UI for loading server-side CSV files into new ClickHouse local and
distributed tables from a JupyterHub terminal.

The load path uses chunked pandas reads and ClickHouse `raw_insert` with
`JSONEachRow`. This is the default path because HTTP/proxy deployments can fail
on `client.insert()` / `insert_df()` while accepting JSONEachRow payloads.

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
- The app scans CSV chunks to infer ClickHouse types.
- Default CSV encoding is `utf_8`.
- Default separator is `,`.
- Encoding and separator are selectable in the UI, including `cp1251` and `;`.
- The inferred schema can be edited before DDL generation and load, including
  source-to-target column names and include/exclude flags.
- Only the distributed table name is entered manually.
- The local table name is generated as `{distributed_table}_local`.
- If either target table already exists on the cluster, the load is blocked before DDL or insert.
- Data is inserted sequentially in pandas chunks, default `1_000_000` rows.
- Strict preflight validation is enabled by default, so selected types are checked before DDL/load.

Default ClickHouse certificate paths match the source notebook:

- `/home/jovyan/tsh/clickhouse-prod.crt`
- `/home/jovyan/tsh/clickhouse-prod.key`
