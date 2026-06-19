from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from csv_click.errors import (
    CertificateError,
    ClickHouseConnectionError,
    ExistingTableError,
)
from csv_click.schema import CsvSchema


DEFAULT_HOST = "tp17.wb-bank.ru"
DEFAULT_PORT = 443
DEFAULT_CLIENT_CERT = "/home/jovyan/tsh/clickhouse-prod.crt"
DEFAULT_CLIENT_KEY = "/home/jovyan/tsh/clickhouse-prod.key"


class ClickHouseClient(Protocol):
    def query(self, sql: str):
        ...

    def insert(self, table: str, data, column_names: list[str], database: str):
        ...


@dataclass
class ClickHouseConfig:
    database: str = "sandbox"
    cluster: str = "clickhouse"
    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    username: str = ""
    password: str = ""
    secure: bool = True
    verify: bool = False
    client_cert: str = DEFAULT_CLIENT_CERT
    client_key: str = DEFAULT_CLIENT_KEY


@dataclass
class TableNames:
    distributed: str
    local: str


def build_table_names(distributed_table: str) -> TableNames:
    quote_identifier(distributed_table)
    return TableNames(distributed=distributed_table, local=f"{distributed_table}_local")


def quote_identifier(identifier: str) -> str:
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
        return identifier
    raise ValueError(f"Unsafe ClickHouse identifier: {identifier}")


def get_client(config: ClickHouseConfig):
    _validate_certificate_files(config)
    try:
        import clickhouse_connect

        return clickhouse_connect.get_client(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password or None,
            secure=config.secure,
            verify=config.verify,
            client_cert=config.client_cert,
            client_cert_key=config.client_key,
            connect_timeout=60,
            send_receive_timeout=1800,
        )
    except CertificateError:
        raise
    except Exception as exc:
        raise ClickHouseConnectionError(f"ClickHouse client initialization failed: {exc}") from exc


def test_connection(client: ClickHouseClient) -> None:
    try:
        client.query("SELECT 1")
    except Exception as exc:
        raise ClickHouseConnectionError(f"ClickHouse SELECT 1 failed: {exc}") from exc


def ensure_tables_do_not_exist(
    client: ClickHouseClient,
    config: ClickHouseConfig,
    distributed_table: str,
    local_table: str,
) -> None:
    database = quote_identifier(config.database)
    cluster = quote_identifier(config.cluster)
    distributed_table = quote_identifier(distributed_table)
    local_table = quote_identifier(local_table)
    sql = f"""
SELECT
    hostName() AS host_name,
    database,
    name
FROM clusterAllReplicas('{cluster}', system.tables)
WHERE database = '{database}'
    AND name IN ('{distributed_table}', '{local_table}')
ORDER BY host_name, name
"""
    result = client.query(sql)
    rows = getattr(result, "result_rows", [])
    if rows:
        found = ", ".join(f"{row[1]}.{row[2]} on {row[0]}" for row in rows)
        raise ExistingTableError(f"Target table already exists: {found}")


def build_create_local_table_sql(
    database: str,
    table: str,
    cluster: str,
    schema: CsvSchema,
    order_by: str,
    partition_by: str | None = None,
) -> str:
    if not order_by.strip():
        raise ValueError("ORDER BY is required")
    database = quote_identifier(database)
    table = quote_identifier(table)
    cluster = quote_identifier(cluster)
    columns_sql = ",\n    ".join(
        f"`{column.column_name}` {column.final_type}" for column in schema.columns
    )
    partition_sql = f"\nPARTITION BY {partition_by.strip()}" if partition_by and partition_by.strip() else ""
    return f"""CREATE TABLE {database}.{table} ON CLUSTER '{cluster}'
(
    {columns_sql}
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{database}/{table}', '{{replica}}'){partition_sql}
ORDER BY {order_by.strip()}"""


def build_create_distributed_table_sql(
    database: str,
    distributed_table: str,
    local_table: str,
    cluster: str,
) -> str:
    database = quote_identifier(database)
    distributed_table = quote_identifier(distributed_table)
    local_table = quote_identifier(local_table)
    cluster = quote_identifier(cluster)
    return f"""CREATE TABLE {database}.{distributed_table} ON CLUSTER '{cluster}'
AS {database}.{local_table}
ENGINE = Distributed('{cluster}', '{database}', '{local_table}', rand())"""


def create_tables(
    client: ClickHouseClient,
    config: ClickHouseConfig,
    schema: CsvSchema,
    distributed_table: str,
    order_by: str,
    partition_by: str | None = None,
) -> TableNames:
    names = build_table_names(distributed_table)
    ensure_tables_do_not_exist(client, config, names.distributed, names.local)
    client.query(
        build_create_local_table_sql(
            database=config.database,
            table=names.local,
            cluster=config.cluster,
            schema=schema,
            order_by=order_by,
            partition_by=partition_by,
        )
    )
    client.query(
        build_create_distributed_table_sql(
            database=config.database,
            distributed_table=names.distributed,
            local_table=names.local,
            cluster=config.cluster,
        )
    )
    return names


def insert_batch(
    client: ClickHouseClient,
    config: ClickHouseConfig,
    distributed_table: str,
    schema: CsvSchema,
    rows: list[list[object]],
) -> None:
    client.insert(
        table=distributed_table,
        data=rows,
        column_names=schema.column_names,
        database=config.database,
    )


def _validate_certificate_files(config: ClickHouseConfig) -> None:
    if not config.secure:
        return
    missing = [
        path
        for path in [config.client_cert, config.client_key]
        if path and not Path(path).exists()
    ]
    if missing:
        raise CertificateError("ClickHouse certificate file is missing: " + ", ".join(missing))
