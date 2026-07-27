from __future__ import annotations

import re
import ssl
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Protocol

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

    def raw_insert(self, **kwargs):
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


def quote_column_identifier(identifier: str) -> str:
    identifier = identifier.strip()
    if not identifier or "`" in identifier or "\x00" in identifier:
        raise ValueError(f"Unsafe ClickHouse column identifier: {identifier}")
    return f"`{identifier}`"


def get_client(config: ClickHouseConfig):
    _validate_certificate_files(config)
    try:
        import clickhouse_connect

        kwargs = {
            "host": config.host,
            "port": config.port,
            "username": config.username,
            "password": config.password or None,
            "secure": config.secure,
            "verify": config.verify,
            "connect_timeout": 60,
            "send_receive_timeout": 1800,
        }
        if config.secure:
            kwargs["client_cert"] = config.client_cert
            kwargs["client_cert_key"] = config.client_key

        return clickhouse_connect.get_client(**kwargs)
    except CertificateError:
        raise
    except Exception as exc:
        raise ClickHouseConnectionError(
            f"ClickHouse client initialization failed: {_format_connection_error(config, exc)}"
        ) from exc


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
    existing = _existing_target_tables(client, config, distributed_table, local_table)
    if existing:
        found = ", ".join(sorted(existing))
        raise ExistingTableError(f"Target table already exists: {found}")


def _existing_target_tables(
    client: ClickHouseClient,
    config: ClickHouseConfig,
    distributed_table: str,
    local_table: str,
) -> set[str]:
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
    return {str(row[2]) for row in rows}


def drop_target_tables(
    client: ClickHouseClient,
    config: ClickHouseConfig,
    distributed_table: str,
    local_table: str,
    log_callback: Callable[[str], None] | None = None,
) -> None:
    database = quote_identifier(config.database)
    cluster = quote_identifier(config.cluster)
    distributed_table = quote_identifier(distributed_table)
    local_table = quote_identifier(local_table)
    for table in [distributed_table, local_table]:
        sql = f"DROP TABLE IF EXISTS {database}.{table}\nON CLUSTER {cluster}"
        if log_callback:
            log_callback(f"Cleanup: {database}.{table}")
        client.query(sql)


def _table_exists_on_cluster(
    client: ClickHouseClient,
    config: ClickHouseConfig,
    table: str,
) -> bool:
    database = quote_identifier(config.database)
    cluster = quote_identifier(config.cluster)
    table = quote_identifier(table)
    sql = f"""
SELECT
    hostName() AS host_name,
    database,
    name
FROM clusterAllReplicas('{cluster}', system.tables)
WHERE database = '{database}'
    AND name = '{table}'
ORDER BY host_name, name
"""
    result = client.query(sql)
    return bool(getattr(result, "result_rows", []))


def _verify_table_exists_on_cluster(
    client: ClickHouseClient,
    config: ClickHouseConfig,
    table: str,
    attempts: int = 30,
    interval_seconds: float = 1.0,
) -> None:
    for attempt in range(max(1, attempts)):
        if _table_exists_on_cluster(client, config, table):
            return
        if attempt + 1 < attempts:
            time.sleep(interval_seconds)
    else:
        raise ClickHouseConnectionError(
            f"Table {config.database}.{table} is not visible on cluster {config.cluster} after CREATE"
        )


def _validate_single_statement_expression(label: str, expression: str | None) -> None:
    if expression and ";" in expression:
        raise ValueError(f"{label} cannot contain semicolon")


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
    _validate_single_statement_expression("PARTITION BY", partition_by)
    database = quote_identifier(database)
    table = quote_identifier(table)
    cluster = quote_identifier(cluster)
    order_by_sql = quote_column_identifier(order_by)
    columns_sql = ",\n    ".join(
        f"`{column.column_name}` {column.final_type}" for column in schema.columns
    )
    partition_sql = f"\nPARTITION BY {partition_by.strip()}" if partition_by and partition_by.strip() else ""
    return f"""CREATE TABLE {database}.{table}
ON CLUSTER {cluster}
(
    {columns_sql}
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{{shard}}-{{uuid}}/{table}',
 '{{replica}}'){partition_sql}
ORDER BY {order_by_sql}
SETTINGS index_granularity = 8192"""


def build_create_distributed_table_sql(
    database: str,
    distributed_table: str,
    local_table: str,
    cluster: str,
    sharding_key: str = "",
) -> str:
    database = quote_identifier(database)
    distributed_table = quote_identifier(distributed_table)
    local_table = quote_identifier(local_table)
    cluster = quote_identifier(cluster)
    sharding_key = sharding_key.strip()
    if not sharding_key:
        raise ValueError("Distributed sharding key is required")
    sharding_key_sql = f"sipHash64({quote_column_identifier(sharding_key)})"
    return f"""CREATE TABLE {database}.{distributed_table}
ON CLUSTER {cluster}
AS {database}.{local_table}
ENGINE = Distributed(
    '{cluster}',
    '{database}',
    '{local_table}',
    {sharding_key_sql}
)"""


def create_tables(
    client: ClickHouseClient,
    config: ClickHouseConfig,
    schema: CsvSchema,
    distributed_table: str,
    order_by: str,
    partition_by: str | None = None,
    sharding_key: str = "",
    log_callback: Callable[[str], None] | None = None,
    verify_attempts: int = 30,
    verify_interval_seconds: float = 1.0,
) -> TableNames:
    names = build_table_names(distributed_table)
    existing = _existing_target_tables(client, config, names.distributed, names.local)
    expected = {names.distributed, names.local}
    if existing == expected:
        found = ", ".join(sorted(existing))
        raise ExistingTableError(f"Target table already exists: {found}")
    if existing:
        if log_callback:
            log_callback("Partial target table state found. Dropping target tables before retry.")
        drop_target_tables(client, config, names.distributed, names.local, log_callback)

    created = False
    try:
        if log_callback:
            log_callback(f"Creating local table {config.database}.{names.local}")
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
        created = True
        _verify_table_exists_on_cluster(
            client,
            config,
            names.local,
            attempts=verify_attempts,
            interval_seconds=verify_interval_seconds,
        )

        if log_callback:
            log_callback(f"Creating distributed table {config.database}.{names.distributed}")
        client.query(
            build_create_distributed_table_sql(
                database=config.database,
                distributed_table=names.distributed,
                local_table=names.local,
                cluster=config.cluster,
                sharding_key=sharding_key,
            )
        )
        _verify_table_exists_on_cluster(
            client,
            config,
            names.distributed,
            attempts=verify_attempts,
            interval_seconds=verify_interval_seconds,
        )
    except Exception:
        if created:
            drop_target_tables(client, config, names.distributed, names.local, log_callback)
        raise
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


def raw_insert_batch(
    client: ClickHouseClient,
    database: str,
    table: str,
    column_names: list[str],
    payload: bytes,
) -> dict[str, str]:
    """Отправляет блок и возвращает разобранный заголовок ``X-ClickHouse-Summary``.

    ⚠️ Пустого dict здесь не бывает, и проверять на него бессмысленно: драйвер
    безусловно дописывает в сводку ключ ``query_id``
    (``httpclient.py:437-445``) даже когда самого заголовка в ответе не было.
    Поэтому «прокси срезал заголовок» определяется отсутствием ``elapsed_ns``,
    а не пустотой словаря — см. :func:`summary_elapsed_ns`.

    ⚠️ ``elapsed_ns`` в этом заголовке покрывает только работу инициатора.
    ``settings`` здесь не передаются, поэтому ``distributed_foreground_insert``
    остаётся серверным дефолтом 0, и ответ приходит как только записан
    spool-файл: пересылка по шардам, репликация и мержи в него не входят.
    """
    result = client.raw_insert(
        table=f"{quote_identifier(database)}.{quote_identifier(table)}",
        column_names=column_names,
        insert_block=payload,
        fmt="JSONEachRow",
    )
    return getattr(result, "summary", None) or {}


def summary_elapsed_ns(summary: dict[str, str]) -> int | None:
    """Серверное время из ``X-ClickHouse-Summary`` в наносекундах.

    ``None`` означает «сервер этого не сообщил»: заголовок срезан прокси либо в
    нём нет разбираемого ``elapsed_ns``. Ноль означал бы «сервер не потратил
    времени» — это разные вещи, и на их различении стоит весь вывод «канал
    против сервера», ради которого делается фаза 0.
    """
    raw_value = summary.get("elapsed_ns")
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


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
    if config.client_cert:
        expires_at = _certificate_expires_at(config.client_cert)
        if expires_at <= datetime.now(timezone.utc):
            raise CertificateError(
                "ClickHouse client certificate is expired: "
                f"{config.client_cert} expired at {expires_at.isoformat()}. "
                f"Current client_key={config.client_key}. "
                "Update the cert/key paths in the UI or renew the client certificate."
            )


def _certificate_expires_at(cert_path: str) -> datetime:
    try:
        decoded = ssl._ssl._test_decode_cert(str(Path(cert_path)))  # noqa: SLF001
        not_after = decoded["notAfter"]
        expires_at = ssl.cert_time_to_seconds(not_after)
    except Exception as exc:
        raise CertificateError(
            f"Cannot read ClickHouse client certificate expiration from {cert_path}: {exc}"
        ) from exc
    return datetime.fromtimestamp(expires_at, tz=timezone.utc)


def _format_connection_error(config: ClickHouseConfig, exc: Exception) -> str:
    message = str(exc)
    normalized = message.lower()
    if "certificate_expired" not in normalized and "certificate expired" not in normalized:
        return message
    return (
        f"{message}. The client certificate may be expired. "
        f"Current client_cert={config.client_cert}, client_key={config.client_key}. "
        "Update these paths in the UI or replace the certificate files."
    )
