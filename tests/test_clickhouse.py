from datetime import datetime, timedelta, timezone
import sys
from types import SimpleNamespace

import pytest

import csv_click.clickhouse as clickhouse
from csv_click.clickhouse import (
    ClickHouseConfig,
    ExistingTableError,
    _format_connection_error,
    _validate_certificate_files,
    build_create_distributed_table_sql,
    build_create_local_table_sql,
    build_table_names,
    ensure_tables_do_not_exist,
    get_client,
    quote_identifier,
    raw_insert_batch,
)
from csv_click.errors import CertificateError
from csv_click.schema import CsvColumn, CsvSchema


class FakeClient:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return type("Result", (), {"result_rows": self.rows})()

    def raw_insert(self, **kwargs):
        self.raw_insert_kwargs = kwargs


def sample_schema() -> CsvSchema:
    return CsvSchema(
        columns=[
            CsvColumn(
                column_name="id",
                source_name="id",
                inferred_type="UInt64",
                final_type="UInt64",
                nullable=False,
                sample_values=["1"],
                notes="",
            ),
            CsvColumn(
                column_name="name",
                source_name="name",
                inferred_type="String",
                final_type="String",
                nullable=False,
                sample_values=["Alice"],
                notes="",
            ),
        ]
    )


def test_build_table_names_uses_local_suffix() -> None:
    names = build_table_names("orders")

    assert names.distributed == "orders"
    assert names.local == "orders_local"


def test_build_local_ddl_contains_cluster_replicated_engine_order_and_partition() -> None:
    sql = build_create_local_table_sql(
        database="sandbox",
        table="orders_local",
        cluster="clickhouse",
        schema=sample_schema(),
        order_by="id",
        partition_by="toYYYYMM(id)",
    )

    assert "CREATE TABLE sandbox.orders_local\nON CLUSTER clickhouse" in sql
    assert "ReplicatedMergeTree" in sql
    assert "'/clickhouse/tables/{shard}-{uuid}/orders_local'" in sql
    assert "PARTITION BY toYYYYMM(id)" in sql
    assert "ORDER BY id" in sql
    assert "SETTINGS index_granularity = 8192" in sql
    assert not sql.rstrip().endswith(";")


def test_build_distributed_ddl_uses_local_table() -> None:
    sql = build_create_distributed_table_sql(
        database="sandbox",
        distributed_table="orders",
        local_table="orders_local",
        cluster="clickhouse",
        sharding_key="sipHash64(ID)",
    )

    assert "CREATE TABLE sandbox.orders\nON CLUSTER clickhouse" in sql
    assert "AS sandbox.orders_local" in sql
    assert """ENGINE = Distributed(
    'clickhouse',
    'sandbox',
    'orders_local',
    sipHash64(ID)
)""" in sql
    assert not sql.rstrip().endswith(";")


def test_build_distributed_ddl_requires_sharding_key() -> None:
    try:
        build_create_distributed_table_sql(
            database="sandbox",
            distributed_table="orders",
            local_table="orders_local",
            cluster="clickhouse",
            sharding_key="",
        )
    except ValueError as exc:
        assert "sharding key" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_existing_table_check_blocks_create_load() -> None:
    client = FakeClient([("host1", "sandbox", "orders")])
    config = ClickHouseConfig(database="sandbox", cluster="clickhouse")

    try:
        ensure_tables_do_not_exist(client, config, "orders", "orders_local")
    except ExistingTableError as exc:
        assert "orders" in str(exc)
    else:
        raise AssertionError("Expected ExistingTableError")


def test_quote_identifier_rejects_unsafe_table_names() -> None:
    assert quote_identifier("orders_local") == "orders_local"

    try:
        quote_identifier("orders; DROP TABLE x")
    except ValueError as exc:
        assert "Unsafe" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_validate_certificate_files_reports_missing_certificates(tmp_path) -> None:
    missing_cert = tmp_path / "missing.crt"
    missing_key = tmp_path / "missing.key"
    config = ClickHouseConfig(client_cert=str(missing_cert), client_key=str(missing_key))

    try:
        _validate_certificate_files(config)
    except CertificateError as exc:
        assert str(missing_cert) in str(exc)
        assert str(missing_key) in str(exc)
    else:
        raise AssertionError("Expected CertificateError")


def test_validate_certificate_files_reports_expired_client_certificate(tmp_path, monkeypatch) -> None:
    cert = tmp_path / "client.crt"
    key = tmp_path / "client.key"
    cert.write_text("expired cert", encoding="utf-8")
    key.write_text("client key", encoding="utf-8")
    expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    monkeypatch.setattr(clickhouse, "_certificate_expires_at", lambda path: expires_at)
    config = ClickHouseConfig(client_cert=str(cert), client_key=str(key))

    with pytest.raises(CertificateError) as exc_info:
        _validate_certificate_files(config)

    message = str(exc_info.value)
    assert "client certificate is expired" in message
    assert str(cert) in message
    assert str(key) in message


def test_get_client_does_not_pass_client_cert_when_connection_is_insecure(monkeypatch) -> None:
    captured_kwargs = {}

    def fake_get_client(**kwargs):
        captured_kwargs.update(kwargs)
        return object()

    fake_module = SimpleNamespace(get_client=fake_get_client)
    monkeypatch.setitem(sys.modules, "clickhouse_connect", fake_module)

    get_client(
        ClickHouseConfig(
            secure=False,
            client_cert="/tmp/expired.crt",
            client_key="/tmp/expired.key",
        )
    )

    assert "client_cert" not in captured_kwargs
    assert "client_cert_key" not in captured_kwargs


def test_format_connection_error_explains_expired_client_certificate() -> None:
    config = ClickHouseConfig(client_cert="/tmp/current.crt", client_key="/tmp/current.key")
    message = _format_connection_error(
        config,
        Exception("[SSL: SSLV3_ALERT_CERTIFICATE_EXPIRED] sslv3 alert certificate expired"),
    )

    assert "client certificate may be expired" in message
    assert "/tmp/current.crt" in message
    assert "/tmp/current.key" in message


def test_raw_insert_batch_uses_json_each_row() -> None:
    client = FakeClient([])

    raw_insert_batch(
        client=client,
        database="sandbox",
        table="target_table",
        column_names=["ID"],
        payload=b'{"ID":1}',
    )

    assert client.raw_insert_kwargs == {
        "table": "sandbox.target_table",
        "column_names": ["ID"],
        "insert_block": b'{"ID":1}',
        "fmt": "JSONEachRow",
    }
