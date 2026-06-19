from csv_click.clickhouse import (
    ClickHouseConfig,
    ExistingTableError,
    _validate_certificate_files,
    build_create_distributed_table_sql,
    build_create_local_table_sql,
    build_table_names,
    ensure_tables_do_not_exist,
    quote_identifier,
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

    assert "CREATE TABLE sandbox.orders_local ON CLUSTER 'clickhouse'" in sql
    assert "ReplicatedMergeTree" in sql
    assert "PARTITION BY toYYYYMM(id)" in sql
    assert "ORDER BY id" in sql


def test_build_distributed_ddl_uses_local_table() -> None:
    sql = build_create_distributed_table_sql(
        database="sandbox",
        distributed_table="orders",
        local_table="orders_local",
        cluster="clickhouse",
    )

    assert "CREATE TABLE sandbox.orders ON CLUSTER 'clickhouse'" in sql
    assert "AS sandbox.orders_local" in sql
    assert "Distributed('clickhouse', 'sandbox', 'orders_local', rand())" in sql


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
