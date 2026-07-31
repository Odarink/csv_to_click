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
    create_tables,
    ensure_tables_do_not_exist,
    get_client,
    quote_identifier,
    raw_insert_batch,
    summary_elapsed_ns,
)
from csv_click.errors import CertificateError, ClickHouseConnectionError
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
        return SimpleNamespace(summary={"written_rows": "1", "elapsed_ns": "1234567"})


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
    assert "ORDER BY `id`" in sql
    assert "SETTINGS index_granularity = 8192" in sql
    assert not sql.rstrip().endswith(";")


def test_build_local_ddl_backticks_a_cyrillic_column_name() -> None:
    """Кириллическое имя колонки уезжает в DDL как есть, в бэктиках."""
    schema = CsvSchema(
        columns=[
            CsvColumn(
                column_name="инн",
                source_name="ИНН",
                inferred_type="UInt64",
                final_type="UInt64",
                nullable=False,
                sample_values=["10000"],
                notes="",
            )
        ]
    )

    sql = build_create_local_table_sql(
        database="sandbox",
        table="sellers_local",
        cluster="clickhouse",
        schema=schema,
        order_by="инн",
    )

    assert "`инн` UInt64" in sql
    assert "ORDER BY `инн`" in sql


def test_build_local_ddl_rejects_a_backtick_in_a_column_name() -> None:
    """Имя из редактора типов нормализацию не проходит: бэктик закрыл бы кавычку.

    `mappings_to_schema` проверяет целевое имя только на непустоту, поэтому
    оператор мог дописать в DDL произвольный текст через `x`` DEFAULT 1, ``y`.
    """
    schema = CsvSchema(
        columns=[
            CsvColumn(
                column_name="x` DEFAULT 1, `y",
                source_name="x",
                inferred_type="String",
                final_type="String",
                nullable=False,
                sample_values=[],
                notes="",
            )
        ]
    )

    with pytest.raises(ValueError, match="Unsafe ClickHouse column identifier"):
        build_create_local_table_sql(
            database="sandbox",
            table="orders_local",
            cluster="clickhouse",
            schema=schema,
            order_by="x",
        )


def test_build_distributed_ddl_uses_local_table() -> None:
    sql = build_create_distributed_table_sql(
        database="sandbox",
        distributed_table="orders",
        local_table="orders_local",
        cluster="clickhouse",
        sharding_key="ID",
    )

    assert "CREATE TABLE sandbox.orders\nON CLUSTER clickhouse" in sql
    assert "AS sandbox.orders_local" in sql
    assert """ENGINE = Distributed(
    'clickhouse',
    'sandbox',
    'orders_local',
    sipHash64(`ID`)
)""" in sql
    assert not sql.rstrip().endswith(";")


def test_build_local_ddl_rejects_multi_statement_partition() -> None:
    with pytest.raises(ValueError, match="PARTITION BY"):
        build_create_local_table_sql(
            database="sandbox",
            table="orders_local",
            cluster="clickhouse",
            schema=sample_schema(),
            order_by="id",
            partition_by="toYYYYMM(id); DROP TABLE x",
        )


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


class PartialDDLClient:
    def __init__(self) -> None:
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        if "name IN ('orders', 'orders_local')" in sql:
            dropped = any("DROP TABLE IF EXISTS" in query for query in self.queries)
            rows = [] if dropped else [("host1", "sandbox", "orders")]
            return type("Result", (), {"result_rows": rows})()
        if "name = 'orders_local'" in sql:
            return type("Result", (), {"result_rows": [("host1", "sandbox", "orders_local")]})()
        if "name = 'orders'" in sql:
            return type("Result", (), {"result_rows": [("host1", "sandbox", "orders")]})()
        return type("Result", (), {"result_rows": []})()


def test_create_tables_drops_partial_state_and_verifies_local_before_distributed() -> None:
    client = PartialDDLClient()

    create_tables(
        client=client,
        config=ClickHouseConfig(database="sandbox", cluster="clickhouse"),
        schema=sample_schema(),
        distributed_table="orders",
        order_by="id",
        sharding_key="id",
    )

    queries = client.queries
    assert any("DROP TABLE IF EXISTS sandbox.orders\nON CLUSTER clickhouse" in query for query in queries)
    assert any("DROP TABLE IF EXISTS sandbox.orders_local\nON CLUSTER clickhouse" in query for query in queries)
    local_create_idx = next(i for i, query in enumerate(queries) if "CREATE TABLE sandbox.orders_local" in query)
    local_verify_idx = next(i for i, query in enumerate(queries) if "name = 'orders_local'" in query)
    distributed_create_idx = next(i for i, query in enumerate(queries) if "CREATE TABLE sandbox.orders\n" in query)
    assert local_create_idx < local_verify_idx < distributed_create_idx


class MissingLocalDDLClient:
    def __init__(self) -> None:
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        return type("Result", (), {"result_rows": []})()


def test_create_tables_stops_when_local_table_is_not_visible() -> None:
    client = MissingLocalDDLClient()

    with pytest.raises(ClickHouseConnectionError, match="orders_local"):
        create_tables(
            client=client,
            config=ClickHouseConfig(database="sandbox", cluster="clickhouse"),
            schema=sample_schema(),
            distributed_table="orders",
            order_by="id",
            sharding_key="id",
            verify_attempts=1,
            verify_interval_seconds=0,
        )

    assert not any("CREATE TABLE sandbox.orders\n" in query for query in client.queries)


def test_create_tables_blocks_when_both_target_tables_exist() -> None:
    client = FakeClient(
        [
            ("host1", "sandbox", "orders"),
            ("host1", "sandbox", "orders_local"),
        ]
    )

    with pytest.raises(ExistingTableError):
        create_tables(
            client=client,
            config=ClickHouseConfig(database="sandbox", cluster="clickhouse"),
            schema=sample_schema(),
            distributed_table="orders",
            order_by="id",
            sharding_key="id",
        )

    assert not any("DROP TABLE" in query for query in client.queries)


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
        # None, а не отсутствие ключа: драйвер по этому аргументу решает, ставить
        # ли `Content-Encoding` и переносить ли INSERT в параметры URL.
        "compression": None,
    }


def test_raw_insert_batch_declares_the_codec_when_the_body_is_compressed() -> None:
    """Тело сжимает вызывающий; драйвер обязан объявить кодек заголовком.

    Без этого сервер получит сжатые байты как JSONEachRow и отвергнет их.
    """
    client = FakeClient([])

    raw_insert_batch(
        client=client,
        database="sandbox",
        table="target_table",
        column_names=["ID"],
        payload=b"\x28\xb5\x2f\xfd",
        compression="zstd",
    )

    assert client.raw_insert_kwargs["compression"] == "zstd"
    assert client.raw_insert_kwargs["insert_block"] == b"\x28\xb5\x2f\xfd"


def test_raw_insert_batch_returns_server_summary() -> None:
    client = FakeClient([])

    summary = raw_insert_batch(
        client=client,
        database="sandbox",
        table="target_table",
        column_names=["ID"],
        payload=b'{"ID":1}',
    )

    assert summary == {"written_rows": "1", "elapsed_ns": "1234567"}


def test_raw_insert_batch_returns_what_the_driver_produces_for_a_stripped_header() -> None:
    """Драйвер безусловно дописывает в сводку query_id (httpclient.py:444), то
    есть при срезанном прокси заголовке возвращается НЕ пустой словарь, а
    словарь без elapsed_ns. Тест держит именно эту форму: фейк, возвращающий
    None или {}, зеленил бы проверку, которой в бою не бывает."""

    class StrippedSummaryClient:
        def raw_insert(self, **kwargs):
            return SimpleNamespace(summary={"query_id": "01234567-89ab-cdef"})

    summary = raw_insert_batch(
        client=StrippedSummaryClient(),
        database="sandbox",
        table="target_table",
        column_names=["ID"],
        payload=b'{"ID":1}',
    )

    assert summary == {"query_id": "01234567-89ab-cdef"}
    assert summary_elapsed_ns(summary) is None


def test_summary_elapsed_ns_separates_absent_from_zero() -> None:
    assert summary_elapsed_ns({"elapsed_ns": "1234567"}) == 1234567
    assert summary_elapsed_ns({"elapsed_ns": "0"}) == 0
    # None, а не 0: «сервер не сообщил» и «сервер потратил ноль» — разные вещи,
    # и на их различении стоит весь вывод «канал против сервера».
    assert summary_elapsed_ns({}) is None
    assert summary_elapsed_ns({"query_id": "abc"}) is None
    assert summary_elapsed_ns({"elapsed_ns": ""}) is None
    assert summary_elapsed_ns({"elapsed_ns": "not-a-number"}) is None


def test_the_installed_driver_never_returns_an_empty_summary() -> None:
    """Опора для теста выше: проверяем на реальном коде драйвера, что пустой
    сводки не бывает даже когда в ответе нет ни одного нужного заголовка."""
    from clickhouse_connect.driver.httpclient import HttpClient

    assert HttpClient._summary(SimpleNamespace(headers={})) == {"query_id": ""}
