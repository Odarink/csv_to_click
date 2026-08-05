"""Загрузка CSV в фоновом потоке, свободная от Streamlit.

Раньше продюсер жил на потоке скрипта Streamlit внутри ``_create_and_load``,
и любой ``st.*``-вызов мог бросить ``RerunException``: случайный клик убивал
часовую заливку, а отменить её было нельзя вовсе. Здесь то же тело загрузки
исполняется в обычном потоке; интерфейс только читает состояние задачи и
взводит отмену.

В этом модуле НЕТ обращений к Streamlit, и это держит тест: ``st.*`` из
чужого потока молча не работает (missing ScriptRunContext), а исключения
Streamlit не должны уметь дотянуться до загрузки.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from csv_click.clickhouse import (
    ClickHouseConfig,
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
    CsvLoadCancelled,
    CsvSchemaError,
    ExistingTableError,
    TableCleanupError,
)
from csv_click.load_stats import (
    BlockProgress,
    DriverRetryCounter,
    LoadStats,
    RunConfig,
    arrow_pool_high_water_bytes,
    describe_connection_path,
    format_load_stats_lines,
    write_run_record,
)
from csv_click.pandas_loader import (
    ReadOptions,
    SchemaMapping,
    load_csv_via_raw_insert,
    validate_csv_sample_with_pandas_chunks,
    validate_csv_with_pandas_chunks,
)
from csv_click.schema import CsvSchema


LARGE_CSV_PRECHECK_THRESHOLD_BYTES = 50 * 1024 * 1024
SAMPLE_PRECHECK_ROWS = 200_000
INSERT_PAYLOAD_SAFETY_RATIO = 0.9

#: Судьба таблиц в записи о прогоне. После ``514466c`` сбой оставляет либо
#: удалённые, либо оставленные с данными таблицы, и оператор из присланного
#: файла обязан их различать.
TABLES_NOT_CREATED = "not_created"
TABLES_CREATED = "created"
TABLES_KEPT_WITH_DATA = "kept_with_data"
TABLES_DROPPED_AS_EMPTY = "dropped_as_empty"
TABLES_CLEANUP_FAILED = "cleanup_failed"


def _effective_insert_payload_bytes(max_insert_payload_mb: int) -> int:
    configured_bytes = max_insert_payload_mb * 1024 * 1024
    return max(1, int(configured_bytes * INSERT_PAYLOAD_SAFETY_RATIO))


def _format_load_error(exc: Exception) -> str:
    message = str(exc)
    normalized = message.lower()
    if "unknown_table" in normalized or "does not exist" in normalized:
        return (
            "ClickHouse load failed during load step because the target table is not visible "
            f"after DDL creation: {message}"
        )
    return f"Unexpected load error: {message}"


def _handle_tables_after_stopped_load(
    client,
    config: ClickHouseConfig,
    distributed_table: str,
    log_callback,
    stats: LoadStats,
    reason: str = "failed",
) -> str:
    """Убрать за остановленной загрузкой - но только то, что ещё пусто.

    Чистка задумана как откат создания таблиц, и она безопасна ровно до первой
    вставки. Дальше это уничтожение работы: один транзиентный 5xx на 900-м
    блоке из 1000 стирал всё залитое, и повторять пришлось бы с нуля. Отмена
    оператором идёт той же логикой: передумал - не значит согласился потерять
    уже залитые блоки.

    Решение принимается по ПОДТВЕРЖДЁННЫМ блокам, и только по ним.
    `blocks_unconfirmed` для этого не годится: туда попадают и отменённые при
    гашении блоки, которые не отправлялись вовсе (`cancel_pending`), и упавшие
    до отправки - на создании клиента воркера или на сжатии. Обрыв связи сразу
    после DDL давал на параллельном пути `blocks_unconfirmed=N` при пустых
    таблицах, и пара оставалась навсегда, хотя раньше откатывалась сама.
    Отличить «сервер отверг» от «ответ не дошёл» тоже нельзя: драйвер бросает
    `OperationalError` (подкласс `DatabaseError`) и на то, и на другое.

    Цена решения названа честно: если единственный блок всё-таки долетел, он
    уйдёт вместе с таблицей. Это блок из начала файла, и перезалив с нуля
    дешевле, чем каждый раз дропать руками пустую пару после отказа кодека.

    Возвращает судьбу таблиц для записи о прогоне: оператор из присланного
    файла обязан различать «оставлены с данными» и «удалены как пустые».
    """
    if stats.blocks:
        table_names = build_table_names(distributed_table)
        what_is_inside = (
            f"{stats.rows} rows in {stats.blocks} confirmed block(s) are already there"
        )
        if stats.blocks_unconfirmed:
            what_is_inside += (
                f", and {stats.blocks_unconfirmed} more block(s) were never confirmed, "
                "so the real count can be higher"
            )
        log_callback(
            f"Load {reason} and the target tables are KEPT: {what_is_inside}. "
            f"Check {config.database}.{table_names.distributed} and drop both it and "
            f"{config.database}.{table_names.local} yourself before reloading - the "
            "next run refuses a name that already exists."
        )
        return TABLES_KEPT_WITH_DATA
    if stats.blocks_unconfirmed:
        log_callback(
            f"{stats.blocks_unconfirmed} block(s) never came back confirmed and no block "
            "did, so the tables are treated as empty and dropped. If one of them did "
            "land after all, it goes with the table - reload from the start."
        )
    try:
        _cleanup_after_stopped_load(client, config, distributed_table, log_callback, reason)
    except Exception as cleanup_exc:
        # Удаление не удалось: таблицы, скорее всего, остались. Прятать это
        # нельзя - следующая загрузка откажется от занятого имени.
        log_callback(f"Cleanup error: {cleanup_exc}")
        return TABLES_CLEANUP_FAILED
    return TABLES_DROPPED_AS_EMPTY


def _cleanup_after_stopped_load(
    client,
    config: ClickHouseConfig,
    distributed_table: str,
    log_callback,
    reason: str,
) -> None:
    table_names = build_table_names(distributed_table)
    log_callback(f"Load {reason} after table creation. Dropping target tables.")
    drop_target_tables(
        client=client,
        config=config,
        distributed_table=table_names.distributed,
        local_table=table_names.local,
        log_callback=log_callback,
    )


class LoadJob:
    """Одна загрузка: поток, отмена, лог, счётчики, исход и запись о прогоне.

    ``Thread`` и ``Event`` держатся композицией, а не наследованием:
    ``Thread._stop`` - существующий метод, его зовут ``join()`` и
    ``is_alive()``, и одноимённое поле в наследнике роняет их уже после
    полезной работы.

    Счётчики (``stats``) пишет один поток - поток задачи; интерфейс их только
    читает. Целые числа атомарны под GIL, рассинхрон на долю секунды виден
    лишь как чуть отстающая цифра прогресса.
    """

    def __init__(
        self,
        *,
        config: ClickHouseConfig,
        csv_path: str,
        read_options: ReadOptions,
        schema: CsvSchema,
        mappings: list[SchemaMapping],
        distributed_table: str,
        order_by: str,
        partition_by: str | None,
        sharding_key: str,
        max_insert_payload_mb: int,
        load_workers: int,
        insert_compression: str,
        strict_preflight: bool,
        schema_inference_mode: str,
        encoding_warning: str | None = None,
    ) -> None:
        self.config = config
        self.csv_path = str(csv_path)
        self.read_options = read_options
        self.schema = schema
        self.mappings = list(mappings)
        self.distributed_table = distributed_table
        self.table_names = build_table_names(distributed_table)
        self.order_by = order_by
        self.partition_by = partition_by
        self.sharding_key = sharding_key
        self.max_insert_payload_mb = int(max_insert_payload_mb)
        self.max_insert_payload_bytes = _effective_insert_payload_bytes(self.max_insert_payload_mb)
        self.load_workers = int(load_workers)
        self.insert_compression = insert_compression
        self.strict_preflight = strict_preflight
        self.encoding_warning = encoding_warning
        # Собирается в конструкторе, на потоке скрипта: запись о прогоне
        # обязана уцелеть при любом раннем падении внутри run().
        self.run_config = RunConfig(
            batch_size=read_options.batch_size,
            max_insert_payload_mb=self.max_insert_payload_mb,
            effective_insert_payload_bytes=self.max_insert_payload_bytes,
            load_workers=self.load_workers,
            insert_compression=insert_compression,
            strict_preflight=strict_preflight,
            schema_inference_mode=schema_inference_mode,
            separator=read_options.separator,
            encoding=read_options.encoding,
            database=config.database,
            table=distributed_table,
            cluster=config.cluster,
            order_by=order_by,
            partition_by=partition_by,
            sharding_key=sharding_key,
        )

        self.stats = LoadStats()
        self.phase = "Waiting to start..."
        self.outcome: str | None = None
        self.error_message: str | None = None
        self.tables_fate: str = TABLES_NOT_CREATED
        self.record_path: Path | None = None
        self._log: list[str] = []
        self._log_lock = threading.Lock()
        self._cancel = threading.Event()
        self._finished = threading.Event()
        self._started = False
        self._thread: threading.Thread | None = None

    # --- состояние для интерфейса -------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self._started and not self._finished.is_set()

    @property
    def is_finished(self) -> bool:
        return self._finished.is_set()

    @property
    def cancel_requested(self) -> bool:
        return self._cancel.is_set()

    def log_lines(self) -> list[str]:
        with self._log_lock:
            return list(self._log)

    # --- управление ----------------------------------------------------------------

    def request_cancel(self) -> None:
        """Взводит отмену. Не мгновенная: блоки в полёте доедут и будут
        засчитаны, во время connect/DDL отмена сработает на границе фаз."""
        if not self._cancel.is_set():
            self._cancel.set()
            self.log(
                "Cancel requested: blocks already in flight will finish and be counted, "
                "nothing new goes out."
            )

    def start(self) -> None:
        """Запускает задачу в daemon-потоке.

        Daemon намеренно: закрытие процесса Streamlit убивает загрузку ровно
        так же, как раньше её убивал уход потока скрипта. Иначе Ctrl+C ждал бы
        часовую заливку.
        """
        # `_started` ставится до старта потока: между `start_load_job` двух
        # сессий задача не должна быть видна «не живой».
        self._started = True
        thread = threading.Thread(target=self.run, name="csv-click-load", daemon=True)
        self._thread = thread
        thread.start()

    def wait(self, timeout: float | None = None) -> bool:
        """Дожидается конца задачи; нужен тестам и оболочке, не интерфейсу."""
        return self._finished.wait(timeout)

    def log(self, message: str) -> None:
        line = f"{time.strftime('%H:%M:%S')} {message}"
        with self._log_lock:
            self._log.append(line)

    # --- сама работа ----------------------------------------------------------------

    def _raise_if_cancelled(self) -> None:
        if self._cancel.is_set():
            raise CsvLoadCancelled("The load was cancelled by the operator")

    def _on_block(self, block: BlockProgress) -> None:
        payload_mb = block.wire_bytes / 1024 / 1024
        self.log(
            f"Loaded chunk {block.chunk_number}, block {block.block_number}: "
            f"{block.block_rows} rows, {payload_mb:.2f} MB, total {block.rows_total}."
        )

    def run(self) -> None:
        """Тело загрузки; исполняется в потоке задачи, тесты зовут синхронно.

        Порядок фаз, тексты сообщений и ветки исключений сохранены от
        ``_create_and_load``: preflight → connect → DDL → загрузка → судьба
        таблиц → запись прогона.
        """
        self._started = True
        start = time.time()
        stats = self.stats
        # Отметка пула PyArrow монотонна и живёт весь процесс, поэтому снимаем
        # её и до, и после: этой загрузке принадлежит только прирост.
        stats.arrow_bytes_at_start = arrow_pool_high_water_bytes()
        client = None
        tables_created = False
        # Ставится сразу после возврата загрузчика: дальше уже нельзя ни
        # объявлять провал загрузки, ни удалять таблицы.
        load_completed = False
        outcome = "failed"
        error_message: str | None = None
        fate = TABLES_NOT_CREATED
        try:
            if self.encoding_warning:
                self.log(self.encoding_warning)
            stats.src_bytes = Path(self.csv_path).stat().st_size
            configured_insert_payload_bytes = self.max_insert_payload_mb * 1024 * 1024
            configured_insert_payload_mb = configured_insert_payload_bytes / 1024 / 1024
            effective_insert_payload_mb = self.max_insert_payload_bytes / 1024 / 1024
            self.log(
                "Load settings: batch size "
                f"{self.read_options.batch_size}, load workers {self.load_workers}, "
                f"configured max insert payload {configured_insert_payload_mb:.2f} MB, "
                f"effective insert payload {effective_insert_payload_mb:.2f} MB."
            )
            if self.max_insert_payload_bytes < configured_insert_payload_bytes:
                self.log(
                    "Effective insert payload limit is lower than the configured UI value "
                    "to stay below ClickHouse HTTP/proxy read limits."
                )
            self._raise_if_cancelled()
            preflight_started = time.perf_counter()
            if self.strict_preflight:
                if stats.src_bytes > LARGE_CSV_PRECHECK_THRESHOLD_BYTES:
                    sample_rows = max(SAMPLE_PRECHECK_ROWS, self.read_options.batch_size)
                    self.log(
                        "File is larger than 50 MB; using sample validation for the first "
                        f"{sample_rows} rows instead of full strict validation."
                    )
                    self.phase = "Validating first CSV rows against selected types..."
                    validated_rows = validate_csv_sample_with_pandas_chunks(
                        self.csv_path,
                        self.read_options,
                        self.mappings,
                        max_insert_payload_bytes=self.max_insert_payload_bytes,
                        sample_rows=sample_rows,
                    )
                    self.log(f"Sample validation finished: first {validated_rows} rows only.")
                else:
                    self.log("Validating CSV chunks against selected types.")
                    self.phase = "Validating CSV chunks against selected types..."
                    total_rows = validate_csv_with_pandas_chunks(
                        self.csv_path,
                        self.read_options,
                        self.mappings,
                        max_insert_payload_bytes=self.max_insert_payload_bytes,
                    )
                    self.log(f"Strict validation finished: {total_rows} rows.")
            stats.preflight_s = time.perf_counter() - preflight_started

            self._raise_if_cancelled()
            self.log("Connecting to ClickHouse.")
            self.phase = "Connecting to ClickHouse..."
            connect_started = time.perf_counter()
            client = get_client(self.config)
            test_connection(client)
            stats.connect_s = time.perf_counter() - connect_started
            # Соединение сейчас простаивает в пуле — единственный момент, когда
            # из него можно достать адреса.
            stats.connection_path = describe_connection_path(client)
            self.log("ClickHouse connection OK.")

            self._raise_if_cancelled()
            self.log("Checking existing tables and creating DDL.")
            self.phase = "Checking existing tables and creating DDL..."
            ddl_started = time.perf_counter()
            create_tables(
                client=client,
                config=self.config,
                schema=self.schema,
                distributed_table=self.distributed_table,
                order_by=self.order_by,
                partition_by=self.partition_by,
                sharding_key=self.sharding_key,
                log_callback=self.log,
            )
            stats.ddl_s = time.perf_counter() - ddl_started
            tables_created = True
            fate = TABLES_CREATED
            self.log("Target tables are created and visible on cluster.")

            self._raise_if_cancelled()
            self.log("Loading CSV chunks through JSONEachRow.")
            self.phase = "Loading CSV chunks through JSONEachRow..."

            driver_retries = DriverRetryCounter()
            insert_started = time.perf_counter()
            try:
                with driver_retries:
                    load_csv_via_raw_insert(
                        client=client,
                        csv_path=self.csv_path,
                        read_options=self.read_options,
                        database=self.config.database,
                        table=self.distributed_table,
                        mappings=self.mappings,
                        max_insert_payload_bytes=self.max_insert_payload_bytes,
                        worker_count=self.load_workers,
                        client_factory=lambda: get_client(self.config),
                        progress_callback=self._on_block,
                        compression=self.insert_compression,
                        stats=stats,
                        cancel_callback=self._cancel.is_set,
                    )
            finally:
                # insert_wall_s замеряется строго вокруг загрузки: preflight,
                # connect и DDL в него не входят, иначе server % считался бы от
                # чужого времени.
                stats.insert_wall_s = time.perf_counter() - insert_started
                stats.driver_retries = driver_retries.count

            # Данные в ClickHouse. Дальше идёт только отчёт: его сбой не имеет
            # права ни объявить провал загрузки, ни удалить таблицы.
            load_completed = True
            outcome = "ok"
            elapsed = time.time() - start
            self.phase = "Finished"
            self.log(f"Load finished: {stats.rows} rows in {elapsed:.2f} sec.")
            for line in format_load_stats_lines(stats):
                self.log(line)
        except CsvLoadCancelled as exc:
            outcome = "cancelled"
            if tables_created and client is not None:
                fate = _handle_tables_after_stopped_load(
                    client,
                    self.config,
                    self.distributed_table,
                    self.log,
                    stats,
                    reason="cancelled",
                )
            error_message = str(exc)
            self.log(error_message)
        except CertificateError as exc:
            error_message = f"Certificate error: {exc}"
            self.log(error_message)
        except ExistingTableError as exc:
            error_message = f"Existing table error: {exc}"
            self.log(error_message)
        except TableCleanupError as exc:
            # Откат внутри create_tables сам упал: локальная таблица, скорее
            # всего, осталась на кластере, и not_created здесь было бы враньём.
            fate = TABLES_CLEANUP_FAILED
            error_message = str(exc)
            self.log(error_message)
        except (CsvSchemaError, ClickHouseConnectionError, CsvClickError) as exc:
            # Не при `load_completed`: строки уже в таблицах, и сбой отчёта не
            # повод их уничтожать.
            if tables_created and client is not None and not load_completed:
                fate = _handle_tables_after_stopped_load(
                    client, self.config, self.distributed_table, self.log, stats
                )
            error_message = str(exc)
            self.log(error_message)
        except Exception as exc:
            if tables_created and client is not None and not load_completed:
                fate = _handle_tables_after_stopped_load(
                    client, self.config, self.distributed_table, self.log, stats
                )
            if load_completed:
                # Загрузка прошла, сломался отчёт. Прогонять текст через
                # `_format_load_error` нельзя: он объясняет сбой ЗАГРУЗКИ и,
                # например, обычную ошибку сокета выдаёт за невидимую таблицу.
                error_message = f"The load finished, but reporting it failed: {exc}"
            else:
                error_message = _format_load_error(exc)
            self.log(error_message)
        finally:
            stats.total_s = time.time() - start
            stats.arrow_bytes = arrow_pool_high_water_bytes()
            if outcome == "failed" and error_message is None:
                # Страховка от неожиданного BaseException. Штатно недостижима:
                # в теле задачи нет st.*, бросать RerunException некому.
                outcome = "interrupted"
                error_message = "the load thread was interrupted before the load finished"
            try:
                try:
                    record_path = write_run_record(
                        config=self.run_config,
                        stats=stats,
                        csv_path=Path(self.csv_path),
                        outcome=outcome,
                        error=error_message,
                        timestamp=datetime.now(timezone.utc),
                        tables={
                            "distributed": self.table_names.distributed,
                            "local": self.table_names.local,
                            "fate": fate,
                        },
                    )
                    self.record_path = record_path
                    self.log(f"Run record saved to {record_path}")
                except OSError as write_exc:
                    self.log(f"Could not save the run record: {write_exc}")
            finally:
                # Исход и отметка завершения выставляются, ЧТО БЫ НИ случилось
                # с записью: не-OSError отсюда летит дальше громко, но задача
                # не имеет права остаться «вечно живой» — иначе реестр не
                # примет ни одной загрузки до перезапуска процесса. Исход
                # публикуется ДО отметки: кто увидел `is_finished`, обязан
                # увидеть и заполненный итог.
                self.outcome = outcome
                self.error_message = error_message
                self.tables_fate = fate
                self._finished.set()


# --- реестр: одна активная задача на процесс -------------------------------------

_registry_lock = threading.Lock()
_current_job: LoadJob | None = None


def start_load_job(job: LoadJob) -> bool:
    """Регистрирует задачу и запускает её поток; ``False`` — живая уже есть.

    Одна задача на процесс: двойной клик по кнопке загрузки не имеет права
    запустить вторую заливку в ту же таблицу. Реестр живёт здесь, а не в
    ``app.py``: Streamlit пере-исполняет сам скрипт на каждом прогоне, и его
    глобалы не переживают rerun; импортированный модуль — переживает.
    Побочный выигрыш: после F5 новая сессия видит ту же задачу.

    Старт под локом намеренно: между записью в реестр и стартом потока задача
    не должна быть видна «не живой», иначе вторая сессия успела бы её заменить.
    """
    global _current_job
    with _registry_lock:
        if _current_job is not None and _current_job.is_running:
            return False
        previous = _current_job
        _current_job = job
        try:
            job.start()
        except BaseException:
            # thread.start() упал (нехватка ресурсов ОС): без отката в реестре
            # осталась бы фантомная «вечно живая» задача, и процесс не принял
            # бы больше ни одной загрузки. Ошибка летит дальше громко.
            _current_job = previous
            raise
    return True


def current_load_job() -> LoadJob | None:
    """Задача процесса — живая или последняя завершённая, для итога на экране."""
    with _registry_lock:
        return _current_job


def reset_load_job_registry() -> None:
    """Забыть задачу; нужен тестам. Живой поток не трогается — он daemon."""
    global _current_job
    with _registry_lock:
        _current_job = None
