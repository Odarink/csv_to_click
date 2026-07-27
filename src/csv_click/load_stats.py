"""Счётчики и запись о прогоне загрузки.

Фаза 0 плана оптимизации: до этого приложение печатало единственное число
«Load finished: N rows in X sec», в которое попадали preflight, connect и DDL,
а серверные тайминги, уже приходящие в заголовке ``X-ClickHouse-Summary``,
выбрасывались. Без раздельных часов ни одну оптимизацию нельзя ни принять,
ни отвергнуть.
"""

from __future__ import annotations

import ctypes
import ipaddress
import json
import logging
import platform
import re
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path


DEFAULT_RUN_LOG_DIR = Path.home() / ".csv_click" / "runs"

DRIVER_LOGGER_NAME = "clickhouse_connect.driver.httpclient"
DRIVER_RETRY_MESSAGE = "Retrying remotely closed connection"

TRACKED_PACKAGES: tuple[str, ...] = (
    "pandas",
    "numpy",
    "pyarrow",
    "clickhouse-connect",
    "urllib3",
    "streamlit",
    "zstandard",
    "lz4",
)

# GetDriveTypeW, winbase.h
_DRIVE_TYPES: dict[int, str] = {
    0: "unknown",
    1: "no_root_dir",
    2: "removable",
    3: "fixed",
    4: "remote",
    5: "cdrom",
    6: "ramdisk",
}

# Признаки того, что байтов файла нет на машине (OneDrive и прочие cloud filter
# providers), winnt.h.
_FILE_ATTRIBUTE_OFFLINE = 0x0000_1000
_FILE_ATTRIBUTE_RECALL_ON_OPEN = 0x0004_0000
_FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS = 0x0040_0000
_CLOUD_PLACEHOLDER_ATTRIBUTES = (
    _FILE_ATTRIBUTE_OFFLINE
    | _FILE_ATTRIBUTE_RECALL_ON_OPEN
    | _FILE_ATTRIBUTE_RECALL_ON_DATA_ACCESS
)


@dataclass(frozen=True)
class BlockProgress:
    """Один отправленный блок JSONEachRow.

    ``raw_bytes`` — размер тела до сжатия, ``wire_bytes`` — то, что реально ушло
    в HTTP. До фазы 2 они равны; писать оба нужно с самого начала, иначе после
    появления сжатия коэффициент будет не с чем сравнить.

    ``server_time_reported`` — сообщил ли сервер своё время по этому блоку.
    Проверять на пустоту саму сводку нельзя: драйвер всегда дописывает в неё
    ``query_id`` (``httpclient.py:444``), поэтому пустой она не бывает даже
    когда прокси срезал заголовок целиком.
    """

    chunk_number: int
    block_number: int
    block_rows: int
    rows_total: int
    raw_bytes: int
    wire_bytes: int
    server_ns: int
    server_time_reported: bool


@dataclass
class LoadStats:
    """Счётчики одной загрузки.

    Мутируется по ходу работы и поэтому передаётся в загрузчик снаружи: если
    загрузка упадёт на середине, вызывающий сохранит то, что уже собрано, —
    именно эти числа нужны для диагностики падения при росте batch.
    """

    rows: int = 0
    blocks: int = 0
    blocks_without_server_time: int = 0
    src_bytes: int = 0
    raw_bytes: int = 0
    wire_bytes: int = 0
    read_s: float = 0.0
    convert_s: float = 0.0
    serialize_s: float = 0.0
    compress_s: float = 0.0
    preflight_s: float = 0.0
    connect_s: float = 0.0
    ddl_s: float = 0.0
    insert_wall_s: float = 0.0
    total_s: float = 0.0
    server_ns: int = 0
    #: Сколько продюсер простоял, ожидая свободного места в очереди вставок.
    #: Оборачивается ровно ожидание готового блока, не тело цикла: иначе сюда
    #: попал бы `progress_callback`, который ходит в Streamlit.
    producer_stall_s: float = 0.0
    #: Сумма стенных часов вокруг `raw_insert` во ВСЕХ воркерах. Вместе с
    #: `worker_count` и `insert_wall_s` отвечает на «пул был занят или ждал».
    insert_busy_s: float = 0.0
    #: Сумма ожиданий блока в очереди пула: от `submit` до начала отправки.
    insert_queue_s: float = 0.0
    driver_retries: int = 0
    #: Итератор чанков был исчерпан, то есть продюсер увидел конец CSV.
    #:
    #: Обещает РОВНО это и ничего больше. ``False`` не означает «в таблице
    #: префикс»: прерывание на последнем блоке тоже оставляет ``False``, хотя
    #: строки все — конец файла просто не был подтверждён ещё одним ``next()``.
    #: И ``True`` не означает «всё доехало»: на параллельном пути флаг ставится,
    #: когда блоки отданы воркерам, а часть их могла быть отменена.
    #:
    #: «Таблица полная» = ``source_fully_read and blocks_unconfirmed == 0`` при
    #: ``outcome == "ok"``. В остальных случаях сверяйте ``count()``.
    source_fully_read: bool = False
    #: Блоки, отданные на вставку, но не подтверждённые сервером: отменённые при
    #: гашении и упавшие. Их строк в таблице может не быть, и в ``rows`` они не
    #: попадают — без этого счётчика потеря хвоста выглядела бы как успех.
    blocks_unconfirmed: int = 0
    worker_count: int = 1
    arrow_bytes_at_start: int | None = None
    arrow_bytes: int | None = None
    connection_path: dict[str, object] | None = None

    def add_block(self, progress: BlockProgress) -> None:
        self.rows = progress.rows_total
        self.blocks += 1
        self.raw_bytes += progress.raw_bytes
        self.wire_bytes += progress.wire_bytes
        self.server_ns += progress.server_ns
        if not progress.server_time_reported:
            self.blocks_without_server_time += 1

    @property
    def server_share(self) -> float | None:
        """Доля ``insert_wall_s``, которую сервер отчитался как своё время.

        ``None``, когда считать её нельзя, и это НЕ то же самое, что ноль:

        * ``insert_wall_s`` ещё не замерен или блоков не было вовсе;
        * хотя бы один блок пришёл без ``elapsed_ns`` — прокси срезает
          ``X-ClickHouse-Summary``, и тогда ``server_ns`` занижен на неизвестную
          величину, а ноль читался бы как «сервер не тратил времени»;
        * ``worker_count > 1``. Запросы при этом идут одновременно, а
          ``server_ns`` — их СУММА, поэтому отношение к одним стенным часам
          доходит до ``worker_count`` раз и долей уже не является: при шести
          воркерах честные 50 мс на блок дают «514%». Абсолютную сумму печатать
          можно, отношение — нельзя.

        ⚠️ Даже когда доля посчитана, она измеряет только ПЕРВУЮ половину
        серверной работы. ``raw_insert_batch`` не передаёт ``settings``, поэтому
        ``distributed_foreground_insert`` остаётся серверным дефолтом 0, и HTTP
        200 приходит как только на инициаторе записан spool-файл. Пересылка по
        шардам, реплицирование и мержи в ``elapsed_ns`` не попадают вообще.
        Значит малый ``server_share`` НЕ даёт права заключить «остальное — провод».
        """
        if self.blocks_without_server_time or self.blocks == 0:
            return None
        if self.worker_count != 1 or self.insert_wall_s <= 0:
            return None
        return (self.server_ns / 1_000_000_000) / self.insert_wall_s

    @property
    def server_share_of_insert(self) -> float | None:
        """Какая доля ВСТАВКИ была серверным временем. Работает при любом числе
        воркеров, в отличие от :attr:`server_share`.

        Знаменатель — не стенные часы, а ``insert_busy_s``: обе величины суммы
        по одним и тем же блокам, поэтому одновременность запросов сокращается и
        доля остаётся долей. Это тот же вопрос, что задаёт ``server_share``, но
        заданный так, чтобы на него можно было ответить.

        ``None``, когда считать нельзя: блоков не было, вставка не замерена или
        хотя бы один блок пришёл без ``elapsed_ns``.
        """
        if self.blocks == 0 or self.insert_busy_s <= 0:
            return None
        if self.blocks_without_server_time:
            return None
        return (self.server_ns / 1_000_000_000) / self.insert_busy_s

    @property
    def worker_occupancy(self) -> float | None:
        """Насколько пул воркеров был занят: 1,0 — работали непрерывно.

        Отвечает на «кто кого ждал». Заметно ниже единицы — воркеры простаивали,
        значит узкое место в продюсере. Около единицы вместе с большим
        ``producer_stall_s`` — наоборот, продюсер ждал провод или сервер.
        """
        if self.blocks == 0 or self.insert_wall_s <= 0 or self.worker_count <= 0:
            return None
        return self.insert_busy_s / (self.worker_count * self.insert_wall_s)

    @property
    def producer_unattributed_s(self) -> float | None:
        """Время потока продюсера, не попавшее ни в одну измеренную стадию.

        Сюда попадают `progress_callback`, отправка задач в пул и работа с
        набором futures. Величина нужна, чтобы разложение стадий было ПОЛНЫМ:
        пока такого поля нет, любой незамеренный кусок молча приписывается
        «ожиданию HTTP» — ровно так прогон 5 и оставил 85% времени без объяснения.
        """
        if self.blocks == 0 or self.insert_wall_s <= 0:
            return None
        measured = self.read_s + self.convert_s + self.serialize_s + self.producer_stall_s
        return self.insert_wall_s - measured


@dataclass(frozen=True)
class RunConfig:
    """Позиции ручек, с которыми фактически шёл прогон.

    Ровно то, чего нельзя восстановить сегодня: ``~/.csv_click/settings.json``
    старше полей ``max_insert_payload_mb`` и ``load_workers``, а действующие
    значения писались только в ``st.empty()``, который любой rerun уничтожает.
    Без этой записи сравнение «до/после» не имеет смысла.
    """

    batch_size: int
    max_insert_payload_mb: int
    effective_insert_payload_bytes: int
    load_workers: int
    strict_preflight: bool
    schema_inference_mode: str
    separator: str
    encoding: str
    database: str
    table: str
    cluster: str
    order_by: str
    partition_by: str | None
    sharding_key: str


class DriverRetryCounter(logging.Handler):
    """Считает молчаливые перезаливки тела запроса драйвером.

    clickhouse-connect повторяет POST целиком один раз на
    ``ConnectionResetError``/``BrokenPipeError``, сообщая об этом только на
    уровне DEBUG (``httpclient.py:602-617``, ``max_attempts = max(2, retries + 1)``
    = 2 на пути инсерта). Каждое такое событие невидимо удваивает отправленные
    байты и читается как доказательство узкого канала, поэтому
    ``N × payload_bytes`` надо вычитать до любого расчёта МБ/с.

    Контекстный менеджер поднимает уровень логгера драйвера до DEBUG на время
    загрузки и отключает ему propagate, чтобы одна DEBUG-строка на запрос не
    ушла в консоль; записи от WARNING и выше пробрасываются дальше вручную,
    чтобы реальные предупреждения драйвера не потерялись.

    Логгер процесс-глобальный, а Streamlit может запустить вторую загрузку, пока
    первая ещё жива. Поэтому состояние логгера сохраняет только САМЫЙ ПЕРВЫЙ
    экземпляр, а восстанавливает — ПОСЛЕДНИЙ отцепившийся, кто бы им ни оказался.
    Выходить экземпляры могут не по стеку: если начавшая первой загрузка первой и
    закончится, восстановление «на своём выходе» вернуло бы логгер в исходное
    состояние посреди работы второй, а та потом записала бы поверх DEBUG с
    ``propagate=False`` навсегда.
    """

    #: Состояние логгера до того, как его тронул первый экземпляр.
    _saved_state: tuple[int, bool] | None = None

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.count = 0
        self._logger: logging.Logger | None = None
        self._parent: logging.Logger | None = None
        self._forwards_warnings = False

    def emit(self, record: logging.LogRecord) -> None:
        # Handler.handle() держит лок вокруг emit(), поэтому инкремент безопасен
        # при нескольких воркер-потоках драйвера.
        if DRIVER_RETRY_MESSAGE in record.getMessage():
            self.count += 1
        # Пробрасывает только первый прицепившийся — иначе пересекающиеся
        # счётчики продублировали бы одно предупреждение в логе.
        if self._forwards_warnings and record.levelno >= logging.WARNING and self._parent is not None:
            self._parent.handle(record)

    def __enter__(self) -> DriverRetryCounter:
        logger = logging.getLogger(DRIVER_LOGGER_NAME)
        self._logger = logger
        self._parent = logger.parent
        self._forwards_warnings = not _attached_retry_counters(logger)
        if DriverRetryCounter._saved_state is None:
            DriverRetryCounter._saved_state = (logger.level, logger.propagate)
            logger.setLevel(logging.DEBUG)
            logger.propagate = False
        logger.addHandler(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self._logger is None:
            return
        self._logger.removeHandler(self)
        if _attached_retry_counters(self._logger):
            return
        saved = DriverRetryCounter._saved_state
        DriverRetryCounter._saved_state = None
        if saved is not None:
            level, propagate = saved
            self._logger.setLevel(level)
            self._logger.propagate = propagate


def _attached_retry_counters(logger: logging.Logger) -> list[DriverRetryCounter]:
    return [handler for handler in logger.handlers if isinstance(handler, DriverRetryCounter)]


def library_versions() -> dict[str, str]:
    versions: dict[str, str] = {"python": platform.python_version()}
    for package in TRACKED_PACKAGES:
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            versions[package] = "not installed"
    return versions


def arrow_pool_high_water_bytes() -> int | None:
    """Высшая точка пула PyArrow, ``None`` если pyarrow не установлен.

    В pandas 3.0 каждая строковая колонка живёт в этом пуле, а ``tracemalloc``
    его не видит. Величина монотонная и на весь процесс, а не на загрузку:
    между двумя прогонами в одной сессии Streamlit её сравнивать нельзя.
    """
    try:
        import pyarrow
    except ImportError:
        return None
    return int(pyarrow.default_memory_pool().max_memory())


def first_pooled_socket(client: object):
    """Первый живой сокет из пула urllib3 внутри клиента, или ``None``.

    Два подводных камня, оба проверены на установленной версии:
    ``RecentlyUsedContainer.values()`` бросает ``NotImplementedError``
    («Iteration over this class is unlikely to be threadsafe»), поэтому идти
    можно только через ``keys()``; а пул соединений — ``LifoQueue``, и
    следующий запрос получит ПОСЛЕДНИЙ элемент ``queue``, поэтому перебирать
    надо с конца, иначе вернётся соединение, которое так и останется простаивать.
    """
    pools = getattr(getattr(client, "http", None), "pools", None)
    if pools is None:
        return None
    for key in list(pools.keys()):
        pool = pools.get(key)
        connections = getattr(getattr(pool, "pool", None), "queue", None)
        for connection in reversed(list(connections or [])):
            sock = getattr(connection, "sock", None)
            if sock is not None and sock.fileno() != -1:
                return sock
    return None


def describe_connection_path(client: object) -> dict[str, object]:
    """Каким сетевым путём шла загрузка — без самих адресов.

    Закрывает вопрос, без которого сравнение «до/после» не имеет смысла: прогон
    шёл через VPN-туннель или напрямую. Для этого достаточно СЕТИ /16 и признака
    приватности; адреса хостов не записываются намеренно. Запись о прогоне — это
    то, что оператор прикладывает к issue или к письму, и ни адрес его машины,
    ни адрес узла кластера туда попадать не должны.

    Ловит любое исключение и кладёт его в поле результата: это лезет во
    внутренности urllib3, а диагностика не имеет права уронить
    сорокапятиминутную загрузку.
    """
    try:
        sock = first_pooled_socket(client)
        if sock is None:
            return {"unavailable": "no open socket in the urllib3 pool"}
        local = _network_of(sock.getsockname()[0])
        remote = _network_of(sock.getpeername()[0])
        return {
            "local_network": local[0],
            "local_is_private": local[1],
            "remote_network": remote[0],
            "remote_is_private": remote[1],
        }
    except Exception as exc:
        return {"unavailable": f"{type(exc).__name__}: {exc}"}


def _network_of(address: str) -> tuple[str, bool | None]:
    """Сеть /16 и признак приватности. Хостовая часть отбрасывается."""
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return ("unparsed", None)
    prefix = 16 if parsed.version == 4 else 32
    network = ipaddress.ip_network(f"{address}/{prefix}", strict=False)
    return (str(network), parsed.is_private)


def describe_source_file(csv_path: Path) -> dict[str, object]:
    """Факты о файле-источнике для записи о прогоне.

    Приложение проверяет источник одним ``Path.exists()``, а это проходит и для
    сетевого диска, и для облачной заглушки, чьих байтов на машине нет, — то
    есть ровно для двух случаев, которые мимикрируют под «медленный канал».
    Ошибки чтения атрибутов не глотаем: они попадают в запись значением поля,
    чтобы непроверенный прогон нельзя было спутать с проверенным.
    """
    facts: dict[str, object] = {
        "path": str(csv_path),
        "size_bytes": None,
        "drive_type": _drive_type(csv_path),
        "cloud_placeholder": None,
    }
    try:
        stat_result = csv_path.stat()
    except OSError as exc:
        facts["stat_error"] = str(exc)
        return facts

    facts["size_bytes"] = stat_result.st_size
    attributes = getattr(stat_result, "st_file_attributes", None)
    if attributes is not None:
        facts["cloud_placeholder"] = bool(attributes & _CLOUD_PLACEHOLDER_ATTRIBUTES)
    return facts


SERVER_TIME_CAVEAT = (
    "This covers the initiator only: with async Distributed insert the shard-side commit, "
    "replication and merges are not included, so a small share does not prove the rest "
    "was the wire."
)


def format_load_stats_lines(stats: LoadStats) -> list[str]:
    """Раскладка прогона по часам — без неё «45 минут» не на что списать."""
    lines = [
        f"Timing: preflight {stats.preflight_s:.2f} s, connect {stats.connect_s:.2f} s, "
        f"DDL {stats.ddl_s:.2f} s, insert wall {stats.insert_wall_s:.2f} s.",
        # compress печатается только когда сжатие реально было: «compress 0.00 s»
        # неотличимо от измерения «сжатие ничего не стоит», а сжатия в пути ещё нет.
        f"Client stages inside insert: read {stats.read_s:.2f} s, "
        f"convert {stats.convert_s:.2f} s, serialize {stats.serialize_s:.2f} s"
        + (
            f", compress {stats.compress_s:.2f} s."
            if stats.compress_s
            else ". The body is not compressed on this path, so there is no compress stage."
        ),
        f"Bytes: source {stats.src_bytes / 1024 / 1024:.1f} MB, raw payload "
        f"{stats.raw_bytes / 1024 / 1024:.1f} MB, on wire "
        f"{stats.wire_bytes / 1024 / 1024:.1f} MB in {stats.blocks} blocks.",
        _server_time_line(stats),
    ]

    if stats.arrow_bytes is not None:
        started_at = stats.arrow_bytes_at_start or 0
        lines.append(
            f"PyArrow pool high-water: {stats.arrow_bytes / 1024 / 1024:.1f} MB at the end "
            f"against {started_at / 1024 / 1024:.1f} MB before the load. The mark is "
            "monotonic and process-wide, so only the growth is attributable to this run."
        )

    occupancy = stats.worker_occupancy
    if occupancy is not None:
        share = stats.server_share_of_insert
        share_text = (
            f"of which the server reported {share * 100:.1f}%"
            if share is not None
            else "the server's share is not computable"
        )
        lines.append(
            f"Who waited for whom: producer stalled {stats.producer_stall_s:.2f} s on a full "
            f"queue, workers were busy {occupancy * 100:.0f}% of the time, one insert took "
            f"{stats.insert_busy_s / stats.blocks:.2f} s on average and {share_text}. "
            "Low occupancy means the producer is the limit; high occupancy with a long stall "
            "means the wire or the server is."
        )

    unattributed = stats.producer_unattributed_s
    if unattributed is not None and stats.insert_wall_s > 0:
        lines.append(
            f"Unattributed producer time: {unattributed:.2f} s "
            f"({unattributed / stats.insert_wall_s * 100:.1f}% of insert wall). This is the "
            "progress callback, submitting tasks and bookkeeping — a large value here means "
            "the stage breakdown is hiding something."
        )

    if stats.driver_retries:
        lines.append(
            f"WARNING: the driver silently re-uploaded a request body "
            f"{stats.driver_retries} time(s), and wire_bytes above does NOT include those "
            f"copies. The link carried up to {stats.driver_retries} extra payloads, so its "
            "real throughput is HIGHER than these bytes imply — do not read the low figure "
            "as a narrow pipe."
        )
    return lines


def _server_time_line(stats: LoadStats) -> str:
    server_s = stats.server_ns / 1_000_000_000
    if stats.blocks == 0:
        return "Server time: no blocks were inserted, nothing to report."
    if stats.blocks_without_server_time:
        return (
            f"Server time is not computable: {stats.blocks_without_server_time} of "
            f"{stats.blocks} blocks returned no elapsed_ns, which is what a proxy that "
            "strips X-ClickHouse-Summary looks like. The server time is understated by an "
            "unknown amount, so no share is printed."
        )
    if stats.worker_count != 1:
        return (
            f"Server reported {server_s:.2f} s summed over {stats.worker_count} concurrent "
            f"connections, against {stats.insert_wall_s:.2f} s of insert wall time. That sum "
            "is NOT a share of wall time because the requests overlapped, and dividing it "
            "would inflate the result by up to the worker count. Re-run with "
            f"load_workers=1 for a comparable share. {SERVER_TIME_CAVEAT}"
        )
    share = stats.server_share
    if share is None:
        return f"Server reported {server_s:.2f} s; insert wall time is not known yet."
    return f"Server reported {server_s:.2f} s = {share * 100:.1f}% of insert wall time. {SERVER_TIME_CAVEAT}"


def write_run_record(
    *,
    config: RunConfig,
    stats: LoadStats,
    csv_path: Path,
    outcome: str,
    timestamp: datetime,
    error: str | None = None,
    directory: Path = DEFAULT_RUN_LOG_DIR,
) -> Path:
    """Сохраняет конфигурацию и счётчики прогона в файл и возвращает путь."""
    directory.mkdir(parents=True, exist_ok=True)

    stats_record = asdict(stats)
    stats_record["server_share"] = stats.server_share
    record = {
        "outcome": outcome,
        "error": error,
        "finished_at": timestamp.isoformat(),
        "platform": platform.platform(),
        "config": asdict(config),
        "stats": stats_record,
        "source": describe_source_file(csv_path),
        "libraries": library_versions(),
    }

    stamp = timestamp.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    record_path = _unused_path(directory / f"{stamp}-{_safe_file_name(config.table)}.json")
    record_path.write_text(
        json.dumps(record, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return record_path


def _unused_path(path: Path) -> Path:
    """Не даёт двум прогонам в одну секунду затереть друг друга.

    Имя состоит из секунды и имени таблицы, а быстрое падение до вставки
    укладывается в ту же секунду, что и предыдущее, — потерять запись о падении
    дороже, чем носить суффикс.
    """
    if not path.exists():
        return path
    for suffix in range(2, 1000):
        candidate = path.with_name(f"{path.stem}-{suffix}{path.suffix}")
        if not candidate.exists():
            return candidate
    raise OSError(f"Too many run records for the same second: {path}")


def _safe_file_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", name)


def _drive_type(csv_path: Path) -> str:
    if sys.platform != "win32":
        return "not_windows"
    anchor = csv_path.anchor
    if not anchor:
        return "unknown"
    try:
        code = ctypes.windll.kernel32.GetDriveTypeW(ctypes.c_wchar_p(anchor))
    except (AttributeError, OSError) as exc:
        return f"unavailable: {exc}"
    return _DRIVE_TYPES.get(code, f"code_{code}")
