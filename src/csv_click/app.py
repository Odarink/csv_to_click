from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import streamlit as st

from csv_click.clickhouse import (
    ClickHouseConfig,
    build_create_distributed_table_sql,
    build_create_local_table_sql,
    build_table_names,
    get_client,
    test_connection,
)
from csv_click.errors import (
    CertificateError,
    ClickHouseConnectionError,
    CsvReadCancelled,
    CsvSchemaError,
)
from csv_click.load_job import (
    TABLES_CLEANUP_FAILED,
    TABLES_CREATED,
    TABLES_DROPPED_AS_EMPTY,
    TABLES_KEPT_WITH_DATA,
    TABLES_NOT_CREATED,
    LoadJob,
    current_load_job,
    start_load_job,
)
from csv_click.load_stats import LoadStats
from csv_click.pandas_loader import (
    COMPRESSION_CODECS,
    DEFAULT_SCHEMA_SAMPLE_ROWS,
    ReadOptions,
    analyze_csv_with_pandas_sample,
    analyze_csv_with_pandas_chunks,
    choose_read_options_for_preview,
    mappings_from_editor_rows,
    mappings_to_editor_rows,
    mappings_to_schema,
    schema_to_mappings,
)
from csv_click.schema import (
    CLICKHOUSE_TYPE_OPTIONS,
    CsvSchema,
)
from csv_click.settings import AppSettings, load_app_settings, save_app_settings


@dataclass(frozen=True)
class LoadStep:
    key: str
    title: str


#: Единственный источник правды о шагах. Заголовки и строка пути берутся отсюда,
#: а не пишутся руками: иначе нумерация разъедется при первой вставке шага.
LOAD_STEPS: tuple[LoadStep, ...] = (
    LoadStep("csv", "Read CSV"),
    LoadStep("mapping", "Column mapping"),
    LoadStep("types", "Type review"),
    LoadStep("params", "ClickHouse and load parameters"),
    LoadStep("load", "Create tables and load"),
)


def _step_number(key: str) -> int | None:
    for index, step in enumerate(LOAD_STEPS, start=1):
        if step.key == key:
            return index
    return None


def step_heading(key: str) -> str:
    """Заголовок блока с номером: «Step 2 of 5 — Column mapping».

    Раньше заголовки были не связаны между собой, и сколько шагов всего, узнать
    было нельзя. А поскольку следующий блок появляется только после
    подтверждения предыдущего, отсутствие блока читалось как «интерфейс пропал».
    """
    number = _step_number(key)
    if number is None:
        return key
    return f"Step {number} of {len(LOAD_STEPS)} — {LOAD_STEPS[number - 1].title}"


def step_path_line(current_key: str) -> str:
    """Одна строка пути: что пройдено, где мы, что впереди.

    Строка, а не `st.progress`: полоса показывает долю, но не названия, а вопрос
    пользователя - «что дальше». Неизвестный ключ не роняет экран: строка просто
    остаётся без отметки «вы здесь».
    """
    current = _step_number(current_key)
    parts: list[str] = []
    for index, step in enumerate(LOAD_STEPS, start=1):
        if current is not None and index < current:
            parts.append(f"{index}. {step.title} ✓")
        elif current is not None and index == current:
            parts.append(f"**{index}. {step.title} ← вы здесь**")
        else:
            parts.append(f"{index}. {step.title}")
    return " · ".join(parts)


#: Шаги, отрисованные за этот прогон. Ключ к тому, чтобы страница не врала о
#: позиции: спрашивать не флаги, а то, что реально попало на экран.
RENDERED_STEPS_KEY = "rendered_steps"


def _mark_step_rendered(step_key: str) -> None:
    if step_key:
        st.session_state.setdefault(RENDERED_STEPS_KEY, []).append(step_key)


def current_step_key() -> str:
    """Самый глубокий шаг, который РЕАЛЬНО отрисовался за этот прогон.

    Выводить шаг из подтверждающих флагов оказалось нельзя: блок ниже может
    упасть на проверке - дубликат имени колонки, неверный `custom_type` - и
    вернуть `None`, не сняв флаг. Тогда страница заканчивалась вторым шагом,
    а строка сверху ставила галочки на втором и третьем и объявляла четвёртый.
    Флаг говорит «это подтверждали», а не «это сейчас на экране».

    Заодно так появляется последний шаг: у него своего флага нет вовсе, и по
    флагам он не мог стать текущим никогда.
    """
    rendered = st.session_state.get(RENDERED_STEPS_KEY) or []
    numbered = [(_step_number(key) or 0, key) for key in rendered]
    if not numbered:
        return LOAD_STEPS[0].key
    return max(numbered)[1]


def main() -> None:
    st.set_page_config(page_title="CSV to ClickHouse", layout="wide")
    st.title("CSV to ClickHouse")
    # Место занимается сразу, а заполняется в конце: состояние меняют сами блоки,
    # которые рисуются ниже. Нарисованная до них строка отставала на шаг - после
    # «Apply types» показывала третий, когда на экране уже стоял четвёртый.
    path_slot = st.empty()
    st.session_state[HELP_SLOTS_KEY] = []
    st.session_state[RENDERED_STEPS_KEY] = []
    try:
        _render_load_flow()
        # Панель загрузки живёт ВНЕ потока шагов: после F5 session_state пуст и
        # поток обрывается на первом шаге, а идущая в фоне заливка и итог
        # последней обязаны остаться на экране.
        _render_load_panel()
    finally:
        path_slot.caption(step_path_line(current_step_key()))
        _fill_step_help_slots()


SCHEMA_INFERENCE_SAMPLE = "Fast sample, 100000 rows"
SCHEMA_INFERENCE_FULL_SCAN = "Full scan"
SCHEMA_INFERENCE_OPTIONS = [SCHEMA_INFERENCE_SAMPLE, SCHEMA_INFERENCE_FULL_SCAN]
CSV_READ_STATE_KEYS = [
    "csv_path",
    "schema_rows",
    "mapping_rows",
    "type_rows",
    "mapping_confirmed",
    "types_confirmed",
    "load_params",
    "csv_preview_rows",
    "csv_preview_warning",
]
#: Слоты выбора ключей: живут в пределах одного прочитанного CSV. Выбор оператора
#: обязан переживать перерисовки, но НЕ переезжать на другой файл: у чужого файла
#: колонка с тем же именем - другие данные, а ключ выглядел бы выбранным.
CHOICE_STATE_KEYS = [
    "order_by_choice",
    "sharding_column_choice",
]
#: Сколько последних строк лога уходит в сокет. Полный лог копится в памяти, а
#: отправка его целиком на каждый блок и была той самой O(n²).
LOAD_LOG_TAIL_LINES = 400
#: Как часто фрагмент прогресса перечитывает состояние задачи. Загрузка больше
#: не пишет в интерфейс сама — интерфейс читает её состояние по таймеру, и
#: RerunException физически не может дотянуться до продюсера.
LOAD_PROGRESS_POLL_S = 1.0


def _render_load_flow() -> None:
    csv_context = _render_csv_path_step()
    if not csv_context:
        return

    mappings = _render_column_mapping_editor()
    if mappings is None:
        return

    schema = _render_type_editor()
    if schema is None:
        return

    params = _render_connection_and_load_form(schema)
    if not params:
        return

    config = params["config"]
    csv_path = csv_context["csv_path"]
    read_options = params["read_options"]
    distributed_table = params["distributed_table"]
    table_names = build_table_names(distributed_table)

    st.info(f"Local table: `{table_names.local}`")

    col_test, _ = st.columns(2)
    with col_test:
        if st.button(
            "Test connection",
            type="secondary",
            use_container_width=True,
            key="test_connection_final_button",
        ):
            _test_connection(config)

    try:
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
            sharding_key=params["sharding_key"],
        )
    except ValueError as exc:
        st.error(f"DDL parameter error: {exc}")
        return

    st.subheader(step_heading("load"))
    _render_final_actions_help()

    if st.button("Preview DDL", use_container_width=True):
        st.subheader("Local table DDL")
        st.code(ddl_local, language="sql")
        st.subheader("Distributed table DDL")
        st.code(ddl_distributed, language="sql")

    if st.button("Create tables and load", type="primary", use_container_width=True):
        _start_load(
            config=config,
            csv_path=csv_path,
            read_options=read_options,
            schema=schema,
            distributed_table=distributed_table,
            order_by=params["order_by"],
            partition_by=params["partition_by"],
            max_insert_payload_mb=params["max_insert_payload_mb"],
            load_workers=params["load_workers"],
            insert_compression=params["insert_compression"],
            strict_preflight=params["strict_preflight"],
            sharding_key=params["sharding_key"],
        )


#: Отложенные подсказки текущего прогона: место занято, содержимое допишется в
#: конце. Живут в `session_state`, а не в модуле: у процесса может быть несколько
#: сессий, и общий на всех список смешал бы их подсказки.
HELP_SLOTS_KEY = "help_slots"


def _render_step_help(title: str, body: str, step_key: str = "") -> None:
    """Объяснение шага. Раскрыто, только если это ТЕКУЩИЙ шаг.

    Тексты были и раньше, и они по делу - но все лежали свёрнутыми, и человек,
    не знавший, что внутри объяснение, туда не заглядывал. Раскрывать все нельзя:
    экран превращается в простыню.

    Место занимается сейчас, а раскрытие решается в конце прогона: состояние
    меняют сами блоки, которые рисуются ниже. Решая на месте, подсказка первого
    шага оставалась раскрытой после того, как человек уже ушёл на второй, - и
    раскрытыми оказывались две.
    """
    slot = st.empty()
    # Подсказка есть у каждого шага и рисуется в его начале, поэтому она же -
    # единственная надёжная отметка «этот блок на экране».
    _mark_step_rendered(step_key)
    st.session_state.setdefault(HELP_SLOTS_KEY, []).append((slot, title, body, step_key))


def _fill_step_help_slots() -> None:
    current = current_step_key()
    for slot, title, body, step_key in st.session_state.get(HELP_SLOTS_KEY, []):
        with slot.expander(title, expanded=bool(step_key) and step_key == current):
            st.markdown(body)


def _render_csv_path_help() -> None:
    _render_step_help(
        "Как указать CSV файл",
        r"""
Укажите путь к CSV файлу на той машине, где запущен Streamlit. Если приложение
запущено на сервере или в JupyterHub, путь должен быть серверным, а не путем с
вашего локального компьютера.

Примеры:

- Windows: `C:\Users\<user>\Downloads\data.csv`
- Linux/JupyterHub: `/home/jovyan/data/data.csv`

`Separator` задает разделитель колонок: `,`, `;`, `\t`, `|` или свое значение
через `custom`. `Encoding` задает кодировку файла: обычно `utf_8`, для русских
CSV из Windows часто подходит `cp1251` или `windows-1251`.

После `Read CSV` приложение прочитает preview, подберет эффективные настройки
чтения и определит начальную схему колонок.
""",
        step_key="csv",
    )


def _render_column_mapping_help() -> None:
    _render_step_help(
        "Как настроить колонки",
        """
Проверьте, какие колонки из CSV попадут в ClickHouse:

- `source_name` - исходное имя колонки в CSV;
- target_name станет именем колонки в ClickHouse;
- `include` определяет, загружать колонку или исключить ее.

Пустые `target_name` и дублирующиеся целевые имена не допускаются. После проверки
нажмите `Apply column mapping`, чтобы перейти к типам.
""",
        step_key="mapping",
    )


def _render_type_review_help() -> None:
    _render_step_help(
        "Как проверить типы",
        """
Проверьте типы перед созданием таблицы:

- `inferred_type` - тип, который приложение определило по CSV;
- `final_type` - итоговый тип ClickHouse для загрузки;
- custom_type переопределяет final_type, если поле заполнено;
- `nullable` оборачивает тип в `Nullable(...)`;
- `sample_values` помогает сверить тип с реальными значениями;
- `notes` объясняет, почему выбран именно такой тип.

Примеры: `Decimal(18, 2)` для денежных сумм, `Nullable(DateTime)` для дат и
времени с пропусками.

Колонки, где есть ведущий ноль, ведущий плюс или не-ASCII цифры (счета, БИК,
ИНН, КПП, телефоны, индексы), остаются `String`: числовой тип съел бы эти
символы. Поменять на `UInt64` можно вручную, но тогда `00123` уедет как `123`.

Решение принимается по просмотренным строкам. В режиме `Fast sample` первое
такое значение может лежать за выборкой - тогда колонка останется числом, и
`Strict preflight validation` этого НЕ поймает: разбор `00123` в число проходит
без ошибки. Если такие колонки возможны, берите `Full scan` или ставьте
`String` руками.
""",
        step_key="types",
    )


def _render_clickhouse_params_help() -> None:
    _render_step_help(
        "Как заполнить параметры ClickHouse",
        """
Заполните параметры целевой таблицы и подключения:

- `Database` - база ClickHouse, куда будет создана таблица;
- `Distributed table name` - имя распределенной таблицы;
- `Cluster` - кластер для `ON CLUSTER`;
- для my_table будет создана локальная таблица my_table_local;
- `ORDER BY` - ключ сортировки локальной `ReplicatedMergeTree`;
- `PARTITION BY` - необязательное выражение партиционирования;
- `Distributed sharding key` - колонка для `sipHash64(...)` в `Distributed`;
- `Batch size` - размер CSV чанка при проверке и загрузке;
- `Max insert payload, MB` - максимальный размер одного HTTP `JSONEachRow` insert request;
- `Strict preflight validation` заранее проверяет конвертацию CSV в выбранные
  типы ClickHouse.

Пример: ORDER BY = customer_id, `PARTITION BY = toYYYYMM(dt)`,
`sharding key = customer_id`.
""",
        step_key="params",
    )


def _render_final_actions_help() -> None:
    _render_step_help(
        "Порядок финальных действий",
        """
Рекомендуемый порядок:

1. Нажмите `Test connection`, чтобы проверить подключение через `SELECT 1`.
2. Нажмите `Preview DDL`, если нужно посмотреть SQL перед созданием таблиц.
3. Нажмите `Create tables and load`, чтобы создать таблицы и загрузить CSV.

Preview DDL только показывает SQL и ничего не создает. `Create tables and load`
создает local и distributed таблицы, затем загружает CSV чанками через
`JSONEachRow`.

Загрузка блокируется, если distributed или local таблица уже существует. Для
`my_table` проверяются `my_table` и `my_table_local`.
""",
        step_key="load",
    )


def _choice_widget_key(state_key: str) -> str:
    return f"{state_key}_widget"


def _remember_choice(state_key: str) -> None:
    """Запомнить выбор ОПЕРАТОРА. Вызывается Streamlit только при его действии."""
    st.session_state[state_key] = st.session_state[_choice_widget_key(state_key)]


def _remembered_selectbox(label: str, options: list[str], state_key: str) -> str | None:
    """Selectbox, переживающий пропуск отрисовки и смену списка колонок.

    Streamlit хранит состояние виджета, только пока тот рисуется на каждом
    прогоне: стоит форме пропасть на один rerun - выбор молча съезжает на первую
    колонку, и таблица создаётся не с тем ключом. Свой слот в session_state
    живёт независимо от виджета и переживает пропуск.

    Три обязательства, каждое куплено дефектом:

    - `index=` НЕ передаётся. В Streamlit 1.58 index входит в тождество виджета,
      поэтому восстановление через index ломало второй выбор подряд: оператор
      исправлял ошибочный ключ, клик уходил в старое тождество и молча пропадал.
      Значение восстанавливается предустановкой ключа виджета, а `key=` держит
      тождество постоянным.
    - Слот пишется ТОЛЬКО из `on_change`, то есть по действию оператора. Иначе
      нетронутый дефолт запоминался как выбор и позже всплывал сообщением
      "Previously selected" про то, чего оператор не выбирал.
    - Колонка ушла из списка - выбор аннулируется здесь же, слот удаляется, и
      предупреждение звучит один раз, в тот прогон, когда это случилось. Держать
      пропавший выбор в слоте было нельзя: `on_change` срабатывает только на
      ИЗМЕНЕНИЕ значения (`_widget_changed` в session_state.py), поэтому выбрать
      объявленную замену оператор не мог - слот оставался прежним, предупреждение
      висело неснимаемо, а возврат колонки молча уводил ключ от того, что
      оператор видел последним. Цена решения: вернувшаяся колонка выбор не
      воскрешает, и это честнее - её исключил сам оператор, предупреждённый.

    Замену в ключ виджета не пишем: Streamlit сам гасит значение вне options.
    И что оператору объявлена ровно та колонка, которая уедет в DDL, и что гашение
    вообще случилось, держит один тест - test_dropped_order_by_selection_warns_and_falls_back:
    он сверяет текст предупреждения с `load_params` на том прогоне, где оно звучит.
    """
    widget_key = _choice_widget_key(state_key)
    stored = st.session_state.get(state_key)
    if stored is not None and stored not in options:
        if options:
            st.warning(
                f'Previously selected {label} "{stored}" is no longer among the '
                f'columns; falling back to "{options[0]}".'
            )
        st.session_state.pop(state_key, None)
        stored = None
    if stored is not None:
        st.session_state[widget_key] = stored
    return st.selectbox(
        label,
        options=options,
        key=widget_key,
        on_change=_remember_choice,
        args=(state_key,),
    )


def _render_connection_and_load_form(schema: CsvSchema) -> dict[str, object] | None:
    settings = _get_app_settings()
    target_names = _schema_target_names(schema)
    st.subheader(step_heading("params"))
    _render_clickhouse_params_help()
    left, right = st.columns(2)
    with left:
        database = st.text_input("Database", value=settings.database)
        distributed_table = st.text_input("Distributed table name")
        cluster = st.text_input("Cluster", value=settings.cluster)
        order_by = _remembered_selectbox("ORDER BY", target_names, "order_by_choice")
        partition_by = st.text_input("PARTITION BY (optional)")
        sharding_column = _remembered_selectbox(
            "Distributed sharding key", target_names, "sharding_column_choice"
        )
        batch_size = st.number_input(
            "Batch size",
            min_value=1,
            value=settings.batch_size,
            step=10_000,
        )
        max_insert_payload_mb = st.number_input(
            "Max insert payload, MB",
            min_value=1,
            value=settings.max_insert_payload_mb,
            step=1,
            help="Upper bound for one HTTP JSONEachRow insert request.",
        )
        load_workers = st.number_input(
            "Load workers",
            min_value=1,
            max_value=6,
            value=settings.load_workers,
            step=1,
            help="Parallel HTTP JSONEachRow insert workers. Use 1 for sequential loading.",
        )
        compression_options = ["off", *COMPRESSION_CODECS]
        insert_compression = st.selectbox(
            "Insert compression",
            options=compression_options,
            index=compression_options.index(settings.insert_compression)
            if settings.insert_compression in compression_options
            else 0,
            key="insert_compression_select",
            help=(
                "Compress the request body before sending. Measured on the operator's "
                "profile: zstd gives 3.8x fewer bytes for 19 s of CPU on a 9.5 GB load. "
                "Off by default because Content-Encoding has never been tried against "
                "this proxy — turn it on and the first block will tell you within seconds."
            ),
        )
        strict_preflight = st.checkbox(
            "Strict preflight validation",
            value=settings.strict_preflight,
        )
    with right:
        host = st.text_input("Host", value=settings.host)
        port = st.number_input("Port", min_value=1, max_value=65535, value=settings.port)
        username = st.text_input(
            "Username",
            value=settings.username or os.getenv("CLICKHOUSE_USER", ""),
        )
        password = st.text_input(
            "Password",
            value=os.getenv("CLICKHOUSE_PASSWORD", ""),
            type="password",
        )
        secure = st.checkbox("Secure", value=settings.secure)
        verify = st.checkbox("Verify TLS", value=settings.verify)
        client_cert = st.text_input("Client cert path", value=settings.client_cert)
        client_key = st.text_input("Client key path", value=settings.client_key)

    apply_col, test_col = st.columns(2)
    with apply_col:
        submitted = st.button(
            "Apply parameters",
            use_container_width=True,
            key="apply_load_params_button",
        )
    with test_col:
        test_submitted = st.button(
            "Test connection",
            use_container_width=True,
            key="test_load_params_connection_button",
        )

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

    if test_submitted:
        _test_connection(config)

    errors = []
    if not database:
        errors.append("Database is required")
    if not distributed_table:
        errors.append("Distributed table name is required")
    if not order_by:
        errors.append("ORDER BY is required")
    if not sharding_column:
        errors.append("Distributed sharding key is required")

    if errors:
        if submitted:
            for error in errors:
                st.error(error)
        return None

    csv_read_options = st.session_state.get("csv_read_options", ReadOptions())
    read_options = ReadOptions(
        separator=csv_read_options.separator,
        encoding=csv_read_options.encoding,
        batch_size=int(batch_size),
    )
    params = {
        "read_options": read_options,
        "distributed_table": distributed_table,
        "order_by": order_by,
        "partition_by": partition_by or None,
        "sharding_key": sharding_column,
        "batch_size": int(batch_size),
        "max_insert_payload_mb": int(max_insert_payload_mb),
        "load_workers": int(load_workers),
        "insert_compression": insert_compression,
        "strict_preflight": strict_preflight,
        "config": config,
    }
    st.session_state["load_params"] = params
    if submitted:
        _save_app_settings(
            AppSettings(
                host=host,
                port=int(port),
                username=username,
                secure=secure,
                verify=verify,
                client_cert=client_cert,
                client_key=client_key,
                database=database,
                cluster=cluster,
                batch_size=int(batch_size),
                max_insert_payload_mb=int(max_insert_payload_mb),
                load_workers=int(load_workers),
                insert_compression=insert_compression,
                strict_preflight=strict_preflight,
                separator=read_options.separator,
                encoding=read_options.encoding,
            )
        )
    return params


def _get_app_settings() -> AppSettings:
    if "app_settings" not in st.session_state:
        st.session_state["app_settings"] = load_app_settings()
    return st.session_state["app_settings"]


def _save_app_settings(settings: AppSettings) -> None:
    try:
        save_app_settings(settings)
    except OSError as exc:
        st.warning(f"Could not save UI settings: {exc}")
    else:
        st.session_state["app_settings"] = settings


def _render_csv_path_step() -> dict[str, object] | None:
    current_options = st.session_state.get(
        "csv_read_options",
        _read_options_from_settings(_get_app_settings()),
    )
    st.subheader(step_heading("csv"))
    _render_csv_path_help()
    with st.form("csv_path_form"):
        csv_col, button_col = st.columns([5, 1])
        with csv_col:
            csv_path = st.text_input("CSV path", value=st.session_state.get("csv_path", ""))
        with button_col:
            st.write("")
            submitted = st.form_submit_button("Read CSV", use_container_width=True)

        st.subheader("CSV read settings")
        settings_left, settings_right = st.columns(2)
        with settings_left:
            separator_choice = st.selectbox(
                "Separator",
                options=[",", ";", "\\t", "|", "custom"],
                index=_separator_index(current_options.separator),
            )
            custom_separator = st.text_input(
                "Custom separator",
                value="" if current_options.separator in {",", ";", "\t", "|"} else current_options.separator,
            )
        with settings_right:
            encoding_choice = st.selectbox(
                "Encoding",
                options=["utf_8", "cp1251", "windows-1251", "utf-8-sig", "custom"],
                index=_encoding_index(current_options.encoding),
            )
            custom_encoding = st.text_input(
                "Custom encoding",
                value="" if current_options.encoding in {"utf_8", "cp1251", "windows-1251", "utf-8-sig"} else current_options.encoding,
            )
        current_schema_analysis_mode = st.session_state.get(
            "schema_analysis_mode",
            SCHEMA_INFERENCE_SAMPLE,
        )
        schema_analysis_mode = st.radio(
            "Schema inference mode",
            options=SCHEMA_INFERENCE_OPTIONS,
            index=0
            if current_schema_analysis_mode not in SCHEMA_INFERENCE_OPTIONS
            else SCHEMA_INFERENCE_OPTIONS.index(current_schema_analysis_mode),
            horizontal=True,
            help=(
                "Fast sample reads only the first 100000 rows for draft types. "
                "Full scan reads the entire CSV before type review."
            ),
        )

    if submitted:
        read_options = _read_options_from_form(
            separator_choice=separator_choice,
            custom_separator=custom_separator,
            encoding_choice=encoding_choice,
            custom_encoding=custom_encoding,
            current_options=current_options,
        )
        if read_options is not None:
            st.session_state["csv_read_options"] = read_options
            st.session_state["schema_analysis_mode"] = schema_analysis_mode
            _apply_csv_path(csv_path, read_options, schema_analysis_mode)

    if "csv_path" not in st.session_state:
        return None

    if st.button("Stop read CSV / choose another file", type="secondary", use_container_width=True):
        _request_stop_csv_read()
        st.rerun()

    st.caption(f"CSV path: `{st.session_state['csv_path']}`")
    _render_csv_preview()
    return {
        "csv_path": st.session_state["csv_path"],
        "read_options": st.session_state.get(
            "csv_read_options",
            _read_options_from_settings(_get_app_settings()),
        ),
    }


def _read_options_from_form(
    *,
    separator_choice: str,
    custom_separator: str,
    encoding_choice: str,
    custom_encoding: str,
    current_options: ReadOptions,
) -> ReadOptions | None:
    separator = custom_separator if separator_choice == "custom" else separator_choice
    separator = "\t" if separator == "\\t" else separator
    encoding = custom_encoding if encoding_choice == "custom" else encoding_choice
    if not separator:
        st.error("Separator is required")
        return None
    if not encoding:
        st.error("Encoding is required")
        return None
    return ReadOptions(
        separator=separator,
        encoding=encoding,
        batch_size=current_options.batch_size,
    )


def _render_csv_preview() -> None:
    preview = st.session_state.get("csv_preview_rows")
    if preview is None:
        return
    st.subheader("CSV preview")
    warning = st.session_state.get("csv_preview_warning")
    if warning:
        st.warning(str(warning))
    st.dataframe(preview, hide_index=True, use_container_width=True)


def _schema_target_names(schema: CsvSchema) -> list[str]:
    return [column.column_name for column in schema.columns]


def _separator_index(separator: str) -> int:
    normalized = "\\t" if separator == "\t" else separator
    options = [",", ";", "\\t", "|", "custom"]
    return options.index(normalized) if normalized in options else options.index("custom")


def _encoding_index(encoding: str) -> int:
    options = ["utf_8", "cp1251", "windows-1251", "utf-8-sig", "custom"]
    return options.index(encoding) if encoding in options else options.index("custom")


def _apply_csv_path(
    csv_path: str,
    read_options: ReadOptions,
    schema_analysis_mode: str = SCHEMA_INFERENCE_SAMPLE,
) -> None:
    if not csv_path:
        st.error("CSV path is required")
        return
    if not Path(csv_path).exists():
        st.error(f"CSV path does not exist: {csv_path}")
        return

    st.session_state["csv_read_cancel_requested"] = False
    st.session_state["csv_path"] = csv_path
    st.session_state["csv_read_options"] = read_options
    st.session_state["schema_analysis_mode"] = schema_analysis_mode
    _clear_csv_read_state(include_path=False)
    _analyze_csv(csv_path, read_options, schema_analysis_mode)


def _clear_csv_read_state(include_path: bool = True) -> None:
    for key in CSV_READ_STATE_KEYS:
        if key == "csv_path" and not include_path:
            continue
        st.session_state.pop(key, None)
    for key in CHOICE_STATE_KEYS:
        st.session_state.pop(key, None)
        st.session_state.pop(_choice_widget_key(key), None)


def _request_stop_csv_read() -> None:
    st.session_state["csv_read_cancel_requested"] = True
    _clear_csv_read_state()


def _csv_read_cancel_requested() -> bool:
    return bool(st.session_state.get("csv_read_cancel_requested", False))


def _read_options_from_settings(settings: AppSettings) -> ReadOptions:
    return ReadOptions(
        separator=settings.separator,
        encoding=settings.encoding,
        batch_size=settings.batch_size,
    )


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


def _analyze_csv(
    csv_path: str,
    read_options: ReadOptions,
    schema_analysis_mode: str = SCHEMA_INFERENCE_SAMPLE,
) -> None:
    try:
        spinner_text = (
            "Scanning full CSV chunks to infer schema..."
            if schema_analysis_mode == SCHEMA_INFERENCE_FULL_SCAN
            else f"Scanning first {DEFAULT_SCHEMA_SAMPLE_ROWS} rows to infer draft schema..."
        )
        with st.spinner(spinner_text):
            if _csv_read_cancel_requested():
                raise CsvReadCancelled("CSV read was stopped")
            effective_options, preview, warning = choose_read_options_for_preview(
                csv_path,
                read_options,
                nrows=20,
            )
            if _csv_read_cancel_requested():
                raise CsvReadCancelled("CSV read was stopped")
            if schema_analysis_mode == SCHEMA_INFERENCE_FULL_SCAN:
                schema = analyze_csv_with_pandas_chunks(
                    csv_path,
                    effective_options,
                    cancel_callback=_csv_read_cancel_requested,
                )
            else:
                schema = analyze_csv_with_pandas_sample(
                    csv_path,
                    effective_options,
                    nrows=DEFAULT_SCHEMA_SAMPLE_ROWS,
                )
            if _csv_read_cancel_requested():
                raise CsvReadCancelled("CSV read was stopped")
    except CsvReadCancelled:
        _clear_csv_read_state()
        st.warning("CSV read was stopped. Choose another file and press Read CSV.")
        return
    except CsvSchemaError as exc:
        st.error(f"CSV schema error: {exc}")
        return

    st.session_state["csv_read_options"] = effective_options
    st.session_state["schema_rows"] = mappings_to_editor_rows(schema_to_mappings(schema))
    st.session_state["mapping_rows"] = _schema_rows_to_mapping_rows(st.session_state["schema_rows"])
    st.session_state["csv_preview_rows"] = preview
    st.session_state["csv_preview_warning"] = warning.message if warning else None
    if schema_analysis_mode != SCHEMA_INFERENCE_FULL_SCAN:
        st.warning(
            "Schema was inferred from the first 100000 rows only. Review Type review carefully; "
            "use Full scan for exact inference or Strict preflight validation before loading."
        )
    st.success(f"Schema inferred for {len(schema.columns)} columns")


def _render_column_mapping_editor() -> list[dict[str, object]] | None:
    schema_rows = st.session_state.get("schema_rows")
    if not schema_rows:
        return None

    st.subheader(step_heading("mapping"))
    _render_column_mapping_help()
    mapping_rows = st.session_state.get("mapping_rows") or _schema_rows_to_mapping_rows(schema_rows)
    edited = st.data_editor(
        pd.DataFrame(mapping_rows),
        hide_index=True,
        use_container_width=True,
        disabled=["source_name"],
        column_config={
            "include": st.column_config.CheckboxColumn("include"),
            "target_name": st.column_config.TextColumn("target_name", required=True),
        },
        key="mapping_editor",
    )
    edited_rows = edited.to_dict(orient="records")
    st.session_state["mapping_rows"] = edited_rows

    if st.button("Apply column mapping", type="primary", use_container_width=True):
        try:
            _validate_mapping_rows(edited_rows)
        except CsvSchemaError as exc:
            st.error(f"Column mapping error: {exc}")
            return None
        st.session_state["mapping_confirmed"] = True
        st.session_state["types_confirmed"] = False
        st.session_state.pop("load_params", None)
        st.session_state["type_rows"] = _type_rows_from_mapping(schema_rows, edited_rows)

    if not st.session_state.get("mapping_confirmed"):
        st.info("Apply column mapping to continue to type review.")
        return None

    return st.session_state["mapping_rows"]


def _render_type_editor() -> CsvSchema | None:
    rows = st.session_state.get("type_rows")
    if not rows:
        return None

    st.subheader(step_heading("types"))
    _render_type_review_help()
    edited = st.data_editor(
        pd.DataFrame(rows),
        hide_index=True,
        use_container_width=True,
        disabled=["source_name", "target_name", "inferred_type", "sample_values", "notes"],
        column_config={
            "final_type": st.column_config.SelectboxColumn(
                "final_type",
                options=CLICKHOUSE_TYPE_OPTIONS,
                required=True,
            ),
            "custom_type": st.column_config.TextColumn(
                "custom_type",
                help="Optional manual ClickHouse type. If filled, it overrides final_type.",
            ),
            "nullable": st.column_config.CheckboxColumn("nullable"),
        },
        key="type_editor",
    )
    edited_rows = edited.to_dict(orient="records")
    st.session_state["type_rows"] = edited_rows
    try:
        schema = mappings_to_schema(mappings_from_editor_rows(edited_rows))
    except CsvSchemaError as exc:
        st.error(f"Type editor error: {exc}")
        return None

    if st.button("Apply types", type="primary", use_container_width=True):
        st.session_state["types_confirmed"] = True
        st.session_state.pop("load_params", None)

    if not st.session_state.get("types_confirmed"):
        st.info("Apply types to continue to ClickHouse settings.")
        return None

    return schema


def _schema_rows_to_mapping_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "source_name": row["source_name"],
            "target_name": row["target_name"],
            "include": row.get("include", True),
        }
        for row in rows
    ]


def _type_rows_from_mapping(
    schema_rows: list[dict[str, object]],
    mapping_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    schema_by_source = {str(row["source_name"]): row for row in schema_rows}
    type_rows = []
    for mapping in mapping_rows:
        if not bool(mapping.get("include", True)):
            continue
        source_name = str(mapping["source_name"])
        source_row = schema_by_source[source_name]
        type_rows.append(
            {
                "source_name": source_name,
                "target_name": str(mapping["target_name"]),
                "include": True,
                "inferred_type": source_row["inferred_type"],
                "final_type": source_row["final_type"],
                "custom_type": "",
                "nullable": source_row["nullable"],
                "sample_values": source_row["sample_values"],
                "notes": source_row["notes"],
            }
        )
    return type_rows


def _validate_mapping_rows(rows: list[dict[str, object]]) -> None:
    target_names = [
        str(row.get("target_name", "")).strip()
        for row in rows
        if bool(row.get("include", True))
    ]
    if not target_names:
        raise CsvSchemaError("At least one column must be included")
    empty_names = [name for name in target_names if not name]
    if empty_names:
        raise CsvSchemaError("Target column name cannot be empty")
    duplicates = sorted({name for name in target_names if target_names.count(name) > 1})
    if duplicates:
        raise CsvSchemaError("Duplicate target column names: " + ", ".join(duplicates))


def _render_load_log(log_container, log_messages: list[str]) -> None:
    tail = log_messages[-LOAD_LOG_TAIL_LINES:]
    hidden = len(log_messages) - len(tail)
    text = "\n".join(tail)
    if hidden:
        text = f"... {hidden} earlier lines omitted ...\n{text}"
    log_container.code(text, language="text")


def load_progress_line(stats: LoadStats) -> tuple[float, str]:
    """Доля и подпись прогресса чтения файла.

    Доля считается по прочитанным БАЙТАМ: общее число строк большого CSV
    неизвестно, а размер известен всегда - прежний прогресс по строкам был
    мёртв для файлов крупнее 50 МБ. Подпись говорит явно, что это прочитано
    из файла, а не подтверждено сервером: чтение опережает вставку на блоки
    в полёте и упреждающий буфер pandas.
    """
    if stats.src_bytes <= 0:
        return 0.0, "Reading the source file..."
    fraction = min(1.0, stats.src_read_bytes / stats.src_bytes)
    read_mb = stats.src_read_bytes / 1024 / 1024
    total_mb = stats.src_bytes / 1024 / 1024
    return fraction, (
        f"Read {read_mb:.0f} of {total_mb:.0f} MB from the file ({fraction * 100:.0f}%); "
        "reading runs ahead of confirmed inserts."
    )


#: Подписи судьбы таблиц - человеческим языком то, что запись прогона держит
#: в tables.fate. Оператор видит их и на экране, и (значением fate) в JSON.
TABLES_FATE_CAPTIONS = {
    TABLES_CREATED: "Both target tables are on the cluster with the loaded rows.",
    TABLES_NOT_CREATED: "No tables were created by this run.",
    TABLES_KEPT_WITH_DATA: (
        "The target tables are KEPT with the rows that already landed; "
        "check the log and drop them yourself before reloading."
    ),
    TABLES_DROPPED_AS_EMPTY: "The target tables were dropped as empty.",
    TABLES_CLEANUP_FAILED: (
        "Dropping the target tables FAILED; they are probably still there - see the log."
    ),
}


def tables_fate_caption(fate: str) -> str:
    """Подпись судьбы таблиц. Незнакомый fate показывается как есть, не молчит."""
    return TABLES_FATE_CAPTIONS.get(fate, f"Tables fate: {fate}")


def _start_load(
    *,
    config: ClickHouseConfig,
    csv_path: str,
    read_options: ReadOptions,
    schema: CsvSchema,
    distributed_table: str,
    order_by: str,
    partition_by: str | None,
    sharding_key: str,
    max_insert_payload_mb: int,
    load_workers: int,
    insert_compression: str,
    strict_preflight: bool,
) -> None:
    """Собирает и запускает фоновую задачу загрузки.

    Всё, что трогает session_state, происходит здесь, на потоке скрипта и ДО
    старта: у фонового потока нет ScriptRunContext, и st.* оттуда не работает.
    Сама загрузка - в csv_click.load_job, без единого обращения к Streamlit.
    """
    active = current_load_job()
    if active is not None and active.is_running:
        st.warning("A load is already running. Cancel it or wait for it to finish.")
        return
    try:
        mappings = mappings_from_editor_rows(st.session_state["type_rows"])
        effective_read_options, _, encoding_warning = choose_read_options_for_preview(
            csv_path,
            read_options,
            nrows=20,
        )
    except CsvSchemaError as exc:
        st.error(f"CSV schema error: {exc}")
        return
    except OSError as exc:
        # Файл исчез или недоступен между «Read CSV» и кликом загрузки: путь
        # проверяется на существование только на первом шаге. Раньше это
        # роняло прогон сырым трейсбеком (регрессия против main).
        st.error(f"CSV file error: {exc}")
        return
    st.session_state["csv_read_options"] = effective_read_options
    job = LoadJob(
        config=config,
        csv_path=csv_path,
        read_options=effective_read_options,
        schema=schema,
        mappings=mappings,
        distributed_table=distributed_table,
        order_by=order_by,
        partition_by=partition_by,
        sharding_key=sharding_key,
        max_insert_payload_mb=max_insert_payload_mb,
        load_workers=load_workers,
        insert_compression=insert_compression,
        strict_preflight=strict_preflight,
        schema_inference_mode=st.session_state.get(
            "schema_analysis_mode",
            SCHEMA_INFERENCE_SAMPLE,
        ),
        encoding_warning=encoding_warning.message if encoding_warning else None,
    )
    if not start_load_job(job):
        # Реестр перепроверяет под локом: две сессии могли нажать одновременно.
        st.warning("A load is already running. Cancel it or wait for it to finish.")


def _render_load_panel() -> None:
    """Живой прогресс или итог последней загрузки этого процесса.

    Рисуется из состояния задачи на КАЖДОМ прогоне, а не из одноразовых
    st.empty(): именно так итог переживает любые перерисовки - раньше поля
    0b/8a после rerun были видны только в JSON-записи.
    """
    job = current_load_job()
    if job is None:
        return
    if job.is_running:
        _render_running_load(job)
    else:
        _render_finished_load(job)


def _render_running_load(job: LoadJob) -> None:
    """Опрос задачи раз в секунду фрагментом.

    Интерфейс читает задачу, а не загрузка дёргает интерфейс: RerunException
    из st.* больше не достаёт до продюсера, и случайный клик не убивает
    часовую заливку. Клик по кнопке внутри фрагмента перерисовывает только
    фрагмент.
    """

    @st.fragment(run_every=LOAD_PROGRESS_POLL_S)
    def _load_progress_fragment() -> None:
        current = current_load_job() or job
        if not current.is_running:
            # Задача закончилась между тиками: один полный прогон, и итог
            # рисуется обычным путём, уже вне фрагмента.
            st.rerun(scope="app")
            return
        st.info(current.phase)
        fraction, caption = load_progress_line(current.stats)
        st.progress(fraction)
        st.caption(caption)
        st.metric("Inserted rows", current.stats.rows)
        if st.button(
            "Cancel load",
            key="cancel_load_button",
            type="secondary",
            disabled=current.cancel_requested,
        ):
            current.request_cancel()
        if current.cancel_requested:
            st.warning(
                "Cancelling: blocks already in flight will finish and be counted, "
                "nothing new goes out."
            )
        _render_load_log(st.empty(), current.log_lines())

    _load_progress_fragment()


def _render_finished_load(job: LoadJob) -> None:
    """Итог завершённой задачи; переживает перерисовки, пока не начата новая."""
    if job.outcome == "ok":
        st.success(f"Load finished: {job.stats.rows} rows in {job.stats.total_s:.2f} sec")
        if job.error_message:
            # Загрузка прошла, но отчёт о ней сломался - это надо видеть.
            st.warning(job.error_message)
    elif job.outcome == "cancelled":
        st.warning(
            f"Load cancelled: {job.stats.rows} rows in {job.stats.blocks} block(s) "
            "had already landed before the stop."
        )
    else:
        st.error(job.error_message or "The load failed before it could explain itself.")
    st.caption(tables_fate_caption(job.tables_fate))
    if job.record_path is not None:
        st.caption(f"Run record: `{job.record_path}`")
    with st.expander("Load log", expanded=job.outcome != "ok"):
        # Тот же хвост, что и у живого прогресса: полный лог (строка на каждый
        # блок) уезжал в сокет на каждом прогоне каждой сессии — ровно та
        # O(n²), против которой введён LOAD_LOG_TAIL_LINES. Важное — ошибки,
        # судьба таблиц, разбивка по часам — стоит в конце лога и в хвост
        # попадает всегда.
        _render_load_log(st.empty(), job.log_lines())


if __name__ == "__main__":
    main()
