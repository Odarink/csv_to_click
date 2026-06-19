# CSV to ClickHouse

Streamlit-приложение для загрузки CSV-файлов с сервера в новые таблицы
ClickHouse: локальную `ReplicatedMergeTree` и распределенную `Distributed`.

Загрузка выполняется через pandas chunks и `clickhouse-connect.raw_insert()` в
формате `JSONEachRow`. Этот путь выбран как основной, потому что в HTTP/proxy
окружениях `client.insert()` и `insert_df()` могут падать, а `JSONEachRow`
обычно проходит стабильно.

## Требования

- Python 3.11+
- `uv`
- доступ к ClickHouse
- CSV-файл должен лежать на той машине, где запущен Streamlit
- CSV должен содержать строку заголовков
- для secure-подключения нужны клиентские сертификат и ключ

Зависимости описаны в `pyproject.toml` и фиксируются через `uv.lock`.

## Быстрый запуск

Если зависимости уже установлены в активном venv, например `night_3_11`, и
доступа к PyPI нет или он нестабилен, не запускайте `uv sync`. Запускайте
приложение напрямую из активного окружения:

```bash
cd ~/strmlt
python -c "import streamlit, pandas, clickhouse_connect; print('dependencies OK')"
export PYTHONPATH="$PWD/src"
python -m streamlit run src/csv_click/app.py --server.address 0.0.0.0 --server.port 8501
```

Альтернатива через `uv`, если нужно использовать именно активный venv и не
синхронизировать `.venv` проекта:

```bash
cd ~/strmlt
export PYTHONPATH="$PWD/src"
uv run --active --no-sync streamlit run src/csv_click/app.py --server.address 0.0.0.0 --server.port 8501
```

`--active` говорит `uv` использовать текущий активированный venv, а `--no-sync`
отключает автоматическую синхронизацию зависимостей перед запуском.

Если в Jupyter terminal команда `uv` не найдена:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv --version
```

Если `curl` недоступен, используйте `wget`:

```bash
wget -qO- https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
uv --version
```

После установки можно продолжать запуск из директории репозитория.

Установить зависимости:

```bash
uv sync
```

При необходимости задать пользователя и пароль ClickHouse:

```bash
export CLICKHOUSE_USER="<user>"
export CLICKHOUSE_PASSWORD="<password>"
```

Запустить приложение:

```bash
uv run streamlit run src/csv_click/app.py --server.address 0.0.0.0 --server.port 8501
```

Если приложение запускается в JupyterHub, открыть его через proxy:

```text
https://<jupyterhub-host>/user/<username>/proxy/8501/
```

При локальном запуске открыть:

```text
http://localhost:8501
```

## Параметры подключения

Поля по умолчанию:

- `Host`: `tp17.wb-bank.ru`
- `Port`: `443`
- `Database`: `sandbox`
- `Cluster`: `clickhouse`
- `Secure`: включено
- `Verify TLS`: выключено
- `Client cert path`: `/home/jovyan/tsh/clickhouse-prod.crt`
- `Client key path`: `/home/jovyan/tsh/clickhouse-prod.key`

`Username` и `Password` можно ввести в UI или передать через переменные
окружения `CLICKHOUSE_USER` и `CLICKHOUSE_PASSWORD`.

Если `Secure` включен, приложение до подключения проверит наличие файлов
сертификата и ключа.

## Как пользоваться

1. В поле `CSV path` указать путь к CSV-файлу на сервере.
2. Указать `Database`, `Distributed table name`, `Cluster`.
3. Заполнить `ORDER BY`. Это обязательный параметр для локальной
   `ReplicatedMergeTree`.
4. При необходимости заполнить `PARTITION BY (optional)`.
5. При необходимости изменить `Distributed sharding key`. По умолчанию
   используется `rand()`.
6. Выбрать `Batch size`. По умолчанию загружается по `1_000_000` строк в чанке.
7. Выбрать разделитель CSV: `,`, `;`, `\t`, `|` или `custom`.
8. Выбрать кодировку: `utf_8`, `cp1251`, `windows-1251`, `utf-8-sig` или
   `custom`.
9. Оставить `Strict preflight validation` включенным, если нужно заранее
   проверить конвертацию CSV в выбранные типы ClickHouse до создания и загрузки.
10. Нажать `Apply parameters`.
11. Нажать `Test connection`, чтобы проверить подключение к ClickHouse через
    `SELECT 1`.
12. Нажать `Analyze CSV`. Приложение просканирует CSV чанками, выведет схему и
    примеры значений.
13. В таблице `Schema` проверить и отредактировать:
    - `target_name`: имя колонки в ClickHouse;
    - `include`: загружать колонку или исключить;
    - `final_type`: итоговый тип ClickHouse;
    - `nullable`: обернуть тип в `Nullable(...)`.
14. Нажать `Preview DDL`, чтобы посмотреть SQL создания локальной и
    распределенной таблиц.
15. Нажать `Create tables and load`, чтобы создать таблицы и загрузить данные.

## Что создает приложение

Если указать распределенную таблицу `my_table`, приложение создаст:

- локальную таблицу `my_table_local`;
- распределенную таблицу `my_table`.

Локальная таблица создается как:

```sql
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/<database>/<table>', '{replica}')
ORDER BY <ORDER BY>
```

Если заполнен `PARTITION BY`, он будет добавлен в DDL локальной таблицы.

Распределенная таблица создается как:

```sql
ENGINE = Distributed('<cluster>', '<database>', '<local_table>', <sharding_key>)
```

Перед созданием приложение проверяет через `clusterAllReplicas(system.tables)`,
что ни локальная, ни распределенная целевая таблица еще не существуют. Если
хотя бы одна из них уже есть, создание и загрузка блокируются.

## Правила обработки CSV

- Заголовок CSV обязателен.
- Имена колонок нормализуются под ClickHouse identifier:
  - приводятся к нижнему регистру;
  - неподходящие символы заменяются на `_`;
  - повторяющиеся `_` схлопываются;
  - если имя начинается с цифры, добавляется префикс `col_`.
- Если после нормализации появляются дубли колонок, анализ CSV завершается
  ошибкой.
- Пустые значения приводят к выбору `Nullable(...)` при автоопределении схемы.
- Поддерживаемые итоговые типы:
  - `String`, `Int64`, `UInt64`, `Float64`;
  - `Decimal(18, 2)`, `Decimal(38, 10)`;
  - `Date`, `DateTime`, `Bool`;
  - `Nullable(...)` для этих типов.

## Проверка перед загрузкой

При включенном `Strict preflight validation` приложение до создания таблиц и
загрузки:

- читает CSV теми же чанками;
- применяет выбранные маппинги колонок;
- проверяет конвертацию значений в выбранные типы ClickHouse;
- формирует `JSONEachRow` payload для проверки сериализации.

Это увеличивает время перед загрузкой, но снижает риск создать таблицу и затем
упасть на несовместимом значении в середине файла.

## Проверки разработки

Запустить тесты:

```bash
uv run --extra dev pytest
```
