# CSV to ClickHouse

Streamlit-приложение для загрузки CSV-файлов с сервера в новые таблицы
ClickHouse: локальную `ReplicatedMergeTree` и распределенную `Distributed`.

Загрузка выполняется через pandas chunks и `clickhouse-connect.raw_insert()` в
формате `JSONEachRow`. Этот путь выбран как основной, потому что в HTTP/proxy
окружениях `client.insert()` и `insert_df()` могут падать, а `JSONEachRow`
обычно проходит стабильно.

## Требования

- Python 3.11+
- доступ к ClickHouse
- CSV-файл должен лежать на той машине, где запущен Streamlit
- CSV должен содержать строку заголовков
- для secure-подключения нужны клиентские сертификат и ключ

Для локального запуска на Windows runtime-зависимости устанавливаются из
`requirements.txt`. `pyproject.toml` и `uv.lock` сохранены для разработки и
альтернативного запуска через `uv`.

## Локальный запуск на Windows

Откройте папку проекта и запустите `loader.bat`.

При первом запуске файл сам:

- перейдет в директорию проекта;
- найдет установленный Python 3.11+;
- создаст `.venv`, если окружения еще нет;
- обновит `pip`;
- установит зависимости из `requirements.txt`;
- проверит, что доступны `streamlit`, `pandas` и `clickhouse_connect`;
- запустит приложение на `http://localhost:8501`.

Если Python 3.11+ не установлен, `loader.bat` остановится и покажет команду:

```cmd
winget install -e --id Python.Python.3.12
```

После установки Python заново откройте папку проекта и снова запустите
`loader.bat`.

При необходимости задайте пользователя и пароль ClickHouse перед запуском:

```cmd
set CLICKHOUSE_USER=<user>
set CLICKHOUSE_PASSWORD=<password>
loader.bat
```

Если используется secure-подключение с клиентскими сертификатами, укажите в UI
Windows-пути к сертификату и ключу, например:

```text
C:\Users\<username>\tsh\clickhouse-prod.crt
C:\Users\<username>\tsh\clickhouse-prod.key
```

## Ручной запуск и диагностика

Ручные команды нужны только для диагностики, если `loader.bat` завершился с
ошибкой.

Проверьте установленные версии Python:

```cmd
py -0p
```

Для проекта нужен Python 3.11 или новее. Создать окружение вручную можно через
установленную подходящую версию, например:

```cmd
py -3.12 -m venv .venv
```

Если в списке есть только Python 3.11, используйте:

```cmd
py -3.11 -m venv .venv
```

Активируйте окружение:

```cmd
.venv\Scripts\activate.bat
```

Проверьте версию Python внутри активированного окружения:

```cmd
python --version
```

Обновите `pip` и установите зависимости:

```cmd
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Проверьте, что основные библиотеки доступны:

```cmd
python -c "import streamlit, pandas, clickhouse_connect; print('dependencies OK')"
```

Запустите приложение вручную:

```cmd
set PYTHONPATH=%CD%\src
python -m streamlit run src\csv_click\app.py --server.address 127.0.0.1 --server.port 8501
```

## Альтернативный запуск через uv

`uv` не требуется для основного локального запуска на Windows. Его можно
использовать для разработки, если нужно синхронизировать зависимости из
`pyproject.toml` и `uv.lock`:

```bash
uv sync
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
6. Выбрать `Batch size`. По умолчанию загружается по `100_000` строк в CSV чанке.
7. Выбрать `Max insert payload, MB`. По умолчанию один HTTP `JSONEachRow` insert request ограничен `16` MB.
8. Выбрать разделитель CSV: `,`, `;`, `\t`, `|` или `custom`.
9. Выбрать кодировку: `utf_8`, `cp1251`, `windows-1251`, `utf-8-sig` или
   `custom`.
10. Оставить `Strict preflight validation` включенным, если нужно заранее
   проверить конвертацию CSV в выбранные типы ClickHouse до создания и загрузки.
11. Нажать `Apply parameters`.
12. Нажать `Test connection`, чтобы проверить подключение к ClickHouse через
    `SELECT 1`.
13. Нажать `Analyze CSV`. Приложение просканирует CSV чанками, выведет схему и
    примеры значений.
14. В таблице `Schema` проверить и отредактировать:
    - `target_name`: имя колонки в ClickHouse;
    - `include`: загружать колонку или исключить;
    - `final_type`: итоговый тип ClickHouse;
    - `nullable`: обернуть тип в `Nullable(...)`.
15. Нажать `Preview DDL`, чтобы посмотреть SQL создания локальной и
    распределенной таблиц.
16. Нажать `Create tables and load`, чтобы создать таблицы и загрузить данные.

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
- формирует bounded `JSONEachRow` payload для проверки сериализации и лимита размера insert request.

Это увеличивает время перед загрузкой, но снижает риск создать таблицу и затем
упасть на несовместимом значении в середине файла.

## Проверки разработки

Запустить тесты:

```bash
uv run --extra dev pytest
```
