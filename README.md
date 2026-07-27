# CSV to ClickHouse

Streamlit-приложение для загрузки CSV-файлов с сервера в новые таблицы
ClickHouse: локальную `ReplicatedMergeTree` и распределенную `Distributed`.

Загрузка выполняется через pandas chunks и `clickhouse-connect.raw_insert()` в
формате `JSONEachRow`. Этот путь выбран как основной, потому что в HTTP/proxy
окружениях `client.insert()` и `insert_df()` могут падать, а `JSONEachRow`
обычно проходит стабильно.

## Требования

- `uv` — единственное, что нужно установить руками; Python он скачает сам
- доступ к ClickHouse
- CSV-файл должен лежать на той машине, где запущен Streamlit
- CSV должен содержать строку заголовков
- для secure-подключения нужны клиентские сертификат и ключ

Окружение ставится из `uv.lock` командой `uv sync --locked --extra dev` — это
единственный поддерживаемый способ запуска. Версии библиотек зафиксированы
намеренно: иначе `pandas`, `clickhouse-connect` или `urllib3` могут смениться
между двумя загрузками, и сравнение их времени перестаёт что-либо значить.
Флаг `--locked` выбран вместо `--frozen` осознанно: `--frozen` использует
локфайл, вообще не сверяя его с `pyproject.toml`, а `--locked` откажется
запускаться на разъехавшемся локфайле — молчаливое перерешивание как раз и есть
то, что делает прогоны несравнимыми. Версия интерпретатора закреплена в
`.python-version`, зависимости — в `pyproject.toml` и `uv.lock`.

Группа `dev` ставится вместе с рантаймом сознательно: `uv sync` по умолчанию
приводит окружение в точное соответствие локфайлу, то есть без `--extra dev`
каждый запуск `loader.bat` удалял бы `pytest` из того же `.venv`, которым
проверяется код. Побочный эффект: `pip` из `.venv` удаляется — он больше не
нужен, пакеты ставит `uv`.

## Локальный запуск на Windows

Откройте папку проекта и запустите `loader.bat`.

При первом запуске файл сам:

- перейдет в директорию проекта;
- поставит окружение из `uv.lock` командой `uv sync --locked --extra dev`
  (Python версии из `.python-version` `uv` при необходимости скачает сам);
- проверит, что доступны `streamlit`, `pandas` и `clickhouse_connect`;
- запустит приложение на `http://localhost:8501`.

Если `uv` не установлен, `loader.bat` остановится и покажет, как его поставить.
Без прав администратора:

```cmd
py -3.12 -m pip install --user uv
```

С правами администратора:

```cmd
winget install --source winget -e --id astral-sh.uv
```

`--source winget` обязателен: без него winget сначала идёт в источник `msstore`
и на машине с корпоративным перехватом TLS падает с
`0x8a15005e : The server certificate did not match any of the expected values`.

После установки заново запустите `loader.bat`. Менять `PATH` вручную не нужно:
`pip install --user` кладёт `uv.exe` в каталог пользовательских скриптов Python,
которого на `PATH` обычно нет, и `loader.bat` ищет его там сам.

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

Проверьте, что `uv` доступен:

```cmd
uv --version
```

Поставьте окружение из локфайла:

```cmd
uv sync --locked --extra dev
```

Если `uv sync --locked` отказывается работать, потому что локфайл разошёлся с
`pyproject.toml`, пересоберите его — но помните, что это меняет версии библиотек
и обнуляет сравнимость с предыдущими прогонами:

```cmd
uv lock
```

Проверьте, что основные библиотеки доступны:

```cmd
.venv\Scripts\python.exe -c "import streamlit, pandas, clickhouse_connect; print('dependencies OK')"
```

Запустите приложение вручную:

```cmd
set PYTHONPATH=%CD%\src
.venv\Scripts\python.exe -m streamlit run src\csv_click\app.py --server.address 127.0.0.1 --server.port 8501
```

## Запуск на Linux и в JupyterHub

```bash
uv sync --locked --extra dev
uv run --locked --extra dev streamlit run src/csv_click/app.py --server.address 0.0.0.0 --server.port 8501
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
7. Выбрать `Max insert payload, MB`. Значение по умолчанию — `16`, но приложение
   намеренно режет на 10% ниже введённого (`INSERT_PAYLOAD_SAFETY_RATIO`), чтобы
   остаться под лимитом HTTP/прокси, так что фактическая граница одного
   `JSONEachRow` insert request — `14.4` MB. Действующее значение печатается в лог
   загрузки строкой `Load settings` и сохраняется в записи о прогоне.
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

Запустить тесты (только из корня репозитория — часть тестов читает файлы по
относительным путям):

```bash
uv run --locked --extra dev pytest
```

`--locked` здесь не формальность: без него `uv run` может перерешать окружение и
переписать `uv.lock`, то есть та самая команда, которой проверяют код, тихо
сдвинет версии, зафиксированные ради сравнимости прогонов.
