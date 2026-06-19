# Пример загрузки pandas DataFrame в ClickHouse через JSONEachRow

Кейс: загрузка небольшого справочника из `pandas.DataFrame` в ClickHouse через `clickhouse_connect`, когда стандартные методы `insert_df()` / `client.insert()` падают с ошибкой HTTP 500.

Итоговый рабочий подход: подготовить данные в pandas, привести `NaN / pd.NA / numpy`-типы к Python-native значениям и выполнить вставку через `client.raw_insert(..., fmt='JSONEachRow')`.

---

## 1. Подключение к ClickHouse

```python
import clickhouse_connect

client = clickhouse_connect.get_client(
    host='<CLICKHOUSE_HOST>',
    port=443,
    username='<USERNAME>',
    password='<PASSWORD>',        # если используется пароль
    secure=True,
    verify=False,                 # зависит от контура
    client_cert='<PATH_TO_CERT>', # если используется mTLS
    client_cert_key='<PATH_TO_KEY>'
)

print(client.query('SELECT 1').result_rows[0])
```

---

## 2. DDL локальной таблицы ReplicatedMergeTree

```sql
CREATE TABLE sandbox.adhoc_cft_r2_vid_oper_local
ON CLUSTER clickhouse
(
    `ID` Int64,
    `SN` Nullable(Int64),
    `SU` Nullable(Int64),
    `C_NAME` Nullable(String),
    `C_CODE` Nullable(String),
    `C_BUS_PROCESS` Nullable(Int64),
    `C_CODE_GROUP` Nullable(String),
    `C_DEPEND_PLAN_OPER` Nullable(Int64),
    `C_SPOSOB_KVIT_0` Nullable(Int64),
    `C_CORR_VID_OPER` Nullable(Int64),
    `C_IS_GRACE` Nullable(Int64)
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}-{uuid}/adhoc_cft_r2_vid_oper_local',
    '{replica}'
)
ORDER BY ID
SETTINGS index_granularity = 8192;
```

> Примечание: для production-таблиц чаще используют более стабильный путь вида  
> `'/clickhouse/tables/{shard}/sandbox/adhoc_cft_r2_vid_oper_local'`.  
> В ad-hoc сценарии текущий вариант с `{uuid}` может быть допустим, если принят на контуре.

---

## 3. DDL Distributed-таблицы

```sql
CREATE TABLE sandbox.adhoc_cft_r2_vid_oper
ON CLUSTER clickhouse
AS sandbox.adhoc_cft_r2_vid_oper_local
ENGINE = Distributed(
    'clickhouse',
    'sandbox',
    'adhoc_cft_r2_vid_oper_local',
    sipHash64(ID)
);
```

---

## 4. Чтение CSV в pandas

```python
import pandas as pd

SCHEMA = 'sandbox'
TABLE = 'adhoc_cft_r2_vid_oper'

# Пример: CSV из CFT/справочника в cp1251
# sep и encoding нужно настроить под свой файл.
df1 = pd.read_csv(
    'z#r2_vid_oper.csv',
    sep=';',
    encoding='cp1251'
)

# Убираем пробелы в названиях колонок.
df1.columns = df1.columns.str.strip()

df1.info()
```

---

## 5. Выбор и переименование колонок

В исходном DataFrame была колонка `C_SPOSOB_KVIT#0`. Для ClickHouse и Python-кода лучше переименовать ее в `C_SPOSOB_KVIT_0`.

```python
columns = [
    'ID',
    'SN',
    'SU',
    'C_NAME',
    'C_CODE',
    'C_BUS_PROCESS',
    'C_CODE_GROUP',
    'C_DEPEND_PLAN_OPER',
    'C_SPOSOB_KVIT_0',
    'C_CORR_VID_OPER',
    'C_IS_GRACE',
]

df_output = df1.copy()

df_output = df_output.rename(columns={
    'C_SPOSOB_KVIT#0': 'C_SPOSOB_KVIT_0'
})

df_output = df_output[columns]
```

---

## 6. Подготовка типов pandas под ClickHouse

Основная проблема: pandas хранит nullable-числа как `float64` из-за `NaN`. Для `Nullable(Int64)` в ClickHouse нужно получить Python `int` или `None`.

```python
import pandas as pd

int_cols = [
    'ID',
    'SN',
    'SU',
    'C_SPOSOB_KVIT_0',
]

nullable_int_cols = [
    'C_BUS_PROCESS',
    'C_DEPEND_PLAN_OPER',
    'C_CORR_VID_OPER',
    'C_IS_GRACE',
]

str_cols = [
    'C_NAME',
    'C_CODE',
    'C_CODE_GROUP',
]

# Обычные integer-колонки без NULL.
for col in int_cols:
    df_output[col] = pd.to_numeric(df_output[col], errors='raise').astype('int64')

# Nullable integer-колонки: строго object с int / None.
for col in nullable_int_cols:
    df_output[col] = pd.Series(
        [None if pd.isna(x) else int(x) for x in df_output[col]],
        dtype='object'
    )

# Nullable string-колонки: object со str / None.
for col in str_cols:
    df_output[col] = pd.Series(
        [None if pd.isna(x) else str(x) for x in df_output[col]],
        dtype='object'
    )
```

---

## 7. Проверка типов перед вставкой

```python
print(df_output.dtypes)

for col in nullable_int_cols:
    print(
        col,
        df_output[col].map(lambda x: type(x).__name__).value_counts(dropna=False)
    )
```

Ожидаемо для nullable integer колонок:

```text
int
NoneType
```

То есть не должно быть `float`, `nan`, `NAType`.

---

## 8. Проверка состояния replicated-таблицы

```python
res = client.query("""
SELECT
    hostName(),
    database,
    table,
    is_readonly,
    is_session_expired,
    future_parts,
    queue_size,
    absolute_delay,
    zookeeper_exception
FROM clusterAllReplicas('clickhouse', system.replicas)
WHERE database = 'sandbox'
  AND table = 'adhoc_cft_r2_vid_oper_local'
ORDER BY hostName()
""")

for row in res.result_rows:
    print(row)
```

Если `is_readonly = 1` или есть `zookeeper_exception`, вставка может падать не из-за pandas, а из-за репликации.

---

## 9. Быстрый тест ручной вставки

Перед загрузкой pandas полезно проверить, что таблица принимает обычный SQL `INSERT VALUES`.

```python
client.command("""
INSERT INTO sandbox.adhoc_cft_r2_vid_oper_local
(
    ID,
    SN,
    SU,
    C_NAME,
    C_CODE,
    C_BUS_PROCESS,
    C_CODE_GROUP,
    C_DEPEND_PLAN_OPER,
    C_SPOSOB_KVIT_0,
    C_CORR_VID_OPER,
    C_IS_GRACE
)
VALUES
(
    999999999,
    1,
    1,
    'test',
    'test',
    NULL,
    'test',
    NULL,
    1,
    NULL,
    NULL
)
""")
```

Если этот запрос успешен, значит DDL и права на insert рабочие.

---

## 10. Рабочая вставка через JSONEachRow

```python
import json
import numpy as np
import pandas as pd

def clean_value(x):
    if pd.isna(x):
        return None
    if isinstance(x, np.integer):
        return int(x)
    if isinstance(x, np.floating):
        if float(x).is_integer():
            return int(x)
        return float(x)
    return x

records = []

for row in df_output[columns].to_dict(orient='records'):
    clean_row = {
        col: clean_value(row[col])
        for col in columns
    }
    records.append(clean_row)

payload = '\n'.join(
    json.dumps(r, ensure_ascii=False, allow_nan=False)
    for r in records
).encode('utf-8')

client.raw_insert(
    table='sandbox.adhoc_cft_r2_vid_oper',
    column_names=columns,
    insert_block=payload,
    fmt='JSONEachRow'
)
```

---

## 11. Проверка результата

```python
res = client.query("""
SELECT
    count() AS cnt,
    min(ID) AS min_id,
    max(ID) AS max_id
FROM sandbox.adhoc_cft_r2_vid_oper
""")

print(res.result_rows)
```

Дополнительно можно проверить local-таблицу:

```python
res = client.query("""
SELECT
    count() AS cnt,
    min(ID) AS min_id,
    max(ID) AS max_id
FROM sandbox.adhoc_cft_r2_vid_oper_local
""")

print(res.result_rows)
```

---

## 12. Почему не `insert_df()` / `client.insert()`

В этом кейсе стандартные методы:

```python
client.insert_df(...)
client.insert(...)
```

падали с ошибкой:

```text
DatabaseError: HTTPDriver ... returned response code 500
```

При этом:

- ручной `INSERT VALUES` проходил успешно;
- таблица и права были рабочие;
- `system.replicas` возвращал ожидаемый результат;
- nullable-поля были подготовлены как `int / NoneType`;
- `client.insert(..., column_type_names=...)` тоже падал.

Вывод: проблема была не в DDL и не в pandas-типах, а в способе отправки данных через `clickhouse_connect` в binary/native insert поверх HTTP/proxy.

Для небольшого справочника на ~1–2 тысячи строк `JSONEachRow` оказался самым стабильным вариантом.

---

## 13. Мини-чеклист перед загрузкой

1. Пустые колонки из pandas удалены.
2. Колонки со спецсимволами переименованы, например `C_SPOSOB_KVIT#0` → `C_SPOSOB_KVIT_0`.
3. Порядок `columns` совпадает с DDL ClickHouse.
4. `Nullable(Int64)` в pandas имеет значения `int / NoneType`, а не `float / NaN`.
5. `json.dumps(..., allow_nan=False)` не падает.
6. Ручной `INSERT VALUES` проходит.
7. `system.replicas` не показывает `readonly` или `zookeeper_exception`.
8. Вставка выполняется через `raw_insert(..., fmt='JSONEachRow')`.

---

## 14. Короткий итог

Для ad-hoc загрузки pandas → ClickHouse через HTTP-контур:

```python
client.raw_insert(
    table='sandbox.adhoc_cft_r2_vid_oper',
    column_names=columns,
    insert_block=payload,
    fmt='JSONEachRow'
)
```

Это надежнее, чем `insert_df()` / `client.insert()`, если HTTP/proxy возвращает неинформативный `500`.
