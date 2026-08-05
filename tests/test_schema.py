from pathlib import Path

import pandas as pd
import pytest

from csv_click.schema import (
    CLICKHOUSE_TYPE_OPTIONS,
    CsvSchemaError,
    analyze_csv_schema,
    normalize_identifier,
)


def write_csv(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_analyze_csv_schema_requires_header(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "empty.csv", "")

    with pytest.raises(CsvSchemaError, match="header"):
        analyze_csv_schema(csv_path)


def test_analyze_csv_schema_infers_types_from_full_file(tmp_path: Path) -> None:
    csv_path = write_csv(
        tmp_path / "sample.csv",
        "\n".join(
            [
                "id,amount,created_dt,flag,comment",
                "1,10.50,2026-06-18,true,ok",
                "2,0.01,2026-06-19,false,",
                "3,999999999999.12,2026-06-20,true,late value",
            ]
        ),
    )

    result = analyze_csv_schema(csv_path)

    assert [column.column_name for column in result.columns] == [
        "id",
        "amount",
        "created_dt",
        "flag",
        "comment",
    ]
    assert [column.final_type for column in result.columns] == [
        "UInt64",
        "Decimal(18, 2)",
        "Date",
        "Bool",
        "Nullable(String)",
    ]


def test_mixed_column_falls_back_to_string_with_note(tmp_path: Path) -> None:
    csv_path = write_csv(
        tmp_path / "mixed.csv",
        "id,value\n1,10\n2,abc\n",
    )

    result = analyze_csv_schema(csv_path)

    value_column = result.columns[1]
    assert value_column.final_type == "String"
    assert "fallback" in value_column.notes.lower()


def test_unwrap_nullable_sees_through_lowcardinality() -> None:
    """Nullable у категориальной колонки живёт ВНУТРИ LowCardinality:
    unwrap обязан видеть его сквозь обёртку, иначе конвертер считает колонку
    не-nullable и роняет загрузку на первом же пропуске."""
    from csv_click.schema import unwrap_nullable

    assert unwrap_nullable("LowCardinality(Nullable(String))") == (
        True,
        "LowCardinality(String)",
    )
    assert unwrap_nullable("LowCardinality(String)") == (False, "LowCardinality(String)")
    # Прежний контракт не тронут.
    assert unwrap_nullable("Nullable(Int64)") == (True, "Int64")
    assert unwrap_nullable("String") == (False, "String")


def test_numeric_zero_one_column_infers_uint_not_bool(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "numeric_flags.csv", "id,flag\n1,0\n2,1\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[1].final_type == "UInt64"


@pytest.mark.parametrize(
    ("values", "want_type"),
    [
        # Разбор съедает ведущий ноль и ведущий плюс, а текст обратно не вернуть.
        # В банковской выгрузке это счета, БИК, ИНН, КПП, телефоны и индексы.
        (["00123456789", "00987654321"], "String"),
        (["044525225", "045004641"], "String"),
        (["+79001234567", "+79001234568"], "String"),
        (["007", "42"], "String"),
        (["00.5", "1.25"], "String"),
        (["-007", "-42"], "String"),
        # Граница длины: двузначные коды с нулём впереди - месяцы, регионы,
        # коды операций. `01` уехало бы единицей.
        (["01", "07"], "String"),
        # Не-ASCII цифры разбор перепишет целиком, а не только префикс:
        # `١٢٣٤` уедет как `1234`.
        (["٠١٢٣", "١٢٣٤"], "String"),
        (["１２３", "４５６"], "String"),
        # Числа, из которых разбор ничего не выкусывает, обязаны остаться числами.
        (["0", "1", "2"], "UInt64"),
        (["-7", "42"], "Int64"),
        (["0.5", "1.25"], "Decimal(18, 2)"),
        (["1e5", "2e5"], "Decimal(18, 2)"),
        (["2024-01-05", "2024-02-06"], "Date"),
        # Год с ведущими нулями - сентинел `DateTime.MinValue` из выгрузок .NET.
        # Разбор даты ничего не теряет, поэтому числом колонка не становится;
        # но первый год нашей эры не вмещает ни `Date`, ни `Date32`, и текстом
        # он хотя бы доедет без подмены (см. `_date_type`).
        (["0001-01-01", "0999-12-31"], "String"),
        (["true", "false"], "Bool"),
    ],
)
def test_numeric_inference_refuses_types_that_would_eat_a_leading_zero_or_plus(
    tmp_path: Path, values: list[str], want_type: str
) -> None:
    csv_path = write_csv(tmp_path / "column.csv", "code\n" + "".join(f"{value}\n" for value in values))

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == want_type


@pytest.mark.parametrize(
    ("value", "want_type"),
    [
        # Эти написания стоят в `NA_MARKERS` загрузчика, то есть путь загрузки
        # читает их пропуском: колонка обязана быть Nullable, иначе загрузка
        # падает на первом блоке.
        ("nan", "Nullable(Decimal(18, 2))"),
        ("NaN", "Nullable(Decimal(18, 2))"),
        ("-nan", "Nullable(Decimal(18, 2))"),
        ("-NaN", "Nullable(Decimal(18, 2))"),
        # А эти `float` читает, но в списке маркеров их НЕТ. Пропуском загрузчик
        # их не считает, а `to_json` напечатал бы `null` - деньги молча пропали
        # бы. Такая колонка обязана остаться текстом.
        ("NAN", "String"),
        ("Nan", "String"),
        ("nAn", "String"),
        ("+nan", "String"),
        # Бесконечность - не пропуск и не число, которое можно отправить:
        # загрузчик отвергает её сам и советует String.
        ("inf", "String"),
        ("-inf", "String"),
        ("Infinity", "String"),
        # Этот литерал читает только `Decimal`, и в ClickHouse его не отправить.
        ("sNaN", "String"),
    ],
)
def test_float_literals_get_a_type_the_load_path_accepts(
    tmp_path: Path, value: str, want_type: str
) -> None:
    """`Decimal()` принимает эти литералы, а показатель степени у них не число.

    Оператору такой файл возвращал `TypeError` про str и int из внутренностей
    инференса, то есть анализ не проходил вообще. `nan` в CSV - обычное дело:
    так пропуски пишет сам pandas. Тип обязан не только выбраться, но и пройти
    строгую проверку - иначе анализ отработал, а загрузка падает.
    """
    from csv_click.pandas_loader import (
        ReadOptions,
        schema_to_mappings,
        validate_csv_with_pandas_chunks,
    )

    csv_path = write_csv(tmp_path / "floats.csv", f"amount\n1.5\n{value}\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == want_type
    validate_csv_with_pandas_chunks(
        csv_path, ReadOptions(batch_size=10), schema_to_mappings(schema)
    )


@pytest.mark.parametrize(
    ("value", "want_type"),
    [
        # Обычные деньги: тип не меняется.
        ("1500.50", "Decimal(18, 2)"),
        # Ровно 18 значащих цифр - предел Decimal(18, 2).
        ("1234567890123456.78", "Decimal(18, 2)"),
        # 19 значащих: в Decimal(18, 2) уже не влезает.
        ("12345678901234567.89", "Decimal(38, 2)"),
        ("12345678901234567890.12", "Decimal(38, 2)"),
        # 42 значащих: нужен Decimal256.
        ("1" * 40 + ".25", "Decimal(76, 2)"),
        # Больше 76 значащих не вмещает ни один Decimal.
        ("1" * 80 + ".25", "String"),
        # Тот же счёт для дробных: 28 + 10 влезает в 38, 30 + 10 уже нет.
        ("1234567890123456789012345678.1234567890", "Decimal(38, 10)"),
        ("123456789012345678901234567890.1234567890", "Decimal(76, 10)"),
        # Ширина не СУЖАЕТСЯ: в режиме `Fast sample` за выборкой останутся
        # значения крупнее, и запас, который был до правки, обязан сохраниться.
        ("0.1234567890", "Decimal(38, 10)"),
        ("0.25", "Decimal(18, 2)"),
        # Ноль в экспоненциальной записи - это ноль, а не 31 цифра. `adjusted()`
        # у нулевой мантиссы возвращает показатель степени, и одна такая ячейка
        # уводила денежную колонку в String.
        ("0E+30", "Decimal(18, 2)"),
        ("-0E+20", "Decimal(18, 2)"),
        ("0E+79", "Decimal(18, 2)"),
    ],
)
def test_decimal_precision_is_wide_enough_for_the_value(
    tmp_path: Path, value: str, want_type: str
) -> None:
    """Точность обязана вмещать цифры, а не только знаки после запятой.

    Инференс считал один scale, поэтому значению с 20 целыми цифрами доставался
    `Decimal(18, 2)`, где целых всего 16.
    """
    csv_path = write_csv(tmp_path / "amounts.csv", f"amount\n{value}\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == want_type


@pytest.mark.parametrize(
    "value",
    [
        "1500.50",
        "12345678901234567.89",
        "1" * 40 + ".25",
        "0.1234567890",
        "1234567890123456789012345678.1234567890",
        "123456789012345678901234567890.1234567890",
    ],
)
def test_inferred_type_is_offered_by_the_type_editor(tmp_path: Path, value: str) -> None:
    """Тип, который выбрал инференс, обязан быть в списке редактора.

    Редактор рисует `final_type` как `SelectboxColumn` с фиксированными
    опциями. Тип вне списка сервер сохраняет, но оператор, тронув ячейку, не
    сможет вернуть его - выбирать будет не из чего.
    """
    csv_path = write_csv(tmp_path / "amounts.csv", f"amount\n{value}\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type in CLICKHOUSE_TYPE_OPTIONS


def test_nullable_variant_is_offered_for_every_plain_option() -> None:
    plain = [option for option in CLICKHOUSE_TYPE_OPTIONS if not option.startswith("Nullable(")]

    missing = [name for name in plain if f"Nullable({name})" not in CLICKHOUSE_TYPE_OPTIONS]

    assert not missing


def test_value_too_wide_for_any_decimal_says_why_in_notes(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "huge.csv", "amount\n" + "1" * 80 + ".25\n")

    schema = analyze_csv_schema(csv_path)

    notes = schema.columns[0].notes.lower()
    assert "digits" in notes, notes


@pytest.mark.parametrize("value", ["NAN", "Nan", "nAn", "+nan", "inf", "Infinity"])
def test_unlisted_nan_and_infinity_reach_the_wire_as_text(tmp_path: Path, value: str) -> None:
    """Написание, которого нет в маркерах пропуска, обязано доехать значением.

    Иначе `to_json` печатает `null`: строгая проверка проходит, прогон
    отчитывается успехом, а сумма исчезает. Это худший исход из возможных, и
    именно он получался, пока инференс считал такие ячейки пропуском.
    """
    from csv_click.pandas_loader import (
        ReadOptions,
        chunk_to_json_lines,
        convert_chunk_to_schema,
        schema_to_mappings,
        validate_csv_with_pandas_chunks,
    )

    csv_path = write_csv(tmp_path / "amounts.csv", f"amount\n1500.50\n{value}\n")
    schema = analyze_csv_schema(csv_path)
    mappings = schema_to_mappings(schema)

    validate_csv_with_pandas_chunks(csv_path, ReadOptions(batch_size=10), mappings)

    chunk = pd.DataFrame({"amount": ["1500.50", value]}, dtype="object")
    payload = chunk_to_json_lines(convert_chunk_to_schema(chunk, mappings, 1), ["amount"]).decode()
    assert "null" not in payload, payload
    assert value in payload, payload


def test_column_of_only_missing_markers_does_not_claim_to_be_empty(tmp_path: Path) -> None:
    """`nan` - маркер пропуска, но для String-колонки это текст, и он доедет.

    Пометка про пустую колонку рядом с образцом `nan` посылает оператора искать
    не ту причину, а Nullable у колонки без пропусков - неправда о данных.
    """
    csv_path = write_csv(tmp_path / "markers.csv", "note\nnan\nnan\n")

    schema = analyze_csv_schema(csv_path)
    column = schema.columns[0]

    assert column.final_type == "String"
    assert "empty" not in column.notes.lower(), column.notes


def test_missing_marker_beside_text_keeps_the_column_not_nullable(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "mixed.csv", "note\nnan\nAcme\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "String"


@pytest.mark.parametrize(
    ("value", "want_type"),
    [
        # ClickHouse `Date` начинается с 1970-01-01 и кончается 2149-06-06.
        ("1970-01-01", "Date"),
        ("2149-06-06", "Date"),
        # За его границами вмещает `Date32`: 1900-01-01 .. 2299-12-31. Это годы
        # рождения и сентинелы вроде 1900-01-01 из выгрузок .NET.
        ("1900-01-01", "Date32"),
        ("1950-06-15", "Date32"),
        ("1969-12-31", "Date32"),
        ("2149-06-07", "Date32"),
        ("2299-12-31", "Date32"),
        # Дальше не вмещает ни один тип даты.
        ("2300-01-01", "String"),
        ("1899-12-31", "String"),
    ],
)
def test_date_type_holds_the_dates_in_the_column(
    tmp_path: Path, value: str, want_type: str
) -> None:
    """Дата вне диапазона типа - тихая порча: локально всё проходит.

    Ни инференс, ни строгая проверка не смотрели на границы, поэтому 1950 год
    уезжал в `Date`, который начинается с 1970-го.
    """
    from csv_click.pandas_loader import (
        ReadOptions,
        schema_to_mappings,
        validate_csv_with_pandas_chunks,
    )

    csv_path = write_csv(tmp_path / "dates.csv", f"dt\n2024-01-05\n{value}\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == want_type
    validate_csv_with_pandas_chunks(
        csv_path, ReadOptions(batch_size=10), schema_to_mappings(schema)
    )


@pytest.mark.parametrize(
    ("value", "want_type"),
    [
        # Обычные числа тип не меняют.
        ("42", "UInt64"),
        ("-7", "Int64"),
        # Ровно границы 64 бит.
        ("18446744073709551615", "UInt64"),
        ("-9223372036854775808", "Int64"),
        # На единицу дальше - и уже не влезает.
        ("18446744073709551616", "Decimal(38, 0)"),
        ("-9223372036854775809", "Decimal(38, 0)"),
        ("9" * 20, "Decimal(38, 0)"),
        ("9" * 38, "Decimal(38, 0)"),
        # Шире 38 цифр держит только Decimal256.
        ("9" * 39, "Decimal(76, 0)"),
        ("-" + "9" * 76, "Decimal(76, 0)"),
        # Ровно на границах. `Decimal(38, 0)` держит |x| < 10**38, поэтому само
        # 10**38 в него уже не влезает - ни с плюсом, ни с минусом.
        ("1" + "0" * 38, "Decimal(76, 0)"),
        ("-1" + "0" * 38, "Decimal(76, 0)"),
        ("1" + "0" * 76, "String"),
        ("-1" + "0" * 76, "String"),
        # Дальше не вмещает ни один числовой тип.
        ("9" * 77, "String"),
    ],
)
def test_integer_type_holds_the_value(tmp_path: Path, value: str, want_type: str) -> None:
    """Целое вне 64 бит роняло загрузку голым `OverflowError`.

    Ни имени колонки, ни значения в сообщении: это не `CsvSchemaError`, а ошибка
    из недр pandas. Причём строгая проверка по csv-пути такое значение
    пропускала - падал уже путь загрузки, посреди файла.
    """
    from csv_click.pandas_loader import (
        ReadOptions,
        schema_to_mappings,
        validate_csv_with_pandas_chunks,
    )

    csv_path = write_csv(tmp_path / "ints.csv", f"n\n1\n{value}\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == want_type
    validate_csv_with_pandas_chunks(
        csv_path, ReadOptions(batch_size=10), schema_to_mappings(schema)
    )


def test_type_choice_does_not_depend_on_the_decimal_context(tmp_path: Path) -> None:
    """Границы типов не должны зависеть от точности контекста `decimal`.

    Считанные как `Decimal(10) ** 38 - 1`, они молча округлялись до 10**38:
    контексту по умолчанию хватает 28 значащих цифр, а результату нужно 38.
    Колонка с 39-значным числом получала `Decimal(38, 0)`, который держит 38.
    Контекст - глобальная настройка процесса, и менять её может кто угодно.
    """
    import decimal

    csv_path = write_csv(tmp_path / "boundary.csv", "n\n1\n" + "1" + "0" * 38 + "\n")

    with decimal.localcontext() as ctx:
        ctx.prec = 5
        schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "Decimal(76, 0)"


def test_integer_bounds_come_from_the_whole_column_not_the_last_row(tmp_path: Path) -> None:
    """Границы накапливаются: выходящее значение может стоять где угодно."""
    csv_path = write_csv(tmp_path / "unordered_ints.csv", "n\n" + "9" * 20 + "\n1\n2\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "Decimal(38, 0)"


def test_negative_bound_from_the_first_row_still_widens(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "negative_first.csv", "n\n-9223372036854775809\n1\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "Decimal(38, 0)"


def test_a_widened_integer_reaches_the_wire_exactly(tmp_path: Path) -> None:
    """Расширение бессмысленно, если значение всё равно портится по пути."""
    from csv_click.pandas_loader import (
        chunk_to_json_lines,
        convert_chunk_to_schema,
        schema_to_mappings,
    )

    huge = "9" * 30
    csv_path = write_csv(tmp_path / "huge_ints.csv", f"n\n1\n{huge}\n")
    schema = analyze_csv_schema(csv_path)

    chunk = pd.DataFrame({"n": ["1", huge]}, dtype="object")
    payload = chunk_to_json_lines(
        convert_chunk_to_schema(chunk, schema_to_mappings(schema), 1), ["n"]
    ).decode()

    assert huge in payload, payload


def test_widened_integer_column_says_why_in_notes(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "wide_int.csv", "n\n1\n" + "9" * 20 + "\n")

    schema = analyze_csv_schema(csv_path)

    notes = schema.columns[0].notes.lower()
    assert "uint64" in notes, notes


@pytest.mark.parametrize("date_type", ["Date", "Date32"])
def test_date_types_refuse_a_value_that_is_not_a_date(date_type: str) -> None:
    """Каждый тип из выпадающего списка обязан проверять значения.

    `Date32` появился в списке вместе с этой правкой, а ветки в конвертере не
    получил и молча принимал любой текст: строгая проверка на такой колонке не
    проверяла ничего.
    """
    from csv_click.schema import convert_value

    with pytest.raises(CsvSchemaError):
        convert_value("hello", date_type)
    with pytest.raises(CsvSchemaError):
        convert_value("hello", f"Nullable({date_type})")


def test_date32_column_refuses_a_form_clickhouse_cannot_read(tmp_path: Path) -> None:
    """Неделя по ISO разбирается Python-ом, но не ClickHouse.

    До появления `Date32` такая колонка была `Date`, и строгая проверка её
    отвергала. Тип не должен превращать громкий отказ в тихую отправку.
    """
    from csv_click.pandas_loader import (
        ReadOptions,
        schema_to_mappings,
        validate_csv_with_pandas_chunks,
    )

    csv_path = write_csv(tmp_path / "isoweek.csv", "dt\n1950-06-15\n1960-W02-3\n")
    schema = analyze_csv_schema(csv_path)

    with pytest.raises(CsvSchemaError):
        validate_csv_with_pandas_chunks(
            csv_path, ReadOptions(batch_size=10), schema_to_mappings(schema)
        )


def test_date_bounds_come_from_the_whole_column_not_the_last_row(tmp_path: Path) -> None:
    """Границы накапливаются: выходящая дата может стоять где угодно в файле."""
    csv_path = write_csv(
        tmp_path / "unordered.csv", "dt\n1950-06-15\n2024-01-05\n2024-02-06\n"
    )

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "Date32"


def test_date_beyond_date32_anywhere_in_the_column_forces_string(tmp_path: Path) -> None:
    csv_path = write_csv(
        tmp_path / "beyond.csv", "dt\n2024-01-05\n2300-01-01\n2024-02-06\n"
    )

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "String"


@pytest.mark.parametrize(
    "value",
    ["2024-01-05T10:00:00+03:00", "2024-01-05T10:00:00Z", "2024-01-05T10:00:00-05:00"],
)
def test_timezone_offset_does_not_infer_a_naive_datetime(tmp_path: Path, value: str) -> None:
    """Офсет пояса путь загрузки не разбирает: формат жёсткий, без зоны.

    Инференс всё равно выбирал `DateTime`, и загрузка падала на первом блоке -
    причём сообщение называло соседнюю, исправную строку.
    """
    from csv_click.pandas_loader import (
        ReadOptions,
        schema_to_mappings,
        validate_csv_with_pandas_chunks,
    )

    csv_path = write_csv(tmp_path / "stamps.csv", f"ts\n2024-01-05T09:00:00+03:00\n{value}\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "String"
    assert "zone" in schema.columns[0].notes.lower(), schema.columns[0].notes
    validate_csv_with_pandas_chunks(
        csv_path, ReadOptions(batch_size=10), schema_to_mappings(schema)
    )


def test_plain_datetime_still_infers_datetime(tmp_path: Path) -> None:
    from csv_click.pandas_loader import (
        ReadOptions,
        schema_to_mappings,
        validate_csv_with_pandas_chunks,
    )

    csv_path = write_csv(tmp_path / "plain.csv", "ts\n2024-01-05 09:00:00\n2024-02-06 10:30:00\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "DateTime"
    validate_csv_with_pandas_chunks(
        csv_path, ReadOptions(batch_size=10), schema_to_mappings(schema)
    )


def test_one_zero_cell_does_not_widen_the_column(tmp_path: Path) -> None:
    """Ноль рядом с обычными суммами не должен ни расширять тип, ни врать в пометке."""
    csv_path = write_csv(tmp_path / "amounts.csv", "amount\n1500.50\n0E+79\n99.99\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "Decimal(18, 2)"
    assert schema.columns[0].notes == ""


def test_leading_zero_column_says_in_notes_why_it_stayed_string(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "accounts.csv", "account\n00123456789\n00987654321\n")

    schema = analyze_csv_schema(csv_path)

    notes = schema.columns[0].notes
    assert "zero" in notes.lower(), notes


def test_mixed_column_with_a_leading_zero_still_blames_the_mix(tmp_path: Path) -> None:
    """Колонка не числовая вовсе, и объяснение обязано быть про смесь.

    Пометка про ведущие нули здесь послала бы оператора искать не ту причину.
    """
    csv_path = write_csv(tmp_path / "mixed_zero.csv", "code\n007\nabc\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "String"
    assert "mixed" in schema.columns[0].notes.lower(), schema.columns[0].notes


def test_leading_zero_column_with_empty_cells_becomes_nullable_string(tmp_path: Path) -> None:
    csv_path = write_csv(tmp_path / "accounts_nullable.csv", "account,name\n00123456789,a\n,b\n")

    schema = analyze_csv_schema(csv_path)

    assert schema.columns[0].final_type == "Nullable(String)"


def test_clickhouse_type_options_include_nullable_dropdown_values() -> None:
    assert "String" in CLICKHOUSE_TYPE_OPTIONS
    assert "Nullable(Decimal(38, 10))" in CLICKHOUSE_TYPE_OPTIONS


def test_normalize_identifier_rejects_duplicate_columns_after_normalization() -> None:
    assert normalize_identifier("Order ID") == "order_id"
    assert normalize_identifier("123") == "col_123"


def test_normalize_identifier_keeps_cyrillic_letters() -> None:
    """Заголовок целиком из кириллицы обязан давать имя, а не пустоту.

    ASCII-класс стирал такое имя до `_`, а `strip("_")` — до пустой строки, и
    выгрузка падала на «CSV header contains an empty column name». Кириллица —
    не край, а обычный случай выгрузок этого проекта.
    """
    assert normalize_identifier("ИНН") == "инн"
    assert normalize_identifier("Организационно-правовая форма") == "организационно_правовая_форма"
    assert normalize_identifier("Id Селлера") == "id_селлера"


def test_normalize_identifier_still_rejects_a_name_without_letters_or_digits() -> None:
    """Пустое имя всё ещё обязано ловиться: сообщение об ошибке достижимо."""
    with pytest.raises(CsvSchemaError, match="empty column name"):
        normalize_identifier(" --- ")


def test_analyze_csv_schema_reads_a_cyrillic_header(tmp_path: Path) -> None:
    csv_path = write_csv(
        tmp_path / "sellers.csv",
        "\r\n".join(
            [
                "Id Селлера;ИНН;Наименование селлера;Полное наименование селлера;"
                "Организационно-правовая форма",
                "35;10000;perfume shop;aaaa aaaa aaaa;Самозанятый",
                "48;10000;bbbb bbbb bbbb;bbbb bbbb bbbb;Самозанятый",
                "",
            ]
        ),
    )

    schema = analyze_csv_schema(csv_path)

    assert [column.column_name for column in schema.columns] == [
        "id_селлера",
        "инн",
        "наименование_селлера",
        "полное_наименование_селлера",
        "организационно_правовая_форма",
    ]
    assert schema.source_names[1] == "ИНН"
