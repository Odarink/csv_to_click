"""README проверяется тестами, а не вниманием: он уже разошёлся с приложением.

На 2026-08-03 он обещал кнопку `Analyze CSV`, которой нет, ставил
`Apply parameters` до чтения CSV и не упоминал `Apply column mapping` и
`Apply types` - два шага, которые пропустить нельзя. Идущий по README застревал.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

from csv_click.schema import CLICKHOUSE_TYPE_OPTIONS

#: Кнопки, названия которых README вправе не упоминать: они не шаги сценария.
_NOT_A_STEP = frozenset({"Stop read CSV / choose another file"})


def _readme() -> str:
    return Path("README.md").read_text(encoding="utf-8")


def _app_button_labels() -> set[str]:
    """Названия кнопок из `app.py`, взятые разбором, а не регуляркой.

    Регулярка по тексту пропускала вызовы с переносом строки, а их там больше
    половины: `st.button(\n    "Apply parameters",` выглядит иначе, чем
    `st.button("Preview DDL"`.
    """
    tree = ast.parse(Path("src/csv_click/app.py").read_text(encoding="utf-8"))
    labels: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in {"button", "form_submit_button"}:
            continue
        if node.args and isinstance(node.args[0], ast.Constant):
            value = node.args[0].value
            if isinstance(value, str):
                labels.add(value)
    return labels


def test_readme_names_only_buttons_that_exist() -> None:
    """Каждое `Название` в бэктиках, похожее на кнопку, обязано существовать."""
    labels = _app_button_labels()
    assert labels, "не нашлось ни одной кнопки - разбор app.py сломан"

    quoted = set(re.findall(r"`([A-Z][A-Za-z0-9 /]{2,40})`", _readme()))
    # Из упомянутого в бэктиках берём только то, что выглядит как действие:
    # заголовки таблиц и имена полей проверяются другими тестами.
    button_like = {name for name in quoted if name.split()[0] in {"Apply", "Read", "Analyze", "Create", "Preview", "Test", "Stop"}}

    unknown = button_like - labels
    assert not unknown, f"README обещает кнопки, которых нет в приложении: {sorted(unknown)}"


def test_readme_describes_the_steps_that_cannot_be_skipped() -> None:
    readme = _readme()

    for required in ("Apply column mapping", "Apply types", "Create tables and load"):
        assert f"`{required}`" in readme, required


def test_readme_lists_the_steps_in_the_order_the_app_enforces() -> None:
    """Порядок в приложении задан `main()`: CSV, маппинг, типы, потом параметры.

    README держал `Apply parameters` одиннадцатым шагом, до чтения файла, хотя
    формы параметров до подтверждения типов на экране нет вовсе.
    """
    readme = _readme()

    assert readme.index("`Apply column mapping`") < readme.index("`Apply types`")
    assert readme.index("`Apply types`") < readme.index("`Apply parameters`")
    assert readme.index("`Apply parameters`") < readme.index("`Create tables and load`")


def test_readme_type_list_matches_the_editor() -> None:
    """Список типов отставал на шесть значений, включая все широкие Decimal.

    Проверяется именно СПИСОК, а не весь текст: тип может упоминаться в
    объяснении рядом, и тогда пропажа его из перечня остаётся незамеченной -
    мутация ровно так и выжила.
    """
    readme = _readme()
    start = readme.index("Поддерживаемые итоговые типы")
    type_list = readme[start : readme.index("\n\n", start)]
    plain = [option for option in CLICKHOUSE_TYPE_OPTIONS if not option.startswith("Nullable(")]

    missing = [option for option in plain if f"`{option}`" not in type_list]
    assert not missing, f"список типов в README не знает про: {missing}"

    # И обратная сторона: README не вправе обещать тип, которого редактор не
    # предлагает. Односторонняя проверка пропустила `Decimal(38, 0)` и
    # `Decimal(76, 0)`, дописанные в расчёте на ещё не влитую ветку.
    promised = set(re.findall(r"`(Decimal\([^`]+\)|Date32|DateTime|Date|Bool|String|U?Int64|Float64)`", type_list))
    unknown = promised - set(CLICKHOUSE_TYPE_OPTIONS)
    assert not unknown, f"README обещает типы, которых нет в редакторе: {sorted(unknown)}"


def test_readme_does_not_promise_a_default_sharding_key() -> None:
    """`rand()` из кода удалён: ключ выбирается из колонок и обязателен."""
    assert "rand()" not in _readme()
