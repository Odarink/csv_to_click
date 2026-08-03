"""Видно ли, где ты в процессе и сколько осталось.

Заголовки шагов существовали, но не были связаны: сколько их всего, узнать было
нельзя, а исчезнувший блок читался как «интерфейс пропал», а не «это впереди».
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from streamlit.testing.v1 import AppTest

from csv_click.app import LOAD_STEPS, step_heading, step_path_line


def test_every_step_knows_its_number_and_the_total() -> None:
    assert len(LOAD_STEPS) >= 4

    for index, step in enumerate(LOAD_STEPS, start=1):
        heading = step_heading(step.key)
        assert heading.startswith(f"Step {index} of {len(LOAD_STEPS)}")
        assert step.title in heading


def test_the_path_line_marks_done_current_and_upcoming() -> None:
    """Одна строка отвечает на вопрос «что дальше», которого не было видно."""
    line = step_path_line(current_key=LOAD_STEPS[1].key)

    for step in LOAD_STEPS:
        assert step.title in line
    done, current = LOAD_STEPS[0].title, LOAD_STEPS[1].title
    assert line.index(done) < line.index(current)
    # Пройденный помечен, текущий назван - иначе строка не отвечает ни на что.
    assert "✓" in line
    assert "вы здесь" in line


def test_the_path_line_survives_an_unknown_step() -> None:
    """Опечатка в ключе не должна ронять весь экран ради одной строки."""
    line = step_path_line(current_key="no-such-step")

    assert LOAD_STEPS[0].title in line
    assert "вы здесь" not in line


def _app() -> None:
    import streamlit as st

    from csv_click.app import main

    st.session_state.setdefault("app_settings_loaded", False)
    main()


def test_the_first_screen_shows_the_whole_path() -> None:
    at = AppTest.from_function(_app, default_timeout=30)
    at.run()

    # Строка пути рисуется `st.caption`, а он в AppTest лежит отдельно от
    # `markdown` - собираем оба, иначе тест проверяет не тот экран.
    body = "\n".join(
        str(block.value) for block in [*at.markdown, *at.caption, *at.subheader]
    )
    assert "Step 1 of" in body
    # Все шаги названы сразу: пользователь видит длину пути до первого клика.
    for step in LOAD_STEPS:
        assert step.title in body, step.title


def test_the_path_line_keeps_up_with_the_opened_step(tmp_path: Path) -> None:
    """Строка не должна отставать: состояние меняют сами блоки, ниже неё.

    Нарисованная до них, она после «Apply types» показывала третий шаг, пока на
    экране уже стоял четвёртый - то есть ровно то расхождение, от которого
    ориентир и должен спасать.
    """
    csv_path = tmp_path / "steps.csv"
    csv_path.write_text("id,amount\n1,10.50\n2,20.25\n", encoding="utf-8")

    at = AppTest.from_function(_app, default_timeout=60)
    at.run()
    next(field for field in at.text_input if field.label == "CSV path").set_value(str(csv_path))
    next(button for button in at.button if "Read CSV" in button.label).click()
    at.run()
    def path_line(app: AppTest) -> str:
        return next(str(block.value) for block in app.caption if "вы здесь" in str(block.value))

    next(button for button in at.button if button.label == "Apply column mapping").click()
    at.run()
    # Промежуточный шаг проверяется отдельно: в конечном состоянии подтверждены
    # оба флага, и подмена одного другим осталась бы незамеченной.
    assert "3. Type review ← вы здесь" in path_line(at), path_line(at)

    next(button for button in at.button if button.label == "Apply types").click()
    at.run()

    headings = [str(block.value) for block in at.subheader]
    assert any(heading.startswith("Step 4 of") for heading in headings), headings
    assert "4. ClickHouse and load parameters ← вы здесь" in path_line(at), path_line(at)


def test_only_the_current_step_explains_itself(tmp_path: Path) -> None:
    """Подсказки написаны хорошо, но лежали в свёрнутых блоках - их не видели.

    Раскрыт ровно один: тот, на котором человек стоит. Раскрывать все значит
    залить экран текстом, а свернуть все - вернуться к тому, с чего начали.
    """
    csv_path = tmp_path / "help.csv"
    csv_path.write_text("id,amount\n1,10.50\n", encoding="utf-8")

    def expanded_labels(app: AppTest) -> list[str]:
        # `Expander` в AppTest 1.58 не отдаёт состояние сам - оно в `proto`.
        return [
            node.proto.label
            for node in app.main
            if type(node).__name__ == "Expander" and node.proto.expanded
        ]

    at = AppTest.from_function(_app, default_timeout=60)
    at.run()
    assert len(expanded_labels(at)) == 1

    next(field for field in at.text_input if field.label == "CSV path").set_value(str(csv_path))
    next(button for button in at.button if "Read CSV" in button.label).click()
    at.run()

    opened = expanded_labels(at)
    assert len(opened) == 1, opened
    assert "колонки" in opened[0].lower()


def _expanded_labels(app: AppTest) -> list[str]:
    return [
        node.proto.label
        for node in app.main
        if type(node).__name__ == "Expander" and node.proto.expanded
    ]


def _path_line(app: AppTest) -> str:
    return next(str(block.value) for block in app.caption if "вы здесь" in str(block.value))


def _reach_types(at: AppTest, csv_path: Path) -> None:
    next(field for field in at.text_input if field.label == "CSV path").set_value(str(csv_path))
    next(button for button in at.button if "Read CSV" in button.label).click()
    at.run()
    next(button for button in at.button if button.label == "Apply column mapping").click()
    at.run()


def test_a_failed_mapping_does_not_claim_a_later_step(tmp_path: Path) -> None:
    """Блок упал на проверке - значит человек на нём, а не дальше.

    Подтверждающие флаги при ошибке не снимаются, поэтому шаг, выведенный из
    них, забегал вперёд: страница заканчивалась вторым шагом, а строка сверху
    ставила галочки на втором и третьем и объявляла четвёртый. Та же
    рассинхронизация, от которой строку и делали, только в другую сторону.
    """
    csv_path = tmp_path / "dup.csv"
    csv_path.write_text("id,amount\n1,10.50\n", encoding="utf-8")

    at = AppTest.from_function(_app, default_timeout=60)
    at.run()
    _reach_types(at, csv_path)
    next(button for button in at.button if button.label == "Apply types").click()
    at.run()

    rows = at.session_state["mapping_rows"]
    rows[1]["target_name"] = rows[0]["target_name"]
    at.session_state["mapping_rows"] = rows
    next(button for button in at.button if button.label == "Apply column mapping").click()
    at.run()

    assert at.error, "тест не воспроизвёл отказ проверки"
    path = _path_line(at)
    assert "2. Column mapping ← вы здесь" in path, path
    assert "3. Type review ✓" not in path, path
    # И объяснение того шага, где человек застрял, обязано быть раскрыто.
    assert any("колонки" in label.lower() for label in _expanded_labels(at)), _expanded_labels(at)


def test_the_last_step_can_be_the_current_one(tmp_path: Path) -> None:
    """Шаг 5 - экран, где нажимают «Create tables and load».

    Ветки для него не было: строка держала человека на четвёртом шаге, блок
    финальных действий оставался без заголовка, а его подсказка - про порядок
    действий и про то, что загрузка блокируется существующей таблицей - не
    раскрывалась никогда.
    """
    csv_path = tmp_path / "last.csv"
    csv_path.write_text("id,amount\n1,10.50\n", encoding="utf-8")

    at = AppTest.from_function(_app, default_timeout=60)
    at.run()
    _reach_types(at, csv_path)
    next(button for button in at.button if button.label == "Apply types").click()
    at.run()
    next(field for field in at.text_input if field.label == "Distributed table name").set_value("t")
    next(button for button in at.button if button.label == "Apply parameters").click()
    at.run()

    headings = [str(block.value) for block in at.subheader]
    assert any(heading.startswith("Step 5 of") for heading in headings), headings
    assert "5. Create tables and load ← вы здесь" in _path_line(at), _path_line(at)
    assert any("финальных" in label.lower() for label in _expanded_labels(at)), _expanded_labels(at)


def test_the_path_line_appears_exactly_once() -> None:
    """Нарисованная в каждом блоке, она копилась и противоречила себе: к шагу 4
    на экране висели четыре строки, и первая утверждала, что мы на первом."""
    at = AppTest.from_function(_app, default_timeout=30)
    at.run()

    path_lines = [block for block in at.caption if "вы здесь" in str(block.value)]
    assert len(path_lines) == 1
