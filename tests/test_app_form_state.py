from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from streamlit.testing.v1 import AppTest


def _form_app() -> None:
    # AppTest.from_function runs this body in isolation: imports stay inside.
    import streamlit as st

    from csv_click.app import _clear_csv_read_state, _render_connection_and_load_form
    from csv_click.schema import CsvColumn, CsvSchema
    from csv_click.settings import AppSettings

    st.session_state.setdefault("app_settings", AppSettings())

    if st.session_state.get("form_probe_new_file"):
        # Что делает "Read CSV" другого файла: та же чистка состояния разбора.
        st.session_state["form_probe_new_file"] = False
        _clear_csv_read_state(include_path=False)

    names = st.session_state.get("form_probe_names") or ["alpha", "beta", "gamma"]
    columns = [
        CsvColumn(
            column_name=name,
            source_name=name,
            inferred_type="String",
            final_type="String",
            nullable=False,
            sample_values=[],
        )
        for name in names
    ]
    if not st.session_state.get("form_probe_hide"):
        _render_connection_and_load_form(CsvSchema(columns=columns))


def _start() -> AppTest:
    at = AppTest.from_function(_form_app, default_timeout=30)
    at.run()
    return at


def _selectbox(at: AppTest, label: str):
    return next(box for box in at.selectbox if box.label == label)


def _text_input(at: AppTest, label: str):
    return next(field for field in at.text_input if field.label == label)


def _hide_form_for_one_run(at: AppTest) -> None:
    at.session_state["form_probe_hide"] = True
    at.run()
    at.session_state["form_probe_hide"] = False
    at.run()


def test_order_by_and_sharding_key_survive_form_hidden_rerun() -> None:
    at = _start()
    _selectbox(at, "ORDER BY").select("beta")
    _selectbox(at, "Distributed sharding key").select("gamma")
    at.run()

    _hide_form_for_one_run(at)

    assert _selectbox(at, "ORDER BY").value == "beta"
    assert _selectbox(at, "Distributed sharding key").value == "gamma"


def test_load_params_keep_operator_keys_after_form_hidden_rerun() -> None:
    at = _start()
    _selectbox(at, "ORDER BY").select("beta")
    _selectbox(at, "Distributed sharding key").select("gamma")
    _text_input(at, "Distributed table name").set_value("probe_table")
    at.run()
    assert at.session_state["load_params"]["order_by"] == "beta"

    _hide_form_for_one_run(at)
    _text_input(at, "Distributed table name").set_value("probe_table")
    at.run()

    params = at.session_state["load_params"]
    assert params["order_by"] == "beta"
    assert params["sharding_key"] == "gamma"


def test_order_by_selection_survives_added_column() -> None:
    at = _start()
    _selectbox(at, "ORDER BY").select("beta")
    at.run()

    at.session_state["form_probe_names"] = ["alpha", "beta", "gamma", "delta"]
    at.run()

    assert _selectbox(at, "ORDER BY").value == "beta"


def test_dropped_order_by_selection_warns_and_falls_back() -> None:
    at = _start()
    _text_input(at, "Distributed table name").set_value("probe_table")
    at.run()
    _selectbox(at, "ORDER BY").select("beta")
    at.run()

    at.session_state["form_probe_names"] = ["alpha", "gamma"]
    at.run()

    # Проверяется ТОТ прогон, в котором предупреждение звучит: оно звучит один
    # раз, и на следующем прогоне сверять уже нечего. Колонок остаётся две,
    # поэтому объявление не той колонки отличимо от объявления первой.
    assert _selectbox(at, "ORDER BY").value == "alpha"
    used = at.session_state["load_params"]["order_by"]
    assert used == "alpha"
    warnings = [str(w.value) for w in at.warning]
    assert any(
        f'"beta" is no longer among the columns; falling back to "{used}".' in message
        for message in warnings
    )


def test_second_consecutive_selection_is_applied() -> None:
    at = _start()
    # Имя таблицы заполняется ЗАРАНЕЕ: любое действие между двумя выборами
    # ре-синхронизирует виджет и прячет как раз тот дефект, что здесь проверяется.
    _text_input(at, "Distributed table name").set_value("probe_table")
    at.run()

    _selectbox(at, "ORDER BY").select("beta")
    at.run()
    _selectbox(at, "ORDER BY").select("gamma")
    at.run()

    assert _selectbox(at, "ORDER BY").value == "gamma"
    assert at.session_state["load_params"]["order_by"] == "gamma"


def test_dropped_selection_is_announced_once_not_on_every_rerun() -> None:
    at = _start()
    _selectbox(at, "ORDER BY").select("beta")
    at.run()
    at.session_state["form_probe_names"] = ["alpha", "gamma"]
    at.run()
    assert any("beta" in str(w.value) for w in at.warning)

    _text_input(at, "Distributed table name").set_value("probe_table")
    at.run()

    assert not [str(w.value) for w in at.warning]


def test_returning_column_does_not_silently_take_the_key_back() -> None:
    at = _start()
    _selectbox(at, "ORDER BY").select("beta")
    at.run()
    at.session_state["form_probe_names"] = ["alpha", "gamma"]
    at.run()
    assert _selectbox(at, "ORDER BY").value == "alpha"

    at.session_state["form_probe_names"] = ["alpha", "beta", "gamma"]
    at.run()

    assert _selectbox(at, "ORDER BY").value == "alpha"
    assert not [str(w.value) for w in at.warning]


def test_untouched_defaults_are_not_reported_as_operator_choices() -> None:
    at = _start()

    at.session_state["form_probe_names"] = ["delta", "epsilon"]
    at.run()

    assert _selectbox(at, "ORDER BY").value == "delta"
    assert not [str(w.value) for w in at.warning]


def test_reading_another_csv_does_not_carry_the_previous_keys_over() -> None:
    at = _start()
    _selectbox(at, "ORDER BY").select("gamma")
    _selectbox(at, "Distributed sharding key").select("beta")
    at.run()

    at.session_state["form_probe_new_file"] = True
    at.session_state["form_probe_names"] = ["delta", "beta", "gamma"]
    at.run()

    assert _selectbox(at, "ORDER BY").value == "delta"
    assert _selectbox(at, "Distributed sharding key").value == "delta"
    assert not [str(w.value) for w in at.warning]
