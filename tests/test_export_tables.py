# -*- coding: utf-8 -*-
"""
`pycorn.export_tables` origins: 'PyCORN calculated' only when every input is stored or
calculated; an analysis input (Vc, rebuilt pauses, where a row ends) makes a column
'PyCORN analysis' (owner, 2026-11-09).
"""
from pycorn import export_tables as et

# inputs that are PyCORN's choices: CV needs Vc, UTC and wall-clock seconds the rebuilt pauses,
# a row's end and length where PyCORN ends it
ANALYSIS_INPUT = ("cv", "utc", "start_s", "end_s", "end_time", "end_volume", "length_")
STORED_OFFSET_UTC = {("calibration", "date_utc"), ("audit_log", "time_utc")}  # stored date - stored offset


def _columns():
    for key, table in et.TABLES.items():
        for column, info in table.columns.items():
            yield key, column, info


def test_every_column_has_origin_meaning_and_check():
    for key, column, info in _columns():
        origin, meaning, check = info
        assert origin in et.ORIGIN_NOTES, (key, column)
        assert bool(check) == (origin == et.CALCULATED), (key, column)  # a calculated column says how to check it


def test_no_calculated_column_takes_an_analysis_input():
    for key, column, (origin, _, _) in _columns():
        if (key, column) in STORED_OFFSET_UTC:
            continue
        if any(column == a or column.startswith(a) or column.endswith("_" + a) for a in ANALYSIS_INPUT):
            assert origin == et.ANALYSIS, (key, column)


def test_axes_and_curves():
    assert et.column_origin("curves", "UV 1_280: time (min)")[0] == et.UNICORN
    assert "adjusted" not in et._CURVE_COLUMN.pattern
    assert et.column_origin("curves", "UV 1_280: CV")[0] == et.ANALYSIS
    assert et.column_origin("curves", "UV 1_280: UTC")[0] == et.ANALYSIS
    assert et.column_origin("curves", "UV 1_280: value (mAU)")[0] == et.UNICORN
    assert et.column_origin("curves", "UV 1_280 (mg/ml): value (mg/ml)")[0] == et.ANALYSIS
    assert et.column_check("curves", "UV 1_280: value (mAU)") == ""


def test_user_inputs_have_their_own_origin():
    assert et.column_origin("concentration", "epsilon")[0] == et.USER
    assert et.column_origin("concentration", "response_factor")[0] == et.USER
    assert et.column_origin("bypass_uv", "loop_volume_ml")[0] == et.USER
    assert et.column_origin("bypass_uv", "sample_concentration_mg_ml")[0] == et.USER
    # a formula on user inputs alone stays calculated; a column that is stored or yours by run is analysis
    assert et.column_origin("concentration", "epsilon_ml_per_mg_cm")[0] == et.CALCULATED
    assert et.column_origin("run_info", "column_length_m")[0] == et.ANALYSIS


def test_page_default_columns_are_real_columns():
    # the wide tables the page shows a default set of (owner, 2026-11-09); test_browser checks the rest
    wide = {"phases", "injections", "fractions", "peaks", "hint_injections", "variables", "method_steps", "column",
            "calibration", "audit_log", "phase_windows"}
    assert {k for k, t in et.TABLES.items() if t.shown} == wide
    for key in ("phases", "injections", "fractions", "hint_injections", "phase_windows"):
        assert set(et.TABLES[key].shown) <= set(et.TABLES[key].columns), key
    for key in ("phases", "injections", "fractions"):
        for column in ("blocks", "inlets", "column_position", "injection_valve", "set_marks"):
            assert column in et.TABLES[key].shown, (key, column)


def test_tables_of_pycorn_rules_are_analysis():
    for key in ("phases", "injections", "fractions", "sections", "pauses", "hint_blocks", "hint_injections",
                "run_info", "warnings"):
        assert et.origin(key) == et.ANALYSIS, key
