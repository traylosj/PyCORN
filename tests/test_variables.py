import glob
import math
import os
from collections import Counter, OrderedDict

import pytest

from pycorn import Run, open_folder, open_run
from pycorn.variables import COLUMNS, parse_number

SAMPLE = os.path.join("..", "samples", "sample.zip")
TEST_FILES = os.path.join("..", "unicorn_test_files")


def _criterion(kind, name, value, unit=None, with_unit=True):
    c = OrderedDict([("Name", kind), ("Keyword1", name), ("Keyword2", value)])
    if with_unit:
        c["ExtraDisplayInformation"] = unit
    return c


def _run(criteria):
    return Run(source="zip", locator="synthetic", result={"Name": "synthetic", "search_criteria": criteria},
               chromatograms=OrderedDict(), metadata=OrderedDict(), evaluation_log=[], manifest=[],
               entries=OrderedDict())


# Agrees with SQL Server's TRY_CAST(... AS float), checked against SQL Server; empty text is NaN here (SQL Server: 0)
@pytest.mark.parametrize("text,expected", [
    ("0.070", 0.07), ("7e3", 7000.0), ("007", 7.0), (" 1 ", 1.0), ("1.", 1.0), (".5", 0.5), ("+1", 1.0),
    ("1E+3", 1000.0), ("1d2", 100.0), ("\t2", 2.0), ("+.5e-2", 0.005),
    ("", None), (None, None), ("-", None), ("1e", None), ("inf", None), ("nan", None), ("1,5", None),
    ("1_000", None), ("0x10", None), ("2\t", None), ("1e400", None), ("e3", None), ("12 3", None),
    ("In-line", None), ("Position 5", None),
])
def test_parse_number_like_sql_server(text, expected):
    number = parse_number(text)
    assert math.isnan(number) if expected is None else number == expected


def test_variables_one_row_per_stored_row():
    run = _run([
        _criterion("VariableValue", "UV1", "280", "nm"),
        _criterion("ScoutingVariableValue", "Sampleflow", "0.040", "ml/min"),
        _criterion("ScoutingVariableValue", "InletA", "A4", ""),
        _criterion("VariableValue", "pH cell", "In-line", with_unit=False),
        _criterion("QuestionAnswer", "Operator?", None),
    ])
    frame = run.variables()
    assert list(frame.columns) == COLUMNS
    assert frame["variable"].tolist() == ["UV1", "Sampleflow", "InletA", "pH cell", "Operator?"]
    # text exactly as stored; an empty element (None from the XML reader) is '' as in the database
    assert frame["value"].tolist() == ["280", "0.040", "A4", "In-line", ""]
    # unit: '' when stored empty, None when absent (NULL in the database)
    assert frame["unit"].tolist() == ["nm", "ml/min", "", None, ""]
    assert frame["scouting"].tolist() == [False, True, True, False, False]
    assert frame["number"].tolist()[:2] == [280.0, 0.04]
    assert frame["number"].iloc[2:].isna().all()


def test_variable_lookup():
    run = _run([_criterion("VariableValue", "Flow rate", "0.350", "ml/min"),
                _criterion("ScoutingVariableValue", "ColVolume", "0.350", "ml"),
                _criterion("QuestionAnswer", "Operator?", "yes")])
    v = run.variable("ColVolume")
    assert (v.value, v.number, v.unit, v.scouting) == ("0.350", 0.35, "ml", True)
    assert run.variable("Flow rate").scouting is False
    with pytest.raises(KeyError, match="available: Flow rate, ColVolume"):
        run.variable("Operator?")  # start-protocol answers are not method variables
    with pytest.raises(KeyError, match="no method variable 'Missing'"):
        run.variable("Missing")


def test_scouting_value_wins_if_a_name_repeats():
    # not seen in UNICORN data (each variable is stored once), but the override rule decides it
    run = _run([_criterion("VariableValue", "Flow", "1.0"), _criterion("ScoutingVariableValue", "Flow", "2.0")])
    assert run.variable("Flow").value == "2.0"


def test_no_search_criteria():
    run = _run([])
    assert run.variables().empty and list(run.variables().columns) == COLUMNS


def test_sample_zip():
    frame = open_run(SAMPLE).variables()
    assert len(frame) and frame["kind"].notna().all()


def _local_runs():
    paths = sorted(glob.glob(os.path.join(TEST_FILES, "U7.*", "*.zip")))
    runs = [open_run(p) for p in paths]
    for p in sorted(glob.glob(os.path.join(TEST_FILES, "UFOL", "*.UFol"))):
        runs.extend(open_folder(p))
    return runs


def test_real_runs():
    runs = _local_runs()
    if not runs:
        pytest.skip("no test files in %s" % TEST_FILES)
    scouted = 0
    for run in runs:
        criteria = run.result["search_criteria"]
        frame = run.variables()
        assert len(frame) == len(criteria)
        assert frame["value"].tolist() == [c.get("Keyword2") or "" for c in criteria]
        assert frame["unit"].tolist() == [
            (c["ExtraDisplayInformation"] or "") if "ExtraDisplayInformation" in c else None for c in criteria]
        variables = frame[frame["kind"].isin(["VariableValue", "ScoutingVariableValue"])]
        # UNICORN stores each variable once: a scouted variable has no separate method row
        assert not [n for n, k in Counter(variables["variable"]).items() if k > 1]
        for row in variables.itertuples():
            v = run.variable(row.variable)
            assert (v.value, v.scouting) == (row.value, row.scouting)
        scouted += bool(frame["scouting"].any())
    assert scouted, "a local result with scouting rows was expected"
