# -*- coding: utf-8 -*-
"""
M22: curve statistics over windows relative to a phase (`pycorn.window_stats`), its table in
`pycorn export` (`--window`, `--window-curves`) and its display decimals.

Synthetic runs give exact numbers (axes on a 0.25 min grid, exact in binary); the samples check
the export path, repeated phases and CV windows on real data.
"""
import json
import os
from collections import OrderedDict

import numpy as np
import pandas as pd
import pytest

from pycorn import cli
from pycorn import run_export as rx
from pycorn.model import Chromatogram, Curve, Event, EventCurve, Run
from pycorn.sources import open_run
from pycorn.window_stats import (COLUMNS, INTERIM_RULE, Window, parse_window, parse_windows, phase_windows,
                                 window_stats, window_stats_rows)

HERE = os.path.dirname(os.path.abspath(__file__))
BLANK = os.path.join(HERE, "..", "samples", "u73_blank.zip")  # phases with Equilibration twice, Vc known
SAMPLE = os.path.join(HERE, "..", "samples", "sample.zip")


# ---------------------------------------------------------------------------
# synthetic run: 1 ml/min, Vc 2 ml (CV = time / 2), phases Equilibration 0-5, Load 5-10,
# Equilibration 10-15 (End Phase), run log to 20 min
# ---------------------------------------------------------------------------

def _curve(name, values, step=0.25, data_type="Conduction", unit="mS/cm", precision=2, cv=True, time=True):
    t = np.arange(0.0, 20.0 + step / 2, step)
    return Curve(name=name, data_type=data_type, unit=unit, precision=precision, curve_number=1,
                 time=t if time else None, volume=t.copy(), values=np.asarray(values(t), dtype=np.float32),
                 time_unit="min", volume_unit="ml", isochrone="Time" if time else "Volume", is_original=True,
                 column_volume=2.0 if cv else None, column_volume_unit="ml", uv_info=None, point_sets=[], xml={},
                 cv=t / 2 if cv else None)


def _event(t, text, subtype, cv=True):
    return Event(time=t, volume=t, text=text, type="Method", subtype=subtype, instruction_feedback=None, xml={},
                 cv=t / 2 if cv else None)


def _run(cv=True, extra_curves=()):
    status = " (Issued) (Processing) (Completed)"
    events = [_event(0.0, "Phase Equilibration" + status, "BlockStart", cv),
              _event(5.0, "Phase Load" + status, "BlockStart", cv),
              _event(10.0, "Phase Equilibration" + status, "BlockStart", cv),
              _event(15.0, "End Phase" + status, "BlockEnd", cv),
              _event(20.0, "End", "Undefined", cv)]
    log = EventCurve(name="Run Log", type="Logbook", curve_number=9, time_unit="min", volume_unit="ml",
                     is_original=True, column_volume=None, column_volume_unit=None, events=events, xml={})
    curves = [_curve("Cond", lambda t: t, cv=cv), _curve("pH", lambda t: 7.0 + 0 * t, step=1.0, data_type="pH",
                                                         unit=None, cv=cv)] + list(extra_curves)
    chrom = Chromatogram(name="Chrom.1", chromatogram_id=1, info={}, curves=OrderedDict((c.name, c) for c in curves),
                         event_curves=OrderedDict([(log.name, log)]), peak_tables=[], pool_tables=None)
    return Run(source="zip", locator="synthetic", result={"Name": "synthetic"},
               chromatograms=OrderedDict([(chrom.name, chrom)]), metadata=OrderedDict(), evaluation_log=[],
               manifest=[], entries=OrderedDict())


def _rows(windows, curves=("Cond",), run=None):
    run = run or _run()
    return window_stats_rows(run, run.chromatogram(), windows, list(curves))


# --- parsing -------------------------------------------------------------------

def test_parse_window_forms():
    w = parse_window("load=Sample Application:1:3 CV")
    assert w == Window("load", "Sample Application", 1.0, 3.0, "CV", "start", "start") and w.axis == "cv"
    w = parse_window("tail = Elution:end-2:end cv")
    assert (w.name, w.start_anchor, w.start, w.end_anchor, w.end, w.unit) == ("tail", "end", -2.0, "end", 0.0, "CV")
    assert parse_window("a=Wash:start:start+5 ml") == Window("a", "Wash", 0.0, 5.0, "ml", "start", "start")
    w = parse_window("b=Step 1: high salt:0.5:end+1 min")  # the phase name may hold ':' and spaces
    assert (w.phase, w.start, w.end_anchor, w.end, w.axis) == ("Step 1: high salt", 0.5, "end", 1.0, "time")
    assert parse_window(w.text()) == w
    assert parse_window("c=X:-1:2 L").axis == "volume"


@pytest.mark.parametrize("text", ["Load:1:3 CV", "=Load:1:3 CV", "w=Load:1:3", "w=Load:1 CV", "w=:1:3 CV",
                                  "w=Load:x:3 CV", "w=Load:1:3 mS/cm", "w=Load:3:1 CV", "w=Load:end:end-1 CV",
                                  "w=Load:middle:3 CV"])
def test_parse_window_rejects(text):
    with pytest.raises(ValueError):
        parse_window(text)


def test_parse_windows_rejects_a_name_twice():
    with pytest.raises(ValueError, match="twice"):
        parse_windows(["a=Load:0:1 min", "a=Load:1:2 min"])


# --- phase_windows / window_stats ---------------------------------------------------

def test_time_window_from_phase_start():
    rows, warnings = _rows(["w=Load:1:3 min"])
    assert warnings == [] and len(rows) == 1
    r = rows[0]
    assert list(r) == COLUMNS
    assert (r["phase"], r["occurrence"], r["phase_number"], r["axis"]) == ("Phase Load", 1, 2, "time")
    assert (r["start_anchor"], r["start_offset"], r["end_anchor"], r["end_offset"]) == ("phase start", 1.0,
                                                                                      "phase start", 3.0)
    assert (r["start_time"], r["end_time"]) == (6.0, 8.0)
    assert r["start_volume"] == pytest.approx(6.0) and r["end_cv"] == pytest.approx(4.0)
    values = np.arange(6.0, 8.0001, 0.25)  # inclusive bounds
    assert r["n"] == len(values) == 9
    assert r["mean"] == pytest.approx(7.0) and r["median"] == pytest.approx(7.0)
    assert r["sd"] == pytest.approx(np.std(values, ddof=1))
    assert (r["min"], r["max"]) == (6.0, 8.0) and r["warnings"] == []


def test_cv_window_from_phase_end_and_unit_conversion():
    rows, _ = _rows(["tail=Load:end-1:end CV", "sec=Load:60:120 s"])
    tail, sec = rows
    assert (tail["start_cv"], tail["end_cv"]) == (4.0, 5.0)
    assert tail["start_anchor"] == "phase end" and tail["start_offset"] == -1.0
    assert tail["start_time"] == pytest.approx(8.0) and tail["end_time"] == 10.0  # the phase end as logged
    assert tail["mean"] == pytest.approx(9.0) and tail["n"] == 9
    assert (sec["start_time"], sec["end_time"]) == (6.0, 7.0) and sec["window_unit"] == "s"


def test_repeated_phase_gives_one_row_per_occurrence():
    rows, _ = _rows(["eq=equilibration:0:1 min"])  # case and the 'Phase ' prefix ignored
    assert [(r["occurrence"], r["phase_number"], r["start_time"]) for r in rows] == [(1, 1, 0.0), (2, 3, 10.0)]
    assert [r["mean"] for r in rows] == pytest.approx([0.5, 10.5])


def test_missing_phase_and_curve_warn_without_rows():
    rows, warnings = _rows(["s=Strip:0:1 min"], curves=("Cond", "nosuch"))
    assert rows == []
    assert any("no phase 'Strip'" in w and "Phase Load" in w for w in warnings)
    assert any("nosuch" in w for w in warnings)


def test_window_clipped_to_phase_and_run_end():
    rows, _ = _rows(["long=Load:-1:99 min", "last=Equilibration:4:9 min"])
    long_, eq1, eq2 = rows
    assert (long_["start_time"], long_["end_time"]) == (5.0, 10.0)
    assert any("before the phase start" in w for w in long_["warnings"])
    assert any("after the phase end" in w for w in long_["warnings"])
    assert eq1["end_time"] == 5.0 and eq2["end_time"] == 15.0  # the End Phase entry ends the last phase
    assert eq2["n"] == 5 and eq2["mean"] == pytest.approx(14.5)


def test_empty_window_has_n_zero_and_no_statistics():
    rows, _ = _rows(["after=Load:end+1:end+2 min"])
    r = rows[0]
    assert r["n"] == 0 and all(r[k] is None for k in ("mean", "median", "sd", "min", "max"))
    assert any("empty" in w for w in r["warnings"])
    one, _ = _rows(["one=Load:1:1.1 min"])  # one point: no SD
    assert one[0]["n"] == 1 and one[0]["sd"] is None and one[0]["mean"] == 6.0


def test_curves_on_their_own_grids_and_axes():
    vol = _curve("Vol only", lambda t: 2 * t, time=False)  # a volume-grid curve: no time axis
    rows, _ = _rows(["w=Load:1:3 min"], curves=("ph", "Cond", "Vol only"), run=_run(extra_curves=[vol]))
    by = {r["curve"]: r for r in rows}
    assert by["pH"]["n"] == 3 and by["pH"]["mean"] == pytest.approx(7.0)  # 1 min grid: 6, 7, 8
    assert by["Cond"]["n"] == 9
    assert by["Vol only"]["n"] == 9 and by["Vol only"]["mean"] == pytest.approx(14.0)
    assert any("no time axis" in w for w in by["Vol only"]["warnings"])
    assert by["pH"]["curve_precision"] == 2 and by["Cond"]["curve_unit"] == "mS/cm"


def test_cv_window_without_vc_warns_without_rows():
    run = _run(cv=False)
    rows, warnings = _rows(["c=Load:0:1 CV", "t=Load:0:1 min"], run=run)
    assert [r["window"] for r in rows] == ["t"] and rows[0]["start_cv"] is None
    assert any("no column volume (Vc)" in w for w in warnings)


def test_window_stats_plain_window_and_nan():
    curve = _curve("Cond", lambda t: np.where(t == 1.0, np.nan, t))
    out = window_stats(curve, {"axis": "time", "start": 0.5, "end": 1.5, "unit": "min"})
    assert out["n"] == 4 and out["mean"] == pytest.approx(1.0)  # 0.5, 0.75, 1.25, 1.5; NaN left out
    out = window_stats(curve, {"axis": "time", "start": 30.0, "end": 60.0, "unit": "s"})
    assert out["n"] == 2 and out["max"] == 0.75  # 0.5 to 1 min, the NaN at 1 min left out


def test_no_window_or_no_curve_is_an_error():
    run = _run()
    with pytest.raises(ValueError, match="no default window"):
        window_stats_rows(run, run.chromatogram(), [], ["Cond"])
    with pytest.raises(ValueError, match="no default curves"):
        window_stats_rows(run, run.chromatogram(), ["w=Load:0:1 min"], [])


def test_display_decimals_follow_the_curve_and_window_unit():
    from pycorn.display_precision import column_decimals
    from pycorn.window_stats import to_frame
    run = _run(extra_curves=[_curve("UV", lambda t: t, data_type="UV", unit="mAU", precision=3)])
    rows, _ = window_stats_rows(run, run.chromatogram(), ["w=Load:1:3 min", "v=Load:0:1 L"], ["Cond", "UV"])
    frame = to_frame(rows)
    found = dict(zip(frame.columns, column_decimals(frame)))
    assert found["mean"] == [2, 3, 2, 3] and found["max"] == [2, 3, 2, 3]
    assert found["start_offset"] == [2, 2, 6, 6]  # min, L
    assert found["start_time"] == 2 and found["end_cv"] == 2 and found["n"] is None
    assert frame["mean"].iloc[0] == rows[0]["mean"]  # values never rounded


# --- real samples -------------------------------------------------------------

def test_sample_repeated_phase_and_roles():
    run = open_run(BLANK)
    chrom = run.chromatogram()
    rows, warnings = window_stats_rows(run, chrom, ["eq=Equilibration:end-2:end CV"], ["conductivity", "ph"])
    assert warnings == []
    assert sorted({r["occurrence"] for r in rows}) == [1, 2]
    assert {r["curve"] for r in rows} == {c.name for c in chrom.curves_with_role("conductivity")} | {
        c.name for c in chrom.curves_with_role("ph")}
    phases = [p for p in chrom.table_rows("phase") if p["name"] == "Phase Equilibration"]
    for r in rows:
        p = phases[r["occurrence"] - 1]
        assert r["end_cv"] == p["end_cv"] and r["start_cv"] == pytest.approx(p["end_cv"] - 2)
        assert r["n"] > 0 and r["min"] <= r["median"] <= r["max"]


def test_export_writes_the_table_csv_and_sheet(tmp_path):
    options = rx.ExportOptions(windows=["load=Sample Application:0:end CV", "tail=Elution:end-1:end CV",
                                        "x=Strip:0:1 ml"], window_curves=["conductivity", "uv"])
    run = open_run(BLANK)
    written = rx.write_run(run, tmp_path / "run", formats=("csv", "xlsx"), options=options)
    name = "Chrom.1_phase_window_statistics.csv"
    assert name in written
    frame = pd.read_csv(tmp_path / "run" / name)
    assert list(frame.columns) == COLUMNS and set(frame["window"]) == {"load", "tail"}
    assert all(isinstance(json.loads(w), list) for w in frame["warnings"])
    warnings = pd.read_csv(tmp_path / "run" / "warnings.csv")
    texts = list(warnings["message"])
    assert INTERIM_RULE in texts and any("no phase 'Strip'" in t for t in texts)
    guide = pd.read_csv(tmp_path / "run" / "column_guide.csv")
    mine = guide[guide["table"] == "Phase window statistics"]
    assert set(mine["column"]) == set(COLUMNS) and set(mine["table_origin"]) == {"PyCORN calculated"}
    assert (mine.set_index("column").loc["mean", "origin"]) == "PyCORN calculated"
    from openpyxl import load_workbook
    book = [f for f in written if f.endswith(".xlsx")][0]
    assert "Phase window statistics" in load_workbook(tmp_path / "run" / book, read_only=True).sheetnames


def test_export_without_windows_writes_no_table(tmp_path):
    written = rx.write_run(open_run(SAMPLE), tmp_path / "run", formats=("csv",))
    assert not any("phase_window" in f for f in written)


def test_cli_window_flags(tmp_path, capsys):
    args = ["export", SAMPLE, "--out", "x", "--window", "w=Run:0:end min", "--window", "v=Run:1:2 CV",
            "--window-curves", "uv,Cond"]
    o = cli.export_options(cli.build_parser().parse_args(args))
    assert o.windows == ["w=Run:0:end min", "v=Run:1:2 CV"] and o.window_curves == ["uv", "Cond"]
    for bad in (["--window", "w=Run:0:end min"], ["--window-curves", "uv"],
                ["--window", "w=Run:0:1", "--window-curves", "uv"]):
        assert cli.main(["export", SAMPLE, "--out", str(tmp_path / "o")] + bad) == 2
        assert "error:" in capsys.readouterr().err
    out = tmp_path / "out"
    assert cli.main(["export", SAMPLE, "--out", str(out), "--window", "w=Run:0:end min",
                     "--window-curves", "uv"]) == 0
    found = [p for p in out.rglob("*_phase_window_statistics.csv")]
    assert len(found) == 1 and len(pd.read_csv(found[0])) >= 1
