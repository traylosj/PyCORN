# -*- coding: utf-8 -*-
"""
`pycorn windows` (Track 9, A7): M22 window statistics without an export. The rows are the
export's `phase_window_statistics` table; no default window or curve (D28); --json / --jsonl /
--csv - for programs with people masked unless --no-mask; --chromatogram as the export's (W17).
Public samples and the synthetic multi-chromatogram result only.
"""
import io
import json
import os

import pandas as pd
import pytest

from pycorn import cli, db_connector
from pycorn import run_export as rx
from pycorn import window_stats
from pycorn.db_connector import ENV_FILE_VAR
from pycorn.json_output import SCHEMA_VERSION
from pycorn.version import __version__

import multichrom_fixture as mf

SAMPLES = os.path.abspath("../samples")
BLANK = os.path.join(SAMPLES, "u73_blank.zip")  # Equilibration twice, Vc known
SAMPLE = os.path.join(SAMPLES, "sample.zip")  # one phase 'Run'
WINDOWS = ["--window", "eq=Equilibration:end-2:end CV", "--window", "all=Equilibration:0:end+1 min"]
CURVES = ["--window-curves", "conductivity,uv"]
# made up for the tests: what a real run holds in these fields
SYSTEM, USER, COMPUTER = "AKTA-Bay7", "LAB\\jdoe", "LABPC-042"


@pytest.fixture(autouse=True)
def _no_settings_from_outside(monkeypatch, tmp_path):
    for key in list(os.environ):
        if key.upper().startswith("UNICORN_") or key.upper() == ENV_FILE_VAR:
            monkeypatch.delenv(key)
    monkeypatch.setattr(db_connector, "DEFAULT_ENV_FILE", tmp_path / "checkout" / "sql" / ".env")
    monkeypatch.setattr(db_connector, "user_env_file", lambda: tmp_path / "user" / ".env")
    monkeypatch.setattr(db_connector, "_local_server_answers", lambda port: False)


@pytest.fixture(scope="module")
def multi(tmp_path_factory):
    return str(mf.build(tmp_path_factory.mktemp("multi") / "multi.zip"))


def _main(capsys, *argv):
    code = cli.main(list(argv))
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _error(err):
    lines = [l for l in err.splitlines() if l.startswith("{")]
    assert len(lines) == 1
    return json.loads(lines[0])["error"]


# --- the same rows as the export ------------------------------------------------------

def test_rows_equal_the_export_table(tmp_path, capsys):
    out = tmp_path / "exp"
    assert cli.main(["export", BLANK, "--out", str(out)] + WINDOWS + CURVES) == 0
    capsys.readouterr()
    exported = [p for p in out.rglob("*_phase_window_statistics.csv")]
    assert len(exported) == 1
    file = tmp_path / "w.csv"
    code, stdout, err = _main(capsys, "windows", BLANK, *WINDOWS, *CURVES, "--csv", str(file))
    assert code == 0
    mine = pd.read_csv(file)
    assert list(mine.columns) == cli.WINDOW_ROW_COLUMNS + window_stats.COLUMNS
    theirs = pd.read_csv(exported[0])
    pd.testing.assert_frame_equal(mine[window_stats.COLUMNS], theirs)
    assert set(mine["window"]) == {"eq", "all"} and sorted(set(mine["occurrence"])) == [1, 2]
    # the human table and the export's warnings: clipping and the interim rule
    lines = stdout.splitlines()
    assert lines[0] == "%d row(s) of window statistics" % len(mine)
    assert lines[1].split() == ["run", "chrom", "window", "phase", "#", "range", "curve", "n", "mean", "median",
                                "sd", "min", "max"]
    assert any("clipped to it" in l for l in lines) and lines[-2] == "note: " + window_stats.INTERIM_RULE
    assert lines[-1] == "written: %s" % file


def test_rows_are_window_stats_rows(capsys):
    from pycorn import open_run
    run = open_run(BLANK)
    rows, _ = window_stats.window_stats_rows(run, run.chromatogram(), ["eq=Equilibration:end-2:end CV"], ["ph"])
    code, out, _ = _main(capsys, "windows", BLANK, "--window", "eq=Equilibration:end-2:end CV",
                         "--window-curves", "ph", "--json")
    assert code == 0
    shown = json.loads(out)["rows"]
    assert len(shown) == len(rows) > 0
    for mine, theirs in zip(shown, rows):
        assert mine["source"] == "zip" and mine["result_id"] is None
        assert {k: mine[k] for k in theirs} == json.loads(json.dumps(theirs))


# --- no default window or curve (D28) ---------------------------------------------------

@pytest.mark.parametrize("flags", [CURVES, WINDOWS, WINDOWS + ["--window-curves", " , "],
                                   ["--window", "w=Run:0:1", "--window-curves", "uv"],
                                   ["--window", "w=Run:0:1 min", "--window", "w=Run:1:2 min", "--window-curves", "uv"]])
def test_missing_or_bad_window_or_curves_exit_2(capsys, flags):
    code, out, err = _main(capsys, "windows", SAMPLE, *flags)
    assert code == 2 and out == "" and err.startswith("error: --window")
    for mode in ("--json", "--jsonl"):
        code, out, err = _main(capsys, "windows", SAMPLE, *flags, mode)
        assert code == 2 and out == ""
        e = _error(err)
        assert e["key"] == "bad_flag" and e["exit"] == 2 and e["text"].startswith("--window")


def test_inputs_and_stdout_twice(capsys):
    code, out, err = _main(capsys, "windows", *WINDOWS, *CURVES)  # neither files nor --from
    assert code == 2 and "--from" in err
    code, out, err = _main(capsys, "windows", SAMPLE, *WINDOWS, *CURVES, "--json", "--csv", "-")
    assert code == 2 and _error(err)["key"] == "bad_flag"


# --- exit 1: something asked for gave nothing ---------------------------------------------

def test_exit_1_when_a_window_or_curve_gives_nothing(capsys):
    code, out, err = _main(capsys, "windows", BLANK, "--window", "x=No Such Phase:0:1 min", *CURVES)
    assert code == 1 and "window x gave no rows" in out and "no phase 'No Such Phase'" in err
    code, out, err = _main(capsys, "windows", BLANK, "--window", "eq=Equilibration:0:1 min",
                           "--window-curves", "conductivity,nosuchcurve", "--json")
    assert code == 1
    doc = json.loads(out)
    assert doc["rows"] and any("no curve 'nosuchcurve'" in p for p in doc["problems"])
    assert any("nosuchcurve" in w["text"] for w in doc["warnings"])
    code, out, err = _main(capsys, "windows", BLANK, os.path.join(SAMPLES, "nothere.zip"), "--window",
                           "eq=Equilibration:0:1 min", *CURVES)
    assert code == 2  # a file named on the command line that is not there: an error, as for export


# --- for programs ---------------------------------------------------------------------

def test_json_and_jsonl_envelope(capsys):
    code, out, err = _main(capsys, "windows", BLANK, SAMPLE, "--window", "eq=Equilibration:end-2:end CV",
                           "--window-curves", "conductivity", "--json")
    assert code == 1  # sample.zip has no Equilibration phase
    doc = json.loads(out)
    assert list(doc)[:4] == ["schema", "version", "command", "masked"]
    assert doc["schema"] == SCHEMA_VERSION == 1 and doc["version"] == __version__
    assert doc["command"] == "windows" and doc["masked"] is True
    assert list(doc)[4:] == ["rows", "runs", "not_read", "problems", "warnings", "notes"]
    assert doc["runs"] == 2 and doc["not_read"] == [] and doc["notes"] == [window_stats.INTERIM_RULE]
    assert list(doc["rows"][0]) == cli.WINDOW_ROW_COLUMNS + window_stats.COLUMNS
    assert "row(s) of window statistics" in err and "incomplete:" in err
    code, out, err = _main(capsys, "windows", BLANK, SAMPLE, "--window", "eq=Equilibration:end-2:end CV",
                           "--window-curves", "conductivity", "--jsonl")
    lines = [json.loads(l) for l in out.splitlines()]
    head, rows, tail = lines[0], lines[1:-1], lines[-1]
    assert code == 1 and head["type"] == "header" and head["command"] == "windows" and head["masked"] is True
    assert {r["type"] for r in rows} == {"row"} and tail["type"] == "summary"
    assert tail["rows"] == len(rows) == len(doc["rows"]) and tail["problems"] == doc["problems"]


@pytest.fixture
def planted(monkeypatch):
    """Every loaded run stores a planted user, computer and system; each row's warnings quote them."""
    real_runs, real_rows = rx._selected_runs, window_stats.window_stats_rows
    said = "seen on %s by %s at %s" % (SYSTEM, USER, COMPUTER)

    def runs(*a, **kw):
        for i, run in real_runs(*a, **kw):
            run.result.update({"CreatedBy": USER, "ComputerName": COMPUTER, "SystemName": SYSTEM})
            yield i, run

    def rows(*a, **kw):
        found, notes = real_rows(*a, **kw)
        for r in found:
            r["warnings"] = list(r["warnings"]) + [said]
        return found, notes + [said]
    monkeypatch.setattr(rx, "_selected_runs", runs)
    monkeypatch.setattr(window_stats, "window_stats_rows", rows)
    return said


def _people(text):
    return [v for v in (SYSTEM, "jdoe", COMPUTER) if v in text]


def test_csv_stdout_masks_people_only(tmp_path, capsys, planted):
    flags = ["windows", BLANK, "--window", "eq=Equilibration:end-2:end CV", "--window-curves", "conductivity"]
    file = tmp_path / "w.csv"
    code, out, err = _main(capsys, *flags, "--csv", str(file))
    assert code == 0 and planted in out + err  # human mode and the file: as stored
    stored = pd.read_csv(file)
    assert all(planted in json.loads(w) for w in stored["warnings"])
    code, out, err = _main(capsys, *flags, "--csv", "-")
    assert code == 0 and _people(out + err) == []
    masked = pd.read_csv(io.StringIO(out))
    # people masked; run, chromatogram, window, phase and curve names shown
    for column in ("run", "chromatogram", "window", "phase", "curve"):
        assert list(masked[column]) == list(stored[column])
    assert all("System" in w and "User" in w and "Computer" in w for w in masked["warnings"])
    assert any(l.startswith("warning: Run1 Chrom.1: seen on System") for l in err.splitlines())
    code, out, err = _main(capsys, *flags, "--csv", "-", "--no-mask")
    assert code == 0 and planted in err
    pd.testing.assert_frame_equal(pd.read_csv(io.StringIO(out)), stored)
    code, out, err = _main(capsys, *flags, "--json")
    assert _people(out + err) == [] and json.loads(out)["masked"] is True
    code, out, err = _main(capsys, *flags, "--json", "--no-mask")
    doc = json.loads(out)
    assert doc["masked"] is False and planted in doc["rows"][0]["warnings"]


# --- which chromatograms (W17) ------------------------------------------------------------

def test_chromatogram_selection(multi, capsys):
    flags = ["windows", multi, "--window", "w=Run:0:1 ml", "--window-curves", "uv", "--json"]
    code, out, err = _main(capsys, *flags)
    assert code == 0
    assert {r["chromatogram"] for r in json.loads(out)["rows"]} == {"Chrom.1"}  # the original by default
    code, out, err = _main(capsys, *flags, "--chromatogram", mf.IMPORTED)
    rows = json.loads(out)["rows"]
    assert rows and {r["chromatogram"] for r in rows} == {mf.IMPORTED}
    assert any(w["text"].startswith("imported chromatogram: run log") for w in json.loads(out)["warnings"])
    code, out, err = _main(capsys, *flags, "--chromatogram", "all")
    assert {r["chromatogram"] for r in json.loads(out)["rows"]} >= {"Chrom.1", mf.IMPORTED, mf.COPY}
    code, out, err = _main(capsys, *flags, "--chromatogram", "nosuch")
    assert code == 1 and json.loads(out)["rows"] == [] and "no chromatogram 'nosuch'" in err
    assert any("no chromatogram selected" in p for p in json.loads(out)["problems"])
