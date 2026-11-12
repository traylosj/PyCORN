# -*- coding: utf-8 -*-
"""`pycorn find` / `pycorn export` (run search step 5): argument parsing, export layout, files."""
import json
import os
import pickle
import shutil

import numpy as np
import pandas as pd
import pytest

from pycorn import cli
from pycorn import run_export as rx
from pycorn.sources import open_run

SAMPLE = os.path.abspath("../samples/sample.zip")


# --- parsing -----------------------------------------------------------------

@pytest.mark.parametrize("text, expected", [
    ("Flow rate>=1 ml/min", ("Flow rate", (">=", 1.0, "ml/min"))),
    ("UV1==280", ("UV1", ("==", 280.0))),
    ("Column position=Position 2", ("Column position", "Position 2")),
    ("Sample_ID", ("Sample_ID", None)),
    ("pH != 7.5", ("pH", ("!=", 7.5))),
    ("Volume<2e3 ml", ("Volume", ("<", 2000.0, "ml"))),
    ("Name=a>=b", ("Name", "a>=b")),  # the first operator splits
])
def test_parse_var(text, expected):
    assert cli.parse_var(text) == expected


@pytest.mark.parametrize("text", ["Flow rate>=x", ">=1", ""])
def test_parse_var_rejects(text):
    with pytest.raises(ValueError):
        cli.parse_var(text)


def test_parse_ranges_and_ints():
    assert cli.parse_range("2019-01-01..2019-06-30") == ("2019-01-01", "2019-06-30")
    assert cli.parse_range("2019-01-01..") == ("2019-01-01", None)
    assert cli.parse_range("2019-01-01") == ("2019-01-01", "2019-01-01")
    assert cli.parse_ints("3") == 3
    assert cli.parse_ints("3,5") == [3, 5]  # a list: any of them
    assert cli.parse_ints("3..8") == (3, 8)  # a tuple: a range
    assert cli.parse_ints("2..") == (2, None)


def test_parse_sources():
    assert cli.parse_sources(None) is None
    assert cli.parse_sources(["discover"]) == "discover"
    assert cli.parse_sources(["default,lab2"]) == ["default", "lab2"]
    assert cli.parse_sources(["lab2:A,B", "default"]) == {"lab2": ["A", "B"], "default": None}
    assert cli.parse_sources(["lab2:*"]) == {"lab2": "*"}


def test_filters_from_arguments():
    args = cli.build_parser().parse_args(
        ["find", "--folder", "/DefaultHome/**", "--name", "*a*", "--name", "*b*", "--system", "AKTA*",
         "--var", "Flow rate>=1 ml/min", "--var", "Flow rate<5", "--var", "Sample_ID", "--has-role", "uv,ph",
         "--no-peaks", "--run-index", "1..3", "--run-start", "2020-01-01.."])
    f = cli.filters_from(args)
    assert f["folder"] == "/DefaultHome/**" and f["name"] == ["*a*", "*b*"] and f["system"] == "AKTA*"
    assert f["variables"] == {"Flow rate": [(">=", 1.0, "ml/min"), ("<", 5.0)], "Sample_ID": None}
    assert f["has_curve_roles"] == ["uv", "ph"] and f["has_peak_tables"] is False
    assert f["run_index"] == (1, 3) and f["run_start"] == ("2020-01-01", None)
    assert "include_hidden" not in f and "state" not in f


def test_ascii_console_text():
    assert cli.ascii_text("Bioreaktor ü 5") == "Bioreaktor \\xfc 5"


# --- layout ------------------------------------------------------------------

@pytest.mark.parametrize("text, expected", [
    ("Run 1: a/b", "Run 1_ a_b"), ("CON", "_CON"), ("com1.txt", "_com1.txt"), ("  x. ", "x"), ("", "_"),
    (None, "_"), ("ä <ok>", "ä _ok_"),
])
def test_clean_name(text, expected):
    assert rx.clean_name(text) == expected


def _catalog(rows):
    cols = ["source", "database", "result_id", "name", "folder_path"]
    return pd.DataFrame([dict(zip(cols, r)) for r in rows], columns=cols)


def test_run_folders_all_colliding_names_get_the_id():
    cat = _catalog([
        ("default", "DB", 7, "Run", "/Home/P"),
        ("default", "DB", 9, "run", "/Home/P"),  # same folder, case differs: still a collision
        ("default", "DB", 11, "Run", "/Home/Q"),  # another folder: plain name
        ("default", "DB", 12, "P", "/Home"),  # same as the folder P: gets its id too
        ("default", "DB", 13, "Other", "/Home/P"),
    ])
    labels = ["default#1"] * len(cat)
    got = [str(p).replace("\\", "/") for p in rx.run_folders(cat, labels)]
    assert got == ["default#1/Home/P/Run_7", "default#1/Home/P/run_9", "default#1/Home/Q/Run",
                   "default#1/Home/P_12", "default#1/Home/P/Other"]


def test_run_folders_databases_and_files_apart():
    cat = _catalog([("default", "A", 5, "Run", "/H"), ("default", "B", 5, "Run", "/H")])
    got = [str(p).replace("\\", "/") for p in rx.run_folders(cat, ["default#1", "default#2"])]
    assert got == ["default#1/H/Run", "default#2/H/Run"]
    files = pd.DataFrame({"name": ["Run", "Run"], "folder_path": ["/H", "/H"], "result_id": [None, None],
                          "original_id": [None, None], "path": ["/x/a.zip", "/y/a.zip"]})
    got = [str(p).replace("\\", "/") for p in rx.run_folders(files, ["zip", "zip"])]
    assert got == ["zip/H/Run_a", "zip/H/Run_a_2"]  # same file name: a counter settles it


# --- writing -----------------------------------------------------------------

@pytest.fixture(scope="module")
def run():
    return open_run(SAMPLE)


def test_write_run_csv_and_pickle(run, tmp_path):
    folder = tmp_path / "out" / "run"
    written = rx.write_run(run, folder, ["csv", "pickle"])
    assert set(written) == set(os.listdir(folder)) and "method_variables.csv" in written
    assert not (tmp_path / "out" / ("run" + ".partial")).exists()
    for name, chrom in run.chromatograms.items():
        stem = rx.clean_name(name)
        curves = pd.read_csv(folder / (stem + ".csv"), low_memory=False)
        for cname, c in chrom.curves.items():
            if c.values is None:
                continue
            col = [h for h in curves.columns if h.startswith(cname + ": value")]
            assert len(col) == 1
            values = curves[col[0]].dropna().to_numpy()
            assert len(values) == len(c.values) and abs(values - c.values).max() < 1e-4
        with open(folder / (stem + ".pkl"), "rb") as f:
            assert pickle.load(f) == {name: run.to_res3_dict()[name]}
        events = pd.read_csv(folder / (stem + "_events.csv"))
        assert len(events) == sum(len(ec.events) for ec in chrom.event_curves.values())
    with pytest.raises(FileExistsError):
        rx.write_run(run, folder, ["csv"])
    assert rx.write_run(run, folder, ["csv"], overwrite=True)
    assert not any(n.endswith(".pkl") for n in os.listdir(folder))  # replaced, not merged


def test_write_run_xlsx(run, tmp_path):
    try:
        rx.excel_engine()
    except ImportError:
        pytest.skip("no xlsxwriter or openpyxl")
    written = rx.write_run(run, tmp_path / "r", ["xlsx"])
    assert written[0] == "%s_Chrom.1.xlsx" % run.name  # apart from other runs' workbooks
    openpyxl = pytest.importorskip("openpyxl")
    book = openpyxl.load_workbook(tmp_path / "r" / written[0])
    # summary and chart first; no pauses, pools or start questions in this run: those sheets are left out, as
    # in csv; one warning (its UV peak table's 1 cm normalization is not known); the tables' titles, as the
    # in-browser page names them (export_tables)
    assert book.sheetnames == ["Summary", "Chart", "Curves", "Events", "Peaks", "Peak table summary", "Phases",
                               "Injections", "Fractions",
                               "Run log sections", "Method variables", "Run and method", "Method steps",
                               "Scouting runs", "Calibration", "Instrument", "System settings", "Audit log",
                               "Column and timeline", "Warnings", "Column guide", "chart_marks"]
    assert book["chart_marks"].sheet_state == "hidden"
    assert book["Curves"].freeze_panes == "A2" and book["Curves"].auto_filter.ref


def test_write_run_extra_tables(run, tmp_path):
    folder = tmp_path / "r"
    written = rx.write_run(run, folder, ["csv"])
    assert {"column_and_timeline.csv", "warnings.csv", "Chrom.1_phases.csv", "Chrom.1_run_log_sections.csv"} <= set(written)
    chrom = run.chromatogram()
    phases = pd.read_csv(folder / "Chrom.1_phases.csv")
    assert list(phases["name"]) == list(chrom.phase_table()["name"])
    assert "+" not in str(phases["start_utc"][0])  # UTC without a zone
    for kind, sheet in (("injection", "injections"), ("fraction", "fractions")):
        expected = len(chrom.table_rows(kind))
        assert (pd.read_csv(folder / ("Chrom.1_%s.csv" % sheet)).shape[0] if expected else 0) == expected
    sections = pd.read_csv(folder / "Chrom.1_run_log_sections.csv")
    assert {"column_flow_ml_min", "percent_b", "system_flow_measured"} <= set(sections.columns)
    assert "inlets" in sections.columns and "inlets_in_use" not in sections.columns  # one column, in-use text
    info = pd.read_csv(folder / "column_and_timeline.csv")
    assert list(info["chromatogram"]) == list(run.chromatograms)
    assert info["vc"][0] == run.column_volume and info["timeline_quality"][0] == run.timeline["quality"]
    assert json.loads(info["vc_candidates"][0])[0]["source"] == run.column_volume_candidates[0].source
    assert not (folder / "Chrom.1_pauses.csv").exists()  # no pause in the sample: no file
    assert list(pd.read_csv(folder / "warnings.csv").columns) == rx.WARNING_COLUMNS
    assert info["pycorn_version"][0].startswith(rx.provenance(None)["version"] + " (commit ")


def test_column_guide_says_unicorn_or_pycorn_for_every_column(run, tmp_path):
    folder = tmp_path / "r"
    written = rx.write_run(run, folder, ["csv"], options=rx.ExportOptions(method_hints=True))
    guide = pd.read_csv(folder / "column_guide.csv").fillna("")
    assert list(guide.columns) == ["table", "csv_file", "table_origin", "column", "origin", "meaning", "check"]
    assert set(guide["origin"]) == {"UNICORN", "PyCORN calculated", "PyCORN analysis", "PyCORN"}
    by = {(t, c): o for t, c, o in zip(guide["table"], guide["column"], guide["origin"])}
    assert by[("Curves", "<curve>: value (<unit>)")] == "UNICORN"
    assert by[("Column and timeline", "retention_zero_time")] == "UNICORN"
    assert by[("Curves", "<curve>: CV")] == "PyCORN analysis"  # Vc is PyCORN's choice
    assert by[("Events", "text")] == "UNICORN" and by[("Events", "utc")] == "PyCORN analysis"
    assert by[("Planned vs run - blocks", "status")] == "PyCORN analysis"
    calculated = guide[guide["origin"] == "PyCORN calculated"]
    assert (calculated["check"] != "").all() and (guide.loc[guide["origin"] != "PyCORN calculated", "check"] == "").all()
    # every column of every csv written is in the guide, under its table's csv name
    for f in written:
        if not f.endswith(".csv") or f == "column_guide.csv":
            continue
        listed = set(guide.loc[guide["csv_file"].str.replace("<chromatogram>", "Chrom.1") == f, "column"])
        columns = pd.read_csv(folder / f, nrows=0).columns
        assert len(columns) and all(rx_generic(f, c) in listed for c in columns), f
    # meanings for every PyCORN column of the PyCORN-built tables
    built = guide[guide["origin"].str.startswith("PyCORN") & ~guide["table"].isin(["Column guide"])]
    assert (built["meaning"] != "").all(), list(built.loc[built["meaning"] == "", "column"])


def rx_generic(f, column):
    from pycorn.export_tables import generic_column
    return generic_column("curves" if f == "Chrom.1.csv" else "", column)


def test_readme_and_provenance_make_the_export_reproducible(run, tmp_path):
    folder = tmp_path / "r"
    options = rx.ExportOptions(method_hints=True, route="CLI: pycorn export",
                               command="pycorn export sample.zip --out o --method-hints")
    written = rx.write_run(run, folder, ["csv"], options=options)
    assert {"README.txt", "provenance.json"} <= set(written)
    prov = json.loads((folder / "provenance.json").read_text(encoding="utf-8"))
    assert prov["app"] == "PyCORN" and prov["repository"] == "https://github.com/traylosj/PyCORN"
    assert prov["route"] == "CLI: pycorn export" and prov["command"] == options.command
    assert prov["options"] == {"method_hints": True} and prov["formats"] == ["csv"] and prov["run"] == run.name
    readme = (folder / "README.txt").read_text(encoding="utf-8")
    assert options.command in readme and "PyCORN %s" % prov["version"] in readme
    for f in written:  # every other file is described
        assert f == "README.txt" or f in readme, f
    info = pd.read_csv(folder / "column_and_timeline.csv")
    assert info["export_route"][0] == "CLI: pycorn export"
    assert json.loads(info["export_options"][0]) == {"method_hints": True}


def test_cli_records_its_command_line(tmp_path):
    out = tmp_path / "o"
    assert cli.main(["export", SAMPLE, "--out", str(out), "--method-hints"]) == 0
    prov = json.loads(next(out.rglob("provenance.json")).read_text(encoding="utf-8"))
    assert prov["route"] == "CLI: pycorn export" and prov["command"].startswith("pycorn export ")
    assert prov["command"].endswith("--method-hints") and prov["options"] == {"method_hints": True}


def test_warnings_frame_splits_tags(run):
    import copy
    r = copy.copy(run)
    r.warnings = ["Chrom.1: UTC: clock gap", "Chrom.1: no retention-zero setting", "loose"]
    w = rx.warnings_frame(r, [("Chrom.1", "sections", "x")])
    assert w.values.tolist() == [["Chrom.1", "run", "UTC", "clock gap"],
                                 ["Chrom.1", "run", "", "no retention-zero setting"],
                                 ["", "run", "", "loose"], ["Chrom.1", "sections", "", "x"]]


def test_extra_table_failure_still_writes_the_run(run, tmp_path, monkeypatch):
    def broken(run, chrom, result=None):
        raise RuntimeError("boom")
    monkeypatch.setattr(rx, "sections_frame", broken)
    written = rx.write_run(run, tmp_path / "r", ["csv"])
    assert "Chrom.1.csv" in written and "Chrom.1_run_log_sections.csv" not in written
    w = pd.read_csv(tmp_path / "r" / "warnings.csv")
    assert any(s == "export" and "sections not written (RuntimeError: boom)" in m
               for s, m in zip(w["source"], w["message"]))


def test_export_files_end_to_end(tmp_path):
    src = tmp_path / "in"
    src.mkdir()
    for name in ("a.zip", "b.zip"):  # the same run twice: both get an id
        shutil.copy(SAMPLE, src / name)
    out = tmp_path / "exports"
    assert cli.main(["export", str(src), "--out", str(out)]) == 0
    index = pd.read_csv(out / "index.csv")
    assert list(index["export_status"]) == ["written", "written"]
    folders = sorted(index["export_folder"])
    assert folders[0] != folders[1] and all(f.startswith("zip") for f in folders)
    assert all((out / f).is_dir() for f in folders)
    assert cli.main(["export", str(src), "--out", str(out)]) == 0
    assert list(pd.read_csv(out / "index.csv")["export_status"]) == ["skipped (exists)"] * 2


def test_export_from_a_file_catalog_csv(tmp_path):
    from pycorn import catalog_from_files
    selection = tmp_path / "files.csv"
    catalog_from_files(SAMPLE).to_csv(selection, index=False)
    out = tmp_path / "exports"
    assert cli.main(["export", "--from", str(selection), "--out", str(out)]) == 0
    index = pd.read_csv(out / "index.csv")
    assert list(index["export_status"]) == ["written"] and index["export_folder"][0].startswith("zip")


def test_export_from_a_mixed_csv(tmp_path, monkeypatch):
    from pycorn import catalog_from_files
    from pycorn.db_connector import PyCORNDatabase
    db = PyCORNDatabase.__new__(PyCORNDatabase)

    def db_rows(self, cat, **kw):  # the database row loads as the sample, marked as read from SQL
        for s, d, r in zip(cat["source"], cat["database"], cat["result_id"]):
            run = open_run(SAMPLE)
            run.source, run.origin = "sql", {"source": s, "database": d, "result_id": int(r)}
            yield run

    monkeypatch.setattr(PyCORNDatabase, "_load_db_rows", db_rows)
    rows = pd.DataFrame({"source": ["a"], "database": ["db"], "result_id": [5], cli.LABEL: ["a#1"],
                         "name": ["from db"], "folder_path": ["/DefaultHome"]})
    selection = tmp_path / "mixed.csv"
    pd.concat([rows, catalog_from_files(SAMPLE)], ignore_index=True).to_csv(selection, index=False)
    out = tmp_path / "exports"
    args = cli.build_parser().parse_args(["export", "--from", str(selection), "--out", str(out)])
    assert cli.run_export(args, lambda: db) == 0
    index = pd.read_csv(out / "index.csv")
    assert list(index["export_status"]) == ["written", "written"]
    assert index["export_folder"][0].startswith("a#1") and index["export_folder"][1].startswith("zip")


def test_export_stores_the_stamp_of_the_run_as_loaded(tmp_path, monkeypatch):
    from pycorn.db_connector import PyCORNDatabase
    from pycorn.model import DbTable
    db = PyCORNDatabase.__new__(PyCORNDatabase)
    loaded = "0000000000000002"  # changed in UNICORN after the search, which saw ...01

    def db_rows(self, cat, **kw):
        run = open_run(SAMPLE)
        run.source, run.origin = "sql", {"source": "a", "database": "db", "result_id": 5}
        run.tables = {"dbo.Result": DbTable("dbo.Result", "owned", {"tstamp": "timestamp"},
                                            [{"tstamp": bytes.fromhex(loaded)}])}
        yield run

    monkeypatch.setattr(PyCORNDatabase, "_load_db_rows", db_rows)
    selection = tmp_path / "runs.csv"
    pd.DataFrame({"source": ["a"], "database": ["db"], "result_id": [5], cli.LABEL: ["a#1"], "name": ["r"],
                  "tstamp": ["0000000000000001"]}).to_csv(selection, index=False)
    out = tmp_path / "exports"
    args = cli.build_parser().parse_args(["export", "--from", str(selection), "--out", str(out)])
    assert cli.run_export(args, lambda: db) == 0
    index = pd.read_csv(out / "index.csv", dtype=str)
    assert index["export_stamp"][0] == loaded
    assert pd.read_csv(out / index["export_folder"][0] / "column_and_timeline.csv", dtype=str)["export_stamp"][0] == loaded
    assert rx.loaded_stamp(open_run(SAMPLE)) is None  # files: their own stamp


def test_cadet_file_not_written_is_incomplete_and_exit_1(tmp_path, capsys):
    # the sample stores no column geometry: the Core input can't be written
    code, index = _export(SAMPLE, tmp_path, "--cadet-core", "6")
    assert code == 1 and list(index["export_status"]) == ["incomplete"]
    assert "not written: CADET-Core 6 input for Chrom.1" in capsys.readouterr().out
    folder = tmp_path / index["export_folder"][0]
    assert (folder / "Chrom.1.csv").exists()  # the rest is still written
    # with the column given it is complete
    code, index = _export(SAMPLE, tmp_path / "given", "--cadet-core", "6", "--bed-height", "2.5 cm",
                          "--diameter", "0.5 cm")
    assert code == 0 and list(index["export_status"]) == ["written"]


def test_cadet_parameter_blocks_from_json_files(tmp_path, capsys):
    # owner, 2026-11-08: one generic block per target, passed on unchanged
    core = tmp_path / "core.json"
    core.write_text(json.dumps({"input": {"model": {"unit_001": {"COL_POROSITY": 0.31, "LIN_KA": [1.0]}}}}))
    proc = tmp_path / "process.json"
    proc.write_text(json.dumps({"flow_sheet": {"column": {"bed_porosity": 0.31}}}))
    out = tmp_path / "out"
    code, index = _export(SAMPLE, out, "--cadet", "--cadet-core", "6", "--bed-height", "2.5 cm", "--diameter",
                          "0.5 cm", "--core-overrides", str(core), "--process-overrides", str(proc))
    assert code == 0
    folder = out / index["export_folder"][0]
    with open(folder / "Chrom.1_core_v6.json", encoding="utf-8") as f:
        written = json.load(f)
    assert written["input"]["model"]["unit_001"]["COL_POROSITY"] == 0.31
    assert written["input"]["model"]["unit_001"]["LIN_KA"] == [1.0]  # a name pycorn doesn't know: unchanged
    with open(folder / "Chrom.1_cadet" / "experiment.json", encoding="utf-8") as f:
        assert json.load(f)["process_overrides"] == {"flow_sheet": {"column": {"bed_porosity": 0.31}}}
    # checked before anything is loaded
    bad = tmp_path / "bad.json"
    bad.write_text("[1, 2]")
    assert cli.main(["export", SAMPLE, "--out", str(tmp_path / "b"), "--cadet", "--process-overrides", str(bad)]) == 2
    assert "one JSON object" in capsys.readouterr().err
    assert cli.main(["export", SAMPLE, "--out", str(tmp_path / "c"), "--cadet", "--core-overrides", str(core)]) == 2
    assert "only used with --cadet-core" in capsys.readouterr().err


def test_export_csvs_carry_a_bom_for_excel(tmp_path):
    code, index = _export(SAMPLE, tmp_path)
    folder = tmp_path / index["export_folder"][0]
    for path in (folder / "column_and_timeline.csv", folder / "Chrom.1.csv", tmp_path / "index.csv"):
        assert path.read_bytes()[:3] == b"\xef\xbb\xbf", path.name
    assert not pd.read_csv(folder / "column_and_timeline.csv").columns[0].startswith("﻿")  # pandas skips it


def test_read_selection_rejects_rows_without_database_or_path(tmp_path):
    selection = tmp_path / "bad.csv"
    pd.DataFrame({"source": ["a", "zip"], "database": ["db", None], "result_id": [1, None],
                  "path": [None, None]}).to_csv(selection, index=False)
    with pytest.raises(ValueError, match="neither"):
        cli.read_selection(selection)


def test_export_needs_one_input(tmp_path, capsys):
    assert cli.main(["export", "--out", str(tmp_path)]) == 2
    assert "error:" in capsys.readouterr().err
    assert cli.main(["export", SAMPLE, "--out", str(tmp_path), "--format", "docx"]) == 2


# --- plain-language messages (ROADMAP item 5) ------------------------------------

def test_files_not_read_exit_1_with_the_reason(tmp_path, capsys):
    import zipfile
    (tmp_path / "bad.zip").write_text("not a zip")
    with zipfile.ZipFile(tmp_path / "other.zip", "w") as z:
        z.writestr("readme.txt", "hello")
    for name, reason in (("bad.zip", "not a zip file, or the file is damaged"),
                         ("other.zip", "not a UNICORN result")):
        assert cli.main(["export", str(tmp_path / name), "--out", str(tmp_path / "out")]) == 1
        captured = capsys.readouterr()
        assert "warning: %s not read: %s" % (name, reason) in captured.err
        assert "done: 1 file(s) not read" in captured.out
    assert cli.main(["bypass-uv", str(tmp_path / "bad.zip")]) == 1
    assert "incomplete: 1 file(s) not read" in capsys.readouterr().out


def test_epsilon_at_a_missing_wavelength_is_incomplete(tmp_path, capsys):
    code, index = _export(SAMPLE, tmp_path, "--epsilon", "999=1.4")
    assert code == 1 and list(index["export_status"]) == ["incomplete"]
    assert "not written: mg/ml at 999 nm for Chrom.1" in capsys.readouterr().out


def test_plain_errors_for_files_and_folders(tmp_path, capsys):
    assert cli.main(["export", "--from", str(tmp_path / "missing.csv"), "--out", str(tmp_path / "o")]) == 2
    assert capsys.readouterr().err.strip() == "error: --from: no such file: %s" % (tmp_path / "missing.csv")
    (tmp_path / "afile").write_text("x")
    assert cli.main(["export", SAMPLE, "--out", str(tmp_path / "afile")]) == 2
    assert "is a file: give a folder" in capsys.readouterr().err


def test_unexpected_error_is_one_line_and_debug_shows_the_traceback(tmp_path, capsys, monkeypatch):
    assert cli.main(["export", "--out", str(tmp_path), "--debug"]) == 2  # a worded error, with its traceback
    err = capsys.readouterr().err
    assert "Traceback" in err and "error: export: give --from" in err

    def broken(args, db_factory):
        raise TypeError("something nobody worded")
    monkeypatch.setattr(cli, "run_export", broken)
    assert cli.main(["export", SAMPLE, "--out", str(tmp_path)]) == 3
    err = capsys.readouterr().err
    assert "unexpected error (TypeError: something nobody worded)" in err and "--debug" in err
    assert "Traceback" not in err
    for argv in (["--debug", "export", SAMPLE, "--out", str(tmp_path)],
                 ["export", SAMPLE, "--out", str(tmp_path), "--debug"]):  # before or after the command
        assert cli.main(argv) == 3
        assert "Traceback" in capsys.readouterr().err


def test_find_says_which_source_was_not_reached_and_exits_1(tmp_path, capsys):
    from pycorn.db_connector import PyCORNDatabase
    path = tmp_path / ".env"
    path.write_text("UNICORN_SOURCES=lab2\nUNICORN_LAB2_SERVER=db.example.invalid\nUNICORN_LAB2_DATABASES=X\n")
    db = PyCORNDatabase(env_file=str(path))

    def refuse(source=None):
        raise ConnectionError("cannot reach the SQL Server: check the server name")
    db._connect = refuse
    args = cli.build_parser().parse_args(["find"])
    assert cli.run_find(args, db) == 1
    captured = capsys.readouterr()
    assert "0 result(s) match in the databases reached." in captured.out
    assert "not searched: source 'lab2': cannot reach the SQL Server" in captured.err
    assert "pycorn-setup --test-only" in captured.err and "db.example.invalid" not in captured.err + captured.out


# --- opt-in export flags ---------------------------------------------------------

def _options(*argv):
    return cli.export_options(cli.build_parser().parse_args(["export", SAMPLE, "--out", "x"] + list(argv)))


def test_parse_export_options():
    assert cli.parse_units(["volume=L,flow=L/h", "time=h"]) == {"volume": "L", "flow": "L/h", "time": "h"}
    assert cli.parse_quantity("10 cm", "--bed-height", "m") == (10.0, "cm")
    assert cli.parse_per_wavelength(["280=1.4", "260=2 ml/(mg*cm)"], "--epsilon", "ml/(mg*cm)") == {
        280.0: (1.4, "ml/(mg*cm)"), 260.0: (2.0, "ml/(mg*cm)")}
    o = _options("--epsilon-molar", "280=43824", "--mw", "66000 g/mol", "--path-length", "0.2",
                 "--bed-height", "10 cm", "--diameter", "1 cm", "--cadet-core", "5")
    assert o.epsilon_molar == {280.0: (43824.0, "1/(M*cm)")} and o.molecular_weight == (66000.0, "g/mol")
    assert o.path_length_cm == 0.2 and o.bed_height == (10.0, "cm") and o.cadet_core == "5"
    assert o.core_format == "json" and not o.override_geometry


@pytest.mark.parametrize("argv", [
    ["--units", "speed=m/s"], ["--units", "volume=min"], ["--units", "volume"],
    ["--bed-height", "10"], ["--bed-height", "10 ml"], ["--diameter", "-1 cm"], ["--path-length", "0"],
    ["--epsilon", "x=1"], ["--epsilon", "280=1.4", "--epsilon", "280=1.5"],
    ["--epsilon", "280=1.4", "--epsilon-molar", "280=43824", "--mw", "66000"],
    ["--epsilon-molar", "280=43824"], ["--override-geometry"],
    ["--core-format", "hdf5"],
])
def test_export_options_rejected_before_loading(argv, tmp_path, capsys):
    assert cli.main(["export", SAMPLE, "--out", str(tmp_path / "o")] + argv) == 2
    assert "error:" in capsys.readouterr().err
    assert not (tmp_path / "o").exists()  # nothing loaded or written


def test_core_format_hdf5_needs_h5py(tmp_path, monkeypatch, capsys):
    import importlib.util
    real = importlib.util.find_spec
    monkeypatch.setattr(importlib.util, "find_spec", lambda name, *a: None if name == "h5py" else real(name, *a))
    assert cli.main(["export", SAMPLE, "--out", str(tmp_path), "--cadet-core", "6", "--core-format", "hdf5"]) == 2
    assert "h5py" in capsys.readouterr().err


def _warnings(folder):
    return pd.read_csv(folder / "warnings.csv").fillna("")


def test_export_units(run, tmp_path):
    folder = tmp_path / "r"
    rx.write_run(run, folder, ["csv"], options=rx.ExportOptions(units={"volume": "L"}))
    curves = pd.read_csv(folder / "Chrom.1.csv", low_memory=False)
    uv = run.chromatogram().curve("UV 1_280")
    col = "UV 1_280: volume (L)"
    assert col in curves.columns
    assert abs(curves[col].dropna().to_numpy() - uv.volume / 1000).max() < 1e-9
    info = pd.read_csv(folder / "column_and_timeline.csv")
    assert info["vc_unit"][0] == "L" and abs(info["vc"][0] - run.column_volume / 1000) < 1e-12
    phases = pd.read_csv(folder / "Chrom.1_phases.csv")
    raw = run.chromatogram().phase_table()
    assert abs(phases["start_volume"].iloc[-1] - raw["start_volume"].iloc[-1] / 1000) < 1e-9
    assert run.chromatogram().curve("UV 1_280").volume_unit == "ml"  # the loaded run is unchanged


def test_export_epsilon_per_wavelength(run, tmp_path):
    folder = tmp_path / "r"
    options = rx.ExportOptions(epsilon={280.0: (1.4, "ml/(mg*cm)"), 999.0: (1.0, "ml/(mg*cm)")},
                               epsilon_molar={295.0: (43824.0, "1/(M*cm)")}, molecular_weight=(66000.0, "g/mol"))
    rx.write_run(run, folder, ["csv"], options=options)
    curves = pd.read_csv(folder / "Chrom.1.csv", low_memory=False)
    uv = run.chromatogram().curve("UV 1_280")
    path, _ = uv_path(uv)
    got = curves["UV 1_280 (mg/ml): value (mg/ml)"].dropna().to_numpy()
    assert abs(got - uv.values / 1000 / (1.4 * path)).max() < 1e-9
    assert "UV 1_280 (mg/ml): volume (ml)" in curves.columns and "UV 1_280 (mg/ml): CV" in curves.columns
    assert "UV 2_295 (mg/ml): value (mg/ml)" in curves.columns
    assert not any(c.startswith("UV 3_0 (mg/ml)") for c in curves.columns)  # 0 nm: off
    conc = pd.read_csv(folder / "Chrom.1_concentration.csv")
    assert list(conc["curve"]) == ["UV 1_280", "UV 2_295"] and set(conc["wavelength_source"]) == {"run_log"}
    assert abs(conc["epsilon_ml_per_mg_cm"][1] - 43824 / 66000) < 1e-12
    w = _warnings(folder)
    assert any(s == "concentration" and m.startswith("no UV curve at 999 nm") for s, m in zip(w["source"], w["message"]))
    # issue #1: every mg/ml curve gets the accuracy note; no response factor unless given
    notes = [m for s, m in zip(w["source"], w["message"]) if s == "concentration" and "is an estimate" in m]
    assert len(notes) == 2 and conc["response_factor"].isna().all()


def test_export_uv_response(run, tmp_path):
    folder = tmp_path / "r"
    options = rx.ExportOptions(epsilon={280.0: (1.4, "ml/(mg*cm)")}, uv_response=0.92)
    rx.write_run(run, folder, ["csv"], options=options)
    uv = run.chromatogram().curve("UV 1_280")
    path, _ = uv_path(uv)
    curves = pd.read_csv(folder / "Chrom.1.csv", low_memory=False)
    got = curves["UV 1_280 (mg/ml): value (mg/ml)"].dropna().to_numpy()
    assert np.allclose(got, uv.values.astype(float) / 1000 / (1.4 * 0.92 * path), rtol=1e-9, atol=1e-12)
    conc = pd.read_csv(folder / "Chrom.1_concentration.csv")
    assert conc["response_factor"][0] == 0.92
    # without an extinction coefficient the factor is unused: a note
    rx.write_run(run, tmp_path / "s", ["csv"], options=rx.ExportOptions(uv_response=0.92))
    w = _warnings(tmp_path / "s")
    assert any(m.startswith("--uv-response not used") for m in w["message"])
    with pytest.raises(ValueError, match="above 0"):
        cli.parse_factor("0", "--uv-response")


def test_export_bypass_uv(tmp_path):
    """--bypass-uv: a synthetic run with a by-pass plateau, then the sample (which has none)."""
    from test_uv_calibration import _events, _plateau, _run, _uv
    synthetic = _run(_events(), curves=[_uv(_plateau)])
    from pycorn.uv_calibration import bypass_uv_readings
    problems = []
    frame = rx.bypass_uv_frame(synthetic, synthetic.chromatogram(),
                               rx.ExportOptions(bypass_uv=True, sample_concentration=(2.0, "mg/ml")), problems)
    assert list(frame["kind"]) == ["plateau"] and frame["epsilon_ml_per_mg_cm"][0] == pytest.approx(2.5)
    assert frame["epsilon_ml_per_mg_cm"][0] == bypass_uv_readings(synthetic, sample_concentration=2.0)[0][
        "epsilon_ml_per_mg_cm"]
    assert any("is an estimate" in m for _, _, m in problems)
    args = cli.build_parser().parse_args(["export", "x.zip", "--out", "o", "--sample-conc", "1.2 mg/ml",
                                          "--loop-volume", "20 ul", "--absorbance-limit", "2500"])
    options = cli.export_options(args)
    assert options.bypass_uv and options.sample_concentration == (1.2, "mg/ml")
    assert options.loop_volume == (20.0, "ul") and options.absorbance_limit == (2500.0, "mAU")


def test_bypass_uv_command(tmp_path, capsys):
    out = tmp_path / "bypass.csv"
    assert cli.main(["bypass-uv", SAMPLE, "--sample-conc", "1 mg/ml", "--csv", str(out)]) == 0
    assert "reading(s) of sample sent to the column by-pass" in capsys.readouterr().out
    assert "epsilon_ml_per_mg_cm" in pd.read_csv(out).columns
    assert cli.main(["bypass-uv", "--sample-conc", "1 mg/ml"]) == 2  # no input


def uv_path(curve):
    from pycorn.units import uv_path_length_cm
    return uv_path_length_cm(curve)


def _without_uv_path(run, name):
    """A copy of `run` whose curve `name` has no CurveUVInfo (the run gives no path)."""
    import copy
    import dataclasses
    r = copy.copy(run)
    r.chromatograms = {k: copy.copy(c) for k, c in run.chromatograms.items()}
    chrom = r.chromatogram()
    chrom.curves = dict(chrom.curves)
    chrom.curves[name] = dataclasses.replace(chrom.curves[name], uv_info=None)
    return r


def test_path_length_only_where_the_run_gives_none(run, tmp_path):
    from pycorn.concentration import concentration
    r = _without_uv_path(run, "UV 1_280")
    with pytest.raises(ValueError, match="no path length"):
        concentration(r.chromatogram().curve("UV 1_280"), epsilon=1.0)
    filled = concentration(r.chromatogram().curve("UV 1_280"), epsilon=1.0, fallback_path_cm=1.0, fallback_basis="b")
    assert filled.conversion["path_length_cm"] == 1.0 and filled.conversion["path_length_basis"] == "b"
    kept = concentration(run.chromatogram().curve("UV 1_280"), epsilon=1.0, fallback_path_cm=1.0)
    assert kept.conversion["path_length_basis"] == "NominalUVPathLength"  # stored path wins
    folder = tmp_path / "r"
    options = rx.ExportOptions(epsilon={280.0: (1.0, "ml/(mg*cm)"), 295.0: (1.0, "ml/(mg*cm)")}, path_length_cm=1.0)
    rx.write_run(r, folder, ["csv"], options=options)
    conc = pd.read_csv(folder / "Chrom.1_concentration.csv").set_index("curve")
    assert conc.loc["UV 1_280", "path_length_basis"] == rx.PATH_BASIS and conc.loc["UV 1_280", "path_length_cm"] == 1.0
    assert conc.loc["UV 2_295", "path_length_basis"] == "NominalUVPathLength"
    w = _warnings(folder)
    filled_lines = [m for s, m in zip(w["source"], w["message"]) if s == "concentration" and "no UV path length" in m]
    assert len(filled_lines) == 1 and filled_lines[0].startswith("UV 1_280:")


def test_path_length_alone_is_noted(run, tmp_path):
    rx.write_run(run, tmp_path / "r", ["csv"], options=rx.ExportOptions(path_length_cm=1.0))
    assert any("--path-length not used" in m for m in _warnings(tmp_path / "r")["message"])


def test_export_method_hints(run, tmp_path):
    from pycorn.method_hints import method_hints
    folder = tmp_path / "r"
    written = rx.write_run(run, folder, ["csv"], options=rx.ExportOptions(method_hints=True))
    hints = method_hints(run, run.chromatogram())
    assert hints.available and hints.blocks
    blocks = pd.read_csv(folder / "Chrom.1_planned_vs_run_blocks.csv")
    assert list(blocks["block"]) == [b["block"] for b in hints.blocks]
    assert ("Chrom.1_planned_vs_run_injections.csv" in written) == bool(hints.injections)
    assert "Chrom.1_planned_vs_run_blocks.csv" not in rx.write_run(run, tmp_path / "plain", ["csv"])  # opt-in


def test_export_caller_geometry_fallback_and_override(run, tmp_path):
    folder = tmp_path / "r"
    options = rx.ExportOptions(bed_height=(10.0, "cm"), diameter=(0.3, "cm"), particle_diameter=(34.0, "um"))
    rx.write_run(run, folder, ["csv"], options=options)
    info = pd.read_csv(folder / "column_and_timeline.csv")
    assert json.loads(info["geometry_sources"][0]) == {"length": "fallback (--bed-height)",
                                                       "diameter": "fallback (--diameter)",
                                                       "particle_diameter": "fallback (--particle-diameter)"}
    assert abs(info["column_length_m"][0] - 0.1) < 1e-12 and abs(info["particle_radius_m"][0] - 17e-6) < 1e-12
    assert info["vc_source"][0] == run.column_volume_source  # stored Vc kept
    w = _warnings(folder)
    assert sum(s == "geometry" and "given" in m for s, m in zip(w["source"], w["message"])) == 3


def test_export_vc_from_caller_geometry_last_resort(run, tmp_path, monkeypatch):
    import copy
    from pycorn import column_volume
    real = column_volume.collect_candidates
    # a run with no stored Vc source: only the caller's geometry is left
    monkeypatch.setattr(column_volume, "collect_candidates",
                        lambda *a, **k: [c for c in real(*a, **k) if c.source == "geometry"])
    r = copy.deepcopy(run)
    from pycorn.axes import add_axes
    add_axes(r)
    assert r.column_volume is None
    folder = tmp_path / "r"
    rx.write_run(r, folder, ["csv"], options=rx.ExportOptions(bed_height=(10.0, "cm"), diameter=(0.3, "cm")))
    info = pd.read_csv(folder / "column_and_timeline.csv")
    expected = 3.141592653589793 * 0.3 ** 2 / 4 * 10.0
    assert info["vc_source"][0] == "geometry" and abs(info["vc"][0] - expected) < 1e-9
    assert "--bed-height 10 cm" in info["vc_location"][0]
    curves = pd.read_csv(folder / "Chrom.1.csv", low_memory=False)
    assert curves["UV 1_280: CV"].notna().any()
    w = _warnings(folder)
    assert any(s == "geometry" and "last resort" in m for s, m in zip(w["source"], w["message"]))
    assert any(s == "run" and c == "Vc" and "column geometry given" in m
               for s, c, m in zip(w["source"], w["category"], w["message"]))


def test_export_cadet_and_core(run, tmp_path):
    folder = tmp_path / "r"
    options = rx.ExportOptions(cadet=True, cadet_core="5", bed_height=(10.0, "cm"), diameter=(0.3, "cm"),
                               epsilon={280.0: (1.4, "ml/(mg*cm)")})
    written = rx.write_run(run, folder, ["csv"], options=options)
    assert "Chrom.1_cadet/" in written and "Chrom.1_core_v5.json" in written
    with open(folder / "Chrom.1_cadet" / "experiment.json", encoding="utf-8") as f:
        assert f.read()
    with open(folder / "Chrom.1_core_v5.json", encoding="utf-8") as f:
        core = json.load(f)
    assert core["meta"]["CORE_VERSION"] == "5"
    w = _warnings(folder)
    assert {"cadet", "cadet_core"} <= set(w["source"])
    assert not any(s == "cadet" and "for the primary UV wavelength" in m for s, m in zip(w["source"], w["message"]))


def test_parse_system_volume_options():
    o = _options("--cadet-core", "6", "--mixer-volume", "0.6 ml", "--tubing-volume", "200 ul",
                 "--sample-inlets", "a5,B2", "--sample-inlets", "A5", "--no-system-volumes")
    assert o.mixer_volume == (0.6, "ml") and o.tubing_volume == (200.0, "ul")
    assert o.sample_inlets == ["A5", "B2"] and o.system_volumes is False
    o = _options("--cadet")
    assert o.mixer_volume is None and o.sample_inlets is None and o.system_volumes is True


@pytest.mark.parametrize("argv", [
    ["--tubing-volume", "0.2 ml"], ["--mixer-volume", "0.6 ml"], ["--sample-inlets", "A5"],
    ["--no-system-volumes"], ["--cadet", "--tubing-volume", "0.2 cm"],
    ["--cadet", "--mixer-volume", "0"], ["--cadet", "--sample-inlets", "5A"],
])
def test_system_volume_options_rejected(argv, tmp_path, capsys):
    assert cli.main(["export", SAMPLE, "--out", str(tmp_path / "o")] + argv) == 2
    assert "error:" in capsys.readouterr().err
    assert not (tmp_path / "o").exists()


def test_export_system_volumes(run, tmp_path):
    geometry = dict(cadet=True, cadet_core="6", bed_height=(10.0, "cm"), diameter=(0.3, "cm"))
    folder = tmp_path / "r"
    rx.write_run(run, folder, ["csv"], options=rx.ExportOptions(
        mixer_volume=(0.6, "ml"), tubing_volume=(0.2, "ml"), sample_inlets=["Z9"], **geometry))
    with open(folder / "Chrom.1_cadet" / "experiment.json", encoding="utf-8") as f:
        sidecar = json.load(f)
    system = sidecar["system"]
    assert {"mixer", "tubing"} <= {u[0] for u in sidecar["process_inputs"]["units"]}
    assert system["mixer_source"] == "caller" and system["tubing_source"] == "caller"
    assert abs(system["tubing_m3"] - 0.2e-6) < 1e-12
    with open(folder / "Chrom.1_core_v6.json", encoding="utf-8") as f:
        names = json.load(f)["meta"]["UNIT_NAMES"]
    assert {"mixer", "tubing"} <= set(names)
    assert not any("tubing volume" in m for m in _warnings(folder)["message"])
    assert any("sample_inlets Z9: no section draws from them" in m for m in _warnings(folder)["message"])

    folder = tmp_path / "none"
    rx.write_run(run, folder, ["csv"], options=rx.ExportOptions(system_volumes=False, **geometry))
    with open(folder / "Chrom.1_core_v6.json", encoding="utf-8") as f:
        names = json.load(f)["meta"]["UNIT_NAMES"]
    assert not {"mixer", "tubing"} & set(names)
    with open(folder / "Chrom.1_cadet" / "experiment.json", encoding="utf-8") as f:
        assert not {"mixer", "tubing"} & {u[0] for u in json.load(f)["process_inputs"]["units"]}
    assert any(s == "cadet" and "tubing volume" in m for s, m in zip(_warnings(folder)["source"],
                                                                   _warnings(folder)["message"]))


def test_export_core_without_geometry_still_writes_the_run(run, tmp_path):
    folder = tmp_path / "r"
    written = rx.write_run(run, folder, ["csv"], options=rx.ExportOptions(cadet_core="6"))
    assert "Chrom.1.csv" in written and not any(n.startswith("Chrom.1_core") for n in written)
    w = _warnings(folder)
    assert any(s == "cadet_core" and "--bed-height" in m for s, m in zip(w["source"], w["message"]))


def test_export_flags_end_to_end(tmp_path):
    out = tmp_path / "o"
    assert cli.main(["export", SAMPLE, "--out", str(out), "--units", "volume=L", "--epsilon", "280=1.4",
                     "--method-hints", "--cadet", "--bed-height", "10 cm", "--diameter", "0.3 cm"]) == 0
    index = pd.read_csv(out / "index.csv")
    files = index["export_files"][0].split(";")
    assert {"Chrom.1_concentration.csv", "Chrom.1_planned_vs_run_blocks.csv", "Chrom.1_cadet/"} <= set(files)


# --- review fixes (docs/ROADMAP.md, "Fix plan (export review)") ------------------

def test_pickle_only_still_writes_the_run_files(run, tmp_path):
    folder = tmp_path / "r"
    options = rx.ExportOptions(cadet=True, epsilon={280.0: (1.4, "ml/(mg*cm)")}, method_hints=True)
    written = rx.write_run(run, folder, ["pickle"], options=options)
    assert {"Chrom.1.pkl", "Chrom.1_cadet/", "method_variables.csv", "column_and_timeline.csv", "warnings.csv"} <= set(written)
    assert not any(n.startswith("Chrom.1_") and n.endswith(".csv") for n in written)  # no tables
    w = _warnings(folder)
    sources = set(w["source"])
    assert "cadet" in sources  # the CADET notes are kept
    notes = [m for s, m in zip(w["source"], w["message"]) if s == "export"]
    assert any(m.startswith("--epsilon not used") for m in notes)
    assert any(m.startswith("--method-hints not used") for m in notes)


def test_failed_run_leaves_no_partial_folder(run, tmp_path, monkeypatch):
    try:
        rx.excel_engine()
    except ImportError:
        pytest.skip("no xlsxwriter or openpyxl")
    monkeypatch.setattr(rx, "EXCEL_ROWS", 2)
    with pytest.raises(ValueError, match="Excel sheet"):
        rx.write_run(run, tmp_path / "r", ["csv", "xlsx"])
    assert os.listdir(tmp_path) == []  # neither the run folder nor 'r.partial'


def _export(src, out, *flags):
    code = cli.main(["export", str(src), "--out", str(out)] + list(flags))
    return code, pd.read_csv(out / "index.csv", dtype={"export_changed": str}).fillna("")


def _repack(path):
    """Rewrite a .zip uncompressed: the same members (and run), other bytes."""
    import zipfile
    with zipfile.ZipFile(path) as z:
        members = [(i, z.read(i)) for i in z.infolist()]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_STORED) as z:
        for info, data in members:
            info.compress_type = zipfile.ZIP_STORED
            z.writestr(info, data)


def test_same_stamp_hashes_only_when_size_or_time_differ(tmp_path):
    f = tmp_path / "f.bin"
    f.write_bytes(b"abcd")
    row = {"path": str(f)}
    full = rx.row_stamp(row, content=True)
    st = os.stat(f)
    assert rx.same_stamp(full, rx.row_stamp(row), row) == (True, full)  # equal size and time: no read
    os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns + 10 ** 9))
    assert rx.same_stamp(full, rx.row_stamp(row), row)[0]  # touched, same content
    f.write_bytes(b"abce")  # same size, other content
    same, now = rx.same_stamp(full, rx.row_stamp(row), row)
    assert not same and now == rx.row_stamp(row, content=True)
    assert not rx.same_stamp("%d:1" % st.st_size, rx.row_stamp(row), row)[0]  # old stamp, no hash: time decides
    assert rx.same_stamp("0x01", "0x01", {}) == (True, "0x01") and not rx.same_stamp("0x01", "0x02", {})[0]


def test_update_exports_changed_runs_again(tmp_path):
    src = tmp_path / "a.zip"
    shutil.copy(SAMPLE, src)
    out = tmp_path / "o"
    code, index = _export(src, out)
    folder = out / index["export_folder"][0]
    stamp = index["export_stamp"][0]
    st = os.stat(src)
    assert code == 0 and stamp == "%d:%d:%s" % (st.st_size, st.st_mtime_ns, rx._sha256(src))
    assert pd.read_csv(folder / "column_and_timeline.csv", dtype=str)["export_stamp"][0] == stamp
    code, index = _export(src, out)
    assert list(index["export_status"]) == ["skipped (exists)"] and list(index["export_changed"]) == [""]
    os.utime(src, ns=(st.st_atime_ns, st.st_mtime_ns + 10 ** 9))  # touched: same content
    code, index = _export(src, out)
    assert list(index["export_status"]) == ["skipped (exists)"]
    assert index["export_stamp"][0] == rx.row_stamp({"path": str(src)}, content=True) != stamp  # now: same hash
    _repack(src)  # the same run, other bytes: the file changed
    code, index = _export(src, out)
    assert list(index["export_status"]) == ["skipped (changed since export)"]
    assert list(index["export_changed"]) == ["yes"]
    code, index = _export(src, out, "--update")
    assert code == 0 and list(index["export_status"]) == ["written"] and list(index["export_changed"]) == ["yes"]
    assert list(_export(src, out, "--update")[1]["export_status"]) == ["skipped (exists)"]


def test_update_reexports_folders_without_a_stamp(tmp_path):
    out = tmp_path / "o"
    code, index = _export(SAMPLE, out)
    info = out / index["export_folder"][0] / "column_and_timeline.csv"
    pd.read_csv(info).drop(columns=["export_stamp"]).to_csv(info, index=False)  # as written before stamps
    code, index = _export(SAMPLE, out)
    assert list(index["export_status"]) == ["skipped (exists, not checked)"]
    assert list(index["export_changed"]) == ["unknown"]
    code, index = _export(SAMPLE, out, "--update")
    assert list(index["export_status"]) == ["written"]
    assert "export_stamp" in pd.read_csv(info).columns


def test_xlsx_only_folders_keep_the_stamp_in_the_workbook(tmp_path):
    # W4: no csv in the folder; the stamp is read from the workbook's own sheet (an .xlsx is a zip), as before
    # from column_and_timeline.csv, which older folders still hold
    src = tmp_path / "a.zip"
    shutil.copy(SAMPLE, src)
    out = tmp_path / "o"
    code, index = _export(src, out, "--format", "xlsx")
    folder = out / index["export_folder"][0]
    assert code == 0 and sorted(p.suffix for p in folder.iterdir()) == [".txt", ".xlsx"]
    stamp = index["export_stamp"][0]
    assert rx.stored_stamp(folder) == (True, stamp)
    code, index = _export(src, out, "--format", "xlsx")
    assert list(index["export_status"]) == ["skipped (exists)"] and list(index["export_changed"]) == [""]
    _repack(src)
    code, index = _export(src, out, "--format", "xlsx")
    assert list(index["export_status"]) == ["skipped (changed since export)"]
    assert list(_export(src, out, "--format", "xlsx", "--update")[1]["export_status"]) == ["written"]
    # a workbook with no stamp column, or none at all, is 'not checked'
    book = next(folder.glob("*.xlsx"))
    book.write_bytes(b"not a zip")
    assert rx.stored_stamp(folder) == (False, None)
    for p in folder.glob("*"):
        p.unlink()
    assert rx.stored_stamp(folder) == (False, None)


def test_run_level_csvs_only_with_csv_among_the_formats(tmp_path):
    out = tmp_path / "o"
    assert cli.main(["export", SAMPLE, "--out", str(out), "--format", "xlsx,csv"]) == 0
    folder = next(out.rglob("README.txt")).parent
    assert (folder / "warnings.csv").exists() and (folder / "provenance.json").exists()
    assert any(p.suffix == ".xlsx" for p in folder.iterdir())
    out = tmp_path / "p"
    assert cli.main(["export", SAMPLE, "--out", str(out), "--format", "xlsx"]) == 0
    readme = next(out.rglob("README.txt")).read_text(encoding="utf-8")
    assert ".csv" not in readme.split("Files", 1)[1].split("Conventions")[0] and "Column guide" in readme


def test_cadet_files_say_the_export_is_experimental(tmp_path, capsys):
    # W15: a line in warnings.csv and in the command's output whenever CADET files are written
    from pycorn.exporters.cadet.api import CADET_EXPERIMENTAL_NOTE
    out = tmp_path / "o"
    assert cli.main(["export", SAMPLE, "--out", str(out), "--cadet"]) in (0, 1)
    assert "note: " + CADET_EXPERIMENTAL_NOTE in capsys.readouterr().out
    folder = next(out.rglob("README.txt")).parent
    warns = pd.read_csv(folder / "warnings.csv")
    assert (warns["message"] == CADET_EXPERIMENTAL_NOTE).sum() == 1
    out = tmp_path / "n"
    assert cli.main(["export", SAMPLE, "--out", str(out)]) == 0
    assert CADET_EXPERIMENTAL_NOTE not in capsys.readouterr().out


def test_update_and_overwrite_together_rejected(tmp_path, capsys):
    assert cli.main(["export", SAMPLE, "--out", str(tmp_path), "--update", "--overwrite"]) == 2
    assert "--update" in capsys.readouterr().err


def test_read_selection_drops_repeated_runs(tmp_path):
    from pycorn import catalog_from_files
    rows = pd.DataFrame({"source": ["a", "a", "a"], "database": ["db", "DB", "db"], "result_id": [5, 5, 6]})
    files = catalog_from_files(SAMPLE)
    selection = tmp_path / "s.csv"
    pd.concat([rows, files, files], ignore_index=True).to_csv(selection, index=False)
    with pytest.warns(UserWarning, match="2 row"):  # the 'DB' spelling and the second file row
        frame = cli.read_selection(selection)
    assert len(frame) == 3 and list(frame["result_id"].iloc[:2]) == [5, 6]


class _FakeFindDb:
    """One result, 'r' in database 'db' of source 'a'."""

    def database_labels(self, sources, strict=False):
        return {("a", "db"): "a#1"}

    def count(self, **kw):
        return 1

    def _find_page(self, limit, after, sources, strict, filters, dedupe):
        return pd.DataFrame({"source": ["a"], "database": ["db"], "result_id": [1], "name": ["r"]}), False, None


def test_find_csv_creates_its_folder(tmp_path):
    path = tmp_path / "new" / "deeper" / "runs.csv"
    args = cli.build_parser().parse_args(["find", "--csv", str(path)])
    assert cli.run_find(args, _FakeFindDb()) == 0
    assert pd.read_csv(path)["db_label"][0] == "a#1"


def test_find_show_0_prints_no_table(tmp_path, capsys):
    assert cli.run_find(cli.build_parser().parse_args(["find", "--show", "0"]), _FakeFindDb()) == 0
    assert capsys.readouterr().out.splitlines() == ["1 result(s) match."]


def test_mw_with_mass_epsilon_only_for_the_cadet_export(run, tmp_path):
    # D29: CADET works in mol/m^3, so --mw also goes with --epsilon when the CADET export is on
    o = _options("--epsilon", "280=1.4", "--mw", "66 kg/mol", "--cadet")
    assert o.molecular_weight == (66.0, "kg/mol") and o.epsilon == {280.0: (1.4, "ml/(mg*cm)")}
    assert _options("--epsilon", "280=1.4", "--mw", "66000", "--cadet-core", "6").molecular_weight[0] == 66000.0
    # D29 (3): --mw that nothing uses is a note, not an error
    assert _options("--mw", "66000").molecular_weight == (66000.0, "g/mol")
    rx.write_run(run, tmp_path / "o", ["csv"], options=rx.ExportOptions(
        epsilon={280.0: (1.4, "ml/(mg*cm)")}, molecular_weight=(66000.0, "g/mol")))
    w = _warnings(tmp_path / "o")
    assert any(s == "export" and m.startswith("--mw not used") for s, m in zip(w["source"], w["message"]))


def test_cadet_export_warns_on_several_chromatograms(run, tmp_path):
    # D28 (6), amended: warn and write them all until users settle on a rule
    import copy
    two = copy.copy(run)
    two.chromatograms = dict(run.chromatograms)
    two.chromatograms["Chrom.2"] = run.chromatograms["Chrom.1"]
    written = rx.write_run(two, tmp_path / "r", ["csv"], options=rx.ExportOptions(cadet=True))
    assert "Chrom.1_cadet/" in written and "Chrom.2_cadet/" in written
    w = _warnings(tmp_path / "r")
    assert any(s == "cadet" and "2 chromatograms selected (Chrom.1, Chrom.2): CADET files written for all" in m
               for s, m in zip(w["source"], w["message"]))
    rx.write_run(run, tmp_path / "one", ["csv"], options=rx.ExportOptions(cadet=True))
    assert not any("chromatograms" in m for m in _warnings(tmp_path / "one")["message"])


def test_cadet_export_mol_per_m3_from_mass_epsilon(run, tmp_path):
    eps = {280.0: (1.4, "ml/(mg*cm)")}
    rx.write_run(run, tmp_path / "mw", ["csv"], options=rx.ExportOptions(
        cadet=True, epsilon=eps, molecular_weight=(66000.0, "g/mol")))
    assert "UV_1_280_mol_m_3.csv" in os.listdir(tmp_path / "mw" / "Chrom.1_cadet")
    assert not any("--mw" in m for m in _warnings(tmp_path / "mw")["message"])
    rx.write_run(run, tmp_path / "none", ["csv"], options=rx.ExportOptions(cadet=True, epsilon=eps))
    assert "UV_1_280_mol_m_3.csv" not in os.listdir(tmp_path / "none" / "Chrom.1_cadet")
    w = _warnings(tmp_path / "none")
    assert any(s == "cadet" and "--epsilon without --mw" in m for s, m in zip(w["source"], w["message"]))
