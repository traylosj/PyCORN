# -*- coding: utf-8 -*-
"""
Machine-readable output of the `pycorn` command (Track 9, A4): --json, --jsonl, --csv -,
structured errors, masking of people on stdout, the database named by its label only.
"""
import io
import json
import os
import re

import pandas as pd
import pytest

from pycorn import catalog_from_files, cli, db_connector
from pycorn.db_connector import ENV_FILE_VAR, PyCORNDatabase
from pycorn.json_output import (SCHEMA_VERSION, PeopleMasker, document, error_document, home_tilde, mask_people,
                                tilde_text)
from pycorn.messages import ERROR_KEYS, EXIT_CODES
from pycorn.version import __version__

SAMPLES = os.path.abspath("../samples")
SAMPLE = os.path.join(SAMPLES, "sample.zip")
TWO = [SAMPLE, os.path.join(SAMPLES, "u711_manual_run.zip")]
# made up for the tests: what a real catalog holds in these columns
PLANTED = {"system": "AKTA-Bay7", "user": "LAB\\jdoe", "database": "SecretDb_Prod", "server": "db.example.invalid"}
IP = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
ENVELOPE = ("schema", "version", "command", "masked")


@pytest.fixture(autouse=True)
def _no_settings_from_outside(monkeypatch, tmp_path):
    """No UNICORN_* or PYCORN_ENV_FILE from the shell, no repository or per-user settings file."""
    for key in list(os.environ):
        if key.upper().startswith("UNICORN_") or key.upper() == ENV_FILE_VAR:
            monkeypatch.delenv(key)
    monkeypatch.setattr(db_connector, "DEFAULT_ENV_FILE", tmp_path / "checkout" / "sql" / ".env")
    monkeypatch.setattr(db_connector, "user_env_file", lambda: tmp_path / "user" / ".env")
    monkeypatch.setattr(db_connector, "_local_server_answers", lambda port: False)


def _main(capsys, *argv):
    code = cli.main(list(argv))
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _envelope_ok(doc, command, masked=True):
    assert list(doc)[:4] == list(ENVELOPE)
    assert doc["schema"] == SCHEMA_VERSION == 1 and doc["version"] == __version__
    assert doc["command"] == command and doc["masked"] is masked


def _lines(out):
    lines = [json.loads(line) for line in out.splitlines()]
    head, tail = lines[0], lines[-1]
    assert head["type"] == "header" and head["schema"] == 1 and head["version"] == __version__
    assert tail["type"] == "summary"
    return head, lines[1:-1], tail


def _no_secrets(*texts):
    for text in texts:
        for value in (PLANTED["database"], PLANTED["server"]):
            assert value not in text
        assert not IP.search(text)


# --- the shared module ------------------------------------------------------------

def test_document_and_summary_document_share_the_envelope():
    from pycorn.summary import SCHEMA_VERSION as INSPECT_SCHEMA
    from pycorn.summary import document as inspect_document
    doc = document("find", True, rows=[])
    _envelope_ok(doc, "find")
    old = inspect_document([], False, problems=[])
    assert INSPECT_SCHEMA == SCHEMA_VERSION and list(old) == list(ENVELOPE) + ["runs", "problems"]
    assert old["command"] == "inspect" and old["masked"] is False


def test_home_folder_as_tilde():
    home = os.path.expanduser("~")
    inside = os.path.join(home, "results", "a.zip")
    assert home_tilde(inside) == os.path.join("~", "results", "a.zip")
    assert home_tilde(home) == "~"
    outside = os.path.abspath(os.sep + "elsewhere-pycorn-test")
    assert home_tilde(outside) == outside
    assert tilde_text("not found: %s" % inside) == "not found: %s" % os.path.join("~", "results", "a.zip")
    assert tilde_text("%s2\\x" % home) == "%s2\\x" % home  # another folder that starts the same


def test_people_masker_numbers_once_per_document():
    masker = PeopleMasker()
    row = {"system": PLANTED["system"], "created_by": PLANTED["user"], "name": "AKTA-Bay7 run 3",
           "match_key": "2020-01-01T00:00:00Z|AKTA-Bay7|1"}
    out = mask_people(row, masker)
    assert out["system"] == "System1" and out["created_by"] == "User1"
    assert out["match_key"] == "2020-01-01T00:00:00Z|System1|1"
    assert out["name"] == row["name"]  # run names are shown
    again = mask_people({"system": "akta-bay7", "created_by": "Default"}, masker)
    assert again["system"] == "System1"  # the same name in another case
    assert again["created_by"] == "Default"  # a built-in account stays
    assert masker.text("by jdoe") == "by User3"  # the login's parts learned with it (LAB: User2)
    assert mask_people({"system": "Other"}, masker)["system"] == "System2"


def test_error_keys_and_exit_codes_documented():
    doc = error_document("not found: x", "file_not_found", 2)
    assert doc == {"error": {"text": "not found: x", "key": "file_not_found", "exit": 2}}
    assert {"bad_flag", "no_settings", "connection_failed", "file_not_found", "unexpected"} <= set(ERROR_KEYS)
    from pycorn.schema import build_schema
    schema = build_schema()
    assert set(schema["error_keys"]) == set(ERROR_KEYS) and set(EXIT_CODES) >= {0, 1, 2, 3, 130}


# --- export --------------------------------------------------------------------------

def test_export_json_document(tmp_path, capsys):
    code, out, err = _main(capsys, "export", *TWO, "--out", str(tmp_path / "o"), "--json")
    assert code == 0
    doc = json.loads(out)
    _envelope_ok(doc, "export")
    assert doc["totals"]["selected"] == 2 and doc["totals"]["written"] == 2 and doc["totals"]["failed"] == 0
    index = pd.read_csv(tmp_path / "o" / "index.csv")
    assert len(doc["rows"]) == len(index)
    row = doc["rows"][0]
    assert row["export_status"] == "written" and row["export_folder"] == index["export_folder"][0]
    assert "database" not in row and row["db_label"] == "zip" and row["position"] == 0
    assert set(index.columns) - {"database"} <= set(row)
    assert "written:" in err and "written:" not in out  # human lines on stderr
    _no_secrets(out)


def test_export_jsonl_streams_rows_with_header_and_summary(tmp_path, capsys):
    _main(capsys, "export", *TWO, "--out", str(tmp_path / "o"))  # a first export: the second run skips them
    code, out, err = _main(capsys, "export", *TWO, "--out", str(tmp_path / "o"), "--jsonl")
    assert code == 0
    head, rows, tail = _lines(out)
    assert head["command"] == "export" and head["masked"] is True
    assert [r["type"] for r in rows] == ["run", "run"]
    assert all(r["export_status"] == "skipped (exists)" for r in rows)
    assert tail["totals"]["skipped"] == 2 and tail["problems"] == []
    assert tail["out"] == home_tilde(os.path.abspath(str(tmp_path / "o")))


def test_export_masks_people_on_stdout_not_in_files(tmp_path, capsys):
    catalog = catalog_from_files(SAMPLE)
    catalog["system"], catalog["created_by"] = PLANTED["system"], PLANTED["user"]
    catalog["match_key"] = [k.replace("System1", PLANTED["system"]) for k in catalog["match_key"]]
    selection = tmp_path / "sel.csv"
    catalog.to_csv(selection, index=False)
    code, out, _ = _main(capsys, "export", "--from", str(selection), "--out", str(tmp_path / "o"), "--json")
    assert code == 0
    row = json.loads(out)["rows"][0]
    assert row["system"] == "System1" and row["created_by"] == "User1" and "|System1|" in row["match_key"]
    assert PLANTED["system"] not in out and "jdoe" not in out
    assert row["path"].startswith("~") or not os.path.abspath(SAMPLE).startswith(os.path.expanduser("~"))
    index = pd.read_csv(tmp_path / "o" / "index.csv")  # the file keeps today's rule: as stored
    assert index["system"][0] == PLANTED["system"] and index["created_by"][0] == PLANTED["user"]
    code, out, _ = _main(capsys, "export", "--from", str(selection), "--out", str(tmp_path / "o"), "--json",
                         "--no-mask")
    doc = json.loads(out)
    assert doc["masked"] is False and doc["rows"][0]["system"] == PLANTED["system"]


def test_export_human_output_unchanged(tmp_path, capsys):
    out_dir = str(tmp_path / "o")
    code, out, err = _main(capsys, "export", *TWO, "--out", out_dir)
    assert code == 0 and err == ""
    lines = out.splitlines()
    assert lines[0] == "2 run(s) selected, 2 to export, 0 skipped (already there); 0 changed since export or not checkable"
    assert [l.split(" (")[0].split(os.sep)[0] for l in lines[1:3]] == ["written: zip", "written: zip"]
    assert lines[3].startswith("done: 2 written; index: ") and lines[3].endswith("index.csv")
    assert len(lines) == 4 and not out.lstrip().startswith("{")


# --- bypass-uv -----------------------------------------------------------------------

def test_bypass_uv_json_jsonl_and_csv_stdout(tmp_path, capsys):
    code, out, err = _main(capsys, "bypass-uv", *TWO, "--json")
    assert code == 0
    doc = json.loads(out)
    _envelope_ok(doc, "bypass-uv")
    assert len(doc["readings"]) >= 1 and doc["runs"] == 2 and doc["problems"] == []
    assert "reading(s) of sample" in err and "reading(s)" not in out
    code, out, _ = _main(capsys, "bypass-uv", *TWO, "--jsonl")
    head, rows, tail = _lines(out)
    assert {r["type"] for r in rows} == {"reading"} and tail["readings"] == len(rows) == len(doc["readings"])
    file = tmp_path / "b.csv"
    _main(capsys, "bypass-uv", *TWO, "--csv", str(file))
    for flags in (["--no-mask"], []):  # the sample holds no people: masked equals as stored
        code, out, err = _main(capsys, "bypass-uv", *TWO, "--csv", "-", *flags)
        assert code == 0 and "reading(s) of sample" in err
        assert pd.read_csv(io.StringIO(out)).equals(pd.read_csv(file))


def test_bypass_reading_people_masked():
    masker = PeopleMasker()
    masker.add(PLANTED["system"], "System")
    row = {"source": "zip", "run": "AKTA-Bay7 test", "chromatogram": "Chrom.1", "curve": "UV",
           "warnings": ["seen on AKTA-Bay7"], "blocks": "AKTA-Bay7 wash", "max": 3.0}
    shown = cli._shown_reading(row, masker)
    assert shown["run"] == row["run"] and shown["warnings"] == ["seen on System1"]
    assert shown["blocks"] == "System1 wash" and shown["max"] == 3.0
    assert cli._shown_reading(row, None) == row


def test_bypass_uv_human_output_unchanged(capsys):
    code, out, err = _main(capsys, "bypass-uv", *TWO)
    assert code == 0
    lines = out.splitlines()
    assert lines[0] == "1 reading(s) of sample sent to the column by-pass"
    assert lines[1].split() == ["run", "chrom", "win", "curve", "nm", "kind", "max", "A/cm", "epsilon"]
    assert lines[-1].startswith("note: ")


# --- inspect -------------------------------------------------------------------------

def test_inspect_jsonl(capsys):
    code, out, _ = _main(capsys, "inspect", *TWO, "--jsonl")
    assert code == 0
    head, rows, tail = _lines(out)
    assert head["command"] == "inspect" and head["masked"] is True
    assert [r["type"] for r in rows] == ["run", "run"] and rows[0]["source"]["file"] == "File1.zip"
    assert tail["runs"] == 2 and tail["problems"] == []
    code, out, _ = _main(capsys, "inspect", *TWO, "--json")
    doc = json.loads(out)
    _envelope_ok(doc, "inspect")
    assert len(doc["runs"]) == 2 and doc["runs"][1]["source"]["file"] == "File2.zip"


# --- find (no database: a stand-in) ----------------------------------------------------

class _FindDb:
    """Two results in database PLANTED['database'] of source 'a' (label 'a#1'), with people in them."""

    unreachable = {}

    def database_labels(self, sources, strict=False):
        return {("a", PLANTED["database"].lower()): "a#1"}

    def count(self, **kw):
        return 2

    def _find_page(self, limit, after, sources, strict, filters, dedupe):
        page = pd.DataFrame({
            "source": ["a", "a"], "database": [PLANTED["database"]] * 2, "result_id": [1, 2],
            "name": ["Run on AKTA-Bay7", "r2"], "folder_path": ["/P/x", "/P/y"],
            "system": [PLANTED["system"], PLANTED["system"]], "created_by": [PLANTED["user"], "Default"],
            "match_key": ["2020-01-01T00:00:00Z|%s|1" % PLANTED["system"], None]})
        return page, True, ("a", PLANTED["database"], 2)


def _find(capsys, *argv):
    args = cli.build_parser().parse_args(["find"] + list(argv))
    code = cli.run_find(args, _FindDb())
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def test_find_json_label_only_people_masked(capsys):
    code, out, err = _find(capsys, "--json")
    assert code == 0
    doc = json.loads(out)
    _envelope_ok(doc, "find")
    assert doc["total"] == 2 and doc["more"] is True and doc["next_after"] == "a#1:2"
    row = doc["rows"][0]
    assert row["db_label"] == "a#1" and "database" not in row
    assert row["system"] == "System1" and row["created_by"] == "User1" and row["match_key"].endswith("|System1|1")
    assert row["name"] == "Run on AKTA-Bay7" and row["folder_path"] == "/P/x"  # names are shown
    assert doc["rows"][1]["created_by"] == "Default"
    assert "2 result(s) match." in err and "More results follow: --after a#1:2" in err
    assert "AKTA-Bay7" not in err  # no table on stderr either
    _no_secrets(out, err)
    code, out, _ = _find(capsys, "--json", "--no-mask")
    doc = json.loads(out)
    assert doc["masked"] is False and doc["rows"][0]["system"] == PLANTED["system"]
    _no_secrets(out)


def test_find_jsonl(capsys):
    code, out, _ = _find(capsys, "--jsonl")
    head, rows, tail = _lines(out)
    assert head["command"] == "find" and [r["type"] for r in rows] == ["row", "row"]
    assert tail["total"] == 2 and tail["rows"] == 2 and tail["next_after"] == "a#1:2"
    _no_secrets(out)


def test_find_csv_stdout(tmp_path, capsys):
    file = tmp_path / "runs.csv"
    _find(capsys, "--csv", str(file))
    code, out, err = _find(capsys, "--csv", "-", "--no-mask")
    assert code == 0 and "2 result(s) match." in err and "written to" not in err
    stored = pd.read_csv(file)
    assert pd.read_csv(io.StringIO(out)).equals(stored.drop(columns=["database"]))  # the label names the database
    code, out, _ = _find(capsys, "--csv", "-")
    masked = pd.read_csv(io.StringIO(out))
    assert list(masked["system"]) == ["System1", "System1"] and list(masked["created_by"]) == ["User1", "Default"]
    assert list(masked["name"]) == list(stored["name"])
    _no_secrets(out)


def test_export_from_a_stdout_csv_finds_the_database_by_label(tmp_path, capsys):
    frame = pd.read_csv(io.StringIO(_find(capsys, "--csv", "-")[1]))
    selection = tmp_path / "s.csv"
    frame.to_csv(selection, index=False)
    catalog = cli.read_selection(str(selection))
    assert catalog["database"].isna().all()
    resolved = cli.resolve_labels(catalog, lambda: _FindDb())
    assert list(resolved["database"]) == [PLANTED["database"].lower()] * 2
    frame[cli.LABEL] = "a#9"
    frame.to_csv(selection, index=False)
    with pytest.raises(ValueError, match="no database labelled 'a#9'"):
        cli.resolve_labels(cli.read_selection(str(selection)), lambda: _FindDb())


# --- stderr masked like stdout (owner, 2026-11-11) -------------------------------------------

COMPUTER = "LABPC-0042"
SAID = "Method Run 1 User %s at computer %s is in control of %s" % (PLANTED["user"], COMPUTER, PLANTED["system"])


@pytest.fixture
def planted_runs(monkeypatch):
    """Every loaded run stores the planted user, computer and system; the export and by-pass warn with them."""
    import warnings as w
    from pycorn import run_export as rx
    from pycorn import uv_calibration
    # every command loads runs through run_export._selected_runs (export, bypass-uv through cli._runs)
    real_runs, real_write, real_readings = rx._selected_runs, rx.write_run, uv_calibration.bypass_uv_readings

    def runs(*a, **kw):
        for i, run in real_runs(*a, **kw):
            run.result.update({"CreatedBy": PLANTED["user"], "ComputerName": COMPUTER, "SystemName": PLANTED["system"]})
            yield i, run

    def write_run(*a, **kw):
        w.warn(SAID)
        return real_write(*a, **kw)

    def readings(*a, **kw):
        w.warn(SAID)
        out = real_readings(*a, **kw)
        for r in out:
            r["warnings"] = list(r["warnings"]) + [SAID]
        return out
    monkeypatch.setattr(rx, "_selected_runs", runs)
    monkeypatch.setattr(rx, "write_run", write_run)
    monkeypatch.setattr(uv_calibration, "bypass_uv_readings", readings)


def _planted_selection(tmp_path):
    catalog = catalog_from_files(TWO)
    catalog["system"], catalog["created_by"] = PLANTED["system"], PLANTED["user"]
    selection = tmp_path / "sel.csv"
    catalog.to_csv(selection, index=False)
    return str(selection)


def _shown(text):
    return [v for v in (PLANTED["system"], "jdoe", COMPUTER) if v in text]


def test_export_stderr_masked_like_stdout(tmp_path, capsys, planted_runs):
    code, out, err = _main(capsys, "export", "--from", _planted_selection(tmp_path), "--out", str(tmp_path / "o"),
                           "--json")
    assert code == 0
    row = json.loads(out)["rows"][0]
    assert _shown(out + err) == []
    said = [l for l in err.splitlines() if l.startswith("warning: Method Run 1")]
    assert said and all("User %s" % row["created_by"] in l and "of %s" % row["system"] in l for l in said)
    assert row["system"] == "System1" and row["created_by"] == "User1" and "Computer1" in said[0]
    code, out, err = _main(capsys, "export", "--from", _planted_selection(tmp_path), "--out", str(tmp_path / "o"),
                           "--json", "--no-mask", "--overwrite")
    assert code == 0 and SAID in err and json.loads(out)["rows"][0]["system"] == PLANTED["system"]


def test_bypass_uv_stderr_masked_like_stdout(tmp_path, capsys, planted_runs):
    code, out, err = _main(capsys, "bypass-uv", *TWO, "--json")
    assert code == 0
    readings = json.loads(out)["readings"]
    assert readings and _shown(out + err) == []
    on_stdout = readings[0]["warnings"][-1]
    on_stderr = [l[len("warning: "):] for l in err.splitlines() if l.startswith("warning: Method Run 1")]
    # the samples' catalog already says 'System1' (anonymized): the planted system is the next label
    assert on_stderr and on_stdout in on_stderr
    assert re.search(r"User User\d at computer Computer1 is in control of System\d$", on_stdout)
    code, out, err = _main(capsys, "bypass-uv", *TWO, "--json", "--no-mask")
    assert SAID in err and SAID in json.loads(out)["readings"][0]["warnings"]
    code, out, err = _main(capsys, "bypass-uv", *TWO)  # human mode: as before, nothing masked
    assert SAID in out or SAID in err


def test_error_text_masked_in_machine_mode(tmp_path, capsys, planted_runs, monkeypatch):
    from pycorn import run_export as rx

    def broken(*a, **kw):
        raise ValueError("disk of %s full" % PLANTED["system"])
    monkeypatch.setattr(rx, "write_run", broken)
    code, out, err = _main(capsys, "export", "--from", _planted_selection(tmp_path), "--out", str(tmp_path / "o"),
                           "--json", "--strict")
    assert code == 2 and _error(err)["text"] == "disk of System1 full" and _shown(err) == []
    code, out, err = _main(capsys, "export", "--from", _planted_selection(tmp_path), "--out", str(tmp_path / "o"),
                           "--json", "--strict", "--no-mask")
    assert PLANTED["system"] in _error(err)["text"]


# --- errors ----------------------------------------------------------------------------

def _error(err):
    lines = [l for l in err.splitlines() if l.startswith("{")]
    assert len(lines) == 1
    return json.loads(lines[0])["error"]


def test_structured_error_missing_file(capsys):
    code, out, err = _main(capsys, "export", "nothere.zip", "--out", "x", "--json")
    assert code == 2 and out == ""
    e = _error(err)
    assert e["key"] == "file_not_found" and e["exit"] == 2 and "nothere.zip" in e["text"]
    home = os.path.expanduser("~")
    assert home not in e["text"] or not os.getcwd().startswith(home)


def test_structured_error_bad_flag(capsys):
    with pytest.raises(SystemExit) as stop:
        cli.main(["export", "--jsonl", "--no-such-flag", "--out", "x"])
    assert stop.value.code == 2
    e = _error(capsys.readouterr().err)
    assert e["key"] == "bad_flag" and e["exit"] == 2 and "--no-such-flag" in e["text"]
    with pytest.raises(SystemExit):
        cli.main(["find", "--json", "--jsonl"])
    assert _error(capsys.readouterr().err)["key"] == "bad_flag"
    code, out, err = _main(capsys, "bypass-uv", SAMPLE, "--json", "--csv", "-")
    assert code == 2 and _error(err)["key"] == "bad_flag"
    code, out, err = _main(capsys, "export", SAMPLE, "--out", "x", "--json", "--units", "volume")
    assert code == 2 and _error(err)["key"] == "bad_flag"
    with pytest.raises(SystemExit):  # without --json: the usual usage text
        cli.main(["export", "--no-such-flag"])
    err = capsys.readouterr().err
    assert "usage:" in err and not err.lstrip().startswith("{")


def test_structured_error_missing_settings(capsys):
    code, out, err = _main(capsys, "find", "--json")
    assert code == 2 and out == ""
    e = _error(err)
    assert e["key"] == "no_settings" and e["exit"] == 2 and "pycorn-setup" in e["text"]
    code, out, err = _main(capsys, "find")  # plain text otherwise, as before
    assert code == 2 and err.startswith("error: ") and "{" not in err


def test_structured_error_connection_and_unreachable(tmp_path, capsys, monkeypatch):
    env = tmp_path / "s.env"
    env.write_text("UNICORN_SERVER=%s\nUNICORN_DATABASE=%s\nUNICORN_USER=reader\nUNICORN_PASSWORD=x\n"
                   % (PLANTED["server"], PLANTED["database"]))

    def refuse(self, source=None, *a, **kw):
        raise ConnectionError("cannot reach the SQL Server: check the server name")
    monkeypatch.setattr(PyCORNDatabase, "_connect", refuse)
    code, out, err = _main(capsys, "--env-file", str(env), "find", "--json")
    assert code == 1
    doc = json.loads(out)
    assert doc["unreachable"] and doc["rows"] == [] and doc["problems"]
    _no_secrets(out, err)
    code, out, err = _main(capsys, "--env-file", str(env), "find", "--json", "--strict")
    assert code == 2 and out == ""
    e = _error(err)
    assert e["key"] == "connection_failed" and "hint" in e
    _no_secrets(err)


def test_unexpected_error_structured(tmp_path, capsys, monkeypatch):
    def broken(args, db_factory):
        raise TypeError("something nobody worded")
    monkeypatch.setattr(cli, "run_export", broken)
    code, out, err = _main(capsys, "export", SAMPLE, "--out", str(tmp_path), "--json")
    assert code == 3
    e = _error(err)
    assert e["key"] == "unexpected" and e["exit"] == 3 and "TypeError" in e["text"] and "hint" in e
    code, out, err = _main(capsys, "export", SAMPLE, "--out", str(tmp_path), "--json", "--debug")
    assert code == 3 and "Traceback" in err and _error(err)["key"] == "unexpected"
