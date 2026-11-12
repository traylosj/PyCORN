# -*- coding: utf-8 -*-
"""The Python side of the in-browser page (`pycorn.browser`, ROADMAP item 6), run here in CPython."""
import json
import os
import re
import zipfile

import numpy as np
import pandas as pd
import pytest

from pycorn import browser

SAMPLES = os.path.abspath("../samples")
FILES = [os.path.join(SAMPLES, n) for n in ("sample.zip", "u711_column_test.zip", "u73_blank.zip")]
TEMPLATE = os.path.abspath("../web/index.html")


@pytest.fixture(scope="module")
def session():
    s = browser.Session()
    added = json.loads(s.add(FILES + [os.path.join(SAMPLES, "SAMPLE_ZIP_CONTENTS.md")]))
    assert len(added["runs"]) == 3
    return s


def test_add_names_files_not_read(session, tmp_path):
    bad = tmp_path / "broken.zip"
    bad.write_bytes(b"not a zip")
    added = json.loads(session.add([str(bad)]))
    assert added["runs"] == []
    assert [r[0] for r in added["not_read"]] == ["broken.zip"]
    assert json.loads(browser.Session().add([os.path.join(SAMPLES, "SAMPLE_ZIP_CONTENTS.md")]))["not_read"][0][1] \
        == "not a UNICORN result file (.zip, .Result or .UFol)"


def test_rows_are_listed_with_file_names_only(session):
    rows = json.loads(session.rows())
    assert [r["index"] for r in rows] == [0, 1, 2]
    assert {r["file"] for r in rows} == {os.path.basename(f) for f in FILES}  # no folders: they can name the user
    assert set(browser.LIST_COLUMNS) <= set(rows[0])


def test_view(session):
    v = json.loads(session.view(0))
    assert "error" not in v
    chrom = v["chromatograms"][0]
    labels = [label for label, _ in chrom["summary"]]
    assert labels[0] == "Run" and "Column volume (Vc)" in labels and "Exported by" not in labels
    plot = chrom["plot"]
    assert plot["axis"] in ("volume", "time") and plot["curves"]
    keys = [a["key"] for a in plot["axes"]]
    assert plot["axis"] in keys and set(keys) <= set(browser.AXES)
    assert all(len(c["x"][k]) == len(c["y"]) <= browser.MAX_POINTS * 3 // 2 for c in plot["curves"] for k in keys)
    assert {m["kind"] for m in plot["marks"]} <= {"injections"}
    assert {t["kind"] for t in plot["tracks"]} <= {"phases", "inlets", "fractions"}
    for name in ("phases", "injections", "fractions"):
        assert set(chrom[name]) == {"columns", "rows", "origins", "meanings", "checks", "decimals"}
        assert len(chrom[name]["origins"]) == len(chrom[name]["checks"]) == len(chrom[name]["columns"]) \
            == len(chrom[name]["decimals"])
    # display decimals (display_precision): the run-log rule per unit, the rows keep full values
    decimals = dict(zip(chrom["phases"]["columns"], chrom["phases"]["decimals"]))
    assert decimals["start_time"] == 2 and decimals["start_volume"] == 3 and decimals["length_cv"] == 2
    assert decimals["name"] is None and decimals["text"] is None
    assert all(isinstance(a["decimals"], int) for a in plot["axes"] if a["key"] != "clock")
    # every column says whether it is UNICORN's, calculated or analysis (export_tables), the page marks them
    phases = dict(zip(chrom["phases"]["columns"], chrom["phases"]["origins"]))
    assert phases["text"] == "UNICORN" and phases["start_time"] == "PyCORN calculated"
    assert phases["start_cv"] == "PyCORN analysis"
    variables = dict(zip(v["variables"]["columns"], v["variables"]["origins"]))
    assert variables["value"] == "UNICORN" and variables["number"] == "PyCORN calculated"
    checks = dict(zip(v["variables"]["columns"], v["variables"]["checks"]))
    assert checks["number"] and not checks["value"]
    assert v["variables"]["rows"] and v["warnings"]["columns"] == ["chromatogram", "source", "category", "message"]
    json.dumps(v, allow_nan=False)  # NaN would break JSON.parse in the page


def test_plot_offers_every_live_curve():
    from pycorn import open_run
    from pycorn.excel_export import _live, chart_series
    chrom = next(iter(open_run(FILES[0]).chromatograms.values()))
    plot = browser.plot_data(chrom)
    names = [c["name"] for c in plot["curves"]]
    assert len(names) == len(set(names))
    # the Excel chart's curves first and shown, every other live curve after them, off and on the right axis
    default = [c.name for c, _ in chart_series(chrom, [("curves", None)])]
    assert [c["name"] for c in plot["curves"] if c["shown"]] == default == names[:len(default)]
    extras = plot["curves"][len(default):]
    assert extras and all(not c["shown"] and c["right"] for c in extras)
    assert all(_live(chrom.curves[c["name"]]) for c in extras)


def test_plot_tracks_phases_fractions_and_pauses():
    # u711_proa_day12.zip: 6 phases (Equilibration twice), a fraction track, 2 pauses after a wash
    s = browser.Session()
    s.add([os.path.join(SAMPLES, "u711_proa_day12.zip")])
    plot = json.loads(s.view(0))["chromatograms"][0]["plot"]
    axes = {a["key"]: a for a in plot["axes"]}
    assert list(axes) == ["clock", "time", "volume", "cv"] and plot["axis"] == "volume"
    assert axes["clock"]["unit"].startswith("local, UTC") and axes["cv"]["unit"] == "CV"
    tracks = {t["kind"]: t["bars"] for t in plot["tracks"]}
    phases = tracks["phases"]
    # without the 'Phase ' prefix UNICORN logs (owner, 2026-11-09)
    assert [b["name"] for b in phases][-5:] == ["Equilibration", "Sample Application", "Column Wash", "Elution",
                                                "Equilibration"]
    for key in axes:
        bars = [b for b in phases if key in b["start"]]
        assert len(bars) >= 5 and all(b["end"][key] > b["start"][key] for b in bars)
        assert all(a["end"][key] == pytest.approx(b["start"][key]) for a, b in zip(bars, bars[1:]))  # no gaps
        last = max(x for x in plot["curves"][0]["x"][key] if x is not None)
        assert bars[-1]["end"][key] == pytest.approx(last, rel=0.01)
    assert tracks["fractions"] and tracks["fractions"][0]["pseudo"]
    # a pause has no method time, volume or CV: a point where the run stood; on the clock it lasts
    pauses = plot["pauses"]
    assert len(pauses) == 2 and all(p["label"].startswith("pause ") for p in pauses)
    assert all(0 <= p["at"]["volume"] <= phases[-1]["end"]["volume"] for p in pauses)
    assert all(set(p["at"]) == {"time", "volume", "cv"} for p in pauses)
    assert all(50e3 < p["end"] - p["start"] < 56e3 for p in pauses)  # 53 s, in ms
    # the clock includes the first pause (in Equilibration), which stored time leaves out
    elution = next(b for b in phases if b["name"] == "Elution")
    wall = (elution["start"]["clock"] - phases[1]["start"]["clock"]) / 1000
    stored = (elution["start"]["time"] - phases[1]["start"]["time"]) * 60
    assert 45 < wall - stored < 56
    json.dumps(plot, allow_nan=False)


def test_plot_inlets_track():
    # one bar per stretch drawing from the same inlets, from the executed run log (D17), on every axis
    s = browser.Session()
    added = json.loads(s.add([os.path.join(SAMPLES, "u711_proa_day12.zip"), os.path.join(SAMPLES, "u711_manual_run.zip")]))
    index = {run["file"]: run["index"] for run in added["runs"]}
    simple = json.loads(s.view(index["u711_proa_day12.zip"]))["chromatograms"][0]["plot"]
    manual = json.loads(s.view(index["u711_manual_run.zip"]))["chromatograms"][0]["plot"]
    # after the phases, before the fractions; a lane name of its own (it is not a table)
    assert [t["kind"] for t in simple["tracks"]] == ["phases", "inlets", "fractions"]
    inlets = next(t for t in simple["tracks"] if t["kind"] == "inlets")
    # the method sets only line A: the loop while injected, and line B at 100 %B elution: the
    # system configuration has no inlet valve on line B, so it shows as 'B' (no 'B1' exists)
    assert inlets["title"] == "Inlets" and [b["name"] for b in inlets["bars"]] == [
        "A1 (assumed)", "A1", "A1, Loop", "A1", "B", "A1"]
    assert set(inlets["bars"][1]["start"]) == {"clock", "time", "volume", "cv"}  # bar 0: the first seconds only
    elution = inlets["bars"][4]
    assert [p[1] for p in elution["percent_b"]["time"]][:2] == [100.0, 100.0]
    assert elution["percent_b"]["time"][0][0] == pytest.approx(elution["start"]["time"])
    # a run that changes inlets: line A, line B, then the loop or sample inlet, in time order
    bars = next(t for t in manual["tracks"] if t["kind"] == "inlets")["bars"]
    names = [b["name"] for b in bars]
    assert "A2+B2, Loop" in names and names[-1] == "A1"  # a constant mix of A and B: '+'
    for key in ("time", "volume", "clock"):
        on = [b for b in bars if key in b["start"]]  # a stretch of a second or two has no volume
        assert all(b["end"][key] > b["start"][key] for b in on)
        assert all(a["end"][key] == pytest.approx(b["start"][key]) for a, b in zip(on, on[1:]))  # no gaps
    json.dumps(manual, allow_nan=False)


def test_overlay_of_several_runs_by_role():
    # W11: runs read once into a cache of thinned curves (the run itself dropped), curves by role
    s = browser.Session()
    files = FILES + [os.path.join(SAMPLES, "u711_manual_run.zip")]
    rows = json.loads(s.add(files))["runs"]
    ids = [r["index"] for r in rows]
    assert all(json.loads(s.overlay_run(i))["curves"] > 0 for i in ids)
    assert set(s._overlay) == {(i, None) for i in ids} and s._shown is None  # cached, no run kept
    out = json.loads(s.overlay(ids))
    json.dumps(out, allow_nan=False)
    # default: the Excel chart's roles of the first run, on volume when every run has it
    assert out["shown_roles"] == ["uv", "conductivity"] and out["axis"] == "volume"
    assert [r["index"] for r in out["runs"]] == ids and not out["no_axis"] and not out["failed"]
    assert out["requested"] == len(ids) and out["max"] == browser.OVERLAY_MAX
    roles = [r["role"] for r in out["roles"]]
    assert roles == [r for r in browser_roles() if r in roles] and {"uv", "conductivity"} <= set(roles)
    for run in out["runs"]:
        assert {c["role"] for c in run["curves"]} == {"uv", "conductivity"}
        for c in run["curves"]:
            assert len(c["x"]) == len(c["y"]) <= browser.OVERLAY_POINTS * 3 // 2
            assert c["name"] in [n for r in out["roles"] if r["role"] == c["role"] for n in r["curves"]]
    # one unit per role and per axis (the first run's)
    for role in ("uv", "conductivity"):
        assert len({c["unit"] for run in out["runs"] for c in run["curves"] if c["role"] == role}) == 1
    assert {a["key"] for a in out["axes"]} <= set(browser.AXES)
    # other roles and axes from the cache; a run without the axis is left out and named
    cv = json.loads(s.overlay(ids, ["pressure"], "cv"))
    assert cv["axis"] == "cv" and cv["shown_roles"] == ["pressure"]
    manual = next(r["index"] for r in rows if r["file"] == "u711_manual_run.zip")  # no column volume
    assert [r["index"] for r in cv["no_axis"]] == [manual]
    assert manual not in [r["index"] for r in cv["runs"]]
    assert all(c["role"] == "pressure" for r in cv["runs"] for c in r["curves"])
    # an unknown role or axis: dropped, or the default axis
    assert json.loads(s.overlay(ids[:1], ["no such role"], "no axis"))["shown_roles"] == []
    # at most OVERLAY_MAX runs; a row that can't be read is named, the rest drawn
    many = json.loads(s.overlay(ids * 3))
    assert many["requested"] == len(ids) * 3 and len(many["runs"]) == browser.OVERLAY_MAX
    bad = json.loads(s.overlay([ids[0], 99]))
    assert [r["index"] for r in bad["runs"]] == [ids[0]]
    assert bad["failed"] == [{"index": 99, "pick": [99, None], "message": {"key": "msg.no_run", "values": {"index": 99}, "text": "no run 99"}}]
    assert "error" in json.loads(s.overlay_run(99))
    s.clear()
    assert not s._overlay


def test_overlay_cache_is_bounded(monkeypatch):
    s = browser.Session()
    ids = [r["index"] for r in json.loads(s.add(FILES))["runs"]]
    monkeypatch.setattr(browser, "OVERLAY_CACHE", 2)
    for i in ids:
        s.overlay_run(i)
    assert list(s._overlay) == [(i, None) for i in ids[-2:]]  # the oldest goes first


def test_overlay_converts_units_to_the_first_runs():
    # a run in other units (time in s, volume in l, UV in AU) is drawn in the first run's units
    from pycorn import open_run
    a, b = open_run(FILES[0]), open_run(FILES[1])
    ea, eb = browser.overlay_entry(a), browser.overlay_entry(b)
    for c in eb["curves"]:
        if c["role"] == "uv" and c["unit"] == "mAU":
            c["unit"], c["y"] = "AU", c["y"] / 1000.0
        if c["x_unit"].get("volume") == "ml":
            c["x_unit"]["volume"], c["x"]["volume"] = "l", c["x"]["volume"] / 1000.0
    out = browser.overlay_data([(0, ea), (1, eb)], ["uv"], "volume")
    first = next(c for c in out["runs"][0]["curves"])
    other = next(c for c in out["runs"][1]["curves"])
    assert first["unit"] == other["unit"] == "mAU"
    ref = browser.overlay_entry(b)["curves"][0]
    assert other["y"][:5] == pytest.approx(list(ref["y"][:5])) and other["x"][:5] == pytest.approx(list(ref["x"]["volume"][:5]))


def browser_roles():
    from pycorn.curve_roles import ROLES
    return list(ROLES)


# M22 on the page: two public samples with the same phases (Equilibration twice)
WINDOW_FILES = [os.path.join(SAMPLES, n) for n in ("u711_proa_day12.zip", "u73_blank.zip")]
WINDOWS = ["tail=Elution:end-2:end CV", "eq=Equilibration:0:1 min"]


def test_window_stats_rows_are_window_stats_rows():
    # the page's table: every run's `window_stats_rows`, in the export's columns and order
    from pycorn import open_run
    from pycorn.window_stats import COLUMNS, INTERIM_RULE, window_axis, window_stats_rows
    s = browser.Session()
    ids = [r["index"] for r in json.loads(s.add(WINDOW_FILES))["runs"]]
    info = json.loads(s.window_info([[i, None] for i in ids]))
    assert info["phases"][-4:] == ["Equilibration", "Sample Application", "Column Wash", "Elution"]  # once each
    assert "uv" in info["roles"] and info["roles"] == [r for r in browser_roles() if r in info["roles"]]
    assert info["curves"] and info["failed"] == []
    assert info["units"] == list(browser.WINDOW_UNITS) and all(window_axis(u) for u in info["units"])
    out = json.loads(s.window_stats(ids, WINDOWS, ["uv"]))
    json.dumps(out, allow_nan=False)
    t = out["table"]
    assert t["columns"] == COLUMNS
    expected, counts = [], []
    for path in WINDOW_FILES:
        run = open_run(path)
        rows = window_stats_rows(run, run.chromatogram(), WINDOWS, ["uv"])[0]
        expected += rows
        counts.append(len(rows))
    assert len(t["rows"]) == len(expected) > 0
    for got, want in zip(t["rows"], expected):
        for column, value in zip(COLUMNS, got):
            if column == "warnings":
                assert json.loads(value) == want[column]
            elif isinstance(want[column], float):
                assert value == pytest.approx(want[column], rel=1e-12)
            else:
                assert value == want[column], column
    eq = [r for r in t["rows"] if r[COLUMNS.index("window")] == "eq"]
    assert sorted({r[COLUMNS.index("occurrence")] for r in eq}) == [1, 2]  # interim rule: a row per occurrence
    decimals = dict(zip(t["columns"], t["decimals"]))
    assert decimals["mean"] is not None and decimals["run"] is None  # display_precision: the curve's precision
    assert out["interim"] == INTERIM_RULE and not any(INTERIM_RULE in w for w in out["warnings"])
    # the run column as the page names runs (its overlay legend)
    named = json.loads(s.window_stats(json.dumps(ids), json.dumps(WINDOWS), json.dumps(["uv"]),
                                      json.dumps({str(ids[0]): "first"})))
    assert [r[0] for r in named["table"]["rows"]] == ["first"] * counts[0] + [expected[-1]["run"]] * counts[1]
    assert len(s._windows) == 2  # kept per run, windows and curves
    s.clear()
    assert not s._windows and not s._window_info


def test_window_stats_has_no_default_and_bad_text_is_a_message():
    s = browser.Session()
    s.add(WINDOW_FILES[:1])
    for windows, curves in (([], ["uv"]), (WINDOWS, []), ([], []), ([" "], [None])):
        r = json.loads(s.window_stats([0], windows, curves))
        assert set(r) == {"error", "message"} and "no default" in r["error"], (windows, curves)
    bad = json.loads(s.window_stats([0], ["Elution:0:1 min"], ["uv"]))
    assert set(bad) == {"error", "message"} and "NAME=PHASE:FROM:TO UNIT" in bad["error"]
    assert bad["message"]["key"] == "msg.error" and bad["message"]["values"] == {"error": bad["error"]}
    # the form's fields from the text, in the CLI syntax; the parser's sentence when it does not parse
    assert json.loads(s.parse_window("tail=Elution:end-2:end CV")) == {"window": {
        "name": "tail", "phase": "Elution", "from": "end-2", "to": "end", "unit": "CV",
        "text": "tail=Elution:end-2:end CV"}}
    assert json.loads(s.parse_window("w=Phase Sample Application:start+1:3.5 ml"))["window"]["from"] == "1"
    for text in ("w=Elution:2:1 CV", "w=Elution:0:1 parsec", "Elution:0:1 min", "w=Elution:a:1 min", ""):
        r = json.loads(s.parse_window(text))
        assert set(r) == {"error", "message"} and r["error"], text
    # a phase or curve a run lacks: a note and no rows; a row that can't be read: failed
    r = json.loads(s.window_stats([[0, None], [9, None]], ["x=No Such Phase:0:1 min"], ["uv", "no such curve"]))
    assert r["table"]["rows"] == [] and r["table"]["columns"]
    assert any("No Such Phase" in w for w in r["warnings"]) and any("no such curve" in w for w in r["warnings"])
    assert [f["index"] for f in r["failed"]] == [9] and r["failed"][0]["message"]["key"] == "msg.no_run"


def test_download_with_windows_writes_the_clis_table(tmp_path):
    # M22: the page's windows in its options: the file `pycorn export --options` writes from them
    from pycorn import cli
    from pycorn.run_export import ExportOptions
    opts = {"windows": WINDOWS, "window_curves": ["uv"], "groups": []}
    d = browser.options_dict(opts)
    assert d["windows"] == WINDOWS and d["window_curves"] == ["uv"] and d["tables"] == ["curves", "phase_windows"]
    assert "phase_windows" not in browser.options_dict(dict(opts, groups=["key"]))["tables"]  # a group keeps it
    for half in ({"windows": WINDOWS}, {"window_curves": ["uv"]}):  # both or neither
        assert "windows" not in browser.options_dict(half) and "window_curves" not in browser.options_dict(half)
    s = browser.Session()
    s.add(WINDOW_FILES[:1])
    r = json.loads(s.download(0, "csv", str(tmp_path / "page"), opts))
    with zipfile.ZipFile(r["path"]) as z:
        name = next(n for n in z.namelist() if n.endswith("_phase_window_statistics.csv"))
        page = pd.read_csv(z.open(name))
    options = tmp_path / "options.json"
    options.write_text(json.dumps(ExportOptions.from_dict(d).to_dict()), encoding="utf-8")
    assert cli.main(["export", WINDOW_FILES[0], "--out", str(tmp_path / "cli"), "--format", "csv",
                     "--options", str(options)]) == 0
    found = next((tmp_path / "cli").rglob("*_phase_window_statistics.csv"))
    pd.testing.assert_frame_equal(page, pd.read_csv(found))
    assert len(page) == len(json.loads(s.window_stats([0], WINDOWS, ["uv"]))["table"]["rows"])


def test_view_of_a_missing_row_is_a_sentence(session):
    # W7: errors carry a key the page translates, the values and the English text
    assert json.loads(session.view(99)) == {"error": "no run 99", "message": {
        "key": "msg.no_run", "values": {"index": 99}, "text": "no run 99"}}


def test_messages_have_keys_in_en_json(session, tmp_path):
    en = _texts()["en"]
    assert set(browser.MESSAGES) <= set(en)
    assert {k for k in en if k.startswith("msg.")} == set(browser.MESSAGES)
    holes = lambda s: sorted(re.findall(r"\{(\w+)\}", s))
    s = session
    folder = str(tmp_path / "x")
    for call, key in ((lambda: s.download(0, "pdf", folder), "msg.download_unknown"),
                      (lambda: s.export_start(None, kind="pdf", folder=folder), "msg.all_kind"),
                      (lambda: s.download(0, "csv", folder, {"sample": {"mode": "zip"}}), "msg.size_unknown")):
        m = json.loads(call())["message"]
        assert m["key"] == key and set(m["values"]) == set(holes(en[key])) and m["text"]
    assert browser.error_message(ValueError("boom")) == {"key": "msg.error", "values": {"error": "boom"}, "text": "boom"}


def test_files_not_read_have_messages(tmp_path):
    bad = tmp_path / "broken.zip"
    bad.write_bytes(b"not a zip")
    s = browser.Session()
    name, reason, msg = json.loads(s.add([str(bad)]))["not_read"][0]
    assert msg == {"key": "msg.not_read", "values": {"reason": reason}, "text": reason}
    kind = json.loads(s.add([os.path.join(SAMPLES, "SAMPLE_ZIP_CONTENTS.md")]))["not_read"][0][2]
    assert kind["key"] == "msg.not_read_type" and kind["values"] == {}


def test_download_xlsx_and_csv(session, tmp_path):
    x = json.loads(session.download(0, "xlsx", str(tmp_path / "x")))
    # W4: a small zip of the workbook and README.txt, no csv files, no provenance.json
    assert x["name"].endswith("_xlsx.zip") and zipfile.is_zipfile(x["path"])
    members = [n.split("/", 1)[1] for n in zipfile.ZipFile(x["path"]).namelist()]
    assert sorted(m.rsplit(".", 1)[1] for m in members) == ["txt", "xlsx"] and "README.txt" in members
    book = next(n for n in zipfile.ZipFile(x["path"]).namelist() if n.endswith(".xlsx"))
    with zipfile.ZipFile(x["path"]) as z:
        (tmp_path / "book.xlsx").write_bytes(z.read(book))
    c = json.loads(session.download(0, "csv", str(tmp_path / "c")))
    assert c["name"].endswith("_csv.zip")
    names = zipfile.ZipFile(c["path"]).namelist()
    assert any(n.endswith("/warnings.csv") for n in names) and any(n.endswith("/method_variables.csv") for n in names)
    assert any(n.endswith("/README.txt") for n in names) and any(n.endswith("/column_guide.csv") for n in names)
    assert len({n.split("/")[0] for n in names}) == 1
    prov = json.loads(zipfile.ZipFile(c["path"]).read(next(n for n in names if n.endswith("/provenance.json"))))
    assert prov["route"] == browser.WEB_ROUTE and prov["options"] == {"method_hints": True}
    sheets = pd.ExcelFile(tmp_path / "book.xlsx").sheet_names
    assert sheets[0] == "Summary" and "Planned vs run - blocks" in sheets and "Column guide" in sheets


def test_download_size_choices(session, tmp_path):
    def size(mode, value=None, name="s"):
        r = json.loads(session.download(0, "csv", str(tmp_path / name), json.dumps(
            {"sample": {"mode": mode, "value": value}})))
        if "error" in r:
            return r
        with zipfile.ZipFile(r["path"]) as z:
            return sum(i.file_size for i in z.infolist()), r
    full, _ = size("all", None, "a")
    hybrid, r = size("hybrid", 500, "h")
    every, _ = size("every", 20, "e")
    grid, _ = size("grid", 1, "g")
    assert hybrid < 0.5 * full and every < 0.5 * full and grid < 0.5 * full
    assert "give a number" in size("hybrid", None, "m")["error"]
    assert "unknown size" in size("zip", 5, "z")["error"]
    assert size("hybrid", 1, "b")["message"]["key"] == "msg.size_hybrid_min"
    assert size("every", 0, "d")["message"]["key"] == "msg.size_every_min"
    assert size("grid", 0, "f")["message"]["key"] == "msg.size_grid_min"


def _curve_columns(path):
    with zipfile.ZipFile(path) as z:
        name = next(n for n in z.namelist() if n.endswith(".csv") and "_" not in os.path.basename(n))
        frame = pd.read_csv(z.open(name), nrows=3)
    return {h.split(":")[0] for h in frame.columns}


def test_download_writes_the_ticked_curves_at_full_resolution(session, tmp_path):
    # W1: the curve names the page sends decide the curves table; a name this run lacks is left out, with a warning
    plot = json.loads(session.view(0))["chromatograms"][0]["plot"]
    names = [c["name"] for c in plot["curves"]]
    assert len(names) > 2
    ticked = names[:2]
    r = json.loads(session.download(0, "csv", str(tmp_path / "r"), {"curves": ticked + ["not a curve"]}))
    assert "error" not in r
    assert _curve_columns(r["path"]) == set(ticked)
    with zipfile.ZipFile(r["path"]) as z:
        warns = pd.read_csv(z.open(next(n for n in z.namelist() if n.endswith("/warnings.csv"))))
        full = next(n for n in z.namelist() if n.endswith(".csv") and "_" not in os.path.basename(n))
        rows = len(pd.read_csv(z.open(full)))
    assert (warns["source"] == "curves").any() and "not a curve" in " ".join(warns["message"])
    every = json.loads(session.download(0, "csv", str(tmp_path / "e"), {}))
    assert _curve_columns(every["path"]) >= set(names)  # no curves given: every curve
    chrom = next(iter(session._load(0).chromatograms.values()))
    assert rows == max(len(chrom.curves[n].values) for n in ticked)  # every stored point
    none = json.loads(session.download(0, "csv", str(tmp_path / "n"), {"curves": []}))
    assert none["message"]["key"] == "msg.no_curves"


def test_download_all_is_one_zip_with_a_folder_per_run(session, tmp_path):
    start = json.loads(session.export_start(None, kind="csv", folder=str(tmp_path / "all"), options={"mask": True}))
    assert start == {"runs": 3, "ids": [0, 1, 2]}
    assert all(json.loads(session.export_one(i))["status"] == "written" for i in range(3))
    r = json.loads(session.export_finish())
    assert r["name"] == "PyCORN_export_csv.zip" and r["warnings"] == []
    names = zipfile.ZipFile(r["path"]).namelist()
    assert "PyCORN_export/index.csv" in names
    readmes = [n for n in names if n.endswith("/README.txt")]
    assert len(readmes) == 3 and all(n.startswith("PyCORN_export/zip/") for n in readmes)
    index = pd.read_csv(zipfile.ZipFile(r["path"]).open("PyCORN_export/index.csv"))
    assert list(index["export_status"]) == ["written"] * 3
    prov = json.loads(zipfile.ZipFile(r["path"]).read(readmes[0].replace("README.txt", "provenance.json")))
    assert prov["route"] == browser.WEB_ROUTE + ", download all" and prov["options"]["mask_identifying"] is True
    assert "error" in json.loads(session.export_start(None, kind="pdf", folder=str(tmp_path / "x")))


def test_download_only_the_ticked_runs_and_curves(session, tmp_path):
    # W3 + W1: export_start(ids, curves=[names]) writes those runs, with those curves
    names = [c["name"] for c in json.loads(session.view(0))["chromatograms"][0]["plot"]["curves"]][:1]
    start = json.loads(session.export_start([2, 0], names, "xlsx", str(tmp_path / "t")))
    assert start == {"runs": 2, "ids": [2, 0]}
    assert [json.loads(session.export_one(i))["status"] for i in start["ids"]] == ["written", "written"]
    r = json.loads(session.export_finish())
    index = pd.read_csv(zipfile.ZipFile(r["path"]).open("PyCORN_export/index.csv"))
    assert len(index) == 2 and r["name"] == "PyCORN_export_xlsx.zip" and r["messages"] == []
    with zipfile.ZipFile(r["path"]) as z:
        books = [n for n in z.namelist() if n.endswith(".xlsx")]
        assert len(books) == 2
        for book in books:
            with z.open(book) as f:
                heads = pd.read_excel(f, sheet_name="Curves", nrows=1).columns
            assert {h.split(":")[0] for h in heads} == set(names)
    assert json.loads(session.export_start([7], kind="xlsx", folder=str(tmp_path / "u")))["message"]["key"] == "msg.no_run"


def test_three_excel_routes_give_the_same_files_and_sheets(session, tmp_path):
    # W4: the page's single run, its all-runs zip and `pycorn export --format xlsx` hold the workbook and
    # README.txt, no csv, no provenance.json, and the same sheets
    from pycorn import cli
    single = json.loads(session.download(0, "xlsx", str(tmp_path / "s")))
    with zipfile.ZipFile(single["path"]) as z:
        single_files = sorted(n.split("/", 1)[1] for n in z.namelist())
        book = next(n for n in z.namelist() if n.endswith(".xlsx"))
        (tmp_path / "one.xlsx").write_bytes(z.read(book))
    session.export_start([0], kind="xlsx", folder=str(tmp_path / "a"))
    session.export_one(0)
    everything = json.loads(session.export_finish())
    with zipfile.ZipFile(everything["path"]) as z:
        names = [n for n in z.namelist() if n not in ("PyCORN_export/index.csv", "PyCORN_export/options.json")]
        all_files = sorted(n.rsplit("/", 1)[1] for n in names)
        (tmp_path / "all.xlsx").write_bytes(z.read(next(n for n in names if n.endswith(".xlsx"))))
    out = tmp_path / "cli"
    assert cli.main(["export", FILES[0], "--out", str(out), "--format", "xlsx", "--method-hints"]) == 0
    folder = next(p for p in out.rglob("README.txt")).parent
    cli_files = sorted(p.name for p in folder.iterdir())
    assert single_files == all_files == cli_files and len(cli_files) == 2 and "README.txt" in cli_files
    sheets = [pd.ExcelFile(p).sheet_names for p in (tmp_path / "one.xlsx", tmp_path / "all.xlsx",
                                                    folder / next(f for f in cli_files if f.endswith(".xlsx")))]
    assert sheets[0] == sheets[1] == sheets[2] and "Column guide" in sheets[0] and "Column and timeline" in sheets[0]


def test_names_label_the_page_as_the_downloads():
    n = json.loads(browser.names())
    assert n["tables"]["hint_blocks"]["title"] == "Planned vs run - blocks"
    assert n["tables"]["warnings"]["origin"] == "PyCORN analysis" and n["tables"]["peak_summary"]["origin"] == "UNICORN"
    assert n["version"].startswith(browser_version())
    assert all(isinstance(t["shown"], list) for t in n["tables"].values())
    from pycorn.exporters.cadet.api import CADET_EXPERIMENTAL_NOTE
    assert n["cadet_experimental"] == CADET_EXPERIMENTAL_NOTE and "not yet tested" in CADET_EXPERIMENTAL_NOTE
    # W16: the Download panel's groups, each table named, each group a text in every language
    assert [g for g, _ in n["groups"]] == ["key", "peaks", "method", "details", "audit", "runlog"]
    assert all(k in n["tables"] for _, keys in n["groups"] for k in keys)
    assert all("dl.group." + g in texts for texts in _texts().values() for g, _ in n["groups"])


def test_download_takes_the_ticked_groups(session, tmp_path):
    # W16: unticked groups stay out; the curves and the always-in tables go in; all ticked: every table
    import zipfile
    from pycorn.run_export import GROUP_NAMES
    assert browser.export_options({"groups": list(GROUP_NAMES)}, "x").tables is None
    assert browser.export_options({"groups": []}, "x").tables == ["curves"]
    r = json.loads(session.download(0, "csv", str(tmp_path / "d"), {"groups": ["peaks"]}))
    files = {os.path.basename(n) for n in zipfile.ZipFile(r["path"]).namelist()}
    assert any(f.endswith("_peaks.csv") for f in files) and "column_and_timeline.csv" in files
    assert not any(f.endswith("_phases.csv") or f == "method_steps.csv" for f in files)


def test_page_default_columns_are_sent(session):
    # every default column of a wide table is a column the page gets for some sample run
    shown = {k: set(t["shown"]) for k, t in json.loads(browser.names())["tables"].items() if t["shown"]}
    sent = {k: set() for k in shown}
    for index in range(3):
        v = json.loads(session.view(index))
        for chrom in v["chromatograms"]:
            for key in sent:
                sent[key] |= set((chrom.get(key) or v["details"].get(key) or (v.get(key) if key == "variables" else None)
                                  or {"columns": []})["columns"])
    # M22: the window table comes from window_stats, not view
    sent["phase_windows"] |= set(json.loads(session.window_stats([0], WINDOWS, ["uv"]))["table"]["columns"])
    for key in shown:
        assert shown[key] <= sent[key], (key, shown[key] - sent[key])


def test_freeze_through_of_every_table_is_sent_and_is_a_column(session):
    # W13: the page freezes the columns up to and including this one; it must be one the table has
    from pycorn.export_tables import TABLES
    tables = json.loads(browser.names())["tables"]
    assert set(tables) == set(TABLES)
    assert all(isinstance(t["freeze_through"], str) for t in tables.values())
    assert tables["phases"]["freeze_through"] == "name" and tables["method_steps"]["freeze_through"] == "block"
    assert tables["curves"]["freeze_through"] == ""
    seen = {}
    for index in range(3):
        v = json.loads(session.view(index))
        for chrom in v["chromatograms"]:
            for key, t in chrom.items():
                if isinstance(t, dict) and "columns" in t:
                    seen.setdefault(key, set()).update(t["columns"])
        for key, t in v["details"].items():
            seen.setdefault(key, set()).update(t["columns"])
        seen.setdefault("variables", set()).update(v["variables"]["columns"])
        seen.setdefault("warnings", set()).update(v["warnings"]["columns"])
    for key, columns in seen.items():
        if key in tables and tables[key]["freeze_through"]:
            assert tables[key]["freeze_through"] in columns, key


def test_default_columns_of_the_method_tables_in_their_order(session):
    # the page shows defaults in their listed order; for the method information tables the csv
    # and Excel files hold them in that order too
    names = json.loads(browser.names())["tables"]
    order = {k: list(names[k]["shown"]) for k in ("variables", "method_steps", "column", "calibration", "peaks")}
    for index in range(3):
        v = json.loads(session.view(index))
        tables = [dict(chrom, **v["details"], variables=v["variables"]) for chrom in v["chromatograms"]]
        for found in tables:
            for key, shown in order.items():
                columns = (found.get(key) or {"columns": []})["columns"]
                assert [c for c in columns if c in shown] == [c for c in shown if c in columns], key


def browser_version():
    from pycorn.version import __version__
    return __version__


def test_download_core(session, tmp_path):
    no_geometry = json.loads(session.download(0, "core", str(tmp_path / "a"), json.dumps({"version": "6"})))
    if "error" in no_geometry:  # the sample links no column: the user is told what to give
        assert "bed-height" in no_geometry["error"] or "bed_height" in no_geometry["error"]
    options = {"version": "5", "bed_height": [10, "cm"], "diameter": [0.5, "cm"], "particle_diameter": [None, "um"],
               "overrides": {"input": {"model": {"unit_001": {"col_porosity": 0.37}}}}}
    r = json.loads(session.download(0, "core", str(tmp_path / "b"), json.dumps(options)))
    assert r["name"].endswith("_core_v5.json"), r
    core = json.load(open(r["path"], encoding="utf-8"))
    column = next(u for k, u in core["input"]["model"].items() if k.startswith("unit_") and "col_porosity" in u)
    assert column["col_porosity"] == 0.37  # D28: passed on unchanged
    from pycorn.exporters.cadet.api import CADET_EXPERIMENTAL_NOTE
    assert any(CADET_EXPERIMENTAL_NOTE in w for w in r["warnings"])  # W15
    assert json.loads(session.download(0, "core", str(tmp_path / "c"), "{}"))["error"] == \
        "pick the CADET-Core version: 5 or 6"
    assert "unknown download" in json.loads(session.download(0, "pdf", str(tmp_path / "d")))["error"]


def test_session_loads_a_fresh_run_per_download(session, tmp_path):
    # caller geometry changes a run's axes (D26); the page's shown run must not change with it
    session.view(0)
    shown = session._shown[1]
    session.download(0, "core", str(tmp_path / "g"), {"version": "5", "bed_height": [10, "cm"], "diameter": [0.5, "cm"]})
    assert session._shown[1] is shown


def test_progress_reports_the_steps_of_a_view_and_a_download(tmp_path):
    # the page's status line while Python works (web worker): known keys only, values as JSON
    steps = []
    s = browser.Session(progress=lambda key, values: steps.append((key, json.loads(values))))
    s.add(FILES[:1])
    assert steps == []  # adding reports nothing itself: the page counts the files
    s.view(0)
    keys = [k for k, _ in steps]
    assert keys[0] == "step.reading_run" and keys[-1] == "step.details"
    assert {"step.tables", "step.plot"} <= set(keys)
    assert all(v.get("chromatogram") for k, v in steps if k in ("step.tables", "step.plot"))
    del steps[:]
    s.download(0, "csv", str(tmp_path / "c"))
    assert [k for k, _ in steps] == ["step.reading_run", "step.writing", "step.zipping"]
    assert steps[1][1] == {"kind": "csv"}
    del steps[:]
    s.export_start(None, kind="xlsx", folder=str(tmp_path / "all"))
    s.export_one(0)
    s.export_finish()
    assert [k for k, _ in steps] == ["step.reading_run", "step.writing", "step.zipping"]
    assert {k for k, _ in steps} <= set(browser.STEPS)


def test_table_cells_are_json_ready():
    frame = pd.DataFrame({"a": [1.5, np.nan], "t": pd.to_datetime(["2024-01-02 03:04:05", None]),
                          "b": [np.int64(2), None], "s": ["x", None]})
    t = browser.table(frame)
    assert t["columns"] == ["a", "t", "b", "s"]
    assert t["rows"] == [[1.5, "2024-01-02T03:04:05", 2, "x"], [None, None, None, None]]
    assert browser.table(None) == {"columns": [], "rows": [], "origins": [], "meanings": [], "checks": [],
                                   "decimals": []}
    json.dumps(t, allow_nan=False)


def test_template_has_one_wheel_marker():
    # tools/build_web_page.py puts the wheels there
    with open(TEMPLATE, encoding="utf-8") as f:
        assert f.read().count("__PYCORN_WHEELS__") == 1


def _build_module():
    import importlib.util
    spec = importlib.util.spec_from_file_location("build_web_page", os.path.abspath("../tools/build_web_page.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_template_has_a_content_security_policy_with_the_script_hash():
    # the policy is what lets the browser enforce "files are never sent anywhere": it names the two
    # CDNs and this page's own address, nothing else; the build puts the hash of the page's script in
    with open(TEMPLATE, encoding="utf-8") as f:
        page = f.read()
    policy = re.findall(r'http-equiv="Content-Security-Policy" content="([^"]*)"', page)
    assert len(policy) == 1 and page.count("__PYCORN_SCRIPT_HASH__") == 1
    assert "default-src 'none'" in policy[0]
    assert "connect-src 'self' https://cdn.jsdelivr.net;" in policy[0]
    # the CDN scripts the policy allows are the ones the page loads, no others: Plotly by a script
    # element, Pyodide by the engine (web worker), fetched with its integrity hash
    loaded = set(re.findall(r'<script src="(https://[^/]+)/', page))
    pyodide = re.findall(r'const PYODIDE = "(https://[^/]+)/[^"]*";\s*const PYODIDE_SRI = "(sha384-[A-Za-z0-9+/=]{64})";', page)
    assert len(pyodide) == 1
    loaded.add(pyodide[0][0])
    assert loaded == {"https://cdn.jsdelivr.net", "https://cdn.plot.ly"}
    assert "worker-src blob:" in policy[0]
    for host in loaded:
        assert host in policy[0]
    # the browser hashes the script text byte for byte; one program script, found the way the build finds it
    build = _build_module()
    assert re.fullmatch(r"'sha256-[A-Za-z0-9+/]{43}='", build.script_hash(page))


def _texts():
    folder = os.path.abspath("../web/i18n")
    out = {}
    for name in sorted(os.listdir(folder)):
        if name.endswith(".json"):
            with open(os.path.join(folder, name), encoding="utf-8") as f:
                out[name[:-5]] = json.load(f)
    return out


def test_template_texts_are_all_in_en_json():
    # every key the page asks for exists; every key in en.json is used (no stale texts)
    with open(TEMPLATE, encoding="utf-8") as f:
        page = f.read()
    assert page.count("__PYCORN_STRINGS__") == 1
    used = set(re.findall(r'data-i18n(?:-html)?="([\w.]+)"', page))
    used |= set(re.findall(r'\bi18n\("([\w.]+)"', page))
    used |= set(re.findall(r'\bjob\("([\w.]+)"', page))  # the engine's progress lines
    used |= set(re.findall(r'\bmessage\("([\w.]+)"', page))  # the page's and engine's own errors ({key, values, text})
    used |= set(browser.STEPS)  # Python's progress steps (Session.progress)
    used |= set(browser.MESSAGES)  # Python's messages (browser.message)
    en = _texts()["en"]
    assert used - set(en) == set()
    assert set(en) - used - {"lang.name"} == set()


# new English keys of 0.33.0 (Track 3) that Track 5 has not translated yet; empty this once it has
PENDING_BLOCKS = ()


def test_other_languages_match_en_json():
    # a translation holds only en.json's keys and the same {placeholders}: a missing key falls back
    # to English, a renamed placeholder would show as text
    texts = _texts()
    en = texts.pop("en")
    holes = lambda s: sorted(re.findall(r"\{(\w+)\}", s))
    assert {"de", "es", "fr", "it", "pt", "sv"} <= set(texts)
    tags = lambda s: sorted(re.findall(r"</?\w+", s))
    for code, strings in texts.items():
        # Track 5 translates a release's new blocks after the English text is in: until then a
        # translation may lack the keys of a block listed in PENDING_BLOCKS (it shows English)
        pending = {k for k in en if k.startswith(PENDING_BLOCKS)}
        assert set(strings) <= set(en) and set(en) - set(strings) <= pending, (code, sorted(set(strings) ^ set(en)))
        strings = {k: v for k, v in strings.items()}
        assert strings["lang.name"] and strings["translation.note"], code  # picker label, machine-translation note
        for key, text in strings.items():
            if key != "lang.name":
                assert tags(text) == tags(en[key]), (code, key)
        for key, text in strings.items():
            assert holes(text) == holes(en[key]), (code, key)
