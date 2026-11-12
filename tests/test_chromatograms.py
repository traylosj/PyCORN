"""
W17 (owner, 2026-11-11): every chromatogram of a result selectable everywhere; the run's original
chromatogram the default. Tested on a synthetic result (`multichrom_fixture`: sample.zip plus an
imported, a copied-in and a derived chromatogram), never on data from a database.
"""
import json
import os
import zipfile

import numpy as np
import pandas as pd
import pytest

import pycorn
from pycorn import browser, cli, open_run
from pycorn import run_export as rx
from pycorn.file_catalog import catalog_from_files, file_chromatograms
from pycorn.model import chromatogram_kind, describe_chromatograms, select_chromatograms

import multichrom_fixture as mf

ORIGINAL = "Chrom.1"
GEOMETRY = dict(bed_height=(10.0, "cm"), diameter=(0.3, "cm"))


@pytest.fixture(scope="module")
def multi(tmp_path_factory):
    return str(mf.build(tmp_path_factory.mktemp("multi") / "multi.zip"))


@pytest.fixture(scope="module")
def run(multi):
    return open_run(multi)


# -- the model -----------------------------------------------------------------

def test_kind_rule():
    assert chromatogram_kind({}, [True, True], [True]) == "original"
    assert chromatogram_kind({}, [None], []) == "original"  # not stored: not taken as derived
    assert chromatogram_kind({}, [], []) == "original"
    assert chromatogram_kind({}, [False, True], [False]) == "original"  # a derived curve on the run's own
    assert chromatogram_kind({}, [False], [False]) == "derived"
    assert chromatogram_kind({"OriginalResultID": 7}, [False], [False]) == "imported"
    assert chromatogram_kind({"OriginalResultID": ""}, [False], []) == "derived"


def test_classification(run):
    d = {x["name"]: x for x in run.describe_chromatograms()}
    assert list(d) == [ORIGINAL, mf.IMPORTED, mf.COPY, mf.DERIVED]
    assert [x["kind"] for x in d.values()] == ["original", "imported", "imported", "derived"]
    assert d[mf.IMPORTED]["source_result_id"] == mf.SOURCE_ID
    assert d[mf.IMPORTED]["source_result_name"] == mf.SOURCE_NAME
    assert d[mf.IMPORTED]["copy_of_run"] is False and d[mf.COPY]["copy_of_run"] is True
    assert d[ORIGINAL]["copy_of_run"] is None and d[mf.DERIVED]["source_result_id"] is None
    assert d[mf.COPY]["label"].endswith("(a copy of this run)")
    assert d[mf.IMPORTED]["label"] == "imported from %s" % mf.SOURCE_NAME
    assert run.chromatograms[mf.IMPORTED].kind == "imported"
    assert run.chromatograms[mf.IMPORTED].source_result == {"id": mf.SOURCE_ID, "name": mf.SOURCE_NAME}
    assert run.chromatograms[ORIGINAL].source_result is None


def test_copy_of_run_by_result_id():
    items = [("a", "original", {}), ("b", "imported", {"OriginalResultID": 5, "OriginalResultName": "x"}),
             ("c", "imported", {"OriginalResultID": 6, "OriginalResultName": "x"})]
    d = describe_chromatograms(items, result_id=5, result_name="x")
    assert [x["copy_of_run"] for x in d] == [None, True, False]  # the ID wins over the name


def test_light_read_classifies_as_the_full_read(multi, run):
    assert file_chromatograms(multi) == run.describe_chromatograms()
    assert catalog_from_files([multi])["n_chromatograms"].tolist() == [4]


def test_selection(run):
    assert run.select_chromatograms() == [ORIGINAL]
    assert run.chromatogram().name == ORIGINAL
    assert run.select_chromatograms("imported") == [mf.IMPORTED, mf.COPY]
    assert run.select_chromatograms(["derived", ORIGINAL]) == [ORIGINAL, mf.DERIVED]  # stored order
    assert run.select_chromatograms("all") == list(run.chromatograms)
    assert run.select_chromatograms([mf.DERIVED]) == [mf.DERIVED]
    with pytest.raises(KeyError, match="no chromatogram 'nope'"):
        run.select_chromatograms(["nope"])
    assert run.chromatogram_selection(["nope", "derived"]) == ([mf.DERIVED], ["nope"])


def test_selection_without_an_original():
    d = [{"name": "x", "kind": "derived"}, {"name": "y", "kind": "imported"}]
    assert select_chromatograms(d) == (["x"], [])  # the first, as before W17


def test_role_lookups_follow_the_chromatogram(run):
    original, imported = run.chromatograms[ORIGINAL], run.chromatograms[mf.IMPORTED]
    assert original.curves_with_role("uv") and imported.curves_with_role("uv")
    other = [c.name for c in imported.other_run_curves()]
    assert len(other) == 1 and other[0] in imported.curves  # kept as a curve ...
    assert [c.name for c in imported.curves_with_role("uv")] == [c.name for c in original.curves_with_role("uv")
                                                                  if c.name not in other]  # ... without a role
    assert imported.curves_with_role("uv", original_only=True) == []  # asked for: original curves only
    assert imported.curves_of_type("UV") and not imported.curves_of_type("UV", original_only=True)
    assert not original.other_run_curves() and not run.chromatograms[mf.COPY].other_run_curves()


def test_window_stats_on_a_picked_chromatogram(run):
    from pycorn.window_stats import window_stats_rows
    rows, _ = window_stats_rows(run, run.chromatograms[mf.DERIVED], ["w=Elution:0:1 CV"], ["uv"])
    base, _ = window_stats_rows(run, run.chromatogram(), ["w=Elution:0:1 CV"], ["uv"])
    assert len(rows) == len(base)


# -- an imported chromatogram's run log (owner, 2026-11-11) -------------------------

THIS_RESULT = "taken from this result"  # the warning for this result's run-level data used (owner)


def _notes(warnings, text=THIS_RESULT):
    return [w for w in warnings if "imported chromatogram" in w and text in w]


def test_imported_run_log_by_stored_type(run):
    original, imported = run.chromatograms[ORIGINAL], run.chromatograms[mf.IMPORTED]
    assert original.run_log().type == "Logbook" and original.run_log_source() is None
    assert imported.run_log().type == "ImportedLogbook"
    assert imported.run_log().name == original.run_log().name  # the name is the instrument's, not a key
    source = imported.run_log_source()
    assert (source["source_result_id"], source["source_result_name"]) == (mf.SOURCE_ID, mf.SOURCE_NAME)
    assert run.chromatograms[mf.DERIVED].run_log().type == "Logbook"  # unchanged: as stored
    assert run.chromatograms[mf.DERIVED].run_log_source() is None


def test_who_holds_the_run(run):
    assert run.holds_run_of(run.chromatograms[ORIGINAL]) and run.holds_run_of(run.chromatograms[mf.DERIVED])
    assert run.holds_run_of(run.chromatograms[mf.COPY])  # a copy of this same run
    assert not run.holds_run_of(run.chromatograms[mf.IMPORTED])


@pytest.mark.parametrize("kind", ["phase", "injection", "fraction"])
def test_imported_tables_are_the_source_runs(run, kind):
    original = run.chromatograms[ORIGINAL]._table(kind, None)
    imported = run.chromatograms[mf.IMPORTED]._table(kind, None)
    copy = run.chromatograms[mf.COPY]._table(kind, None)
    assert len(original)
    # inlets from this result's configuration (owner: used, with a warning in the sections)
    pd.testing.assert_frame_equal(imported.astype(str), original.astype(str))
    pd.testing.assert_frame_equal(copy.astype(str), original.astype(str))
    # another method run's marks and a pooled curve are not rows
    assert not {mf.OTHER_RUN_INJECTION, mf.POOLED_FRACTION} & set(imported["event_curve"])


def test_imported_axes_follow_its_run_log(run):
    original, imported = run.chromatograms[ORIGINAL], run.chromatograms[mf.IMPORTED]
    assert imported.column_path["bypass"] == original.column_path["bypass"] and original.column_path["switches"]
    assert len(imported.timeline["anchors"]) == len(original.timeline["anchors"]) > 0
    assert imported.timeline["pauses"] == original.timeline["pauses"]
    for name, curve in original.curves.items():
        other = imported.curves[name]
        for axis in ("cv", "utc"):
            if getattr(curve, axis) is not None:
                np.testing.assert_array_equal(getattr(other, axis), getattr(curve, axis))
    sources = [c.source for c in imported.column_volume_info["candidates"]]
    assert sources == [c.source for c in original.column_volume_info["candidates"]]  # this result's too
    assert "run_log" in sources and imported.column_volume_info["source"] == "curves"
    assert not _notes(run.warnings)  # Vc from its own curves: nothing taken from this result


def test_imported_vc_from_this_result_is_warned(run):
    import copy
    from pycorn.column_volume import resolve_column_volume
    bare = copy.deepcopy(run)
    for chrom in bare.chromatograms.values():  # no Vc on the curves, run log or peak tables
        for item in list(chrom.curves.values()) + list(chrom.event_curves.values()):
            item.column_volume = None
        chrom.peak_tables = []
    imported = resolve_column_volume(bare, bare.chromatograms[mf.IMPORTED])
    assert imported["source"] == "run_information"
    assert len(_notes(imported["warnings"], "Vc (run_information) taken from this result")) == 1
    for name in (ORIGINAL, mf.COPY):
        info = resolve_column_volume(bare, bare.chromatograms[name])
        assert info["source"] == "run_information" and not _notes(info["warnings"])


def test_imported_utc_start_from_this_result_is_warned(run):
    import copy
    from pycorn.timeline import build_timeline
    bare = copy.deepcopy(run)
    bare.result["run_information"] = list(bare.result.get("run_information") or []) + [
        {"type": "RunStartDate", "value": "03/10/2023 16:30:07 -05:00"}]
    for chrom in bare.chromatograms.values():  # no grid start and no 'Method Run' anchor: RunStartDate decides
        for item in list(chrom.curves.values()) + list(chrom.event_curves.values()):
            for key in ("ChromatogramStartTime", "MethodStartTime"):
                item.xml.pop(key, None)
        for ec in chrom.run_log_curves():
            ec.events = [e for e in ec.events if not (e.text or "").startswith("Method Run")]
    tl = build_timeline(bare, bare.chromatograms[mf.IMPORTED])
    assert tl["start_source"] == "RunStartDate" and len(_notes(tl["warnings"], "the UTC start")) == 1
    assert not _notes(build_timeline(bare, bare.chromatograms[mf.COPY])["warnings"])


def test_imported_sections_are_labelled(run):
    from pycorn.run_log_sections import run_log_sections
    original = run_log_sections(run, run.chromatograms[ORIGINAL])
    imported = run_log_sections(run, run.chromatograms[mf.IMPORTED])
    copy = run_log_sections(run, run.chromatograms[mf.COPY])
    assert original["run_log_source"] is None and not any("imported chromatogram" in w for w in original["warnings"])
    assert len(imported["sections"]) == len(original["sections"]) > 0
    assert imported["run_log_source"]["source_result_id"] == mf.SOURCE_ID
    assert any(w.startswith("imported chromatogram: run log") and mf.SOURCE_NAME in w for w in imported["warnings"])
    # inlets from this result's configuration, labels as the rules give, one warning
    assert [s.inlet_states for s in imported["sections"]] == [s.inlet_states for s in original["sections"]]
    assert len(_notes(imported["warnings"], "the system configuration")) == 1
    assert not _notes(copy["warnings"])  # a copy of this same run: nothing to warn


def test_imported_method_hints_from_this_result(run):
    from pycorn.method_hints import method_hints
    hints = method_hints(run, run.chromatograms[mf.IMPORTED])
    assert hints.available and len(_notes(hints.warnings, "the method taken")) == 1
    copy = method_hints(run, run.chromatograms[mf.COPY])
    assert copy.available and not _notes(copy.warnings)
    assert [r["status"] for r in hints.blocks] == [r["status"] for r in method_hints(run, run.chromatogram()).blocks]


def test_imported_window_stats(run):
    from pycorn.window_stats import window_stats_rows
    imported = run.chromatograms[mf.IMPORTED]
    rows, notes = window_stats_rows(run, imported, ["w=Run:0:1 ml"], ["uv"])
    base, base_notes = window_stats_rows(run, run.chromatogram(), ["w=Run:0:1 ml"], ["uv"])
    other = {c.name for c in imported.other_run_curves()}
    kept = [r for r in base if r["curve"] not in other]
    assert len(rows) == len(kept) > 0 and len(kept) < len(base)  # the other run's UV curve has no role
    assert [r["mean"] for r in rows] == [r["mean"] for r in kept]
    assert any(n.startswith("imported chromatogram: run log") for n in notes)
    assert not any("imported chromatogram" in n for n in base_notes)


def test_cadet_experiment_of_an_imported_chromatogram(run):
    import copy
    from pycorn.exporters.cadet import build_experiment
    imported = run.chromatograms[mf.IMPORTED]
    exp = build_experiment(run, imported, **GEOMETRY)
    base = build_experiment(run, run.chromatogram(), **GEOMETRY)
    assert len(exp.sections) == len(base.sections) > 0
    assert exp.source["run_log_source"]["source_result_id"] == mf.SOURCE_ID
    assert "run_log_source" not in base.source  # the original's experiment as before
    assert not _notes(base.warnings)
    # this result's data used, each once with a warning
    assert exp.system["mixer_m3"] == base.system["mixer_m3"] and exp.column["sources"] == base.column["sources"]
    assert len(_notes(exp.warnings, "the method taken")) == 1
    assert len(_notes(exp.warnings, "the system configuration")) == 1
    assert len(_notes(exp.warnings, "the mixer volume")) == (base.system["mixer_source"] is not None)
    assert len(_notes(exp.warnings, "the column type")) == ("ColumnTypeData" in base.column["sources"].values())
    assert len(_notes(exp.warnings, "the delay volume")) == bool(base.system["delay_volume_hints"])
    # UV signals: the other run's UV curve has no role
    other = {c.name for c in imported.other_run_curves()}
    assert [s.name for s in exp.signals] == [s.name for s in base.signals if s.name not in other]
    assert not _notes(build_experiment(run, run.chromatograms[mf.COPY], **GEOMETRY).warnings)
    # truly no run log: the clear error stays
    bare = copy.deepcopy(run)
    chrom = bare.chromatograms[mf.IMPORTED]
    for key in [k for k, ec in chrom.event_curves.items() if ec.type == "ImportedLogbook"]:
        del chrom.event_curves[key]
    with pytest.raises(ValueError, match="no run-log sections"):
        build_experiment(bare, chrom, **GEOMETRY)


def test_export_of_an_imported_chromatogram(run, tmp_path):
    written = rx.write_run(run, tmp_path / "r", ["csv"], options=rx.ExportOptions(chromatograms=[mf.IMPORTED]))
    for table in ("phases", "injections", "fractions", "run_log_sections"):
        assert "Imported run_%s.csv" % table in written, table
    w = pd.read_csv(tmp_path / "r" / "warnings.csv").fillna("")
    mine = [m for c, m in zip(w["chromatogram"], w["message"]) if c == mf.IMPORTED]
    assert any(m.startswith("imported chromatogram: run log") for m in mine)
    other = [m for s, m in zip(w["source"], w["message"]) if s == "curves" and "another method run" in m]
    assert len(other) == 1 and other[0].startswith("imported chromatogram: 1 curve(s)")
    curves = pd.read_csv(tmp_path / "r" / "Imported run.csv", nrows=3)
    name = run.chromatograms[mf.IMPORTED].other_run_curves()[0].name
    assert any(c.startswith(name) for c in curves.columns)  # the other run's curve is still written

# -- export ----------------------------------------------------------------------

def test_export_default_is_the_original_chromatogram(run, tmp_path):
    written = rx.write_run(run, tmp_path / "r", ["csv"])
    assert "Chrom.1.csv" in written and not any(f.startswith(("Imported", "Copy", "Derived")) for f in written)
    info = pd.read_csv(tmp_path / "r" / "column_and_timeline.csv")
    assert info["chromatogram"].tolist() == [ORIGINAL] and info["chromatogram_kind"].tolist() == ["original"]
    w = pd.read_csv(tmp_path / "r" / "warnings.csv").fillna("")
    note = [m for s, m in zip(w["source"], w["message"]) if s == "chromatograms"]
    assert len(note) == 1 and note[0].startswith("3 other chromatogram(s) not written") and mf.IMPORTED in note[0]


def test_export_picked_chromatograms(run, tmp_path):
    opts = rx.ExportOptions(chromatograms=["imported", mf.DERIVED, "nope"])
    written = rx.write_run(run, tmp_path / "r", ["csv"], options=opts)
    assert "Imported run.csv" in written and "Copy of the run.csv" in written and "Derived.csv" in written
    assert "Chrom.1.csv" not in written
    info = pd.read_csv(tmp_path / "r" / "column_and_timeline.csv").fillna("")
    assert info["chromatogram_kind"].tolist() == ["imported", "imported", "derived"]
    assert info["source_result_name"].tolist()[:1] == [mf.SOURCE_NAME]
    curves = pd.read_csv(tmp_path / "r" / "Imported run.csv", nrows=5)
    assert any(c.startswith("UV 1_280") for c in curves.columns)  # its non-original curves are written
    w = pd.read_csv(tmp_path / "r" / "warnings.csv").fillna("")
    assert any(s == "chromatograms" and "no chromatogram 'nope'" in m for s, m in zip(w["source"], w["message"]))
    assert not any("not written:" in m for m in w["message"])  # no default note when picked


def test_export_all_keeps_the_default_stems(run, tmp_path):
    a = rx.write_run(run, tmp_path / "a", ["csv"])
    b = rx.write_run(run, tmp_path / "b", ["csv", "xlsx"], options=rx.ExportOptions(chromatograms=["all"]))
    assert {f for f in a if f.startswith("Chrom.1")} <= set(b)
    assert sum(f.endswith(".xlsx") for f in b) == 4


def test_export_options_reject_an_empty_selection(run, tmp_path):
    with pytest.raises(ValueError, match="chromatograms= is empty"):
        rx.write_run(run, tmp_path / "r", ["csv"], options=rx.ExportOptions(chromatograms=[]))


def test_cli_chromatogram_flag(multi, tmp_path):
    assert cli.main(["export", multi, "--out", str(tmp_path / "d")]) == 0
    files = {p.name for p in (tmp_path / "d").rglob("*.csv")}
    assert "Chrom.1.csv" in files and "Derived.csv" not in files
    assert cli.main(["export", multi, "--out", str(tmp_path / "p"), "--chromatogram", "derived",
                     "--chromatogram", mf.IMPORTED]) == 0
    files = {p.name for p in (tmp_path / "p").rglob("*.csv")}
    assert {"Derived.csv", "Imported run.csv"} <= files and "Chrom.1.csv" not in files
    args = cli.build_parser().parse_args(["export", multi, "--out", "x", "--chromatogram", "all"])
    assert cli.export_options(args).chromatograms == ["all"]
    find = cli.build_parser().parse_args(["find", "--chromatograms", "2.."])  # the count filter is unchanged
    assert find.chromatograms == "2.."


def test_export_cadet_default_and_pick(run, tmp_path):
    out = pycorn.export_cadet(run, str(tmp_path / "d"), core_version="6", cadet_process=False, **GEOMETRY)
    assert out["chromatograms"] == [ORIGINAL]
    assert any("other chromatogram(s) not written" in w for w in out["warnings"])
    two = pycorn.export_cadet(run, str(tmp_path / "t"), core_version="6", cadet_process=False,
                              chromatogram=[ORIGINAL, "derived"], **GEOMETRY)
    assert two["chromatograms"] == [ORIGINAL, mf.DERIVED]
    assert sorted(os.listdir(tmp_path / "t")) == ["Chrom.1_core_v6.json", "Derived_core_v6.json"]
    with pytest.raises(KeyError):
        pycorn.export_cadet(run, str(tmp_path / "x"), core_version="6", cadet_process=False, chromatogram="nope",
                            **GEOMETRY)


# -- the page ---------------------------------------------------------------------

def test_page_rows_list_the_chromatograms(multi):
    s = browser.Session()
    rows = json.loads(s.add([multi, os.path.abspath("../samples/sample.zip")]))["runs"]
    assert [c["kind"] for c in rows[0]["chromatograms"]] == ["original", "imported", "imported", "derived"]
    assert rows[0]["chromatograms"][2]["copy_of_run"] is True
    assert rows[1]["chromatograms"] == []  # one chromatogram: no picker


def test_page_view_follows_the_ticked_chromatograms(multi):
    s = browser.Session()
    s.add([multi])
    v = json.loads(s.view(0))
    assert [c["name"] for c in v["chromatograms"]] == [ORIGINAL]
    assert len(v["all_chromatograms"]) == 4
    assert ["Chromatogram kind", "original"] in v["chromatograms"][0]["summary"]
    v = json.loads(s.view(0, json.dumps([mf.IMPORTED, ORIGINAL])))
    assert [c["name"] for c in v["chromatograms"]] == [ORIGINAL, mf.IMPORTED]
    imported = v["chromatograms"][1]
    assert imported["kind"] == "imported" and imported["source_result_name"] == mf.SOURCE_NAME
    assert any(c["shown"] for c in imported["plot"]["curves"])  # its non-original curves are drawn
    assert json.loads(s.view(0, json.dumps(["nope"])))["chromatograms"][0]["name"] == ORIGINAL


def test_page_downloads_follow_the_ticked_chromatograms(multi, tmp_path):
    s = browser.Session()
    s.add([multi])
    r = json.loads(s.download(0, "csv", str(tmp_path / "a"), {"chromatograms": [mf.DERIVED]}))
    with zipfile.ZipFile(r["path"]) as z:
        names = [n.split("/")[-1] for n in z.namelist()]
    assert "Derived.csv" in names and "Chrom.1.csv" not in names
    r = json.loads(s.download(0, "csv", str(tmp_path / "b"), {}))
    with zipfile.ZipFile(r["path"]) as z:
        names = [n.split("/")[-1] for n in z.namelist()]
    assert "Chrom.1.csv" in names and "Derived.csv" not in names
    start = json.loads(s.export_start([0], kind="csv", folder=str(tmp_path / "all"),
                                      options={"chromatograms": {"0": [ORIGINAL, mf.IMPORTED]}}))
    assert start["runs"] == 1
    s.export_one(0)
    r = json.loads(s.export_finish())
    with zipfile.ZipFile(r["path"]) as z:
        names = [n.split("/")[-1] for n in z.namelist()]
    assert "Chrom.1.csv" in names and "Imported run.csv" in names and "Derived.csv" not in names


def test_page_overlay_of_two_chromatograms_of_one_run(multi):
    s = browser.Session()
    s.add([multi])
    out = json.loads(s.overlay([[0, ORIGINAL], [0, mf.IMPORTED]]))
    assert [r["pick"] for r in out["runs"]] == [[0, ORIGINAL], [0, mf.IMPORTED]]
    assert [r["kind"] for r in out["runs"]] == ["original", "imported"]
    assert all(r["curves"] for r in out["runs"])
    plain = json.loads(s.overlay([0]))
    assert plain["runs"][0]["chromatogram"] == ORIGINAL and plain["runs"][0]["pick"] == [0, None]
    capped = json.loads(s.overlay([[0, name] for name in [ORIGINAL, mf.IMPORTED, mf.COPY, mf.DERIVED]] * 3))
    assert capped["requested"] == 12 and len(capped["runs"]) == browser.OVERLAY_MAX  # run x chromatogram
    bad = json.loads(s.overlay([[0, "nope"]]))
    assert bad["failed"][0]["pick"] == [0, "nope"] and not bad["runs"]
    assert json.loads(s.overlay_run(0, mf.DERIVED))["chromatogram"] == mf.DERIVED
