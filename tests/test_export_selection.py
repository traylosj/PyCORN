# -*- coding: utf-8 -*-
"""
Track 9, A5: the batch export as a function (`run_export.export_selection`), the export
options as data (`ExportOptions.to_dict` / `from_dict`, `pycorn export --options`) and the
page's Download all writing the same index.csv columns as `pycorn export`.
"""
import json
import os
import zipfile

import pandas as pd
import pytest

import pycorn
from pycorn import browser, catalog_from_files, cli
from pycorn import run_export as rx
from pycorn.run_export import INDEX_COLUMNS, ExportOptions, export_selection

SAMPLES = os.path.abspath("../samples")
TWO = [os.path.join(SAMPLES, n) for n in ("sample.zip", "u711_manual_run.zip")]


@pytest.fixture(autouse=True)
def _no_settings_from_outside(monkeypatch):
    for key in list(os.environ):
        if key.upper().startswith("UNICORN_") or key.upper() == "PYCORN_ENV_FILE":
            monkeypatch.delenv(key)


def _full_options():
    return ExportOptions(
        units={"volume": "L"}, method_hints=True, mask_identifying=True, path_length_cm=0.2,
        epsilon={280.0: (1.4, "ml/(mg*cm)")}, epsilon_molar={260.0: (43824.0, "1/(M*cm)")},
        molecular_weight=(66000.0, "g/mol"), uv_response=0.92, bypass_uv=True, sample_concentration=(1.2, "mg/ml"),
        loop_volume=(20.0, "ul"), absorbance_limit=(2500.0, "mAU"), windows=["load=Sample Application:1:3 CV"],
        window_curves=["uv"], bed_height=(10.0, "cm"), diameter=(0.5, "cm"), particle_diameter=(34.0, "um"),
        override_geometry=True, cadet=True, cadet_core="6", core_format="hdf5", mixer_volume=(0.6, "ml"),
        tubing_volume=(0.2, "ml"), sample_inlets=["A5"], system_volumes=False,
        core_overrides={"input": {"model": {"x": 1}}}, process_overrides={"a": [1, 2]}, tables=["key", "peaks"],
        curves=["UV"], curve_roles=["conductivity"], curves_missing_ok=True, max_points=500, method="hybrid",
        chromatograms=["all"])


# --- options as data ------------------------------------------------------------------------

def test_options_round_trip_through_json():
    options = _full_options()
    d = options.to_dict()
    assert "route" not in d and "command" not in d  # provenance only
    assert list(d) == [f for f in ExportOptions.__dataclass_fields__ if f not in ("route", "command")]
    assert d["bed_height"] == [10.0, "cm"] and d["epsilon"] == {"280": [1.4, "ml/(mg*cm)"]}
    back = ExportOptions.from_dict(json.loads(json.dumps(d)))
    assert back == options
    assert ExportOptions.from_dict(ExportOptions().to_dict()) == ExportOptions()
    assert ExportOptions.from_dict({}) == ExportOptions()


def test_options_partial_dict_and_provenance_block():
    assert ExportOptions.from_dict({"method_hints": True, "cadet_core": 6}) == ExportOptions(method_hints=True,
                                                                                          cadet_core="6")
    prov = rx.provenance(ExportOptions(epsilon={280.0: (1.4, "ml/(mg*cm)")}, bed_height=(1.0, "cm")))
    assert ExportOptions.from_dict(json.loads(json.dumps(prov["options"]))) == ExportOptions(
        epsilon={280.0: (1.4, "ml/(mg*cm)")}, bed_height=(1.0, "cm"))


@pytest.mark.parametrize("d, words", [
    ({"method_hint": True}, "unknown key(s) 'method_hint'"),
    ({"bed_height": 10}, "bed_height must be [value, \"unit\"]"),
    ({"bed_height": [10, "ml"]}, "bed_height"),
    ({"bed_height": [-1, "cm"]}, "bed_height must be above 0"),
    ({"method_hints": "yes"}, "method_hints must be true or false"),
    ({"max_points": 1.5}, "max_points must be a whole number"),
    ({"curves": "UV"}, "curves must be a list of texts"),
    ({"epsilon": {"x": [1, "ml/(mg*cm)"]}}, "epsilon: 'x' is not a wavelength"),
    ({"units": {"volume": "kg"}}, "options: units:"),
    ({"tables": ["nope"]}, "unknown table(s) nope"),
])
def test_options_errors_name_the_key(d, words):
    with pytest.raises(ValueError) as e:
        ExportOptions.from_dict(d)
    assert words in str(e.value)


def test_options_not_an_object():
    with pytest.raises(ValueError, match="one JSON object"):
        ExportOptions.from_dict([1])


# --- export_selection ------------------------------------------------------------------------

def test_export_selection_writes_runs_index_and_options(tmp_path):
    assert "export_selection" in pycorn.__all__ and pycorn.export_selection is export_selection
    catalog = catalog_from_files(TWO)
    events = []
    options = ExportOptions(method_hints=True, tables=["key"])
    index = export_selection(catalog, tmp_path / "out", ["csv"], options=options, on_run=events.append)
    assert list(index.columns[:6]) == list(INDEX_COLUMNS)
    assert list(index.columns[6:]) == list(catalog.columns)
    assert list(index["export_status"]) == ["written", "written"]
    on_disk = pd.read_csv(tmp_path / "out" / "index.csv", dtype=str).fillna("")
    assert list(on_disk.columns) == list(index.columns)
    assert list(on_disk["export_files"]) == list(index["export_files"])
    stored = json.loads((tmp_path / "out" / "options.json").read_text(encoding="utf-8"))
    assert ExportOptions.from_dict(stored) == options
    kinds = [e["event"] for e in events]
    assert kinds[0] == "selected" and kinds.count("loaded") == 2 and kinds.count("row") == 2
    assert events[0]["to_export"] == 2 and events[0]["labels"] == ["zip", "zip"]
    rows = [e for e in events if e["event"] == "row"]
    assert all(list(e["row"]) == list(INDEX_COLUMNS) and e["written"] for e in rows)
    for folder in index["export_folder"]:
        assert (tmp_path / "out" / folder / "README.txt").exists()
    again = export_selection(catalog, tmp_path / "out")
    assert list(again["export_status"]) == ["skipped (exists)"] * 2
    with pytest.raises(ValueError, match="give one of them"):
        export_selection(catalog, tmp_path / "out", update=True, overwrite=True)
    with pytest.raises(ValueError, match="unknown format"):
        export_selection(catalog, tmp_path / "out", ["pdf"])


def test_export_selection_gives_the_cli_index(tmp_path):
    index = export_selection(catalog_from_files(TWO), tmp_path / "py")
    assert cli.main(["export", *TWO, "--out", str(tmp_path / "cli")]) == 0
    by_cli = pd.read_csv(tmp_path / "cli" / "index.csv", dtype=str).fillna("")
    by_py = pd.read_csv(tmp_path / "py" / "index.csv", dtype=str).fillna("")
    assert list(by_cli.columns) == list(by_py.columns) == list(index.columns)
    assert by_cli.drop(columns=["export_stamp"]).equals(by_py.drop(columns=["export_stamp"]))
    assert json.loads((tmp_path / "cli" / "options.json").read_text(encoding="utf-8")) == ExportOptions().to_dict()


def test_export_selection_failed_run_is_a_row(tmp_path, monkeypatch):
    def broken(*a, **kw):
        raise OSError("disk full")
    monkeypatch.setattr(rx, "write_run", broken)
    events = []
    with pytest.warns(UserWarning, match="not written"):
        index = export_selection(catalog_from_files(TWO[:1]), tmp_path, on_run=events.append)
    assert list(index["export_status"]) == ["failed"]
    assert [e["event"] for e in events] == ["selected", "loaded", "failed", "row"]
    with pytest.raises(OSError):
        export_selection(catalog_from_files(TWO[:1]), tmp_path / "s", strict=True)


def test_export_error_says_why(tmp_path):
    import shutil
    gone = tmp_path / "gone.zip"
    shutil.copy(TWO[1], gone)
    catalog = catalog_from_files([TWO[0], str(gone)])
    gone.unlink()
    with pytest.warns(UserWarning, match="not found"):
        index = export_selection(catalog, tmp_path / "out", options=ExportOptions(cadet_core="6"))
    by_status = dict(zip(index["export_status"], index["export_error"]))
    assert set(by_status) == {"incomplete", "not loaded"}
    assert by_status["incomplete"].startswith("not written: CADET-Core 6 input for ")
    assert "gone.zip not found" in by_status["not loaded"]
    on_disk = pd.read_csv(tmp_path / "out" / "index.csv", dtype=str).fillna("")
    assert list(on_disk.columns[:6]) == list(INDEX_COLUMNS) and INDEX_COLUMNS[3] == "export_error"
    good = export_selection(catalog_from_files(TWO[:1]), tmp_path / "good")
    assert list(good["export_error"]) == [""]
    home_file = os.path.join(os.path.expanduser("~"), "x.zip")
    assert rx.export_error(home_file + " not found") == os.path.join("~", "x.zip") + " not found"


def _planted_csv(tmp_path):
    catalog = catalog_from_files(TWO)
    for column, value in PLANTED.items():
        catalog[column] = value
    path = tmp_path / "sel.csv"
    catalog.to_csv(path, index=False)
    return str(path)


def test_cli_index_masked_with_mask_identifying(tmp_path):
    selection = _planted_csv(tmp_path)
    assert cli.main(["export", "--from", selection, "--out", str(tmp_path / "m"), "--mask-identifying"]) == 0
    masked = pd.read_csv(tmp_path / "m" / "index.csv", dtype=str).fillna("")
    assert list(masked["system"]) == ["System1"] * 2 and list(masked["created_by"]) == ["User1"] * 2
    text = (tmp_path / "m" / "index.csv").read_text(encoding="utf-8-sig")
    assert not any(v in text for v in PLANTED.values()) and "jdoe" not in text
    assert cli.main(["export", "--from", selection, "--out", str(tmp_path / "p")]) == 0
    plain = pd.read_csv(tmp_path / "p" / "index.csv", dtype=str)
    assert list(plain["created_by"]) == [PLANTED["created_by"]] * 2 and list(plain["system"]) == [PLANTED["system"]] * 2


def test_mask_index_is_the_pages_rule():
    frame = pd.DataFrame({"export_error": ["LAB\\jdoe not allowed", ""], "system": ["AKTA-Bay7", "AKTA-Bay7"],
                          "created_by": ["LAB\\jdoe", "Default"], "match_key": ["k|AKTA-Bay7", None]})
    out = rx.mask_index(frame)
    assert list(out["system"]) == ["System1", "System1"] and list(out["created_by"]) == ["User1", "Default"]
    assert out["export_error"][0] == "User1 not allowed" and out["match_key"][0] == "k|System1"


# --- pycorn export --options -------------------------------------------------------------------

def _args(*argv):
    return cli.build_parser().parse_args(["export", *TWO, "--out", "x", *argv])


def test_cli_options_file_and_flags_over_it(tmp_path):
    path = tmp_path / "o.json"
    path.write_text(json.dumps({"method_hints": True, "tables": ["key"], "curves": ["UV"],
                                "bed_height": [10, "cm"], "core_format": "json"}), encoding="utf-8")
    options = cli.export_options(_args("--options", str(path)))
    assert options.method_hints and options.tables == ["key"] and options.bed_height == (10, "cm")
    assert options.route == "CLI: pycorn export"
    options = cli.export_options(_args("--options", str(path), "--tables", "peaks", "--bed-height", "2 cm"))
    assert options.tables == ["peaks"] and options.bed_height == (2.0, "cm") and options.curves == ["UV"]
    assert cli.export_options(_args()) == cli.export_options(_args("--options", str(_written(tmp_path, {}))))


def _written(tmp_path, d):
    path = tmp_path / "empty.json"
    path.write_text(json.dumps(d), encoding="utf-8")
    return path


def test_cli_options_file_errors(tmp_path, capsys):
    bad = _written(tmp_path, {"method_hint": True})
    code = cli.main(["export", *TWO, "--out", str(tmp_path / "o"), "--options", str(bad)])
    assert code == 2 and "unknown key(s) 'method_hint'" in capsys.readouterr().err
    assert not (tmp_path / "o").exists()


def test_an_export_reproduces_from_its_options_json(tmp_path):
    assert cli.main(["export", TWO[0], "--out", str(tmp_path / "a"), "--method-hints", "--tables", "key,method"]) == 0
    assert cli.main(["export", TWO[0], "--out", str(tmp_path / "b"), "--options",
                     str(tmp_path / "a" / "options.json")]) == 0
    files = [sorted(p.relative_to(tmp_path / x).as_posix() for p in (tmp_path / x).rglob("*.csv")) for x in "ab"]
    assert files[0] == files[1] and any("planned_vs_run" in f for f in files[0])


# --- the page ------------------------------------------------------------------------------

def test_page_options_are_the_same_dict():
    choices = {"mask": True, "curves": ["UV 1_280"], "sample": {"mode": "hybrid", "value": 300},
               "groups": ["key"], "chromatograms": ["all"]}
    options = browser.export_options(choices, "web page")
    assert options == ExportOptions(method_hints=True, mask_identifying=True, curves=["UV 1_280"],
                                    curves_missing_ok=True, max_points=300, method="hybrid", tables=["key", "curves"],
                                    chromatograms=["all"], route="web page")
    assert ExportOptions.from_dict(options.to_dict()).to_dict() == options.to_dict()
    assert browser.export_options({}, "r") == ExportOptions(method_hints=True, route="r")


PLANTED = {"system": "AKTA-Bay7", "created_by": "LAB\\jdoe"}  # made up: what a real catalog holds there


def test_page_download_all_index_has_the_cli_columns(tmp_path):
    session = browser.Session()
    session.add(TWO)
    for column, value in PLANTED.items():
        session.catalog[column] = value
    start = json.loads(session.export_start(None, kind="csv", folder=str(tmp_path / "all"), options={"mask": True}))
    assert [json.loads(session.export_one(i))["status"] for i in start["ids"]] == ["written", "written"]
    r = json.loads(session.export_finish())
    with zipfile.ZipFile(r["path"]) as z:
        page = pd.read_csv(z.open("PyCORN_export/index.csv"), dtype=str).fillna("")
        stored = json.loads(z.read("PyCORN_export/options.json"))
    assert cli.main(["export", *TWO, "--out", str(tmp_path / "cli")]) == 0
    by_cli = pd.read_csv(tmp_path / "cli" / "index.csv", dtype=str).fillna("")
    assert list(page.columns) == list(by_cli.columns)
    assert list(page["export_status"]) == ["written"] * 2
    assert [f.replace("\\", "/") for f in by_cli["export_folder"]] == list(page["export_folder"])
    assert all(f.endswith("README.txt") or "README.txt" in f.split(";") for f in page["export_files"])
    assert ExportOptions.from_dict(stored) == ExportOptions(method_hints=True, mask_identifying=True)
    # masked: the catalog's people columns as labels, as the masked files hold them
    assert list(page["system"]) == ["System1"] * 2 and list(page["created_by"]) == ["User1"] * 2
    assert not any(v in page.to_csv() for v in PLANTED.values()) and "jdoe" not in page.to_csv()
    session.export_start(None, kind="csv", folder=str(tmp_path / "plain"), options={})
    for i in start["ids"]:
        session.export_one(i)
    with zipfile.ZipFile(json.loads(session.export_finish())["path"]) as z:
        plain = pd.read_csv(z.open("PyCORN_export/index.csv"), dtype=str)
    assert list(plain["created_by"]) == [PLANTED["created_by"]] * 2  # not masked unless asked, as the files
