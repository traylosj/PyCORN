# -*- coding: utf-8 -*-
"""Run-level tables (`pycorn.run_details`): method, calibration, instrument, settings, audit log; masking."""
import base64
import json
import os
import pickle

import pandas as pd
import pytest

from pycorn import browser, open_run
from pycorn.run_details import BUILDERS, TABLES, Masker
from pycorn.run_export import ExportOptions, write_run

SAMPLES = os.path.abspath("../samples")
# made up for the tests: what a real result holds in these fields
PLANTED = {"user": "LAB\\jdoe", "computer": "LABPC-0042", "system": "AKTA-Bay7", "icu": "SACX0001",
           "module": "8675309", "ip": "10.20.30.40"}
STATISTICS = ('<?xml version="1.0" encoding="utf-16"?><Information><OperationalStatistics><Modules>'
              '<Module name="Mixer (M9)"><Items><Item name="Serial Number" value="%s" />'
              '<Item name="Mixer run time" value="12" unit="hour" /></Items></Module>'
              '</Modules></OperationalStatistics></Information>' % PLANTED["module"])


def load(name="u73_blank.zip"):
    return open_run(os.path.join(SAMPLES, name))


def planted_run():
    """u73_blank with identifying values put back where UNICORN stores them."""
    run = load()
    run.result["CreatedBy"] = PLANTED["user"]
    system = run.metadata["SystemData"]["Systems"]["System"]
    system.update(Name=PLANTED["system"], ComputerName=PLANTED["computer"], ICUSerialNumber=PLANTED["icu"],
                  ICUIPAddress=PLANTED["ip"],
                  OperationalStatistics=base64.b64encode(STATISTICS.encode("utf-8")).decode())
    for chrom in run.chromatograms.values():
        for ec in chrom.event_curves.values():
            for e in ec.events:
                if (e.text or "").startswith("Method Run"):
                    e.text = "Method Run 1/1/2021 Method: Method1 Result: /Folder1/Run1 User %s at computer %s " \
                             "is in control" % (PLANTED["user"], PLANTED["computer"])
    return run


def test_tables_from_a_sample():
    run = load()
    frames = {name: build(run) for name, build in BUILDERS.items()}
    assert list(frames) == list(TABLES)
    cal = frames["calibration"]
    ph = cal[cal["calibration"] == "pH"]
    # as UNICORN's report lists it: the reference values, then the constants
    assert list(ph["kind"]) == ["reference", "reference", "constant", "constant"]
    assert list(ph["value"].iloc[:2]) == [7.0, 4.01] and list(ph["item"].iloc[:2]) == ["Reference value 1",
                                                                                     "Reference value 2"]
    slope = ph[ph["item"] == "Calibrated electrode slope"]
    assert len(slope) == 1 and slope["unit"].iloc[0] == "%"  # 'Calibrated electrode slope; %' as stored
    assert str(ph["date_utc"].iloc[0]) == "2021-07-19 22:10:15.313000"  # local 18:10 at -240 min
    inst = frames["instrument"]
    mixer = inst[inst["group"] == "mixer"]
    assert list(mixer[["item", "value", "unit"]].iloc[0]) == ["Volume", "0.6", "ml"]
    assert inst["item"].str.startswith("Real length").any()
    assert not inst["item"].str.contains(r"(?i)(?:ip|ipaddress|port)$").any()  # never network details
    steps = frames["method_steps"]
    assert len(steps) and steps["run_values"].str.contains("=").any()
    assert {"Base", "End_Block"} <= set(steps["instruction"])
    assert "Alarm UV1" in set(frames["system_settings"]["instruction"])
    assert len(frames["audit_log"]) >= 1
    details = frames["run_details"]
    assert ("method", "name", "Method1") in set(map(tuple, details.values))


def test_method_text_as_unicorn_prints_it():
    from pycorn.run_details import method_steps_frame
    steps = method_steps_frame(load("u711_cadet_reference.zip"))
    lines = list(steps["line"])
    # from the main block, each called block right after its call, one level deeper
    assert lines[0].startswith("0.00 Base: CV, Vc=5.027 {ml}, (") and lines[0].endswith(")#Column type")
    assert lines[1] == "0.00 Phase: Method Settings" and steps["depth"].iloc[1] == 0
    assert steps["phase"].iloc[1] == "Method Settings"  # the call row belongs to the phase it opens
    assert "    0.00 System flow: (5.000)#Flow rate {ml/min}, (Pre column pressure)#Pressure control" in lines
    hold = lines.index("    0.00 Block: Hold until UV stable signal")
    assert lines[hold + 1:hold + 4] == ["        0.00 Base: Time, Any",
                                        "        0.00 Watch UV parameters: 0.00 {mAU}, 1.00 {mAU}",
                                        "        0.00 Hold until: UV 1, Stable signal, 0.03 {min}, 5.00 {base}"]
    assert "        (5.00)#Equilibration volume End_Block" in lines
    assert not steps["parameters"].str.contains("linear flow selected").any()  # hidden, as in UNICORN
    sub = steps[steps["block"] == "Equilibrate"]
    assert set(sub["phase"]) == {"Equilibration"} and set(sub["path"]) == {"Main > EQUILIBRATION > Equilibrate"}
    assert list(steps["step"]) == list(range(1, len(steps) + 1))


def test_variable_list_as_unicorn_shows_it():
    from pycorn.run_details import method_variables_frame
    frame = method_variables_frame(load("u711_cadet_reference.zip")).set_index("variable")
    assert list(frame.index[:3]) == ["Column type", "Pre column pressure limit", "Delta column pressure limit"]
    assert tuple(frame.loc["Equilibration volume", ["phase", "block", "unit", "stored_unit"]]) == (
        "Equilibration", "Equilibrate", "CV", "{base}")
    assert frame.loc["Empty loop with", "unit"] == "ml"  # the Inject block's base is Volume
    assert frame.loc["Flow rate", "block"] == "METHOD SETTINGS"  # where it is first used


def test_column_peak_summary_dates_and_naming():
    from pycorn.run_details import column_frame, run_details_frame
    from pycorn.run_export import peak_summary_frame, peaks_frame
    run = load("u711_cadet_reference.zip")
    column = column_frame(run).set_index("parameter")
    assert column.loc["Column volume", "value"] == pytest.approx(5.0265, abs=1e-4)
    assert column.loc["Default linear flow rate", "value"] == pytest.approx(149.21, abs=0.01)
    assert tuple(column.loc["Typical loading range", ["value", "unit"]]) == ("5-150", "mg")
    assert column.loc["Resin code number", "group"] == "Ordering information"
    assert not column.index.str.contains("PackingData").any()
    chrom = run.chromatograms["Chrom.1"]
    summary = peak_summary_frame(chrom).set_index("field")
    assert summary.loc["Baseline name", "value"] == "UV 1_280_CUT_TEMP@100,BASEM"
    assert tuple(summary.loc["Total area", ["value", "unit"]]) == (pytest.approx(9557.472), "ml*mAU")
    peaks = peaks_frame(chrom)
    assert peaks["retention_unit"].iloc[0] == "ml" and peaks["retention_zero_injection"].iloc[0] == 1
    # 30.34 ml from the injection is 55.64 ml on the run's volume axis
    assert peaks["max_peak_on_run_axis"].iloc[0] == pytest.approx(
        peaks["MaxPeakRetention"].iloc[0] + chrom.retention_zero["volume"])
    details = set(map(tuple, run_details_frame(run).values))
    assert ("result", "created", "2025-11-18T11:17:39.581 -05:00") in details
    assert ("method", "result name", "Method name, without unique identifier") in details


def test_tables_empty_without_metadata():
    run = load()
    run.metadata.clear()
    run.evaluation_log = []
    for name, build in BUILDERS.items():
        frame = build(run)
        assert name == "run_details" or frame.empty, name


def test_masker_replaces_people_hosts_and_serials():
    run = planted_run()
    masker = Masker(run)
    kinds = {v.rstrip("0123456789") for v in masker.names.values()}
    assert {"User", "Computer", "System", "Serial"} <= kinds
    text = masker.text("User %s at computer %s, serial %s" % (PLANTED["user"], PLANTED["computer"],
                                                               PLANTED["module"]))
    assert not any(PLANTED[k] in text for k in ("user", "computer", "module"))
    assert masker.text("Default") == "Default"  # built-in account
    assert masker.text("2728 hours") == "2728 hours"


def _all_text(folder):
    out = []
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if name.endswith(".csv"):
            out.append(open(path, encoding="utf-8-sig").read())
        elif name.endswith(".xlsx"):
            out += [f.to_csv() for f in pd.read_excel(path, sheet_name=None).values()]
        elif name.endswith(".pkl"):
            with open(path, "rb") as f:
                out.append(repr(pickle.load(f)))
    return "\n".join(out)


def test_export_shows_by_default_masks_on_request_never_ip(tmp_path):
    plain = _all_text(tmp_path / "plain") if write_run(planted_run(), tmp_path / "plain",
                                                       formats=("csv", "xlsx", "pickle")) else ""
    for key in ("computer", "system", "icu", "module"):
        assert PLANTED[key] in plain, key
    assert PLANTED["ip"] not in plain
    write_run(planted_run(), tmp_path / "masked", formats=("csv", "xlsx", "pickle"),
              options=ExportOptions(mask_identifying=True))
    masked = _all_text(tmp_path / "masked")
    for key, value in PLANTED.items():
        assert value not in masked, key
    assert "jdoe" not in masked  # the login without its domain too
    assert "Computer1" in masked and "Serial" in masked


def test_new_tables_in_every_export_path(tmp_path):
    written = write_run(load(), tmp_path / "run", formats=("csv", "xlsx"))
    for name in ("run_and_method", "method_steps", "calibration", "instrument", "system_settings", "audit_log"):
        assert name + ".csv" in written
    assert "scouting_runs.csv" not in written  # empty: left out
    book = next(w for w in written if w.endswith(".xlsx"))
    sheets = pd.ExcelFile(tmp_path / "run" / book).sheet_names
    assert {"Run and method", "Method steps", "Calibration", "Instrument", "System settings", "Audit log"} <= set(sheets)
    top = pd.read_excel(tmp_path / "run" / book, sheet_name="Summary").iloc[:, :2].values.tolist()
    summary = dict(top[:next(i for i, r in enumerate(top) if pd.isna(r[0]))])  # the run block, before the sheet list
    assert summary["Method"] == "Method1" and summary["Instrument"] == "AKTA pure 25"


def test_browser_view_and_masked_download(tmp_path):
    s = browser.Session()
    s.add([os.path.join(SAMPLES, "u711_column_test.zip")])
    v = json.loads(s.view(0))
    assert list(v["details"]) == list(TABLES)
    assert v["details"]["calibration"]["rows"]
    chrom = v["chromatograms"][0]
    assert "hint_blocks" in chrom and "hint_injections" in chrom
    r = json.loads(s.download(0, "csv", str(tmp_path / "out"), json.dumps({"mask": True})))
    assert "error" not in r and r["name"].endswith("_csv.zip")
