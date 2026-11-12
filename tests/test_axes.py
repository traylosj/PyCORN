"""
Units, column volume (Vc), CV, UTC, the phase/injection/fraction tables and concentration.

Most cases are synthetic: no real data has L, AU, L/h, a day-first date, a DST change or an
untimestamped pause (docs/PHASE0_FINDINGS.md). Real-data checks use samples/sample.zip and,
when present, the local test files.
"""
import copy
import datetime as dt
import glob
import os
from collections import OrderedDict

import numpy as np
import pytest

from pycorn import open_folder, open_run
from pycorn.axes import add_axes, is_axes_warning
from pycorn.column_volume import column_path, in_column_volume, resolve_column_volume
from pycorn.concentration import ACCURACY_NOTE, concentration
from pycorn.event_tables import zero_offsets
from pycorn.model import Chromatogram, Curve, Event, EventCurve, Run
from pycorn.timeline import apply_utc, build_timeline
from pycorn.units import (UnitError, conversion_factor, convert, convert_decimals, display_decimals, quantity,
                          unit_info, uv_path_length_cm)

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLE = os.path.join(HERE, "..", "samples", "sample.zip")
TEST_FILES = os.path.join(HERE, "..", "unicorn_test_files")
UTC = dt.timezone.utc


@pytest.fixture(scope="module")
def sample():
    return open_run(SAMPLE)


# ---------------------------------------------------------------------------
# synthetic runs
# ---------------------------------------------------------------------------

def _curve(name="UV 1_280", data_type="UV", unit="mAU", time=None, volume=None, values=None, uv_info=None,
           column_volume=None, column_volume_unit=None, volume_unit="ml", start="2024-03-09T12:00:00.000",
           offset=-300):
    time = np.asarray(time if time is not None else np.arange(0, 10.0, 0.5))
    volume = np.asarray(volume if volume is not None else time, dtype=np.float32)
    values = np.asarray(values if values is not None else np.ones(len(time)), dtype=np.float32)
    return Curve(name=name, data_type=data_type, unit=unit, precision=3, curve_number=1, time=time, volume=volume,
                 values=values, time_unit="min", volume_unit=volume_unit, isochrone="Time", is_original=True,
                 column_volume=column_volume, column_volume_unit=column_volume_unit, uv_info=uv_info, point_sets=[],
                 xml={"MethodStartTime": start, "MethodStartTimeUtcOffsetMinutes": str(offset)})


def _event(time, text, volume=None, subtype="Undefined", type_="Method"):
    return Event(time=time, volume=time if volume is None else volume, text=text, type=type_, subtype=subtype,
                 instruction_feedback=None, xml={})


def _log(events, name="Run Log", type_="Logbook"):
    return EventCurve(name=name, type=type_, curve_number=9, time_unit="min", volume_unit="ml", is_original=True,
                      column_volume=None, column_volume_unit=None, events=events, xml={})


def _chrom(curves=None, event_curves=None, volume_unit="ml"):
    curves = curves if curves is not None else [_curve()]
    return Chromatogram(name="Chrom.1", chromatogram_id=1, info={"VolumeUnit": volume_unit, "TimeUnit": "min"},
                        curves=OrderedDict((c.name, c) for c in curves),
                        event_curves=OrderedDict((e.name, e) for e in (event_curves or [])), peak_tables=[],
                        pool_tables=None)


def _run(chrom, run_information=None, metadata=None, name="run"):
    return Run(source="zip", locator="synthetic", result={"Name": name, "run_information": run_information or []},
               chromatograms=OrderedDict([(chrom.name, chrom)]), metadata=metadata or OrderedDict(),
               evaluation_log=[], manifest=[], entries=OrderedDict())


# ---------------------------------------------------------------------------
# units
# ---------------------------------------------------------------------------

def test_unit_table_spellings_and_conversions():
    assert quantity("ml") == quantity("mL") == quantity("L") == quantity("µl") == "volume"
    assert conversion_factor("L/h", "ml/min") == pytest.approx(1000 / 60)
    assert convert(np.array([60.0]), "ml/min", "L/h")[0] == pytest.approx(3.6)
    assert conversion_factor("AU", "mAU") == 1000
    assert conversion_factor("AU/cm", "mAU/cm") == 1000
    assert conversion_factor("s", "min") == pytest.approx(1 / 60)
    assert unit_info("MS/cm") is None  # case matters: not milli
    assert unit_info(None) is None and quantity("") is None
    with pytest.raises(UnitError):
        conversion_factor("mAU", "ml")
    with pytest.raises(UnitError):
        conversion_factor("furlong", "ml")


def test_uv_path_length():
    normalized = _curve(uv_info={"IsUVValuesNormalizedToNominalUVPathLength": True, "UVPathLength": 2.08,
                                 "NominalUVPathLength": 2})
    actual = _curve(uv_info={"IsUVValuesNormalizedToNominalUVPathLength": False, "UVPathLength": 2.08,
                             "NominalUVPathLength": 2})
    assert uv_path_length_cm(normalized) == (pytest.approx(0.2), "NominalUVPathLength")
    assert uv_path_length_cm(actual) == (pytest.approx(0.208), "UVPathLength")
    assert uv_path_length_cm(_curve(unit="mAU/cm")) == (1.0, "per_cm_unit")
    assert uv_path_length_cm(_curve())[0] is None  # no CurveUVInfo: never guessed
    assert uv_path_length_cm(_curve(unit="mS/cm", data_type="Conduction"))[0] is None


def test_normalized_copy_converts_and_records(sample):
    n = sample.normalized(units={"volume": "L", "flow": "L/h", "absorbance": "AU", "time": "h"},
                          uv_path_length_cm=1.0)
    chrom, raw = n.chromatogram(), sample.chromatogram()
    uv, raw_uv = chrom.curves_with_role("uv")[0], raw.curves_with_role("uv")[0]
    assert uv.unit == "AU" and uv.volume_unit == "L" and uv.time_unit == "h"
    assert uv.conversion["path_length_cm_from"] == pytest.approx(0.2)
    assert uv.values[100] == pytest.approx(raw_uv.values[100] / 1000 * 5)
    assert uv.volume[-1] == pytest.approx(raw_uv.volume[-1] / 1000)
    flow = chrom.curve("System flow")
    assert flow.unit == "L/h" and flow.values.max() == pytest.approx(raw.curve("System flow").values.max() * 0.06)
    assert chrom.curve("Sample flow (CV/h)").unit == "CV/h"  # not a volumetric flow
    assert np.array_equal(uv.cv, raw_uv.cv)  # CV doesn't depend on units
    assert chrom.column_volume_info["value"] == pytest.approx(0.0007) and chrom.column_volume_info["unit"] == "L"
    assert chrom.event_curves["Injection"].events[0].volume == pytest.approx(0.01357932)
    # the original is unchanged
    assert raw_uv.unit == "mAU" and raw_uv.conversion is None and raw_uv.volume_unit == "ml"
    assert raw.column_volume_info["value"] == 0.7
    # display decimals carried with the values: mAU -> AU (x0.001) and the 0.2 -> 1 cm path (x5)
    assert raw_uv.precision == 3 and uv.precision == convert_decimals(3, 0.001 * 5) == 6
    assert flow.precision == convert_decimals(raw.curve("System flow").precision, 0.06)


def test_display_decimals_follow_every_conversion():
    # the run-log rule (owner, 2026-11-10), carried to other units: never coarser than the source step
    assert [display_decimals(u) for u in ("min", "s", "sec", "h")] == [2, 1, 1, 4]
    assert [display_decimals(u) for u in ("ml", "mL", "L", "m^3")] == [3, 3, 6, 9]
    assert [display_decimals(u) for u in ("ml/min", "L/h", "CV", "%")] == [3, 5, 2, 1]
    assert display_decimals("mAU") is None and display_decimals(None) is None and display_decimals("x") is None
    # powers of 10 shift exactly (same significant figures), both ways
    assert convert_decimals(3, conversion_factor("ml", "L")) == 6
    assert convert_decimals(6, conversion_factor("L", "ml")) == 3
    assert convert_decimals(2, conversion_factor("min", "s")) == 1  # 0.01 min = 0.6 s
    assert convert_decimals(1, conversion_factor("s", "min")) == 3  # 0.1 s = 0.00167 min
    assert convert_decimals(0, 1000.0) == 0 and convert_decimals(None, 2.0) is None


def test_column_decimals_by_name_and_row_unit():
    import pandas as pd
    from pycorn.display_precision import column_decimals
    frame = pd.DataFrame({"name": ["a", "b"], "start_time": [1.0, 2.0], "length_volume": [0.1, 0.2],
                          "end_cv": [1.0, 2.0], "time_unit": ["min", "s"], "volume_unit": ["L", "L"],
                          "elapsed_s": [1.0, 2.0], "percent_b_slope_per_s": [0.1, 0.2],
                          "base": ["CV", "ml"], "planned": [5.0, 3.1]})
    found = dict(zip(frame.columns, column_decimals(frame)))
    assert found["start_time"] == [2, 1] and found["length_volume"] == 6 and found["end_cv"] == 2
    assert found["elapsed_s"] == 1 and found["planned"] == [2, 3]
    assert found["name"] is None and found["percent_b_slope_per_s"] is None
    curve = _curve(unit="mAU")
    curve.precision = 3
    curves = pd.DataFrame({"UV: time (min)": [0.0], "UV: volume (L)": [0.0], "UV: CV": [0.0],
                           "UV: value (mAU)": [1.0], "Other: value (mS/cm)": [1.0]})
    assert column_decimals(curves, curves={"UV": curve}) == [2, 6, 2, 3, None]


def test_normalized_records_axis_only_conversions(sample):
    n = sample.normalized(units={"volume": "L"})
    uv = n.chromatogram().curves_with_role("uv")[0]  # values untouched, volume axis converted
    c = uv.conversion
    assert c["raw_unit"] == c["unit"] == "mAU" and c["factor"] == 1.0
    assert dict(c["axes"]["volume"]) == {"raw_unit": "ml", "unit": "L", "factor": pytest.approx(0.001)}
    assert "time" not in c["axes"]
    both = sample.normalized(units={"absorbance": "AU", "time": "h"}).chromatogram().curves_with_role("uv")[0]
    assert both.conversion["unit"] == "AU" and both.conversion["axes"]["time"]["unit"] == "h"
    assert sample.normalized(units={}).chromatogram().curves_with_role("uv")[0].conversion is None


def test_normalized_uv_path_is_not_divided_out_twice(sample):
    raw_uv = sample.chromatogram().curves_with_role("uv")[0]
    once = sample.normalized(uv_path_length_cm=1.0)
    uv = once.chromatogram().curves_with_role("uv")[0]
    assert uv_path_length_cm(uv) == (pytest.approx(1.0), "NominalUVPathLength")
    assert uv.uv_info["UVPathLength"] == raw_uv.uv_info["UVPathLength"]  # the cell is unchanged
    assert raw_uv.uv_info["NominalUVPathLength"] == pytest.approx(2.0)  # the original run too
    raw_c = concentration(raw_uv, epsilon=1.0).values
    assert concentration(uv, epsilon=1.0).values[100] == pytest.approx(raw_c[100])
    twice = once.normalized(uv_path_length_cm=1.0).chromatogram().curves_with_role("uv")[0]
    assert twice.values[100] == pytest.approx(uv.values[100])
    assert concentration(twice, epsilon=1.0).values[100] == pytest.approx(raw_c[100])


def test_normalized_keeps_unconvertible_curves_with_warning():
    chrom = _chrom([_curve(unit="weird")])
    out = _run(chrom).normalized(units={"absorbance": "AU"}, uv_path_length_cm=1.0)
    assert out.chromatogram().curves["UV 1_280"].unit == "weird"
    with pytest.raises(UnitError):
        _run(chrom).normalized(units={"absorbance": "ml"})


# ---------------------------------------------------------------------------
# column volume
# ---------------------------------------------------------------------------

def test_sample_column_volume_sources(sample):
    info = sample.chromatogram().column_volume_info
    assert (sample.column_volume, sample.column_volume_unit, sample.column_volume_source) == (0.7, "ml", "curves")
    sources = {c.source: c for c in info["candidates"]}
    for source in ("curves", "run_information", "run_log", "flow_ratio", "peak_table"):
        assert sources[source].value == pytest.approx(0.7, rel=1e-6), source
    assert sources["method"].value == 0.353 and sources["method"].confidence == "low"  # F7: wrong, not used
    assert info["disagreements"] == []


def _vc(chrom, run_information=None, metadata=None, name="run", **kwargs):
    run = _run(chrom, run_information, metadata, name)
    return resolve_column_volume(run, chrom, **kwargs)


def test_each_column_volume_source_alone():
    assert _vc(_chrom([_curve(column_volume=1.5, column_volume_unit="ml")]))["source"] == "curves"
    used = [{"type": "ColumnInformation", "value": {"usedColumns": {"column": {"name": "X", "volume": 2.0,
                                                                              "volumeUnit": "L"}}}}]
    info = _vc(_chrom(), run_information=used)
    assert (info["value"], info["unit"], info["source"]) == (2.0, "L", "run_information")
    log = _log([_event(0, "Base CV, Vc=24.5 {L}, Any")])
    info = _vc(_chrom(event_curves=[log]))
    assert (info["value"], info["unit"], info["source"]) == (24.5, "L", "run_log")
    t = np.arange(10.0)
    flow = _curve("System flow", "Other", "L/h", time=t, values=np.full(10, 70.0))
    rate = _curve("System flow (CV/h)", "Other", "CV/h", time=t, values=np.full(10, 100.0))
    info = _vc(_chrom([flow, rate]))
    assert info["source"] == "flow_ratio" and info["value"] == pytest.approx(700.0) and info["unit"] == "ml"
    # the override wins, in the chromatogram's volume unit unless given
    info = _vc(_chrom([_curve(column_volume=1.5, column_volume_unit="ml")]), column_volume=3.0)
    assert (info["value"], info["unit"], info["source"]) == (3.0, "ml", "override")
    info = _vc(_chrom(), column_volume=0.003, column_volume_unit="L")
    assert (info["value"], info["unit"]) == (0.003, "L")


def test_column_volume_priority_disagreement_and_repeats():
    log = _log([_event(0, "Base CV, Vc=0.700 {ml}, Any"), _event(5, "Base CV, Vc=1.000 {ml}, Any")])
    info = _vc(_chrom([_curve(column_volume=0.8, column_volume_unit="ml")], [log]))
    assert info["source"] == "curves" and info["value"] == 0.8
    assert {c.value for c in info["disagreements"]} == {0.7, 1.0}
    assert any("disagrees" in w for w in info["warnings"])
    info = _vc(_chrom(event_curves=[log]))
    assert info["source"] == "run_log" and info["value"] == 0.7
    assert any("2 different column volumes" in w for w in info["warnings"])
    # 0.7 ml and 0.0007 L agree once converted
    log = _log([_event(0, "Base CV, Vc=0.0007 {L}, Any")])
    assert _vc(_chrom([_curve(column_volume=0.7, column_volume_unit="ml")], [log]))["disagreements"] == []


def test_low_confidence_and_missing_column_volume():
    method = OrderedDict([("MethodData", {"Method": {"Text": "<mColumnVolume>0.353</mColumnVolume>"}})])
    info = _vc(_chrom(), metadata=method)
    assert info["value"] is None and info["candidates"][0].source == "method"
    assert any("low-confidence" in w for w in info["warnings"])
    info = _vc(_chrom())
    assert info["value"] is None and any("no column volume found" in w for w in info["warnings"])
    # a Base CV text in an unknown unit is not used
    info = _vc(_chrom(event_curves=[_log([_event(0, "Base CV, Vc=5 {gallons}, Any")])]))
    assert info["value"] is None and any("not a known volume unit" in w for w in info["warnings"])


def test_geometry_volume_is_a_last_resort():
    given = (5.0265e-3, "L", "--bed-height 2.5 cm, --diameter 1.6 cm")
    info = _vc(_chrom(), geometry_volume=given)
    assert (info["value"], info["unit"], info["source"]) == (5.0265e-3, "L", "geometry")
    assert any("last resort" in w for w in info["warnings"])
    # any stored Vc wins, even one far from the geometry (no disagreement reported for it)
    info = _vc(_chrom(event_curves=[_log([_event(0, "Base CV, Vc=0.700 {ml}, Any")])]), geometry_volume=given)
    assert (info["value"], info["source"]) == (0.7, "run_log") and not info["disagreements"]
    assert info["candidates"][-1].source == "geometry"
    assert [w for w in info["warnings"] if "column geometry given" in w] == [
        "Vc 0.005026 L from the column geometry given (--bed-height 2.5 cm, --diameter 1.6 cm) not used: "
        "the run has Vc 0.7 ml (run_log); the geometry is only a last resort"]  # not silent (owner, 2026-11-08)
    # not over a low-confidence or name-hint candidate either: those stay unused, geometry is used
    method = OrderedDict([("MethodData", {"Method": {"Text": "<mColumnVolume>0.353</mColumnVolume>"}})])
    assert _vc(_chrom(), metadata=method, geometry_volume=given)["source"] == "geometry"
    assert _vc(_chrom(), geometry_volume=(5.0, "gallons", "x"))["value"] is None


def test_name_hint_is_opt_in():
    assert _vc(_chrom(), name="Column 5 ml run")["value"] is None
    info = _vc(_chrom(), name="Column 5 ml run", use_name_hint=True)
    assert (info["value"], info["unit"], info["source"]) == (5.0, "ml", "name_hint")


# ---------------------------------------------------------------------------
# CV and the column valve
# ---------------------------------------------------------------------------

def test_in_column_volume():
    assert in_column_volume(10.0, []) == 10.0
    assert np.allclose(in_column_volume(np.array([0, 1, 2, 5, 8]), [(1, 4)]), [0, 1, 1, 2, 5])
    assert np.allclose(in_column_volume(np.array([0, 3, 9]), [(2, float("inf"))]), [0, 2, 2])


def test_sample_cv_skips_bypass(sample):
    chrom = sample.chromatogram()
    assert chrom.column_path["positions"] == ["1"]
    (start, end), = chrom.column_path["bypass"]
    assert (start, end) == (pytest.approx(0.06797, abs=1e-4), pytest.approx(10.0717, abs=1e-3))
    uv = chrom.curves_with_role("uv")[0]
    expected = (uv.volume[-1] - (end - start)) / 0.7
    assert uv.cv[-1] == pytest.approx(expected, rel=1e-6)
    assert np.all(np.diff(uv.cv) >= -1e-12)
    injection = chrom.event_curves["Injection"].events[0]
    assert injection.cv == pytest.approx((injection.volume - (end - start)) / 0.7, rel=1e-6)
    # 0.068 ml before the first switch: the usual start-up volume, below the 0.1 ml threshold
    assert not any("ran before the first column position entry" in w for w in sample.warnings)


def _valve_log(*entries):
    return _log([_event(t, text, volume=v) for t, v, text in entries])


def test_column_path_switches_positions_and_unknown_texts():
    log = _valve_log((0, 0, "Column position 1 Down flow (Issued) (Processing)"),
                     (0, 0, "Column position 1 Down flow (Completed)"),
                     (1, 2, "Column position By-pass Down flow (Completed)"),
                     (2, 5, "Column position Position 2 Up flow (Completed)"),
                     (3, 6, "Column position somewhere strange"))
    path = column_path(_chrom(event_curves=[log]))
    assert path["bypass"] == [(2.0, 5.0)]
    assert path["positions"] == ["1", "2"]
    assert any("2 column positions" in w for w in path["warnings"])
    assert any("not recognized" in w for w in path["warnings"])
    assert not any("before the first" in w for w in path["warnings"])  # first switch at 0


def test_volume_before_the_first_switch_warns_above_threshold():
    late = _valve_log((1, 0.5, "Column position By-pass Down flow (Completed)"),
                      (2, 3, "Column position 1 Down flow (Completed)"))
    assert any("0.5 ml ran before" in w for w in column_path(_chrom(event_curves=[late]))["warnings"])
    early = _valve_log((0.1, 0.05, "Column position By-pass Down flow (Completed)"),
                       (2, 3, "Column position 1 Down flow (Completed)"))
    assert not any("ran before" in w for w in column_path(_chrom(event_curves=[early]))["warnings"])


def test_cv_none_without_column_volume():
    run = _run(_chrom())
    add_axes(run)
    assert run.chromatogram().curves["UV 1_280"].cv is None
    add_axes(run, column_volume=2.0)  # recompute with an override; old warnings replaced
    assert run.chromatogram().curves["UV 1_280"].cv[-1] == pytest.approx(9.5 / 2)
    assert not any("no column volume" in w for w in run.warnings)


# ---------------------------------------------------------------------------
# UTC
# ---------------------------------------------------------------------------

def test_sample_utc_has_no_pauses(sample):
    tl = sample.timeline
    assert tl["start_utc"] == dt.datetime(2023, 3, 10, 21, 30, 7, 838000, tzinfo=UTC)
    assert tl["pauses"] == [] and tl["quality"] == "exact"
    assert abs(tl["residual_s"]) < 2
    uv = sample.chromatogram().curves_with_role("uv")[0]
    assert uv.utc.dtype == np.dtype("datetime64[us]")
    assert uv.utc[0] == np.datetime64("2023-03-10T21:30:07.838") + np.timedelta64(int(uv.time[0] * 60e6), "us")
    end = sample.chromatogram().event_curves["Run Log"].events[-1]
    assert isinstance(end.utc, np.datetime64)  # events as curves: datetime64[us], naive UTC
    end_utc = np.datetime64(tl["end_utc"].replace(tzinfo=None), "us")
    assert abs((end.utc - end_utc) / np.timedelta64(1, "s")) < 5
    phase = sample.chromatogram().phase_table().iloc[-1]
    assert (uv.utc[-1] - phase["start_utc"]) / np.timedelta64(1, "s") >= 0  # curves and events subtract


START = "2024-03-09T12:00:00.000"  # local, offset -300 (EST)


def _utc_chrom(log_entries, curve_time=None, start=START, offset=-300):
    curve = _curve(time=curve_time if curve_time is not None else np.arange(0, 10.0, 0.5), start=start,
                   offset=offset)
    return _chrom([curve], [_log([_event(t, text) for t, text in log_entries])])


def _timeline(chrom):
    tl = build_timeline(_run(chrom), chrom)
    apply_utc(chrom, tl)
    return tl


def test_grid_starts_at_chromatogram_start_else_method_start():
    # a later chromatogram of a method run starts after the method (owner, 2026-11-08)
    curve = _curve(time=np.array([0.0, 1.0]))
    curve.xml.update({"ChromatogramStartTime": "2024-03-09T13:00:00.000", "ChromatogramStartTimeUtcOffsetMinutes": "-300"})
    chrom = _chrom([curve])
    tl = _timeline(chrom)
    assert tl["start_source"] == "ChromatogramStartTime"
    assert tl["start_utc"] == dt.datetime(2024, 3, 9, 18, 0, tzinfo=UTC)
    assert curve.utc[1] == np.datetime64("2024-03-09T18:01:00", "us")
    tl = _timeline(_chrom([_curve(time=np.array([0.0, 1.0]))]))
    assert tl["start_source"] == "MethodStartTime"
    assert tl["start_utc"] == dt.datetime(2024, 3, 9, 17, 0, tzinfo=UTC)


def test_exact_pause_from_pause_and_continue():
    chrom = _utc_chrom([(0, "Method Run 3/9/2024 12:00:00 PM -05:00 Method: m"),
                        (2, "Pause Infinite min 3/9/2024 12:02:00 PM -05:00 (Issued) (Processing)"),
                        (2, "Pause Infinite min 3/9/2024 12:02:01 PM -05:00 (Completed)"),
                        (2, "Continue 3/9/2024 12:07:00 PM -05:00 (Issued) (Manual)"),
                        (5, "End 3/9/2024 12:10:00 PM -05:00 (Issued) (Processing)"),
                        (5, "End 3/9/2024 12:10:04 PM -05:00 (Completed)")])
    tl = _timeline(chrom)
    (p,) = tl["pauses"]
    assert (p["elapsed_s"], p["duration_s"], p["source"], p["quality"]) == (120, 300, "pause_text", "exact")
    assert tl["quality"] == "exact" and tl["residual_s"] == pytest.approx(0)
    curve = chrom.curves["UV 1_280"]
    at_pause = int(np.where(curve.time == 2)[0][0])
    assert curve.utc[at_pause] == np.datetime64("2024-03-09T17:02:00")  # pre-pause instant
    assert curve.utc[at_pause + 1] == np.datetime64("2024-03-09T17:07:30")
    events = chrom.event_curves["Run Log"].events
    assert events[1].utc == np.datetime64("2024-03-09T17:02:00", "us")  # the pause itself
    assert events[3].utc == np.datetime64("2024-03-09T17:07:00", "us")  # the resume entry: after


def test_continue_without_pause_adds_the_gap():
    # UNICORN logs only the system's Continue after e.g. a System wash
    chrom = _utc_chrom([(0, "Method Run 3/9/2024 12:00:00 PM -05:00"),
                        (1, "Continue 3/9/2024 12:02:00 PM -05:00 (Issued) (System)"),
                        (3, "End 3/9/2024 12:04:00 PM -05:00 (Issued) (Processing)")])
    tl = _timeline(chrom)
    (p,) = tl["pauses"]
    assert (p["elapsed_s"], p["duration_s"], p["source"]) == (60, 60, "continue_without_pause")
    assert tl["quality"] == "inferred" and abs(tl["residual_s"]) < 1e-6


def test_continue_refused_by_an_alarm_keeps_the_wait_until_the_next_continue():
    # seen in a run log: an alarm refuses a Continue (its Completed entry comes after the system's
    # ignored Pause); the stop lasts until the operator's next Continue
    chrom = _utc_chrom([(0, "Method Run 3/9/2024 12:00:00 PM -05:00"),
                        (2, "Pause Infinite min 3/9/2024 12:02:00 PM -05:00 (Issued) (System)"),
                        (2, "Continue 3/9/2024 12:05:00 PM -05:00 (Issued) (Manual)"),
                        (2, "(Alarm) In order to continue, the filled tubes have to be replaced"),
                        (2, "Pause Infinite min 3/9/2024 12:05:01 PM -05:00 (Issued) (System)"),
                        (2, "Pause Infinite min 3/9/2024 12:05:01 PM -05:00 (Processing) (Ignored) (System)"),
                        (2, "Continue 3/9/2024 12:05:01 PM -05:00 (Completed) (Manual)"),
                        (2, "Continue 3/9/2024 12:09:00 PM -05:00 (Issued) (Manual)"),
                        (5, "End 3/9/2024 12:12:00 PM -05:00 (Issued) (Processing)")])
    tl = _timeline(chrom)
    assert sum(p["duration_s"] for p in tl["pauses"]) == pytest.approx(420)
    assert all(p["elapsed_s"] == 120 for p in tl["pauses"])
    assert abs(tl["residual_s"]) < 1 and tl["gaps"] == []


def test_pause_while_paused_is_one_pause():
    chrom = _utc_chrom([(0, "Method Run 3/9/2024 12:00:00 PM -05:00"),
                        (2, "Pause Infinite min 3/9/2024 12:02:00 PM -05:00 (Issued) (Processing)"),
                        (2, "Pause Infinite min 3/9/2024 12:10:00 PM -05:00 (Issued) (Manual)"),
                        (2, "Continue 3/9/2024 12:12:00 PM -05:00 (Issued) (Manual)"),
                        (5, "End 3/9/2024 12:15:00 PM -05:00 (Issued) (Processing)")])
    tl = _timeline(chrom)
    (p,) = tl["pauses"]
    assert (p["elapsed_s"], p["duration_s"], p["quality"]) == (120, 600, "exact")
    assert tl["quality"] == "exact" and abs(tl["residual_s"]) < 1e-6


def test_whole_second_truncation_does_not_add_up():
    # 6 pauses whose texts are each cut to whole seconds (0.9 s early): durations from
    # resume minus the predicted start keep the end within a second
    entries, clock = [(0, "Method Run 3/9/2024 12:00:00 PM -05:00")], 0.0
    for k in range(6):
        t = 1 + k  # elapsed min
        start = 60 * t + clock
        clock += 30.9
        entries += [(t, "Pause Infinite min 3/9/2024 12:%02d:%02d PM -05:00 (Issued)" % divmod(int(start), 60)),
                    (t, "Continue 3/9/2024 12:%02d:%02d PM -05:00 (Issued) (Manual)" % divmod(int(start + 30.9), 60))]
    entries.append((8, "End 3/9/2024 12:%02d:%02d PM -05:00 (Issued)" % divmod(int(480 + clock), 60)))
    tl = _timeline(_utc_chrom(entries))
    assert len(tl["pauses"]) == 6 and tl["gaps"] == []
    assert abs(tl["residual_s"]) < 1


def test_alarm_before_end_is_a_clock_stop():
    # an alarm stops the clock; the operator ends the run 5 min later with no Continue
    log = _log([_event(0, "Method Run 3/9/2024 12:00:00 PM -05:00"),
                _event(4, "(Alarm) The system pressure has exceeded the set high pressure limit.", subtype="Alarm",
                       type_="Alert"),
                _event(4, "End 3/9/2024 12:09:00 PM -05:00 (Issued)")])
    chrom = _chrom([_curve(time=np.arange(0, 4.5, 0.5))], [log])
    tl = _timeline(chrom)
    (p,) = tl["pauses"]
    assert (p["elapsed_s"], p["duration_s"], p["source"], p["cause"]) == (240, 300, "clock_stop", "alarm")
    assert tl["gaps"] == [] and abs(tl["residual_s"]) < 1e-6
    # the clock ran on after the alarm (no Continue logged): the gap goes at the alarm
    log = _log([_event(0, "Method Run 3/9/2024 12:00:00 PM -05:00"),
                _event(2, "(Error) Fraction collector door was opened", subtype="Error", type_="Alert"),
                _event(5, "End 3/9/2024 12:08:00 PM -05:00 (Issued)")])
    tl = _timeline(_chrom([_curve()], [log]))
    (p,) = tl["pauses"]
    assert (p["elapsed_s"], p["duration_s"], p["cause"]) == (120, 180, "error")


def test_continue_without_pause_records_the_cause():
    log = _log([_event(0, "Method Run 3/9/2024 12:00:00 PM -05:00"),
                _event(0.9, "System wash 10 {ml} Injection valve (W1) (Issued) (Processing)"),
                _event(1, "Continue 3/9/2024 12:02:00 PM -05:00 (Issued) (System)"),
                _event(3, "End 3/9/2024 12:04:00 PM -05:00 (Issued)")])
    (p,) = _timeline(_chrom([_curve()], [log]))["pauses"]
    assert (p["elapsed_s"], p["source"], p["cause"]) == (60, "continue_without_pause", "wash")


def test_untimestamped_pause_takes_the_residual():
    chrom = _utc_chrom([(0, "Method Run 3/9/2024 12:00:00 PM -05:00"),
                        (2, "Pause (Issued)"), (2, "Continue (Manual)"),
                        (5, "End 3/9/2024 12:08:00 PM -05:00 (Issued)")])
    tl = _timeline(chrom)
    (p,) = tl["pauses"]
    assert (p["duration_s"], p["quality"]) == (180, "inferred")


def test_several_untimestamped_pauses_stop_the_timeline():
    chrom = _utc_chrom([(0, "Method Run 3/9/2024 12:00:00 PM -05:00"),
                        (2, "Pause (Issued)"), (2, "Continue (Manual)"),
                        (4, "Pause (Issued)"), (4, "Continue (Manual)"),
                        (6, "End 3/9/2024 12:20:00 PM -05:00 (Issued)")])
    tl = _timeline(chrom)
    assert tl["quality"] == "ambiguous" and tl["valid_until_s"] == 120
    utc = chrom.curves["UV 1_280"].utc
    t = chrom.curves["UV 1_280"].time
    assert not np.any(np.isnat(utc[t <= 2])) and np.all(np.isnat(utc[t > 2]))
    assert chrom.event_curves["Run Log"].events[-1].utc is None


def test_pause_without_resume_is_unresolved():
    chrom = _utc_chrom([(0, "Method Run 3/9/2024 12:00:00 PM -05:00"),
                        (2, "Pause Infinite min 3/9/2024 12:02:00 PM -05:00 (Issued)")], curve_time=np.arange(0, 2.5, 0.5))
    tl = _timeline(chrom)
    assert tl["quality"] == "unresolved" and tl["valid_until_s"] == 120
    assert any("no timestamped End" in w for w in tl["warnings"])


def test_hold_is_not_a_pause_and_unexplained_gaps_are_reported():
    chrom = _utc_chrom([(0, "Method Run 3/9/2024 12:00:00 PM -05:00"),
                        (2, "Hold 3/9/2024 12:02:00 PM -05:00"),
                        (2.5, "Hold until UV 1_280 Greater than 100 {mAU}"),
                        (5, "End 3/9/2024 12:06:00 PM -05:00 (Issued)")])
    tl = _timeline(chrom)
    assert tl["pauses"] == [] and tl["unplaced_s"] == pytest.approx(60)
    assert any("no pause to explain it" in w for w in tl["warnings"])


def test_dst_change_and_pause_offset_from_resume():
    # US DST starts 2024-03-10 02:00 local: -05:00 before, -04:00 after
    chrom = _utc_chrom([(0, "Method Run 3/10/2024 1:50:00 AM -05:00"),
                        (5, "Pause Infinite min 3/10/2024 1:55:00 AM (Issued)"),  # no offset: the resume's
                        (5, "Continue 3/10/2024 3:05:00 AM -04:00 (Manual)"),
                        (8, "End 3/10/2024 3:08:00 AM -04:00 (Issued)")],
                       start="2024-03-10T01:50:00.000", offset=-300)
    tl = _timeline(chrom)
    (p,) = tl["pauses"]
    # the resume re-anchors the clock: 06:55 UTC (predicted) to 07:05 UTC is 10 min across the change
    assert p["duration_s"] == pytest.approx(10 * 60)
    # the Pause text, read with the resume's -04:00, is an hour off: only a check, so the pause is inferred
    assert p["before_pause_s"] == pytest.approx(-3600) and p["quality"] == "inferred"
    assert abs(tl["residual_s"]) < 1e-6


def test_day_first_dates_when_a_date_proves_it():
    chrom = _utc_chrom([(0, "Method Run 13/03/2024 12:00:00 -04:00"),
                        (5, "End 13/03/2024 12:05:00 -04:00 (Issued)")],
                       start="2024-03-13T12:00:00.000", offset=-240)
    tl = _timeline(chrom)
    assert tl["end_utc"] == dt.datetime(2024, 3, 13, 16, 5, tzinfo=UTC) and abs(tl["residual_s"]) < 1e-6
    # ambiguous dates: the order that matches the method start wins
    chrom = _utc_chrom([(0, "Method Run 04/03/2024 12:00:00 -04:00"),
                        (5, "End 04/03/2024 12:05:00 -04:00 (Issued)")],
                       start="2024-03-04T12:00:00.000", offset=-240)
    assert _timeline(chrom)["end_utc"] == dt.datetime(2024, 3, 4, 16, 5, tzinfo=UTC)


def test_no_start_means_no_utc():
    chrom = _chrom([_curve(start=None)], [_log([_event(0, "Base CV, Vc=1 {ml}")])])
    chrom.curves["UV 1_280"].xml = {}
    tl = _timeline(chrom)
    assert tl["start_utc"] is None and chrom.curves["UV 1_280"].utc is None


# ---------------------------------------------------------------------------
# phase, injection and fraction tables
# ---------------------------------------------------------------------------

def test_sample_tables(sample):
    chrom = sample.chromatogram()
    phases = chrom.phase_table()
    assert list(phases["name"]) == ["Phase Method Settings", "Phase Run"]  # first phase at 0: no pseudo-row
    assert list(phases["end_source"]) == ["next", "end_phase"]
    assert phases["end_cv"].iloc[-1] == pytest.approx(chrom.curves_with_role("uv")[0].cv[-1], rel=1e-3)
    injections = chrom.injection_table()
    assert list(injections["pseudo"]) == [True, False]
    assert list(injections["name"]) == ["Before injection", "Injection 1"]  # the event itself has no text
    assert injections["start_volume"].iloc[1] == pytest.approx(13.57932)
    fractions = chrom.fraction_table()
    assert list(fractions["name"]) == ["Waste", "Frac", "Waste"] and list(fractions["pseudo"]) == [True, False, False]
    assert fractions["end_volume"].iloc[0] == fractions["start_volume"].iloc[1]
    assert list(fractions["end_source"]) == ["next", "next", "run_end"]
    offsets = chrom.zero_offsets("injection", 1)
    assert offsets["volume"] == chrom.retention_zero["volume"] and offsets["time"] == chrom.retention_zero["time"]
    assert offsets["utc"] == chrom.event_curves["Injection"].events[0].utc
    with pytest.raises(KeyError):
        chrom.zero_offsets("injection", 7)


def test_phase_table_without_end_phase_and_with_pseudo_row():
    log = _log([_event(0, "Method Run 3/9/2024 12:00:00 PM -05:00 Method: m", subtype="BlockStart"),
                _event(1, "Phase Equilibration (Issued) (Processing) (Completed)", subtype="BlockStart"),
                _event(3, "Phase Elution (Issued) (Processing) (Completed)", subtype="BlockStart"),
                _event(4, "Block wash (Issued) (Processing) (Completed)", subtype="BlockStart"),
                _event(6, "Stop", subtype="Undefined")])
    rows = _chrom(event_curves=[log]).table_rows("phase")
    assert [(r["number"], r["name"], r["pseudo"], r["end_time"], r["end_source"]) for r in rows] == [
        (0, "Method Run 3/9/2024 12:00:00 PM -05:00 Method: m", True, 1, "next"),
        (1, "Phase Equilibration", False, 3, "next"),
        (2, "Phase Elution", False, 6, "last_log_event")]
    assert zero_offsets(rows[2])["time"] == 3


def test_phase_zero_from_ui_settings(sample):
    import pycorn.sources.zip_source as zip_source
    chrom = copy.deepcopy(sample.chromatogram())
    chrom.info["UISettings"] = {"ChromatogramLayoutSettings": {"ScaleSettings": {"XAxis": {
        "IsAdjustToZeroAtPhaseSelected": "True", "AdjustRetentionToZeroAtPhase": "Run",
        "AdjustRetentionToZeroAtPhaseX": 0}}}}
    chrom.info.pop("ChromatogramLayoutXml", None)
    warnings = []
    zero = zip_source._retention_zero(chrom, warnings)
    assert zero["source"] == "ui_settings_phase" and zero["phase"] == "Run"
    assert zero["time"] == pytest.approx(0.043333, abs=1e-5)
    assert any("unverified" in w for w in warnings)
    chrom.info["UISettings"]["ChromatogramLayoutSettings"]["ScaleSettings"]["XAxis"]["AdjustRetentionToZeroAtPhase"] = "Nope"
    warnings = []
    zero = zip_source._retention_zero(chrom, warnings)
    assert zero["time"] is None and any("adjusted axes not built" in w for w in warnings)


# ---------------------------------------------------------------------------
# concentration
# ---------------------------------------------------------------------------

UV_NORMALIZED = {"IsUVValuesNormalizedToNominalUVPathLength": True, "UVPathLength": 2.08, "NominalUVPathLength": 2}


def test_concentration_mass_and_molar():
    curve = _curve(values=np.full(20, 500.0), uv_info=UV_NORMALIZED)  # 0.5 AU over 0.2 cm
    c = concentration(curve, epsilon=1.25)
    assert c.unit == "mg/ml" and c.values[0] == pytest.approx(0.5 / (1.25 * 0.2))
    assert c.conversion["path_length_basis"] == "NominalUVPathLength" and curve.unit == "mAU"
    # molar: 43824 1/(M*cm), 66.4 kDa -> 0.66 ml/(mg*cm)
    c = concentration(curve, epsilon_molar=43824, molecular_weight=66.4, molecular_weight_unit="kDa")
    assert c.conversion["epsilon_ml_per_mg_cm"] == pytest.approx(43824 / 66400)
    actual = _curve(values=np.full(20, 500.0), uv_info=dict(UV_NORMALIZED, IsUVValuesNormalizedToNominalUVPathLength=False))
    assert concentration(actual, epsilon=1.0).values[0] == pytest.approx(0.5 / 0.208)


def test_concentration_per_cm_and_missing_inputs():
    per_cm = _curve(unit="mAU/cm", values=np.full(20, 500.0))
    assert concentration(per_cm, epsilon=1.0).values[0] == pytest.approx(0.5)  # no second division
    with pytest.raises(ValueError, match="no path length"):
        concentration(_curve(), epsilon=1.0)
    with pytest.raises(ValueError, match="no extinction coefficient"):
        concentration(_curve(uv_info=UV_NORMALIZED))
    limited = concentration(_curve(values=np.array([100.0, 3000.0]), time=[0, 1], uv_info=UV_NORMALIZED),
                            epsilon=1.0, absorbance_limit=2, absorbance_limit_unit="AU")
    assert np.isfinite(limited.values[0]) and np.isnan(limited.values[1])


def test_concentration_response_factor_and_note():
    curve = _curve(values=np.full(20, 500.0), uv_info=UV_NORMALIZED)
    plain = concentration(curve, epsilon=1.25)
    assert plain.conversion["response_factor"] is None and plain.conversion["note"] == ACCURACY_NOTE
    low = concentration(curve, epsilon=1.25, response_factor=0.92)  # detector reads 8 % low
    assert low.values[0] == pytest.approx(plain.values[0] / 0.92)
    assert low.conversion["epsilon_ml_per_mg_cm"] == pytest.approx(1.25 * 0.92)
    assert low.conversion["epsilon_given_ml_per_mg_cm"] == pytest.approx(1.25)
    assert low.conversion["response_factor"] == 0.92 and low.conversion["note"] == ACCURACY_NOTE
    with pytest.raises(ValueError, match="response factor"):
        concentration(curve, epsilon=1.25, response_factor=0)


# ---------------------------------------------------------------------------
# local test files (skipped when absent)
# ---------------------------------------------------------------------------

def _local_runs():
    runs = [open_run(p) for p in sorted(glob.glob(os.path.join(TEST_FILES, "U7.*", "*.zip")))]
    for path in sorted(glob.glob(os.path.join(TEST_FILES, "UFOL", "*.UFol"))):
        runs += open_folder(path)
    return runs


def test_local_runs_utc_closes_at_the_end():
    runs = _local_runs()
    if not runs:
        pytest.skip("no local test files in %s" % TEST_FILES)
    for run in runs:
        tl = run.timeline
        assert tl["quality"] in ("exact", "inferred")
        assert tl["residual_s"] is not None and abs(tl["residual_s"]) < 2
        assert tl["unplaced_s"] == 0
        notes = [w for w in run.warnings if is_axes_warning(w) and ": UTC: " in w]
        # imported results keep the start to the whole second, and UNICORN 7.11 adds an evaluated
        # curve with no time axis; nothing else is warned, and the 7.3 exports have neither
        assert all("to the whole second" in w or "no time axis stored" in w for w in notes), notes
        if os.sep + "U7.3" + os.sep in run.locator:
            assert not notes
    # every logged operator Pause becomes one pause; the folder exports have some (Still Open #4)
    for run in runs:
        tl = run.timeline
        assert sum(a["kind"] == "pause" for a in tl["anchors"]) == \
            sum(p["source"] == "pause_text" for p in tl["pauses"])
    assert any(p["source"] == "pause_text" for r in runs if r.source == "ufol" for p in r.timeline["pauses"])


def test_local_runs_cv():
    runs = _local_runs()
    if not runs:
        pytest.skip("no local test files in %s" % TEST_FILES)
    for run in runs:
        chrom = run.chromatogram()
        if run.column_volume is None:
            assert all(c.cv is None for c in chrom.curves.values())
            continue
        for curve in chrom.curves.values():
            if curve.volume is not None and curve.is_original:
                assert np.all(np.diff(curve.cv) >= -1e-9)
                assert curve.cv[-1] <= curve.volume[-1] / run.column_volume + 1e-6
