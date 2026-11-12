"""
Sections from the executed run log (pycorn/run_log_sections.py, docs/ROADMAP.md D13-D16).

Synthetic runs cover the state machine; the local test files (gitignored), when present, are
checked against their block events and their flow and Conc B curves.
"""
import glob
import os
from collections import OrderedDict

import numpy as np
import pytest

from pycorn import open_run
from pycorn.model import Chromatogram, Curve, Event, EventCurve, Run
from pycorn.run_log_sections import (SPAN_COLUMNS, column_flow, inlets_text, run_log_sections, sample_use,
                                     span_details, to_frame)

HERE = os.path.dirname(os.path.abspath(__file__))
TEST_FILES = os.path.join(HERE, "..", "unicorn_test_files")


def _curve(name, values_fn, unit, data_type="Other", end_min=20.0):
    t = np.arange(0.0, end_min + 1e-9, 1.0 / 60)
    values = np.asarray([values_fn(x) for x in t], dtype=np.float32)
    return Curve(name=name, data_type=data_type, unit=unit, precision=3, curve_number=1, time=t, volume=t,
                 values=values, time_unit="min", volume_unit="ml", isochrone="Time", is_original=True,
                 column_volume=None, column_volume_unit=None, uv_info=None, point_sets=[], xml={})


def _e(time_min, text, type_="Method", subtype="Undefined", status=" (Completed)"):
    return Event(time=time_min, volume=time_min, text=text + status, type=type_, subtype=subtype,
                 instruction_feedback=None, xml={})


def _block(time_min, text, start=True):
    return _e(time_min, text, subtype="BlockStart" if start else "BlockEnd", status=" (Issued) (Processing) (Completed)")


def _run(events, curves=(), pauses=(), vc_ml=None):
    log = EventCurve(name="Run Log", type="Logbook", curve_number=9, time_unit="min", volume_unit="ml",
                     is_original=True, column_volume=None, column_volume_unit=None, events=list(events), xml={})
    chrom = Chromatogram(name="Chrom.1", chromatogram_id=1, info={}, curves=OrderedDict((c.name, c) for c in curves),
                         event_curves=OrderedDict([(log.name, log)]), peak_tables=[], pool_tables=None)
    chrom.timeline = {"pauses": [OrderedDict([("elapsed_s", p[0]), ("duration_s", p[1]), ("source", "pause_text")])
                                 for p in pauses]}
    chrom.column_volume_info = {"value": vc_ml, "unit": "ml"} if vc_ml is not None else {"value": None}
    return Run(source="zip", locator="synthetic", result={"Name": "run"},
               chromatograms=OrderedDict([(chrom.name, chrom)]), metadata=OrderedDict(), evaluation_log=[],
               manifest=[], entries=OrderedDict())


def _start(*events):
    return [_e(0.0, "Method Run 1/1/2024 10:00:00 AM -05:00", subtype="BlockStart", status=""),
            _e(0.0, "System flow 5.000 {ml/min} Off"), _e(0.0, "Gradient 0.0 {%B} 0.00 {min}")] + list(events)


def _b_at(result, wall_s):
    for s in result["sections"]:
        if s.start_s <= wall_s <= s.end_s and not s.paused:
            return s.percent_b_at(wall_s)
    raise AssertionError("no section at %s" % wall_s)


def _sections_cover(result):
    secs = result["sections"]
    assert secs[0].start_s == 0.0
    for a, b in zip(secs, secs[1:]):
        assert b.start_s == pytest.approx(a.end_s)
    assert secs[-1].end_s == pytest.approx(result["end_s"])


# ---------------------------------------------------------------------------
# %B: steps, ramps, interruptions
# ---------------------------------------------------------------------------

def test_step_and_time_ramp_then_hold():
    r = run_log_sections(_run(_start(_e(2.0, "Gradient 100.0 {%B} 4.00 {min}"), _e(10.0, "End 1/1/2024", status=""))))
    _sections_cover(r)
    assert _b_at(r, 60.0) == 0.0
    assert _b_at(r, 120.0 + 120.0) == pytest.approx(50.0)
    assert _b_at(r, 400.0) == pytest.approx(100.0)  # ramp over at 6 min, then holds
    assert any("end of %B ramp" in c for s in r["sections"] for c in s.changes)
    assert not [w for w in r["warnings"] if not w.startswith("inlets:")]  # no system configuration: inlets not known


def test_volume_ramp_follows_the_system_flow_and_cv_uses_vc():
    # 10 CV of a 2 ml column = 20 ml: 2 min at 5 ml/min, then 1 ml/min for the rest
    events = _start(_e(1.0, "Gradient 100.0 {%B} 10.00 {CV}"), _e(2.0, "System flow 1.000 {ml/min} Off"),
                    _e(30.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events, vc_ml=2.0))
    assert _b_at(r, 120.0) == pytest.approx(25.0)  # 5 ml of 20
    # remaining 15 ml at 1 ml/min end at 2 + 15 = 17 min
    assert _b_at(r, 17 * 60.0 - 1) == pytest.approx(100.0 - 100.0 / 20 / 60, abs=1e-6)
    assert _b_at(r, 18 * 60.0) == pytest.approx(100.0)


def test_cv_ramp_without_vc_is_unknown_and_warned():
    r = run_log_sections(_run(_start(_e(1.0, "Gradient 100.0 {%B} 10.00 {CV}"), _e(5.0, "End 1/1/2024", status=""))))
    assert _b_at(r, 120.0) is None
    assert any("no Vc" in w for w in r["warnings"])


def test_a_new_gradient_takes_over_from_the_value_reached():
    events = _start(_e(1.0, "Gradient 100.0 {%B} 10.00 {min}"), _e(3.0, "Gradient 0.0 {%B} 2.00 {min}", "Manual"),
                    _e(8.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events))
    assert _b_at(r, 180.0) == pytest.approx(20.0)
    assert _b_at(r, 240.0) == pytest.approx(10.0)
    assert _b_at(r, 400.0) == pytest.approx(0.0)


def test_only_completed_entries_count():
    events = _start(_e(1.0, "Gradient 50.0 {%B} 0.00 {min}", status=" (Issued) (Processing)"),
                    _e(1.5, "Gradient 50.0 {%B} 0.00 {min}"), _e(3.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events))
    assert _b_at(r, 80.0) == 0.0 and _b_at(r, 100.0) == 50.0


# ---------------------------------------------------------------------------
# Hold and pauses
# ---------------------------------------------------------------------------

def test_hold_freezes_ramps_and_defers_gradient_instructions_to_continue():
    events = _start(_e(1.0, "Gradient 100.0 {%B} 10.00 {min}"),
                    _e(2.0, "Hold 1/1/2024 10:02:00 AM -05:00", "Manual"),
                    _e(2.5, "Gradient 0.0 {%B} 0.00 {min}", "Manual"),
                    _e(3.0, "System flow 2.000 {ml/min} Off", "Manual"),
                    _e(4.0, "Continue 1/1/2024 10:04:00 AM -05:00", "Manual"),
                    _e(6.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events))
    assert _b_at(r, 150.0) == pytest.approx(10.0)  # ramp stopped at the Hold (1 min of 10)
    assert _b_at(r, 230.0) == pytest.approx(10.0)
    assert _b_at(r, 250.0) == pytest.approx(0.0)  # the deferred step acts at the Continue
    held = [s for s in r["sections"] if s.held]
    assert held and held[0].elapsed_start_s == pytest.approx(120.0)
    flows = {round(s.elapsed_start_s): s.system_flow_ml_min for s in r["sections"]}
    assert flows[180] == 2.0  # flow changes act during the Hold
    assert any("waits for Continue" in c for s in r["sections"] for c in s.changes)


def test_a_pause_during_a_hold_resumes_into_the_hold():
    events = _start(_e(1.0, "Hold 1/1/2024 10:01:00 AM -05:00", "Manual"),
                    _e(2.0, "Pause Infinite min 1/1/2024 10:02:00 AM -05:00", "Manual"),
                    _e(2.0, "Continue 1/1/2024 10:03:00 AM -05:00", "Manual"),
                    _e(2.5, "Gradient 100.0 {%B} 0.00 {min}", "Manual"),
                    _e(4.0, "Continue 1/1/2024 10:05:00 AM -05:00", "Manual"),
                    _e(6.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events, pauses=[(120.0, 60.0)]))
    assert _b_at(r, 60.0 + 200.0) == 0.0  # still held after the first Continue (wall clock: +60 s pause)
    assert _b_at(r, 240.0 + 70.0) == 100.0


def test_a_hold_during_a_pause_takes_over_and_the_next_continue_ends_it():
    events = _start(_e(1.0, "Pause Infinite min 1/1/2024 10:01:00 AM -05:00"),
                    _e(1.0, "Hold 1/1/2024 10:02:00 AM -05:00", "Manual"),
                    _e(2.0, "Gradient 100.0 {%B} 0.00 {min}", "Manual"),
                    _e(3.0, "Continue 1/1/2024 10:04:00 AM -05:00", "Manual"),
                    _e(5.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events, pauses=[(60.0, 60.0)]))
    assert _b_at(r, 60.0 + 150.0) == 0.0  # gradient at 2 min elapsed waits (wall = elapsed + 60 s)
    assert _b_at(r, 60.0 + 190.0) == 100.0  # Continue at 3 min elapsed


def test_pause_is_a_zero_flow_section_and_shifts_the_clock():
    events = _start(_e(1.0, "Gradient 100.0 {%B} 4.00 {min}"), _e(8.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events, pauses=[(120.0, 30.0)]))
    _sections_cover(r)
    paused = [s for s in r["sections"] if s.paused]
    assert len(paused) == 1
    p = paused[0]
    assert (p.start_s, p.end_s, p.column_flow_ml_min, p.percent_b) == (120.0, 150.0, 0.0, (25.0, 0.0))
    assert _b_at(r, 151.0) == pytest.approx(25.0 + 100.0 / 240)  # the ramp goes on after the pause
    assert r["end_s"] == pytest.approx(8 * 60.0 + 30.0)
    assert any("zero-flow section" in w for w in r["warnings"])


def test_instructions_at_a_pause_act_after_it():
    events = _start(_e(2.0, "System flow 1.000 {ml/min} Off", "Manual"), _e(4.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events, pauses=[(120.0, 60.0)]))
    p = next(s for s in r["sections"] if s.paused)
    before = r["sections"][r["sections"].index(p) - 1]
    after = r["sections"][r["sections"].index(p) + 1]
    assert before.system_flow_ml_min == 5.0 and after.system_flow_ml_min == 1.0
    assert after.start_s == pytest.approx(180.0)


# ---------------------------------------------------------------------------
# flow onto the column, feed, valves, quaternary
# ---------------------------------------------------------------------------

def test_column_flow_rule():
    assert column_flow(5.0, 0.2, "Manual load") == 5.0
    assert column_flow(5.0, 0.2, "Inject") == 5.0
    assert column_flow(5.0, 0.2, "Direct inject") == 0.2
    assert column_flow(5.0, 0.2, "System pump waste") == 0.0
    assert column_flow(5.0, 0.2, None) == 5.0
    assert column_flow(5.0, 0.2, "Inject", bypass=True) == 0.0


def test_feed_valves_and_inlets():
    events = _start(_e(0.5, "Inlet A A2"), _e(0.5, "Sample inlet S3"),
                    _e(1.0, "Injection valve Direct inject", "Manual"), _e(1.0, "Sample flow 0.500 {ml/min} Off"),
                    _e(2.0, "Injection valve System pump waste"), _e(3.0, "Injection valve Inject"),
                    _e(4.0, "Injection valve Manual load"), _e(4.5, "Column position By-pass Down flow"),
                    _e(5.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events))
    by_start = {round(s.elapsed_start_s): s for s in r["sections"]}
    assert by_start[60].feed and by_start[60].column_flow_ml_min == 0.5
    assert not by_start[120].feed and by_start[120].column_flow_ml_min == 0.0
    assert by_start[180].feed and by_start[180].column_flow_ml_min == 5.0
    assert not by_start[240].feed
    assert by_start[270].bypass and by_start[270].column_position == "By-pass"
    assert by_start[270].column_flow_ml_min == 0.0 and by_start[270].system_flow_ml_min == 5.0
    assert by_start[60].inlets == {"A": "A2", "sample": "S3"}
    assert any("System pump waste" in w for w in r["warnings"])


def test_inlets_follow_percent_b_and_mark_assumed_positions():
    # a system with a valve on every line (stored configuration); line B is never set: the
    # valve's first position B1, an assumption marked as one (test_system_configuration.py: the rest)
    from test_system_configuration import DISPLAY_2, FAMILY_2, configured
    events = _start(_e(0.5, "Inlet A A2"), _e(1.0, "Injection valve Inject"), _e(2.0, "Injection valve Manual load"),
                    _e(3.0, "Gradient 100.0 {%B} 2.00 {min}"), _e(6.0, "Gradient 0.0 {%B} 0.00 {min}"),
                    _e(7.0, "Injection valve Direct inject"), _e(8.0, "Sample inlet S2"), _e(9.0, "End 1/1/2024", status=""))
    r = run_log_sections(configured(_run(events), FAMILY_2, DISPLAY_2))
    use = {round(s.elapsed_start_s): inlets_text(s) for s in r["sections"]}
    assert use[0] == "A1 (assumed)" and use[30] == "A2" and use[60] == "A2, Loop"
    assert use[180] == "ramp A2 to B1 (assumed)" and use[300] == "B1 (assumed)" and use[360] == "A2"
    assert use[420] == "A2, Buffer (assumed)" and use[480] == "A2, S2"
    assert inlets_text(r["sections"][[round(s.elapsed_start_s) for s in r["sections"]].index(180)],
                         ramp="%s -> %s") == "A2 -> B1 (assumed)"
    # a constant %B between 0 and 100: both lines joined with '+'
    mix = run_log_sections(configured(_run(_start(_e(1.0, "Gradient 30.0 {%B} 0.00 {min}"),
                                                  _e(2.0, "End 1/1/2024", status=""))), FAMILY_2, DISPLAY_2))
    assert [inlets_text(s) for s in mix["sections"]][-1] == "A1 (assumed)+B1 (assumed)"
    # the phase / injection / fraction columns and the sections table carry it too, with how sure
    assert span_details(r, 0, 540)["inlets"].startswith("A1 (assumed) -> A2 -> A2, Loop")
    assert span_details(r, 0, 540)["inlets_source"].startswith("assumed -> known -> assumed")
    assert list(to_frame(r)["inlets"])[:2] == ["A1 (assumed)", "A2"]
    assert list(to_frame(r)["inlets_source"])[:2] == ["assumed", "known"]
    assert "inlets_in_use" not in span_details(r, 0, 540) and "inlets_in_use" not in to_frame(r).columns


def test_sample_use_by_valve_bypass_and_buffer_inlet():
    assert sample_use("Manual load") is None
    assert sample_use("Inject") == "feed" and sample_use("Inject", bypass=True) == "to_bypass"
    assert sample_use("Direct inject", sample_inlet="S1") == "feed"
    assert sample_use("Direct inject", bypass=True, sample_inlet="Buffer") == "buffer"
    assert sample_use("Inject", sample_inlet="Buffer") == "feed"  # the loop, not the sample pump
    events = _start(_e(0.5, "Column position By-pass Down flow"), _e(1.0, "Injection valve Inject"),
                    _e(2.0, "Column position Position 1 Down flow"), _e(3.0, "Sample inlet Buffer"),
                    _e(3.0, "Sample flow 1.000 {ml/min} Off"), _e(3.0, "Injection valve Direct inject"),
                    _e(4.0, "Injection valve Manual load"), _e(5.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events))
    uses = {round(s.elapsed_start_s): (s.sample_use, s.feed) for s in r["sections"]}
    assert uses[60] == ("to_bypass", False)
    assert uses[120] == ("feed", True)
    assert uses[180] == ("buffer", False)
    assert uses[240] == (None, False)
    assert "sample_use" in to_frame(r).columns


def test_quaternary_gradient_sets_q1_to_q4():
    events = _start(_e(1.0, "Quaternary gradient 100.0 {%} 0.0 {%} 0.0 {%} 0.0 {%} 0.00 {min}"),
                    _e(2.0, "Quaternary gradient 0.0 {%} 50.0 {%} 0.0 {%} 50.0 {%} 2.00 {min}"),
                    _e(6.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events))
    first = r["sections"][0]
    assert first.percent_q is None  # no quaternary instruction yet
    ramp = next(s for s in r["sections"] if s.elapsed_start_s == pytest.approx(120.0))
    assert ramp.percent_q[0] == (100.0, 0.0, 0.0, 0.0)
    assert ramp.percent_q[1] == pytest.approx((-100.0 / 120, 50.0 / 120, 0.0, 50.0 / 120))
    assert r["sections"][-1].percent_q[0] == pytest.approx((0.0, 50.0, 0.0, 50.0))


def test_wavelength_and_vc_from_base():
    events = _start(_e(0.0, "Base CV, Vc=4.000 {ml}, Some column", status=""),
                    _e(0.1, "Wavelength 280 {nm} Off {nm} 260 {nm}"), _e(1.0, "Gradient 100.0 {%B} 1.00 {CV}"),
                    _e(3.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events, vc_ml=99.0))
    s = r["sections"][-1]
    assert s.wavelengths_nm == (280.0, 0.0, 260.0)
    assert s.vc_ml == 4.0
    assert _b_at(r, 60.0 + 24.0) == pytest.approx(50.0)  # 4 ml at 5 ml/min = 48 s


# ---------------------------------------------------------------------------
# settings without an instruction; checks against curves
# ---------------------------------------------------------------------------

def test_flow_before_the_first_instruction_comes_from_the_curve():
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"),
              _e(2.0, "System flow 5.000 {ml/min} Off"), _e(4.0, "End 1/1/2024", status="")]
    flow = _curve("System flow", lambda t: 3.0 if t < 2 else 5.0, "ml/min")
    r = run_log_sections(_run(events, curves=[flow]))
    first = r["sections"][0]
    assert first.system_flow_ml_min == pytest.approx(3.0) and first.sources["system_flow"] == "curve"
    assert first.sample_flow_ml_min == 0.0 and first.sources["sample_flow"] == "default"
    assert any("system flow at 0.0-120.0 s" in w and "from the curve" in w for w in r["warnings"])


def test_checks_warn_on_mismatch_only():
    events = _start(_e(2.0, "Gradient 100.0 {%B} 4.00 {min}"), _e(10.0, "End 1/1/2024", status=""))
    good_b = _curve("Conc B", lambda t: 0.0 if t < 2 else min(100.0, (t - 2) * 25.0), "%")
    flow = _curve("System flow", lambda t: 5.0, "ml/min")
    r = run_log_sections(_run(events, curves=[good_b, flow]))
    assert not [w for w in r["warnings"] if "differs" in w or "curve median" in w]
    assert r["checks"][0]["system_flow_measured"] == pytest.approx(5.0)
    bad_b = _curve("Conc B", lambda t: 0.0, "%")
    slow = _curve("System flow", lambda t: 4.0, "ml/min")
    r = run_log_sections(_run(events, curves=[bad_b, slow]))
    assert any("%B differs" in w for w in r["warnings"])
    assert any("curve median 4" in w for w in r["warnings"])


def test_unrecognized_instructions_are_reported_once_and_known_ones_ignored():
    events = _start(_e(1.0, "Alarm system pressure Enabled 4.00 {MPa} 0.00 {MPa}"), _e(1.0, "Next breakpoint", "Manual"),
                    _e(1.0, "Flow 3.000 {ml/min}"), _e(1.5, "Flow 3.000 {ml/min}"), _e(3.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events))
    unknown = [w for w in r["warnings"] if "not recognized" in w]
    assert unknown == ["run-log instruction not recognized, ignored for sections: 'Flow 3.000 {ml/min}'"]


def test_blocks_and_set_marks():
    events = _start(_block(0.0, "Phase Equilibration"), _block(0.0, "Block Equilibrate"),
                    _e(1.0, 'Set mark "load start"', subtype="SetMark", status=" (Issued) (Processing) (Completed)"),
                    _block(2.0, "End_Block", start=False), _block(2.0, "Block Wash"),
                    _block(3.0, "End_Block", start=False), _block(3.0, "End Phase", start=False),
                    _e(4.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events))
    names = [(round(s.elapsed_start_s), s.phase, s.block) for s in r["sections"]]
    assert names == [(0, "Equilibration", "Equilibrate"), (120, "Equilibration", "Wash"), (180, None, None)]
    assert r["sections"][0].set_marks == ["load start"]
    assert len(to_frame(r)) == 3


def test_span_details_join_a_split_ramp_and_skip_pauses():
    # ramp 0 -> 100 %B over 4 min from 1 min, split by a flow change at 3 min and a 30 s pause at 2 min
    events = _start(_e(1.0, "Gradient 100.0 {%B} 4.00 {min}"), _e(3.0, "System flow 1.000 {ml/min} Off"),
                    _e(8.0, "End 1/1/2024", status=""))
    r = run_log_sections(_run(events, pauses=[(120.0, 30.0)]))
    whole = span_details(r, 0.0, 480.0)
    assert list(whole) == SPAN_COLUMNS
    assert whole["percent_b"] == "0 -> ramp 0 to 100 -> 100"
    assert whole["system_flow_ml_min"] == whole["column_flow_ml_min"] == "5 -> 1"  # the pause gives no 0
    assert whole["percent_q"] is None and whole["set_marks"] is None
    # elapsed 120-240 s: from the pause (25 %B) to 75 %B, cut inside the section after the flow change
    assert span_details(r, 120.0, 240.0)["percent_b"] == "ramp 25 to 75"
    assert span_details(r, 480.0, 480.0)["system_flow_ml_min"] is None


def test_phase_table_gets_lengths_and_what_ran():
    from pycorn.run_export import event_table_frame
    events = _start(_block(0.0, "Phase Equilibration"), _block(0.0, "Block Equilibrate"),
                    _e(1.0, 'Set mark "load start"', subtype="SetMark", status=" (Issued) (Processing) (Completed)"),
                    _block(2.0, "End_Block", start=False), _block(2.0, "Block Wash"),
                    _e(2.0, "Inlet A A2"), _block(3.0, "End_Block", start=False),
                    _block(3.0, "End Phase", start=False), _e(4.0, "End 1/1/2024", status=""))
    run = _run(events)
    chrom = run.chromatogram()
    empty = event_table_frame(chrom, "phase", False)  # the sections could not be built
    assert list(empty.columns) == list(chrom.phase_table().columns) and empty["blocks"].isna().all()
    assert list(empty["length_time"]) == [3.0]
    for frame in (chrom.phase_table(), event_table_frame(chrom, "phase", run_log_sections(run, chrom))):
        row = frame.iloc[0]
        assert (row["blocks"], row["set_marks"], row["inlets"]) == ("Equilibrate -> Wash", "load start", "A (not known) -> A2")
        assert (row["system_flow_ml_min"], row["percent_b"], row["length_time"]) == ("5", "0", 3.0)


def test_no_run_log():
    run = _run([])
    run.chromatogram().event_curves.clear()
    assert run_log_sections(run)["warnings"] == ["no run log; no sections"]


# ---------------------------------------------------------------------------
# local runs (gitignored)
# ---------------------------------------------------------------------------

LOCAL = sorted(glob.glob(os.path.join(TEST_FILES, "U7.*", "*.zip")))


@pytest.mark.skipif(not LOCAL, reason="local test files not present")
@pytest.mark.parametrize("path", LOCAL, ids=os.path.basename)
def test_local_runs_match_blocks_and_curves(path):
    run = open_run(path)
    chrom = run.chromatogram()
    r = run_log_sections(run)
    _sections_cover(r)
    assert not [w for w in r["warnings"] if "differs" in w or "curve median" in w or "not recognized" in w]
    starts = {round(s.elapsed_start_s, 3) for s in r["sections"] if not s.paused}
    log = chrom.event_curves_of_type("Logbook")[0]
    for e in log.events:
        if e.subtype == "BlockStart" and (e.text or "").startswith("Block ") and e.time * 60 < r["end_elapsed_s"]:
            assert round(e.time * 60, 3) in starts, e.text
    pauses = [s for s in r["sections"] if s.paused]
    assert len(pauses) == len(chrom.timeline["pauses"])
    assert r["end_s"] == pytest.approx(r["end_elapsed_s"] + sum(p["duration_s"] for p in chrom.timeline["pauses"]))
