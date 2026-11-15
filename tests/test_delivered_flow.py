"""
The flow the pump delivered, from UNICORN's volume axis (pycorn/delivered_flow.py; Decide 6 in
docs/RUN_STRUCTURE_REVIEW.md): which pump the method-flow entry names, the tolerance, the fitted
stop / ramp / set-flow pieces, and their use in the CADET model view.
"""
import os

import numpy as np
import pytest

from pycorn import open_run
from pycorn.delivered_flow import delivered_flow, fit_flow, method_flow_basis, pump_rule, volume_axis
from pycorn.exporters.cadet import model_view
from pycorn.exporters.cadet.view import _linear_volume, _time_for_volume
from pycorn.model import Curve
from pycorn.run_log_sections import run_log_sections

from test_run_log_sections import _e, _run

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLES = os.path.join(HERE, "..", "samples")
RAMP = 0.42  # ml/min per s, a pressure-control ramp as the public samples show it


def _axis(flow_fn, end_s, hz=10.0, name="UV 1_280"):
    """A curve whose stored volume is the integral of `flow_fn(t_s)` ml/min, sampled at `hz` (float32 as stored)."""
    t = np.arange(0.0, end_s + 1e-9, 1.0 / hz)
    fine = np.arange(0.0, end_s + 1e-9, 0.001)
    q = np.array([flow_fn(x) for x in fine])
    cum = np.concatenate([[0.0], np.cumsum(0.5 * (q[1:] + q[:-1]) * np.diff(fine))]) / 60.0
    v = np.interp(t, fine, cum).astype(np.float32)
    return Curve(name=name, data_type="UV", unit="mAU", precision=3, curve_number=1, time=t / 60.0, volume=v,
                 values=np.zeros(len(t), dtype=np.float32), time_unit="min", volume_unit="ml", isochrone="Time",
                 is_original=True, column_volume=None, column_volume_unit=None, uv_info=None, point_sets=[], xml={})


def _flow_at(knots_t, knots_q, t):
    return float(np.interp(t, knots_t, knots_q))


def _sections(run):
    return run_log_sections(run)["sections"]


# --- the method-flow entry -----------------------------------------------------------------------

def test_method_flow_entries_name_the_pump():
    run = _run([_e(0.0, "The method flow is now calculated based on the system flow. (System)", type_="System",
                   status=""),
                _e(2.0, "The method flow is now calculated based on the sample flow. (System)", type_="System",
                   status=""),
                _e(5.0, "The method flow is now calculated based on the system flow. (System)", type_="System",
                   status="")])
    assert method_flow_basis(run.chromatogram()) == [(0.0, "system"), (120.0, "sample"), (300.0, "system")]


def test_without_the_entry_the_rule_picks_the_pump():
    assert pump_rule(5.0, 0.0) == "system"
    assert pump_rule(0.001, 0.17) == "sample"  # a system flow of 0.001 ml/min counts as stopped
    assert pump_rule(0.17, 0.17) == "system"


def test_the_named_pump_sets_the_flow_compared():
    # system 2 ml/min, sample 0.5 ml/min; the axis follows the sample pump from 1 min on
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
              _e(0.0, "Sample flow 0.500 {ml/min} Off"),
              _e(0.0, "The method flow is now calculated based on the system flow. (System)", type_="System",
                 status=""),
              _e(1.0, "Injection valve Direct inject"),
              _e(1.0, "The method flow is now calculated based on the sample flow. (System)", type_="System",
                 status=""),
              _e(3.0, "End 1/1/2024", status="")]
    run = _run(events, curves=[_axis(lambda t: 2.0 if t < 60 else 0.5, 180.0)])
    rows, warnings = delivered_flow(run.chromatogram(), _sections(run))
    live = [r for r in rows if r is not None]
    assert [(r["pump"], r["pump_source"], r["set_ml_min"]) for r in live] == [("system", "run_log", 2.0),
                                                                               ("sample", "run_log", 0.5)]
    assert not any(r["fitted"] for r in live) and not warnings


# --- tolerance and fit ---------------------------------------------------------------------------

def test_a_matching_axis_keeps_the_set_flow():
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
              _e(4.0, "End 1/1/2024", status="")]
    run = _run(events, curves=[_axis(lambda t: 2.0, 240.0)])
    rows, warnings = delivered_flow(run.chromatogram(), _sections(run))
    assert rows[0]["fitted"] is False and rows[0]["pieces"] == [(0.0, 240.0, 2.0, 0.0)]
    # no method-flow entry in this run: said once
    assert len(warnings) == 1 and "names no method-flow pump" in warnings[0]


def test_stop_then_ramp_then_set_flow():
    # a valve turn stops the pump 1 s, then it ramps 0 -> 5 ml/min at 0.42 ml/min per s
    def q(t):
        return 0.0 if t < 1.0 else min(5.0, (t - 1.0) * RAMP)
    curve = _axis(q, 60.0)
    t, v = curve.time * 60.0, curve.volume.astype(float)
    kt, kq, kind = fit_flow(t, v, 0.0, 60.0, 5.0)
    assert kind == "fitted"
    assert np.sum(0.5 * (kq[1:] + kq[:-1]) * np.diff(kt)) / 60.0 == pytest.approx(v[-1] - v[0], abs=1e-9)
    assert _flow_at(kt, kq, 0.5) == pytest.approx(0.0, abs=0.1)
    assert _flow_at(kt, kq, 7.0) == pytest.approx(6.0 * RAMP, abs=0.15)
    assert _flow_at(kt, kq, 30.0) == 5.0  # the plateau is the set flow itself
    rising = [(a, b) for a, b, qa, qb in zip(kt[:-1], kt[1:], kq[:-1], kq[1:]) if qb > qa + 0.05]
    assert sum(b - a for a, b in rising) == pytest.approx(5.0 / RAMP, abs=0.5)


def test_a_ramp_cut_off_by_the_section_end():
    # ramping from 0 at 0.42 ml/min per s, the section ends after 6 s, far below the set 4 ml/min
    curve = _axis(lambda t: t * RAMP, 6.0)
    t, v = curve.time * 60.0, curve.volume.astype(float)
    kt, kq, kind = fit_flow(t, v, 0.0, 6.0, 4.0)
    assert kind == "fitted"
    assert kq[-1] == pytest.approx(6.0 * RAMP, abs=0.15)
    assert max(kq) < 4.0
    assert np.sum(0.5 * (kq[1:] + kq[:-1]) * np.diff(kt)) / 60.0 == pytest.approx(v[-1] - v[0], abs=1e-9)


def test_a_stop_inside_a_section_and_a_step_back():
    # pressure control off: 10 ml/min, the pump stops 1.2 s at 20 s and steps back
    curve = _axis(lambda t: 0.0 if 20.0 <= t < 21.2 else 10.0, 40.0)
    t, v = curve.time * 60.0, curve.volume.astype(float)
    kt, kq, kind = fit_flow(t, v, 0.0, 40.0, 10.0)
    assert _flow_at(kt, kq, 10.0) == 10.0 and _flow_at(kt, kq, 30.0) == 10.0
    assert _flow_at(kt, kq, 20.6) == pytest.approx(0.0, abs=0.5)
    assert len(kt) - 1 <= 6  # a step is one fast change, not many tiny pieces
    assert np.all(kq >= 0)


def test_a_short_section_gets_one_constant_flow():
    curve = _axis(lambda t: 2.5, 10.0)
    t, v = curve.time * 60.0, curve.volume.astype(float)
    kt, kq, kind = fit_flow(t, v, 4.0, 4.6, 5.0)
    assert kind == "mean" and kq.tolist() == pytest.approx([2.5, 2.5])


def test_the_fit_in_sections_with_a_warning():
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 1.000 {ml/min} Off"),
              _e(1.0, "System flow 5.000 {ml/min} Pre column pressure"), _e(3.0, "End 1/1/2024", status="")]
    run = _run(events, curves=[_axis(lambda t: 1.0 if t < 60 else min(5.0, 1.0 + (t - 60) * RAMP), 180.0)])
    rows, warnings = delivered_flow(run.chromatogram(), _sections(run))
    assert rows[0]["fitted"] is False
    assert rows[1]["fitted"] and rows[1]["kind"] == "fitted"
    assert rows[1]["ramp_s"] == pytest.approx(4.0 / RAMP, abs=0.5)
    assert rows[1]["axis_ml"] < rows[1]["logged_ml"]
    assert any("60.0-180.0 s" in w and "fitted to the axis" in w and "ramped" in w for w in warnings)


def test_volume_booked_at_a_pause_is_left_out():
    # 10 ml/min; a pause at 60 s elapsed; the first axis step after it holds 0.04 ml extra (no time)
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 10.000 {ml/min} Off"),
              _e(2.0, "End 1/1/2024", status="")]
    curve = _axis(lambda t: 10.0, 120.0)
    curve.volume = curve.volume + np.where(curve.time * 60.0 > 60.0 + 1e-6, 0.04, 0.0).astype(np.float32)
    run = _run(events, curves=[curve], pauses=[(60.0, 30.0)])
    rows, warnings = delivered_flow(run.chromatogram(), _sections(run))
    live = [r for r in rows if r is not None]
    assert len(live) == 2 and not any(r["fitted"] for r in live)
    assert live[1]["axis_ml"] == pytest.approx(10.0, abs=0.002)
    assert any("books 0.04 ml at 1 pause(s)" in w for w in warnings)


# --- CADET ---------------------------------------------------------------------------------------

def test_linear_flow_volume_and_time():
    poly = (1.0, 0.5)  # 1 rising 0.5 per s
    assert _linear_volume(poly, 2.0) == pytest.approx(3.0)
    assert _time_for_volume(poly, 3.0, 10.0) == pytest.approx(2.0)
    assert _time_for_volume((2.0, 0.0), 3.0, 10.0) == pytest.approx(1.5)


def test_cadet_sections_follow_the_fitted_flow():
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 1.000 {ml/min} Off"),
              _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"),
              _e(1.0, "System flow 5.000 {ml/min} Pre column pressure"), _e(3.0, "End 1/1/2024", status="")]
    curve = _axis(lambda t: 1.0 if t < 60 else min(5.0, 1.0 + (t - 60) * RAMP), 180.0)
    run = _run(events, curves=[curve])
    exp = model_view(run)
    total = sum(_linear_volume(s.flow_m3_s, s.end_s - s.start_s) for s in exp.sections) * 1e6
    assert total == pytest.approx(float(curve.volume[-1]), abs=1e-4)
    assert all(a.end_s == pytest.approx(b.start_s) for a, b in zip(exp.sections, exp.sections[1:]))
    assert {s.flow_source for s in exp.sections} == {"run_log", "volume_axis"}
    ramp = [s for s in exp.sections if s.flow_m3_s[1] > 0]
    assert ramp and ramp[0].start_s == pytest.approx(60.0, abs=0.3)
    plain = model_view(run, flow_from_axis=False)
    assert len(plain.sections) == 2 and {s.flow_source for s in plain.sections} == {"run_log"}


# --- public samples ------------------------------------------------------------------------------

def _feed_ml(exp):
    return sum(_linear_volume(s.flow_m3_s, s.end_s - s.start_s) for s in exp.sections if s.feed) * 1e6


def test_u73_feed_is_the_volume_axis():
    # the injection valve turns at half flow just before the feed's flow drops: 0.417 ml logged, 0.389 ml delivered
    run = open_run(os.path.join(SAMPLES, "u73_blank.zip"))
    assert _feed_ml(model_view(run, flow_from_axis=False)) == pytest.approx(0.4167, abs=5e-4)
    assert _feed_ml(model_view(run)) == pytest.approx(0.3892, abs=5e-4)


@pytest.mark.parametrize("name", ["u711_cadet_reference.zip", "u711_proa_day12.zip", "u73_blank.zip"])
def test_pressure_control_ramps_in_the_samples(name):
    run = open_run(os.path.join(SAMPLES, name))
    chrom = run.chromatogram()
    rows, _ = delivered_flow(chrom, _sections(run))
    ramps = sorted(r["ramp_s"] for r in rows if r is not None and r["fitted"] and r["ramp_s"] > 5)
    # 1 -> 5 ml/min (about 9.5 s) and 0 -> 5 ml/min after the pause (about 12 s)
    assert any(8.0 <= x <= 11.0 for x in ramps) and any(11.0 < x <= 13.0 for x in ramps)


@pytest.mark.parametrize("name", sorted(f for f in os.listdir(SAMPLES) if f.endswith(".zip")))
def test_fitted_volume_equals_the_axis_in_every_sample(name):
    run = open_run(os.path.join(SAMPLES, name))
    chrom = run.chromatogram()
    axis = volume_axis(chrom)
    if axis is None:
        pytest.skip("no volume axis")
    rows, _ = delivered_flow(chrom, _sections(run))
    sets = [None if r is None else (r["set_ml_min"] or 0.0) for r in rows]
    for k, r in enumerate(rows):
        if r is None or not r["fitted"]:
            continue
        pieces = r["pieces"]
        volume = sum((q0 * (b - a) + 0.5 * slope * (b - a) ** 2) / 60.0 for a, b, q0, slope in pieces)
        assert volume == pytest.approx(r["axis_ml"], abs=1e-6)
        assert all(q0 >= -1e-9 and q0 + slope * (b - a) >= -1e-9 for a, b, q0, slope in pieces)
        top = max(max(q0, q0 + slope * (b - a)) for a, b, q0, slope in pieces)
        # never far above the set flow in force here or just before (a pump running down from it)
        near = [x for x in sets[max(k - 2, 0):k + 3] if x is not None]
        if max(near) > 0:  # all stopped as logged: only the axis says what ran
            assert top <= max(near) * 1.05 + 0.1
