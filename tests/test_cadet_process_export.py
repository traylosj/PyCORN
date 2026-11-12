"""
CADET-Process objects and CSV/JSON (pycorn/exporters/cadet/process_export.py, docs/ROADMAP.md
C2). The CADET-Process tests are skipped when it isn't installed; the rest needs no CADET import.
"""
import glob
import json
import os
from types import SimpleNamespace

import numpy as np
import pytest

from pycorn import open_run
from pycorn.exporters.cadet import build_experiment, process_inputs, to_csv
from pycorn.exporters.cadet.process_export import _mix, _section_flow_at, system_flows, system_units

from test_cadet_experiment import _uv, _with_injection
from test_run_log_sections import _curve, _e, _run

HERE = os.path.dirname(os.path.abspath(__file__))
LOCAL = sorted(glob.glob(os.path.join(HERE, "..", "unicorn_test_files", "U7.*", "*.zip")))
Q = 2.0 / 6e7  # 2 ml/min in m^3/s


def _experiment(**kwargs):
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
              _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"), _e(0.0, "Wavelength 280 {nm} 0 {nm} 0 {nm}"),
              _e(1.0, "Injection valve Inject"), _e(2.0, "Injection valve Manual load"),
              _e(3.0, "Gradient 100.0 {%B} 4.00 {min}"), _e(9.0, "End 1/1/2024", status="")]
    curves = [_uv("UV 1_280", 50.0), _curve("Cond", lambda t: 10.0, "mS/cm", data_type="Conduction")]
    run = _with_injection(_run(events, curves=curves), 1.0)
    return build_experiment(run, **kwargs)


def _at(rows, t):
    return next(r for r in rows if r["start_s"] <= t < r["end_s"])


def _bypass_experiment(**kwargs):
    """2 ml/min; the column by-passed 0.5-2.5 min, and %B steps to 100 at 1 min (inside the by-pass)."""
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
              _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"), _e(0.0, "Wavelength 280 {nm} 0 {nm} 0 {nm}"),
              _e(0.5, "Column position By-pass Down flow"), _e(1.0, "Gradient 100.0 {%B} 0.00 {min}"),
              _e(2.5, "Column position Position 1 Down flow"), _e(5.0, "End 1/1/2024", status="")]
    return build_experiment(_run(events, curves=[_uv("UV 1_280", 50.0)]), **kwargs)


def test_system_flow_goes_past_a_bypassed_column():
    # owner, 2026-11-08 (M2): with flow during a by-pass the mixer stays on and in line
    exp = _bypass_experiment(mixer_volume=(0.6, "ml"), tubing_volume=(50, "ul"))
    inputs = process_inputs(exp)
    assert inputs["bypass_flow"]
    units = system_units(exp, inputs)
    rows = inputs["sections"]
    during, after = _at(rows, 100.0), _at(rows, 200.0)
    assert during["flow_m3_s"][0] == 0.0  # the column gets nothing
    assert during["bypass_flow_m3_s"][0] == pytest.approx(Q) and during["buffer_flow_m3_s"][0] == pytest.approx(Q)
    assert during["c_buffer"][0, 0] == pytest.approx(100.0)  # the new buffer reaches the mixer in the by-pass
    flows = {(a, b): q[0] for a, b, q in system_flows(units, during, True)}
    assert flows[("mixer", "column")] == 0.0
    assert flows[("mixer", "tubing")] == pytest.approx(Q) and flows[("tubing", "outlet")] == pytest.approx(Q)
    flows = {(a, b): q[0] for a, b, q in system_flows(units, after, True)}
    assert flows[("mixer", "tubing")] == 0.0 and flows[("mixer", "column")] == pytest.approx(Q)
    assert flows[("tubing", "outlet")] == pytest.approx(Q)
    # without CSTRs the by-pass goes from the buffer inlet to the outlet
    plain = process_inputs(_bypass_experiment())
    flows = {(a, b): q[0] for a, b, q in system_flows(system_units(_bypass_experiment(), plain), _at(
        plain["sections"], 100.0), True)}
    assert flows[("buffer", "outlet")] == pytest.approx(Q) and flows[("buffer", "column")] == 0.0
    # a run without a by-pass has no such connection
    assert not process_inputs(_experiment())["bypass_flow"]


def test_process_overrides_go_in_unchanged_in_cadet_process_names(tmp_path):
    # owner, 2026-11-08: one generic parameter block, no assumptions about what it holds
    exp = _bypass_experiment(mixer_volume=(0.6, "ml"), bed_height=(2.5, "cm"), diameter=(1.6, "cm"))
    block = {"flow_sheet": {"column": {"bed_porosity": 0.31, "axial_dispersion": 2e-7}}}
    side = json.load(open(to_csv(exp, str(tmp_path), process_overrides=block), encoding="utf-8"))
    assert side["process_overrides"] == block  # carried as given, for process.parameters = ...
    with pytest.raises(TypeError, match="process_overrides must be a dict"):
        to_csv(exp, str(tmp_path / "x"), process_overrides=[("flow_sheet", {})])
    pytest.importorskip("CADETProcess")
    from pycorn.exporters.cadet import to_cadet_process
    out = to_cadet_process(exp, process_overrides=block)
    column = out["process"].flow_sheet["column"]
    assert column.bed_porosity == 0.31 and list(column.axial_dispersion)[0] == 2e-7
    assert any("caller process_overrides set" in w and "flow_sheet.column.bed_porosity" in w for w in out["warnings"])
    assert block == {"flow_sheet": {"column": {"bed_porosity": 0.31, "axial_dispersion": 2e-7}}}  # caller's untouched
    with pytest.raises(ValueError, match="not accepted by CADET-Process"):  # CADET-Process checks the names
        to_cadet_process(exp, process_overrides={"flow_sheet": {"column": {"bed_porosty": 0.3}}})


def test_process_splits_the_mixer_past_a_bypassed_column():
    pytest.importorskip("CADETProcess")
    from pycorn.exporters.cadet import to_cadet_process
    proc = to_cadet_process(_bypass_experiment(mixer_volume=(0.6, "ml"), tubing_volume=(50, "ul")))["process"]
    splits = [e for e in proc.events if e.name.endswith("_mixer_split")]
    # to the column, or (by-passed) all past it; simulated in docs/CADET_GUIDE.md's terms by
    # tests/test_cadet_core_run.py on the Core input, which uses the same flows
    assert splits and {tuple(e.state) for e in splits} == {(1.0, 0.0), (0.0, 1.0)}


def test_placeholder_components_follow_percent_b_and_feed():
    inputs = process_inputs(_experiment())
    assert inputs["placeholder"] and inputs["components"] == ["salt", "sample"]
    assert any("placeholder" in w for w in inputs["warnings"])
    rows = inputs["sections"]
    assert inputs["cycle_time_s"] == pytest.approx(540.0)
    feed = _at(rows, 90.0)
    assert feed["feed"] and feed["c"][1, 0] == 1.0 and feed["c"][0, 0] == 0.0
    assert feed["flow_m3_s"] == (pytest.approx(Q), 0.0)
    ramp = _at(rows, 300.0)
    assert not ramp["feed"] and ramp["c"][1, 0] == 0.0
    assert ramp["c"][0, 1] == pytest.approx(100.0 / 240.0)  # 0 -> 100 mol/m^3 salt over 4 min
    assert ramp["c"][0, 0] + ramp["c"][0, 1] * (ramp["end_s"] - ramp["start_s"]) == pytest.approx(
        100.0 * (ramp["end_s"] - 180.0) / 240.0)
    assert _at(rows, 500.0)["c"][0, 0] == pytest.approx(100.0)


def test_caller_concentrations_are_all_or_nothing():
    exp = _experiment()
    with pytest.raises(ValueError, match=r"uses line B from 180\.0 s.*buffers\['B'\]"):
        process_inputs(exp, components=["salt", "mAb"], buffers={"A": [20, 0]}, feed=[20, 1.5])
    with pytest.raises(ValueError, match="feed"):
        process_inputs(exp, components=["salt", "mAb"], buffers={"A": [20, 0], "B": [500, 0]})
    with pytest.raises(ValueError, match="without components"):
        process_inputs(exp, buffers={"A": [20, 0]})
    with pytest.raises(ValueError, match="unknown component"):
        process_inputs(exp, components=["salt", "mAb"], buffers={"A": {"NaCl": 20}, "B": [500, 0]}, feed=[20, 1])
    inputs = process_inputs(exp, components=["salt", "mAb"], buffers={"A": {"salt": 20}, "B": [500, 0]},
                            feed={"salt": 20, "mAb": 1.5})
    assert not inputs["placeholder"]
    rows = inputs["sections"]
    assert list(_at(rows, 90.0)["c"][:, 0]) == [20.0, 1.5]
    ramp = _at(rows, 300.0)
    assert ramp["c"][0, 1] == pytest.approx(480.0 / 240.0) and ramp["c"][1].tolist() == [0, 0, 0, 0]
    assert _at(rows, 500.0)["c"][0, 0] == pytest.approx(500.0)


def _valve_experiment(a2="A2", b="B1"):
    """A buffer valve on each line: equilibrate on A1, wash on A2 from 3 min, B from 5 min."""
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
              _e(0.0, "Inlet A A1"), _e(0.0, "Inlet B %s" % b), _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"),
              _e(0.0, "Wavelength 280 {nm} 0 {nm} 0 {nm}"), _e(3.0, "Inlet A %s" % a2),
              _e(5.0, "Gradient 100.0 {%B} 0.00 {min}"), _e(7.0, "End 1/1/2024", status="")]
    return build_experiment(_run(events, curves=[_uv("UV 1_280", 50.0)]))


def test_buffers_per_inlet_position_with_the_line_as_fallback():
    exp = _valve_experiment()
    inputs = process_inputs(exp, components=["salt"], buffers={"A1": [20], "A2": [1000], "B": [500]})
    rows = inputs["sections"]
    assert _at(rows, 60.0)["c"][0, 0] == 20.0  # A1
    assert _at(rows, 240.0)["c"][0, 0] == 1000.0  # A2, the same pump line after the switch
    assert _at(rows, 360.0)["c"][0, 0] == 500.0  # B1 through the line key
    assert not any("stands for inlets" in w for w in inputs["warnings"])
    # the line alone for two positions: used for both, with a warning
    inputs = process_inputs(exp, components=["salt"], buffers={"A": [20], "B": [500]})
    assert _at(inputs["sections"], 240.0)["c"][0, 0] == 20.0
    assert any("buffers['A'] stands for inlets A1, A2" in w for w in inputs["warnings"])
    with pytest.raises(ValueError, match=r"inlet A2 \(line A\) from 180\.0 s.*buffers\['A2'\] or buffers\['A'\]"):
        process_inputs(exp, components=["salt"], buffers={"A1": [20], "B": [500]})
    with pytest.raises(ValueError, match=r"A3 is neither a pump line.*line A: A1, A2; line B: B1"):
        process_inputs(exp, components=["salt"], buffers={"A1": [20], "A2": [0], "A3": [0], "B": [500]})


def test_buffers_take_a_process_system_s_own_inlet_names():
    exp = _valve_experiment(a2="WFI", b="Elution_buffer")
    inputs = process_inputs(exp, components=["salt"], buffers={"A1": [20], "WFI": [0], "Elution_buffer": [500]})
    assert [_at(inputs["sections"], t)["c"][0, 0] for t in (60.0, 240.0, 360.0)] == [20.0, 0.0, 500.0]


def test_quaternary_mix_takes_line_a():
    s = SimpleNamespace(percent_b=(0.0, 0.0), percent_q=((50.0, 50.0, 0.0, 0.0), (0.0, 0.0, 0.0, 0.0)))
    c = _mix(s, {"Q1": np.array([100.0]), "Q2": np.array([0.0]), "B": np.array([1000.0])}, 1)
    assert c[0, 0] == pytest.approx(50.0)
    s = SimpleNamespace(percent_b=(50.0, 1.0), percent_q=None)  # A 10, B 30 at 50 %B rising 1 %/s
    c = _mix(s, {"A": np.array([10.0]), "B": np.array([30.0])}, 1)
    assert c[0, :2].tolist() == pytest.approx([20.0, 0.2])


def test_section_flow_at_times():
    exp = _experiment()
    q = _section_flow_at(exp.sections, np.array([-1.0, 30.0, 539.0, 540.0, 600.0]))
    assert q.tolist() == pytest.approx([0.0, Q, Q, Q, 0.0])


def test_csv_and_sidecar(tmp_path):
    exp = _experiment(epsilon=1.4, molecular_weight=150000)
    path = to_csv(exp, str(tmp_path))
    side = json.load(open(path, encoding="utf-8"))
    names = {f["name"]: f for f in side["files"]}
    assert {"UV 1_280", "UV 1_280 mol/m^3", "Cond"} <= set(names)
    uv = names["UV 1_280"]
    assert uv["unit"] == "AU/cm" and uv["primary"] and uv["columns"] == ["time_s", "AU/cm"]
    data = np.loadtxt(os.path.join(str(tmp_path), uv["file"]), delimiter=",")
    assert data.shape[1] == 2 and np.all(np.diff(data[:, 0]) > 0)
    assert data[0, 1] == pytest.approx(0.05 / 0.2)
    cond = np.loadtxt(os.path.join(str(tmp_path), names["Cond"]["file"]), delimiter=",")
    assert cond[0, 1] == pytest.approx(1.0)  # 10 mS/cm = 1 S/m
    assert side["process_inputs"]["placeholder"] and len(side["sections"]) == len(exp.sections)
    assert "origin" not in side["source"] and "locator" not in json.dumps(side)


def test_csv_system_volumes_flag(tmp_path):
    exp = _experiment(mixer_volume=(0.6, "ml"), tubing_volume=(50, "ul"))
    side = json.load(open(to_csv(exp, str(tmp_path / "with")), encoding="utf-8"))
    assert [u[0] for u in side["process_inputs"]["units"]] == ["buffer", "feed", "mixer", "column", "tubing",
                                                                "outlet"]
    side = json.load(open(to_csv(exp, str(tmp_path / "without"), system_volumes=False), encoding="utf-8"))
    assert [u[0] for u in side["process_inputs"]["units"]] == ["buffer", "feed", "column", "outlet"]


def test_csv_without_process_inputs_still_writes(tmp_path):
    exp = _experiment()
    side = json.load(open(to_csv(exp, str(tmp_path), components=["salt"], buffers={"A": [1]}), encoding="utf-8"))
    assert side["process_inputs"] is None
    assert any(w.startswith("process: no inputs") for w in side["warnings"])


# --- CADET-Process 0.12 (skipped when not installed) ----------------------------------------------

def test_references_nrmse_against_itself_is_zero():
    pytest.importorskip("CADETProcess")
    from CADETProcess.comparison import NRMSE
    from pycorn.exporters.cadet import to_references
    exp = _experiment(epsilon=1.4, molecular_weight=150000)
    refs, _ = to_references(exp)
    assert [r.name for r in refs][:2] == ["UV 1_280", "UV 1_280 mol/m^3"]
    uv = refs[0]
    assert uv.unit == "AU/cm" and uv.primary and uv.not_column_outlet == []
    assert uv.solution.shape == (len(exp.signals[0].time_s), 1)
    for ref in refs:
        assert float(np.max(np.abs(NRMSE(ref).evaluate(ref)))) == 0.0


def test_process_skeleton():
    pytest.importorskip("CADETProcess")
    from pycorn.exporters.cadet import to_process
    exp = _experiment()
    process, inputs = to_process(exp)
    assert process.cycle_time == pytest.approx(540.0)
    # no stored mixer and no tubing in the synthetic run: buffer and feed inlets only (D27)
    assert [u[0] for u in inputs["units"]] == ["buffer", "feed", "column", "outlet"]
    assert len(process.events) == 4 * len(inputs["sections"])
    assert exp.column["length_m"] is None  # no linked column (D10)
    assert any("left for the caller: length, diameter" in w for w in inputs["warnings"])
    feed = next(e for e in process.events if e.name == "s001_feed_c")
    assert feed.time == pytest.approx(60.0)
    timelines = process.parameter_timelines
    # (t, inlet, salt, sample, flow): valve feed enters through 'feed' while 'buffer' stops
    for t, inlet, salt, sample, q in ((30.0, "buffer", 0.0, 0.0, Q), (90.0, "feed", 0.0, 1.0, Q),
                                      (90.0, "buffer", 0.0, 0.0, 0.0), (300.0, "buffer", 50.0, 0.0, Q),
                                      (500.0, "buffer", 100.0, 0.0, Q), (500.0, "feed", 0.0, 0.0, 0.0)):
        if q:
            assert np.ravel(timelines["flow_sheet.%s.c" % inlet].value(t))[:2].tolist() == pytest.approx([salt, sample])
        assert np.ravel(timelines["flow_sheet.%s.flow_rate" % inlet].value(t))[0] == pytest.approx(q)

    # with a mixer and a tubing volume: CSTR outflow = inflow; the mixer holds during valve feed
    exp = _experiment(mixer_volume=(0.6, "ml"), tubing_volume=(50, "ul"))
    process, inputs = to_process(exp)
    assert [u[0] for u in inputs["units"]] == ["buffer", "feed", "mixer", "column", "tubing", "outlet"]
    assert process.flow_sheet["mixer"].init_liquid_volume == pytest.approx(0.6e-6)
    timelines = process.parameter_timelines
    for t, mixer_q, tubing_q in ((30.0, Q, Q), (90.0, 0.0, Q), (300.0, Q, Q)):
        assert np.ravel(timelines["flow_sheet.mixer.flow_rate"].value(t))[0] == pytest.approx(mixer_q)
        assert np.ravel(timelines["flow_sheet.tubing.flow_rate"].value(t))[0] == pytest.approx(tubing_q)
    with pytest.raises(ValueError, match="column_model"):
        to_process(exp, column_model="Tank")
    with pytest.raises(ValueError, match="method_start"):
        to_process(_experiment(time_zero="injection"))


def test_fractionation_reference(tmp_path):
    pytest.importorskip("CADETProcess")
    from pycorn.exporters.cadet import to_fractionation_reference
    exp = _experiment()
    ref, warnings = to_fractionation_reference(exp)
    assert ref is None and warnings
    exp.fractions = [SimpleNamespace(tube="T1", start_s=100.0, end_s=160.0, volume_m3=2e-6,
                                     mass_kg={"mAb": 1.5e-6}),
                     SimpleNamespace(tube="T2", start_s=None, end_s=None, volume_m3=None, mass_kg={"mAb": 1e-6})]
    ref, _ = to_fractionation_reference(exp)
    assert ref is None
    ref, warnings = to_fractionation_reference(exp, molecular_weights={"mAb": 150000})
    assert len(ref.fractions) == 1 and any("T2" in w for w in warnings)
    assert ref.fractions[0].mass[0] == pytest.approx(1.5e-6 / 150.0)  # mol


@pytest.mark.skipif(not LOCAL, reason="local test files not present")
@pytest.mark.parametrize("path", LOCAL[:3], ids=os.path.basename)
def test_local_runs_export(path, tmp_path):
    exp = build_experiment(open_run(path))
    side = json.load(open(to_csv(exp, str(tmp_path)), encoding="utf-8"))
    assert side["process_inputs"]["placeholder"]
    try:
        import CADETProcess  # noqa: F401
    except ImportError:
        return
    from CADETProcess.comparison import NRMSE
    from pycorn.exporters.cadet import to_cadet_process
    out = to_cadet_process(exp)
    assert out["process"].cycle_time == pytest.approx(exp.sections[-1].end_s)
    for ref in out["references"]:
        assert float(np.max(np.abs(NRMSE(ref).evaluate(ref)))) == 0.0
