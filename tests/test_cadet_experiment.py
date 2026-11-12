"""
`CadetExperiment` and the signal conversions (pycorn/exporters/cadet, docs/ROADMAP.md C1).

Synthetic runs for the rules; the local test files (gitignored), when present, for real data.
"""
import glob
import os

import numpy as np
import pytest

from pycorn import open_run
from pycorn.exporters.cadet import build_experiment, process_inputs, read_fraction_masses, salt_calibration_points
from pycorn.exporters.cadet.calibration import salt_from_conductivity, uv_au_per_cm, uv_concentration
from pycorn.model import Event, EventCurve

from test_run_log_sections import _block, _curve, _e, _run

HERE = os.path.dirname(os.path.abspath(__file__))
LOCAL = sorted(glob.glob(os.path.join(HERE, "..", "unicorn_test_files", "U7.*", "*.zip")))
UV_INFO = {"IsUVValuesNormalizedToNominalUVPathLength": True, "UVPathLength": 2.01, "NominalUVPathLength": 2}


def _uv(name, value=100.0, uv_info=UV_INFO, unit="mAU"):
    c = _curve(name, lambda t: value, unit, data_type="UV")
    c.uv_info = dict(uv_info) if uv_info is not None else None
    return c


def _events(*extra):
    return [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
            _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"), _e(0.0, "Wavelength 280 {nm} 0 {nm} 260 {nm}")] + list(extra)


def _with_injection(run, time_min):
    chrom = run.chromatogram()
    ec = EventCurve(name="Injection", type="Injection", curve_number=3, time_unit="min", volume_unit="ml",
                    is_original=True, column_volume=None, column_volume_unit=None,
                    events=[Event(time=time_min, volume=time_min * 2, text=None, type=None, subtype=None,
                                  instruction_feedback=None, xml={})], xml={})
    chrom.event_curves[ec.name] = ec
    return run


def test_time_zero_is_checked():
    with pytest.raises(ValueError, match="time_zero"):
        build_experiment(_run(_events(_e(5.0, "End 1/1/2024", status=""))), time_zero="start")


def test_sections_in_si_and_loop_feed_cut():
    events = _events(_e(1.0, "Injection valve Inject"), _e(4.0, "Injection valve Manual load"),
                     _e(6.0, "End 1/1/2024", status=""))
    run = _run(events, curves=[_uv("UV 1_280")])
    exp = build_experiment(run, loop_volume=(1.0, "ml"))
    feed = [s for s in exp.sections if s.feed]
    assert len(feed) == 1
    assert (feed[0].start_s, feed[0].end_s, feed[0].feed_source) == (60.0, pytest.approx(90.0), "loop_volume")
    after = next(s for s in exp.sections if s.start_s == pytest.approx(90.0))
    assert not after.feed and after.injection_valve == "Inject"
    assert feed[0].flow_m3_s == (pytest.approx(2.0 / 6e7), 0.0)
    exp = build_experiment(run)
    assert [s.end_s - s.start_s for s in exp.sections if s.feed] == [pytest.approx(180.0)]
    assert any("no loop volume given" in w for w in exp.warnings)


def test_direct_inject_is_not_cut_by_the_loop_volume():
    events = _events(_e(1.0, "Sample flow 0.500 {ml/min} Off"), _e(1.0, "Injection valve Direct inject"),
                     _e(3.0, "Injection valve Manual load"), _e(4.0, "End 1/1/2024", status=""))
    exp = build_experiment(_run(events), loop_volume=(0.1, "ml"))
    feed = [s for s in exp.sections if s.feed]
    assert sum(s.end_s - s.start_s for s in feed) == pytest.approx(120.0)
    assert feed[0].flow_m3_s[0] == pytest.approx(0.5 / 6e7)


def _feed_spans(exp):
    return [(s.start_s, s.end_s) for s in exp.sections if s.feed]


def test_bypass_is_not_feed_but_uses_up_the_loop():
    # D18, D19: 2 ml/min; 1 min at by-pass (2 ml) then onto the column; a 3 ml loop ends 30 s later
    events = _events(_e(0.5, "Column position By-pass Down flow"), _e(1.0, "Injection valve Inject"),
                     _e(2.0, "Column position Position 1 Down flow"), _e(4.0, "Injection valve Manual load"),
                     _e(5.0, "End 1/1/2024", status=""))
    run = _with_injection(_run(events, curves=[_uv("UV 1_280"), _curve("Cond", lambda t: 10.0, "mS/cm",
                                                                         data_type="Conduction")]), 1.0)
    exp = build_experiment(run, loop_volume=(3.0, "ml"))
    assert _feed_spans(exp) == [(120.0, pytest.approx(150.0))]
    uses = [(round(s.start_s), s.sample_use) for s in exp.sections if s.injection_valve == "Inject"]
    assert uses == [(60, "to_bypass"), (120, "feed"), (150, "buffer")]
    # the whole by-pass window (from 30 s) is not column outlet, buffer part included
    assert exp.bypass == [{"start_s": 30.0, "end_s": 120.0, "sample": True}]
    assert exp.primary_uv.not_column_outlet == [(30.0, 120.0)]
    assert any(s.role == "conductivity" and s.not_column_outlet == [(30.0, 120.0)] for s in exp.signals)
    assert any("sample sent to the column by-pass" in w for w in exp.warnings)
    shifted = build_experiment(run, loop_volume=(3.0, "ml"), time_zero="injection")
    assert shifted.primary_uv.not_column_outlet == [(-30.0, 60.0)]
    assert shifted.bypass[0]["start_s"] == -30.0
    # a loop used up at by-pass leaves no feed
    assert _feed_spans(build_experiment(run, loop_volume=(1.0, "ml"))) == []


def test_buffer_through_the_bypass_is_not_column_outlet():
    events = _events(_e(1.0, "Column position By-pass Down flow"), _e(2.0, "Column position Position 1 Down flow"),
                     _e(3.0, "End 1/1/2024", status=""))
    exp = build_experiment(_run(events, curves=[_uv("UV 1_280")]))
    assert exp.bypass == [{"start_s": 60.0, "end_s": 120.0, "sample": False}]
    assert exp.primary_uv.not_column_outlet == [(60.0, 120.0)]
    assert any("1 column by-pass window(s) without sample (60.0-120.0 s)" in w for w in exp.warnings)


def _cond(t):
    # 5 mS/cm buffer A (outlet 5.05 after the first by-pass), 50 mS/cm at 50 %B by-pass, 25 at the outlet
    return 5.0 if t < 3 else 5.05 if t < 6 else 25.0 if t < 8 else 50.0


def test_buffer_readings_at_bypass_outlet_check_and_salt():
    events = _events(_e(0.0, "Inlet A A1"), _e(0.0, "Inlet B B1"), _e(1.0, "Column position By-pass Down flow"),
                     _e(3.0, "Column position Position 1 Down flow"), _e(6.0, "Gradient 50.0 {%B} 0.00 {min}"),
                     _e(8.0, "Column position By-pass Down flow"), _e(10.0, "Column position Position 1 Down flow"),
                     _e(10.25, "End 1/1/2024", status=""))
    curves = [_curve("Cond", _cond, "mS/cm", data_type="Conduction"), _curve("pH", lambda t: 7.0, "pH", data_type="pH")]
    exp = build_experiment(_run(events, curves=curves), buffer_salt={"A1": 20.0, "B": 1000.0})
    first, second = exp.buffer_readings
    assert (first["start_s"], first["end_s"], first["percent_b"]) == (60.0, 180.0, 0.0)
    assert (first["read_start_s"], first["read_end_s"]) == (pytest.approx(140.0), 180.0)
    assert first["conductivity_s_m"] == pytest.approx(0.5) and first["conductivity_settled"]
    assert first["ph"] == pytest.approx(7.0) and first["ph_settled"]
    assert first["outlet"]["conductivity_matches"] and first["outlet"]["ph_matches"]
    assert first["salt_mol_m3"] == 20.0 and second["salt_mol_m3"] == pytest.approx(510.0)
    assert second["outlet"]["start_s"] == 360.0 and second["outlet"]["conductivity_matches"] is False
    assert any("conductivity at the column outlet 360.0-480.0 s (25 mS/cm)" in w for w in exp.warnings)
    assert salt_calibration_points(exp.buffer_readings) == [(pytest.approx(5.0), 20.0), (pytest.approx(50.0), 510.0)]
    assert build_experiment(_run(events, curves=curves)).buffer_readings[0]["salt_mol_m3"] is None


@pytest.mark.parametrize("outlet_ph, matches", [(6.92, True), (6.85, False)])
def test_buffer_ph_outlet_match_tolerance(outlet_ph, matches):
    # owner (rev 7f): the outlet may read up to 0.1 pH off the by-pass buffer without a warning
    events = _events(_e(0.0, "Inlet A A1"), _e(1.0, "Column position By-pass Down flow"),
                     _e(3.0, "Column position Position 1 Down flow"), _e(6.0, "End 1/1/2024", status=""))
    curves = [_curve("Cond", lambda t: 5.0, "mS/cm", data_type="Conduction"),
              _curve("pH", lambda t: 7.0 if 0.99 < t < 3.01 else outlet_ph, "pH", data_type="pH")]
    exp = build_experiment(_run(events, curves=curves))
    assert exp.buffer_readings[0]["outlet"]["ph_matches"] is matches
    assert any(w.startswith("pH at the column outlet") for w in exp.warnings) is not matches


def test_direct_inject_from_the_buffer_inlet_only_is_not_feed():
    # D21 (a): a system test
    events = _events(_e(1.0, "System flow 0.000 {ml/min} Off"), _e(1.0, "Injection valve Direct inject"),
                     _e(1.0, "Sample inlet Buffer"), _e(1.0, "Sample flow 5.000 {ml/min} Off"),
                     _e(3.0, "Injection valve Manual load"), _e(4.0, "End 1/1/2024", status=""))
    for path in (None, (0.5, "ml")):
        exp = build_experiment(_run(events), sample_path_volume=path)
        assert _feed_spans(exp) == []
        assert any("buffer only" in w for w in exp.warnings)
        assert {s.sample_use for s in exp.sections if s.injection_valve == "Direct inject"} == {"buffer"}


def _chase_events():
    # S1 at 1 ml/min from 1 min, the sample inlet to Buffer at 2 min (Finalize sample injection)
    return _events(_e(1.0, "Sample inlet S1"), _e(1.0, "Sample flow 1.000 {ml/min} Off"),
                   _e(1.0, "Injection valve Direct inject"), _e(2.0, "Sample inlet Buffer"),
                   _e(3.0, "Injection valve Manual load"), _e(4.0, "End 1/1/2024", status=""))


def test_buffer_chase_ends_the_feed_at_the_switch_without_a_path_volume():
    exp = build_experiment(_run(_chase_events()))
    assert _feed_spans(exp) == [(60.0, 120.0)]
    assert any("feed ends at the switch" in w for w in exp.warnings)


def test_buffer_chase_pushes_the_path_volume_on_with_one():
    # D21 (b): 0.5 ml path at 1 ml/min: the feed runs 30 s past the switch
    exp = build_experiment(_run(_chase_events()), sample_path_volume=(500.0, "ul"))
    feed = [s for s in exp.sections if s.feed]
    assert [(s.start_s, s.end_s) for s in feed] == [(60.0, 120.0), (120.0, pytest.approx(150.0))]
    assert [s.feed_source for s in feed] == ["valve", "sample_path_volume"]
    assert all(s.sample_at_valve == "S1" for s in feed)
    after = next(s for s in exp.sections if s.start_s == pytest.approx(150.0))
    assert after.sample_use == "buffer" and after.sample_at_valve == "Buffer"
    assert any("content before the first sample pump flow is unknown" in w for w in exp.warnings)
    assert not any("feed ends at the switch" in w for w in exp.warnings)


def _washed_events():
    # the sample flow path washed with buffer (to waste) before a direct inject of S1
    return _events(_e(0.5, "Sample inlet Buffer"), _e(0.5, "Sample flow 1.000 {ml/min} Off"),
                   _e(0.5, "Injection valve Sample pump waste"), _e(1.0, "Sample inlet S1"),
                   _e(1.0, "Injection valve Direct inject"), _e(3.0, "Injection valve Manual load"),
                   _e(4.0, "End 1/1/2024", status=""))


def test_buffer_washed_path_delays_the_feed():
    exp = build_experiment(_run(_washed_events()), sample_path_volume=(0.5, "ml"))
    assert _feed_spans(exp) == [(pytest.approx(90.0), 180.0)]
    first = next(s for s in exp.sections if s.start_s == 60.0)
    assert first.sample_use == "buffer" and first.injection_valve == "Direct inject"
    exp = build_experiment(_run(_washed_events()))
    assert _feed_spans(exp) == [(60.0, 180.0)]
    assert any("held Buffer" in w for w in exp.warnings)


def test_uv_channels_wavelengths_primary_and_concentration():
    run = _run(_events(_e(5.0, "End 1/1/2024", status="")),
               curves=[_uv("UV 1_280"), _uv("UV 2_0", 3.0), _uv("UV 3_260", 50.0)])
    exp = build_experiment(run, epsilon=1.0, molecular_weight=50000)
    names = [s.name for s in exp.signals if s.role == "uv"]
    assert names == ["UV 1_280", "UV 3_260"]
    assert any("UV 2_0: channel off" in w for w in exp.warnings)
    uv = exp.primary_uv
    assert uv.name == "UV 1_280" and uv.wavelength_source == "run_log" and uv.export_unit == "AU/cm"
    assert uv.export_values[0] == pytest.approx(0.1 / 0.2)  # 100 mAU over the nominal 2 mm
    assert uv.concentration["kg/m^3"][0] == pytest.approx(0.5)
    assert uv.concentration["mol/m^3"][0] == pytest.approx(0.5 / 50.0)
    assert exp.signal("UV 3_260").concentration is None
    assert any(w.startswith("UV 1_280: concentration from UV") for w in exp.warnings)  # issue #1
    scaled = build_experiment(run, epsilon=1.0, molecular_weight=50000, response_factor=0.8)
    assert scaled.primary_uv.concentration["kg/m^3"][0] == pytest.approx(0.5 / 0.8)
    assert scaled.primary_uv.concentration["conversion"]["response_factor"] == 0.8


def test_uv_without_path_or_wavelength_instruction():
    events = [_e(0.0, "System flow 1.000 {ml/min} Off"), _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"),
              _e(2.0, "End 1/1/2024", status="")]
    exp = build_experiment(_run(events, curves=[_uv("UV 1_254", uv_info=None)]))
    uv = exp.primary_uv
    assert uv.export_unit == "AU" and uv.wavelength_nm == 254.0 and uv.wavelength_source == "curve_name"
    assert any("no UV path length" in w for w in exp.warnings)
    assert any("no 280 nm UV channel" in w for w in exp.warnings)
    assert any("from the curve name" in w for w in exp.warnings)


def _line_load_events(inlet_at=1.0):
    # DB r637 pattern: the block starts on the old line (A1), the inlet switches to A5, then the mark
    return [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
            _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"), _e(0.0, "Inlet A A1"),
            _block(0.98, "Block SampleLoad"), _e(inlet_at, "Inlet A A5"), _e(3.0, "Inlet A A1"),
            _e(5.0, "End 1/1/2024", status="")]


def test_feed_through_a_pump_line_from_a_mark_after_an_inlet_switch():
    run = _with_injection(_run(_line_load_events()), 1.0 + 0.1 / 60)
    exp = build_experiment(run)
    feed = [s for s in exp.sections if s.feed]
    assert feed and all(s.feed_line == "A" and s.feed_source == "injection_mark" for s in feed)
    # feed from the switch (not the block start, which is still on A1) until A1 comes back
    assert feed[0].start_s == pytest.approx(60.0) and feed[-1].end_s == pytest.approx(180.0)
    assert not any(s.feed for s in exp.sections if s.start_s < 60.0)
    assert any("inlet A switched to A5 at 60.0 s" in w for w in exp.warnings)
    rows = process_inputs(exp)["sections"]
    load = next(r for r in rows if r["feed"])
    assert load["feed_path"] == "line_A" and load["feed_flow_m3_s"] == (0.0, 0.0)
    assert load["buffer_flow_m3_s"][0] > 0 and load["c_buffer"][1, 0] == 1.0  # 'sample' on line A, through the mixer
    assert not process_inputs(exp)["valve_feed"]
    # caller names the sample inlet: same sections, labelled by source
    named = build_experiment(_run(_line_load_events()), sample_inlets=["A5"])
    assert [(s.start_s, s.end_s) for s in named.sections if s.feed] == [(s.start_s, s.end_s) for s in feed]
    assert {s.feed_source for s in named.sections if s.feed} == {"sample_inlets"}


def test_mark_without_an_inlet_switch_is_not_feed():
    # most such marks tested against a lab environment: the inlet in force since method start says nothing
    run = _with_injection(_run(_line_load_events(inlet_at=4.0)), 2.0)
    exp = build_experiment(run)
    assert not any(s.feed for s in exp.sections)
    assert any("no inlet switch within 60 s" in w for w in exp.warnings)


def test_system_volumes_mixer_stored_tubing_from_caller():
    import base64
    run = _run(_events(_e(5.0, "End 1/1/2024", status="")))
    mixer = base64.b64encode(b'<Items><Item Name="Volume" Value="1.4" Unit="ml" /></Items>').decode()
    run.metadata["SystemData"] = {"Systems": {"System": {"MixerInformation": mixer}}}
    run.metadata["SystemSettingData"] = {"SystemSettings": {"InstructionGroup": [{"Instructions": [
        {"Instruction": "Delay volume: Monitor to frac", "Parameters": {"Parameter": "Volume\t435 µl"}}]}]}}
    exp = build_experiment(run)
    assert exp.system["mixer_m3"] == pytest.approx(1.4e-6) and exp.system["mixer_source"] == "SystemData MixerInformation"
    assert exp.system["tubing_m3"] is None and any("column-to-UV tubing volume" in w for w in exp.warnings)
    assert exp.system["delay_volume_hints"] == ["Delay volume: Monitor to frac: Volume 435 ul"]
    exp = build_experiment(run, mixer_volume=(0.6, "ml"), tubing_volume=(50, "ul"))
    assert exp.system["mixer_m3"] == pytest.approx(0.6e-6) and exp.system["mixer_source"] == "caller"
    assert exp.system["tubing_m3"] == pytest.approx(5e-8)
    assert any("given 0.6 ml used over the stored 1.4 ml" in w for w in exp.warnings)


def test_uv_wavelengths_helper_is_the_d8_rule():
    from pycorn.exporters.cadet import uv_wavelengths
    from pycorn.run_log_sections import run_log_sections
    run = _run(_events(_e(5.0, "End 1/1/2024", status="")),
               curves=[_uv("UV 1_280"), _uv("UV 2_0", 3.0), _uv("UV 3_260", 50.0)])
    chrom = run.chromatogram()
    sections = run_log_sections(run, chrom)["sections"]
    rows, warnings = uv_wavelengths(chrom, sections)
    assert [(c.name, ch, nm, src) for c, ch, nm, src in rows] == [("UV 1_280", 1, 280.0, "run_log"),
                                                                  ("UV 3_260", 3, 260.0, "run_log")]
    assert any("UV 2_0: channel off" in w for w in warnings)
    rows, _ = uv_wavelengths(chrom, sections, curves=["UV 2_0"])  # named: kept even at 0 nm
    assert [(c.name, nm) for c, _, nm, _ in rows] == [("UV 2_0", 0.0)]


def test_uv_path_length_fallback_only_where_the_run_has_none():
    events = [_e(0.0, "System flow 1.000 {ml/min} Off"), _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"),
              _e(0.0, "Wavelength 280 {nm} 260 {nm} 254 {nm}"), _e(2.0, "End 1/1/2024", status="")]
    curves = [_uv("UV 1_280", 100.0, uv_info=None), _uv("UV 2_260", 100.0),
              _uv("UV 3_254", 500.0, uv_info=None, unit="mAU/cm")]
    label = "fallback (--path-length)"
    exp = build_experiment(_run(events, curves=curves), uv_path_length_fallback_cm=0.5,
                           uv_path_length_fallback_basis=label, epsilon=1.0)
    filled, stored, per_cm = (exp.signal(n) for n in ("UV 1_280", "UV 2_260", "UV 3_254"))
    assert (filled.uv_path_length_cm, filled.uv_path_basis, filled.export_unit) == (0.5, label, "AU/cm")
    assert filled.export_values[0] == pytest.approx(0.1 / 0.5)
    assert filled.concentration["conversion"]["path_length_basis"] == label
    assert filled.concentration["kg/m^3"][0] == pytest.approx(0.2)
    assert stored.uv_path_length_cm == pytest.approx(0.2) and stored.uv_path_basis != label  # the run's path
    assert per_cm.uv_path_basis == "per_cm_unit" and per_cm.export_values[0] == pytest.approx(0.5)
    used = [w for w in exp.warnings if label in w]
    assert len(used) == 1 and used[0].startswith("UV 1_280: no UV path length in the run")
    with pytest.raises(ValueError, match="above 0 cm"):
        build_experiment(_run(events, curves=curves), uv_path_length_fallback_cm=0.0)


def test_injection_time_zero_and_pause_shift():
    events = _events(_e(4.0, "End 1/1/2024", status=""))
    run = _with_injection(_run(events, curves=[_uv("UV 1_280")], pauses=[(30.0, 10.0)]), 2.0)
    exp = build_experiment(run)
    assert exp.time_zero["injection_offset_s"] == pytest.approx(130.0)  # 120 s elapsed + 10 s pause
    uv = exp.primary_uv
    k = int(np.argmin(np.abs(run.chromatogram().curve("UV 1_280").time - 1.0)))
    assert uv.time_s[k] == pytest.approx(70.0)  # 60 s elapsed + the 10 s pause before it
    shifted = build_experiment(run, time_zero="injection")
    assert shifted.sections[0].start_s == pytest.approx(-130.0)
    assert shifted.primary_uv.time_s[k] == pytest.approx(-60.0)
    assert exp.hints[0]["start_s"] == pytest.approx(0.0)
    assert shifted.hints[0]["start_s"] == pytest.approx(-130.0)  # one time origin in experiment.json
    with pytest.raises(ValueError, match="no injection"):
        build_experiment(_run(events), time_zero="injection")


def test_no_run_log_is_a_clear_error():
    # e.g. an imported chromatogram: its run log ('ImportedLogbook') gives no sections (D13)
    with pytest.raises(ValueError, match="no run-log sections"):
        build_experiment(_run([], curves=[_uv("UV 1_280")]))


def test_fraction_masses(tmp_path):
    path = tmp_path / "assay.csv"
    path.write_text("tube,component,mass_mg,conc_g_per_l\nT1,mAb,2.5,\nT2,mAb,,0.4\nT9,mAb,1,\n", encoding="utf-8")
    masses = read_fraction_masses(str(path))
    assert masses["T1"]["mAb"] == ("mass_kg", pytest.approx(2.5e-6))
    assert masses["T2"]["mAb"] == ("conc_kg_m3", 0.4)
    assert read_fraction_masses(path) == masses  # a pathlib.Path too
    with pytest.raises(ValueError, match="no mass_mg"):
        read_fraction_masses([{"tube": "T1", "component": "x"}])
    events = _events(_e(4.0, "End 1/1/2024", status=""))
    run = _run(events)
    chrom = run.chromatogram()
    chrom.event_curves["Fraction"] = EventCurve(
        name="Fraction", type="Fraction", curve_number=4, time_unit="min", volume_unit="ml", is_original=True,
        column_volume=None, column_volume_unit=None, xml={},
        events=[Event(time=t, volume=2 * t, text=n, type=None, subtype=None, instruction_feedback=None, xml={})
                for t, n in ((1.0, "T1"), (2.0, "T2"), (3.0, "Waste"))])
    exp = build_experiment(run, fraction_masses=str(path))
    by_tube = {f.tube: f for f in exp.fractions}
    assert by_tube["T1"].mass_kg == {"mAb": pytest.approx(2.5e-6)}
    assert by_tube["T2"].volume_m3 == pytest.approx(2e-6)
    assert by_tube["T2"].mass_kg == {"mAb": pytest.approx(0.4 * 2e-6)}
    assert any("tube 'T9'" in w for w in exp.warnings)


def test_calibration_helpers():
    per_cm = _uv("UV", 500.0, unit="mAU/cm")
    values, unit, path, basis, warning = uv_au_per_cm(per_cm)
    assert (unit, path, basis, warning) == ("AU/cm", 1.0, "per_cm_unit", None) and values[0] == pytest.approx(0.5)
    plain = _uv("UV 1_280", 100.0)
    assert uv_au_per_cm(plain, path_length_cm=0.5)[0][0] == pytest.approx(0.2)
    conc = uv_concentration(plain, epsilon=1.0, path_length_cm=0.5)
    assert conc["kg/m^3"][0] == pytest.approx(0.2) and conc["conversion"]["path_length_basis"] == "caller"
    assert salt_from_conductivity([5.0, 10.0, 99.0], [(0.0, 0.0), (10.0, 100.0)]).tolist() == [50.0, 100.0, 100.0]
    assert salt_from_conductivity(np.array([2.0]), lambda x: x * 3)[0] == 6.0
    with pytest.raises(ValueError):
        salt_from_conductivity([1.0], [(1.0, 2.0)])


@pytest.mark.skipif(not LOCAL, reason="local test files not present")
@pytest.mark.parametrize("path", LOCAL, ids=os.path.basename)
def test_local_runs_build(path):
    run = open_run(path)
    if (run.chromatogram().column_volume_info or {}).get("value") is None:
        pytest.skip("no stored Vc: a deliberate edge (a manual run), not a method run")
    exp = build_experiment(run, epsilon=1.4)
    assert exp.primary_uv is not None and exp.primary_uv.wavelength_nm == 280.0
    assert exp.primary_uv.export_unit == "AU/cm" and exp.primary_uv.concentration is not None
    assert any(s.feed for s in exp.sections)
    assert exp.time_zero["injection_offset_s"] > 0
    assert exp.column["vc_m3"] is not None
    for a, b in zip(exp.sections, exp.sections[1:]):
        assert b.start_s == pytest.approx(a.end_s)
    assert "origin" not in exp.source
