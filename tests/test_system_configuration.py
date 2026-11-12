"""
Inlet valves and pumps from the stored system configuration (pycorn/system_configuration.py)
and the inlet labels built on it (pycorn/run_log_sections.py; docs/ROADMAP.md, "Run
structure", inlets from the system configuration; RUN_STRUCTURE_REVIEW 3.10, Decide 22; D27 (4)).

Synthetic configurations in the shape a result stores them (`SystemData` EnabledComponents,
`StrategyData` ComponentStructure), one per name family, then the samples.
"""
import glob
import os
from collections import OrderedDict

import pytest

from pycorn import open_run
from pycorn.exporters.cadet import build_experiment, process_inputs
from pycorn.run_log_sections import (QUATERNARY_NOTE, inlet_state, inlets_source, inlets_text, run_log_sections,
                                     span_details)
from pycorn.system_configuration import ROLES, role_of, system_configuration

from test_cadet_experiment import _uv, _with_injection
from test_run_log_sections import _block, _e, _run

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLES = sorted(glob.glob(os.path.join(HERE, "..", "samples", "*.zip")))

# one family: a parent per inlet with the valve as a sub-component, 'none' entries for no valve
FAMILY_1 = OrderedDict([("Inlet A", True), ("Inlet A none", False), ("Inlet V9-IA", True), ("Inlet A V9-IAB", False),
                        ("Inlet B", False), ("Inlet B none", True), ("Inlet V9-IB", False), ("Inlet B V9-IAB", False),
                        ("Sample inlet valve", False), ("Sample inlet none", True), ("Sample pump", True),
                        ("Mixer", True), ("UV monitor", True)])
DISPLAY_1 = {"Inlet V9-IA": "V9-IA (7-ports)", "Inlet A V9-IAB": "V9-IAB part A (2-ports)",
             "Inlet V9-IB": "V9-IB (7-ports)", "Inlet B V9-IAB": "V9-IAB part B (2-ports)",
             "Sample inlet valve": "Sample inlet (V9-IS)", "Sample pump": "Sample pump S9", "Mixer": "Mixer (M9)"}
# another family: valves by their own names, the sample pump named by what it does, a quaternary valve
FAMILY_2 = OrderedDict([("Inlet valve A", True), ("Inlet A GUI", True), ("2nd Inlet valve A", False),
                        ("Inlet valve B", True), ("Inlet B GUI", True), ("Sample inlet valve", True),
                        ("Sample inlet GUI", True), ("Sample flow", True), ("Quatenary gradient", True),
                        ("Mixer", True)])
DISPLAY_2 = {"Inlet A GUI": "Inlet valve A (V9-IA)", "Inlet B GUI": "Inlet valve B (V9-IB)",
             "2nd Inlet valve A": "Inlet valve A 2nd (V9-A2)", "Sample flow": "Sample pump (P9-S)",
             "Sample inlet GUI": "Sample inlet valve (V9-IS)", "Quatenary gradient": "Quaternary valve (Q9)"}


def _metadata(enabled, display):
    """`run.metadata` entries as a result stores them (parsed XML)."""
    components = [OrderedDict([("Name", n), ("IsEnabled", "true" if on else "false")]) for n, on in enabled.items()]
    structure = {"VisibleComponents": {"Group": [{"@Name": "Valves and pumps", "Component": [
        OrderedDict([("@Name", n), ("@DisplayName", d)]) for n, d in display.items()]}]}}
    return OrderedDict([("SystemData", {"Systems": {"System": {"EnabledComponents": {"EnabledComponent": components}}}}),
                        ("StrategyData", {"StrategyInformation": {"ComponentStructure": structure}})])


def configured(run, enabled, display):
    """`run` with a stored system configuration; the chromatograms read it as at load."""
    run.metadata.update(_metadata(enabled, display))
    for chrom in run.chromatograms.values():
        chrom.system_configuration = system_configuration(run)
    return run


def _config(enabled, display):
    return system_configuration(configured(_run([]), enabled, display))


def _present(config):
    return {r: config.present(r) for r in ROLES}


# ---------------------------------------------------------------------------
# the component map
# ---------------------------------------------------------------------------

def test_first_family_by_names_and_none_entries():
    c = _config(FAMILY_1, DISPLAY_1)
    assert _present(c) == {"inlet_valve_a": True, "inlet_valve_b": False, "sample_inlet_valve": False,
                           "sample_pump": True, "quaternary_valve": None}
    assert c.role("inlet_valve_b").state == "none" and c.role("sample_inlet_valve").state == "none"
    assert c.role("inlet_valve_a").components == ["Inlet A", "Inlet V9-IA"]
    assert c.role("quaternary_valve").state == "not listed"  # absent from the list: not known, never a guess


def test_second_family_by_names_and_hardware_codes():
    c = _config(FAMILY_2, DISPLAY_2)
    assert set(_present(c).values()) == {True}
    assert c.role("sample_pump").components == ["Sample flow"]  # by its code P9-S, not its name
    # without the structure's display names the name alone doesn't say it is the sample pump
    assert _config(FAMILY_2, {}).role("sample_pump").state == "not listed"


def test_disabled_is_not_installed_and_differs_from_absent():
    enabled = OrderedDict((k, v) for k, v in FAMILY_1.items() if k != "Inlet B none")
    c = _config(enabled, DISPLAY_1)
    assert c.role("inlet_valve_b").state == "disabled" and c.present("inlet_valve_b") is False
    assert c.role("inlet_valve_b").components == ["Inlet B", "Inlet V9-IB", "Inlet B V9-IAB"]
    enabled = OrderedDict((k, v) for k, v in FAMILY_1.items() if "Inlet B" not in k and "V9-IB" not in k)
    assert _config(enabled, DISPLAY_1).role("inlet_valve_b").state == "not listed"


def test_unknown_names_are_not_known_and_a_code_is_enough():
    c = _config(OrderedDict([("Pump X", True), ("Valve Y", True)]), {})
    assert set(_present(c).values()) == {None} and {x.role for x in c.components} == {None}
    assert role_of("Valve Y") is None
    # a family not in the name table: its hardware code still gives the role
    c = _config(OrderedDict([("Buffer selector B", True)]), {"Buffer selector B": "Inlet valve B (V9-IB)"})
    assert c.present("inlet_valve_b") is True and c.present("inlet_valve_a") is None


def test_no_configuration_and_conflict():
    c = system_configuration(_run([]))
    assert not c.listed and {s.state for s in c.roles.values()} == {"no configuration"}
    both = OrderedDict([("Inlet B", True), ("Inlet V9-IB", True), ("Inlet B none", True)])
    c = _config(both, {})
    assert c.role("inlet_valve_b").state == "conflict" and c.present("inlet_valve_b") is None
    assert c.summary()["inlet_valve_b"]["present"] is None


# ---------------------------------------------------------------------------
# inlet states
# ---------------------------------------------------------------------------

def test_each_inlet_state():
    one, two = _config(FAMILY_1, DISPLAY_1), _config(FAMILY_2, DISPLAY_2)
    assert inlet_state("B", "B2", None)["label"] == "B2"  # logged always wins
    assert inlet_state("B", "B2", one)["state"] == "known"
    fixed = inlet_state("B", None, one)
    assert (fixed["label"], fixed["position"], fixed["state"]) == ("B", None, "known")
    assumed = inlet_state("B", None, two)
    assert (assumed["label"], assumed["position"], assumed["state"]) == ("B1 (assumed)", "B1", "assumed")
    assert inlet_state("sample", None, two)["label"] == "Buffer (assumed)"
    assert inlet_state("sample", None, one)["label"] == "Sample pump inlet"
    unknown = inlet_state("B", None, None)
    assert (unknown["label"], unknown["state"]) == ("B (not known)", "not known")
    assert inlet_state("sample", None, None)["label"] == "Sample inlet (not known)"


def _elution(*extra):
    """Line A at 0 %B, then 100 %B with no Inlet B logged, then a direct inject without a sample inlet."""
    return [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
            _e(0.0, "Inlet A A2"), _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"), _e(2.0, "Gradient 100.0 {%B} 0.00 {min}"),
            _e(4.0, "Gradient 0.0 {%B} 0.00 {min}"), _e(5.0, "Injection valve Direct inject"),
            _e(6.0, "Injection valve Manual load")] + list(extra) + [_e(8.0, "End 1/1/2024", status="")]


def _texts(run):
    r = run_log_sections(run)
    return r, {round(s.elapsed_start_s): (inlets_text(s), inlets_source(s)) for s in r["sections"]}


def test_no_b_valve_shows_the_line_alone():
    r, use = _texts(configured(_run(_elution()), FAMILY_1, DISPLAY_1))
    assert use[0] == ("A2", "known") and use[120] == ("B", "known")
    assert use[300] == ("A2, Sample pump inlet", "known")
    assert not [w for w in r["warnings"] if w.startswith("inlets:")]
    assert span_details(r, 0, 480)["inlets_source"] == "known"


def test_a_b_valve_with_nothing_logged_takes_its_first_position_assumed():
    r, use = _texts(configured(_run(_elution(_e(7.0, "Inlet B B3"))), FAMILY_2, DISPLAY_2))
    assert use[120] == ("B1 (assumed)", "assumed")
    assert use[300] == ("A2, Buffer (assumed)", "assumed")
    assert any(w.startswith("inlets: line B shown as 'B1 (assumed)' between 120.0 and 240.0 s") for w in r["warnings"])
    assert span_details(r, 0, 480)["inlets_source"] == "known -> assumed -> known -> assumed -> known"
    # an assumed Buffer still counts as feed (owner, 2026-11-11), and the warning says so
    sample = [w for w in r["warnings"] if w.startswith("inlets: sample inlet shown as 'Buffer (assumed)'")]
    assert len(sample) == 1 and sample[0].endswith("counted as feed there, though what was pumped may be buffer")
    assert not any("counted as feed" in w for w in r["warnings"] if w.startswith("inlets: line B"))


def test_configuration_not_known_says_so():
    r, use = _texts(_run(_elution()))
    assert use[120] == ("B (not known)", "not known") and use[300] == ("A2, Sample inlet (not known)", "not known")
    assert any("line B shown as 'B (not known)'" in w and "no configuration" in w for w in r["warnings"])


def test_quaternary_shares_listed_as_logged_with_a_provisional_note():
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
              _e(0.0, "Inlet A A1"), _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"),
              _e(1.0, "Quaternary gradient 25.0 {%} 25.0 {%} 50.0 {%} 0.0 {%} 0.00 {min}"),
              _e(2.0, "Quaternary gradient 0.0 {%} 100.0 {%} 0.0 {%} 0.0 {%} 1.00 {min}"),
              _e(4.0, "End 1/1/2024", status="")]
    r, use = _texts(configured(_run(events), FAMILY_2, DISPLAY_2))
    assert use[0] == ("A1", "known")
    assert use[60] == ("A1, Q1 25%+Q2 25%+Q3 50%", "known")
    assert use[120] == ("A1, Q ramp 25/25/50/0 to 0/100/0/0", "known") and use[180] == ("A1, Q2 100%", "known")
    assert "inlets: " + QUATERNARY_NOTE in r["warnings"]


# ---------------------------------------------------------------------------
# CADET: buffers= keys and the feed route
# ---------------------------------------------------------------------------

def _b_run(family, display, inlet_b=None):
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
              _e(0.0, "Inlet A A1"), _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"),
              _e(0.0, "Wavelength 280 {nm} 0 {nm} 0 {nm}")]
    events += [_e(2.0, "Inlet B %s" % inlet_b)] if inlet_b else []
    events += [_e(2.0, "Gradient 100.0 {%B} 0.00 {min}"), _e(4.0, "End 1/1/2024", status="")]
    run = _run(events, curves=[_uv("UV 1_280", 50.0)])
    return configured(run, family, display) if family else run


def test_cadet_buffer_keys_follow_the_inlet_states():
    # a B valve, no position logged: key the line, with a warning; its assumed position is no key
    exp = build_experiment(_b_run(FAMILY_2, DISPLAY_2))
    hint = next(h for h in exp.hints if h["start_s"] >= 120.0)
    assert hint["inlet_labels"]["B"] == "B1 (assumed)" and hint["inlet_sources"]["B"] == "assumed"
    assert exp.system["inlet_configuration"]["inlet_valve_b"]["present"] is True
    inputs = process_inputs(exp, components=["salt"], buffers={"A1": [20], "B": [500]})
    assert any(w.startswith("buffers['B'] used for line B from 120.0 s") and "'B1 (assumed)'" in w
               for w in inputs["warnings"])
    with pytest.raises(ValueError, match=r"B1 is never logged.*key it 'B'"):
        process_inputs(exp, components=["salt"], buffers={"A1": [20], "B1": [500]})
    # no B valve: 'B' is known, no warning
    exp = build_experiment(_b_run(FAMILY_1, DISPLAY_1))
    inputs = process_inputs(exp, components=["salt"], buffers={"A1": [20], "B": [500]})
    assert not any("used for line B" in w for w in inputs["warnings"])
    # logged: the position as logged, still keyed either way (backward compatible)
    exp = build_experiment(_b_run(FAMILY_2, DISPLAY_2, inlet_b="B2"))
    for key in ("B2", "B"):
        inputs = process_inputs(exp, components=["salt"], buffers={"A1": [20], key: [500]})
        assert not any("used for line B" in w for w in inputs["warnings"])
    # configuration not known: the line key, with a warning
    exp = build_experiment(_b_run(None, None))
    inputs = process_inputs(exp, components=["salt"], buffers={"A1": [20], "B": [500]})
    assert any("'B (not known)'" in w for w in inputs["warnings"])


def _line_load(family, display):
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
              _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"), _e(0.0, "Inlet A A1"),
              _block(0.98, "Block SampleLoad"), _e(1.0, "Inlet A A5"), _e(3.0, "Inlet A A1"),
              _e(5.0, "End 1/1/2024", status="")]
    return _with_injection(configured(_run(events), family, display), 1.0 + 0.1 / 60)


def test_no_sample_pump_only_qualifies_the_feed_warning():
    no_pump = OrderedDict((k, v) for k, v in FAMILY_1.items())
    no_pump["Sample pump"] = False
    exp = build_experiment(_line_load(no_pump, DISPLAY_1))
    with_pump = build_experiment(_line_load(FAMILY_1, DISPLAY_1))
    # the run-log rule decides the feed window; the configuration changes no section
    spans = [(s.start_s, s.end_s, s.feed_line) for s in exp.sections if s.feed]
    assert spans and spans == [(s.start_s, s.end_s, s.feed_line) for s in with_pump.sections if s.feed]
    assert any("no sample pump" in w and "(inferred)" in w for w in exp.warnings)
    assert not any("no sample pump" in w for w in with_pump.warnings)


# ---------------------------------------------------------------------------
# the samples
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("path", SAMPLES, ids=os.path.basename)
def test_samples_read_their_configuration(path):
    run = open_run(path)
    config = run.chromatogram().system_configuration
    assert config is not None and config.listed
    assert {r: config.present(r) for r in ROLES} == {r: run.system_configuration().present(r) for r in ROLES}
    # every sample has a valve on line A and a sample pump; line B either has a valve or is one line
    assert config.present("inlet_valve_a") is True and config.present("sample_pump") is True
    assert config.present("inlet_valve_b") in (True, False)
    r = run_log_sections(run)
    labels = {s.inlet_states["B"]["label"] for s in r["sections"] if not s.inlets.get("B")}
    assert labels <= ({"B1 (assumed)"} if config.present("inlet_valve_b") else {"B"})
    texts = " ".join(inlets_text(s) or "" for s in r["sections"])
    if not config.present("inlet_valve_b"):
        assert "B1" not in texts  # no B1 exists on a system with no B valve
    phases = run.chromatogram().phase_table()
    sources = {part for value in phases["inlets_source"].dropna() for part in value.split(" -> ")}
    assert sources <= {"known", "assumed"}  # the configuration is read: nothing 'not known'
