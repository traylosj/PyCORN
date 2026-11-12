"""
`pycorn.method_hints` (docs/ROADMAP.md, "Method hints", D17/D20).

Synthetic methods for the rules; the local test files (gitignored), when present, for real data.
"""
import glob
import os
from collections import OrderedDict

import pytest

from pycorn import open_run
from pycorn.exporters.cadet import build_experiment
from pycorn.method_hints import method_hints

from test_cadet_experiment import _with_injection
from test_run_log_sections import _block, _e, _run

HERE = os.path.dirname(os.path.abspath(__file__))
LOCAL = sorted(glob.glob(os.path.join(HERE, "..", "unicorn_test_files", "U7.*", "*.zip")))


def _ins(name, bp="0", var=None, **params):
    ins = OrderedDict([("ReadableName", name), ("BreakPointValue", bp)])
    if var:
        ins["BreakPointVariable"] = var
    ins["StrategyInstruction"] = {"StrategyParameter": [{"ParameterName": k.replace("_", " "),
                                                         "ParameterReadableValue": v} for k, v in params.items()]}
    return ins


def _base(kind, volume=None):
    params = [{"GenericCommandParameterName": "Base", "ParameterReadableValue": kind}]
    if volume is not None:
        params.append({"GenericCommandParameterName": "Volume", "ParameterReadableValue": str(volume)})
    return OrderedDict([("ReadableName", "Base"), ("BreakPointValue", "0"),
                        ("GenericCommandInstruction", {"GenericCommandParameter": params})])


def _blk(name, instructions, base="SameAsMain", main=False, phase=None, dialog=None):
    b = OrderedDict([("BlockName", name), ("IsMainBlock", "true" if main else "false"), ("IsUnused", "false")])
    if phase:
        b["MethodPhase"] = {"PhaseName": phase, "ConfigurationProperties": dialog or "<x/>"}
    b["BlockInstructions"] = {"BlockInstruction": [_base(base)] + list(instructions)}
    return b


def _method(blocks, **extra):
    m = OrderedDict([("Description", "m"), ("NormalVolumeUnit", "ml"), ("TechniqueName", "Affinity"),
                     ("Variables", {"Variable": [{"VariableName": "SampleVol", "IsScoutingVariable": "false"}]}),
                     ("Blocks", {"Block": blocks})])
    m.update(extra)
    return {"Method": m}


LOAD = _blk("Load", [_ins("Injection valve", Position="Inject"),
                     _ins("Injection valve", "5", "SampleVol", Position="Manual load"), _ins("End_Block", "5")],
            base="Volume")
WASH = _blk("Wash", [_ins("End_Block", "10", "WashVol")])
MAIN = _blk("Main", [_ins("Block", Block="Load"), _ins("Block", Block="Wash")], base="Volume", main=True)


def _load_run(sample_vol="2.00", wash=None, method=None, extra_events=()):
    """Flow 2 ml/min; block Load 1-2 min with the valve at Inject from 1.01 min; optional Wash."""
    events = [_e(0.0, "Method Run 1/1/2024 10:00:00 AM -05:00", subtype="BlockStart", status=""),
              _e(0.0, "System flow 2.000 {ml/min} Off"), _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"),
              _block(1.0, "Block Load"), _e(1.01, "Injection valve Inject"),
              _e(2.0, "Injection valve Manual load"), _block(2.0, "End_Block", start=False)]
    if wash:
        events += [_block(2.0, "Block Wash"), _block(wash, "End_Block", start=False)]
    events += list(extra_events) + [_e(8.0, "End 1/1/2024", status="")]
    run = _run(sorted(events, key=lambda e: e.time))
    run.metadata["MethodData"] = method or _method([LOAD, WASH, MAIN])
    run.result["search_criteria"] = [{"Name": "VariableValue", "Keyword1": "SampleVol", "Keyword2": sample_vol},
                                     {"Name": "VariableValue", "Keyword1": "WashVol", "Keyword2": "4"}]
    return run


def test_no_method():
    h = method_hints(_run([_e(0.0, "End 1/1/2024", status="")]))
    assert not h.available and h.warnings == ["no MethodData: no method hints"]


def test_run_variable_moves_the_shared_breakpoint_and_gap_is_allowed():
    h = method_hints(_load_run("2.00"))
    inj, = h.injections
    # the method stores 5 ml; the run used 2 ml, which moves the End_Block beside the valve instruction too
    assert (inj["planned"], inj["variable"], inj["planned_end_at"]) == (2.0, "SampleVol", "valve")
    assert inj["executed"] == pytest.approx(2.0 * 0.99)  # valve turned 0.6 s after the block start
    # measured gap: 0.6 s at 2 ml/min before the switch; allowed short: gap + 2 % of the plan
    assert (inj["pre_switch_s"], inj["pre_switch"]) == (pytest.approx(0.6), pytest.approx(0.02))
    assert inj["gap_allowance"] == pytest.approx(0.02 + 0.04)
    assert inj["status"] == "as_planned" and inj["start_s"] == pytest.approx(60.6)
    load = next(b for b in h.blocks if b["block"] == "Load")
    assert (load["planned"], load["executed"], load["status"]) == (2.0, pytest.approx(2.0), "as_planned")
    assert h.variables[0]["variable"] == "SampleVol" and h.variables[0]["source"] == "run"
    assert not h.warnings


@pytest.mark.parametrize("sample_vol, status", [("3.00", "short"), ("1.50", "over")])
def test_injection_short_or_over_warns(sample_vol, status):
    h = method_hints(_load_run(sample_vol))
    assert h.injections[0]["status"] == status
    assert len(h.warnings) == 1 and "the feed is the logged valve period (D20)" in h.warnings[0]


def test_planned_zero_pulse_is_checked():
    load = _blk("Load", [_ins("Injection valve", "1", Position="Inject"), _ins("Injection valve", "1", Position="Manual load"),
                         _ins("End_Block", "1")], base="Volume")
    events = [_e(0.0, "Method Run 1/1/2024 10:00:00 AM -05:00", subtype="BlockStart", status=""),
              _e(0.0, "System flow 2.000 {ml/min} Off"), _block(1.0, "Block Load"), _e(1.5, "Injection valve Inject"),
              _e(1.51, "Injection valve Manual load"), _block(1.6, "End_Block", start=False), _e(3.0, "End 1/1/2024", status="")]
    run = _run(events)
    run.metadata["MethodData"] = _method([load, MAIN])
    inj, = method_hints(run).injections
    assert (inj["planned"], inj["executed"], inj["status"]) == (0.0, pytest.approx(0.02), "over")


def test_injection_planned_to_end_block_when_the_valve_stays():
    load = _blk("Load", [_ins("Injection valve", Position="Inject"), _ins("End_Block", "2", "SampleVol")], base="Volume")
    h = method_hints(_load_run("2.00", method=_method([load, MAIN])))
    inj, = h.injections
    assert (inj["planned"], inj["planned_end_at"], inj["status"]) == (2.0, "End_Block", "as_planned")


def test_block_rows_causes_and_blocks_not_run():
    other = _blk("Strip", [_ins("End_Block", "3")])
    h = method_hints(_load_run("2.00", wash=3.0, method=_method([LOAD, WASH, other, MAIN]),
                               extra_events=[_e(3.0, "Next breakpoint", type_="Manual")]))
    wash = next(b for b in h.blocks if b["block"] == "Wash")
    # WashVol 4 (run) in the main base (Volume): 1 min at 2 ml/min ran
    assert (wash["planned"], wash["executed"], wash["status"], wash["cause"]) == (4.0, pytest.approx(2.0), "shorter",
                                                                                   "next_breakpoint")
    assert h.blocks_not_run == ["Strip"]
    assert not h.warnings  # block rows are report only


def test_stale_dialog_volume_warns():
    phase = _blk("SAMPLE APPLICATION", [_ins("Block", Block="Load")], phase="Sample Application",
                 dialog="<a><mLoopType>Superloop, 10 ml</mLoopType><mSampleVolumeForSuperloop>5</mSampleVolumeForSuperloop>"
                        "<mLoopLoading>true</mLoopLoading></a>")
    h = method_hints(_load_run("2.00", method=_method([LOAD, phase, MAIN])))
    d, = h.sample_application
    assert (d["loop_type"], d["sample_volume"], d["loop_loading"], d["planned_injection"]) == ("Superloop, 10 ml", 5.0,
                                                                                               True, 2.0)
    assert any("dialog sample volume 5 ml differs" in w for w in h.warnings)


def test_bufferpro_fields_by_name():
    recipe = {"RecipeName": "Phosphate pH 7", "pH": "7.0", "SaltConcentration": "0.5",
              "StockSolutions": {"StockSolution": [{"Inlet": "Q1", "Concentration": "1.0", "Substance": "NaH2PO4"},
                                                   {"Inlet": "Q4", "Concentration": "2.0", "Substance": "NaCl"}]}}
    h = method_hints(_load_run("2.00", method=_method([LOAD, MAIN], IsBufferPrepEnabled="true",
                                                       BufferFinalConcentration="0.02", BufferProRecipe=recipe)))
    bp = h.buffer_prep
    assert (bp["recipe"], bp["ph"], bp["salt_concentration"], bp["final_concentration"]) == ("Phosphate pH 7", 7.0, 0.5,
                                                                                             0.02)
    assert [(s["inlet"], s["concentration_M"], s["substance"]) for s in bp["stock_solutions"]] == [
        ("Q1", 1.0, "NaH2PO4"), ("Q4", 2.0, "NaCl")]
    assert any("BufferPro is on" in w for w in h.warnings)
    assert method_hints(_load_run("2.00")).buffer_prep is None


def test_build_experiment_carries_hints_and_shifts_them():
    run = _with_injection(_load_run("3.00"), 1.01)
    exp = build_experiment(run, time_zero="injection")
    inj, = exp.method_hints.injections
    assert inj["start_s"] == pytest.approx(0.0) and inj["status"] == "short"
    assert [w for w in exp.warnings if w.startswith("method: injection in block 'Load'")]
    assert all(s.feed_source != "method" for s in exp.sections)  # the feed stays the valve period (D20)


@pytest.mark.skipif(not LOCAL, reason="local test files not present")
def test_local_superloop_injection_as_planned():
    for path in LOCAL:
        h = method_hints(open_run(path))
        if not h.available:  # a manual run has no method: a deliberate edge
            continue
        injections = [r for r in h.injections if r["position"] == "Inject"]
        if not injections:
            continue
        for r in injections:
            assert r["status"] == "as_planned", (path, r)
            assert r["planned"] - r["gap_allowance"] <= r["executed"] <= r["planned"]
        for d in h.sample_application:
            assert d["planned_injection"] == d["sample_volume"], path
