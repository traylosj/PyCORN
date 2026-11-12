"""
CADET-Core run check (docs/ROADMAP.md C4): the placeholder input simulates on Core 5.1.2 and
6.0.0a5, the sample leaves the column (mass balance), and each Core rejects the other layout.

Skipped unless `cadet-cli` is given: set PYCORN_CADET_CLI_5 and/or PYCORN_CADET_CLI_6 to the `cadet-cli`
of a throwaway venv with `pip install CADET-Core==5.1.2` / `CADET-Core==6.0.0a5`. Needs h5py.
"""
import os
import subprocess

import numpy as np
import pytest

from pycorn import open_run
from pycorn.exporters.cadet import build_experiment, process_inputs
from pycorn.exporters.cadet.core_input import to_core_input, write_json

from test_cadet_core_input import EXPECTED_RUN, GEOMETRY, _exp

CLI = {"5": os.environ.get("PYCORN_CADET_CLI_5"), "6": os.environ.get("PYCORN_CADET_CLI_6")}
LAYOUTS = [("5", {}), ("6", {}), ("6", {"discretization": {"method": "FV"}})]


def _cli(version):
    path = CLI[version]
    if not path or not os.path.isfile(path):
        pytest.skip("set PYCORN_CADET_CLI_%s to a CADET-Core %s cadet-cli" % (version, version))
    return path


def _simulate(cli, core, tmp_path, name):
    src, out = str(tmp_path / (name + ".json")), str(tmp_path / (name + ".h5"))
    write_json(core, src)
    done = subprocess.run([cli, src, out], capture_output=True, text=True, timeout=600)
    return done, out


def _sample_balance(exp, core, cli, tmp_path):
    """Simulate `core`; the sample (placeholder component 1) that left the outlet over what was fed."""
    h5py = pytest.importorskip("h5py")
    done, out = _simulate(cli, core, tmp_path, "run")
    assert done.returncode == 0, done.stderr + done.stdout
    with h5py.File(out, "r") as f:
        t = f["output/solution/SOLUTION_TIMES"][()]
        c = f["output/solution/unit_002/SOLUTION_OUTLET"][()].reshape(len(t), -1)
    assert np.isfinite(c).all()
    rows = process_inputs(exp)["sections"]
    tf = np.linspace(rows[0]["start_s"], rows[-1]["end_s"], 200001)
    q_out, sample_in = np.zeros(len(tf)), np.zeros(len(tf))
    for r in rows:
        on = (tf >= r["start_s"]) & (tf < r["end_s"])
        dt = tf[on] - r["start_s"]
        q_out[on] = (r["flow_m3_s"][0] + r["flow_m3_s"][1] * dt  # the column's, and the flow past it (M2)
                     + r["bypass_flow_m3_s"][0] + r["bypass_flow_m3_s"][1] * dt)
        q_feed =r["feed_flow_m3_s"][0] + r["feed_flow_m3_s"][1] * dt
        sample_in[on] = q_feed * np.polyval(r["c_feed"][1][::-1], dt)
    # on a fine grid with the section flows: the UV output grid has (almost) no points in pauses
    step = tf[1] - tf[0]
    m_in = sample_in.sum() * step
    assert m_in > 0
    return (np.interp(tf, t, c[:, 1]) * q_out).sum() * step / m_in


@pytest.mark.parametrize("version,extra", LAYOUTS, ids=["v5", "v6-DG", "v6-FV"])
def test_placeholder_input_simulates_and_the_sample_comes_out(version, extra, tmp_path):
    cli = _cli(version)
    # mixer and tubing CSTRs (D27); the mixer holds (Q = 0) during the valve feed
    exp = _exp(mixer_volume=(0.6, "ml"), tubing_volume=(50, "ul"))
    core = to_core_input(exp, version, placeholders=True, **extra)
    assert core["meta"]["UNIT_NAMES"] == ["buffer", "column", "outlet", "feed", "mixer", "tubing"]
    assert _sample_balance(exp, core, cli, tmp_path) == pytest.approx(1.0, rel=0.01)


@pytest.mark.parametrize("version,extra", LAYOUTS, ids=["v5", "v6-DG", "v6-FV"])
def test_expected_run_simulates_and_the_sample_comes_out(version, extra, tmp_path):
    # 28 sections, pauses, the feed at section 13: with one switch per flow change (10) the Cores never
    # reached the feed switch and no sample entered the column (X20)
    cli = _cli(version)
    exp = build_experiment(open_run(EXPECTED_RUN))
    core = to_core_input(exp, version, placeholders=True, **extra)
    assert _sample_balance(exp, core, cli, tmp_path) == pytest.approx(1.0, rel=0.01)


@pytest.mark.parametrize("version,extra", LAYOUTS, ids=["v5", "v6-DG", "v6-FV"])
def test_new_buffer_reaches_the_outlet_past_a_bypassed_column(version, extra, tmp_path):
    # owner, 2026-11-08 (M2): the system flow goes through the mixer and around the column, so the
    # UV and conductivity see the new buffer during the by-pass (before: the mixer held, outlet 0)
    from test_cadet_process_export import _bypass_experiment
    h5py = pytest.importorskip("h5py")
    cli = _cli(version)
    exp = _bypass_experiment(mixer_volume=(0.6, "ml"), tubing_volume=(50, "ul"), **GEOMETRY)
    core = to_core_input(exp, version, placeholders=True, **extra)
    done, out = _simulate(cli, core, tmp_path, "bypass")
    assert done.returncode == 0, done.stderr + done.stdout
    with h5py.File(out, "r") as f:
        t = f["output/solution/SOLUTION_TIMES"][()]
        salt = f["output/solution/unit_002/SOLUTION_OUTLET"][()].reshape(len(t), -1)[:, 0]
    at = lambda s: float(np.interp(s, t, salt))  # noqa: E731
    assert at(55.0) < 1.0  # 0 %B until 1 min
    assert at(145.0) == pytest.approx(100.0, rel=0.02)  # 85 s of 2 ml/min through 0.65 ml of CSTRs


@pytest.mark.parametrize("version,other", [("5", "6"), ("6", "5")])
def test_each_core_rejects_the_other_layout(version, other, tmp_path):
    cli = _cli(version)
    core = to_core_input(_exp(), other, placeholders=True)
    done, _ = _simulate(cli, core, tmp_path, "other")
    assert done.returncode != 0


def _walkthrough():
    """examplescripts/cadet_walkthrough.py (the documented example), imported from its path."""
    import importlib.util
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "examplescripts", "cadet_walkthrough.py")
    spec = importlib.util.spec_from_file_location("cadet_walkthrough", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("version", ["5", "6"])
def test_documented_walkthrough_runs_end_to_end(version, tmp_path):
    # docs/CADET_GUIDE.md "Walkthrough": sample.zip with offline load and fraction assays (3 components),
    # exported, simulated, and set next to the measurements
    cli = _cli(version)
    pytest.importorskip("h5py")
    cw = _walkthrough()
    r = cw.walkthrough(cli, version, out_dir=str(tmp_path))
    assert r["balance"]["protein"] == pytest.approx(1.0, abs=0.001)
    assert r["balance"]["impurity"] == pytest.approx(1.0, abs=0.001)
    assert 0.5 < r["balance"]["salt"] < 1.0  # salt still fills the column and mixer at the end
    assert {(f["tube"], f["component"]) for f in r["fractions"]} == {("Frac", "protein"), ("Frac", "impurity")}
    for f in r["fractions"]:  # the example assays were chosen to match the measured UV
        assert f["simulated_mg"] == pytest.approx(f["assay_mg"], rel=0.05)
    with open(r["overlay"], encoding="utf-8") as f:
        header = f.readline().strip().split(",")
    assert header[:3] == ["time_s", "measured_uv_protein_equivalent_mol_m3", "simulated_protein_mol_m3"]
    assert np.isfinite(r["outlet"]).all() and r["outlet"].shape[1] == 3
    # the feed composition from the load assay: protein and impurity split by the impurity %
    feed = cw.load_concentrations(cw.LOAD, cw.MOLECULAR_WEIGHT_G_MOL)
    total = cw.LOAD["total_protein_g_per_l"]
    assert feed["impurity"] * cw.MOLECULAR_WEIGHT_G_MOL["impurity"] / 1000 == pytest.approx(total * 0.04)


def test_walkthrough_core_versions_agree(tmp_path):
    cli5, cli6 = _cli("5"), _cli("6")
    pytest.importorskip("h5py")
    cw = _walkthrough()
    exp = cw.build()
    t5, c5, _ = cw.simulate(exp, cli5, "5", str(tmp_path))
    t6, c6, _ = cw.simulate(exp, cli6, "6", str(tmp_path))
    peak = c5[:, 1].max()
    assert np.abs(np.interp(t5, t6, c6[:, 1]) - c5[:, 1]).max() < 0.01 * peak  # v5 FV vs v6 DG
