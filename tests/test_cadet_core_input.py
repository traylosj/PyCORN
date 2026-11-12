"""
CADET-Core input, v5 and v6 layouts (pycorn/exporters/cadet/core_input.py, docs/ROADMAP.md C3,
D9). No CADET import; the HDF5 test is skipped without h5py. Simulating the files (C4) is
tests/test_cadet_core_run.py.

The expected files in tests/data were checked by hand against the run log of
samples/u711_cadet_reference.zip (owner, C3: committed). To rewrite them after an intended
change, set PYCORN_REGENERATE_EXPECTED=1 and check the diff.
"""
import glob
import json
import os

import numpy as np
import pytest

from pycorn import open_run
from pycorn.exporters.cadet import build_experiment, process_inputs
from pycorn.exporters.cadet.core_input import PLACEHOLDERS, to_core_input, write_hdf5, write_json

from test_cadet_process_export import _experiment

HERE = os.path.dirname(os.path.abspath(__file__))
TEST_FILES = os.path.join(HERE, "..", "unicorn_test_files")
LOCAL = sorted(glob.glob(os.path.join(TEST_FILES, "U7.*", "*.zip")))
EXPECTED = os.path.join(HERE, "data", "core_input_reference_v%s.json")
EXPECTED_RUN = os.path.join(HERE, "..", "samples", "u711_cadet_reference.zip")
GEOMETRY = dict(bed_height=(2.5, "cm"), diameter=(1.6, "cm"), particle_diameter=(85, "um"))
GRM = dict(bed_porosity=0.35, particle_porosity=0.8, axial_dispersion=1e-7, film_diffusion=[1e-5, 2e-5],
           pore_diffusion=1e-11)


def _exp(**kwargs):
    return _experiment(**dict(GEOMETRY, **kwargs))


def test_core_version_is_required_and_checked():
    exp = _exp()
    with pytest.raises(TypeError):
        to_core_input(exp, placeholders=True)
    for bad in (None, "7", 6.0):
        with pytest.raises(ValueError, match="'5' or '6'"):
            to_core_input(exp, bad, placeholders=True)
    assert to_core_input(exp, 6, placeholders=True)["meta"]["TARGET_VERSION"] == "6.0.0a5"


def test_layouts_differ_only_in_the_column():
    exp = _exp()
    v5 = to_core_input(exp, "5", placeholders=True)
    v6 = to_core_input(exp, "6", placeholders=True)
    m5, m6 = v5["input"]["model"], v6["input"]["model"]
    for key in ("unit_000", "unit_002", "connections", "solver"):
        assert m5[key] == m6[key]
    assert v5["input"]["solver"] == v6["input"]["solver"]
    assert v5["input"]["return"] == v6["input"]["return"]
    c5, c6 = m5["unit_001"], m6["unit_001"]
    for key in ("CROSS_SECTION_AREA", "COL_DISPERSION", "COL_POROSITY", "NCOMP", "INIT_C"):
        assert c5[key] == c6[key]
    assert c5["COL_LENGTH"] == c6["BED_LENGTH"] == pytest.approx(0.025)

    # v5 (X16): everything on the unit, FV, no GEOMETRY or particle_type_*
    assert c5["UNIT_TYPE"] == "GENERAL_RATE_MODEL"
    assert "GEOMETRY" not in c5 and not any(k.startswith("particle_type") for k in c5)
    assert c5["PAR_DIFFUSION"] == [PLACEHOLDERS["pore_diffusion"]] * 2
    assert c5["FILM_DIFFUSION"] == [PLACEHOLDERS["film_diffusion"]] * 2
    assert c5["PAR_RADIUS"] == pytest.approx(42.5e-6) and c5["ADSORPTION_MODEL"] == "NONE"
    assert c5["discretization"]["NCOL"] == 100 and c5["discretization"]["NPAR"] == 5
    assert c5["discretization"]["weno"]["WENO_ORDER"] == 3

    # v6 (X1, X2, X17): GEOMETRY, NPARTYPE 1, particle_type_000; DG by default (owner, C3)
    assert c6["UNIT_TYPE"] == "COLUMN_MODEL_1D" and c6["GEOMETRY"] == "AXIAL_FLOW_CYLINDER"
    assert c6["NPARTYPE"] == 1 and "PAR_DIFFUSION" not in c6 and "PAR_RADIUS" not in c6
    pt = c6["particle_type_000"]
    assert (pt["HAS_FILM_DIFFUSION"], pt["HAS_PORE_DIFFUSION"], pt["HAS_SURFACE_DIFFUSION"]) == (1, 1, 0)
    assert pt["PORE_DIFFUSION"] == [PLACEHOLDERS["pore_diffusion"]] * 2
    assert pt["PAR_POROSITY"] == PLACEHOLDERS["particle_porosity"]
    assert pt["NBOUND"] == [0, 0] and "adsorption" not in pt and "INIT_CS" not in pt
    assert c6["discretization"] == {"SPATIAL_METHOD": "DG", "NELEM": 16, "POLYDEG": 4, "USE_ANALYTIC_JACOBIAN": 1}
    assert pt["discretization"] == {"SPATIAL_METHOD": "DG", "PAR_NELEM": 1, "PAR_POLYDEG": 4}


def test_v6_fv_and_legacy_name():
    core = to_core_input(_exp(), "6", placeholders=True, discretization={"method": "FV", "ncol": 50},
                         legacy_unit_type=True)
    c = core["input"]["model"]["unit_001"]
    assert c["UNIT_TYPE"] == "GENERAL_RATE_MODEL" and c["GEOMETRY"] == "AXIAL_FLOW_CYLINDER"
    assert c["discretization"]["SPATIAL_METHOD"] == "FV" and c["discretization"]["NCOL"] == 50
    assert c["particle_type_000"]["discretization"]["NCELLS"] == 5
    with pytest.raises(ValueError, match="FV only"):
        to_core_input(_exp(), "5", placeholders=True, discretization={"method": "DG"})


def test_sections_inlet_and_switches_follow_process_inputs():
    exp = _exp()
    core = to_core_input(exp, "5", placeholders=True)
    rows = process_inputs(exp)["sections"]
    m, s = core["input"]["model"], core["input"]["solver"]
    assert s["sections"]["NSEC"] == len(rows)
    assert s["sections"]["SECTION_TIMES"] == [rows[0]["start_s"]] + [r["end_s"] for r in rows]
    assert len(s["sections"]["SECTION_CONTINUITY"]) == len(rows) - 1
    names = core["meta"]["UNIT_NAMES"]
    assert names == ["buffer", "column", "outlet", "feed"]  # synthetic run: no stored mixer, no tubing
    feed = names.index("feed")
    for k, r in enumerate(rows):
        for unit, key in ((0, "c_buffer"), (feed, "c_feed")):
            sec = m["unit_%03d" % unit]["sec_%03d" % k]
            assert np.allclose(np.column_stack([sec["CONST_COEFF"], sec["LIN_COEFF"], sec["QUAD_COEFF"],
                                                sec["CUBE_COEFF"]]), r[key])
    # one switch per section and every boundary discontinuous (C4, X20: the Cores pick a switch from
    # section % NSWITCHES and skip continuous boundaries)
    switches = [v for k, v in m["connections"].items() if k.startswith("switch_")]
    assert m["connections"]["NSWITCHES"] == len(switches) == len(rows)
    assert [w["SECTION"] for w in switches] == list(range(len(rows)))
    assert s["sections"]["SECTION_CONTINUITY"] == [0] * (len(rows) - 1)
    for k, r in enumerate(rows):
        table = np.asarray(switches[k]["CONNECTIONS"]).reshape(-1, 10)
        assert table[:, 2:6].tolist() == [[-1, -1, -1, -1]] * len(table)
        flows = {(int(a), int(b)): (q0, q1) for a, b, _, _, _, _, q0, q1, _, _ in table}
        assert flows[(0, 1)] == pytest.approx(tuple(r["buffer_flow_m3_s"]))
        assert flows[(feed, 1)] == pytest.approx(tuple(r["feed_flow_m3_s"]))
        assert flows[(1, 2)] == pytest.approx(tuple(r["flow_m3_s"]))
        if r["feed_path"] == "valve":  # the buffer path holds, the feed inlet feeds the column (D27)
            assert flows[(0, 1)] == (0.0, 0.0) and flows[(feed, 1)][0] > 0
    t = s["USER_SOLUTION_TIMES"]
    assert t[0] >= rows[0]["start_s"] and t[-1] <= rows[-1]["end_s"] and np.all(np.diff(t) > 0)


def test_placeholders_listed_and_caller_values_win():
    core = to_core_input(_exp(porosities={"column": 0.4}), "6", placeholders=True, overrides={"input": {"model": {
        "unit_001": {"particle_type_000": {"FILM_DIFFUSION": [3e-6, 3e-6]}}}}})
    meta = core["meta"]
    ph = meta["placeholders"]
    assert "BED_POROSITY" not in ph and "FILM_DIFFUSION" not in ph
    assert ph["PARTICLE_POROSITY"] == PLACEHOLDERS["particle_porosity"] and ph["ADSORPTION_MODEL"] == "NONE"
    assert "PARTICLE_RADIUS" not in ph  # from the linked column
    c = core["input"]["model"]["unit_001"]
    assert c["COL_POROSITY"] == 0.4 and c["particle_type_000"]["FILM_DIFFUSION"] == [3e-6, 3e-6]
    assert meta["PLACEHOLDER_COMPONENTS"] == 1 and meta["COMPONENTS"] == ["salt", "sample"]
    assert any("placeholder parameters" in w for w in meta["WARNINGS"])


def test_overrides_merged_unchanged_and_placeholders_dropped():
    exp = _exp()
    plain = to_core_input(exp, "6", placeholders=True)
    assert "OVERRIDES" not in plain["meta"]
    sma = {"IS_KINETIC": 1, "SMA_LAMBDA": 1200.0, "SMA_KA": [0.0, 35.5], "SMA_KD": [0.0, 1000.0],
           "SMA_NU": [0.0, 4.7], "SMA_SIGMA": [0.0, 11.83]}
    over = {"input": {"model": {"unit_001": {
        "COL_POROSITY": 0.33, "NEW_FIELD_PYCORN_DOESNT_KNOW": [1, 2],
        "particle_type_000": {"ADSORPTION_MODEL": "STERIC_MASS_ACTION", "NBOUND": [1, 1], "adsorption": sma}}},
        "solver": {"time_integrator": {"RELTOL": 1e-5}}}}
    core = to_core_input(exp, "6", placeholders=True, overrides=over)
    c = core["input"]["model"]["unit_001"]
    assert c["COL_POROSITY"] == 0.33 and c["NEW_FIELD_PYCORN_DOESNT_KNOW"] == [1, 2]
    pt = c["particle_type_000"]
    assert pt["ADSORPTION_MODEL"] == "STERIC_MASS_ACTION" and pt["adsorption"] == sma
    assert pt["PAR_POROSITY"] == PLACEHOLDERS["particle_porosity"]  # untouched siblings stay
    assert core["input"]["solver"]["time_integrator"]["RELTOL"] == 1e-5
    assert core["input"]["solver"]["time_integrator"]["ABSTOL"] == plain["input"]["solver"]["time_integrator"]["ABSTOL"]
    ph = core["meta"]["placeholders"]
    assert "BED_POROSITY" not in ph and "ADSORPTION_MODEL" not in ph and "PARTICLE_POROSITY" in ph
    assert "input/model/unit_001/COL_POROSITY" in core["meta"]["OVERRIDES"]
    warnings = core["meta"]["WARNINGS"]
    assert not any(w.startswith("placeholder binding") for w in warnings)
    assert any(w.startswith("placeholder parameters") and "bed_porosity" not in w for w in warnings)
    # new fields are named (a misspelt name looks the same), replaced ones are not
    added = next(w for w in warnings if w.startswith("overrides added field(s)"))
    assert "unit_001/NEW_FIELD_PYCORN_DOESNT_KNOW" in added and "COL_POROSITY" not in added
    # where each placeholder sits, in CADET-Core's names: what to override
    assert plain["meta"]["PLACEHOLDER_PATHS"]["BED_POROSITY"] == ["input/model/unit_001/COL_POROSITY"]
    assert any("bed_porosity=0.37 (COL_POROSITY)" in w for w in plain["meta"]["WARNINGS"])
    # the rest of the file is the same as without overrides
    assert core["input"]["model"]["connections"] == plain["input"]["model"]["connections"]
    assert type(plain["input"]["model"]["unit_001"]["COL_POROSITY"]) is float  # markers stripped


def test_overrides_v5_and_every_placeholder_replaced():
    exp = _exp()
    over = {"input": {"model": {"unit_001": {"COL_POROSITY": 0.3, "PAR_POROSITY": 0.6, "PAR_RADIUS": 4e-5,
                                             "COL_DISPERSION": 1e-7, "FILM_DIFFUSION": [1e-5, 1e-5],
                                             "PAR_DIFFUSION": [1e-11, 1e-11], "ADSORPTION_MODEL": "LINEAR"}}}}
    core = to_core_input(exp, "5", placeholders=True, overrides=over)
    ph = core["meta"].get("placeholders")
    assert ph is None or set(ph) - {"SOURCE"} <= {"PARTICLE_RADIUS"}
    assert not any(w.startswith(("placeholder parameters", "placeholder binding")) for w in core["meta"]["WARNINGS"])
    with pytest.raises(ValueError, match="'input'"):
        to_core_input(exp, "5", placeholders=True, overrides={"meta": {}})
    with pytest.raises(TypeError):
        to_core_input(exp, "5", placeholders=True, overrides=[("input", {})])


def test_missing_values_raise_and_name_them():
    with pytest.raises(ValueError, match="length, cross_section_area.*D10"):
        to_core_input(_experiment(), "5", placeholders=True)  # geometry never gets a placeholder
    with pytest.raises(ValueError, match="bed_porosity, particle_porosity, axial_dispersion, film_diffusion, "
                                         "pore_diffusion.*overrides="):
        to_core_input(_exp(), "6")
    with pytest.raises(ValueError, match="time_zero='method_start'"):
        to_core_input(_exp(time_zero="injection"), "5", placeholders=True)
    # D28: the pycorn-named model arguments are gone; the error points to overrides
    for removed in ({"parameters": GRM}, {"binding": {"model": "NONE"}}):
        with pytest.raises(TypeError, match="removed.*overrides="):
            to_core_input(_exp(), "6", placeholders=True, **removed)
    with pytest.raises(TypeError, match="unexpected"):
        to_core_input(_exp(), "6", placeholders=True, porosity=0.3)
    # caller geometry instead of a linked column (build_experiment, D26)
    core = to_core_input(_experiment(bed_height=(10, "cm"), diameter=(1, "cm")), "5", placeholders=True)
    assert core["input"]["model"]["unit_001"]["CROSS_SECTION_AREA"] == pytest.approx(np.pi * 0.01 ** 2 / 4)


def test_lumped_models():
    v5 = to_core_input(_exp(), "5", column_model="LumpedRateModelWithPores", placeholders=True)
    v6 = to_core_input(_exp(), "6", column_model="LumpedRateModelWithPores", placeholders=True)
    c5, c6 = v5["input"]["model"]["unit_001"], v6["input"]["model"]["unit_001"]
    assert c5["UNIT_TYPE"] == "LUMPED_RATE_MODEL_WITH_PORES" and "NPAR" not in c5["discretization"]
    assert "PAR_DIFFUSION" not in c5 and c5["NBOUND"] == [0, 0] and "adsorption" not in c5
    pt = c6["particle_type_000"]
    assert (pt["HAS_FILM_DIFFUSION"], pt["HAS_PORE_DIFFUSION"]) == (1, 0) and "discretization" not in pt
    assert "PORE_DIFFUSION" not in pt and "PORE_DIFFUSION" not in v6["meta"]["placeholders"]

    lrm = to_core_input(_exp(), "6", column_model="LumpedRateModelWithoutPores", placeholders=True)
    c = lrm["input"]["model"]["unit_001"]
    assert c["TOTAL_POROSITY"] == pytest.approx(0.37 + 0.63 * 0.75) and "COL_POROSITY" not in c
    assert c["particle_type_000"]["HAS_FILM_DIFFUSION"] == 0 and "PAR_RADIUS" not in c["particle_type_000"]
    lrm5 = to_core_input(_exp(), "5", column_model="LumpedRateModelWithoutPores", placeholders=True)
    assert lrm5["input"]["model"]["unit_001"]["UNIT_TYPE"] == "LUMPED_RATE_MODEL_WITHOUT_PORES"


def test_write_json_and_hdf5(tmp_path):
    core = to_core_input(_exp(), "6", placeholders=True)
    path = write_json(core, str(tmp_path / "in.json"))
    with open(path) as f:
        back = json.load(f)
    assert back["input"]["model"]["unit_001"]["GEOMETRY"] == "AXIAL_FLOW_CYLINDER"
    assert back["input"]["solver"]["sections"]["NSEC"] == core["input"]["solver"]["sections"]["NSEC"]
    h5py = pytest.importorskip("h5py")
    path = write_hdf5(core, str(tmp_path / "in.h5"))
    with h5py.File(path, "r") as f:
        assert f["input/model/unit_001/GEOMETRY"][()] == b"AXIAL_FLOW_CYLINDER"
        assert f["input/model/unit_001/particle_type_000/NBOUND"][()].tolist() == [0, 0]
        assert f["input/solver/sections/NSEC"][()] == core["input"]["solver"]["sections"]["NSEC"]


# --- Local runs (skipped when not present) --------------------------------------------------------

def _normalized(core):
    """JSON form for the expected files: no run names or PyCORN version; the solution times as
    count, first and last (checked against the UV grid in the test)."""
    data = json.loads(json.dumps(core))
    for key in ("RUN", "CHROMATOGRAM", "PYCORN_VERSION"):
        data["meta"].pop(key, None)
    t = data["input"]["solver"].pop("USER_SOLUTION_TIMES")
    data["input"]["solver"]["USER_SOLUTION_TIMES_SUMMARY"] = [len(t), t[0], t[-1]]
    return data


@pytest.mark.parametrize("version", ["5", "6"])
def test_reference_run_matches_hand_checked_file(version):
    exp = build_experiment(open_run(EXPECTED_RUN))
    core = to_core_input(exp, version, placeholders=True)
    t = np.asarray(core["input"]["solver"]["USER_SOLUTION_TIMES"])
    uv = exp.primary_uv.time_s
    assert np.array_equal(t, uv[(uv >= 0) & (uv <= exp.sections[-1].end_s)])
    got = _normalized(core)
    path = EXPECTED % version
    if os.getenv("PYCORN_REGENERATE_EXPECTED"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(got, f, indent=1)
            f.write("\n")
    with open(path, encoding="utf-8") as f:
        assert got == json.load(f)


@pytest.mark.skipif(not LOCAL, reason="local test files not present")
@pytest.mark.parametrize("path", LOCAL, ids=os.path.basename)
def test_local_runs_both_layouts(path, tmp_path):
    run = open_run(path)
    exp = build_experiment(run)
    if not exp.column.get("length_m"):  # no linked column: caller geometry (D26)
        exp = build_experiment(run, bed_height=(10, "cm"), diameter=(0.77, "cm"))
    shared = []
    for version in ("5", "6"):
        core = to_core_input(exp, version, placeholders=True)
        write_json(core, str(tmp_path / ("v%s.json" % version)))
        m = core["input"]["model"]
        shared.append((m["unit_000"], m["connections"], core["input"]["solver"]))
    assert shared[0] == shared[1]
