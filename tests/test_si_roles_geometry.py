"""
CADET Phase C0: SI conversion (`units.to_si`), the flow roles and column geometry.

Synthetic cases cover what no real data has (L/h, other units, several columns, caller values);
real-data checks use samples/sample.zip and, when present, the local test files.
"""
import glob
import math
import os
from collections import OrderedDict

import numpy as np
import pytest

from pycorn import open_folder, open_run
from pycorn.column_geometry import column_geometry, column_types, used_columns
from pycorn.curve_roles import curves_with_role
from pycorn.model import Chromatogram, Curve, Run
from pycorn.units import SI_UNITS, UNITS, UnitError, conversion_factor, quantity, si_unit, to_si

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLE = os.path.join(HERE, "..", "samples", "sample.zip")
TEST_FILES = os.path.join(HERE, "..", "unicorn_test_files")


def _curve(name, unit, data_type="Other"):
    t = np.arange(0, 5.0, 0.5)
    return Curve(name=name, data_type=data_type, unit=unit, precision=3, curve_number=1, time=t,
                 volume=t.astype(np.float32), values=np.ones(len(t), dtype=np.float32), time_unit="min",
                 volume_unit="ml", isochrone="Time", is_original=True, column_volume=None, column_volume_unit=None,
                 uv_info=None, point_sets=[], xml={})


def _chrom(curves, vc=None):
    chrom = Chromatogram(name="Chrom.1", chromatogram_id=1, info={"VolumeUnit": "ml", "TimeUnit": "min"},
                         curves=OrderedDict((c.name, c) for c in curves), event_curves=OrderedDict(), peak_tables=[],
                         pool_tables=None)
    if vc is not None:
        chrom.column_volume_info = {"value": vc[0], "unit": vc[1], "source": "override"}
    return chrom


def _column_type(name="Col A", bed="2.5", bed_unit="cm", diameter="1.6", diameter_unit="cm", dp="85", dp_unit="um"):
    return OrderedDict([("Name", name), ("BedHeight", bed), ("BedHeightUnit", bed_unit), ("TotalLiquidVolume", "4.27"),
                        ("ColumnVolumeUnitName", "ml"),
                        ("Hardware", OrderedDict([("Name", "HW"), ("Diameter", diameter), ("DiameterUnit", diameter_unit)])),
                        ("Media", OrderedDict([("Name", "Resin"), ("AverageParticleDiameter", dp),
                                               ("AverageParticleDiameterUnit", dp_unit)]))])


def _run(columns=(("Col A", 5.027, "ml"),), types=None, vc=(5.027, "ml")):
    info = [{"type": "ColumnInformation",
             "value": {"usedColumns": {"column": [OrderedDict([("name", n), ("volume", v), ("volumeUnit", u)])
                                                  for n, v, u in columns]}}}] if columns is not None else []
    types = [_column_type()] if types is None else types
    metadata = OrderedDict([("ColumnTypeData", {"ColumnTypes": {"ColumnType": types[0] if len(types) == 1 else types}}
                             if types else {"ColumnTypes": None})])
    chrom = _chrom([_curve("System flow", "ml/min")], vc=vc)
    return Run(source="zip", locator="synthetic", result={"Name": "run", "run_information": info},
               chromatograms=OrderedDict([(chrom.name, chrom)]), metadata=metadata, evaluation_log=[], manifest=[],
               entries=OrderedDict())


# ---------------------------------------------------------------------------
# SI units
# ---------------------------------------------------------------------------

def test_to_si_values():
    cases = [((5.0, "ml/min"), (5e-6 / 60, "m^3/s")), ((1.0, "L/h"), (1e-3 / 3600, "m^3/s")),
             ((5.027, "ml"), (5.027e-6, "m^3")), ((2.0, "L"), (2e-3, "m^3")), ((1.5, "min"), (90.0, "s")),
             ((2.5, "cm"), (0.025, "m")), ((85.0, "um"), (85e-6, "m")), ((2.0, "mm"), (2e-3, "m")),
             ((300.0, "cm/h"), (3.0 / 3600, "m/s")), ((60.0, "CV/h"), (1 / 60, "1/s")),
             ((0.3, "MPa"), (3e5, "Pa")), ((5.0, "bar"), (5e5, "Pa")), ((100.0, "mS/cm"), (10.0, "S/m")),
             ((1.0, "mg/ml"), (1.0, "kg/m^3")), ((150.0, "mM"), (150.0, "mol/m^3")), ((1.0, "M"), (1000.0, "mol/m^3")),
             ((150000.0, "g/mol"), (150.0, "kg/mol")), ((150.0, "kDa"), (150.0, "kg/mol"))]
    for (value, unit), (expected, target) in cases:
        si, si_u, factor = to_si(value, unit)
        assert si_u == target and si == pytest.approx(expected, rel=1e-12), unit
        assert factor == pytest.approx(expected / value, rel=1e-12)


def test_to_si_arrays_and_none():
    values, unit, _ = to_si(np.array([1.0, 2.0], dtype=np.float32), "ml/min")
    assert unit == "m^3/s" and values.dtype == np.float64
    assert np.allclose(values, [1e-6 / 60, 2e-6 / 60])
    assert to_si(None, "ml")[0] is None


def test_si_units_are_known_and_round_trip():
    for q, unit in SI_UNITS.items():
        assert quantity(unit) == q and si_unit(unit) == unit
        assert conversion_factor(unit, unit) == 1.0


def test_to_si_refuses_what_has_no_si_unit():
    for unit in ("%", "mAU", "AU/cm", "Ratio", "°C", "ml*mAU", "1/(M*cm)"):
        with pytest.raises(UnitError, match="no SI unit"):
            si_unit(unit)
    with pytest.raises(UnitError, match="unknown unit"):
        to_si(1.0, "furlong")
    with pytest.raises(UnitError, match="unknown unit"):
        si_unit(None)


def test_new_spellings_do_not_shadow_existing_ones():
    assert quantity("MPa") == "pressure" and quantity("M") == "molar_concentration"
    assert quantity("mS/cm") == "conductivity" and quantity("m") == "length"
    assert all(isinstance(f, float) for _, f in UNITS.values())


# ---------------------------------------------------------------------------
# flow roles
# ---------------------------------------------------------------------------

def test_flow_roles_by_unit_and_name():
    curves = [_curve("System flow", "ml/min"), _curve("System flow (CV/h)", "CV/h"),
              _curve("System linear flow", "cm/h"), _curve("Sample flow", "ml/min"),
              _curve("Sample flow (CV/h)", "CV/h"), _curve("Sample linear flow", "cm/h"),
              _curve("Flow", "L/h", data_type="Flow"), _curve("Conc B", "%")]
    chrom = _chrom(curves)
    assert [c.name for c in curves_with_role(chrom, "flow")] == ["System flow", "Flow"]
    assert [c.name for c in curves_with_role(chrom, "sample_flow")] == ["Sample flow"]


def test_flow_role_ignores_unknown_units():
    chrom = _chrom([_curve("System flow", "gal/min")])
    assert curves_with_role(chrom, "flow") == []


# ---------------------------------------------------------------------------
# column geometry (synthetic)
# ---------------------------------------------------------------------------

def test_geometry_from_column_type():
    g = column_geometry(_run())
    assert g.name == "Col A" and not g.warnings
    assert g.length_m == pytest.approx(0.025) and g.diameter_m == pytest.approx(0.016)
    assert g.particle_radius_m == pytest.approx(42.5e-6)
    assert g.cross_section_area_m2 == pytest.approx(math.pi * 0.008 ** 2)
    assert g.volume_m3 == pytest.approx(math.pi * 0.008 ** 2 * 0.025)
    assert g.sources == {"length": "ColumnTypeData", "diameter": "ColumnTypeData",
                         "particle_diameter": "ColumnTypeData"}
    assert g.stored["length"] == ("2.5", "cm")
    assert g.hints["total_liquid_volume"] == ("4.27", "ml") and g.hints["media"] == "Resin"
    assert abs(g.vc_check["relative_difference"]) < 1e-3 and g.vc_check["run_vc_source"] == "override"


def test_geometry_in_other_units():
    g = column_geometry(_run(types=[_column_type(bed="25", bed_unit="mm", diameter="0.016", diameter_unit="m")]))
    assert g.length_m == pytest.approx(0.025) and g.diameter_m == pytest.approx(0.016)
    g = column_geometry(_run(types=[_column_type(bed="2.5", bed_unit="inch")]))
    assert g.length_m is None and g.volume_m3 is None and g.diameter_m == pytest.approx(0.016)
    assert any("column length not used" in w and "inch" in w for w in g.warnings)


def test_geometry_vc_disagreement_warns():
    g = column_geometry(_run(vc=(4.0, "ml")))
    assert g.vc_check["relative_difference"] == pytest.approx(5.0265 / 4.0 - 1, rel=1e-3)
    assert any("but the run's Vc is 4 ml" in w for w in g.warnings)
    g = column_geometry(_run(vc=(0.005027, "L")))
    assert not g.warnings
    assert column_geometry(_run(vc=None)).vc_check is None


def test_no_column_type_chosen():
    g = column_geometry(_run(columns=(("Any", 0.35, "ml"),), types=[]))
    assert g.name is None and g.volume_m3 is None and g.sources == {}
    assert "no column type was chosen" in g.warnings[0]
    g = column_geometry(_run(columns=None, types=[]))
    assert "no ColumnInformation" in g.warnings[0]


def test_named_column_missing_from_column_types():
    g = column_geometry(_run(columns=(("Col B", 1.0, "ml"),)))
    assert g.name == "Col B" and g.volume_m3 is None
    assert any("'Col B' is not in the run's ColumnTypeData" in w for w in g.warnings)


def test_several_columns_need_a_name():
    run = _run(columns=(("Col A", 5.0, "ml"), ("Col B", 1.0, "ml")),
               types=[_column_type(), _column_type(name="Col B", bed="10")])
    g = column_geometry(run)
    assert g.name is None and g.volume_m3 is None and "names 2 columns" in g.warnings[0]
    g = column_geometry(run, name="Col B")
    assert g.length_m == pytest.approx(0.10)
    assert [t["Name"] for t in column_types(run)] == ["Col A", "Col B"]
    assert [n for n, _, _ in used_columns(run)] == ["Col A", "Col B"]


def test_caller_values_override():
    g = column_geometry(_run(), bed_height=(20, "mm"), particle_diameter=(50, "um"), override=True)
    assert g.length_m == pytest.approx(0.020) and g.particle_radius_m == pytest.approx(25e-6)
    assert g.sources["length"] == "caller" and g.sources["diameter"] == "ColumnTypeData"
    assert any(w.startswith("column bed height: given 20 mm used over the stored") for w in g.warnings)
    with pytest.raises(ValueError):
        column_geometry(_run(), bed_height=(0, "cm"))


def test_caller_values_are_a_fallback_per_value_by_default():
    # D26: stored values win and the unused caller value is always noted, even when equal
    g = column_geometry(_run(), bed_height=(20, "mm"), diameter=(1.6, "cm"))
    assert g.length_m == pytest.approx(0.025) and g.sources["length"] == "ColumnTypeData"
    assert sum("not used (fallback" in w for w in g.warnings) == 2
    # no linked column: every given value fills in, labelled
    labels = {"length": "fallback (--bed-height)"}
    g = column_geometry(_run(columns=(("Any", 0.35, "ml"),), types=[]),
                        bed_height=(2.5, "cm"), diameter=(1.6, "cm"), fallback_sources=labels)
    assert g.volume_m3 == pytest.approx(5.0265e-6, rel=1e-4) and g.particle_radius_m is None
    assert g.sources == {"length": "fallback (--bed-height)", "diameter": "fallback (diameter=)"}
    assert "column bed height: none stored; given 2.5 cm used (fallback (--bed-height))" in g.warnings
    assert "no column particle diameter" in g.warnings
    # a linked record missing one value: only that one is filled
    g = column_geometry(_run(types=[_column_type(dp="0")]), particle_diameter=(34, "um"))
    assert g.particle_radius_m == pytest.approx(17e-6) and g.sources["particle_diameter"] == "fallback (particle_diameter=)"
    assert g.sources["length"] == "ColumnTypeData"


def test_no_vc_check_when_vc_came_from_the_given_geometry():
    run = _run(columns=(("Any", None, None),), types=[])
    run.chromatogram().column_volume_info = {"value": 5.0265, "unit": "ml", "source": "geometry"}
    g = column_geometry(run, bed_height=(2.5, "cm"), diameter=(1.6, "cm"))
    assert g.vc_check is None and any("computed from the given geometry" in w for w in g.warnings)


def test_unusable_stored_values():
    g = column_geometry(_run(types=[_column_type(bed="", dp="0")]))
    assert g.length_m is None and g.particle_radius_m is None and g.diameter_m == pytest.approx(0.016)
    assert "no column length" in g.warnings and "no column particle diameter" in g.warnings


# ---------------------------------------------------------------------------
# real data
# ---------------------------------------------------------------------------

def test_sample_has_no_column_type():
    run = open_run(SAMPLE)
    assert column_types(run) == []
    assert column_geometry(run).volume_m3 is None
    chrom = run.chromatogram()
    for role in ("flow", "sample_flow"):
        assert all(quantity(c.unit) == "flow" for c in curves_with_role(chrom, role))


def _local_runs():
    runs = [open_run(p) for p in sorted(glob.glob(os.path.join(TEST_FILES, "U7.*", "*.zip")))]
    for path in sorted(glob.glob(os.path.join(TEST_FILES, "UFOL", "**", "*.UFol"), recursive=True)):
        runs += open_folder(path)
    return runs


def test_local_runs_geometry_and_flow():
    runs = _local_runs()
    if not runs:
        pytest.skip("no local test files in %s" % TEST_FILES)
    with_geometry = 0
    for run in runs:
        chrom = run.chromatogram()
        assert len(curves_with_role(chrom, "flow")) == 1
        assert len(curves_with_role(chrom, "sample_flow")) <= 1
        g = column_geometry(run)
        named = [n for n, _, _ in used_columns(run) if n != "Any"]
        if g.volume_m3 is None:
            assert not named or any("not in the run's ColumnTypeData" in w for w in g.warnings)
            continue
        with_geometry += 1
        assert set(g.sources.values()) == {"ColumnTypeData"}
        if g.vc_check is not None:
            assert abs(g.vc_check["relative_difference"]) <= 0.01
    assert with_geometry > 0
