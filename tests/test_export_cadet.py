"""pycorn.export_cadet() (docs/ROADMAP.md, C5, D28): the CLI's CADET output from Python."""
import copy
import json
import os

import pytest

import pycorn
from pycorn import open_run
from pycorn import run_export as rx

SAMPLE = os.path.abspath("../samples/sample.zip")
GEOMETRY = dict(bed_height=(10.0, "cm"), diameter=(0.3, "cm"))


@pytest.fixture(scope="module")
def run():
    return open_run(SAMPLE)


def _core(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def test_writes_what_the_cli_writes(run, tmp_path):
    out = pycorn.export_cadet(run, str(tmp_path / "py"), core_version="6", **GEOMETRY)
    assert out["chromatograms"] == ["Chrom.1"]
    assert sorted(os.path.basename(p) for p in out["written"]) == ["Chrom.1_cadet", "Chrom.1_core_v6.json"]
    assert any(w.startswith("cadet_core: ") for w in out["warnings"])
    rx.write_run(run, tmp_path / "cli", [], options=rx.ExportOptions(cadet=True, cadet_core="6", **GEOMETRY))
    py, cli = _core(tmp_path / "py" / "Chrom.1_core_v6.json"), _core(tmp_path / "cli" / "Chrom.1_core_v6.json")
    # the provenance says how each was asked for (and when); everything else is the same
    made = {k: json.loads(f["meta"].pop("EXPORT")) for k, f in (("py", py), ("cli", cli))}
    assert made["py"]["route"] == "Python: pycorn.export_cadet" and made["cli"]["route"].startswith("Python: ")
    assert made["py"]["options"]["bed_height"] == [10.0, "cm"] and made["py"]["version"] == made["cli"]["version"]
    assert py == cli
    assert sorted(os.listdir(tmp_path / "py" / "Chrom.1_cadet")) == sorted(os.listdir(tmp_path / "cli" / "Chrom.1_cadet"))


def test_core_overrides_reach_the_file(run, tmp_path):
    over = {"input": {"model": {"unit_001": {"COL_POROSITY": 0.31}}}}
    out = pycorn.export_cadet(run, str(tmp_path), core_version="5", cadet_process=False, core_overrides=over,
                              **GEOMETRY)
    assert [os.path.basename(p) for p in out["written"]] == ["Chrom.1_core_v5.json"]
    core = _core(out["written"][0])
    assert core["input"]["model"]["unit_001"]["COL_POROSITY"] == 0.31
    assert "BED_POROSITY" not in core["meta"]["placeholders"]
    assert core["meta"]["OVERRIDES"] == ["input/model/unit_001/COL_POROSITY"]
    assert any("caller overrides" in w for w in out["warnings"])


def test_process_overrides_travel_in_experiment_json(run, tmp_path):
    # owner, 2026-11-08: the same generic block for CADET-Process, carried unchanged
    block = {"flow_sheet": {"column": {"bed_porosity": 0.31}}}
    out = pycorn.export_cadet(run, str(tmp_path), process_overrides=block)
    side = _core(os.path.join(out["written"][0], "experiment.json"))
    assert side["process_overrides"] == block
    with pytest.raises(ValueError, match="cadet_process=True"):
        pycorn.export_cadet(run, str(tmp_path / "x"), core_version="6", cadet_process=False,
                            process_overrides=block, **GEOMETRY)
    with pytest.raises(TypeError, match="must be a dict"):
        pycorn.export_cadet(run, str(tmp_path / "y"), process_overrides="bed_porosity=0.31")


def test_mass_epsilon_with_molecular_weight_gives_mol_per_m3(run, tmp_path):
    # D29: CADET works in mol/m^3; a mass coefficient needs the molecular weight for it
    eps = {280.0: (1.4, "ml/(mg*cm)")}
    out = pycorn.export_cadet(run, str(tmp_path / "mw"), epsilon=eps, molecular_weight=(66000.0, "g/mol"))
    assert "UV_1_280_mol_m_3.csv" in os.listdir(tmp_path / "mw" / "Chrom.1_cadet")
    assert not any("molecular weight" in w for w in out["warnings"])
    out = pycorn.export_cadet(run, str(tmp_path / "none"), epsilon=eps)
    assert "UV_1_280_mol_m_3.csv" not in os.listdir(tmp_path / "none" / "Chrom.1_cadet")
    assert any(w.startswith("cadet: ") and "molecular weight" in w for w in out["warnings"])


def test_existing_files_need_overwrite(run, tmp_path):
    pycorn.export_cadet(run, str(tmp_path), core_version="6", cadet_process=False, **GEOMETRY)
    with pytest.raises(FileExistsError):
        pycorn.export_cadet(run, str(tmp_path), core_version="6", cadet_process=False, **GEOMETRY)
    pycorn.export_cadet(run, str(tmp_path), core_version="6", cadet_process=False, overwrite=True, **GEOMETRY)


def test_several_chromatograms_warn_and_export_all(run, tmp_path):
    # D28 (6), amended: warn and export all until users settle on a rule; chromatogram= picks one
    two = copy.copy(run)
    two.chromatograms = dict(run.chromatograms)
    two.chromatograms["Chrom.2"] = run.chromatograms["Chrom.1"]
    out = pycorn.export_cadet(two, str(tmp_path / "all"), core_version="6", cadet_process=False, **GEOMETRY)
    assert out["chromatograms"] == ["Chrom.1", "Chrom.2"]
    assert sorted(os.listdir(tmp_path / "all")) == ["Chrom.1_core_v6.json", "Chrom.2_core_v6.json"]
    assert any("2 chromatograms selected (Chrom.1, Chrom.2): all exported; pick one with chromatogram=" in w
               for w in out["warnings"])
    one = pycorn.export_cadet(two, str(tmp_path / "one"), core_version="6", cadet_process=False,
                              chromatogram="Chrom.2", **GEOMETRY)
    assert one["chromatograms"] == ["Chrom.2"] and not any("chromatograms" in w for w in one["warnings"])
    with pytest.raises(KeyError, match="Chrom.9"):
        pycorn.export_cadet(two, str(tmp_path), chromatogram="Chrom.9")


def test_bad_arguments_raise_before_writing(run, tmp_path):
    folder = str(tmp_path / "o")
    with pytest.raises(TypeError, match="units"):
        pycorn.export_cadet(run, folder, units={"volume": "L"})
    with pytest.raises(ValueError, match="'5' or '6'"):
        pycorn.export_cadet(run, folder, core_version="7")
    with pytest.raises(ValueError, match="nothing to write"):
        pycorn.export_cadet(run, folder, cadet_process=False)
    with pytest.raises(ValueError, match="core_version"):
        pycorn.export_cadet(run, folder, core_overrides={"input": {}})
    with pytest.raises(ValueError, match="'input'"):
        pycorn.export_cadet(run, folder, core_version="6", core_overrides={"meta": {}})
    # the CLI flags' shapes, with an example rather than "'float' object is not iterable"
    with pytest.raises(TypeError, match=r"per wavelength.*epsilon=\{280"):
        pycorn.export_cadet(run, folder, epsilon=1.4)
    with pytest.raises(TypeError, match=r"molecular_weight is \(value, unit\)"):
        pycorn.export_cadet(run, folder, epsilon={280: (1.4, "ml/(mg*cm)")}, molecular_weight=150000)
    assert not os.path.exists(folder)


def test_core_without_geometry_raises_and_leaves_nothing(run, tmp_path):
    with pytest.raises(ValueError, match="bed_height="):
        pycorn.export_cadet(run, str(tmp_path), core_version="6")
    assert not os.listdir(tmp_path)
