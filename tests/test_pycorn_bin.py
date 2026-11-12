# -*- coding: utf-8 -*-
"""
examplescripts/pycorn-bin.py: UV channel selection, -r/-i for UNICORN 6+ bundles.

The script parses its arguments and runs when loaded, so the functions are loaded with an
input it skips, and the -r/-i checks run it on a copy of samples/sample.zip (csv output; no
matplotlib needed). Only the plot test needs matplotlib.
"""
import os
import runpy
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "examplescripts" / "pycorn-bin.py"
SAMPLE_ZIP = REPO / "samples" / "sample.zip"


@pytest.fixture(scope="module")
def script(tmp_path_factory):
    argv = sys.argv
    sys.argv = [str(SCRIPT), str(tmp_path_factory.mktemp("skip") / "nothing.txt")]
    try:
        return runpy.run_path(str(SCRIPT), run_name="pycorn_bin")
    finally:
        sys.argv = argv


def _run(cwd, *args):
    env = dict(os.environ, PYTHONPATH=str(REPO))
    done = subprocess.run([sys.executable, str(SCRIPT)] + list(args), cwd=str(cwd), env=env,
                          capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    return done.stdout


def test_missing_plot_or_xlsx_package_is_an_error_and_runs_from_a_checkout(tmp_path):
    # no PYTHONPATH: the script finds the package next to its folder
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    tried = 0
    for flags, module in ((["-p"], "matplotlib"), (["-e", "xlsx"], "xlsxwriter")):
        if _has(module):  # installed: the error path can't happen here
            continue
        tried += 1
        done = subprocess.run([sys.executable, str(SCRIPT)] + flags + [str(SAMPLE_ZIP)], cwd=str(tmp_path), env=env,
                              capture_output=True, text=True)
        assert done.returncode == 2 and module in done.stderr, done.stderr
        assert "No module named 'pycorn'" not in done.stderr
        assert not list(tmp_path.iterdir())  # nothing written
    if not tried:
        pytest.skip("matplotlib and xlsxwriter are both installed")


def _has(module):
    import importlib.util
    return importlib.util.find_spec(module) is not None


def _csv(path):
    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            x, y = line.rstrip("\r\n").split(",", 1)
            rows.append((float(x), y))
    return rows


def _entry(name, data_type, unit=None, data=()):
    entry = {"data_name": name, "data_type": data_type, "data": list(data)}
    if unit is not None:
        entry["unit"] = unit
    return entry


def test_uv_blocks_skip_channels_at_0_nm(script):
    entries = [
        # UNICORN 6+: UV by stored type; 0 nm channels spelled 'UV 3_0'
        _entry("UV 1_280", "UV", "mAU"), _entry("UV 2_0", "UV", "mAU"), _entry("UV 3_0", "UV", "mAU"),
        _entry("UV_280", "UV", "mAU"), _entry("UV 2 0 nm", "UV", "mAU"),
        _entry("xUV cell path length", "Other", "cm"), _entry("Cond", "Conduction", "mS/cm"),
        # .res: no curve type, UV by name; 0 nm spelled 'UV2_0nm'
        _entry("UV1_280nm", "curve", "mAU"), _entry("UV2_0nm", "curve", "mAU"), _entry("UV", "curve", "mAU"),
        _entry("Logbook", "annotation"),
    ]
    inp = {e["data_name"]: e for e in entries}
    assert script["uv_blocks"](inp) == ["UV 1_280", "UV_280", "UV1_280nm", "UV"]


def test_chromatogram_view_reduces_curves_and_zeroes_at_injection(script):
    view_class = script["ChromatogramView"]
    entries = {"UV 1_280": _entry("UV 1_280", "UV", "mAU", [(float(i), i * 10.0) for i in range(10)]),
               "Fractions": {"data_name": "Fractions", "data": [(2.0, "A1"), (3.0, "A2"), (4.0, "A3")]}}
    view = view_class(entries, "Chrom.1", [0.0, 1.5, 4.0], reduce=3, inj_sel=-1)
    assert view.inject_vol == 4.0
    assert view["UV 1_280"]["data"] == [(-4.0, 0.0), (-1.0, 30.0), (2.0, 60.0), (5.0, 90.0)]
    assert view["Fractions"]["data"] == [(-2.0, "A1"), (-1.0, "A2"), (0.0, "A3")]  # events are not reduced
    assert entries["UV 1_280"]["data"][1] == (1.0, 10.0)  # the loaded data is unchanged

    stored = view_class(entries, "Chrom.1", [0.0], reduce=1, inj_sel=-1)  # no injection
    assert stored.inject_vol == 0.0 and stored["UV 1_280"]["data"] == entries["UV 1_280"]["data"]


def test_zip_reduce_and_injection_in_csv_output(tmp_path):
    shutil.copy(str(SAMPLE_ZIP), str(tmp_path / "sample.zip"))
    _run(tmp_path, "-e", "csv", "-i", "0", "sample.zip")
    raw_uv = _csv(tmp_path / "sample_Chrom.1_UV 1_280.csv")
    raw_injection = _csv(tmp_path / "sample_Chrom.1_Injection.csv")
    assert len(raw_injection) == 1 and raw_injection[0][0] > 0

    _run(tmp_path, "-e", "csv", "-r", "10", "sample.zip")  # default -i -1: the last injection
    uv = _csv(tmp_path / "sample_Chrom.1_UV 1_280.csv")
    injection = _csv(tmp_path / "sample_Chrom.1_Injection.csv")
    assert injection[0][0] == 0.0
    expected = raw_uv[0::10]
    assert len(uv) == len(expected)
    zero = raw_injection[0][0]
    assert all(abs(x - (x0 - zero)) < 1e-9 and y == y0 for (x, y), (x0, y0) in zip(uv, expected))


def test_zip_plot_skips_channel_at_0_nm(tmp_path):
    pytest.importorskip("matplotlib")
    shutil.copy(str(SAMPLE_ZIP), str(tmp_path / "sample.zip"))
    out = _run(tmp_path, "-p", "-f", "png", "-d", "50", "--par2", "pH", "sample.zip")
    assert "Plotting: UV 1_280" in out and "Plotting: UV 3_0" not in out
    assert (tmp_path / "sample_Chrom.1_plot.png").exists()
