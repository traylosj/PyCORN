"""
Checks from the CADET forum's UNICORN threads (docs/ROADMAP.md, "CADET forum: other threads", M14-M17).

Run on every sample bundle: pH keeps its decimals (the original PyCORN read pH 8.0 as 80), time axes
never repeat a point (a repeated time point broke a CADET-Match fit), and a run without a column
still builds a CADET experiment.
"""
import glob
import os

import numpy as np
import pytest

from pycorn import open_run
from pycorn.exporters.cadet import build_experiment

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLES = sorted(glob.glob(os.path.join(HERE, "..", "samples", "*.zip")))
NO_COLUMN = {"sample.zip", "u711_column_test.zip", "u711_manual_run.zip"}  # UNICORN column 'Any' or none named


@pytest.fixture(scope="module", params=SAMPLES, ids=os.path.basename)
def run(request):
    return open_run(request.param)


def test_ph_values_are_ph_not_ten_times_ph(run):
    for chrom in run.chromatograms.values():
        for curve in chrom.curves_with_role("ph"):
            values = np.asarray(curve.values, dtype=float)
            assert np.nanmin(values) >= 0.0 and np.nanmax(values) <= 14.0, curve.name
            # a probe in a buffer reads a fractional pH; x10 integers (80 for 8.0) would be whole numbers
            if np.nanmax(values) > 0.0:
                assert np.mean(np.abs(values - np.round(values)) > 1e-6) > 0.5, curve.name


def test_time_axes_never_repeat_a_point(run):
    # the volume axis does repeat while the pump is paused (time moves, volume does not); time must not
    for chrom in run.chromatograms.values():
        for curve in chrom.curves.values():
            if curve.time is not None and len(curve.time) > 1:
                assert (np.diff(np.asarray(curve.time, dtype=float)) > 0).all(), curve.name


def test_cadet_signals_have_strictly_increasing_time(run):
    exp = build_experiment(run)
    for signal in exp.signals:
        assert (np.diff(signal.time_s) > 0).all(), signal.name


def test_run_without_a_column_still_builds(request, run):
    exp = build_experiment(run)
    name = os.path.basename(request.node.callspec.id)
    if name in NO_COLUMN:
        assert exp.column.get("name") is None
        assert any(w.startswith("column:") for w in exp.warnings), "no-column run built without saying so"
