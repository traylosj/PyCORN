"""
Regression against UNICORN's own CSV export of the same run (UNICORN 7.3, from the CADET forum).

samples/u73_nonbinding_pulse.zip is the result; samples/u73_nonbinding_pulse_unicorn.csv is what
UNICORN exported for it (curves and Run Log), sanitized: the Run Log texts that identify people or
paths hold the anonymized zip's text, every other byte is UNICORN's.

What the export shows:
- curves are on the injection-adjusted time axis, the Run Log on method time;
- each curve point is one of that curve's steps later than PyCORN's grid (`time_adjusted + step`),
  and the last stored point is left out; values agree to the 7 significant digits UNICORN writes.
  Likely cause: UNICORN's adjustment to the injection, not the grid itself (docs/FACTS.md, F1).
  Pinned here as found.
"""
import os

import numpy as np
import pytest

from pycorn import open_run

SAMPLES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "samples")
ZIP = os.path.join(SAMPLES, "u73_nonbinding_pulse.zip")
CSV = os.path.join(SAMPLES, "u73_nonbinding_pulse_unicorn.csv")


def read_unicorn_csv(path):
    """
    {name: (axis, values)} per column pair; the first three rows hold chromatogram, name and units.
    A Logbook column (last pair) holds '"text",' with commas and quotes inside left unescaped, so
    lines are split by hand: number cells first, the rest of the line is the text.
    """
    with open(path, encoding="utf-8", newline="") as f:
        lines = f.read().split("\r\n")
    names, units = lines[1].split(","), lines[2].split(",")
    pairs = [(c, names[c]) for c in range(0, len(names) - 1, 2) if names[c]]
    logbook = [c for c, _ in pairs if units[c + 1] == "Logbook"]
    n_split = logbook[0] + 1 if logbook else len(names)
    rows = [line.split(",", n_split) for line in lines[3:] if line]
    columns = {}
    for col, name in pairs:
        cells = [(row[col], row[col + 1]) for row in rows if row[col]]
        axis = np.array([float(a) for a, _ in cells])
        values = [v for _, v in cells] if col in logbook else np.array([float(v) for _, v in cells])
        columns[name] = (axis, values)
    return columns


@pytest.fixture(scope="module")
def chrom():
    return open_run(ZIP).chromatograms["Chrom.1"]


@pytest.fixture(scope="module")
def exported():
    return read_unicorn_csv(CSV)


def test_export_has_curves_and_run_log(chrom, exported):
    curves = [n for n in exported if n in chrom.curves]
    assert len(curves) == 4
    assert [n for n in exported if n not in chrom.curves] == ["Run Log"]


def test_curve_points_match_unicorn_one_step_later(chrom, exported):
    for name, (t, v) in exported.items():
        if name not in chrom.curves:
            continue
        curve = chrom.curves[name]
        step = float(curve.xml["DistanceBetweenPoints"])
        grid = np.asarray(curve.time_adjusted, dtype=float) + step
        first = int(np.searchsorted(grid, -1e-9))
        j = first + np.arange(len(t))
        assert j[-1] == len(grid) - 2, name  # every point from the injection on, but the last
        np.testing.assert_allclose(t, grid[j], atol=5e-4, rtol=0, err_msg=name)  # CSV times: 3 decimals
        np.testing.assert_allclose(v, np.asarray(curve.values, dtype=float)[j], rtol=1e-6, atol=1e-6,
                                   err_msg=name)


def test_injection_zero_matches_unicorn(chrom, exported):
    # without the one-step shift the CSV's first point would sit one step before zero
    for name, (t, _) in exported.items():
        if name in chrom.curves:
            assert t[0] >= 0 and t[0] < float(chrom.curves[name].xml["DistanceBetweenPoints"]) + 5e-4, name


def test_run_log_matches_events_on_method_time(chrom, exported):
    t, texts = exported["Run Log"]
    events = chrom.event_curves["Run Log"].events
    assert len(events) == len(t)
    np.testing.assert_allclose(t, [e.time for e in events], atol=5e-4, rtol=0)
    for raw, event in zip(texts, events):
        assert raw.rstrip(",")[1:-1] == event.text  # '"text",' with quotes inside left as they are
