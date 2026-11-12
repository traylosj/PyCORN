# -*- coding: utf-8 -*-
"""Smaller exports: which tables, which curves, thinning and one grid (`pycorn.curve_sampling`)."""
import os

import pandas as pd
import pytest

from pycorn import open_run
from pycorn.cli import export_options, main
from pycorn.run_export import ExportOptions, check_options, write_run

SAMPLES = os.path.abspath("../samples")


@pytest.fixture(scope="module")
def run():
    return open_run(os.path.join(SAMPLES, "sample.zip"))


def _curves(folder, written):
    name = next(w for w in written if w.endswith(".csv") and "_" not in w and w not in
                ("provenance.csv", "readme.csv", "warnings.csv"))
    return pd.read_csv(os.path.join(folder, name))


def _warning(folder, source):
    frame = pd.read_csv(os.path.join(folder, "warnings.csv"))
    return " ".join(frame.loc[frame["source"] == source, "message"].astype(str))


def test_choices_that_contradict_are_refused():
    for bad, message in ((ExportOptions(max_points=100), "needs --method"),
                         (ExportOptions(method="interval"), "--max-points only"),
                         (ExportOptions(max_points=100, method="minmax", derivative_share=0.5), "--derivative-share"),
                         (ExportOptions(max_points=100, method="interval", grid_step=1.0), "give one"),
                         (ExportOptions(grid_axis="time"), "--grid-step only"),
                         (ExportOptions(grid_step=1.0, grid_axis="pH"), "--grid-axis"),
                         (ExportOptions(tables=["curves", "nope"]), "unknown table")):
        with pytest.raises(ValueError, match=message):
            check_options(bad)


def test_tables_and_curves_pick(run, tmp_path):
    written = write_run(run, tmp_path / "a", options=ExportOptions(tables=["curves", "events"], curve_roles=["uv"]))
    assert "warnings.csv" in written and "README.txt" in written and "provenance.json" in written
    assert not any("phases" in w or "peaks" in w or "run_and_method" in w for w in written)
    heads = list(_curves(tmp_path / "a", written).columns)
    assert heads and all(h.split(":")[0].startswith("UV") for h in heads)
    with pytest.raises(ValueError, match="the curves are"):
        write_run(run, tmp_path / "b", options=ExportOptions(curves=["no such curve"]))
    with pytest.raises(ValueError, match="roles are"):
        write_run(run, tmp_path / "c", options=ExportOptions(curve_roles=["nope"]))


def test_tables_by_group(run, tmp_path):
    # W16: --tables takes the page's groups; a group keeps the curves, column and timeline is always in
    from pycorn.run_export import GROUP_NAMES, TABLE_GROUPS, wanted_tables
    assert GROUP_NAMES == ("key", "peaks", "method", "details", "audit", "runlog")
    grouped = [k for _, keys in TABLE_GROUPS for k in keys]
    assert len(grouped) == len(set(grouped))
    assert wanted_tables(None) is None
    assert wanted_tables(["phases"]) == {"phases", "run_info", "warnings", "column_guide"}
    assert {"curves", "peaks", "peak_summary"} <= wanted_tables(["peaks"])
    written = write_run(run, tmp_path / "a", options=ExportOptions(tables=["key", "runlog"]))
    stems = {os.path.splitext(w)[0] for w in written}
    assert any(s.endswith("_phases") for s in stems) and any(s.endswith("_events") for s in stems)
    assert any(s.endswith("_run_log_sections") for s in stems)
    assert "column_and_timeline" in stems and "warnings" in stems and "column_guide" in stems
    assert not any(s.endswith("_peaks") or s in ("method_steps", "run_and_method", "audit_log") for s in stems)
    assert len(_curves(tmp_path / "a", written).columns) > 1  # every curve (no --curves)
    with pytest.raises(ValueError, match="the groups are: key, peaks"):
        check_options(ExportOptions(tables=["keys"]))


def test_each_curve_thinned_on_its_own_axis(run, tmp_path):
    full = write_run(run, tmp_path / "full")
    thin = write_run(run, tmp_path / "thin", options=ExportOptions(
        max_points=500, method="derivative", derivative_share=0.5, tables=["curves"]))
    before, after = _curves(tmp_path / "full", full), _curves(tmp_path / "thin", thin)
    assert len(after) < len(before) and len(after) <= 700
    assert os.path.getsize(tmp_path / "thin" / next(w for w in thin if w.endswith(".csv") and "_" not in w
                                                       and w not in ("warnings.csv",))) < 0.5 * os.path.getsize(
        tmp_path / "full" / next(w for w in full if w.endswith(".csv") and "_" not in w and w != "warnings.csv"))
    assert "thinned to about 500" in _warning(tmp_path / "thin", "sampling")


def test_hybrid_and_every_nth(run, tmp_path):
    full = _curves(tmp_path / "f", write_run(run, tmp_path / "f", options=ExportOptions(tables=["curves"])))
    hy = _curves(tmp_path / "h", write_run(run, tmp_path / "h", options=ExportOptions(
        tables=["curves"], max_points=500, method="hybrid")))
    ev = _curves(tmp_path / "e", write_run(run, tmp_path / "e", options=ExportOptions(tables=["curves"], every_n=10)))
    assert len(hy) < len(full) and len(hy) <= 700
    assert len(full) // 10 <= len(ev) <= len(full) // 10 + 300  # every 10th plus step transitions
    assert "every 10th point" in _warning(tmp_path / "e", "sampling")
    with pytest.raises(ValueError, match="give one"):
        check_options(ExportOptions(every_n=5, grid_step=1.0))
    with pytest.raises(ValueError, match="--derivative-share"):
        check_options(ExportOptions(max_points=100, method="hybrid", derivative_share=0.3))


def test_grid_puts_every_curve_on_one_axis(run, tmp_path):
    written = write_run(run, tmp_path / "g", options=ExportOptions(grid_step=1.0, tables=["curves"]))
    frame = _curves(tmp_path / "g", written)
    grid = frame[[c for c in frame.columns if c.startswith("grid: time")][0]]
    assert grid.diff().dropna().round(9).nunique() == 1  # even steps
    assert any("volume" in c for c in frame.columns)  # the other axes follow
    assert "one time grid" in _warning(tmp_path / "g", "sampling")


def test_cli_flags(run, tmp_path, capsys):
    import argparse
    from pycorn import cli
    parser = cli.build_parser() if hasattr(cli, "build_parser") else None
    if parser is None:
        pytest.skip("no build_parser")
    args = parser.parse_args(["export", os.path.join(SAMPLES, "sample.zip"), "--out", str(tmp_path),
                              "--tables", "curves,events", "--curve-roles", "uv", "--max-points", "300",
                              "--method", "derivative", "--derivative-share", "0.5"])
    options = export_options(args)
    assert options.tables == ["curves", "events"] and options.max_points == 300 and options.derivative_share == 0.5
