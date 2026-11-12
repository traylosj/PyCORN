# -*- coding: utf-8 -*-
"""The Excel workbook for people: summary first, chart by curve role, formatted tables."""
import os

import pandas as pd
import pytest

from pycorn import excel_export as xx
from pycorn import run_export as rx
from pycorn.sources import open_run

SAMPLE = os.path.abspath("../samples/sample.zip")


@pytest.fixture(scope="module")
def run():
    return open_run(SAMPLE)


def _frames(chrom):
    return [("curves", rx.curves_frame(chrom)), ("events", rx.events_frame(chrom)),
            ("peaks", pd.DataFrame()), ("injections", rx.event_table_frame(chrom, "injection")),
            ("fractions", rx.event_table_frame(chrom, "fraction"))]


def test_workbook_name():
    assert xx.workbook_name("Run 1/a", "Chrom.1", rx.clean_name) == "Run 1_a_Chrom.1.xlsx"
    assert xx.workbook_name("", "Chrom.1", rx.clean_name) == "Chrom.1.xlsx"


def test_units_added_to_bare_axis_columns_only(run):
    chrom = run.chromatogram()
    frame = rx.event_table_frame(chrom, "fraction")
    renamed = xx.with_units(chrom, "fractions", frame)
    unit = frame["time_unit"].dropna().iloc[0]
    assert "start_time (%s)" % unit in renamed.columns and "start_cv" in renamed.columns
    assert "length_time (%s)" % unit in renamed.columns and "length_cv" in renamed.columns
    curves = rx.curves_frame(chrom)
    assert list(xx.with_units(chrom, "curves", curves).columns) == list(curves.columns)  # already carry units


def test_chart_series_by_role_and_live_only(run):
    chrom = run.chromatogram()
    series = xx.chart_series(chrom, _frames(chrom))
    left = [c for c, y2 in series if not y2]
    right = [c for c, y2 in series if y2]
    assert left and all(c in chrom.curves_with_role("uv") for c in left)
    assert len(right) == 1 and right[0] in chrom.curves_with_role("conductivity")
    assert xx.chart_series(chrom, [("events", pd.DataFrame())]) == []


def test_summary_names_the_file_not_its_folders(run):
    chrom = run.chromatogram()
    rows = dict(xx.summary_rows(run, chrom, [], []))
    assert rows["Read from"] == "zip file sample.zip"
    assert rows["Warnings"] == "none"


def test_empty_sheets_left_out_but_curves_and_events_kept():
    empty = pd.DataFrame()
    kept = xx.sheets_to_write([("curves", empty), ("events", empty), ("peaks", empty), ("phases", pd.DataFrame({"a": [1]}))])
    assert [s for s, _ in kept] == ["curves", "events", "phases"]


def test_workbook_with_chart(run, tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    pytest.importorskip("xlsxwriter")
    chrom = run.chromatogram()
    path = tmp_path / "w.xlsx"
    xx.write_workbook(path, run, chrom, _frames(chrom), [], "xlsxwriter")
    book = openpyxl.load_workbook(path)
    assert book.sheetnames[:3] == ["Summary", "Chart", "Curves"]
    chart = book.chartsheets[0]._charts[0]
    n_curves = len(xx.chart_series(chrom, _frames(chrom)))
    assert len(chart.series) >= n_curves  # curves, then injection / fraction marks
    assert book["Events"]["C1"].value.startswith("time (")
    # numbers shown with the decimals UNICORN specifies; the cells keep the full values
    heads = {c.value: c.column_letter for c in book["Fractions"][1]}
    cell = book["Fractions"][heads["length_cv"] + "3"]
    assert cell.number_format == "0.00" and cell.value != round(cell.value, 2)
    uv = chrom.curves_with_role("uv")[0]
    heads = {c.value: c.column_letter for c in book["Curves"][1]}
    cell = book["Curves"][heads[rx._header(uv.name, "value", uv.unit)] + "2"]
    assert cell.number_format == "0." + "0" * uv.precision


def test_summary_says_origin_and_how_it_was_made(run, tmp_path):
    pytest.importorskip("xlsxwriter")
    chrom = run.chromatogram()
    path = tmp_path / "w.xlsx"
    prov = rx.provenance(rx.ExportOptions(method_hints=True, route="CLI: pycorn export",
                                          command="pycorn export x.zip --out o --method-hints"), ["xlsx"])
    xx.write_workbook(path, run, chrom, _frames(chrom), [], "xlsxwriter", prov)
    rows = pd.read_excel(path, sheet_name="Summary", header=None).fillna("").values.tolist()
    by_label = {r[0]: r for r in rows}
    assert by_label["Route"][1] == "CLI: pycorn export"
    assert by_label["Command"][1] == "pycorn export x.zip --out o --method-hints"
    assert by_label["Options"][1] == '{"method_hints": true}'
    assert by_label["Repository"][1] == "https://github.com/traylosj/PyCORN"
    assert by_label["Curves"][2] == "UNICORN + PyCORN" and by_label["Injections"][2] == "PyCORN analysis"
    assert {"UNICORN", "PyCORN calculated", "PyCORN analysis"} <= set(by_label)  # the origin legend


def test_plain_tables_without_xlsxwriter(run, tmp_path):
    pytest.importorskip("openpyxl")
    chrom = run.chromatogram()
    path = tmp_path / "w.xlsx"
    xx.write_workbook(path, run, chrom, _frames(chrom), [], "openpyxl")
    assert list(pd.read_excel(path, sheet_name=None)) == ["Summary", "Curves", "Events", "Injections", "Fractions"]
