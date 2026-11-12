# -*- coding: utf-8 -*-
"""
The Excel workbook `pycorn export --format xlsx` writes for one chromatogram, made to be read
by people rather than programs (owner, 2026-11-08):

- `Summary` first: what the run is (name, source, dates, Vc, column), how many warnings to
  check, the units, the PyCORN version and commit, and one line per sheet saying what it
  holds and whether it is UNICORN's data or PyCORN's analysis (`pycorn.export_tables`).
- Sheets take the tables' titles (`export_tables.title`: 'Planned vs run - blocks', ...), the
  same names the in-browser page shows; the CSV files use them in lower case.
- `Chart`: a native Excel chart of the chromatogram against volume (time when a curve has no
  volume): UV curves on the left axis, conductivity (else %B) on the right, injection and
  fraction marks along the bottom. Curves are found by role (`pycorn.curve_roles`), never by
  name; flat curves (e.g. an unused UV channel) are left out.
- Every table sheet: header row frozen, filter buttons, column widths that fit, dates shown
  as dates, numbers shown with the decimals UNICORN specifies (`pycorn.display_precision`;
  the cells keep the full values). Empty tables are left out (as in csv), except curves and events. Columns that
  hold a time or volume without a unit in their name get it in the header here only; the csv
  columns keep their names.

Formatting and the chart need xlsxwriter (a core dependency); with only openpyxl the plain
tables are written in the same order, without them.
"""
import re

import numpy as np

from .export_tables import ORIGIN_NOTES, note, origin, title

KEEP_EMPTY = ("curves", "events")
DATETIME_FORMAT = "yyyy-mm-dd hh:mm:ss.000"
MAX_WIDTH = 50
MAX_LABELS = 60  # more injection / fraction marks than this: markers without labels
_AXIS_COLUMN = re.compile(r"^(start_|end_|length_)?(time|volume)(_adjusted)?$")


def sheets_to_write(frames):
    """(sheet, frame) pairs worth a sheet: every non-empty one, curves and events always."""
    return [(s, f) for s, f in frames if s in KEEP_EMPTY or not f.empty]


def workbook_name(run_name, chrom_stem, clean):
    """'<run name>_<chromatogram>.xlsx', so workbooks copied into one folder stay apart."""
    run_part = clean(run_name or "") if run_name else ""
    return "%s_%s.xlsx" % (run_part, chrom_stem) if run_part else chrom_stem + ".xlsx"


def _one(values):
    """The single non-empty value in `values`, else None."""
    found = {v for v in values if isinstance(v, str) and v}
    return found.pop() if len(found) == 1 else None


def _units(chrom, sheet, frame):
    """(time unit, volume unit) of the axis columns of `frame`, when one unit each."""
    if "time_unit" in frame.columns:
        return _one(frame["time_unit"]), _one(frame["volume_unit"])
    if sheet == "events":
        ecs = list(chrom.event_curves.values())
        return _one(ec.time_unit for ec in ecs), _one(ec.volume_unit for ec in ecs)
    return None, None


def with_units(chrom, sheet, frame):
    """`frame` with '(unit)' after time and volume columns that have no unit in their name."""
    time_unit, volume_unit = _units(chrom, sheet, frame)
    names = {}
    for column in frame.columns:
        m = _AXIS_COLUMN.match(str(column))
        unit = (time_unit if m.group(2) == "time" else volume_unit) if m else None
        if unit:
            names[column] = "%s (%s)" % (column, unit)
    return frame.rename(columns=names) if names else frame


def _text(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return str(value)


def _shown(value, unit):
    """`value` as text with the decimals its unit is shown with (`units.display_decimals`)."""
    from .units import display_decimals
    decimals = display_decimals(unit)
    return _text(value) if decimals is None or _number(value) is None else "%.*f" % (decimals, value)


def _source(run):
    """The file name (no folders: they can name the user) or the database result id."""
    if run.source == "sql":
        return run.locator
    return "%s file %s" % (run.source, re.split(r"[\\/]", run.locator or "")[-1])


def summary_rows(run, chrom, frames, run_frames):
    """[(label, value)] for the summary sheet's top part."""
    info = dict(run_frames).get("run_info")
    row = {}
    if info is not None and len(info) and "chromatogram" in info.columns:
        match = info[info["chromatogram"] == chrom.name]
        row = match.iloc[0].to_dict() if len(match) else {}
    warnings = dict(run_frames).get("warnings")
    n_warnings = 0
    if warnings is not None and len(warnings):
        n_warnings = int(warnings["chromatogram"].isin(["", chrom.name]).sum())
    vc = row.get("vc")
    vc_text = "" if vc is None or vc != vc else "%s %s (from %s)" % (_shown(vc, row.get("vc_unit")),
                                                                    _text(row.get("vc_unit")),
                                                                    _text(row.get("vc_source")))
    curve_sample = next((c for c in chrom.curves.values() if c.values is not None), None)
    from .run_details import local_time_text, summary_items
    from .version import version_text
    # W17: what the chromatogram is (original, imported from <result>, derived)
    kind = next((d["label"] for d in run.describe_chromatograms() if run.chromatograms.get(d["name"]) is chrom),
                chrom.kind)
    return [
        ("Run", run.name), ("Chromatogram", chrom.name), ("Chromatogram kind", kind), ("Read from", _source(run)),
        ("Created", _text(local_time_text(run.result, "Created"))), ("UNICORN version", _text(run.unicorn_version)),
        *summary_items(run),
        ("Start (UTC)", _text(row.get("start_utc"))), ("End (UTC)", _text(row.get("end_utc"))),
        ("Column volume (Vc)", vc_text or "not found"), ("Column", _text(row.get("column_name"))),
        ("Time unit", _text(curve_sample.time_unit if curve_sample else "")),
        ("Volume unit", _text(curve_sample.volume_unit if curve_sample else "")),
        ("Warnings", "%d: see the %s sheet before relying on the numbers" % (n_warnings, title("warnings"))
         if n_warnings else "none"),
        ("Exported by", "PyCORN " + version_text()),
        ("Validation", "PyCORN is validated only on the files and databases it was tested with; "
                       "review the numbers before relying on them."),
    ]


def _decimals(chrom, sheet, frame, curves):
    """Decimals to show per column of `frame` (`display_precision.column_decimals`)."""
    from .display_precision import column_decimals
    time_unit, volume_unit = _units(chrom, sheet, frame)
    sample = next((c for c in chrom.curves.values() if c.values is not None), None)
    return column_decimals(frame, time_unit or (sample.time_unit if sample else None),
                           volume_unit or (sample.volume_unit if sample else None), curves)


def write_workbook(path, run, chrom, frames, run_frames, engine, prov=None, extra_curves=()):
    """
    Write the workbook for `chrom` to `path` (see the module docstring). `prov`: the export's
    provenance (`run_export.provenance`), shown on the summary so the workbook can be made again.
    `extra_curves`: derived curves in the curves table (mg/ml), for their display decimals.
    """
    import pandas as pd
    from .export_tables import provenance_rows
    curves = {c.name: c for c in list(chrom.curves.values()) + list(extra_curves)}
    written = sheets_to_write(frames + run_frames)
    decimals = {s: _decimals(chrom, s, f, curves) for s, f in written}
    tables = [(s, with_units(chrom, s, f)) for s, f in written]
    formatted = engine == "xlsxwriter"
    options = {"datetime_format": DATETIME_FORMAT} if formatted else {}
    with pd.ExcelWriter(path, engine=engine, **options) as writer:
        summary = [(label, value, "") for label, value in summary_rows(run, chrom, frames, run_frames)]
        made = [("How it was made", "", "")] + [(label, text, "") for label, text in provenance_rows(prov)] \
            if prov else []
        has_chart = formatted and bool(chart_series(chrom, frames))
        sheets = ["summary"] + (["chart"] if has_chart else []) + [s for s, _ in tables]
        origins = [("Origin", "What it means", "")] + [(o, n, "") for o, n in ORIGIN_NOTES.items()]
        guide = [("Sheet", "What it holds", "Origin")] + [(title(s), note(s), origin(s)) for s in sheets]
        top = pd.DataFrame(summary + [("", "", "")] + (made + [("", "", "")] if prov else []) + origins
                           + [("", "", "")] + guide, columns=["PyCORN export", "", ""])
        top.to_excel(writer, sheet_name=title("summary"), index=False)
        if not formatted:
            for sheet, frame in tables:
                frame.to_excel(writer, sheet_name=title(sheet), index=False)
            return
        book = writer.book
        bold = book.add_format({"bold": True})
        ws = writer.sheets[title("summary")]
        ws.set_column(0, 0, 22, bold)
        ws.set_column(1, 1, 100)
        ws.set_column(2, 2, 18)
        row = len(summary) + 2  # +1: the header row, +1: the blank row
        if prov:
            ws.write_row(row, 0, made[0], bold)
            row += len(made) + 1
        ws.write_row(row, 0, origins[0], bold)
        ws.write_row(row + len(origins) + 1, 0, guide[0], bold)
        chartsheet = book.add_chartsheet(title("chart")) if has_chart else None
        number_format = _number_formats(book)
        for sheet, frame in tables:
            frame.to_excel(writer, sheet_name=title(sheet), index=False)
            _format_table(writer.sheets[title(sheet)], frame, decimals[sheet], number_format)
        if chartsheet is not None:
            chartsheet.set_chart(_chart(book, run.name, chrom, frames, dict(tables)))


def _width(frame, column):
    import pandas as pd
    if pd.api.types.is_datetime64_any_dtype(frame[column]):
        return len(DATETIME_FORMAT) + 2
    sample = frame[column].head(200)
    longest = max([len(str(column))] + [len(_text(v)) for v in sample])
    return min(max(longest + 2, 8), MAX_WIDTH)


def _number_formats(book):
    """decimals -> a cell format showing that many ('0.000'); made once per workbook."""
    made = {}

    def number_format(decimals):
        if decimals not in made:
            made[decimals] = book.add_format({"num_format": "0." + "0" * decimals if decimals else "0"})
        return made[decimals]
    return number_format


def _number(value):
    """`value` as a float for a number cell, else None (text, missing, booleans)."""
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, np.integer, np.floating)):
        return None
    value = float(value)
    return None if np.isnan(value) or np.isinf(value) else value


def _format_table(ws, frame, decimals=None, number_format=None):
    """
    Frozen header, filter, widths; numbers shown with `decimals` per column (int, one per row,
    or None: Excel's General). The cells keep the full values; only the display rounds.
    """
    ws.freeze_panes(1, 0)
    if len(frame.columns):
        ws.autofilter(0, 0, max(len(frame), 1), len(frame.columns) - 1)
    decimals = decimals or [None] * len(frame.columns)
    for i, (column, places) in enumerate(zip(frame.columns, decimals)):
        if isinstance(places, int):
            ws.set_column(i, i, _width(frame, column), number_format(places))
            continue
        ws.set_column(i, i, _width(frame, column))
        if isinstance(places, list):  # the rows' units differ: a format per cell
            for row, (value, d) in enumerate(zip(frame[column], places), start=1):
                number = _number(value)
                if number is not None and d is not None:
                    ws.write_number(row, i, number, number_format(d))


# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------

def _live(curve):
    values = curve.values
    return values is not None and len(values) > 1 and np.nanmax(values) != np.nanmin(values)


def chart_series(chrom, frames):
    """
    [(curve, y2)] to draw: every live UV curve (left axis), then the first live conductivity
    curve, else %B, on the right axis. Empty when there is no curves sheet or nothing to draw.
    """
    if "curves" not in dict(frames):
        return []
    left = [c for c in chrom.curves_with_role("uv") if _live(c)]
    right = next((c for role in ("conductivity", "conc_b") for c in chrom.curves_with_role(role) if _live(c)), None)
    return [(c, False) for c in left] + ([(right, True)] if right is not None else [])


def _x_axis(series):
    """'volume' when every curve drawn has a volume axis, else 'time'."""
    return "volume" if all(c.volume is not None for c, _ in series) else "time"


def _ref(sheet, first_row, col, last_row):
    from xlsxwriter.utility import xl_rowcol_to_cell
    return "='%s'!%s:%s" % (sheet, xl_rowcol_to_cell(first_row, col, True, True),
                            xl_rowcol_to_cell(last_row, col, True, True))


def _chart(book, run_name, chrom, frames, tables):
    from .run_export import _header
    series = chart_series(chrom, frames)
    axis = _x_axis(series)
    curves = tables["curves"]
    chart = book.add_chart({"type": "scatter", "subtype": "straight"})
    x_unit = None
    for curve, y2 in series:
        unit = curve.volume_unit if axis == "volume" else curve.time_unit
        x_head, y_head = _header(curve.name, axis, unit), _header(curve.name, "value", curve.unit)
        if x_head not in curves.columns or y_head not in curves.columns:
            continue  # curve left out of the table (--curves, --curve-roles)
        x_col, y_col = curves.columns.get_loc(x_head), curves.columns.get_loc(y_head)
        last = int(curves[y_head].last_valid_index() or 0) + 1  # thinned: its own row count
        chart.add_series({"name": "%s (%s)" % (curve.name, curve.unit) if curve.unit else curve.name,
                          "categories": _ref(title("curves"), 1, x_col, last),
                          "values": _ref(title("curves"), 1, y_col, last),
                          "y2_axis": y2, "line": {"width": 1.25}})
        x_unit = x_unit or unit
    _add_marks(book, chart, tables, axis)
    left = [c for c, y2 in series if not y2]
    right = [c for c, y2 in series if y2]
    chart.set_title({"name": "%s: %s" % (run_name, chrom.name) if run_name else chrom.name})
    # labels at the bottom: UV can go below zero
    chart.set_x_axis({"name": "%s (%s)" % (axis, x_unit) if x_unit else axis, "label_position": "low",
                      "major_gridlines": {"visible": True, "line": {"color": "#E0E0E0"}}})
    chart.set_y_axis({"name": _units_label(left)})
    if right:
        chart.set_y2_axis({"name": _units_label(right)})
    chart.set_legend({"position": "bottom"})
    return chart


def _units_label(curves):
    units = sorted({c.unit for c in curves if c.unit})
    return ", ".join(units)


def _add_marks(book, chart, tables, axis):
    """Injection and fraction starts as markers on the x axis, labelled with their names."""
    marks = []
    for sheet, symbol in (("injections", ("triangle", "#C0392B")), ("fractions", ("diamond", "#7F7F7F"))):
        frame = tables.get(sheet)
        if frame is None or not len(frame):
            continue
        x_col = next((c for c in frame.columns if str(c).split(" (")[0] == "start_" + axis), None)
        if x_col is None:
            continue
        rows = frame[~frame["pseudo"].astype(bool)] if "pseudo" in frame.columns else frame
        rows = rows[rows[x_col].notna()]
        if len(rows):
            marks.append((sheet, symbol, list(rows[x_col]), [_text(n) for n in rows["name"]]))
    if not marks:
        return
    ws = book.add_worksheet("chart_marks")
    ws.hide()
    ws.write_row(0, 0, ["kind", "x", "y", "label"])
    row = 1
    for sheet, symbol, xs, labels in marks:
        first = row
        for x, label in zip(xs, labels):
            ws.write_row(row, 0, [sheet, x, 0, label])
            row += 1
        shape, color = symbol
        series = {"name": title(sheet), "categories": _ref("chart_marks", first, 1, row - 1),
                  "values": _ref("chart_marks", first, 2, row - 1), "line": {"none": True},
                  "marker": {"type": shape, "size": 7, "fill": {"color": color}, "border": {"color": color}}}
        if len(xs) <= MAX_LABELS:
            series["data_labels"] = {"custom": [{"value": label} for label in labels],
                                     "position": "above", "font": {"rotation": -45, "size": 8}}
        chart.add_series(series)
