# -*- coding: utf-8 -*-
"""
How many decimals each table column is shown with (owner, 2026-11-10).

Values are never rounded in the model, the tables, CSV or JSON: only what people read is (the
in-browser page, Excel cell formats). The decimals come from what UNICORN specifies, carried
through every unit conversion (`pycorn.units`):

- curve values: the curve's `AmplitudePrecision` (`Curve.precision`)
- time, volume, CV, flow and %B: `units.display_decimals()` of the column's unit (the decimals
  UNICORN prints these units with in its run log; s, L, ... converted from min, ml, ...)

A column is found by its name: '<curve>: <what> (<unit>)' in the curves table; time, volume and
CV columns (start_/end_/length_, _adjusted) in the unit of the row's `time_unit` /
`volume_unit` (else the chromatogram's); `*_s`, `*_ml`, `*_ml_min`; `percent_b` / `percent_q`;
the method-hint amounts in the row's `base` (CV, ml or min); Vc in its `vc_unit`; a phase
window's offsets in its `window_unit` and its statistics (mean, median, sd, min, max) with the
row's `curve_precision` (the curve's `AmplitudePrecision`). Every other
column has no specification (None) and is shown as before.
"""
import re

from .export_tables import _CURVE_COLUMN
from .units import display_decimals

_AXIS = re.compile(r"^(?:start_|end_|length_)?(time|volume|cv)(?:_adjusted)?$")
# columns that hold an amount in the unit named by the row's 'base' column (`method_hints`)
BASE_COLUMNS = ("planned", "planned_start", "planned_end", "executed", "pre_switch", "gap_allowance")
SUFFIX_UNITS = (("_ml_min", "ml/min"), ("_ml", "ml"), ("_s", "s"))
PERCENT_COLUMNS = ("percent_b", "percent_q")
# column -> the column that names its unit
UNIT_COLUMNS = {"vc": "vc_unit", "start_offset": "window_unit", "end_offset": "window_unit"}
# column -> the column that holds its decimals per row (Phase window statistics: the curve's precision)
DECIMALS_COLUMNS = {stat: "curve_precision" for stat in ("mean", "median", "sd", "min", "max")}


def _spec(name, columns, time_unit, volume_unit, curves):
    """('decimals', n), ('unit', unit), ('row', column naming the unit) or None for column `name`."""
    m = _CURVE_COLUMN.match(name)
    if m:
        what = m.group("what")
        if what == "value":
            curve = curves.get(m.group("curve") + (m.group("mgml") or ""))
            return ("decimals", curve.precision) if curve is not None else None
        if what == "CV":
            return "unit", "CV"
        if what in ("time", "volume"):
            return "unit", m.group("unit")
        return None
    m = _AXIS.match(name)
    if m:
        axis = m.group(1)
        if axis == "cv":
            return "unit", "CV"
        if axis + "_unit" in columns:
            return "row", axis + "_unit"
        return "unit", time_unit if axis == "time" else volume_unit
    if name in BASE_COLUMNS and "base" in columns:
        return "row", "base"
    if name in UNIT_COLUMNS and UNIT_COLUMNS[name] in columns:
        return "row", UNIT_COLUMNS[name]
    if name in DECIMALS_COLUMNS and DECIMALS_COLUMNS[name] in columns:
        return "row_decimals", DECIMALS_COLUMNS[name]
    if name in PERCENT_COLUMNS:
        return "unit", "%"
    if name.endswith("_per_s"):  # a rate (percent_b_slope_per_s), not seconds
        return None
    for suffix, unit in SUFFIX_UNITS:
        if name.endswith(suffix):
            return "unit", unit
    return None


def _decimals(value):
    """A stored decimals cell as an int, or None (empty, NaN)."""
    try:
        return int(value) if value is not None and value == value else None
    except (TypeError, ValueError):
        return None


def column_decimals(frame, time_unit=None, volume_unit=None, curves=None):
    """
    One entry per column of `frame`: the decimals to show (int), a list with one per row when
    the rows' units differ, or None (no specification). `time_unit` / `volume_unit`: the
    chromatogram's, for tables without unit columns; `curves`: {name: Curve} for the curves
    table's value columns.
    """
    curves = curves or {}
    columns = [str(c) for c in frame.columns]
    out = []
    for column, name in zip(frame.columns, columns):
        spec = _spec(name, columns, time_unit, volume_unit, curves)
        if spec is None:
            out.append(None)
        elif spec[0] == "decimals":
            out.append(spec[1])
        elif spec[0] == "unit":
            out.append(display_decimals(spec[1]))
        elif spec[0] == "row_decimals":
            found = [_decimals(d) for d in frame[frame.columns[columns.index(spec[1])]]]
            out.append(found[0] if len(set(found)) == 1 else (found or None))
        else:
            found = [display_decimals(u) for u in frame[frame.columns[columns.index(spec[1])]]]
            out.append(found[0] if len(set(found)) == 1 else (found or None))
    return out
