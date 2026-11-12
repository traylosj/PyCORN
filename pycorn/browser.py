# -*- coding: utf-8 -*-
"""
The Python side of the in-browser page (docs/ROADMAP.md, item 6; `web/`, built by
`tools/build_web_page.py`). The page runs PyCORN in Pyodide: files the user drops are written
to the browser's in-memory file system and never leave the PC.

`Session` holds the catalog of the dropped files (`catalog_from_files`, light read) and loads
one run at a time when it is shown or downloaded. Everything it returns is JSON text, so the
page needs no Python objects; downloads are written to a file the page reads back.

- `add(paths)`: catalog rows for new files; files that are not UNICORN results are named.
- `view(index)`: per chromatogram, the summary (as on the Excel summary sheet), the plot
  data (`plot_data`: curves by role, as the Excel chart, thinned for drawing, on every x axis
  the page offers; phase, fraction and pause tracks), phase / injection /
  fraction tables and the method hints (planned vs executed), and per run the variables, the
  run-level tables of `pycorn.run_details` (method, calibration, instrument, ...) and warnings.
- `download(index, kind, folder, options)`: 'xlsx' (a small zip of the workbook of `pycorn export`,
  with the method hints, and README.txt), 'csv' (its csv files, zipped) or 'core' (the CADET-Core
  JSON input, `export_cadet()`, with `CADET_EXPERIMENTAL_NOTE` in its warnings). The page shows
  everything (nothing leaves the PC); masking is a download option, for files that are shared;
  `options['curves']`: the curves ticked on the plot, written at full resolution.
- `export_start(ids, curves=[names], ...)` / `export_one` / `export_finish`: the ticked runs as
  xlsx or csv in one zip, laid out as `pycorn export` lays it out (a folder per run), with index.csv.
- `overlay_run(index, chromatogram)` / `overlay(ids, roles, axis)`: several runs in one plot (W11),
  by curve role, from a small cache of thinned curves per run and chromatogram (`overlay_entry`,
  `OVERLAY_MAX` run x chromatogram pairs at most; an id is a row index or [index, chromatogram]).
- Chromatograms (W17, owner 2026-11-11): a row with several chromatograms lists them with their
  kind (`rows()` 'chromatograms': original, imported with its source result, derived); `view`,
  the downloads (`options['chromatograms']`) and the overlay take the ticked ones; the default
  everywhere is the run's original chromatogram(s).
- Phase window statistics (M22, owner 2026-11-12): `window_info(ids)` (the form's phases, curve
  names and roles of the ticked runs), `parse_window(text)` (the form's fields, or the parser's
  error) and `window_stats(ids, windows, curves, labels)`: the table `pycorn export --window
  --window-curves` writes, for every ticked run in one table; no default window or curve. The
  downloads take them as `options['windows']` / `options['window_curves']`.
- Messages (`message()`, `MESSAGES`): `{key, values, text}`, the key a `msg.*` of web/i18n/en.json;
  errors come as `{'error': English sentence, 'message': {...}}`.
- `names()`: the tables' titles and origins (`pycorn.export_tables`) and the version, so the
  page names everything as the downloads do.
- `Session(progress=...)`: called with a step key (`STEPS`, texts in `web/i18n/`) and its values
  as JSON while a view or download works; the page runs Python in a web worker and shows them.

Owner, 2026-11-08: files only (no database in a browser), CADET-Core JSON is the only CADET
output (no HDF5, no CADET-Process).
"""
import datetime as dt
import json
import math
import os
import re
import shutil
import warnings
import zipfile

import numpy as np

from .messages import Message, plain_error
from .resample import hybrid_index

LIST_COLUMNS = ("name", "folder_path", "run_start_local", "system", "n_chromatograms")
MAX_POINTS = 4000  # points per curve on the page's plot: half derivative-spaced, half evenly spaced, plus min/max per bucket and step transitions (`resample.hybrid_index`)
AXES = ("clock", "time", "volume", "cv")  # the plot's x axes, in the page's picker order
AXIS_LABELS = {"clock": "Clock time", "time": "Method time", "volume": "Volume", "cv": "Column volumes"}
PHASE_PREFIX = re.compile(r"^Phase\s+")
KINDS = ("xlsx", "csv", "core")
CORE_VERSIONS = ("5", "6")
# page option -> export_cadet option, all (value, unit)
CORE_QUANTITIES = ("bed_height", "diameter", "particle_diameter")
WEB_ROUTE = "web page (pycorn.browser)"  # recorded with every download (`run_export.provenance`)
ALL_FOLDER = "PyCORN_export"  # 'Download all': the zip's top folder
# the steps `Session` reports to `progress`; the page's texts for them are in web/i18n/<language>.json
STEPS = ("step.reading_run", "step.tables", "step.plot", "step.details", "step.writing", "step.writing_core",
         "step.zipping")


def _cell(value):
    """A table cell as JSON-ready Python: NaN/NaT as None, times as ISO text."""
    if value is None:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        return None if math.isnan(value) or math.isinf(value) else value
    if isinstance(value, (bool, int, str)):
        return value
    try:
        import pandas as pd
        if value is pd.NaT or (not isinstance(value, (list, tuple, dict)) and pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return str(value)


def table(frame, key=None):
    """
    {'columns': [...], 'rows': [[...]], 'origins': [...], 'meanings': [...], 'checks': [...],
    'decimals': [...]} of a DataFrame (None: empty table); `key`: the export table it is
    (`export_tables`), for each column's origin (UNICORN, PyCORN calculated, PyCORN analysis),
    meaning and check. 'decimals': per column the decimals the page shows (int, a list with one
    per row, or None: no specification; `display_precision`); the rows keep the full values.
    """
    from .display_precision import column_decimals
    from .export_tables import column_check, column_origin
    if frame is None:
        return {"columns": [], "rows": [], "origins": [], "meanings": [], "checks": [], "decimals": []}
    found = [column_origin(key, c) + (column_check(key, c),) if key else ("", "", "") for c in frame.columns]
    return {"columns": [str(c) for c in frame.columns],
            "rows": [[_cell(v) for v in row] for row in frame.itertuples(index=False, name=None)],
            "origins": [o for o, _, _ in found], "meanings": [m for _, m, _ in found],
            "checks": [c for _, _, c in found], "decimals": column_decimals(frame)}


def names():
    """
    JSON of what the page labels with the export's names: {'tables': {key: {'title', 'origin',
    'note', 'shown', 'freeze_through'}}, 'origins': {origin: note}, 'version': '<version> (commit <id>)',
    'cadet_experimental': the one-sentence note on the CADET export (W15, English; the page
    shows its own translation under `dl.cadetExperimental`)}; 'shown': the columns the page
    shows by default (empty: all); 'groups': [[group, [table key, ...]]] for the Download panel
    (W16, `run_export.TABLE_GROUPS`). Built from `pycorn schema`'s dictionary (`schema.page_names`).
    """
    from .schema import page_names
    return json.dumps(page_names())


SAMPLE_MODES = ("all", "grid", "every", "hybrid")

# Messages to the page (W7): {'key': a `msg.*` key of web/i18n/en.json, 'values': {placeholder: value},
# 'text': the English text}. The page translates `key` with `values` and shows `text` for a key it lacks.
# Every key Python can return; the English texts are the `msg.*` block of web/i18n/en.json.
MESSAGES = ("msg.error", "msg.size_unknown", "msg.size_number", "msg.size_hybrid_min", "msg.size_every_min",
            "msg.size_grid_min", "msg.excel_rows", "msg.core_version", "msg.no_run", "msg.download_unknown",
            "msg.all_kind", "msg.no_curves", "msg.not_read_type", "msg.not_read", "msg.run_failed",
            "msg.format.xlsx", "msg.format.csv", "msg.format.core")


def message(key, text, **values):
    """A message for the page: {'key', 'values', 'text'} (`MESSAGES`)."""
    return {"key": key, "values": values, "text": text}


def error_message(e):
    """`message` of exception `e`: its own key when it is a `messages.Message`, else 'msg.error' with the sentence."""
    if isinstance(e, Message):
        return message(e.key, str(e), **e.values)
    text = plain_error(e)
    return message("msg.error", text, error=text)


def _failed(e):
    """JSON of a failed call: {'error': sentence, 'message': `error_message`}."""
    return json.dumps({"error": plain_error(e), "message": error_message(e)})


def export_options(options, route):
    """
    `run_export.ExportOptions` of the page's xlsx / csv downloads: the method hints it shows,
    `options["mask"]`, `options["curves"]` (the curve names ticked on the plot: the curves table
    holds those, at full resolution; a name a run lacks is left out with a warning; None: every
    curve), and `options["sample"]` = {"mode", "value"} for smaller files: 'all' (every
    stored point), 'grid' (one even time grid, value = step in the run's time unit), 'every'
    (every Nth point) or 'hybrid' (about `value` points per curve: half derivative, half evenly
    spaced, min/max and step transitions), `options["groups"]` (W16: the table groups ticked,
    `run_export.TABLE_GROUPS` names; None: every table), `options["chromatograms"]` (W17: the
    chromatogram names ticked; None: the original ones), `options["windows"]` + `options["window_curves"]`
    (M22: the page's phase windows, 'NAME=PHASE:FROM:TO UNIT' texts, and curve roles / names; both or
    neither, as `pycorn export --window --window-curves`). `messages.Message` for a missing or bad value.
    Built from `options_dict` through `ExportOptions.from_dict` (A5: the form `pycorn export
    --options` reads), `route` added.
    """
    from .run_export import ExportOptions
    result = ExportOptions.from_dict(options_dict(options))
    result.route = route
    return result


def options_dict(options):
    """
    The page's choices (`export_options`) in `ExportOptions.to_dict()`'s form, only what differs
    from the defaults (A5): method hints always on (owner Q5: the page shows them). The
    options.json of Download all holds `ExportOptions.from_dict(...).to_dict()` of it.
    """
    from .run_export import GROUP_NAMES
    sample = options.get("sample") or {}
    mode = sample.get("mode") or "all"
    if mode not in SAMPLE_MODES:
        raise Message("msg.size_unknown", "unknown size %r; known: %s" % (mode, ", ".join(SAMPLE_MODES)),
                      mode=str(mode), known=", ".join(SAMPLE_MODES))
    kwargs = {}
    if mode != "all":
        try:
            value = float(sample.get("value"))
        except (TypeError, ValueError):
            raise Message("msg.size_number", "give a number for the %s size" % mode, mode=mode) from None
        if mode == "grid":
            if not value > 0:
                raise Message("msg.size_grid_min", "the grid step must be above 0, got %g" % value, value="%g" % value)
            kwargs["grid_step"] = value
        elif mode == "every":
            if value < 1:
                raise Message("msg.size_every_min", "N must be at least 1, got %d" % value, value="%d" % value)
            kwargs["every_n"] = int(value)
        else:
            if value < 2:
                raise Message("msg.size_hybrid_min", "points per curve must be at least 2, got %d" % value,
                              value="%d" % value)
            kwargs.update(max_points=int(value), method="hybrid")
    chroms = options.get("chromatograms")
    if isinstance(chroms, (list, tuple)) and chroms:  # W17: the ticked chromatograms (a dict: per run, export_one)
        kwargs["chromatograms"] = [str(c) for c in chroms]
    curves = options.get("curves")
    if curves is not None:
        curves = [str(c) for c in curves]
        if not curves:
            raise Message("msg.no_curves", "no curve ticked: tick at least one curve to download")
        kwargs.update(curves=curves, curves_missing_ok=True)
    groups = options.get("groups")
    # M22: the page's phase windows and their curves, both or neither (as --window + --window-curves)
    windows, window_curves = options.get("windows"), options.get("window_curves")
    if windows and window_curves:
        kwargs.update(windows=[str(w) for w in windows], window_curves=[str(c) for c in window_curves])
    if groups is not None:
        groups = [str(g) for g in groups if str(g) in GROUP_NAMES]
        if set(groups) != set(GROUP_NAMES):  # all ticked: every table, as before W16
            kwargs["tables"] = groups + ["curves"]  # the curves: the plot's chips (`curves`)
            if not groups and "windows" in kwargs:  # a group keeps the window table; no group: named
                kwargs["tables"].append("phase_windows")
    return dict(method_hints=True, mask_identifying=bool(options.get("mask")), **kwargs)


def _floats(values):
    """JSON-ready numbers: NaN as None."""
    return [None if math.isnan(v) else float(v) for v in np.asarray(values, dtype=float)]


def _epoch_ms(value, offset_min):
    """
    A UTC time (datetime, Timestamp or datetime64; naive means UTC) as ms since 1970 on the
    instrument's clock, `offset_min` from UTC (the page's date axis shows it as it is); None if unknown.
    """
    import pandas as pd
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if pd.isna(ts):
        return None
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return (ts - pd.Timestamp(0)) / pd.Timedelta(milliseconds=1) + offset_min * 60000.0


def _curve_axis(curve, key, offset_min):
    """`curve`'s x on axis `key` as floats (NaN where unknown; 'clock': `_epoch_ms`); None if it has none."""
    if key == "clock":
        if curve.utc is None:
            return None
        utc = np.asarray(curve.utc, dtype="datetime64[us]")
        ms = utc.astype("int64") / 1000.0 + offset_min * 60000.0
        ms[np.isnat(utc)] = np.nan
        return None if np.isnan(ms).all() else ms
    values = getattr(curve, key)
    return None if values is None else np.asarray(values, dtype=float)


def _axis_info(key, curve, offset):
    """
    {'key', 'label', 'unit', 'decimals'} of a page x axis; `offset`: the run's UTC offset in min
    (None: unknown); 'decimals': shown with (`units.display_decimals`; None for the clock).
    """
    from .units import display_decimals
    if key == "clock":
        if offset is None:
            unit = "UTC"
        else:
            h, m = divmod(abs(int(offset)), 60)
            unit = "local, UTC%s%02d:%02d" % ("-" if offset < 0 else "+", h, m)
        decimals = None
    else:
        unit = {"time": curve.time_unit, "volume": curve.volume_unit, "cv": "CV"}[key]
        decimals = display_decimals(unit)
    return {"key": key, "label": AXIS_LABELS[key], "unit": unit or "", "decimals": decimals}


def _row_x(row, edge, key, offset_min):
    """A table row's 'start' or 'end' (`edge`) on page axis `key`; None if not known."""
    column = "%s_%s" % (edge, "utc" if key == "clock" else key)
    if column not in row.index:
        return None
    if key == "clock":
        return _epoch_ms(row[column], offset_min)
    value = _cell(row[column])
    return None if value is None else float(value)


def _bars(frame, keys, x_end, offset_min, label=lambda name: name):
    """
    One bar per table row: [{'name', 'pseudo', 'start': {axis: x}, 'end': {axis: x}}], on each
    axis in `keys` the row has a start on. A row with no end runs to the next row's start, the
    last one to `x_end[axis]` (the plot's last x). `label`: the bar's text from the row's name.
    """
    rows = [row for _, row in frame.iterrows()]
    starts = {k: [_row_x(row, "start", k, offset_min) for row in rows] for k in keys}
    bars = []
    for i, row in enumerate(rows):
        bar = {"name": label(_cell(row["name"]) or ""), "pseudo": bool(row.get("pseudo")), "start": {}, "end": {}}
        for k in keys:
            a = starts[k][i]
            if a is None:
                continue
            b = _row_x(row, "end", k, offset_min)
            if b is None:
                b = next((s for s in starts[k][i + 1:] if s is not None), x_end.get(k))
            if b is not None and b > a:
                bar["start"][k], bar["end"][k] = a, b
        if bar["start"]:
            bars.append(bar)
    return bars


INLETS_TITLE = "Inlets"  # the track's lane name (a track, not a table: no export_tables entry)


def _inlet_bars(chrom, series, keys, offset_min):
    """
    The inlets track: one bar per stretch of the run that draws from the same inlets
    (`run_log_sections.inlets_text`: 'A1' at 0 %B; at 100 %B 'B' when line B has no inlet valve
    in the run's system configuration, 'B1 (assumed)' when it has one and the run log never set
    it, 'B (not known)' when the configuration doesn't say; 'A1 -> B1' in a %B ramp (from to to), 'A1+B1' at a constant
    mix, 'A1, Loop' while the loop is injected), in the
    same bar format as `_bars`, plus 'percent_b': {axis: [[x, %B], ...]} for the page to draw
    the %B program across the bar (where %B is known). The stretches come from the executed run
    log's sections (`run_log_sections`, D17); a pause inside a stretch is part of it. A
    stretch's ends on volume, CV and the clock are read from the curves at its method time, as
    `_pauses` does.
    """
    from .run_log_sections import inlets_text, run_log_sections
    from .units import conversion_factor
    curve = next((c for c, _ in series if c.time is not None), None)
    sections = run_log_sections(None, chrom)["sections"] if curve is not None else []
    stretches = []  # [text, elapsed start s, elapsed end s, [(elapsed s, %B), ...]]
    for s in sections:
        text = inlets_text(s, ramp="%s -> %s")
        if not (stretches and stretches[-1][0] == text):
            stretches.append([text, s.elapsed_start_s, s.elapsed_end_s, []])
        stretches[-1][2] = s.elapsed_end_s
        if s.percent_b[0] is not None and s.elapsed_end_s > s.elapsed_start_s:
            stretches[-1][3] += [(s.elapsed_start_s, s.percent_b[0]), (s.elapsed_end_s, s.percent_b_at(s.end_s))]
    times = np.asarray(curve.time, dtype=float) if curve is not None else None
    axes = {}
    for k in keys:
        if k == "time":
            continue
        xs = _curve_axis(curve, k, offset_min)
        keep = ~(np.isnan(times) | np.isnan(xs))
        if keep.any():
            axes[k] = (times[keep], xs[keep])

    def at(key, t):
        if key == "time":
            return t
        return float(np.interp(t, *axes[key]))

    bars = []
    for text, a, b, points in stretches:
        if not text or b <= a:
            continue
        to_unit = conversion_factor("s", curve.time_unit)
        bar = {"name": text, "pseudo": False, "start": {}, "end": {}, "percent_b": {}}
        for k in keys:
            if k == "time" or k in axes:
                lo, hi = at(k, a * to_unit), at(k, b * to_unit)
                if hi > lo:
                    bar["start"][k], bar["end"][k] = lo, hi
                    bar["percent_b"][k] = [[at(k, t * to_unit), min(max(float(v), 0.0), 100.0)] for t, v in points]
        if bar["start"]:
            bars.append(bar)
    return bars


def _pauses(chrom, series, keys, offset_min):
    """
    Where UNICORN's clock stopped (`chrom.timeline`): [{'label', 'at': {axis: x}, 'start', 'end'}].
    On method time, volume and CV a pause is a point ('at'): the stored time and the pumps stop,
    so it sits where the run was when it began. On the clock it lasts from 'start' to 'end'.
    """
    from .units import conversion_factor
    pauses = (chrom.timeline or {}).get("pauses") or []
    curve = next((c for c, _ in series if c.time is not None), None)
    out = []
    for p in pauses:
        how = [] if p.get("quality") == "exact" else [p.get("quality") or "inferred"]
        how += [str(p["cause"])] if p.get("cause") else []
        item = {"label": "pause %.0f s" % (p.get("duration_s") or 0) + (" (%s)" % ", ".join(how) if how else ""),
                "at": {}}
        if curve is not None and p.get("elapsed_s") is not None:
            t = p["elapsed_s"] * conversion_factor("s", curve.time_unit)
            if "time" in keys:
                item["at"]["time"] = t
            times = np.asarray(curve.time, dtype=float)
            for k in ("volume", "cv"):
                if k in keys:
                    xs = _curve_axis(curve, k, 0.0)
                    keep = ~(np.isnan(times) | np.isnan(xs))
                    if keep.any():
                        item["at"][k] = float(np.interp(t, times[keep], xs[keep]))
        if "clock" in keys:
            a, b = _epoch_ms(p.get("start_utc"), offset_min), _epoch_ms(p.get("end_utc"), offset_min)
            if a is not None and b is not None:
                item["start"], item["end"] = a, b
        out.append(item)
    return out


def plot_data(chrom):
    """
    The page's chromatogram plot: the curves the Excel chart draws (`excel_export.chart_series`:
    live UV left, conductivity else %B right, by role), shown at first, then every other live
    curve on the right axis, off at first (`shown`); on every x axis the drawn curves all have
    (`AXES`: clock, method time, volume, CV); the default as the Excel chart (volume, else
    time). Injection starts as marks. Tracks above the curves, as the run-structure figures
    (docs/RUN_STRUCTURE_GUIDE.md): the phase and fraction table rows as bars, start to end, the
    inlets (one bar per stretch with the same inlet positions, `_inlet_bars`), and the pauses,
    so the page can say what ran at any x.

    {'axes': [{'key', 'label', 'unit', 'decimals'}], 'axis': default key, 'curves': [{'name', 'unit',
    'decimals', 'right', 'shown', 'y', 'x': {key: [...]}}], 'marks', 'tracks', 'pauses'}; clock values
    are ms since 1970 on the instrument's local clock (`_epoch_ms`); 'decimals' is what the hover
    label shows (the curve's `AmplitudePrecision`, the axis unit's run-log decimals).
    """
    from .excel_export import _live, _x_axis, chart_series
    from .run_export import event_table_frame
    empty = {"axes": [], "axis": None, "curves": [], "marks": [], "tracks": [], "pauses": []}
    series = chart_series(chrom, [("curves", None)])
    if not series:
        return empty
    offset = (chrom.timeline or {}).get("local_utc_offset")
    offset_min = float(offset) if offset is not None else 0.0
    found = {}
    for key in AXES:
        values = [_curve_axis(curve, key, offset_min) for curve, _ in series]
        if all(v is not None for v in values):
            found[key] = values
    keys = list(found)
    if not keys:
        return empty
    # every other live curve too, switched off at first, on the right axis; one without every
    # axis of the drawn curves is left out (it would take an axis away)
    drawn = [(curve, y2, True, [found[k][i] for k in keys]) for i, (curve, y2) in enumerate(series)]
    names = {curve.name for curve, _ in series}
    only = chrom.only_original()  # W17: an imported or derived chromatogram's curves are all non-original
    for curve in chrom.curves.values():
        if curve.name in names or (only and curve.is_original is False) or not _live(curve):
            continue
        values = [_curve_axis(curve, k, offset_min) for k in keys]
        if all(v is not None for v in values):
            drawn.append((curve, True, False, values))
    curves, x_end = [], {}
    for curve, y2, shown, axes in drawn:
        # one index for every axis, placed along method time when there is one (volume repeats in pauses)
        idx = hybrid_index(axes[keys.index("time") if "time" in keys else 0], curve.values, MAX_POINTS)
        xs = {k: v[idx] for k, v in zip(keys, axes)}
        curves.append({"name": curve.name, "unit": curve.unit or "", "decimals": curve.precision,
                       "right": bool(y2), "shown": shown,
                       "y": _floats(np.asarray(curve.values, dtype=float)[idx]),
                       "x": {k: _floats(v) for k, v in xs.items()}})
        if not shown:
            continue  # the tracks end where the drawn curves end
        for k, v in xs.items():
            if (~np.isnan(v)).any():
                x_end[k] = max(x_end.get(k, -np.inf), float(np.nanmax(v)))
    marks, tracks = [], []
    for sheet, kind in (("phases", "phase"), ("injections", "injection"), ("fractions", "fraction")):
        try:
            frame = event_table_frame(chrom, kind, False)  # no run-log columns: positions only
        except Exception:  # no marks or track rather than no plot
            continue
        if not len(frame):
            continue
        if kind != "injection":
            # phases without the 'Phase ' UNICORN logs before each name (owner, 2026-11-09)
            bars = _bars(frame, keys, x_end, offset_min,
                         (lambda name: PHASE_PREFIX.sub("", name)) if kind == "phase" else (lambda name: name))
            if bars:
                tracks.append({"kind": sheet, "bars": bars})
            continue
        rows = [row for _, row in frame.iterrows() if not bool(row.get("pseudo"))]
        marks.append({"kind": sheet, "labels": ["" if _cell(row["name"]) is None else str(row["name"]) for row in rows],
                      "x": {k: [_row_x(row, "start", k, offset_min) for row in rows] for k in keys}})
    try:
        inlets = _inlet_bars(chrom, series, keys, offset_min)
    except Exception:  # no inlet track rather than no plot
        inlets = []
    if inlets:  # after the phases, before the fractions
        place = 1 if tracks and tracks[0]["kind"] == "phases" else 0
        tracks.insert(place, {"kind": "inlets", "title": INLETS_TITLE, "bars": inlets})
    try:
        pauses = _pauses(chrom, series, keys, offset_min)
    except Exception:  # no pause track rather than no plot
        pauses = []
    default = _x_axis(series)
    return {"axes": [_axis_info(k, series[0][0], offset) for k in keys],
            "axis": default if default in keys else keys[0],
            "curves": curves, "marks": marks, "tracks": tracks, "pauses": pauses}


# Overlaid runs (W11, owner 2026-11-11): at most OVERLAY_MAX runs in one plot. Each run is read once,
# its curves by role thinned to about OVERLAY_POINTS points (`resample.hybrid_index`, up to about
# 1.35 times that) and kept in a small cache of OVERLAY_CACHE runs; the run itself is dropped, so
# Pyodide holds the thinned arrays only (a run with 15 role curves: about 15 x 1350 x 5 x 8 B,
# under 1 MB).
OVERLAY_MAX = 10
OVERLAY_POINTS = 1000
OVERLAY_CACHE = 20


def _x_unit(curve, key):
    """The unit of `curve`'s x on page axis `key` (None for the clock: ms on the run's local clock)."""
    return {"clock": None, "time": curve.time_unit, "volume": curve.volume_unit, "cv": "CV"}[key]


def overlay_entry(run, chromatogram=None):
    """
    What the overlay keeps of `run` (W11): its curves by role (`curve_roles.ROLES` order, live
    curves only, as the plot: original ones on the original chromatogram, every curve on an
    imported or derived one), each thinned to about `OVERLAY_POINTS` points on every
    page axis it has (`AXES`), as numpy arrays; the roles the Excel chart draws
    (`excel_export.chart_series`) as the default. `chromatogram`: the one to use (W17, its name;
    KeyError if the run has none of that name); None: the first original chromatogram with a role curve.
    {'name', 'chromatogram', 'kind', 'label' (`Run.describe_chromatograms`), 'offset' (UTC offset
    in min or None), 'default_roles',
    'curves': [{'role', 'name', 'unit', 'decimals', 'y', 'x': {key: array}, 'x_unit': {key: unit}}]}.
    """
    from .curve_roles import ROLES
    from .excel_export import _live, chart_series
    entry = {"name": run.name, "chromatogram": None, "kind": None, "label": None, "offset": None,
             "default_roles": [], "curves": []}
    described = {d["name"]: d for d in run.describe_chromatograms()}
    keys = run.select_chromatograms([chromatogram] if chromatogram is not None else None)
    for key in keys:
        chrom = run.chromatograms[key]
        entry.update(chromatogram=key, kind=described[key]["kind"], label=described[key]["label"])
        offset = (chrom.timeline or {}).get("local_utc_offset")
        offset_min = float(offset) if offset is not None else 0.0
        curves = []
        for role in ROLES:
            for curve in chrom.curves_with_role(role):
                if not _live(curve):
                    continue
                axes = {k: _curve_axis(curve, k, offset_min) for k in AXES}
                axes = {k: v for k, v in axes.items() if v is not None}
                if not axes:
                    continue
                # one index for every axis, placed along method time when there is one (as plot_data)
                idx = hybrid_index(axes.get("time", next(iter(axes.values()))), curve.values, OVERLAY_POINTS)
                curves.append({"role": role, "name": curve.name, "unit": curve.unit or "", "decimals": curve.precision,
                               "y": np.asarray(curve.values, dtype=float)[idx],
                               "x": {k: v[idx] for k, v in axes.items()},
                               "x_unit": {k: _x_unit(curve, k) for k in axes}})
        if curves:
            series = chart_series(chrom, [("curves", None)])
            roles = [role for role in ROLES if any(c is s for s, _ in series for c in chrom.curves_with_role(role))]
            entry.update(offset=offset, curves=curves, default_roles=roles)
            break
    return entry


def _pick(item):
    """An overlay id (W17): a row index, or [index, chromatogram name or None] -> (index, name or None)."""
    if isinstance(item, (list, tuple)):
        index = int(item[0])
        name = item[1] if len(item) > 1 else None
        return index, (None if name is None else str(name))
    return int(item), None


def _factor(from_unit, to_unit):
    """`units.conversion_factor`, 1.0 for the same unit, None where the two can't be converted."""
    from .units import UnitError, conversion_factor
    if (from_unit or "") == (to_unit or ""):
        return 1.0
    try:
        return conversion_factor(from_unit, to_unit)
    except (UnitError, KeyError, TypeError):
        return None


def overlay_data(entries, roles=None, axis=None):
    """
    The overlay plot of cached runs (`overlay_entry`), `entries` = [(id, entry)] in the page's
    order, an id being a row index or [index, chromatogram name or None] (W17: two chromatograms of
    one run are two entries): the curves of `roles` (None: the first run's default roles) on page axis `axis` (None or
    one no run has: volume when every run has it, else method time, else the first one found).
    Each role's values are converted to the unit of its first curve and each run's x to the
    first run's unit where the units convert (`units.conversion_factor`); a curve that can't be
    converted keeps its unit (the page puts it on its own axis). A run with no curve on the axis,
    or whose x can't be converted, is listed in 'no_axis' and left out.

    {'axes': [{'key', 'label', 'unit', 'decimals'}] (every axis any run has), 'axis', 'roles':
    [{'role', 'curves': [names]}] (every role any run has), 'shown_roles', 'runs': [{'index',
    'pick' ([index, chromatogram asked for or None]), 'name', 'chromatogram' (the one drawn),
    'kind', 'label', 'curves': [{'role', 'name', 'unit', 'decimals', 'x', 'y'}]}],
    'no_axis': [{'index', 'pick', 'name', 'chromatogram'}]}.
    """
    from .curve_roles import ROLES
    from .units import convert_decimals, display_decimals
    entries = [(i, e) for i, e in entries if e["curves"]]
    has = {k: [any(k in c["x"] for c in e["curves"]) for _, e in entries] for k in AXES}
    found = [k for k in AXES if any(has[k])]
    if axis not in found:
        axis = "volume" if has["volume"] and all(has["volume"]) else "time" if any(has["time"]) else \
            (found[0] if found else None)
    roles_found = {}
    for _, e in entries:
        for c in e["curves"]:
            names = roles_found.setdefault(c["role"], [])
            if c["name"] not in names:
                names.append(c["name"])
    if roles is None:
        roles = entries[0][1]["default_roles"] if entries else []
    roles = [r for r in roles if r in roles_found]
    # the units everything is shown in: per axis and per role, the first curve's
    x_units, y_units = {}, {}
    for _, e in entries:
        for c in e["curves"]:
            for k, u in c["x_unit"].items():
                x_units.setdefault(k, u)
            y_units.setdefault(c["role"], c["unit"])
    offsets = {e["offset"] for _, e in entries}
    runs, no_axis = [], []
    for pick, e in entries:
        index, picked = _pick(pick)
        pick = [index, picked]  # W17: what the page asked for, its key for colours and labels
        curves = []
        for c in e["curves"]:
            if c["role"] not in roles or axis not in c["x"]:
                continue
            fx = 1.0 if axis == "clock" else _factor(c["x_unit"][axis], x_units[axis])
            if fx is None:
                continue
            fy = _factor(c["unit"], y_units[c["role"]])
            unit, decimals = (c["unit"], c["decimals"]) if fy is None else \
                (y_units[c["role"]], convert_decimals(c["decimals"], fy) if fy != 1.0 else c["decimals"])
            curves.append({"role": c["role"], "name": c["name"], "unit": unit, "decimals": decimals,
                           "x": _floats(c["x"][axis] * fx), "y": _floats(c["y"] * (fy or 1.0))})
        if axis is not None and not any(axis in c["x"] and (axis == "clock" or _factor(c["x_unit"][axis], x_units[axis])
                                                            is not None) for c in e["curves"]):
            no_axis.append({"index": index, "pick": pick, "name": e["name"], "chromatogram": e["chromatogram"]})
            continue
        runs.append({"index": index, "pick": pick, "name": e["name"], "chromatogram": e["chromatogram"],
                     "kind": e.get("kind"), "label": e.get("label"), "curves": curves})
    axes = []
    for k in found:
        if k == "clock":
            offset = next(iter(offsets)) if len(offsets) == 1 else None
            if offset is None:
                unit = "local" if len(offsets) > 1 else "UTC"
            else:
                h, m = divmod(abs(int(offset)), 60)
                unit = "local, UTC%s%02d:%02d" % ("-" if offset < 0 else "+", h, m)
            axes.append({"key": k, "label": AXIS_LABELS[k], "unit": unit, "decimals": None})
        else:
            unit = x_units.get(k) or ""
            axes.append({"key": k, "label": AXIS_LABELS[k], "unit": unit, "decimals": display_decimals(unit)})
    return {"axes": axes, "axis": axis,
            "roles": [{"role": r, "curves": roles_found[r]} for r in ROLES if r in roles_found],
            "shown_roles": roles, "runs": runs, "no_axis": no_axis}


# Phase window statistics (M22, owner 2026-11-12): the page's table of the ticked runs, the table
# `pycorn export --window ... --window-curves ...` writes (`run_export.phase_windows_frame`, built on
# `window_stats.window_stats_rows`); no default window or curve (D28).
WINDOW_UNITS = ("min", "s", "h", "ml", "L", "ul", "CV")  # the page's unit picker (`window_stats.window_axis`)
WINDOW_CACHE = 40  # (run, chromatogram, windows, curves) results kept


def _ids(ids):
    """Overlay-style ids (row index or [index, chromatogram]), or their JSON text -> [(index, name or None)]."""
    if isinstance(ids, str):
        ids = json.loads(ids) if ids.strip() else []
    return [_pick(i) for i in ids or []]


def _texts(values):
    """A list of texts, or its JSON text (None, '' and blank entries left out)."""
    if isinstance(values, str):
        values = json.loads(values) if values.strip() else []
    return [str(v).strip() for v in values or [] if v is not None and str(v).strip()]


def window_form(text):
    """
    {'window': {'name', 'phase', 'from', 'to', 'unit', 'text'}} of one window text, as
    `window_stats.parse_window` reads it (FROM / TO written back in its syntax: '1', 'end',
    'end-2'), or {'error': the parser's sentence, 'message': `error_message`}.
    """
    from .window_stats import _offset_text, parse_window
    try:
        w = parse_window(text)
    except Exception as e:
        return {"error": plain_error(e), "message": error_message(e)}
    return {"window": {"name": w.name, "phase": w.phase, "from": _offset_text(w.start_anchor, w.start),
                       "to": _offset_text(w.end_anchor, w.end), "unit": w.unit, "text": w.text()}}


class Session:
    """The dropped files and the run shown (one run in memory at a time)."""

    def __init__(self, progress=None):
        import collections
        import pandas as pd
        self.catalog = pd.DataFrame()
        self._shown = None  # (index, run)
        self._overlay = collections.OrderedDict()  # (index, chromatogram) -> overlay_entry (W11), oldest first
        self._chroms = {}  # index -> `file_catalog.file_chromatograms` of rows with several (W17)
        self._all = None  # 'Download all' in progress (export_start)
        self._window_info = {}  # (index, chromatogram) -> phases, curves, roles (M22 form)
        self._windows = collections.OrderedDict()  # (index, chromatogram, windows, curves) -> rows (M22)
        self.progress = progress  # progress(step key, JSON of its values): the page's status line

    def _step(self, key, **values):
        if self.progress is not None:
            self.progress(key, json.dumps(values))

    # -- files ---------------------------------------------------------------

    def add(self, paths):
        """
        Add the result files at `paths` to the catalog. JSON: {'runs': [new rows, as `rows()`],
        'not_read': [[file name, reason, message]]}; `message`: `message()` of the reason.
        """
        import pandas as pd
        from .file_catalog import _BUNDLE_EXT, _FOLDER_EXT, catalog_from_files
        paths = [str(p) for p in paths]
        ok = [p for p in paths if os.path.splitext(p)[1].lower() in _BUNDLE_EXT + _FOLDER_EXT]
        type_text = "not a UNICORN result file (.zip, .Result or .UFol)"
        not_read = [[os.path.basename(p), type_text, message("msg.not_read_type", type_text)]
                    for p in paths if p not in ok]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # the reasons are in attrs['not_read']
            new = catalog_from_files(ok) if ok else pd.DataFrame()
        start = len(self.catalog)
        not_read += [[os.path.basename(p), reason, message("msg.not_read", str(reason), reason=str(reason))]
                     for p, reason in new.attrs.get("not_read", [])]
        if len(new):
            self.catalog = pd.concat([self.catalog, new], ignore_index=True) if start else new.reset_index(drop=True)
        return json.dumps({"runs": self._rows(range(start, len(self.catalog))), "not_read": not_read})

    def rows(self):
        """JSON list of every catalog row: index, file, member, `LIST_COLUMNS` and 'chromatograms' (`_rows`)."""
        return json.dumps(self._rows(range(len(self.catalog))))

    def _rows(self, indexes):
        """
        The rows for the page; 'chromatograms' (W17): for a row with several, one
        `Run.describe_chromatograms` dict each ({'name', 'kind', 'source_result_id',
        'source_result_name', 'copy_of_run', 'label'}, from the light read), else [].
        """
        out = []
        for i in indexes:
            row = self.catalog.iloc[i]
            item = {"index": i, "file": os.path.basename(str(row["path"])), "member": _cell(row.get("member")) or ""}
            item.update({c: _cell(row.get(c)) for c in LIST_COLUMNS})
            item["chromatograms"] = self._chromatogram_list(i)
            out.append(item)
        return out

    def _chromatogram_list(self, index):
        """`file_catalog.file_chromatograms` of row `index` when it has several chromatograms (cached); else []."""
        from .file_catalog import file_chromatograms
        if index not in self._chroms:
            row = self.catalog.iloc[index]
            found = []
            try:
                if int(row.get("n_chromatograms") or 0) > 1:
                    found = [{k: _cell(v) for k, v in d.items()}
                             for d in file_chromatograms(str(row["path"]), row.get("member"))]
            except Exception:  # no picker rather than no row: the run still shows its original chromatogram
                found = []
            self._chroms[index] = found
        return self._chroms[index]

    def clear(self):
        import pandas as pd
        self.catalog = pd.DataFrame()
        self._shown = None
        self._overlay.clear()
        self._chroms.clear()
        self._window_info.clear()
        self._windows.clear()

    # -- overlaid runs (W11) -------------------------------------------------

    def _overlay_entry(self, index, chromatogram=None):
        """
        The cached `overlay_entry` of catalog row `index` and `chromatogram` (W17; None: the
        original one): read once, thinned, the run dropped.
        """
        key = (index, chromatogram)
        if key in self._overlay:
            self._overlay.move_to_end(key)
            return self._overlay[key]
        if self._shown is not None and self._shown[0] == index:
            run = self._shown[1]  # the shown run is in memory already
        else:
            self._step("step.reading_run")
            run = self._load(index)
        entry = overlay_entry(run, chromatogram)
        del run  # only the thinned curves stay
        self._overlay[key] = entry
        while len(self._overlay) > OVERLAY_CACHE:
            self._overlay.popitem(last=False)
        return entry

    def overlay_run(self, index, chromatogram=None):
        """
        Read catalog row `index` (its `chromatogram`, W17; None: the original one) into the
        overlay cache (the page calls this once per run and chromatogram, so it can show
        progress and cancel between them). JSON {'index', 'chromatogram', 'name', 'curves':
        count}, or {'error', 'message'} (`_failed`).
        """
        try:
            name = None if chromatogram is None else str(chromatogram)
            entry = self._overlay_entry(int(index), name)
            return json.dumps({"index": int(index), "chromatogram": entry["chromatogram"], "name": entry["name"],
                               "curves": len(entry["curves"])})
        except Exception as e:
            return _failed(e)

    def overlay(self, ids, roles=None, axis=None):
        """
        JSON of the overlay plot of `ids` (`overlay_data`; at most `OVERLAY_MAX`, the first ones,
        'requested' says how many came), read into the cache where they are not yet. An id is a
        catalog row index (its original chromatogram) or [index, chromatogram name] (W17: the
        ticked chromatograms; two of one run are two ids, each counting towards `OVERLAY_MAX`).
        'failed': [{'index', 'pick', 'message'}] for ids that can't be read (left out, the rest drawn).
        """
        try:
            if isinstance(ids, str):  # the page sends its ids as JSON text
                ids = json.loads(ids)
            picks = [_pick(i) for i in ids]
            entries, failed = [], []
            for index, chromatogram in picks[:OVERLAY_MAX]:
                try:
                    entries.append(((index, chromatogram), self._overlay_entry(index, chromatogram)))
                except Exception as e:
                    failed.append({"index": index, "pick": [index, chromatogram], "message": error_message(e)})
            ids = picks
            out = overlay_data(entries, None if roles is None else [str(r) for r in roles], axis)
            out.update(requested=len(ids), max=OVERLAY_MAX, failed=failed)
            return json.dumps(out)
        except Exception as e:
            return _failed(e)

    # -- phase window statistics (M22) ----------------------------------------

    def _run(self, index):
        """The run of row `index`: the shown one when it is that row (in memory already), else read."""
        if self._shown is not None and self._shown[0] == index:
            return self._shown[1]
        self._step("step.reading_run")
        return self._load(index)

    def parse_window(self, text):
        """JSON of `window_form(text)`: the window form's fields, or the parser's error."""
        return json.dumps(window_form(text))

    def window_info(self, ids):
        """
        JSON of what the window form offers for `ids` (as `overlay`: row indexes or [index,
        chromatogram name or None], or their JSON text): {'phases': the phase names (phase table
        rows, 'Phase ' left out; in run order, each once across the runs), 'curves': curve names,
        'roles': the curve roles any of them has (`curve_roles.ROLES` order), 'units':
        `WINDOW_UNITS`, 'failed': [{'index', 'pick', 'message'}]}, or {'error', 'message'}.
        """
        from .curve_roles import ROLES
        from .window_stats import _phase_key
        try:
            phases, seen, curves, roles, failed = [], set(), [], set(), []
            for index, chromatogram in _ids(ids):
                try:
                    info = self._window_info_of(index, chromatogram)
                except Exception as e:
                    failed.append({"index": index, "pick": [index, chromatogram], "message": error_message(e)})
                    continue
                for name in info["phases"]:
                    if _phase_key(name) not in seen:
                        seen.add(_phase_key(name))
                        phases.append(name)
                curves += [c for c in info["curves"] if c not in curves]
                roles.update(info["roles"])
            return json.dumps({"phases": phases, "curves": curves, "roles": [r for r in ROLES if r in roles],
                               "units": list(WINDOW_UNITS), "failed": failed})
        except Exception as e:
            return _failed(e)

    def _window_info_of(self, index, chromatogram):
        """Phases, curve names and roles of row `index`'s `chromatogram` (None: original one(s)), cached."""
        key = (index, chromatogram)
        if key not in self._window_info:
            from .curve_roles import ROLES, curves_with_role
            from .event_tables import phase_rows
            run = self._run(index)
            phases, curves, roles = [], [], set()
            for name in run.select_chromatograms([chromatogram] if chromatogram is not None else None):
                chrom = run.chromatograms[name]
                phases += [PHASE_PREFIX.sub("", str(r["name"])) for r in phase_rows(chrom) if not r["pseudo"]]
                curves += [c for c in chrom.curves if c not in curves]
                roles.update(r for r in ROLES if curves_with_role(chrom, r))
            self._window_info[key] = {"phases": phases, "curves": curves, "roles": sorted(roles)}
        return self._window_info[key]

    def window_stats(self, ids, windows, curves, labels=None):
        """
        JSON of the phase window statistics of `ids` (as `window_info`; an id's chromatogram None:
        the run's original one(s)) in one table: each run's `run_export.phase_windows_frame`
        (`window_stats.window_stats_rows`: the table `pycorn export --window ... --window-curves
        ...` writes, same columns and order), {'table': `table(frame, 'phase_windows')`,
        'warnings': ['<run>: <note>'] (a phase or curve a run lacks, clipping; the interim rule
        left out: 'interim' holds it once), 'interim': `window_stats.INTERIM_RULE`, 'failed':
        [{'index', 'pick', 'message'}]}, or {'error', 'message'} (a window that does not parse; no
        window or no curve: there is no default, D28). `windows`: 'NAME=PHASE:FROM:TO UNIT'
        texts; `curves`: roles and / or curve names; both lists or their JSON text. `labels`:
        {row index: name} for the run column (the page names runs as its overlay legend does);
        None: the run's name. Results are kept per run, chromatogram, windows and curves.
        """
        import pandas as pd
        from .window_stats import COLUMNS, INTERIM_RULE, SYNTAX, parse_windows
        try:
            windows, curves = _texts(windows), _texts(curves)
            if not windows:
                raise ValueError("no window given (no default window): %s" % SYNTAX)
            parse_windows(windows)  # a window that does not parse: the parser's sentence
            if not curves:
                raise ValueError("no curve given (no default curves): give curve names or roles")
            if isinstance(labels, str):
                labels = json.loads(labels) if labels.strip() else None
            labels = {str(k): str(v) for k, v in (labels or {}).items() if v is not None}
            frames, notes, failed = [], [], []
            for index, chromatogram in _ids(ids):
                try:
                    name, found, problems = self._window_rows(index, chromatogram, windows, curves)
                except Exception as e:
                    failed.append({"index": index, "pick": [index, chromatogram], "message": error_message(e)})
                    continue
                label = labels.get(str(index))
                for frame in found:
                    if label:
                        frame = frame.copy()
                        frame["run"] = label
                    frames.append(frame)
                for text in problems:
                    line = "%s: %s" % (label or name, text)
                    if text != INTERIM_RULE and line not in notes:
                        notes.append(line)
            frame = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=COLUMNS)
            return json.dumps({"table": table(frame, "phase_windows"), "warnings": notes, "interim": INTERIM_RULE,
                               "failed": failed})
        except Exception as e:
            return _failed(e)

    def _window_rows(self, index, chromatogram, windows, curves):
        """(run name, [frame per chromatogram with rows], [note]) of row `index` (cached, `WINDOW_CACHE`)."""
        from .run_export import ExportOptions, phase_windows_frame
        key = (index, chromatogram, tuple(windows), tuple(curves))
        if key in self._windows:
            self._windows.move_to_end(key)
            return self._windows[key]
        run = self._run(index)
        options = ExportOptions(windows=list(windows), window_curves=list(curves))
        frames, problems = [], []
        for name in run.select_chromatograms([chromatogram] if chromatogram is not None else None):
            frame = phase_windows_frame(run, run.chromatograms[name], options, problems)
            if frame is not None:
                frames.append(frame)
        entry = (run.name, frames, [text for _, _, text in problems])
        self._windows[key] = entry
        while len(self._windows) > WINDOW_CACHE:
            self._windows.popitem(last=False)
        return entry

    def _load(self, index):
        """The run of catalog row `index`, freshly read (ValueError with the reason if it can't be)."""
        from .file_catalog import load_files
        if not 0 <= index < len(self.catalog):
            raise Message("msg.no_run", "no run %d" % index, index=index)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            runs = list(load_files(self.catalog.iloc[[index]], strict=True))
        return runs[0]

    # -- view ----------------------------------------------------------------

    def view(self, index, chromatograms=None):
        """
        JSON of what the page shows for catalog row `index`: {'name', 'variables', 'warnings',
        'chromatograms': [{'name', 'kind', 'label', 'source_result_name', 'copy_of_run', 'summary',
        'plot', 'phases', 'injections', 'fractions', 'pools', 'peaks', 'peak_summary', 'hint_blocks',
        'hint_injections'}], 'all_chromatograms': `Run.describe_chromatograms`, 'details': {run-level
        table: ...}}, or {'error': plain sentence, 'message': `message()`}. `chromatograms` (W17;
        a list of names or its JSON text): the ones shown; None, or none that the run has: its
        original chromatogram(s).
        """
        try:
            if isinstance(chromatograms, str):
                chromatograms = json.loads(chromatograms) if chromatograms.strip() else None
            if chromatograms is not None:
                chromatograms = [str(c) for c in chromatograms] or None
            return json.dumps(self._view(index, chromatograms))
        except Exception as e:
            return _failed(e)

    def _view(self, index, chromatograms=None):
        from .column_geometry import column_geometry
        from .excel_export import summary_rows
        from .export_tables import title
        from .run_export import (TABLES, _built, details_frames, event_table_frame, hint_frames, other_run_notes,
                                 peak_summary_frame, peaks_frame, run_info_frame, warnings_frame)
        from .run_log_sections import run_log_sections
        self._step("step.reading_run")
        run = self._load(index)
        self._shown = (index, run)
        problems = []
        geometry = _built("column geometry", "", lambda: column_geometry(run), problems)
        if geometry is not None:  # as the export: 'no column ...' only when a column type is known
            problems += [("", "geometry", w) for w in geometry.warnings
                         if geometry.name is not None or not w.startswith("no column ")]
        chroms = []
        described = {d["name"]: d for d in run.describe_chromatograms()}
        picked = run.select_chromatograms(chromatograms, strict=False) or run.select_chromatograms()
        for key in picked:
            chrom = run.chromatograms[key]
            self._step("step.tables", chromatogram=chrom.name)
            rls = _built("sections", chrom.name, lambda: run_log_sections(run, chrom), problems)
            if rls is not None:
                problems += [(chrom.name, "sections", w) for w in rls["warnings"]]
            problems += [(chrom.name, "curves", w) for w in other_run_notes(chrom)]
            built = rls if rls is not None else False  # failed sections: the table, its run-log columns empty
            tables = {sheet: table(_built(sheet, chrom.name,
                                          lambda: event_table_frame(chrom, kind, built, problems), problems), sheet)
                      for sheet, kind in TABLES}
            hints = _built("method hints", chrom.name,
                           lambda: hint_frames(run, chrom, rls["sections"] if rls else None, problems), problems)
            tables["peaks"] = table(_built("peaks", chrom.name, lambda: peaks_frame(chrom), problems), "peaks")
            tables["peak_summary"] = table(_built("peak table summary", chrom.name,
                                                  lambda: peak_summary_frame(chrom, run, problems), problems),
                                           "peak_summary")
            tables["hint_blocks"], tables["hint_injections"] =(table(h, k) for h, k in
                                                                zip(hints or (None, None), ("hint_blocks",
                                                                                            "hint_injections")))
            self._step("step.plot", chromatogram=chrom.name)
            d = described[key]
            chroms.append({"name": key, "kind": d["kind"], "label": d["label"],
                           "source_result_name": d["source_result_name"], "copy_of_run": d["copy_of_run"],
                           "plot": _built("plot", chrom.name, lambda: plot_data(chrom), problems), **tables})
        self._step("step.details")
        from .run_details import method_variables_frame
        variables = _built("variables", "", lambda: method_variables_frame(run), problems)
        details = details_frames(run, problems)
        info = _built("run_info", "", lambda: run_info_frame(run, geometry, chromatograms=picked), problems)
        warns = warnings_frame(run, problems)
        run_frames = ([("run_info", info)] if info is not None else []) + [("warnings", warns)]
        for item, chrom in zip(chroms, (run.chromatograms[k] for k in picked)):
            # the page has a warnings list, not a sheet
            sheet = "the %s sheet" % title("warnings")
            item["summary"] = [[label, _cell(value).replace(sheet, "the %s below" % title("warnings"))
                                if label == "Warnings" else _cell(value)]
                               for label, value in summary_rows(run, chrom, [], run_frames) if label != "Exported by"]
        return {"name": run.name, "chromatograms": chroms, "all_chromatograms": list(described.values()),
                "variables": table(variables if variables is not None else run.variables(), "variables"),
                "details": {name: table(frame, name) for name, frame in details},
                "warnings": table(warns, "warnings")}

    # -- downloads -----------------------------------------------------------

    def download(self, index, kind, folder, options=None):
        """
        Write the `kind` download of catalog row `index` into `folder` (emptied first). JSON:
        {'path', 'name', 'warnings': [text]}, or {'error': plain sentence, 'message': `message()`}.
        'xlsx': a zip `<run>_xlsx.zip` of the workbook(s) and README.txt (W4); 'csv': a zip
        `<run>_csv.zip` of the csv files, README.txt and provenance.json; 'core': the CADET-Core
        JSON (a zip with the experiment's other files when there are several), its warnings
        starting with `CADET_EXPERIMENTAL_NOTE`. `options` (JSON text or dict): for 'xlsx' and
        'csv', mask (true: people, computer, instrument names and serials masked,
        `run_details.Masker`), curves (the names ticked on the plot, `export_options`), sample;
        for 'core', version ('5' / '6'), bed_height / diameter / particle_diameter as [value,
        unit], overrides (CADET-Core's own layout, D28); for all three, chromatograms (W17: the
        names ticked; None: the run's original chromatogram(s)).
        """
        try:
            if kind not in KINDS:
                raise Message("msg.download_unknown", "unknown download %r; known: %s" % (kind, ", ".join(KINDS)),
                              kind=str(kind), known=", ".join(KINDS))
            if isinstance(options, str):
                options = json.loads(options) if options.strip() else {}
            if os.path.isdir(folder):
                shutil.rmtree(folder)
            os.makedirs(folder)
            self._step("step.reading_run")
            run = self._load(index)  # fresh: caller geometry changes a run's axes
            return json.dumps(getattr(self, "_" + kind)(run, folder, options or {}))
        except Exception as e:
            return _failed(e)

    def _export(self, run, folder, fmt, options):
        """The tables of `pycorn export`, with the method hints the page shows; `options`: `export_options`."""
        from .run_export import clean_name, write_run
        target = os.path.join(folder, clean_name(run.name))
        self._step("step.writing", kind=fmt)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            written = write_run(run, target, formats=(fmt,), options=export_options(options, WEB_ROUTE))
        return target, written

    # -- download all --------------------------------------------------------
    # One call per run, so the page can say how far it is: export_start, export_one per row,
    # export_finish. Run folders as `pycorn export` lays them out (`run_export.run_folders`).

    def export_start(self, ids=None, curves=None, kind=None, folder=None, options=None):
        """
        Start a download of the runs `ids` (catalog row indexes: the runs ticked on the page; None:
        every run in the list) as `kind` ('xlsx' or 'csv') into `folder` (emptied first): one folder
        per run, as `pycorn export` writes it (xlsx: the workbook and README.txt), and index.csv.
        `curves`: the curve names ticked on the plot (W1; None: every curve; same as
        `options['curves']`); `options`: as `download` (mask, sample; chromatograms (W17): a list
        for every run, or {row index: [names]} per run, a run not in it with its original
        chromatogram(s)). `kind` and `folder` are
        required (keywords, so the contract `export_start(ids, curves=[names], ...)` holds).
        JSON {'runs': n, 'ids': [index, ...]} or {'error', 'message'}; then `export_one(i)` for
        each id and `export_finish()`.
        """
        from .run_export import run_folders
        try:
            if kind not in ("xlsx", "csv"):
                raise Message("msg.all_kind", "download all: pick xlsx or csv, not %r" % (kind,), kind=str(kind))
            if folder is None:
                raise ValueError("export_start needs the folder to write into")
            if isinstance(options, str):
                options = json.loads(options) if options.strip() else {}
            options = dict(options or {})
            if curves is not None:
                options["curves"] = list(curves)
            ids = list(range(len(self.catalog))) if ids is None else [int(i) for i in ids]
            for i in ids:
                if not 0 <= i < len(self.catalog):
                    raise Message("msg.no_run", "no run %d" % i, index=i)
            export_options(options, WEB_ROUTE)  # a bad size is reported before any run is written
            if os.path.isdir(folder):
                shutil.rmtree(folder)
            root = os.path.join(folder, ALL_FOLDER)
            os.makedirs(root)
            labels = list(self.catalog["source"]) if "source" in self.catalog.columns else ["zip"] * len(self.catalog)
            masker = None
            if options.get("mask"):  # index.csv masked as the files are (`run_export.mask_index`), as pycorn export
                from .json_output import PeopleMasker, learn_catalog
                masker = PeopleMasker()
                learn_catalog(self.catalog.iloc[ids], masker)
            self._all = {"kind": kind, "options": options, "root": root, "rows": {}, "ids": ids, "masker": masker,
                         "folders": run_folders(self.catalog, labels)}
            return json.dumps({"runs": len(ids), "ids": ids})
        except Exception as e:
            return _failed(e)

    def export_one(self, index):
        """
        Write catalog row `index` into the download folder. JSON {'name', 'status', 'error', 'message'};
        `message`: `message()` of the error, None when written. Status as `pycorn export` gives it
        (A5): 'written', 'incomplete', 'failed' (not written) or 'not loaded' (not read).
        """
        from .run_export import export_error, missing_outputs, write_run
        state = self._all
        row = self.catalog.iloc[index]
        entry = {"run": row.get("name"), "export_status": "written", "export_files": "", "error": ""}
        failure = None
        try:
            self._step("step.reading_run")
            run = self._load(index)
        except Exception as e:
            entry["export_status"], entry["error"] = "not loaded", export_error(e)
            failure = error_message(e)
        else:
            if state.get("masker") is not None:
                state["masker"].absorb(run)
            try:
                self._step("step.writing", kind=state["kind"])
                options = dict(state["options"])
                per_run = options.get("chromatograms")
                if isinstance(per_run, dict):  # W17: {row index: [ticked names]}; a run not in it: its original one
                    options["chromatograms"] = per_run.get(str(index), per_run.get(index))
                options = export_options(options, WEB_ROUTE + ", download all")
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    written = write_run(run, os.path.join(state["root"], str(state["folders"][index])),
                                        formats=(state["kind"],), options=options)
                entry["export_files"] = ";".join(written)
                missing = missing_outputs(run, options, written)
                if missing:
                    entry["export_status"], entry["error"] = "incomplete", export_error(missing=missing)
            except Exception as e:
                entry["export_status"], entry["error"] = "failed", export_error(e)
                failure = error_message(e)
        state["rows"][index] = entry
        return json.dumps({"name": _cell(entry["run"]), "status": entry["export_status"], "error": entry["error"],
                           "message": failure})

    def export_finish(self):
        """
        Zip the 'Download all' folder, with index.csv (A5: the columns of `pycorn export`'s,
        `run_export.index_frame`: export_status, export_folder, export_files, export_error,
        export_changed, export_stamp, then the file catalog's; one row per run asked for, in that order) and
        options.json (`ExportOptions.to_dict()` of the page's choices, readable by `pycorn export
        --options`). JSON {'path', 'name', 'warnings': [runs not written], 'messages': [`message()` per
        run not written, key 'msg.run_failed']} or {'error', 'message'}. `name` is `PyCORN_export_<kind>.zip`.
        """
        from .run_export import CSV_ENCODING, INDEX_FILE, index_frame, mask_index, write_options_file
        try:
            state = self._all
            root = state["root"]
            self._step("step.zipping")
            ids = [i for i in state["ids"] if i in state["rows"]]
            rows = [state["rows"][i] for i in ids]
            # the run files hold no export stamp (as before A5): export_changed and export_stamp stay empty
            index = index_frame(self.catalog.iloc[ids], [r["export_status"] for r in rows],
                                [str(state["folders"][i]).replace("\\", "/") for i in ids],
                                [r["export_files"] for r in rows], [""] * len(ids), [None] * len(ids),
                                [r["error"] for r in rows])
            if state.get("masker") is not None:  # the catalog's people columns, as the files hold them: masked
                index = mask_index(index, state["masker"])
            index.to_csv(os.path.join(root, INDEX_FILE), index=False, encoding=CSV_ENCODING)
            shared = dict(state["options"])
            if isinstance(shared.get("chromatograms"), dict):  # chosen per run: not one value for options.json
                shared.pop("chromatograms")
            write_options_file(root, export_options(shared, WEB_ROUTE))
            name = "%s_%s.zip" % (ALL_FOLDER, state["kind"])
            path = os.path.join(os.path.dirname(root), name)
            with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
                for here, _, files in os.walk(root):
                    for f in files:
                        full = os.path.join(here, f)
                        z.write(full, os.path.relpath(full, os.path.dirname(root)).replace("\\", "/"))
            bad = [r for r in rows if r["export_status"] != "written"]
            failed = ["%s: %s" % (r["run"], r["error"]) for r in bad]
            messages = [message("msg.run_failed", text, run=str(r["run"]), error=str(r["error"]))
                        for r, text in zip(bad, failed)]
            return json.dumps({"path": path, "name": name, "warnings": failed, "messages": messages})
        except Exception as e:
            return _failed(e)

    def _xlsx(self, run, folder, options):
        """W4: a small zip `<run>_xlsx.zip` of the workbook(s) and README.txt, as the all-runs folder holds them."""
        target, written = self._export(run, folder, "xlsx", options)
        self._step("step.zipping")
        return _zipped(target, written, os.path.basename(target) + "_xlsx.zip")

    def _csv(self, run, folder, options):
        target, written = self._export(run, folder, "csv", options)
        self._step("step.zipping")
        return _zipped(target, written, os.path.basename(target) + "_csv.zip")

    def _core(self, run, folder, options):
        from .exporters.cadet.api import export_cadet
        from .run_export import clean_name
        version = str(options.get("version") or "")
        if version not in CORE_VERSIONS:
            raise Message("msg.core_version", "pick the CADET-Core version: 5 or 6")
        kwargs = {}
        for name in CORE_QUANTITIES:
            given = options.get(name)
            if given and given[0] not in (None, ""):
                kwargs[name] = (float(given[0]), given[1])
        target = os.path.join(folder, clean_name(run.name) + "_cadet_core")
        self._step("step.writing_core")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = export_cadet(run, target, core_version=version, cadet_process=False,
                                  chromatogram=[str(c) for c in options.get("chromatograms") or []] or None,
                                  core_overrides=options.get("overrides") or None, route=WEB_ROUTE, **kwargs)
        names = [os.path.basename(p) for p in result["written"]]
        if len(names) == 1:
            return {"path": result["written"][0], "name": names[0], "warnings": result["warnings"]}
        out = _zipped(target, names, os.path.basename(target) + ".zip")
        out["warnings"] = result["warnings"]
        return out


def _zipped(folder, names, zip_name):
    """{'path', 'name', 'warnings'} of a zip of `names` in `folder`, under the folder's name."""
    path = os.path.join(os.path.dirname(folder), zip_name)
    top = os.path.basename(folder)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for name in names:
            z.write(os.path.join(folder, name), top + "/" + name)
    return {"path": path, "name": zip_name, "warnings": []}
