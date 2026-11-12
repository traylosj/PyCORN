# -*- coding: utf-8 -*-
"""
Curve statistics over windows relative to a phase (M22; owner, 2026-11-10 and 2026-11-11).

A window is named by the caller and given relative to a phase: the phase name, a start and an
end offset, and a unit. The unit sets the axis: a time unit (s, min, h) the time axis, a volume
unit (ml, L, ul) the volume axis, 'CV' the column-volume axis. Each offset counts from the
phase start, or from the phase end when written with 'end' (`parse_window`):

    load=Sample Application:1:3 CV        1 to 3 CV after the phase start
    tail=Elution:end-2:end CV             the last 2 CV of the phase
    whole=Column Wash:0:end min           the whole phase
    first=Equilibration:start:start+5 ml  the first 5 ml ('start' is the same as 0)

There is no default window, phase or curve (D28): the caller names them all.

Two functions do the work, shared with later analyses (the UV baseline):

- `phase_windows(run, windows, chrom=None)`: each window resolved to absolute ranges, one per
  occurrence of its phase, on the run's time, volume and CV axes.
- `window_stats(curve, window)`: n, mean, median, SD, min and max of the curve's points inside
  one resolved window (or a plain {'axis', 'start', 'end', 'unit'} window on the curve's axes).

`window_stats_rows(run, chrom, windows, curves)` puts them together: one row per phase
occurrence x window x curve (`COLUMNS`), the table `pycorn export --window` writes.

Rules:

- Phases are the phase table's rows (`pycorn.event_tables.phase_rows`), start and end as
  logged; the row before the first phase is not a phase. INTERIM (until the owner decides the
  lead-in and repeats questions): no lead-in folding, and a phase that runs several times gives
  one window per occurrence, numbered from 1 in time order.
- A phase name matches with case, repeated spaces and a leading 'Phase ' ignored ('Sample
  Application' matches 'Phase Sample Application').
- A window is clipped to its phase (start and end as logged; the last phase ends at the run
  end), with a warning. A window that is empty after clipping gives n = 0 and no statistics.
- Each curve is read on its own axes and its own points (curves are sampled on different
  grids); the bounds are inclusive. NaN points are left out and do not count in n. A curve
  without the window's axis is read on another axis the window was resolved on (warned).
- A CV window in a run without Vc (no CV axis): a warning, no rows.
- Values are never rounded here (display only: `pycorn.display_precision`). SD is the sample
  standard deviation (n - 1), empty for n < 2.

`STATISTICS` lists the statistics in column order; each takes (x, y): the curve's points
inside the window on the window's axis and their values, so statistics that need the axis
(slope, start and end value) can be added later without changing the callers.
"""
import re
from collections import OrderedDict
from dataclasses import dataclass

import numpy as np

AXES = ("time", "volume", "cv")
_NUMBER = r"[0-9]*\.?[0-9]+(?:[eE][+-]?[0-9]+)?"
_OFFSET = re.compile(r"^(?:(?P<anchor>start|end)\s*(?:(?P<sign>[+-])\s*(?P<amount>%s))?|(?P<plain>[+-]?%s))$"
                     % (_NUMBER, _NUMBER), re.IGNORECASE)
EXAMPLE = "load=Sample Application:1:3 CV"
SYNTAX = ("NAME=PHASE:FROM:TO UNIT, FROM and TO a number (from the phase start) or end, end-N, end+N "
          "(from the phase end); UNIT a time unit (s, min, h), a volume unit (ml, L) or CV; e.g. '%s', "
          "'tail=Elution:end-2:end CV'" % EXAMPLE)

INTERIM_RULE = ("INTERIM phase rule (until the owner decides lead-in and repeats): phase start and end as "
                "logged, no lead-in folding; a phase that runs several times gives one row per occurrence, "
                "numbered from 1")

COLUMNS = ["run", "chromatogram", "window", "phase", "occurrence", "phase_number", "axis", "window_unit",
           "start_anchor", "start_offset", "end_anchor", "end_offset", "start_time", "end_time", "start_volume",
           "end_volume", "start_cv", "end_cv", "time_unit", "volume_unit", "curve", "curve_unit", "curve_precision",
           "n", "mean", "median", "sd", "min", "max", "warnings"]


def _sd(x, y):
    return float(np.std(y, ddof=1)) if len(y) > 1 else None


# statistic -> f(x, y) on the points inside the window (x on the window's axis); column order
STATISTICS = OrderedDict([
    ("mean", lambda x, y: float(np.mean(y))),
    ("median", lambda x, y: float(np.median(y))),
    ("sd", _sd),
    ("min", lambda x, y: float(np.min(y))),
    ("max", lambda x, y: float(np.max(y))),
])


@dataclass(frozen=True)
class Window:
    """A window relative to a phase, as the caller gives it. Offsets count from `*_anchor` ('start' or 'end')."""
    name: str
    phase: str
    start: float
    end: float
    unit: str
    start_anchor: str = "start"
    end_anchor: str = "start"

    @property
    def axis(self):
        return window_axis(self.unit)

    def text(self):
        """The window in `parse_window`'s syntax."""
        return "%s=%s:%s:%s %s" % (self.name, self.phase, _offset_text(self.start_anchor, self.start),
                                   _offset_text(self.end_anchor, self.end), self.unit)


def _offset_text(anchor, value):
    if anchor == "start":
        return "%g" % value
    return "end" if value == 0 else "end%+g" % value


def window_axis(unit):
    """'time', 'volume' or 'cv' for a window unit; ValueError for any other unit."""
    from .units import quantity
    if isinstance(unit, str) and unit.strip().upper() == "CV":
        return "cv"
    found = quantity(unit)
    if found in ("time", "volume"):
        return found
    raise ValueError("window unit %r: give a time unit (s, min, h), a volume unit (ml, L) or CV" % (unit,))


def _offset(text, spec):
    m = _OFFSET.match(text.strip())
    if not m:
        raise ValueError("window %r: offset %r is not a number, end, end-N or end+N (%s)" % (spec, text, SYNTAX))
    if m.group("plain") is not None:
        return "start", float(m.group("plain"))
    amount = float(m.group("amount") or 0.0)
    return m.group("anchor").lower(), -amount if m.group("sign") == "-" else amount


def parse_window(text):
    """
    A `Window` from 'NAME=PHASE:FROM:TO UNIT' ('load=Sample Application:1:3 CV'). FROM and TO
    are numbers counted from the phase start, or 'end', 'end-N', 'end+N' counted from the phase
    end ('start', 'start+N' also count from the start). The phase name may hold ':' and spaces.
    """
    spec = (text or "").strip()
    name, sep, rest = spec.partition("=")
    if not sep or not name.strip():
        raise ValueError("window %r: give %s" % (spec, SYNTAX))
    body, _, unit = rest.strip().rpartition(" ")
    parts = body.rsplit(":", 2)
    if not unit or len(parts) != 3 or not parts[0].strip():
        raise ValueError("window %r: give %s" % (spec, SYNTAX))
    window_axis(unit)  # checks the unit
    unit = "CV" if unit.upper() == "CV" else unit
    start_anchor, start = _offset(parts[1], spec)
    end_anchor, end = _offset(parts[2], spec)
    if start_anchor == end_anchor and start >= end:
        raise ValueError("window %r: FROM must come before TO" % spec)
    return Window(name.strip(), parts[0].strip(), start, end, unit, start_anchor, end_anchor)


def parse_windows(texts):
    """`parse_window` for each text; ValueError when two windows have the same name."""
    windows = [w if isinstance(w, Window) else parse_window(w) for w in texts or []]
    names = [w.name for w in windows]
    twice = sorted({n for n in names if names.count(n) > 1})
    if twice:
        raise ValueError("window name(s) given twice: %s" % ", ".join(twice))
    return windows


def _phase_key(name):
    key = re.sub(r"\s+", " ", (name or "").strip()).casefold()
    return key[len("phase "):] if key.startswith("phase ") else key


def _axis_unit(axis, row):
    return row["time_unit"] if axis == "time" else row["volume_unit"] if axis == "volume" else "CV"


def _reference_axes(chrom):
    """{axis: float array} of the curve with the most points that has time and volume (CV if any), for mapping."""
    best = None
    for curve in chrom.curves.values():
        if curve.time is None or curve.volume is None or curve.values is None or len(curve.time) < 2:
            continue
        score = (curve.cv is not None, len(curve.time))
        if best is None or score > best[0]:
            best = (score, curve)
    if best is None:
        return None
    curve = best[1]
    out = {"time": np.asarray(curve.time, dtype=float), "volume": np.asarray(curve.volume, dtype=float),
           "units": {"time": curve.time_unit, "volume": curve.volume_unit}}
    if curve.cv is not None:
        out["cv"] = np.asarray(curve.cv, dtype=float)
    return out


def _map(ref, from_axis, value, to_axis, row):
    """`value` on `from_axis` (in the row's unit) on `to_axis` (in the row's unit), by the reference curve."""
    from .units import conversion_factor
    if ref is None or from_axis not in ref or to_axis not in ref:
        return None
    try:
        f_in = 1.0 if from_axis == "cv" else conversion_factor(_axis_unit(from_axis, row), ref["units"][from_axis])
        f_out = 1.0 if to_axis == "cv" else conversion_factor(ref["units"][to_axis], _axis_unit(to_axis, row))
    except Exception:
        return None
    x, y = ref[from_axis], ref[to_axis]
    ok = np.isfinite(x) & np.isfinite(y)
    x, y = x[ok], y[ok]
    order = np.argsort(x, kind="stable")
    x, first = np.unique(x[order], return_index=True)  # first point at each value (CV is flat in by-pass)
    y = y[order][first]
    if len(x) < 2:
        return None
    return float(np.interp(value * f_in, x, y)) * f_out


def phase_windows(run, windows, chrom=None):
    """
    (resolved, warnings): each window of `windows` (`Window` objects or `parse_window` texts)
    resolved for every occurrence of its phase in `chrom` (default: the run's first
    chromatogram). Each resolved window is a dict: `window` (the `Window`), `phase` (as logged),
    `occurrence` (from 1), `phase_number` (its row in the phase table), `axis`,
    `start_<axis>` / `end_<axis>` on every axis (None where the run has no such axis),
    `time_unit`, `volume_unit` and `warnings` (clipping). A phase missing from the run, or a CV
    window without Vc, gives a warning and no window.
    """
    from .event_tables import phase_rows
    from .units import conversion_factor
    chrom = chrom if chrom is not None else run.chromatogram()
    windows = parse_windows(windows)
    phases = [r for r in phase_rows(chrom) if not r["pseudo"]]
    ref = _reference_axes(chrom)
    resolved, warnings = [], []
    source = chrom.run_log_source()  # W17: an imported chromatogram's phases are its source run's
    if source is not None and windows:
        from .model import imported_note
        warnings.append(imported_note(source))
    for window in windows:
        axis = window.axis
        hits = [r for r in phases if _phase_key(r["name"]) == _phase_key(window.phase)]
        if not hits:
            warnings.append("window %s: no phase %r in %s (phases: %s); no rows"
                            % (window.name, window.phase, chrom.name,
                               ", ".join(OrderedDict.fromkeys(r["name"] for r in phases)) or "none"))
            continue
        for occurrence, row in enumerate(hits, 1):
            p0, p1 = row["start_" + axis], row["end_" + axis]
            where = "window %s, %s #%d" % (window.name, row["name"], occurrence)
            if p0 is None or p1 is None:
                why = "no column volume (Vc), so no CV axis" if axis == "cv" else "no %s axis" % axis
                warnings.append("%s: %s; no rows" % (where, why))
                continue
            factor = 1.0
            if axis != "cv":
                try:
                    factor = conversion_factor(window.unit, _axis_unit(axis, row))
                except Exception as e:
                    warnings.append("%s: %s; no rows" % (where, e))
                    continue
            lo = (p0 if window.start_anchor == "start" else p1) + window.start * factor
            hi = (p0 if window.end_anchor == "start" else p1) + window.end * factor
            notes = []
            unit = _axis_unit(axis, row)
            if lo < p0:
                notes.append("starts %g %s before the phase start: clipped to it (no lead-in, interim rule)"
                             % (p0 - lo, unit))
                lo = p0
            if hi > p1:
                end = "the run end" if row["end_source"] == "last_log_event" else "the phase end"
                notes.append("ends %g %s after %s: clipped to it" % (hi - p1, unit, end))
                hi = p1
            if lo > hi:
                notes.append("empty: starts after it ends once placed in the phase (%g to %g %s)" % (lo, hi, unit))
            out = OrderedDict([("window", window), ("phase", row["name"]), ("occurrence", occurrence),
                               ("phase_number", row["number"]), ("axis", axis)])
            for edge, value in (("start", lo), ("end", hi)):
                for other in AXES:
                    if other == axis:
                        found = value
                    elif value in (p0, p1):  # at the phase start or end: the logged value on every axis
                        found = row[("start_" if value == p0 else "end_") + other]
                    else:
                        found = _map(ref, axis, value, other, row) if row["start_" + other] is not None else None
                    out["%s_%s" % (edge, other)] = found
            out.update([("time_unit", row["time_unit"]), ("volume_unit", row["volume_unit"]), ("warnings", notes)])
            resolved.append(out)
    return resolved, warnings


def _bounds(window):
    """(axis, start, end, unit) of a resolved window, or of a plain {'axis', 'start', 'end', 'unit'} one."""
    axis = window["axis"]
    if "start_" + axis in window:
        return axis, window["start_" + axis], window["end_" + axis], _axis_unit(axis, window)
    return axis, window["start"], window["end"], window.get("unit") or ("CV" if axis == "cv" else None)


def _curve_axis(curve, axis):
    return {"time": curve.time, "volume": curve.volume, "cv": curve.cv}[axis]


def window_stats(curve, window):
    """
    Statistics of `curve` over `window`: an OrderedDict of 'n', every `STATISTICS` entry (None
    when n = 0; 'sd' also for n = 1) and 'warnings' (a list). `window`: a resolved window from
    `phase_windows`, or a plain {'axis': 'time'|'volume'|'cv', 'start', 'end', 'unit'} on the
    curve's axes (unit None: the curve's own). Bounds are inclusive; NaN points are left out.
    """
    from .units import conversion_factor
    axis, lo, hi, unit = _bounds(window)
    notes = []
    out = OrderedDict([("n", 0)] + [(k, None) for k in STATISTICS] + [("warnings", notes)])
    x = _curve_axis(curve, axis)
    if x is None and "start_" + axis in window:  # e.g. a curve on a volume grid has no time axis
        for other in AXES:
            if other != axis and _curve_axis(curve, other) is not None and window.get("start_" + other) is not None \
                    and window.get("end_" + other) is not None:
                notes.append("%s has no %s axis: read on its %s axis" % (curve.name, axis, other))
                axis, lo, hi, unit = other, window["start_" + other], window["end_" + other], \
                    _axis_unit(other, window)
                x = _curve_axis(curve, other)
                break
    if x is None or curve.values is None:
        notes.append("%s has no %s axis or no values: no statistics" % (curve.name, axis))
        return out
    if lo is None or hi is None:
        notes.append("the window has no %s bounds: no statistics" % axis)
        return out
    if axis != "cv" and unit:
        own = curve.time_unit if axis == "time" else curve.volume_unit
        try:
            factor = conversion_factor(unit, own) if own else 1.0
        except Exception as e:
            notes.append("%s: %s; no statistics" % (curve.name, e))
            return out
        lo, hi = lo * factor, hi * factor
    x = np.asarray(x, dtype=float)
    y = np.asarray(curve.values, dtype=float)
    n_all = min(len(x), len(y))
    x, y = x[:n_all], y[:n_all]
    finite_x = x[np.isfinite(x)]
    if len(finite_x) > 1 and lo <= hi:
        # a gap of more than about one sampling step (curves end a point or so before the run log)
        step = 1.5 * float(np.median(np.abs(np.diff(finite_x))))
        if hi - finite_x.max() > step:
            notes.append("%s ends before the window end (%g of %g on its %s axis)" % (curve.name, finite_x.max(), hi, axis))
        if finite_x.min() - lo > step:
            notes.append("%s starts after the window start (%g of %g on its %s axis)" % (curve.name, finite_x.min(), lo, axis))
    inside = np.isfinite(x) & np.isfinite(y) & (x >= lo) & (x <= hi)
    xs, ys = x[inside], y[inside]
    out["n"] = int(len(ys))
    if not len(ys):
        notes.append("no %s points in the window" % curve.name)
        return out
    for name, stat in STATISTICS.items():
        out[name] = stat(xs, ys)
    return out


def window_stats_rows(run, chrom, windows, curves):
    """
    (rows, warnings): one row (`COLUMNS`) per phase occurrence x window x curve of `chrom`.
    `curves`: curve names as the run stores them, or roles (`pycorn.curve_roles.ROLES`; a role
    gives every original curve that has it). A key that matches no curve, and every
    `phase_windows` warning, go to `warnings`; each row's `warnings` holds its own notes
    (clipping, an empty window, a curve read on another axis).
    """
    from .curve_roles import resolve_curve_keys
    if not windows:
        raise ValueError("no window given (no default window): %s" % SYNTAX)
    if not curves:
        raise ValueError("no curve given (no default curves): give curve names or roles")
    names, warnings = [], []
    for key in curves:
        try:
            names += [n for n in resolve_curve_keys(chrom, [key]) if n not in names]
        except KeyError as e:
            warnings.append(str(e.args[0]) + "; no rows for it")
    resolved, found = phase_windows(run, windows, chrom)
    warnings += found
    rows = []
    for win in resolved:
        w = win["window"]
        for name in names:
            curve = chrom.curves[name]
            stats = window_stats(curve, win)
            row = OrderedDict([("run", run.name), ("chromatogram", chrom.name), ("window", w.name),
                               ("phase", win["phase"]), ("occurrence", win["occurrence"]),
                               ("phase_number", win["phase_number"]), ("axis", win["axis"]), ("window_unit", w.unit),
                               ("start_anchor", "phase " + w.start_anchor), ("start_offset", w.start),
                               ("end_anchor", "phase " + w.end_anchor), ("end_offset", w.end)])
            row.update((k, win[k]) for k in ("start_time", "end_time", "start_volume", "end_volume", "start_cv",
                                             "end_cv", "time_unit", "volume_unit"))
            row.update([("curve", name), ("curve_unit", curve.unit), ("curve_precision", curve.precision)])
            row.update((k, stats[k]) for k in ["n"] + list(STATISTICS))
            row["warnings"] = list(win["warnings"]) + stats["warnings"]
            rows.append(row)
    return rows, warnings


def to_frame(rows):
    import pandas as pd
    return pd.DataFrame(rows, columns=COLUMNS)
