# -*- coding: utf-8 -*-
"""
The extinction coefficient this detector gives, from what ran (GitHub issue #1).

Runs often send sample through the column by-pass before loading (priming, sample UV/pH
check; D18 'to_bypass' in `pycorn.run_log_sections`). The UV there sees the sample itself,
so with the sample concentration from the caller's offline assay:

- plateau (the signal holds at its maximum): epsilon = (A_max - A_before) / (c * l)
- pulse (a short plug; its maximum is below the sample's own absorbance):
  epsilon = integral(A dV) / (c * V_loop * l), with the caller's loop volume. The volume the
  set-points send while the valve is at Inject is only an upper bound: a fixed loop empties
  early and the valve may stay at Inject (D18-D20), so it is reported, never used.

`bypass_uv_readings` finds every sample-to-by-pass window of a chromatogram and reads every
live UV channel there; `epsilon_from_injection` does the pulse case for a window and an
injected amount the caller gives (e.g. a dedicated calibration run).

A measured coefficient is an estimate too (`pycorn.concentration.ACCURACY_NOTE`, on every
row). No absorbance limit is assumed: with the caller's `absorbance_limit`, readings above it
give no coefficient. Signs of saturation (a flat top), a cut peak or baseline drift are warned.
The limits below were tuned against a lab environment (2026-11-08), which showed both cases
(plateaus, some near the top of the detector's linear range, and short pulses) and windows
with no signal.
"""
from collections import OrderedDict

import numpy as np

from .concentration import ACCURACY_NOTE
from .units import conversion_factor, quantity, uv_path_length_cm

EDGE_FRACTION = 0.05  # share of a window's points at each end taken as baseline
BASELINE_WARN = 0.05  # pulse baseline ends differing by more than this share of the peak height
FLAT_TOP_POINTS = 3  # this many consecutive points at exactly the maximum: the detector may be saturated
PLATEAU_SPAN = 0.02  # a plateau stays within this share of the rise below the maximum ...
PLATEAU_MIN_S = 10.0  # ... for at least this long (in any channel of the window); pulses are much shorter
NO_SIGNAL_AU = 0.02  # a rise below this (stored absorbance, AU) is no signal: buffer or empty line
PULSE_EDGE = 0.01  # a pulse's area runs while the signal is above this share of its height


# ---------------------------------------------------------------------------
# Shared reading of one window
# ---------------------------------------------------------------------------

def _absorbance(curve, path_length_cm):
    """(factor stored -> AU or AU/cm, path cm or None, basis) for a UV curve."""
    q = quantity(curve.unit)
    if q == "absorbance_per_length":
        return conversion_factor(curve.unit, "AU/cm"), 1.0, "per_cm_unit"
    if q != "absorbance":
        raise ValueError("%s: unit %r is not an absorbance" % (curve.name, curve.unit))
    if path_length_cm is not None:
        if not float(path_length_cm) > 0:
            raise ValueError("UV path length must be above 0 cm, not %r" % (path_length_cm,))
        return conversion_factor(curve.unit, "AU"), float(path_length_cm), "caller"
    path, basis = uv_path_length_cm(curve)
    return conversion_factor(curve.unit, "AU"), path, basis


def _edges(n_points):
    return max(1, int(round(n_points * EDGE_FRACTION)))


def _pulse_area(v, a):
    """(area above a line through the two ends, (start, end) baseline, warnings) of one window."""
    n = _edges(len(v))
    v0, a0, v1, a1 = v[:n].mean(), a[:n].mean(), v[-n:].mean(), a[-n:].mean()
    baseline = a0 + (a1 - a0) * (v - v0) / (v1 - v0) if v1 > v0 else np.full_like(a, a0)
    net = a - baseline
    area = float(np.sum((net[1:] + net[:-1]) * np.diff(v)) / 2)  # trapezoid rule
    top = int(np.argmax(net))
    height = float(net[top])
    warnings = []
    if height > 0 and abs(a1 - a0) > BASELINE_WARN * height:
        warnings.append("baseline at the window ends differs by %.0f%% of the peak height: drift, or the peak "
                        "has not returned to baseline" % (100 * abs(a1 - a0) / height))
    if top < n or top >= len(v) - n:
        warnings.append("peak maximum at %g ml is at the window edge: the peak may be cut" % v[top])
    return area, height, (float(a0), float(a1)), warnings


def _flat_top(stored):
    """Longest run of consecutive points at exactly the maximum."""
    longest = run = 0
    for flag in stored == stored.max():
        run = run + 1 if flag else 0
        longest = max(longest, run)
    return longest


def _saturation(stored, curve, absorbance_limit, absorbance_limit_unit):
    """(over the caller's limit, warnings) for the stored values of a window."""
    warnings = []
    peak = float(stored.max())
    flat = _flat_top(stored)
    if flat >= FLAT_TOP_POINTS:
        warnings.append("flat top: %d consecutive points at %g %s; the detector may be saturated"
                        % (flat, peak, curve.unit))
    over = (absorbance_limit is not None and
            peak > absorbance_limit * conversion_factor(absorbance_limit_unit, curve.unit))
    if over:
        warnings.append("maximum %g %s is above the absorbance limit %g %s: outside the linear range, no "
                        "coefficient" % (peak, curve.unit, absorbance_limit, absorbance_limit_unit))
    return over, warnings


# ---------------------------------------------------------------------------
# A window and an injected amount from the caller
# ---------------------------------------------------------------------------

def epsilon_from_injection(chrom, curve, injected_mass, injected_mass_unit="mg", window=None, window_unit="ml",
                           path_length_cm=None, absorbance_limit=None, absorbance_limit_unit="mAU"):
    """
    The mass extinction coefficient this detector gives, from one injection of a known amount
    that bypassed the column: epsilon = integral(A dV) / (injected mass * path length), in
    ml/(mg*cm).

    `curve` is a UV curve of `chrom` (a `Curve` or its name). `injected_mass` is the caller's.
    `window` = (start, end) on the curve's volume axis in `window_unit`; default: the run's
    by-pass interval from the column valve entries (`pycorn.column_volume.column_path`), which
    must be exactly one. The baseline is a line through the mean signal at the window's two
    ends (`EDGE_FRACTION` of the points each); put the window around one peak, starting and
    ending on baseline. `path_length_cm` overrides the stored path of plain absorbance curves.
    Above the caller's `absorbance_limit` (stored units) no coefficient is given.

    Returns an OrderedDict: `epsilon_ml_per_mg_cm` (None above the limit), the inputs and what
    was used, `warnings` and `note` (`ACCURACY_NOTE`). Raises ValueError if something needed is
    missing.
    """
    from .column_volume import column_path
    if isinstance(curve, str):
        curve = chrom.curve(curve)
    to_au, path, basis = _absorbance(curve, path_length_cm)
    if path is None:
        raise ValueError("%s: no path length (%s); give path_length_cm" % (curve.name, basis))
    if curve.values is None or curve.volume is None:
        raise ValueError("%s has no values or no volume axis" % curve.name)
    mass_mg = float(injected_mass) * conversion_factor(injected_mass_unit, "mg")
    if not mass_mg > 0:
        raise ValueError("injected mass must be above 0, not %r %s" % (injected_mass, injected_mass_unit))
    volume = curve.volume.astype(np.float64) * conversion_factor(curve.volume_unit, "ml")
    if window is not None:
        to_ml = conversion_factor(window_unit, "ml")
        start, end = float(window[0]) * to_ml, float(window[1]) * to_ml
        window_source = "caller"
    else:
        bypass = column_path(chrom)["bypass"]
        if len(bypass) != 1:
            found = ", ".join("%g..%g ml" % b for b in bypass) or "none"
            raise ValueError("%s: %d by-pass intervals in the run log (%s); give window=(start, end)"
                             % (chrom.name, len(bypass), found))
        start, end = bypass[0]
        end = min(end, float(np.nanmax(volume)))
        window_source = "bypass"
    if not end > start:
        raise ValueError("window %g..%g ml is empty" % (start, end))
    inside = (volume >= start) & (volume <= end) & np.isfinite(curve.values)
    if inside.sum() < 5:
        raise ValueError("%s: %d points in %g..%g ml; need at least 5" % (curve.name, inside.sum(), start, end))
    v = volume[inside]
    stored = curve.values[inside].astype(np.float64)
    area, height, baseline, warnings = _pulse_area(v, stored * to_au)
    if not area > 0 or not height > 0:
        raise ValueError("%s: no peak above the baseline in %g..%g ml" % (curve.name, start, end))
    if height < NO_SIGNAL_AU:  # as bypass_uv_readings' 'no_signal': likely buffer or an empty line
        warnings.append("peak height %.3g AU is below %g AU: no clear signal (buffer or an empty line?); check "
                        "the window%s" % (height, NO_SIGNAL_AU,
                                          " (the by-pass interval; give window=)" if window is None else ""))
    marks =[e.volume * conversion_factor(ec.volume_unit, "ml") for ec in chrom.event_curves_of_type("Injection")
             for e in ec.events if e.volume is not None]
    in_window = [m for m in marks if start <= m <= end]
    if len(in_window) > 1:
        warnings.append("%d injection marks in %g..%g ml; give one window per injection" % (len(in_window), start, end))
    over, more = _saturation(stored, curve, absorbance_limit, absorbance_limit_unit)
    warnings += more
    return OrderedDict([("epsilon_ml_per_mg_cm", None if over else area / (mass_mg * path)), ("curve", curve.name),
                        ("raw_unit", curve.unit), ("window_ml", (start, end)), ("window_source", window_source),
                        ("area_au_ml", area), ("injected_mg", mass_mg),
                        ("path_length_cm", path), ("path_length_basis", basis),
                        ("max", float(stored.max())), ("baseline_au", baseline),
                        ("warnings", warnings), ("note", ACCURACY_NOTE)])


# ---------------------------------------------------------------------------
# Sample sent to the by-pass, found in the run log
# ---------------------------------------------------------------------------

def bypass_sample_windows(sections):
    """
    The column by-pass windows of `sections` (`run_log_sections(...)['sections']`) during which
    sample went to the by-pass: one OrderedDict each with the elapsed and wall-clock start/end
    (s), `sample_start_elapsed_s` (first sample to the by-pass), `valve_ml` (what the set-points
    send while the valve is there: system flow at 'Inject', sample flow at 'Direct inject'; an
    upper bound for a fixed loop), the blocks and the valve positions. Windows of zero length
    are left out.
    """
    groups, current = [], []
    for s in sections:
        if s.bypass:
            current.append(s)
        elif current:
            groups.append(current)
            current = []
    if current:
        groups.append(current)
    out = []
    for g in groups:
        sample = [s for s in g if s.sample_use == "to_bypass" and not s.paused]
        if not sample or not g[-1].elapsed_end_s > g[0].elapsed_start_s:
            continue
        ml = 0.0
        for s in sample:
            direct = (s.injection_valve or "").lower().startswith("direct")
            flow = s.sample_flow_ml_min if direct else s.system_flow_ml_min
            ml += (flow or 0.0) * (s.elapsed_end_s - s.elapsed_start_s) / 60.0
        out.append(OrderedDict([("elapsed_start_s", g[0].elapsed_start_s), ("elapsed_end_s", g[-1].elapsed_end_s),
                                ("start_s", g[0].start_s), ("end_s", g[-1].end_s),
                                ("sample_start_elapsed_s", sample[0].elapsed_start_s), ("valve_ml", ml),
                                ("blocks", sorted({s.block for s in sample if s.block})),
                                ("valve", sorted({s.injection_valve for s in sample if s.injection_valve}))]))
    return out


READING_COLUMNS = ["window", "start_s", "end_s", "elapsed_start_s", "elapsed_end_s", "blocks", "valve", "valve_ml",
                   "curve", "wavelength_nm", "wavelength_source", "raw_unit", "kind", "before", "max", "plateau_s",
                   "area_au_ml", "loop_volume_ml", "path_length_cm", "path_length_basis", "absorbance_per_cm",
                   "sample_concentration_mg_ml", "epsilon_ml_per_mg_cm", "epsilon_from", "warnings", "note"]


def bypass_uv_readings(run, chrom=None, sample_concentration=None, sample_concentration_unit="mg/ml",
                       loop_volume=None, loop_volume_unit="ml", absorbance_limit=None, absorbance_limit_unit="mAU",
                       path_length_cm=None, sections=None):
    """
    One row (OrderedDict, `READING_COLUMNS`) per sample-to-by-pass window of `chrom` (default:
    the run's first chromatogram) and live UV channel (wavelength by the D8 rule,
    `pycorn.exporters.cadet.uv_wavelengths`).

    `kind`, per window: 'plateau' when a channel stays within `PLATEAU_SPAN` of its rise below
    the maximum for at least `PLATEAU_MIN_S`, else 'pulse'; per channel 'no_signal' (rise below
    `NO_SIGNAL_AU`) or 'too_few_points'. `before` is the signal before the sample (stored
    unit), `max` the maximum. `absorbance_per_cm` is what the sample itself gives: plateau
    (max - before) / path; pulse `area_au_ml` / (path * loop volume), only with the caller's
    `loop_volume` (`valve_ml` is an upper bound, reported only). With the caller's
    `sample_concentration` (mg/ml, from an offline assay), `epsilon_ml_per_mg_cm` =
    absorbance_per_cm / concentration; none above the caller's `absorbance_limit` (stored-unit
    comparison) or without a path. `path_length_cm` overrides the stored path. `sections`:
    `run_log_sections(run, chrom)['sections']` when already built. Every row has `warnings`
    and `note` (`ACCURACY_NOTE`).
    """
    from .exporters.cadet import uv_wavelengths
    from .run_log_sections import run_log_sections
    chrom = chrom if chrom is not None else run.chromatogram()
    if sections is None:
        sections = run_log_sections(run, chrom)["sections"]
    conc = _positive(sample_concentration, sample_concentration_unit, "mg/ml", "sample concentration")
    loop_ml = _positive(loop_volume, loop_volume_unit, "ml", "loop volume")
    windows = bypass_sample_windows(sections)
    channels = uv_wavelengths(chrom, sections)[0] if windows else []
    rows = []
    for i, w in enumerate(windows, 1):
        measured = [(curve, nm, nm_source, _measure(curve, w, path_length_cm))
                    for curve, _, nm, nm_source in channels]
        held = [m["plateau_s"] for _, _, _, m in measured if m.get("plateau_s") is not None]
        kind = "plateau" if held and max(held) >= PLATEAU_MIN_S else "pulse"
        for curve, nm, nm_source, m in measured:
            row = OrderedDict((c, None) for c in READING_COLUMNS)
            row.update([("window", i), ("start_s", w["start_s"]), ("end_s", w["end_s"]),
                        ("elapsed_start_s", w["elapsed_start_s"]), ("elapsed_end_s", w["elapsed_end_s"]),
                        ("blocks", ", ".join(w["blocks"])), ("valve", ", ".join(w["valve"])),
                        ("valve_ml", w["valve_ml"]), ("curve", curve.name), ("wavelength_nm", nm),
                        ("wavelength_source", nm_source), ("raw_unit", curve.unit),
                        ("before", m.get("before")), ("max", m.get("max")), ("plateau_s", m.get("plateau_s")),
                        ("path_length_cm", m["path"]), ("path_length_basis", m["basis"]),
                        ("sample_concentration_mg_ml", conc), ("loop_volume_ml", loop_ml if kind == "pulse" else None),
                        ("note", ACCURACY_NOTE)])
            row["kind"] = m.get("kind") or kind
            row["warnings"] = _finish(row, curve, m, w, conc, loop_ml, absorbance_limit, absorbance_limit_unit)
            rows.append(row)
    return rows


def _positive(value, unit, target, what):
    if value is None:
        return None
    out = float(value) * conversion_factor(unit, target)
    if not out > 0:
        raise ValueError("%s must be above 0, not %r %s" % (what, value, unit))
    return out


def _measure(curve, w, path_length_cm):
    """What one UV curve shows in window `w`: points, the level before the sample, the maximum, the plateau."""
    to_au, path, basis = _absorbance(curve, path_length_cm)
    m = {"path": path, "basis": basis}
    t = curve.time * conversion_factor(curve.time_unit, "s")
    inside = (t >= w["elapsed_start_s"]) & (t <= w["elapsed_end_s"]) & np.isfinite(curve.values)
    if inside.sum() < 5:
        m["kind"] = "too_few_points"
        return m
    stored = curve.values[inside].astype(np.float64)
    ts = t[inside]
    a = stored * to_au
    pre = ts < w["sample_start_elapsed_s"]
    if pre.sum() < 1:
        pre = np.zeros(len(a), bool)
        pre[:_edges(len(a))] = True
    base = float(a[pre].mean())
    rise = float(a.max() - base)
    m.update(stored=stored, a=a, ts=ts, inside=inside, base=base, rise=rise,
             before=float(stored[pre].mean()), max=float(stored.max()))
    if rise < NO_SIGNAL_AU:
        m["kind"] = "no_signal"
        return m
    near = a >= a.max() - PLATEAU_SPAN * rise
    held, start = 0.0, None
    for k, flag in enumerate(near):
        if flag and start is None:
            start = k
        if start is not None and (not flag or k == len(near) - 1):
            end = k if flag else k - 1
            held = max(held, float(ts[end] - ts[start]))
            start = None
    m["plateau_s"] = held
    return m


def _finish(row, curve, m, w, conc, loop_ml, absorbance_limit, absorbance_limit_unit):
    """Fill `row`'s area, absorbance per cm and coefficient from measurement `m`; returns the warnings."""
    warnings = []
    if row["kind"] in ("no_signal", "too_few_points"):
        return warnings
    path = m["path"]
    if path is None:
        warnings.append("%s: no path length (%s)" % (curve.name, m["basis"]))
    over, more = _saturation(m["stored"], curve, absorbance_limit, absorbance_limit_unit)
    warnings += more
    per_cm = None
    if row["kind"] == "plateau":
        row["epsilon_from"] = "plateau maximum"
        if (m["plateau_s"] or 0.0) < PLATEAU_MIN_S:
            warnings.append("this channel holds its maximum %.0f s only (another channel set the plateau)"
                            % m["plateau_s"])
        per_cm = m["rise"] / path if path else None
    else:
        row["epsilon_from"] = "pulse area / loop volume"
        area, more = _pulse(curve, m)
        warnings += more
        row["area_au_ml"] = area
        if loop_ml is None:
            warnings.append("pulse: give the loop volume for a coefficient; the valve period sent %.3g ml, an upper "
                            "bound (a fixed loop empties early)" % w["valve_ml"])
        elif loop_ml > w["valve_ml"] * 1.05 and w["valve_ml"] > 0:
            warnings.append("loop volume %.3g ml is more than the valve period sent (%.3g ml): the loop was not "
                            "emptied" % (loop_ml, w["valve_ml"]))
        if loop_ml is not None and path and area is not None and area > 0:
            per_cm = area / loop_ml / path
    row["absorbance_per_cm"] = per_cm
    if conc is not None and per_cm is not None and not over:
        row["epsilon_ml_per_mg_cm"] = per_cm / conc
    return warnings


def _pulse(curve, m):
    """(area AU*ml above the level before the sample, warnings) of the pulse around the maximum."""
    if curve.volume is None:
        return None, ["%s has no volume axis: no pulse area" % curve.name]
    v = curve.volume[m["inside"]].astype(np.float64) * conversion_factor(curve.volume_unit, "ml")
    net = m["a"] - m["base"]
    top = int(np.argmax(net))
    height = float(net[top])
    above = net > PULSE_EDGE * height
    lo = top
    while lo > 0 and above[lo - 1]:
        lo -= 1
    hi = top
    while hi < len(net) - 1 and above[hi + 1]:
        hi += 1
    lo, hi = max(lo - 1, 0), min(hi + 1, len(net) - 1)  # the first point below the edge on each side
    seg_v, seg = v[lo:hi + 1], net[lo:hi + 1]
    area = float(np.sum((seg[1:] + seg[:-1]) * np.diff(seg_v)) / 2) if hi > lo else 0.0
    warnings = []
    if hi == len(net) - 1:
        warnings.append("pulse still above baseline at the end of the by-pass window: the area may be cut")
    end_level = float(net[-_edges(len(net)):].mean())
    if abs(end_level) > BASELINE_WARN * height:
        warnings.append("signal at the end of the by-pass window is %.0f%% of the pulse height from the level "
                        "before the sample: drift" % (100 * end_level / height))
    return area, warnings
