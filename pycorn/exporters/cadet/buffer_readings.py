# -*- coding: utf-8 -*-
"""
Buffer conductivity and pH read where the column is by-passed (docs/ROADMAP.md, D22).

Owner (rev 7e): by-pass windows with buffer only (no sample), before, between or after the
feed, give the buffer's own conductivity and pH, without the column. Most runs tested
against a lab environment have one (e.g. a `Purge` block before the run).

- Reading: the median over the last third of the window's last steady piece (same inlets,
  constant %B and %Q); `steady` is False when the window ends in a ramp (then the last section
  is read). A piece shorter than `READ_MIN_S` gives no reading (it shows what was left in the
  lines; tuned against a lab environment, where some windows last only seconds). Settled when the spread over that span is within `COND_SETTLED` (relative) for
  conductivity and `PH_SETTLED` (pH units) for pH; unsettled readings are listed, flagged.
- Check: the column outlet in the nearest section run with the same inlets and %B (column in
  line, injection valve not at a sample position, at least `OUTLET_MIN_S`), read the same way;
  a difference beyond `COND_MATCH` (relative) or `PH_MATCH` (pH units) is a warning. Nothing is
  corrected.
- Salt: with the caller's `buffer_salt` ({inlet position or line letter: mol/m^3}) each
  reading gets the salt of the A/B mix at its %B; `salt_calibration_points()` turns settled
  readings into (mS/cm, mol/m^3) points for `calibration.salt_from_conductivity`. Windows on a
  quaternary (%Q) mix get no salt.
"""
from collections import OrderedDict

import numpy as np

from ...curve_roles import curves_with_role
from ...units import UnitError, conversion_factor, to_si

COND_SETTLED = 0.02  # relative spread
PH_SETTLED = 0.05  # pH units
COND_MATCH = 0.02  # outlet vs by-pass, relative
# outlet vs by-pass, pH units (owner, rev 7f): the outlet reads a little lower, typically 0.01-0.09
# (tuned against a lab environment: a 0.05 limit warned too often), plausibly the column itself
PH_MATCH = 0.1
READ_FRACTION = 1.0 / 3  # the last third of the steady piece
OUTLET_MIN_S = 30.0
READ_MIN_S = 30.0  # a shorter steady by-pass piece shows what was left in the lines, not the buffer
MIN_POINTS = 3
# (label, value key, series picker, relative spread, settled limit, outlet match limit)
QUANTITIES = (("conductivity", "conductivity_s_m", lambda cond, ph: cond, True, COND_SETTLED, COND_MATCH),
              ("ph", "ph", lambda cond, ph: ph, False, PH_SETTLED, PH_MATCH))


def _key(s):
    """Inlets and constant %B/%Q of a section; None while %B or %Q ramps or is unknown."""
    b0, b_slope = s.percent_b
    if b0 is None or b_slope != 0:
        return None
    q = None
    if s.percent_q is not None:
        if any(sl != 0 for sl in s.percent_q[1]):
            return None
        q = tuple(round(v, 3) for v in s.percent_q[0])
    return tuple(sorted(s.inlets.items())), round(b0, 3), q


def _series(chrom, role, wall_s):
    """(wall-clock s, values) of the first `role` curve (conductivity in S/m), or None."""
    for curve in curves_with_role(chrom, role):
        if curve.time is None or curve.values is None:
            continue
        t = wall_s(chrom, curve.time.astype(np.float64) * conversion_factor(curve.time_unit, "s"))
        if role == "conductivity":
            try:
                values, unit, _ = to_si(curve.values, curve.unit)
            except UnitError:
                continue
            if unit != "S/m":
                continue
        else:
            values = curve.values
        return np.asarray(t, dtype=np.float64), np.asarray(values, dtype=np.float64)
    return None


def _read(series, t0, t1, relative):
    """(median, spread) of `series` over [t0, t1], or (None, None) with too few points."""
    if series is None:
        return None, None
    t, v = series
    x = v[(t >= t0) & (t <= t1)]
    x = x[np.isfinite(x)]
    if len(x) < MIN_POINTS:
        return None, None
    median = float(np.median(x))
    spread = float(np.ptp(x))
    if relative:
        spread = spread / abs(median) if median else None
    return median, spread


def _runs(sections):
    """Neighbouring non-paused sections with the same key, as (key, start_s, end_s, sections)."""
    out = []
    for s in sections:
        if s.paused:
            continue
        k = _key(s)
        if out and out[-1][0] == k and k is not None and abs(out[-1][2] - s.start_s) < 1e-6:
            out[-1] = (k, out[-1][1], s.end_s, out[-1][3] + [s])
        else:
            out.append((k, s.start_s, s.end_s, [s]))
    return out


def _span(start, end):
    return end - (end - start) * READ_FRACTION, end


def _salt(section, buffer_salt):
    if not buffer_salt or section.percent_q is not None:
        return None
    b = section.percent_b[0]

    def line(letter):
        position = section.inlets.get(letter)
        if position in buffer_salt:
            return buffer_salt[position]
        return buffer_salt.get(letter)

    a_salt = line("A")
    b_salt = line("B") if b else 0.0
    if a_salt is None and b < 100:
        return None
    if b_salt is None:
        return None
    return (a_salt or 0.0) * (1 - b / 100.0) + b_salt * b / 100.0


def buffer_readings(chrom, sections, bypass, wall_s, buffer_salt=None):
    """
    (readings, warnings) for the buffer-only by-pass windows `bypass` (see
    `CadetExperiment.bypass`) over the experiment `sections` (wall-clock s, before any
    time-zero shift). `wall_s(chrom, elapsed_s)` maps stored time to wall-clock time.
    """
    warnings = []
    cond = _series(chrom, "conductivity", wall_s)
    ph = _series(chrom, "ph", wall_s)
    if cond is None and ph is None:
        return [], warnings
    outlet_runs = [r for r in _runs([s for s in sections if not s.bypass and s.sample_use is None])
                   if r[0] is not None and r[2] - r[1] >= OUTLET_MIN_S]
    readings, short = [], 0
    for w in bypass:
        if w["sample"]:
            continue
        inside = [s for s in sections if s.bypass and s.start_s >= w["start_s"] - 1e-6 and s.end_s <= w["end_s"] + 1e-6]
        runs = _runs(inside)
        if not runs:
            continue
        key, r0, r1, secs = runs[-1]
        steady = key is not None
        if not steady:
            r0, r1 = secs[-1].start_s, secs[-1].end_s
        if r1 - r0 < READ_MIN_S:
            short += 1
            continue
        t0, t1 = _span(r0, r1)
        row = OrderedDict([("start_s", w["start_s"]), ("end_s", w["end_s"]), ("read_start_s", t0),
                           ("read_end_s", t1), ("block", secs[-1].block), ("inlets", dict(secs[-1].inlets)),
                           ("percent_b", secs[-1].percent_b[0]),
                           ("percent_q", secs[-1].percent_q[0] if secs[-1].percent_q else None),
                           ("steady", steady), ("whole_window_steady", steady and len(runs) == 1)])
        for label, name, series, relative, limit, _ in QUANTITIES:
            value, spread = _read(series(cond, ph), t0, t1, relative)
            row[name] = value
            row[label + "_spread"] = spread
            row[label + "_settled"] = None if value is None else (spread is not None and spread <= limit and steady)
        row["salt_mol_m3"] = _salt(secs[-1], buffer_salt) if steady else None
        row["outlet"] = None
        same = [r for r in outlet_runs if r[0] == key] if steady else []
        if same:
            _, o0, o1, _ = min(same, key=lambda r: min(abs(r[1] - w["end_s"]), abs(r[2] - w["start_s"])))
            u0, u1 = _span(o0, o1)
            outlet = OrderedDict([("start_s", o0), ("end_s", o1), ("read_start_s", u0), ("read_end_s", u1)])
            for label, name, series, relative, _, limit in QUANTITIES:
                value, _ = _read(series(cond, ph), u0, u1, relative)
                outlet[name] = value
                bypassed = row[name]
                if value is None or bypassed is None or not row[label + "_settled"]:
                    outlet[label + "_matches"] = None
                    continue
                diff = abs(value - bypassed) / abs(bypassed) if relative and bypassed else abs(value - bypassed)
                outlet[label + "_matches"] = diff <= limit
                if diff > limit:
                    to_ms = conversion_factor("S/m", "mS/cm")
                    shown = (value * to_ms, bypassed * to_ms, "mS/cm") if relative else (value, bypassed, "")
                    warnings.append("%s at the column outlet %.1f-%.1f s (%.4g %s) differs from the buffer read at "
                                    "by-pass %.1f-%.1f s (%.4g %s) (D22)"
                                    % (label if relative else "pH", o0, o1, shown[0], shown[2], t0, t1, shown[1],
                                       shown[2]))
            row["outlet"] = outlet
        readings.append(row)
    if short:
        warnings.append("%d buffer-only by-pass window(s) shorter than %g s at one setting: no buffer reading (D22)"
                        % (short, READ_MIN_S))
    unsettled = [r for r in readings if r["conductivity_settled"] is False or r["ph_settled"] is False]
    if unsettled:
        warnings.append("%d by-pass buffer reading(s) not settled (conductivity spread > %g %% or pH spread > %g, "
                        "or %%B ramping) (D22)" % (len(unsettled), COND_SETTLED * 100, PH_SETTLED))
    return readings, warnings


def salt_calibration_points(readings, settled_only=True):
    """
    (conductivity mS/cm, salt mol/m^3) points from buffer readings that have a caller salt
    (see `buffer_readings`), for `calibration.salt_from_conductivity`. Duplicate points are
    kept once.
    """
    points = []
    for r in readings:
        if r["salt_mol_m3"] is None or r["conductivity_s_m"] is None:
            continue
        if settled_only and not r["conductivity_settled"]:
            continue
        point = (r["conductivity_s_m"] * conversion_factor("S/m", "mS/cm"), r["salt_mol_m3"])
        if point not in points:
            points.append(point)
    return points
