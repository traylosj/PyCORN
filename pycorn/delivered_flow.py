# -*- coding: utf-8 -*-
"""
The flow the pump delivered, per run-log section, from UNICORN's volume axis (owner,
2026-11-14; docs/RUN_STRUCTURE_REVIEW.md, Decide 6). For CADET; display outputs keep the
logged set flow.

- The volume axis (the stored volume of the curve with the most points) is the integral of
  UNICORN's method flow: the flow of the pump named by the run log's `(System)` entry `The
  method flow is now calculated based on the system | sample flow.` (docs/FACTS.md). It holds
  what the run log does not: stops while a valve turns, ramps with pressure control, the pump
  stopping before a pause and ramping up after it.
- Which pump: the method-flow entry in force at the section's middle. Results without the
  entry: the system pump, or the sample pump when the system flow is (nearly) 0 and the sample
  pump runs (`pump_rule`).
- How much: a section whose axis volume differs from the logged pump flow x time by more than
  `TOLERANCE_ML` **and** `TOLERANCE_FRACTION` of the logged volume is fitted to the axis, with
  a warning; the others keep the logged set flow.
- Shape: the axis slope (median of 3 neighbours, against single-point spikes) simplified to a
  few straight pieces (stop, ramp, set flow; any order, cut-off ramps too) within
  `FIT_TOLERANCE_ML_MIN` or `FIT_TOLERANCE_FRACTION` of the set flow; knots near the set flow
  take it, knots near 0 take 0, the others are fitted by least squares on the axis volume. No
  ramp rate is assumed: it differs by instrument. The pieces' volume equals the axis volume. Sections too short or too sparse for a shape
  (`MIN_FIT_S`, `MIN_FIT_POINTS`) get one constant flow with the axis volume.
- At a pause the axis can book volume with no elapsed time: its first step after the pause holds
  more than the set flow could deliver in it (pumped while the pump stopped). That volume
  can't be placed in time, so it is left out of the fit, with one warning per run.
- The axis counts pump volume, in column by-pass and at waste too; where the liquid goes is
  the valve positions' business (D14, `run_log_sections.column_flow`), applied by the caller.
"""
import re
from collections import OrderedDict

import numpy as np

from .units import conversion_factor

METHOD_FLOW = re.compile(r"method flow is now calculated based on the (?P<pump>system|sample) flow", re.IGNORECASE)
PUMPS = ("system", "sample")
TOLERANCE_ML = 0.01
TOLERANCE_FRACTION = 0.01
FIT_TOLERANCE_ML_MIN = 0.05  # the axis slope's noise at 0.1 s steps (float32 volumes)
FIT_TOLERANCE_FRACTION = 0.02
STOPPED_ML_MIN = 0.01  # a set flow at or below this counts as stopped (0.001 ml/min is logged as a stop)
PAUSE_JUMP_ML = 0.005  # a first step after a pause holding more than this beyond the set flow is booked volume
STEP_S = 0.5  # neighbouring pieces shorter than this going the same way join into one (a step)
MIN_FIT_S = 1.0
MIN_FIT_POINTS = 5
MAX_PIECES = 24
EPS_S = 1e-6


def method_flow_basis(chrom):
    """
    [(elapsed_s, 'system' | 'sample')] from the run log's method-flow entries, in time order;
    [] when the run logs none.
    """
    log = chrom.run_log()
    if log is None:
        return []
    to_s = conversion_factor(log.time_unit, "s")
    out = []
    for e in log.events:
        m = METHOD_FLOW.search(e.text or "")
        if m and e.time is not None:
            out.append((e.time * to_s, m.group("pump").lower()))
    return sorted(out, key=lambda x: x[0])


def pump_rule(system_flow_ml_min, sample_flow_ml_min):
    """The pump the volume axis counts when the run logs no method-flow entry."""
    if (system_flow_ml_min or 0.0) <= STOPPED_ML_MIN and (sample_flow_ml_min or 0.0) > STOPPED_ML_MIN:
        return "sample"
    return "system"


def volume_axis(chrom):
    """(time_s, volume_ml) of the curve with the most points that has both (sorted, finite), or None."""
    best = None
    for c in chrom.curves.values():
        if c.time is None or c.volume is None or c.time_unit is None or c.volume_unit is None:
            continue
        if c.is_original is False or len(c.time) < 2:
            continue
        if best is None or len(c.time) > len(best.time):
            best = c
    if best is None:
        return None
    t = np.asarray(best.time, dtype=np.float64) * conversion_factor(best.time_unit, "s")
    v = np.asarray(best.volume, dtype=np.float64) * conversion_factor(best.volume_unit, "ml")
    ok = np.isfinite(t) & np.isfinite(v)
    order = np.argsort(t[ok], kind="stable")
    return t[ok][order], v[ok][order]


def _median3(q):
    if len(q) < 3:
        return q.copy()
    out = q.copy()
    out[1:-1] = np.median(np.vstack([q[:-2], q[1:-1], q[2:]]), axis=0)
    return out


def _simplify(x, y, tol):
    """Indices of a polyline through (x, y) within `tol` (vertical distance), first and last kept."""
    keep = {0, len(x) - 1}
    stack = [(0, len(x) - 1)]
    while stack:
        a, b = stack.pop()
        if b - a < 2:
            continue
        line = y[a] + (y[b] - y[a]) * (x[a + 1:b] - x[a]) / (x[b] - x[a])
        dev = np.abs(y[a + 1:b] - line)
        k = int(np.argmax(dev))
        if dev[k] > tol:
            c = a + 1 + k
            keep.add(c)
            stack += [(a, c), (c, b)]
    return sorted(keep)


def _join_steps(x, y, idx):
    """Knots `idx` without those inside a run of short pieces going the same way (a step = one fast ramp)."""
    idx = list(idx)
    changed = True
    while changed:
        changed = False
        for k in range(1, len(idx) - 1):
            a, b, c = idx[k - 1], idx[k], idx[k + 1]
            up1, up2 = y[b] - y[a], y[c] - y[b]
            if (x[b] - x[a] < STEP_S and x[c] - x[b] < STEP_S and up1 * up2 > 0):
                del idx[k]
                changed = True
                break
    return idx


def fit_flow(t, v, t0, t1, set_ml_min, ceiling=None):
    """
    Piecewise-linear flow over elapsed [t0, t1] s fitted to the volume axis (`t` s, `v` ml).
    Returns (knots_t, knots_q, kind): knot times (s, from t0 to t1) and flows (ml/min), kind
    'fitted' or 'mean' (one constant flow: too short or too sparse for a shape).

    Knot times: the axis slope simplified to straight pieces. Knot flows: near the set flow
    the set flow, near 0 zero; the others by least squares on the axis volume, never below 0
    and never above `ceiling` (ml/min, None: no limit). The pieces' volume equals the axis
    volume over [t0, t1].
    """
    duration = t1 - t0
    axis_ml = float(np.interp(t1, t, v) - np.interp(t0, t, v))
    mean = axis_ml / duration * 60.0 if duration > 0 else 0.0
    inside = t[(t > t0 + EPS_S) & (t < t1 - EPS_S)]
    if duration < MIN_FIT_S or len(inside) + 2 < MIN_FIT_POINTS:
        return np.array([t0, t1]), np.array([mean, mean]), "mean"
    tk = np.concatenate([[t0], inside, [t1]])
    vk = np.interp(tk, t, v)
    q = _median3(np.diff(vk) / np.diff(tk) * 60.0)
    # flow at each interval's middle; the polyline spans [t0, t1] with the end intervals' flows
    mid = 0.5 * (tk[1:] + tk[:-1])
    x = np.concatenate([[t0], mid, [t1]])
    y = np.concatenate([[q[0]], q, [q[-1]]])
    tol = max(FIT_TOLERANCE_ML_MIN, FIT_TOLERANCE_FRACTION * abs(set_ml_min or 0.0))
    for _ in range(6):
        idx = _join_steps(x, y, _simplify(x, y, tol))
        if len(idx) - 1 <= MAX_PIECES:
            break
        tol *= 2.0
    kt, kq = x[idx].copy(), y[idx].copy()
    fixed = np.zeros(len(kq), dtype=bool)
    if set_ml_min is not None:
        near = np.abs(kq - set_ml_min) <= tol
        kq[near], fixed[near] = set_ml_min, True
    low = (kq <= tol) & ~fixed
    kq[low], fixed[low] = 0.0, True
    # drop knots inside a flat run (same value on both sides)
    flat = [k for k in range(1, len(kq) - 1) if fixed[k - 1] and fixed[k] and fixed[k + 1]
            and kq[k - 1] == kq[k] == kq[k + 1]]
    kt, kq, fixed = np.delete(kt, flat), np.delete(kq, flat), np.delete(fixed, flat)
    # the free knots' flows: least squares on the axis volume, its total over the section exact
    rel = inside
    basis = _integrated_hats(kt, rel)  # volume (ml) per 1 ml/min at each knot, at each axis point
    total = _integrated_hats(kt, np.array([t1]))[0]
    target = np.interp(rel, t, v) - np.interp(t0, t, v)
    at_set = fixed & (kq != 0.0)
    # first with the set flow held where the axis is near it; when the volume can't close that
    # way (the steady flow sits a little off the set flow), with those knots fitted too
    for hold in (at_set if fixed.all() else fixed, np.zeros(len(kq), dtype=bool)):
        q = _bounded_fit(basis, total, target, axis_ml, kq.copy(), hold.copy(), ceiling)
        if q is not None:
            return kt, q, "fitted"
    return np.array([t0, t1]), np.array([mean, mean]), "mean"


def _bounded_fit(basis, total, target, axis_ml, kq, fixed, ceiling):
    """Knot flows: least squares on the axis volume, total exact, the `fixed` ones kept; within [0, ceiling]; or None."""
    for _ in range(len(kq) + 1):
        if fixed.all():
            return None
        q_free = _constrained_lsq(basis[:, ~fixed], target - basis[:, fixed] @ kq[fixed],
                                  total[~fixed], axis_ml - total[fixed] @ kq[fixed])
        if q_free is None:
            return None
        kq[~fixed] = q_free
        # a pump delivers no negative flow, and nothing above the set flows in force around it
        negative = (~fixed) & (kq < 0)
        above = (~fixed) & (kq > ceiling) if ceiling is not None else np.zeros(len(kq), dtype=bool)
        if not negative.any() and not above.any():
            return kq
        kq[negative], fixed[negative] = 0.0, True
        kq[above], fixed[above] = ceiling, True
    return None


def _integrated_hats(kt, at):
    """
    Matrix (len(at), len(kt)): the volume (ml) from kt[0] to each time in `at` of a flow that is
    1 ml/min at one knot, 0 at the others, linear between (hat functions).
    """
    grid = np.union1d(kt, at)
    out = np.empty((len(at), len(kt)))
    for j in range(len(kt)):
        hat = np.zeros(len(kt))
        hat[j] = 1.0
        q = np.interp(grid, kt, hat)
        cum = np.concatenate([[0.0], np.cumsum(0.5 * (q[1:] + q[:-1]) * np.diff(grid))]) / 60.0
        out[:, j] = np.interp(at, grid, cum)
    return out


def _constrained_lsq(a, b, c, d):
    """x minimising |a x - b| with c . x = d exactly (a particular solution plus c's null space), or None."""
    n = a.shape[1]
    cc = float(c @ c)
    if n == 0 or cc <= 0:
        return None
    x0 = c * (d / cc)
    if n == 1:
        return x0
    null = np.linalg.svd(c.reshape(1, -1))[2][1:].T  # n x (n-1), orthogonal to c
    try:
        z = np.linalg.lstsq(a @ null, b - a @ x0, rcond=None)[0]
    except np.linalg.LinAlgError:
        return None
    x = x0 + null @ z
    return x if np.all(np.isfinite(x)) else None


def _pieces(kt, kq, t0):
    """[(start offset s, end offset s, flow at start ml/min, slope ml/min per s)] from knots."""
    out = []
    for a, b, qa, qb in zip(kt[:-1], kt[1:], kq[:-1], kq[1:]):
        if b - a > EPS_S:
            out.append((float(a - t0), float(b - t0), float(qa), float((qb - qa) / (b - a))))
    return out


def _ramp_s(pieces):
    """Seconds of the pieces whose flow rises by more than the fit tolerance."""
    return sum(b - a for a, b, _, slope in pieces if slope * (b - a) > FIT_TOLERANCE_ML_MIN)


def delivered_flow(chrom, sections, tolerance_ml=TOLERANCE_ML, tolerance_fraction=TOLERANCE_FRACTION):
    """
    Per section of `sections` (`run_log_sections(...)['sections']`), in order: None for a pause,
    else an OrderedDict:

    - `pump`: 'system' | 'sample', the pump the volume axis counts; `pump_source`: 'run_log'
      (method-flow entry) or 'rule' (`pump_rule`)
    - `set_ml_min`: that pump's logged set flow; `logged_ml`, `axis_ml`: volumes over the section
    - `fitted`: True when the section exceeds the tolerance (`kind` 'fitted' or 'mean', see
      `fit_flow`); `pieces`: [(start offset s, end offset s, flow ml/min, slope ml/min per s)]
      from the section's start (one piece at the set flow when not fitted)
    - `ramp_s`: seconds of rising flow in the pieces

    Returns (rows, warnings). Without a volume axis every row keeps the set flow, with a warning.
    """
    warnings = []
    axis = volume_axis(chrom)
    basis = method_flow_basis(chrom)
    if axis is None:
        warnings.append("no curve with a volume axis: CADET flow is the logged set flow")
    if axis is not None and not basis:
        warnings.append("the run log names no method-flow pump: the volume axis taken as the system pump's, or the "
                        "sample pump's where the system flow is 0")
    rows = []
    booked = []  # (wall s, ml) volume the axis books at a pause with no time
    after_pause = False
    live = [k for k, s in enumerate(sections) if not s.paused]
    for i, s in enumerate(sections):
        if s.paused:
            rows.append(None)
            after_pause = True
            continue
        follows_pause, after_pause = after_pause, False
        t0, t1 = s.elapsed_start_s, s.elapsed_end_s
        middle = 0.5 * (t0 + t1)
        named = [p for at, p in basis if at <= middle + EPS_S]
        if named:
            pump, source = named[-1], "run_log"
        elif basis:  # before the first entry: UNICORN logs it at the run start
            pump, source = basis[0][1], "run_log"
        else:
            pump, source = pump_rule(s.system_flow_ml_min, s.sample_flow_ml_min), "rule"
        set_q = s.system_flow_ml_min if pump == "system" else s.sample_flow_ml_min
        duration = t1 - t0
        row = OrderedDict([("pump", pump), ("pump_source", source), ("set_ml_min", set_q), ("logged_ml", None),
                           ("axis_ml", None), ("fitted", False), ("kind", None),
                           ("pieces", [(0.0, duration, set_q or 0.0, 0.0)]), ("ramp_s", 0.0)])
        rows.append(row)
        if axis is None or set_q is None or duration <= EPS_S:
            continue
        t, v = axis
        if t1 < t[0] - EPS_S or t0 > t[-1] + EPS_S:
            continue
        if follows_pause:
            jump = _pause_jump(t, v, t0, set_q)
            if jump > 0:
                booked.append((s.start_s, jump))
                v = v.copy()
                v[t > t0 + EPS_S] -= jump
        logged = set_q * duration / 60.0
        axis_ml = float(np.interp(t1, t, v) - np.interp(t0, t, v))
        row["logged_ml"], row["axis_ml"] = logged, axis_ml
        diff = axis_ml - logged
        if abs(diff) <= tolerance_ml or abs(diff) <= tolerance_fraction * abs(logged):
            continue
        kt, kq, kind = fit_flow(t, v, t0, t1, set_q, _ceiling(sections, live, i, pump))
        row["fitted"], row["kind"] = True, kind
        row["pieces"] = _pieces(kt, kq, t0)
        row["ramp_s"] = _ramp_s(row["pieces"])
        what = ("one constant flow (too short for a shape)" if kind == "mean"
                else "%d piece(s)%s" % (len(row["pieces"]),
                                       ", ramped %.1f s" % row["ramp_s"] if row["ramp_s"] > 0 else ""))
        change = "%+.1f %%" % (100.0 * diff / logged) if logged else "set flow 0"
        warnings.append("%.1f-%.1f s: volume axis %.4g ml, logged %s flow x time %.4g ml (%s); flow fitted to the "
                        "axis: %s" % (s.start_s, s.end_s, axis_ml, pump, logged, change, what))
    if booked:
        warnings.append("the volume axis books %.4g ml at %d pause(s) with no time (%s); not placed in the flow"
                        % (sum(ml for _, ml in booked), len(booked),
                           ", ".join("%.4g ml at %.1f s" % (ml, at) for at, ml in booked)))
    return rows, warnings


def _ceiling(sections, live, i, pump):
    """
    The highest set flow of `pump` in section `i` and the live sections either side of it
    (ml/min, plus the fit tolerance), or None when all are stopped (the axis then has the say).
    """
    k = live.index(i)
    near = [sections[j] for j in live[max(k - 1, 0):k + 2]]
    values = [(s.system_flow_ml_min if pump == "system" else s.sample_flow_ml_min) or 0.0 for s in near]
    top = max(values)
    if top <= STOPPED_ML_MIN:
        return None
    return top + max(FIT_TOLERANCE_ML_MIN, FIT_TOLERANCE_FRACTION * top)


def _pause_jump(t, v, t0, set_ml_min):
    """
    Volume (ml) the axis books in its first step after a pause beyond the set flow over that
    step: pumped while the pump stopped, stored with no elapsed time. 0 when below `PAUSE_JUMP_ML`.
    """
    k = int(np.searchsorted(t, t0 + EPS_S, side="left"))
    if k <= 0 or k >= len(t):
        return 0.0
    step = float(v[k] - np.interp(t0, t, v)) - (set_ml_min or 0.0) * (t[k] - t0) / 60.0
    return step if step > PAUSE_JUMP_ML else 0.0
