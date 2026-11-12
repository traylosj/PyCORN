"""
Down-sampling of one curve: fewer points, the shape kept (ROADMAP M19).

Nothing here has a default size: the caller gives `max_points` or `step` (D28). Values are the
stored values (picked points). Smoothing is not done here (M20); the
only smoothing is inside the derivative weights and never reaches the output values.

Methods (`method=`):
  interval    equal spacing along x
  derivative  more points where the curve changes (share `derivative_share` of the budget)
  minmax      the first, smallest and largest value of each bucket (the web page's rule)
`interval` and `derivative` are one rule: points sit at equal steps of a weight along x,
`(1 - r) * dx / span + r * |dy| / total variation`, with r = `derivative_share` (0 = interval,
1 = derivative, 0.5 = half and half; unset: 0 for interval, 1 for derivative).
"""

import math

import numpy as np

METHODS = ("interval", "derivative", "minmax")
KINDS = ("continuous", "step")


def minmax_index(y, n):
    """
    Indexes of at most about `n` points of `y`: the first, the smallest and the largest value of
    each bucket, and the last point, so peaks and dips stay. NaN values are skipped.
    """
    y = np.asarray(y, dtype=float)
    ok = np.flatnonzero(~np.isnan(y))
    if len(ok) <= n:
        return ok
    y = y[ok]
    buckets = max(n // 3, 1)
    edges = np.linspace(0, len(ok), buckets + 1).astype(int)
    picks = []
    for a, b in zip(edges[:-1], edges[1:]):
        if b <= a:
            continue
        part = y[a:b]
        picks.extend((a, a + int(np.argmin(part)), a + int(np.argmax(part))))
    picks.append(len(ok) - 1)
    return ok[np.unique(picks)]


def _count(x, max_points, step):
    """Points wanted from `max_points` or `step` (exactly one), or ValueError."""
    if (max_points is None) == (step is None):
        raise ValueError("give exactly one of max_points or step (there is no default size)")
    if max_points is not None:
        if max_points < 2:
            raise ValueError(f"max_points must be at least 2, got {max_points}")
        return int(max_points)
    if not step > 0:
        raise ValueError(f"step must be positive, got {step}")
    return int(math.floor((x[-1] - x[0]) / step)) + 1


def _share(method, derivative_share):
    """The derivative share of the weight: 'derivative' is 1 unless the caller blends, others 0."""
    if method == "derivative":
        return 1.0 if derivative_share is None else derivative_share
    return 0.0 if derivative_share is None else derivative_share


def _area(y, x):
    return float((getattr(np, "trapezoid", None) or np.trapz)(y, x)) if len(y) > 1 else 0.0


def _smooth(y, window):
    """Moving average for the weights only (edges padded with the end values)."""
    if window < 3:
        return y
    window |= 1
    pad = window // 2
    padded = np.concatenate((np.full(pad, y[0]), y, np.full(pad, y[-1])))
    return np.convolve(padded, np.ones(window) / window, mode="valid")


def _weighted_index(x, y, n, share):
    """Indexes of `n` points at equal steps of the interval/derivative weight (module docstring)."""
    dx = np.diff(x)
    span = x[-1] - x[0]
    w = (1.0 - share) * dx / span if span > 0 else np.zeros_like(dx)
    if share > 0:
        dy = np.abs(np.diff(_smooth(y, len(y) // max(n, 1))))
        total = dy.sum()
        w = w + share * (dy / total if total > 0 else dx / span)
    cum = np.concatenate(([0.0], np.cumsum(w)))
    targets = np.linspace(0.0, cum[-1], n)
    hi = np.clip(np.searchsorted(cum, targets), 1, len(cum) - 1)
    lo = hi - 1
    pick = np.where(targets - cum[lo] <= cum[hi] - targets, lo, hi)
    return np.unique(np.concatenate(([0, len(x) - 1], pick)))


def _step_index(y, n):
    """Every change of a step signal (the point before and at each change), first and last."""
    change = np.flatnonzero(y[1:] != y[:-1]) + 1
    keep = np.unique(np.concatenate(([0, len(y) - 1], change, change - 1)))
    if n is not None and len(keep) > n:
        raise ValueError(f"a step signal has {len(keep)} points at its changes, more than the {n} asked; "
                         "its changes are never dropped")
    return keep


def select_index(x, y, *, max_points=None, step=None, method="interval", derivative_share=None,
                 kind="continuous", anchors=()):
    """
    Indexes into the original arrays of the points to keep, in order. Pairs with NaN in x or y
    are skipped. `max_points` or `step` (in x units) gives the size; fewer points come back when
    the curve has fewer. `anchors`: x positions whose nearest stored point is kept as well (phase
    and injection starts, either side of a pause); they may take the result past `max_points`.
    kind='step' (flow, valve positions): every change is kept, `method` is ignored.
    """
    if method not in METHODS:
        raise ValueError(f"method must be one of {', '.join(METHODS)}, got {method!r}")
    if kind not in KINDS:
        raise ValueError(f"kind must be one of {', '.join(KINDS)}, got {kind!r}")
    if derivative_share is not None and not 0.0 <= derivative_share <= 1.0:
        raise ValueError(f"derivative_share must be between 0 and 1, got {derivative_share}")
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    ok = np.flatnonzero(~(np.isnan(x) | np.isnan(y)))
    xs, ys = x[ok], y[ok]
    if len(ok) < 2:
        _count(xs if len(xs) else np.zeros(2), max_points, step)
        return ok
    n = _count(xs, max_points, step)
    if kind == "step":
        keep = _step_index(ys, n)
    elif n >= len(ok):
        keep = np.arange(len(ok))
    elif method == "minmax":
        keep = minmax_index(ys, n)
    else:
        keep = _weighted_index(xs, ys, n, _share(method, derivative_share))
    if len(anchors):
        a = np.asarray(anchors, dtype=float)
        a = a[~np.isnan(a)]
        pos = np.clip(np.searchsorted(xs, a), 1, len(xs) - 1)
        near = np.where(a - xs[pos - 1] <= xs[pos] - a, pos - 1, pos)
        keep = np.unique(np.concatenate((keep, near)))
    return ok[keep]


def jump_index(x, y, rel=0.05, limit=None):
    """
    Indexes of the points on both sides of every step transition: a change between neighbours
    (in x order) of more than `rel` of the curve's range and more than 20 times its average
    change, so steps stay square while smooth peaks are not taken for steps. At most `limit`
    transitions (the largest) when given. NaN pairs are skipped.
    """
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    ok = np.flatnonzero(~(np.isnan(x) | np.isnan(y)))
    if len(ok) < 2:
        return ok[:0]
    ys = y[ok]
    dy = np.abs(np.diff(ys))
    span = float(ys.max() - ys.min())
    found = np.flatnonzero((dy > rel * span) & (dy > 20.0 * dy.mean())) if span > 0 else np.array([], dtype=int)
    if limit is not None and len(found) > limit:
        found = found[np.argsort(dy[found])[::-1][:limit]]
    return ok[np.unique(np.concatenate((found, found + 1)))] if len(found) else ok[:0]


def add_jumps(x, y, keep, n):
    """`keep` with both sides of every step transition added (`jump_index`, at most n // 10 of them)."""
    return np.unique(np.concatenate((keep, jump_index(x, y, limit=n // 10)))).astype(int)


def hybrid_index(x, y, n, share=0.5):
    """
    Indexes of about `n` points, the page plot's and the export's 'hybrid': `share` derivative-
    and the rest interval-spaced along x, the first and last point, the smallest and largest value
    of each bucket (`minmax_index` with a quarter of `n`, so a single-sample spike stays) and
    both sides of every step transition (`jump_index`). Values are the stored ones, so the
    result can reach about 1.35 n; a curve shorter than `n` is kept whole.
    """
    keep = select_index(x, y, max_points=n, method='derivative', derivative_share=share)
    ok = np.flatnonzero(~(np.isnan(np.asarray(x, dtype=float)) | np.isnan(np.asarray(y, dtype=float))))
    extremes = ok[minmax_index(np.asarray(y, dtype=float)[ok], max(n // 4, 3))]
    return add_jumps(x, y, np.concatenate((keep, extremes)), n)


def every_index(x, y, every):
    """Every `every`-th point, the last point, and both sides of every step transition."""
    if every < 1:
        raise ValueError(f'every must be at least 1, got {every}')
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    ok = np.flatnonzero(~(np.isnan(x) | np.isnan(y)))
    if len(ok) == 0:
        return ok
    keep = np.unique(np.concatenate((ok[::every], ok[-1:])))
    return add_jumps(x, y, keep, len(keep))


def common_grid(series, *, step=None, max_points=None):
    """
    One evenly spaced grid for several curves. `series`: list of (x, y, kind) with kind
    'continuous' (linear between stored points) or 'step' (previous value held). Returns
    (grid, [y on grid, ...]). The grid runs from the earliest to the latest stored x of any
    curve; a curve is NaN outside its own recorded range (nothing is extrapolated). `step` (x
    units) or `max_points` gives the size, as in `select_index`. Gaps inside a curve (pauses) are
    bridged like any other interval; cut them first if they must stay empty.
    """
    cleaned = []
    for x, y, kind in series:
        if kind not in KINDS:
            raise ValueError(f"kind must be one of {', '.join(KINDS)}, got {kind!r}")
        x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
        keep = ~(np.isnan(x) | np.isnan(y))
        order = np.argsort(x[keep], kind="stable")
        cleaned.append((x[keep][order], y[keep][order], kind))
    if not any(len(x) for x, _, _ in cleaned):
        raise ValueError("common_grid needs points in at least one curve")
    lo = min(x[0] for x, _, _ in cleaned if len(x))
    hi = max(x[-1] for x, _, _ in cleaned if len(x))
    n = _count(np.array([lo, hi]), max_points, step)
    if step is not None:
        grid = lo + step * np.arange(n)
    else:
        grid = np.linspace(lo, hi, n)
    out = []
    for x, y, kind in cleaned:
        values = np.full(len(grid), np.nan)
        inside = (grid >= x[0]) & (grid <= x[-1]) if len(x) else np.zeros(len(grid), dtype=bool)
        if kind == "continuous":
            values[inside] = np.interp(grid[inside], x, y)
        else:
            values[inside] = y[np.searchsorted(x, grid[inside], side="right") - 1]
        out.append(values)
    return grid, out


def compare(x, y, x_thin, y_thin):
    """
    How far a thinned curve is from the stored one: {'points', 'points_thinned',
    'peak_height_diff' (thinned max - stored max, y units), 'peak_height_rel' (share of the stored
    peak), 'area_rel' (trapezoid area, thinned vs stored, share), 'retention_shift' (x of the
    thinned max - x of the stored max, x units)}. Relative values are None when the stored
    value is zero.
    """
    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    keep = ~(np.isnan(x) | np.isnan(y))
    x, y = x[keep], y[keep]
    x_thin, y_thin = np.asarray(x_thin, dtype=float), np.asarray(y_thin, dtype=float)
    if len(y) == 0 or len(y_thin) == 0:
        raise ValueError("compare needs points in both curves")
    i, j = int(np.argmax(y)), int(np.argmax(y_thin))
    area, area_thin = _area(y, x), _area(y_thin, x_thin)
    peak = float(y[i])
    return {"points": len(y), "points_thinned": len(y_thin),
            "peak_height_diff": float(y_thin[j]) - peak,
            "peak_height_rel": (float(y_thin[j]) - peak) / abs(peak) if peak else None,
            "area_rel": (area_thin - area) / abs(area) if area else None,
            "retention_shift": float(x_thin[j] - x[i])}
