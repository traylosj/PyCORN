"""
Which curves, and how many points, go into the curves table of an export (ROADMAP M18/M19).

Three independent choices, all off unless the caller gives them (D28), none touching the CADET
files or the stored run:
  curves / curve_roles  keep only these curves (names from the caller, or roles: 'uv', ...)
  max_points + method   each curve thinned on its own axis (`pycorn.resample`): at most about
                        `max_points` points, plus both sides of every step transition
  grid_step             every curve on one evenly spaced grid of the `grid_axis`, all the
                        curve's other axes interpolated onto it; pauses are bridged
"""

import numpy as np

from .resample import add_jumps, common_grid, compare, every_index, hybrid_index, select_index

METHODS = ("interval", "derivative", "minmax", "hybrid")  # hybrid: 50% derivative + 50% interval, min/max, steps
GRID_AXES = ("time", "volume", "cv")


def check_sampling(options):
    """ValueError for choices that contradict each other, before anything is loaded."""
    thin = options.max_points is not None
    grid = options.grid_step is not None
    every = options.every_n is not None
    if thin + grid + every > 1:
        raise ValueError("--max-points, --every and --grid-step are three ways to size the curves table: give one")
    if every and options.every_n < 1:
        raise ValueError("--every must be at least 1, got %d" % options.every_n)
    if thin and options.method not in METHODS:
        raise ValueError("--max-points needs --method (%s); there is no default" % ", ".join(METHODS))
    if options.method is not None and not thin:
        raise ValueError("--method is used with --max-points only")
    if options.derivative_share is not None:
        if not thin or options.method not in ("interval", "derivative"):
            raise ValueError("--derivative-share goes with --max-points and --method interval or derivative")
        if not 0.0 <= options.derivative_share <= 1.0:
            raise ValueError("--derivative-share must be between 0 and 1, got %g" % options.derivative_share)
    if thin and options.max_points < 2:
        raise ValueError("--max-points must be at least 2, got %d" % options.max_points)
    if grid and not options.grid_step > 0:
        raise ValueError("--grid-step must be positive, got %g" % options.grid_step)
    if options.grid_axis is not None and not grid:
        raise ValueError("--grid-axis is used with --grid-step only")
    if options.grid_axis is not None and options.grid_axis not in GRID_AXES:
        raise ValueError("--grid-axis must be one of %s, got %r" % (", ".join(GRID_AXES), options.grid_axis))


def chosen_curves(chrom, names=None, roles=None, lacking=None):
    """
    {name: curve} of the curves to write, in stored order: the named ones and those of the roles.
    Nothing given: all. An unknown name or role raises ValueError listing what exists; with a
    `lacking` list, unknown names are added to it instead (the page's ticked curves, for a run
    that does not have them all).
    """
    from .curve_roles import ROLES
    curves = dict(chrom.curves)
    if not names and not roles:
        return curves
    keep = set()
    for name in names or []:
        if name not in curves and lacking is not None:
            lacking.append(name)
            continue
        if name not in curves:
            raise ValueError("no curve %r in %s; the curves are: %s" % (name, chrom.name, ", ".join(curves)))
        keep.add(name)
    for role in roles or []:
        if role not in ROLES:
            raise ValueError("unknown curve role %r; the roles are: %s" % (role, ", ".join(ROLES)))
        keep.update(c.name for c in chrom.curves_with_role(role))
    return {n: c for n, c in curves.items() if n in keep}


def _axis(curve, which):
    return {"time": curve.time, "volume": curve.volume, "cv": curve.cv}[which]


def _unit(curve, which):
    return {"time": curve.time_unit, "volume": curve.volume_unit, "cv": None}[which]


def _columns(c):
    """
    [(what, values, unit)] of one curve, in the order of the curves table. No adjusted axes:
    they are time/volume minus the retention zero, which `run_info` writes (owner, 2026-11-09).
    """
    return [("time", c.time, c.time_unit), ("volume", c.volume, c.volume_unit),
            ("CV", c.cv, None), ("UTC", c.utc, None), ("value", c.values, c.unit)]


def _as_float(values):
    """Numbers, or datetimes as ns since 1970, as floats (None: not numeric)."""
    import pandas as pd
    s = pd.Series(values)
    if pd.api.types.is_datetime64_any_dtype(s) or s.dtype == object:
        s = pd.to_datetime(s, utc=True, errors="coerce")
        return s.astype("int64").where(s.notna()).to_numpy(dtype=float), True
    return s.to_numpy(dtype=float), False


def _back(values, was_time):
    import pandas as pd
    if not was_time:
        return values
    out = pd.to_datetime(pd.Series(values).round().astype("Int64"), utc=True)
    return out.dt.tz_localize(None)


def _worst(rows):
    """The largest-magnitude (value, curve) of `rows` [(value, curve)] ignoring None."""
    rows = [(v, n) for v, n in rows if v is not None]
    return max(rows, key=lambda r: abs(r[0])) if rows else (None, None)


def _report(checks, what):
    """One warning line from [(curve, compare dict)]: the largest differences found."""
    if not checks:
        return None
    peak, peak_name = _worst([(c["peak_height_rel"], n) for n, c in checks])
    area, area_name = _worst([(c["area_rel"], n) for n, c in checks])
    shift, shift_name = _worst([(c["retention_shift"], n) for n, c in checks])
    parts = []
    if peak is not None:
        parts.append("peak height %+.2f%% (%s)" % (100 * peak, peak_name))
    if area is not None:
        parts.append("area %+.2f%% (%s)" % (100 * area, area_name))
    if shift is not None:
        parts.append("retention shift %.4g on the x axis (%s)" % (shift, shift_name))
    return "%s; largest difference to the stored curves: %s" % (what, ", ".join(parts))


def thinned(curves, options, header):
    """
    ({column header: values}, warning text or None) with each curve thinned on its own axis:
    `max_points` with a method, or `every_n`; step transitions kept. A curve with no time or
    volume axis, or with fewer points than asked, is written whole.
    """
    import pandas as pd
    columns, checks, cut = {}, [], 0
    for name, c in curves.items():
        if c.values is None:
            continue
        x = c.time if c.time is not None else c.volume
        y = np.asarray(c.values, dtype=float)
        idx = np.arange(len(y))
        if x is not None and len(y) > (options.every_n or options.max_points):
            x = np.asarray(x, dtype=float)
            if options.every_n:
                idx = every_index(x, y, options.every_n)
            elif options.method == "hybrid":
                idx = hybrid_index(x, y, options.max_points)
            else:
                idx = select_index(x, y, max_points=options.max_points, method=options.method,
                                   derivative_share=options.derivative_share)
                idx = add_jumps(x, y, idx, options.max_points)
            checks.append((name, compare(x, y, x[idx], y[idx])))
            cut += 1
        for what, values, unit in _columns(c):
            if values is not None:
                columns[header(name, what, unit)] = pd.Series(np.asarray(values)[idx] if not hasattr(values, "iloc")
                                                              else values.iloc[idx].to_numpy())
    how = "every %d%s point" % (options.every_n, "th" if options.every_n > 3 else "") if options.every_n else \
        "about %d points (%s)" % (options.max_points, options.method)
    text = _report(checks, "%d curve(s) thinned to %s" % (cut, how))
    return columns, text


def on_grid(curves, options, header):
    """
    ({column header: values}, warning text or None): a shared grid column first, then each curve's
    axes and values on it. Linear between stored points inside the curve's own range, empty
    outside it; pauses are bridged.
    """
    import pandas as pd
    which = options.grid_axis or "time"
    usable = {n: c for n, c in curves.items() if c.values is not None and _axis(c, which) is not None}
    if not usable:
        raise ValueError("no curve has a %s axis to build the grid on" % which)
    series = [(np.asarray(_axis(c, which), dtype=float), np.asarray(c.values, dtype=float), "continuous")
              for c in usable.values()]
    grid, values = common_grid(series, step=options.grid_step)
    first = next(iter(usable.values()))
    unit = _unit(first, which)
    columns = {"grid: %s (%s)" % (which, unit) if unit else "grid: %s" % which: pd.Series(grid)}
    checks = []
    for (name, c), (x, y, _), v in zip(usable.items(), series, values):
        for what, data, u in _columns(c):
            if data is None:
                continue
            if what == "value":
                out = v
            else:
                z, was_time = _as_float(data)
                ok = ~(np.isnan(x) | np.isnan(z))
                out = np.full(len(grid), np.nan)
                if ok.sum() > 1:
                    order = np.argsort(x[ok], kind="stable")
                    xs, zs = x[ok][order], z[ok][order]
                    inside = (grid >= xs[0]) & (grid <= xs[-1])
                    out[inside] = np.interp(grid[inside], xs, zs)
                out = _back(out, was_time) if was_time else out
            columns[header(name, what, u)] = pd.Series(out)
        have = ~np.isnan(v)
        if have.sum() > 1 and (~np.isnan(y)).sum() > 1:
            checks.append((name, compare(x, y, grid[have], v[have])))
    text = _report(checks, "curves on one %s grid, step %g, %d rows" % (which, options.grid_step, len(grid)))
    return columns, text
