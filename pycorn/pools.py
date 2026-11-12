# -*- coding: utf-8 -*-
"""
Pooled fractions: the pools a user made in UNICORN's evaluation (Operations > Pool
Fractions), one row each, and the pool each fraction of the Fractions table belongs to.

How a pooled fraction curve is found (never by a fixed name; AGENTS.md, "Curve and event
names vary"): an event curve whose stored type (`EventCurveType`) is `Fraction`, that is not
original data (made in evaluation), and that either has UNICORN's default pool ending in its
name (`,POOL`, as in 'Fraction@11,POOL'; the user may type another name) or holds an event
with a second text (`EventText2`, the pool's last fraction; the database stores it, a .zip
does not). Each of its events starts a mark that ends where the curve's next event starts
(as a fraction ends, `pycorn.event_tables`), else at the run log's last entry. A mark is a
pool when it holds two or more fractions of the original fraction curves (those of the
Fractions table: the fractions that start inside it, in time order), or stores
`EventText2`. A mark holding one fraction (an unpooled fraction) or none (a waste mark) is
not a pool.

An imported chromatogram (W17) stores every event curve as non-original: its Fractions table
reads every fraction curve except the pooled ones (`is_pooled_curve`), so a pooled curve the
user renamed without `EventText2` (a .zip) counts there as fractions.

The pool's first fraction is the event's text (`EventText`), its last `EventText2` when
stored, else its last member. A pool whose members do not run from the stored first to the
stored last fraction gives a warning.

`pool_rows(chrom)`: the rows, with start, end and length on every axis as the Fractions table
has them, plus the pool's name (first and last fraction, 'first-last'), its members and the
pooled curve. `fraction_pools(chrom, rows)`: the pool name of each Fractions table row (None
when in no pool). Tested against a lab environment, on the database and on the .zip of the
same result.
"""
import re
from collections import OrderedDict

from .event_tables import _log, _point_rows, _row

POOL_ENDING = re.compile(r",\s*POOL\s*$", re.I)
EPS = 1e-3  # in the time unit: one moment is stored a little differently on two curves (13.18167 vs 13.181671)
COLUMNS = ["number", "pool", "fractions", "n_fractions", "first_fraction", "last_fraction", "start_time", "end_time",
           "start_volume", "end_volume", "start_cv", "end_cv", "start_utc", "end_utc", "length_time",
           "length_volume", "length_cv", "end_source", "event_curve", "time_unit", "volume_unit"]


def _second_text(event):
    xml = event.xml if isinstance(getattr(event, "xml", None), dict) else {}
    text = xml.get("EventText2")
    return str(text).strip() if text not in (None, "") and str(text).strip() else None


def is_pooled_curve(ec):
    """True for a fraction curve that marks pools: its name ends ', POOL' or an event stores a last fraction."""
    return bool(ec.events) and bool(POOL_ENDING.search(ec.name or "") or any(_second_text(e) for e in ec.events))


def pooled_curves(chrom):
    """The pooled fraction curves of `chrom` (see the module docstring), in stored order."""
    return [ec for ec in chrom.event_curves_of_type("Fraction") if ec.is_original is False and is_pooled_curve(ec)]


def pool_rows(chrom, warnings=None):
    """One row per pool (`COLUMNS`); `warnings`: a list that gets a line per pool whose members do not match."""
    log = _log(chrom)
    run_end = log.events[-1] if log is not None else None
    fractions = [r for r in _point_rows(chrom, "fraction", "Fraction") if not r["pseudo"]]
    rows = []
    for ec in pooled_curves(chrom):
        events = sorted(ec.events, key=lambda e: (e.time if e.time is not None else float("inf")))
        for k, e in enumerate(events):
            end, source = (events[k + 1], "next") if k + 1 < len(events) else (run_end, "run_end")
            row = _row("pool", len(rows) + 1, None, False, e, end, source, ec, e.text)
            t0, t1 = row["start_time"], row["end_time"]
            members = [r["name"] for r in fractions if t0 is not None and r["start_time"] is not None
                       and r["start_time"] >= t0 - EPS and (t1 is None or r["start_time"] < t1 - EPS)]
            stored_last = _second_text(e)
            if stored_last is None and len(members) < 2:
                continue  # an unpooled fraction or a waste mark
            first = (e.text or "").strip() or None
            last = stored_last or members[-1]
            name = first if first == last or first is None else "%s-%s" % (first, last)
            if warnings is not None and (not members or members[0] != first or members[-1] != last):
                warnings.append("pool %r of %r: its fractions on the fraction curve (%s) do not run from %r to %r"
                                % (name, ec.name, ", ".join(members) or "none", first, last))
            out = OrderedDict([("number", row["number"]), ("pool", name), ("fractions", members),
                               ("n_fractions", len(members)), ("first_fraction", first), ("last_fraction", last)])
            out.update((c, row[c]) for c in COLUMNS if c in row and c not in out)
            rows.append(OrderedDict((c, out.get(c)) for c in COLUMNS))
    return rows


def fraction_pools(chrom, fraction_rows, pools=None):
    """The pool name of each row of the Fractions table `fraction_rows` (None: in no pool, or row 0)."""
    pools = pool_rows(chrom) if pools is None else pools
    out = []
    for r in fraction_rows:
        hit = None
        if not r.get("pseudo") and r.get("start_time") is not None:
            for p in pools:
                if r["name"] in p["fractions"] and p["start_time"] - EPS <= r["start_time"] and \
                        (p["end_time"] is None or r["start_time"] < p["end_time"] - EPS):
                    hit = p["pool"]
                    break
        out.append(hit)
    return out


def to_frame(rows):
    import pandas as pd
    return pd.DataFrame(rows, columns=COLUMNS)
