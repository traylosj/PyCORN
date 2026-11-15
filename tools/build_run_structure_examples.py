#!/usr/bin/env python
"""
Worked examples for the owner's review of docs/RUN_STRUCTURE_RECORD.md and the scope
decisions for CADET (docs/ROADMAP.md, "Scope decisions for CADET").

    python -I tools/build_run_structure_examples.py [-o docs/examples] [--samples samples]

One HTML page per example result in samples/ (plain SVG and tables, one inline script for
zooming the figures, opens in any browser, no network) plus index.html and a prototype record JSON per run, written to docs/examples/. The
output is deterministic (no dates, no machine paths): regenerate after any change to the
readers it calls and commit the result with the change. tests/test_run_structure_examples.py
builds it into a temporary folder and checks every page.

Nothing here is part of PyCORN's API: the proposal (docs/RUN_STRUCTURE_RECORD.md) is not built,
so this script only re-reads what today's code returns (run_log_sections, delivered_flow,
event_tables, run_details, model_view, method_hints) and lays it out as the proposal would:
ISA-88 levels, set point next to actual, states, display steps, the inventory of facts UNICORN
stores (or does not) and the ten scope rows. Links point to the markdown that explains each
decision: they are resolved against the headings of the files in docs/ when the pages are
built, and the build prints a warning for any link that no longer resolves.
"""
import argparse
import glob
import json
import os
import re
import sys
import traceback
from collections import OrderedDict
from html import escape as E

import numpy as np

from pycorn import event_tables, open_run, run_details
from pycorn.curve_roles import curves_with_role
from pycorn.delivered_flow import delivered_flow, method_flow_basis, volume_axis
from pycorn.exporters.cadet.view import model_view
from pycorn.method_hints import method_hints
from pycorn.run_log_sections import run_log_sections
from pycorn.units import conversion_factor
from pycorn.uv_calibration import bypass_uv_readings
from pycorn.window_stats import parse_windows, window_stats_rows

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
ap = argparse.ArgumentParser()
ap.add_argument("-o", "--output", default=os.path.join(ROOT, "docs", "examples"))
ap.add_argument("--samples", default=os.path.join(ROOT, "samples"))
ARGS = ap.parse_args()
OUT = ARGS.output
os.makedirs(OUT, exist_ok=True)

CUTOFF_S = 10.0  # display fold (decided 2026-11-12)
FONT = "Segoe UI, Helvetica, Arial, sans-serif"
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
LEVEL_FILLS = ("#e9e8e4", "#d6d5d0")
STEP_FILLS = ("#b7d3f6", "#86b6ef")
C_PREV, C_FEED, C_LINE, C_ACT, C_MEAS, C_BYPASS, C_FRAC = "#eb6834", "#1baf7a", "#2a78d6", "#eb6834", "#8d8c88", "#8e5bd0", "#12908e"
STATE_FILL = {"running": "#d9efe4", "held": "#c9c8c4", "holding": "#f6c9b3", "restarting": "#f6c9b3",
              "paused": "#cfe0f7"}
PALETTE = ("#2a78d6", "#eb6834", "#1baf7a", "#8e5bd0", "#c9a100", "#12908e")

SAMPLE_NOTES = OrderedDict([
    ("u73_blank.zip", "Protein A method, short (blank) injection. UNICORN 7.3. A phase run twice (Equilibration), "
                      "a Hold, pauses inferred from Continue."),
    ("u711_proa_day12.zip", "The same method with a real load, evaluated (peak table, derived baseline). UNICORN 7.11."),
    ("u711_cadet_reference.zip", "The same method; the run behind the hand-checked CADET-Core reference files. "
                                 "Links its column type."),
    ("u711_column_test.zip", "Column test: one phase, loop inject, flow changes inside the phase, ended by hand. "
                             "Inlet valve B, sample inlet valve and quaternary valve installed."),
    ("u711_manual_run.zip", "Manual run: no method, no phases, no Vc. Sample pump with a sample inlet, seven Pauses, "
                            "flow changes. The case where the record has no operations."),
    ("u73_nonbinding_pulse.zip", "Non-binding pulse on a 0.7 ml column, one phase, flow change, column by-pass. "
                                 "Public forum sample, UNICORN's own CSV export exists."),
    ("sample.zip", "Same run as u73_nonbinding_pulse.zip (curve entries byte-identical), sanitized separately."),
])

STATUS = OrderedDict([
    ("stored", ("Stored", "#1baf7a", "UNICORN stores it in the result")),
    ("derived", ("Derived", "#2a78d6", "PyCORN works it out from stored data")),
    ("hint", ("Hint only", "#c9a100", "stored, but not safe to use as a value (text, template, name)")),
    ("caller", ("Caller gives", "#eb6834", "UNICORN does not know it: the user must supply it")),
    ("missing", ("Not stored", "#d03a3a", "UNICORN has no record of it in this result")),
    ("absent", ("Not on this run", "#8d8c88", "module or feature not present on this system or run")),
])


# ----------------------------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------------------------

class H(str):
    """Cell text that is already HTML."""


def esc(x):
    return x if isinstance(x, H) else E("" if x is None else str(x))


def f(v, nd=2, unit=""):
    if v is None or (isinstance(v, float) and not np.isfinite(v)):
        return "-"
    s = ("%.*f" % (nd, v)).rstrip("0").rstrip(".") if nd else "%d" % round(v)
    return s + (" " + unit if unit else "")


def badge(key, text=None):
    label, color, why = STATUS[key]
    return H('<span class="badge" style="background:%s" title="%s">%s</span>' % (color, E(why), E(text or label)))


def table(headers, rows, cls="", widths=None):
    out = ['<table class="%s"><thead><tr>%s</tr></thead><tbody>' % (cls, "".join("<th>%s</th>" % E(h) for h in headers))]
    for r in rows:
        out.append("<tr>%s</tr>" % "".join("<td>%s</td>" % esc(c) for c in r))
    out.append("</tbody></table>")
    return "\n".join(out)


def safe(name, fn, *a, **k):
    """Run one section; on failure return a visible error block (a gap in the page is a finding)."""
    try:
        return fn(*a, **k)
    except Exception as exc:  # noqa: BLE001
        tb = traceback.format_exc(limit=3)
        return '<div class="err"><b>[FAIL] %s:</b> %s<pre>%s</pre></div>' % (E(name), E(str(exc)), E(tb))


def strip_phase(name):
    return re.sub(r"^\s*Phase\s+", "", name or "").strip()


class Svg:
    def __init__(self, w):
        self.w, self.p, self.y = w, [], 0

    def text(self, x, y, s, size=12, fill=INK, anchor="start", weight="normal"):
        self.p.append('<text x="%.1f" y="%.1f" font-family="%s" font-size="%g" fill="%s" text-anchor="%s" '
                      'font-weight="%s">%s</text>' % (x, y, FONT, size, fill, anchor, weight, E(str(s))))

    def rect(self, x, y, w, h, fill, rx=0, title=None, stroke=None, dash=None, opacity=None):
        extra = ""
        if stroke:
            extra += ' stroke="%s" stroke-width="1"' % stroke
        if dash:
            extra += ' stroke-dasharray="%s"' % dash
        if opacity is not None:
            extra += ' fill-opacity="%g"' % opacity
        t = "<title>%s</title>" % E(title) if title else ""
        self.p.append('<rect x="%.2f" y="%.2f" width="%.2f" height="%.2f" fill="%s" rx="%g"%s>%s</rect>'
                      % (x, y, max(w, 1.2), h, fill, rx, extra, t))

    def line(self, x1, y1, x2, y2, stroke=GRID, width=1, dash=None):
        d = ' stroke-dasharray="%s"' % dash if dash else ""
        self.p.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" stroke-width="%g"%s/>'
                      % (x1, y1, x2, y2, stroke, width, d))

    def path(self, pts, stroke, width=1.6, dash=None, fill="none"):
        if any(q is None for q in pts):  # None marks a gap (a pause): draw the pieces apart
            piece = []
            for q in list(pts) + [None]:
                if q is None:
                    self.path(piece, stroke, width, dash, fill)
                    piece = []
                else:
                    piece.append(q)
            return
        if len(pts) < 2:
            return
        d = ' stroke-dasharray="%s"' % dash if dash else ""
        self.p.append('<path d="M %s" fill="%s" stroke="%s" stroke-width="%g" stroke-linejoin="round"%s/>'
                      % (" L ".join("%.1f %.1f" % q for q in pts), fill, stroke, width, d))

    def diamond(self, x, y, title=None):
        t = "<title>%s</title>" % E(title) if title else ""
        self.p.append('<path d="M %.1f %.1f l 6 7 l -6 7 l -6 -7 z" fill="%s">%s</path>' % (x, y, INK, t))

    def out(self):
        return ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="100%%" style="max-width:%dpx;'
                'background:%s">%s</svg>' % (self.w, self.y, self.w, SURFACE, "".join(self.p)))


def decimate(x, y, n=900):
    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    ok = np.isfinite(x) & np.isfinite(y)
    x, y = x[ok], y[ok]
    if len(x) <= 2 * n:
        return x, y
    edges = np.linspace(0, len(x), n + 1).astype(int)
    xs, ys = [], []
    for a, b in zip(edges[:-1], edges[1:]):
        if b <= a:
            continue
        seg = y[a:b]
        i, j = a + int(np.argmin(seg)), a + int(np.argmax(seg))
        for k in sorted({i, j}):
            xs.append(x[k])
            ys.append(y[k])
    return np.array(xs), np.array(ys)


def with_gaps(xs, ys, pauses, wall):
    """[(x, y)] with None where the curve crosses a pause (no data is stored while the run is held)."""
    spans = [(wall(p["elapsed_s"]) - p["duration_s"], wall(p["elapsed_s"])) for p in pauses]
    out, prev = [], None
    for a, b in zip(xs, ys):
        if prev is not None and any(prev <= s0 + 1e-6 and a >= s1 - 1e-6 for s0, s1 in spans):
            out.append(None)
        out.append((a, b))
        prev = a
    return out


def parse_volume_text(text):
    """('48 <mu>l') -> (48.0, 'ul'); the stored micro sign arrives mangled, so any non-ASCII prefix counts."""
    m = re.match(r"^\s*(-?[0-9.,]+)\s*(\S*)\s*$", text or "")
    if not m:
        return None
    unit = m.group(2)
    if unit.lower().endswith("l") and len(unit) == 2 and not unit[0].isalpha():
        unit = "ul"
    elif unit.lower().endswith("l") and len(unit) == 2 and unit[0] in "uU":
        unit = "ul"
    try:
        return float(m.group(1).replace(",", ".")), unit
    except ValueError:
        return None


def ml_of(v, unit):
    return v * {"ul": 1e-3, "ml": 1.0, "l": 1e3, "L": 1e3}.get(unit, float("nan"))


# ----------------------------------------------------------------------------------------------
# analysis of one result
# ----------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------
# links to the markdown that explains each decision
# ----------------------------------------------------------------------------------------------

REL = "../"  # pages live in docs/examples/, the markdown in docs/; never a machine path
RS, RV, RM_, CG, UG, EG = ("RUN_STRUCTURE_RECORD.md", "RUN_STRUCTURE_REVIEW.md", "ROADMAP.md", "CADET_GUIDE.md", "USER_GUIDE.md",
                           "EXPORT_GUIDE.md")
_HEADS = {}
UNLINKED = []


def _heads(fn):
    if fn not in _HEADS:
        out = []
        with open(os.path.join(ROOT, "docs", fn), encoding="utf-8") as fh:
            for line in fh:
                m = re.match(r"^#{1,6}\s+(.*?)\s*$", line)
                if m:
                    out.append(m.group(1))
        _HEADS[fn] = out
    return _HEADS[fn]


def slug(title):
    t = re.sub(r"[`*]", "", title).strip().lower()
    return re.sub(r"[^\w\- ]", "", t).replace(" ", "-")


def ref(label, fn, prefix=None, anchor=None):
    if anchor is None:
        hit = next((h for h in _heads(fn) if h.startswith(prefix)), None)
        if hit is None:
            UNLINKED.append((fn, prefix))
            return H('<span class="err">[unlinked: %s %s]</span>' % (E(fn), E(prefix)))
        anchor = slug(hit)
    return H('<a class="doc" href="%s%s#%s" title="%s">%s</a>' % (REL, fn, anchor, E(fn), E(label)))


def rs(sec):
    """Section of the proposal: rs('3.5') or rs('9')."""
    return ref("proposal §%s" % sec, RS, prefix=sec + (" " if "." in sec else ". "))


def rst(n):
    return ref("Table %s" % n, RS, anchor="table-%s" % n)


def rsf(n):
    return ref("Figure %s" % n, RS, anchor="figure-%s" % n)


def decide(n):
    """The review topic that holds Decide n (the review's own numbering)."""
    topic = {6: "3.4", 9: "3.11", 12: "3.5", 13: "3.7", 14: "3.7", 15: "3.7", 16: "3.7", 17: "3.8", 18: "3.8", 19: "3.8", 20: "3.9",
             21: "3.9", 22: "3.10", 23: "3.11", 24: "3.11", 25: "3.12", 26: "3.12"}.get(n, "3.1")
    return ref("Decide %d" % n, RV, prefix=topic + " ")


def links(*items):
    return H(" ".join(str(i) for i in items))


FACT_REFS = [
    (r"Column volume|Vc |Vc\b|geometry|liquid volume|Void volume|CV axis", lambda: links(rs("3.6.1.2"), ref("Decisions", RM_, "Decisions"))),
    (r"Mixer|tubing|Delay|loop volume|flow path volume|flow-cell|Conductivity cell|Pre-column", lambda: links(rs("3.6.1.1"), rst(7), ref("D19, D21, D27", RM_, "Decisions"))),
    (r"UV cell path|wavelength|Extinction", lambda: links(ref("Scope rows 1, 2", RM_, "Scope decisions for CADET"), ref("UV from by-pass", UG, "Extinction coefficient from the by-pass"))),
    (r"Volume axis|Time axis|UTC|Date and time", lambda: links(rs("3.8"), ref("Axes", UG, "Axes"))),
    (r"Pauses|Holds", lambda: links(rs("3.7"), rst(11), rst(12))),
    (r"Flow the pump delivered|Which pump", lambda: links(decide(6), ref("Scope row 8", RM_, "Scope decisions for CADET"))),
    (r"Inlet|inlet", lambda: links(decide(22), rst(7))),
    (r"Injection valve|Column valve|Outlet valve|Set flow|flow curve|%B ramps", lambda: links(rst(7), decide(14), decide(20))),
    (r"Buffer|BufferPro", lambda: links(rs("3.6.3.1"), rs("3.6.3.2"))),
    (r"Sample concentration|what the load", lambda: links(rs("3.6.3.3"), rs("3.9"))),
    (r"Fraction|Pool|Material ID|Peak|Injection marks", lambda: links(rs("3.6.3"), rst(9), rs("3.1"))),
    (r"Batch ID|Result \(procedure\)|Campaign|Process stage|Series", lambda: links(rs("3.1"), rst(3))),
    (r"Chromatogram|chromatogram", lambda: links(rs("3.4"), rst(5))),
    (r"Unit procedure|cycles", lambda: links(rs("3.3"), rst(4))),
    (r"column ID|Individual column", lambda: links(rs("3.6.1.2"))),
    (r"process_operation", lambda: links(rs("3.6.2"))),
    (r"Equipment module|Plant unit|Instrument|Enabled", lambda: links(rs("3.6.1"))),
    (r"Method|Scouting", lambda: links(rs("3.8"), ref("Method hints", UG, "Method hints"))),
    (r"calibration", lambda: links(rs("3.9"), ref("Calibration in the export", CG, "Your model values"))),
]


def fact_doc(fact):
    for pat, fn in FACT_REFS:
        if re.search(pat, fact):
            return fn()
    return links(rs("3.9"))


def text_doc(text):
    """Docs for one 'check these' sentence."""
    t = text.lower()
    out = []
    for key, fn in (("pause", lambda: links(rs("3.7"), rst(11))), ("flow", lambda: links(decide(15), decide(6))), ("no phases", lambda: links(rs("3.5"), rs("9"))),
                    ("lead-in", lambda: links(rst(22), decide(9))), ("repeated", lambda: links(decide(23), rst(22))), ("fold", lambda: links(decide(18), rs("3.8"))),
                    ("inlet", lambda: links(decide(22))), ("no loop volume", lambda: links(decide(19))), ("vc", lambda: links(rs("3.6.1.2"))),
                    ("scouting", lambda: links(rs("3.1"), rst(3))), ("not store", lambda: links(rs("3.6.1.1"), rst(7)))):
        if key in t:
            out.append(fn())
    return links(*out)


# ----------------------------------------------------------------------------------------------
# raw UNICORN output blocks
# ----------------------------------------------------------------------------------------------

def raw(title, source, text, note=""):
    return ('<details class="rawbox"><summary>%s <span class="src">%s</span></summary>%s<pre class="raw">%s</pre></details>'
            % (E(title), E(source), ("<p class='rawnote'>%s</p>" % E(note)) if note else "", E(text)))


def frame_text(df, limit=400):
    if df is None or not len(df):
        return "(empty)"
    s = df.to_string(index=False, max_colwidth=90)
    lines = s.split("\n")
    if len(lines) > limit:
        lines = lines[:limit] + ["... %d more rows" % (len(lines) - limit)]
    return "\n".join(lines)


def _redact(obj):
    """Drop network keys (IP addresses, ports are never read) from a stored structure."""
    if isinstance(obj, dict):
        return OrderedDict((k, _redact(v)) for k, v in obj.items() if not run_details.NETWORK.search(str(k)))
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj


def raw_html(A):
    run, ch, r = A["run"], A["ch"], A["r"]
    fr = A["frames"]
    out = []
    log = ch.run_log()
    if log is not None:
        to_s = conversion_factor(log.time_unit, "s")
        lines = []
        for e in log.events:
            text = (e.text or "").replace("\r", " ").replace("\n", " | ")
            if text.lower().startswith("batch id"):
                text = "Batch ID: <masked>"
            lines.append("%10.2f s  %9s ml  %s" % ((e.time or 0.0) * to_s, ("%.3f" % e.volume) if e.volume is not None else "-", text))
        out.append(raw("Run log (Logbook): every line UNICORN wrote", "Chrom.N.Xml, event curve 'Logbook' (%d lines)" % len(lines), "\n".join(lines),
                       "The source of every section, pause and state on this page. Time is method time in seconds (pauses excluded)."))
    ent = ["%10.2f s  %-18s %s" % (e["elapsed_s"], e["kind"], e["text"]) for e in r["entries"]]
    out.append(raw("Run-log instructions PyCORN read as executed (run_log_sections)", "derived from the Logbook: %d entries" % len(ent), "\n".join(ent),
                   "kind is PyCORN's reading; lines that match no kind are ignored for sections."))
    ev = []
    for kind in ("phase", "injection", "fraction"):
        ev.append("-- %s event curve" % kind)
        for x in event_tables.rows(ch, kind):
            ev.append("   %-26s start %9.3f min  end %9.3f min  start vol %9s ml  length %9s ml%s" % (
                (x["name"] or "")[:26], x["start_time"] or 0.0, x["end_time"] or 0.0, f(x["start_volume"], 3), f(x["length_volume"], 3), "   (pseudo row)" if x["pseudo"] else ""))
    out.append(raw("Phase, Injection and Fraction event curves", "event curves of the chromatogram", "\n".join(ev)))
    out.append(raw("System settings the run was started with", "SystemSettingData (%d rows)" % len(fr["system_settings"]) if fr["system_settings"] is not None else 0, frame_text(fr["system_settings"], 500)))
    inst = fr["instrument"]
    if inst is not None and len(inst):
        comp = inst[inst["group"] == "enabled components"]["item"].tolist()
        out.append(raw("Instrument configuration, system, mixer and UV items", "InstrumentConfigurationData, StrategyData, SystemData (network items removed)",
                       frame_text(inst[inst["group"] != "enabled components"]) + "\n\nEnabled components (%d):\n  %s" % (len(comp), "\n  ".join(map(str, comp)))))
    try:
        sysd = run_details._doc(run, "SystemData", "Systems").get("System") or {}
        import base64
        mixer = sysd.get("MixerInformation")
        if mixer:
            out.append(raw("MixerInformation as stored (base64 decoded)", "SystemData / MixerInformation",
                           base64.b64decode(mixer).decode("utf-8", "replace"), "The only stored volume on the flow path besides the delay settings."))
    except Exception as exc:  # noqa: BLE001
        out.append(raw("MixerInformation", "SystemData", "[unavailable: %s]" % exc))
    out.append(raw("Column type as stored", "ColumnTypeData (%d rows)" % len(fr["column"]) if fr["column"] is not None else 0, frame_text(fr["column"], 300)))
    out.append(raw("Calibration records", "CalibrationSettingData", frame_text(fr["calibration"])))
    try:
        out.append(raw("Method steps (as UNICORN's report lays them out)", "MethodData", frame_text(run_details.method_steps_frame(run), 300), "A template: hints only (D17)."))
        out.append(raw("Method variables as this run used them", "MethodData variables", frame_text(run.variables(), 120)))
    except Exception as exc:  # noqa: BLE001
        out.append(raw("Method", "MethodData", "[unavailable: %s]" % exc))
    cl = []
    for name, c in ch.curves.items():
        t = np.asarray(c.time, dtype=float) if c.time is not None else None
        cl.append("%-34s %-11s %-9s n=%-7d time %s..%s %s  first values %s" % (
            name[:34], c.data_type, c.unit or "-", len(c.values) if c.values is not None else 0,
            f(float(t[0]), 3) if t is not None and len(t) else "-", f(float(t[-1]), 3) if t is not None and len(t) else "-", c.time_unit or "",
            ", ".join(f(float(v), 4) for v in (c.values[:4] if c.values is not None else []))))
    out.append(raw("Curve inventory (values stay in the zip)", "Chrom.N.Xml and the point files", "\n".join(cl)))
    dl = []
    for i, (s, row) in enumerate(zip(A["S"], A["rows"])):
        if row is None:
            dl.append("%3d  pause %.1f s" % (i, s.end_s - s.start_s))
            continue
        dl.append("%3d  %8.1f-%8.1f s  pump %-6s set %7s ml/min  set x time %8s ml  axis %8s ml  %s" % (
            i, s.start_s, s.end_s, row["pump"], f(row["set_ml_min"], 3), f(row["logged_ml"], 4), f(row["axis_ml"], 4),
            ("FITTED %s: %s" % (row["kind"], "; ".join("%.1f-%.1f s @ %s ml/min slope %s" % (a, b, f(q, 3), f(sl, 4)) for a, b, q, sl in row["pieces"]))) if row["fitted"] else ""))
    out.append(raw("Delivered-flow fit, section by section", "delivered_flow.py on the volume axis", "\n".join(dl)))
    try:
        from pycorn.summary import run_summary
        out.append(raw("Today's machine output: pycorn inspect --json (masked)", "run_summary(run)", json.dumps(run_summary(run), indent=1, default=str)[:60000]))
    except Exception as exc:  # noqa: BLE001
        out.append(raw("pycorn inspect --json", "run_summary", "[unavailable: %s]" % exc))
    if A["view"]:
        out.append(raw("What CADET gets from UNICORN alone: model view, column and system", "model_view(run), no caller input",
                       json.dumps({"column": A["view"].column, "system": A["view"].system, "warnings": A["view"].warnings}, indent=1, default=str)))
    return "\n".join(out)


# ----------------------------------------------------------------------------------------------
# the JSON, made visible
# ----------------------------------------------------------------------------------------------

JSON_MAP = [
    ("record_version, source_system", "which layout and which software wrote the data", "PyCORN", "#record", [("5", None)]),
    ("batch_id", "the run's identity (masked here)", "UNICORN", "#above", [("3.1", None)]),
    ("instrument, column", "instrument model; Vc with its source", "UNICORN", "#path", [("3.6.1", None), ("3.6.1.2", None)]),
    ("procedure", "level PR: the result; chromatograms; links to campaign, series, stage, loads, fractions, pools (all caller's except fraction IDs)", "UNICORN + caller", "#above", [("3.1", None), ("3.4", None)]),
    ("unit_procedures[]", "level UP: label, boundary_source (cycles are the caller's)", "caller", "#above", [("3.3", None)]),
    ("  .operations[]", "level OP: id, display_name, original, instance, state, process_operation, start/end, column_volume set point and actual", "UNICORN + PyCORN", "#levels", [("3.5", None), ("3.8", None)]),
    ("    .phases[]", "level PH: one per UNICORN block; equipment_module", "UNICORN", "#levels", [("3.5", None), ("3.6.1", None)]),
    ("      .sections[]", "level SE: state, start/end (wall clock and method time), column_flow (set point + fitted actual with pieces), percent_b, injection_valve, inlets (known / assumed / not known), bypass", "UNICORN + volume axis", "#tables", [("3.5", None), ("3.7", None), ("3.8", None)]),
    ("states[]", "ISA-88 state bands: held, holding, restarting, paused", "PyCORN", "#levels", [("3.7", None)]),
    ("materials", "loads (set point and actual volume, material ID null = caller), fractions (ID = run + tube)", "UNICORN + caller", "#tables", [("3.6.3", None)]),
    ("volumes", "every volume on the flow path with status stored / not stored", "UNICORN", "#path", [("3.6.1.1", None)]),
    ("facts[]", "the inventory: every fact, who needs it, its status and source", "all", "#facts", [("3.9", None)]),
    ("views.display.steps[]", "display steps. A view, not part of the record: computed from sections each time", "PyCORN", "#levels", [("3.5", None)]),
    ("warnings[], not_known[]", "what was assumed, fitted or missing", "PyCORN", "#warn", [("4", None)]),
]

KEY_GLOSSARY = [
    ("level", "procedure, unit_procedure, operation, phase, section", "ISA-88 levels; section is CADET-Core's word", "table-17"),
    ("element_id", "PR, UP1, OP3, OP3/PH1, OP3/PH1/SE2", "the same key in tables, CADET and JSON, so they join", "table-17"),
    ("original", "level, text (and state for a pause or hold)", "UNICORN's own words, never lost", "table-17"),
    ("instance", "1, 2, ...", "which repeat of a phase that ran more than once", "table-17"),
    ("state", "running, held, paused, holding, restarting, stopping", "ISA-88 states: UNICORN Pause = Held, Hold = Paused", "table-17"),
    ("start, end", "wall_s, method_s (and volume, CV, UTC when built)", "placement on every axis", "table-17"),
    ("value, unit_of_measure", "number at full precision, UCUM unit", "rounding happens only at display", "table-18"),
    ("kind", "set_point, actual", "planned vs what happened", "table-18"),
    ("basis", "logged, measured, calculated, fitted, inferred, assumed, given, default, not_known", "how the number was obtained", "table-18"),
    ("source", "run_log, volume_axis, curve, system_configuration, method_hint, caller, default", "where the inputs came from", "table-18"),
]


def short(v, n=60):
    s = json.dumps(v, default=str)
    return s if len(s) <= n else s[:n] + "..."


def outline(obj, key="record", depth=0, max_depth=5):
    """Collapsible tree of a JSON value: every key with its type and an example."""
    if isinstance(obj, dict):
        inner = "".join(outline(v, k, depth + 1, max_depth) for k, v in obj.items())
        return '<details%s><summary><b>%s</b> <span class="ty">object, %d keys</span></summary><div class="ind">%s</div></details>' % (
            " open" if depth < 2 else "", E(str(key)), len(obj), inner if depth < max_depth else "<i>...</i>")
    if isinstance(obj, list):
        if not obj:
            return '<div class="leaf"><b>%s</b> <span class="ty">empty list</span></div>' % E(str(key))
        first = outline(obj[0], "[0]", depth + 1, max_depth) if isinstance(obj[0], (dict, list)) else '<div class="leaf">[0] %s</div>' % E(short(obj[0]))
        return '<details%s><summary><b>%s</b> <span class="ty">list, %d item(s); first shown</span></summary><div class="ind">%s</div></details>' % (
            " open" if depth < 2 else "", E(str(key)), len(obj), first)
    return '<div class="leaf"><b>%s</b> = <code>%s</code></div>' % (E(str(key)), E(short(obj, 90)))


def json_section(A, rec_name):
    rec = A["rec"]
    text = json.dumps(rec, indent=1, default=str)
    rows = []
    for path, holds, origin, anchor, secs in JSON_MAP:
        rows.append([H("<code>%s</code>" % E(path)), holds, origin, H('<a href="%s">on this page</a>' % anchor), links(*[rs(s) for s, _ in secs])])
    gloss = [[H("<code>%s</code>" % E(k)), v, why, ref("Table " + a.split("-")[1], RS, anchor=a)] for k, v, why, a in KEY_GLOSSARY]
    return "\n".join([
        '<p class="lead">The whole prototype record: <a href="%s">%s</a> (%d bytes, %d lines). Nothing on this page is hidden from it except the '
        'curve values, the scope rows (analysis, not record) and the layout. Read it as: structure first (map and tree), then the exact text.</p>'
        % (E(rec_name), E(rec_name), len(text), text.count("\n") + 1),
        "<h3>Map: JSON path, what it holds, where it is on this page</h3>", table(["JSON path", "holds", "origin", "shown above", "explained in"], rows, ""),
        "<h3>Field glossary (keys on every element and value)</h3>", table(["key", "values", "meaning", "defined in"], gloss, ""),
        "<h3>Tree of this run's record</h3><p class='lead'>Click to open. Lists show their first item.</p><div class='tree'>%s</div>" % outline(rec),
        "<h3>The exact JSON</h3>", raw("Prototype record, complete", rec_name, text),
    ])


def analyze(path):
    A = OrderedDict()
    A["file"] = os.path.basename(path)
    run = open_run(path)
    ch = run.chromatogram()
    r = run_log_sections(run, ch)
    S = r["sections"]
    rows, dwarn = delivered_flow(ch, S)
    A.update(run=run, ch=ch, r=r, S=S, rows=rows, dwarn=dwarn)
    A["pauses"] = r["pauses"]
    A["end_wall"] = r["end_s"]
    A["end_elapsed"] = r["end_elapsed_s"]
    pauses = r["pauses"]

    def wall(e):
        return float(e) + sum(p["duration_s"] for p in pauses if p["elapsed_s"] <= float(e) + 1e-6)

    A["wall"] = wall
    # frames from the stored result
    frames = {}
    for k in ("instrument", "system_settings", "calibration", "column", "run_details"):
        try:
            frames[k] = run_details.BUILDERS[k](run)
        except Exception as exc:  # noqa: BLE001
            frames[k] = None
            A.setdefault("frame_errors", []).append("%s: %s" % (k, exc))
    A["frames"] = frames
    inst = frames["instrument"]
    A["instrument"] = ""
    if inst is not None and len(inst):
        d = inst[(inst["group"] == "instrument configuration") & (inst["item"] == "Description")]
        A["instrument"] = str(d["value"].iloc[0]) if len(d) else ""
    try:
        A["hints"] = method_hints(run, ch, S)
    except Exception as exc:  # noqa: BLE001
        A["hints"] = None
        A.setdefault("frame_errors", []).append("method_hints: %s" % exc)
    try:
        A["view"] = model_view(run)
    except Exception as exc:  # noqa: BLE001
        A["view"] = None
        A.setdefault("frame_errors", []).append("model_view: %s" % exc)

    # operations (UNICORN phases)
    ops, seen = [], {}
    raw = [p for p in event_tables.rows(ch, "phase") if not p["pseudo"]]
    for p in raw:
        to_s = conversion_factor(p["time_unit"], "s")
        a = p["start_time"] * to_s
        b = (p["end_time"] if p["end_time"] is not None else r["end_elapsed_s"] / to_s) * to_s
        name = strip_phase(p["name"])
        seen[name] = seen.get(name, 0) + 1
        ops.append(OrderedDict(name=name, original=p["name"], instance=seen[name], a=a, b=min(b, r["end_elapsed_s"]),
                               start_volume=p["start_volume"], end_volume=p["end_volume"], start_cv=p["start_cv"],
                               end_cv=p["end_cv"], volume_unit=p.get("volume_unit"), synthetic=False))
    for op in ops:
        op["repeats"] = seen[op["name"]]
        op["display_name"] = "%s (%d)" % (op["name"], op["instance"]) if op["repeats"] > 1 else op["name"]
    if not ops:
        ops = [OrderedDict(name="Manual run (no phases)", original="(none: no method, no Phase lines)", instance=1,
                           repeats=1, a=0.0, b=r["end_elapsed_s"], start_volume=None, end_volume=None, start_cv=None,
                           end_cv=None, volume_unit=None, synthetic=True, display_name="Manual run (no phases)")]
    for i, op in enumerate(ops, 1):
        op["id"] = "OP%d" % i
        op["wa"], op["wb"] = wall(op["a"]), wall(op["b"])
    A["ops"] = ops
    A["chroms"] = run.describe_chromatograms()
    st = frames.get("instrument")
    A["system_type"] = ""
    if st is not None and len(st):
        m = st[(st["group"] == "instrument configuration") & (st["item"] == "SystemTypeName")]
        A["system_type"] = str(m["value"].iloc[0]) if len(m) else ""
    rd = frames.get("run_details")
    A["batch_id_present"] = bool(rd is not None and len(rd) and rd.astype(str).apply(lambda col: col.str.contains("Batch ID", case=False, na=False)).any().any())
    A["steps"] = build_steps(S, r["entries"], ops)
    A["states"] = build_states(S, rows)
    A["blocks"] = build_blocks(S)
    return A


def build_steps(S, entries, ops):
    """Display steps: the proposal's rule (sections joined, previous phase's set points folded within 10 s)."""
    kinds = {"flow": "system_flow", "b": "gradient"}
    all_pieces, all_steps = [], []
    for op in ops:
        pieces = []
        for s in S:
            a, b = max(s.elapsed_start_s, op["a"]), min(s.elapsed_end_s, op["b"])
            if s.paused:
                if not op["a"] - 1e-6 <= s.elapsed_start_s < op["b"] - 1e-6:
                    continue
            elif not (s.elapsed_end_s > op["a"] + 1e-6 and s.elapsed_start_s < op["b"] - 1e-6):
                continue
            wa = s.start_s + (a - s.elapsed_start_s)
            wb = s.start_s + (b - s.elapsed_start_s)
            pieces.append(dict(a=a, b=b, wa=wa, wb=wb, flow=0.0 if s.paused else (s.column_flow_ml_min or 0.0),
                               b_pct=s.percent_b[0] or 0.0, slope=s.percent_b[1], feed=bool(s.feed),
                               bypass=bool(s.bypass), paused=s.paused, held=s.held, op=op["id"], notes=[], fold_s=0.0))
        for key, kind in kinds.items():
            first = next((e["elapsed_s"] for e in entries if e["kind"] == kind
                          and op["a"] - 1e-6 <= e["elapsed_s"] < op["b"] - 1e-6), None)
            if first is None or first - op["a"] > CUTOFF_S:
                continue
            after = next((x for x in pieces if x["a"] >= first - 1e-6 and not x["paused"]), None)
            if after is None:
                continue
            for x in pieces:
                if x["b"] <= first + 1e-6 and not x["paused"]:
                    field = "flow" if key == "flow" else "b_pct"
                    if x[field] != after[field]:
                        x["fold_s"] = x["b"] - x["a"]
                        x["notes"].append("first %.1f s at %s %s (previous phase's set point)"
                                          % (x["b"] - x["a"], f(x[field], 2), "ml/min" if key == "flow" else "%B"))
                    x[field] = after[field]
        steps = []
        for x in pieces:
            if steps and not x["paused"] and not steps[-1]["paused"] and all(
                    steps[-1][k] == x[k] for k in ("flow", "b_pct", "slope", "feed", "bypass", "held")):
                steps[-1]["b"], steps[-1]["wb"] = x["b"], x["wb"]
                steps[-1]["notes"] += x["notes"]
                steps[-1]["n_sections"] += 1
            else:
                y = dict(x)
                y["n_sections"] = 1
                steps.append(y)
        all_pieces += pieces
        all_steps += steps
    return all_steps


def build_blocks(S):
    blocks, cur = [], None
    for s in S:
        if s.paused:
            continue
        if s.block and cur and cur["name"] == s.block and abs(cur["wb"] - s.start_s) < 1e-6:
            cur["wb"] = s.end_s
        elif s.block:
            cur = dict(name=s.block, wa=s.start_s, wb=s.end_s)
            blocks.append(cur)
        else:
            cur = None
    return blocks


def _trailing(row, dur):
    if not row or not row["fitted"]:
        return 0.0
    set_q = row["set_ml_min"] or 0.0
    if set_q <= 0:
        return 0.0
    for a, b, q, slope in reversed(row["pieces"]):
        qb = q + slope * (b - a)
        if max(q, qb) >= 0.98 * set_q:
            if slope < 0:
                return dur - min(a + (q - 0.98 * set_q) / -slope, b)
            return dur - b
    return dur


def _leading(row, dur):
    if not row or not row["fitted"]:
        return 0.0
    set_q = row["set_ml_min"] or 0.0
    if set_q <= 0:
        return 0.0
    for a, b, q, slope in row["pieces"]:
        qb = q + slope * (b - a)
        if max(q, qb) >= 0.98 * set_q:
            if slope > 0 and q < 0.98 * set_q:
                return min(a + (0.98 * set_q - q) / slope, dur)
            return a
    return dur


def build_states(S, rows):
    """ISA-88 state bands on the wall clock: running / held / paused (UNICORN Hold) / holding / restarting."""
    bands = []
    for i, s in enumerate(S):
        if s.paused:
            bands.append(dict(state="held", wa=s.start_s, wb=s.end_s, why="UNICORN Pause (ISA-88 Held): %s"
                              % "; ".join(s.changes)))
            continue
        d = s.end_s - s.start_s
        lead = trail = 0.0
        if i > 0 and S[i - 1].paused:
            lead = min(_leading(rows[i], d), d)
        if i + 1 < len(S) and S[i + 1].paused:
            trail = min(_trailing(rows[i], d), d)
        if lead > 0.05:
            bands.append(dict(state="restarting", wa=s.start_s, wb=s.start_s + lead,
                              why="pump ramps up after Continue (fitted to the volume axis, %.1f s)" % lead))
        mid_a, mid_b = s.start_s + (lead if lead > 0.05 else 0), s.end_s - (trail if trail > 0.05 else 0)
        if mid_b > mid_a:
            bands.append(dict(state="paused" if s.held else "running", wa=mid_a, wb=mid_b,
                              why="UNICORN Hold (ISA-88 Paused): flow runs, %B frozen" if s.held else "running"))
        if trail > 0.05:
            bands.append(dict(state="holding", wa=mid_b, wb=s.end_s,
                              why="pump ramps down before Pause (fitted to the volume axis, %.1f s)" % trail))
    return bands


# ----------------------------------------------------------------------------------------------
# figures
# ----------------------------------------------------------------------------------------------

W, L, RM = 1280, 210, 30


def xmap(A):
    end = A["end_wall"] or 1.0
    return lambda t: L + t * (W - L - RM) / end


def axis_ticks(svg, A, X, y):
    end = A["end_wall"]
    step = 60 if end <= 1500 else 300 if end <= 7200 else 900
    stepmin = step / 60.0
    t = 0.0
    while t <= end + 1e-6:
        svg.line(X(t), y - 24, X(t), y - 18, INK2)
        svg.text(X(t), y - 6, "%g" % (t / 60.0), 10.5, INK2, "middle")
        t += step
    svg.text(W - RM, y + 10, "wall-clock time (min, pauses included)", 11, INK2, "end")


def fig_levels(A):
    S, ops, X = A["S"], A["ops"], xmap(A)
    svg = Svg(W)
    y = 30
    svg.text(20, y, "Record levels, states and display steps - %s" % A["file"], 16, weight="600")
    y += 20
    svg.text(20, y, "Hover a bar for details. x: wall-clock time, pauses included (a Held band). Colors repeat shades "
                    "only to separate neighbours.", 11.5, INK2)
    y += 28

    def row(label, sub, h=26):
        nonlocal y
        top = y
        svg.text(L - 12, top + h / 2 + 4, label, 12, INK, "end", "600")
        svg.text(L - 12, top + h / 2 + 18, sub, 10, INK2, "end")
        y += h + 26
        return top, h

    end_w = A["end_wall"] or 1.0
    top, h = row("Procedure", "UNICORN: result = one run")
    svg.rect(X(0), top, X(end_w) - X(0), h, "#c7c6c1", 3, "PR: one result = one method run = one batch (ISA-88 procedure). Linked to other runs only through material and series (section 3.1).")
    svg.text(X(0) + 6, top + 17, "PR  result (batch), %s - %s min wall clock" % (A["file"], f(end_w / 60.0, 1)), 11.5)
    top, h = row("Unit procedure", "UP1 (one by default)")
    svg.rect(X(0), top, X(end_w) - X(0), h, "#d9d8d3", 3, "UP1: one unit procedure by default. Several only when the caller marks cycles (section 3.3).", stroke="#8d8c88", dash="4 3")
    svg.text(X(0) + 6, top + 17, "UP1  whole run on one unit (%s); cycles: none marked (caller's rule)" % (A["system_type"] or "system"), 11.5)
    top, h = row("Chromatogram", "data, not a level (%d)" % len(A["chroms"]), h=22)
    for i, c in enumerate(A["chroms"]):
        w = (X(end_w) - X(0)) / len(A["chroms"])
        svg.rect(X(0) + i * w, top, w - 2, h, "#eef3fb", 3, "%s (%s): how the data was recorded. Several chromatograms are joined on one clock (section 3.4)." % (c["name"], c["kind"]), stroke="#2a78d6", dash="2 2")
        svg.text(X(0) + i * w + 6, top + 15, "%s (%s)" % (c["name"], c["kind"]), 11)

    top, h = row("State", "ISA-88 (section 3.7)")
    for b in A["states"]:
        svg.rect(X(b["wa"]), top, X(b["wb"]) - X(b["wa"]), h, STATE_FILL[b["state"]], 2,
                 "%s: %.1f - %.1f s. %s" % (b["state"], b["wa"], b["wb"], b["why"]),
                 stroke="#9a9993" if b["state"] in ("held", "holding", "restarting") else None)
        if X(b["wb"]) - X(b["wa"]) > 60:
            svg.text(X(b["wa"]) + 4, top + 17, b["state"], 10, INK2)

    top, h = row("Operation", "UNICORN: phase (%d)" % len(ops))
    for i, op in enumerate(ops):
        w = X(op["wb"]) - X(op["wa"])
        tip = "%s %s: %.1f - %.1f s (method time %.1f - %.1f)" % (op["id"], op["display_name"], op["wa"], op["wb"], op["a"], op["b"])
        svg.rect(X(op["wa"]), top, w - 1, h, LEVEL_FILLS[i % 2], 3, tip,
                 dash="4 3" if op["synthetic"] else None, stroke="#8d8c88" if op["synthetic"] else None)
        label = "%s %s" % (op["id"], op["display_name"])
        if w > 110:
            svg.text(X(op["wa"]) + 5, top + 17, label, 11)
        elif w > 8:
            svg.text(X((op["wa"] + op["wb"]) / 2), top - 4 - 11 * (i % 2), label, 10, INK2, "middle")

    top, h = row("Phase", "UNICORN: block (%d)" % len(A["blocks"]))
    if not A["blocks"]:
        svg.text(L + 8, top + 17, "no Block lines in the run log: sections sit directly under the operation", 11, INK2)
    for i, bl in enumerate(A["blocks"]):
        w = X(bl["wb"]) - X(bl["wa"])
        svg.rect(X(bl["wa"]), top, w - 1, h, LEVEL_FILLS[i % 2], 3, "%s: %.1f - %.1f s" % (bl["name"], bl["wa"], bl["wb"]))
        if w > 80:
            svg.text(X(bl["wa"]) + 5, top + 17, bl["name"], 10.5)

    top, h = row("", "", h=44)
    svg.text(L - 12, top - 8, "Activities", 12, INK, "end", "600")
    for s in S:
        if s.paused:
            continue
        if s.feed:
            svg.rect(X(s.start_s), top, X(s.end_s) - X(s.start_s), 12, C_FEED, 2,
                     "sample feed: valve %s, %.1f - %.1f s" % (s.injection_valve, s.start_s, s.end_s))
        elif s.sample_use == "to_bypass":
            svg.rect(X(s.start_s), top, X(s.end_s) - X(s.start_s), 12, "#b9d9c9", 2,
                     "sample sent to the by-pass: %.1f - %.1f s" % (s.start_s, s.end_s))
        if s.bypass:
            svg.rect(X(s.start_s), top + 15, X(s.end_s) - X(s.start_s), 12, C_BYPASS, 2,
                     "column by-passed: %.1f - %.1f s (column flow 0)" % (s.start_s, s.end_s))
    try:
        for fr in event_tables.rows(A["ch"], "fraction"):
            if fr["pseudo"]:
                continue
            a, b = A["wall"](fr["start_time"] * 60), A["wall"](fr["end_time"] * 60)
            svg.rect(X(a), top + 30, X(b) - X(a), 12, C_FRAC, 1, "fraction %s: %.1f - %.1f s, %s ml"
                     % (fr["name"], a, b, f(fr["length_volume"], 2)))
    except Exception:  # noqa: BLE001
        pass
    svg.text(L - 12, top + 11, "feed", 9.5, INK2, "end")
    svg.text(L - 12, top + 26, "by-pass", 9.5, INK2, "end")
    svg.text(L - 12, top + 41, "fractions", 9.5, INK2, "end")

    top, h = row("Section", "%d; CADET-Process: Phase" % len([s for s in S if not s.paused]))
    for i, s in enumerate(S):
        if s.paused:
            svg.diamond(X(s.start_s), top + 6, "pause %.1f s at %.1f s wall clock" % (s.end_s - s.start_s, s.start_s))
            continue
        tip = "SE: %.1f - %.1f s | flow set %s ml/min (column %s) | %%B %s | valve %s | inlets %s | %s" % (
            s.start_s, s.end_s, f(s.system_flow_ml_min, 3), f(s.column_flow_ml_min, 3), f(s.percent_b[0], 1),
            s.injection_valve, s.inlets, "; ".join(s.changes))
        svg.rect(X(s.start_s), top, X(s.end_s) - X(s.start_s) - 1, h, STEP_FILLS[i % 2], 0, tip)

    svg.line(20, y - 14, W - RM, y - 14, INK2, 1, "4 4")
    svg.text(20, y - 2, "Display (not in the record: computed from the sections each time)", 10.5, INK2)
    y += 8
    steps = A["steps"]
    top, h = row("Display step", "%d (sections joined)" % len([x for x in steps if not x["paused"]]), h=30)
    n = 0
    for st in steps:
        if st["paused"]:
            svg.diamond(X(st["wa"]), top + 8, "pause")
            continue
        n += 1
        w = X(st["wb"]) - X(st["wa"])
        tip = "step %d (%s): %.1f - %.1f s | %s ml/min | %s %%B | %d section(s)%s" % (
            n, st["op"], st["wa"], st["wb"], f(st["flow"], 3), f(st["b_pct"], 1), st["n_sections"],
            " | " + "; ".join(st["notes"]) if st["notes"] else "")
        svg.rect(X(st["wa"]), top, w - 1, h, STEP_FILLS[n % 2], 0, tip)
        if st["notes"]:
            svg.rect(X(st["wa"]), top, max(X(st["wa"] + st["fold_s"]) - X(st["wa"]), 4), 7, C_PREV, 0, tip)
        if w > 70:
            svg.text(X(st["wa"]) + 4, top + 12, "%s ml/min" % f(st["flow"], 2), 10)
            svg.text(X(st["wa"]) + 4, top + 25, "%s %%B" % f(st["b_pct"], 0), 10)

    axis_ticks(svg, A, X, y + 8)
    y += 26
    lx = L
    for fill, name in ((STEP_FILLS[1], "section / display step"), (C_PREV, "orange tick: previous phase's set point folded into this step"),
                       (C_FEED, "feed"), (C_BYPASS, "column by-pass"), (C_FRAC, "fraction")):
        svg.rect(lx, y, 13, 11, fill, 2)
        svg.text(lx + 19, y + 10, name, 10.5, INK2)
        lx += 28 + 6.3 * len(name)
    y += 30
    svg.y = y
    return svg.out()


def panel(svg, A, X, y, h, title, unit, series, ymax=None, ymin=0.0, bands=True):
    """One line panel. series: list of dict(points, color, width, dash, label)."""
    allv = [q[1] for s in series for q in s["points"] if q is not None]
    if not allv:
        svg.text(L, y + 14, "%s: no data" % title, 11, INK2)
        return y + 28
    lo = ymin if ymin is not None else min(allv)
    hi = ymax if ymax is not None else max(allv)
    if hi - lo < 1e-12:
        hi = lo + 1.0
    pad = (hi - lo) * 0.06 if ymax is None else 0.0
    hi += pad
    if ymin is None:
        lo -= pad
    if bands:
        for i, op in enumerate(A["ops"]):
            if i % 2 == 0:
                svg.rect(X(op["wa"]), y, X(op["wb"]) - X(op["wa"]), h, "#f1f0ec", 0)
    for k in range(5):
        v = lo + (hi - lo) * k / 4.0
        yy = y + h - (v - lo) / (hi - lo) * h
        svg.line(L, yy, W - RM, yy, GRID)
        svg.text(L - 6, yy + 4, f(v, 2 if hi - lo < 10 else 0), 10, INK2, "end")
    for p in A["pauses"]:
        wa = A["wall"](p["elapsed_s"]) - p["duration_s"]
        svg.rect(X(wa), y, X(wa + p["duration_s"]) - X(wa), h, "#dad9d5", 0, "pause %.1f s" % p["duration_s"], opacity=0.7)
    for s in series:
        pts = [None if q is None else (X(q[0]), y + h - (q[1] - lo) / (hi - lo) * h) for q in s["points"]]
        svg.path(pts, s["color"], s.get("width", 1.6), s.get("dash"))
    svg.text(L - 40, y + h / 2 + 4, title, 12, INK, "end", "600")
    svg.text(L - 40, y + h / 2 + 18, unit, 10, INK2, "end")
    lx = L + 4
    for s in series:
        svg.line(lx, y - 7, lx + 18, y - 7, s["color"], 2.5, s.get("dash"))
        svg.text(lx + 24, y - 3, s["label"], 10, INK2)
        lx += 40 + 6.2 * len(s["label"])
    return y + h + 30


def piece_points(A, which):
    """Step points of the set flow of the counted pump ('set') or the fitted delivered flow ('actual'), wall clock."""
    S, rows = A["S"], A["rows"]
    pts = []
    for s, row in zip(S, rows):
        if s.paused:
            pts += [(s.start_s, 0.0), (s.end_s, 0.0)]
            continue
        if row is None:
            continue
        if which == "set":
            q = row["set_ml_min"] or 0.0
            pts += [(s.start_s, q), (s.end_s, q)]
        else:
            for a, b, q, slope in row["pieces"]:
                pts += [(s.start_s + a, q), (s.start_s + b, q + slope * (b - a))]
    return pts


def fig_setpoint_actual(A):
    X = xmap(A)
    svg = Svg(W)
    y = 30
    svg.text(20, y, "Set point next to actual (section 3.8)", 16, weight="600")
    y += 20
    svg.text(20, y, "Set point: run log (basis logged). Actual: volume axis, fitted pieces (basis fitted). "
                    "Measured: UNICORN's own flow curve. Where the lines differ, a Pause, a valve turn or pressure "
                    "control changed the flow.", 11.5, INK2)
    y += 36
    ch = A["ch"]
    flow_series = [dict(points=piece_points(A, "set"), color=C_LINE, width=2, label="set point (run log)"),
                   dict(points=piece_points(A, "actual"), color=C_ACT, width=1.7, label="actual (volume axis, fitted)")]
    for c in curves_with_role(ch, "flow", original_only=True)[:1]:
        t = np.asarray(c.time, dtype=float) * conversion_factor(c.time_unit, "s")
        tx, vy = decimate([A["wall"](v) for v in t], np.asarray(c.values, dtype=float) * conversion_factor(c.unit, "ml/min"))
        flow_series.append(dict(points=with_gaps(tx, vy, A["pauses"], A["wall"]), color=C_MEAS, width=1.0, label="measured: %s" % c.name))
    ymax = max([q[1] for s in flow_series for q in s["points"] if q is not None] + [1.0])
    y = panel(svg, A, X, y, 130, "Pump flow", "ml/min (counted pump)", flow_series, ymax=ymax * 1.06)
    # %B
    b_series = []
    pts = []
    for s in A["S"]:
        if s.paused:
            continue
        b0, sl = s.percent_b
        if b0 is None:
            continue
        pts += [(s.start_s, b0), (s.end_s, b0 + sl * (s.end_s - s.start_s))]
    b_series.append(dict(points=pts, color=C_LINE, width=2, label="set point (run log)"))
    for c in curves_with_role(ch, "conc_b", original_only=True)[:1]:
        t = np.asarray(c.time, dtype=float) * conversion_factor(c.time_unit, "s")
        tx, vy = decimate([A["wall"](v) for v in t], c.values)
        b_series.append(dict(points=with_gaps(tx, vy, A["pauses"], A["wall"]), color=C_MEAS, width=1.1, label="measured: %s" % c.name))
    y = panel(svg, A, X, y, 100, "%B", "% (set point vs measured)", b_series, ymax=100.0)
    axis_ticks(svg, A, X, y)
    svg.y = y + 26
    return svg.out()


def fig_curves(A):
    X = xmap(A)
    svg = Svg(W)
    y = 30
    svg.text(20, y, "Measured curves by role, on the same clock", 16, weight="600")
    y += 20
    svg.text(20, y, "Roles, not names (curve names differ by instrument). Grey bands: operations alternate. "
                    "Darker bands: pauses.", 11.5, INK2)
    y += 36
    for role, title, unit, height in (("uv", "UV", "stored unit", 110), ("conductivity", "Conductivity", "stored unit", 90),
                                      ("ph", "pH", "", 70), ("pressure", "Pressure", "stored unit", 80),
                                      ("temperature", "Temperature", "stored unit", 60)):
        series = []
        for k, c in enumerate(curves_with_role(A["ch"], role, original_only=True)[:5]):
            if c.time is None or c.values is None or len(c.values) < 2:
                continue
            vals = np.asarray(c.values, dtype=float)
            if role == "uv" and np.nanmax(vals) - np.nanmin(vals) < 1e-9:
                continue
            t = np.asarray(c.time, dtype=float) * conversion_factor(c.time_unit, "s")
            tx, vy = decimate([A["wall"](v) for v in t], vals)
            series.append(dict(points=with_gaps(tx, vy, A["pauses"], A["wall"]), color=PALETTE[k % len(PALETTE)], width=1.2,
                               label="%s (%s)" % (c.name, c.unit or "-")))
        if series:
            y = panel(svg, A, X, y, height, title, unit, series, ymin=None)
    axis_ticks(svg, A, X, y)
    svg.y = y + 26
    return svg.out()


def above_run(A):
    """What sits above and beside the operations for this run: [(row, title, lines, status, why)]."""
    ops, hints = A["ops"], A["hints"]
    real = [o for o in ops if not o["synthetic"]]
    repeats = sorted({o["name"] for o in real if o["repeats"] > 1})
    scout = (hints.scouting if hints and hints.available else None) or {}
    series = bool(scout.get("runs") or scout.get("name"))
    fr = [x for x in event_tables.rows(A["ch"], "fraction") if not x["pseudo"]]
    inj = [x for x in event_tables.rows(A["ch"], "injection") if not x["pseudo"]]
    feeds = len([1 for i, s in enumerate(A["S"]) if s.feed and (i == 0 or not A["S"][i - 1].feed)])
    pk = getattr(A["ch"], "peak_tables", None) or {}
    pl = getattr(A["ch"], "pool_tables", None) or {}
    col = ((A["view"].column or {}).get("name") if A["view"] else None) or "(no column type named)"
    bl = len(A["blocks"])
    return [
        ("chain", "Campaign / series", ["scouting table: %s" % ("%d run(s), %d variable(s)" % (len(scout.get("runs") or []), len(scout.get("variables") or [])) if series else "none"),
                                        "campaign ID: caller"], "stored" if series else "caller",
         "UNICORN stores the scouting link (name, run number) when the method has scouting; a production campaign is the caller's"),
        ("chain", "Process stage", ["capture, polishing ...", "UNICORN has none"], "caller", "ISA-88 process stage: not stored by UNICORN"),
        ("chain", "Procedure = result", ["1 per result (batch)", "Batch ID: %s (masked)" % ("stored" if A["batch_id_present"] else "not found")], "stored", "one method run; Batch ID is the run's identity"),
        ("chain", "Unit procedure", ["UP1 (1)", "cycles: caller marks" + (("; hint: %s repeats" % ", ".join(repeats)) if repeats else "")], "derived",
         "one by default; a repeat in the logged phase order is only offered as a suggested rule, never applied (section 3.3)"),
        ("chain", "Operations", ["%d (UNICORN phases)" % len(ops), "synthetic: no phases" if ops[0]["synthetic"] else "from Phase lines"], "hint" if ops[0]["synthetic"] else "stored",
         "the run log's Phase lines"),
        ("chain", "Phases", ["%d (UNICORN blocks)" % bl, "equipment module: derived"], "stored" if bl else "missing", "the run log's Block lines"),
        ("chain", "Sections", ["%d (executed instructions)" % len([s for s in A["S"] if not s.paused]), "CADET-Process: Phase"], "derived", "from what ran"),
        ("side", "Chromatograms", ["%d: %s" % (len(A["chroms"]), ", ".join("%s (%s)" % (c["name"], c["kind"]) for c in A["chroms"])[:44]), "cut settings: not known (needs a sample)"],
         "stored" if len(A["chroms"]) == 1 else "hint", "several chromatograms: how the run log records the cut is unverified (section 3.4)"),
        ("side", "Unit (equipment)", [A["instrument"][:24] or "-", "system type %s" % (A["system_type"] or "-"), "plant unit ID: caller"], "stored", "InstrumentConfigurationData; the plant's unit or skid ID is the caller's"),
        ("side", "Column (equipment)", [col[:26], "individual column ID: not stored", "use count: across runs"], "hint" if col.startswith("(") else "stored",
         "ColumnTypeData names the type; ColumnIndividualData is empty on these samples (section 3.6.1.2)"),
        ("side", "Loads (material in)", ["%d feed(s), %d injection mark(s)" % (feeds, len(inj)), "material ID: caller"], "caller" if feeds else "absent", "a feed's lot or sample ID is the caller's"),
        ("side", "Fractions / flow-through (out)", ["%d collected" % len(fr), "ID = run + tube (derived)"], "derived" if fr else "absent", "IDs PyCORN can make itself"),
        ("side", "Pools and next load", ["%d peak table(s), %d pool table(s)" % (len(pk), len(pl)), "pool to next load: caller"], "caller", "UNICORN does not store which pool became which load"),
    ]


def fig_hierarchy(A):
    items = above_run(A)
    color = {"stored": "#1baf7a", "missing": "#d03a3a", "hint": "#c9a100", "absent": "#8d8c88", "derived": "#2a78d6", "caller": "#eb6834"}
    svg = Svg(W)
    svg.text(20, 28, "Everything above and beside the operations", 16, weight="600")
    svg.text(20, 48, "Top row: the levels from linked runs down to sections (Figure 2 of the proposal), as this run fills them. "
                     "Bottom rows: data, equipment and material. Border color = who knows it (legend in the page).", 11.5, INK2)
    chain = [i for i in items if i[0] == "chain"]
    side = [i for i in items if i[0] == "side"]
    import textwrap

    def box(x, y, it, bw, bh):
        _, title, lines, st, why = it
        dash = "5 3" if st in ("caller", "missing", "absent") else None
        svg.rect(x, y, bw, bh, "#ffffff", 5, "%s: %s" % (title, why), stroke=color[st], dash=dash)
        svg.p.append('<rect x="%.1f" y="%.1f" width="5" height="%d" fill="%s"/>' % (x, y, bh, color[st]))
        svg.text(x + 12, y + 20, title, 11.5, INK, weight="600")
        wrapped = [w for line in lines for w in textwrap.wrap(line, int((bw - 20) / 5.6))]
        for k, line in enumerate(wrapped[:5]):
            svg.text(x + 12, y + 37 + 13 * k, line, 10, INK2)

    cbw, cbh = 168, 100
    gap = (W - 40 - cbw) / (len(chain) - 1)
    for i, it in enumerate(chain):
        x = 20 + i * gap
        box(x, 70, it, cbw, cbh)
        if i:
            svg.line(x - gap + cbw, 70 + cbh / 2, x, 70 + cbh / 2, "#52514e", 1.5)
            svg.p.append('<path d="M %.1f %.1f l -7 -4 l 0 8 z" fill="#52514e"/>' % (x, 70 + cbh / 2))
    svg.text(20, 70 + cbh + 22, "Above the run (campaign, stage) UNICORN stores almost nothing: those links come from the caller. "
                                "Below the run everything comes from the run log.", 10.5, INK2)
    y2 = 70 + cbh + 44
    per_row, sbw, sbh = 4, 300, 84
    gap2 = (W - 40 - sbw) / (per_row - 1)
    for k, it in enumerate(side):
        box(20 + (k % per_row) * gap2, y2 + (k // per_row) * (sbh + 18), it, sbw, sbh)
    svg.y = y2 + ((len(side) + per_row - 1) // per_row) * (sbh + 18) + 12
    return svg.out()


def fig_flowpath(A, inv):
    """Flow path of the unit with every volume UNICORN stores (green) or does not (red)."""
    vol = inv["volumes"]
    svg = Svg(W)
    svg.text(20, 28, "Flow path of this run's unit, with the volumes on it", 16, weight="600")
    svg.text(20, 48, "Green: stored by UNICORN in this result. Red dashed: UNICORN has no record, the caller must "
                     "give it. Amber: stored as text or a name only. Grey dashed: module not installed. "
                     "Hover for the source.", 11.5, INK2)
    cfg = (A["view"].system.get("inlet_configuration") if A["view"] else None) or {}

    def present(k):
        v = cfg.get(k, {})
        return v.get("present")

    nodes = [("Inlets A/B", "inlet_valve_a" if present("inlet_valve_a") else None, "A: %s, B: %s" % (
        "yes" if present("inlet_valve_a") else "no", "yes" if present("inlet_valve_b") else "no")),
             ("System pumps", "x", ""), ("Mixer", "mixer", vol["mixer"]["text"]), ("Injection valve", "x", ""),
             ("Column", "column", vol["column"]["text"]), ("UV cell", "uv", vol["uv"]["text"]),
             ("Cond / pH", "ph", vol["ph"]["text"]), ("Outlet valve", "x", ""), ("Fractions", "x", "")]
    edges = [("inlet line", vol["inlet_line"]), ("pump to mixer", vol["pump_mixer"]),
             ("mixer to inj. valve", vol["mixer_inj"]), ("inj. valve to column", vol["inj_column"]),
             ("column to UV (tubing)", vol["column_uv"]), ("UV to Cond/pH", vol["uv_ph"]),
             ("monitors to outlet valve", vol["monitor_outlet"]), ("monitors to fractions", vol["monitor_frac"])]
    n = len(nodes)
    gap = (W - 40) / n
    bw, bh, by = gap - 44, 70, 92
    color = {"stored": "#1baf7a", "missing": "#d03a3a", "hint": "#c9a100", "absent": "#8d8c88", "derived": "#2a78d6",
             "caller": "#eb6834"}
    node_status = {"mixer": "mixer", "column": "column", "uv": "uv", "ph": "ph"}

    def node(x, y, name, sub, st, tip):
        dash = "5 3" if st in ("absent", "missing") else None
        svg.rect(x, y, bw, bh, "#ffffff", 5, tip, stroke=color[st], dash=dash)
        svg.p.append('<rect x="%.1f" y="%.1f" width="5" height="%d" fill="%s"/>' % (x, y, bh, color[st]))
        svg.text(x + 12, y + 20, name, 11.5, INK, weight="600")
        for k, line in enumerate([s.strip() for s in sub.split(";")][:3]):
            svg.text(x + 12, y + 37 + 13 * k, line[:24], 10, INK2)

    def marker(x, y, num, st):
        svg.p.append('<circle cx="%.1f" cy="%.1f" r="9" fill="%s"/>' % (x, y, color[st]))
        svg.text(x, y + 4, num, 10.5, "#ffffff", "middle", "700")

    for i, (name, key, sub) in enumerate(nodes):
        x = 20 + i * gap
        st, tip = "stored", name
        if key in node_status:
            st, tip = vol[node_status[key]]["status"], vol[node_status[key]]["why"]
        elif key is None and i == 0:
            st, tip = "absent", "no inlet valve A installed"
        elif key == "x":
            tip = "equipment present (system configuration)"
        node(x, by, name, sub, st, tip)
    legend = []
    for i, (name, e) in enumerate(edges):
        x1, x2 = 20 + i * gap + bw, 20 + (i + 1) * gap
        st = e["status"]
        svg.line(x1, by + bh / 2, x2, by + bh / 2, color[st], 2.5, "5 3" if st in ("missing", "absent") else None)
        marker((x1 + x2) / 2, by + bh / 2, str(i + 1), st)
        legend.append((str(i + 1), name, e))
    # sample branch under the injection valve (node 3)
    sy = by + bh + 50
    x_inlet, x_pump, x_loop = 20 + 1 * gap, 20 + 2 * gap, 20 + 3 * gap
    for x, name, key in ((x_inlet, "Sample inlet valve", "sample_inlet_valve"), (x_pump, "Sample pump", "sample_pump")):
        ok = present(key)
        node(x, sy, name, "installed" if ok else "not installed", "stored" if ok else "absent", "%s: %s" % (name, "installed" if ok else "not installed / not listed"))
    node(x_loop, sy, "Loop / Superloop", vol["loop"]["text"], vol["loop"]["status"], vol["loop"]["why"])
    svg.line(x_inlet + bw, sy + bh / 2, x_pump, sy + bh / 2, color["stored"] if present("sample_pump") else color["absent"], 2.5)
    sp = vol["sample_path"]
    svg.line(x_pump + bw, sy + bh / 2, x_loop - 2, sy + bh / 2, color[sp["status"]], 2.5, "5 3")
    marker(x_pump + bw + (x_loop - x_pump - bw) / 2, sy + bh / 2, str(len(edges) + 1), sp["status"])
    legend.append((str(len(edges) + 1), "sample inlet / pump to injection valve", sp))
    lp = vol["loop"]
    svg.line(x_loop + bw / 2, sy, x_loop + bw / 2, by + bh, color[lp["status"]], 2.5, "5 3" if lp["status"] == "missing" else None)
    marker(x_loop + bw / 2, (sy + by + bh) / 2, str(len(edges) + 2), lp["status"])
    legend.append((str(len(edges) + 2), "loop volume", lp))
    ly = sy + bh + 34
    svg.text(20, ly, "Numbered segments (hover the figure for the source):", 11.5, INK, weight="600")
    ly += 8
    for k, (num, name, e) in enumerate(legend):
        col, row_ = k % 2, k // 2
        x = 20 + col * 620
        y = ly + 20 + row_ * 20
        marker(x + 9, y - 4, num, e["status"])
        svg.text(x + 26, y, "%s: %s" % (name, e["text"] or "-"), 11, INK2)
    svg.y = ly + 20 + ((len(legend) + 1) // 2) * 20 + 10
    return svg.out()


# ----------------------------------------------------------------------------------------------
# the inventory of facts
# ----------------------------------------------------------------------------------------------

def inventory(A):
    run, ch, S, v = A["run"], A["ch"], A["S"], A["view"]
    fr = A["frames"]
    hints = A["hints"]
    facts = []
    vol = OrderedDict()

    def add(group, fact, by, status, value, source, note=""):
        facts.append(OrderedDict(group=group, fact=fact, by=by, status=status, value=value, source=source, note=note))

    def vrec(status, text, why):
        return dict(status=status, text=text, why=why)

    sysd = (v.system if v else {}) or {}
    colv = (v.column if v else {}) or {}
    ss = fr.get("system_settings")
    inst = fr.get("instrument")
    cal = fr.get("calibration")
    colf = fr.get("column")

    # ---- volumes and the path
    vc = colv.get("vc_m3")
    if vc is not None:
        t = "Vc %s ml" % f(vc * 1e6, 3)
        add("Volumes and flow path", "Column volume Vc", "both", "stored" if colv.get("vc_source") in ("curves", "run_information") else "derived",
            t, "source: %s" % colv.get("vc_source"), "priority list of up to ten sources; the CV axis needs it")
        vol["column"] = vrec("stored", t, "Vc from %s" % colv.get("vc_source"))
    else:
        add("Volumes and flow path", "Column volume Vc", "both", "missing", "-", "no source found",
            "no CV axis, no CADET column; the caller gives column_volume=")
        vol["column"] = vrec("missing", "Vc not stored", "no Vc in the result")
    cands = A["run"].column_volume_candidates or []
    add("Volumes and flow path", "Vc sources that agree", "both", "derived" if cands else "missing",
        "%d candidate(s)" % len(cands), "; ".join(sorted({c.source for c in cands})) or "-",
        "a disagreement between sources is a warning")
    geo = colv.get("sources") or {}
    if geo:
        add("Volumes and flow path", "Column geometry: bed height, diameter, particle diameter", "CADET", "stored",
            "L %s mm, d %s mm, particle %s um" % (f((colv.get("length_m") or 0) * 1e3, 2), f((colv.get("diameter_m") or 0) * 1e3, 2),
                                                   f((colv.get("particle_radius_m") or 0) * 2e6, 1)),
            "ColumnTypeData", "only when the run named a column type; checked against Vc, never a source of Vc")
    else:
        add("Volumes and flow path", "Column geometry: bed height, diameter, particle diameter", "CADET", "missing", "-",
            "no ColumnTypeData", "caller gives bed_height= and diameter=")
    for label, key in (("Total liquid volume Vt", "Total liquid volume (Vt)"), ("Void volume Vo", "Void volume (Vo)")):
        val = None
        if colf is not None and len(colf):
            m = colf[colf["parameter"] == key]
            if len(m):
                val = "%s %s" % (f(float(m["value"].iloc[0]), 3), m["unit"].iloc[0] or "")
        add("Volumes and flow path", label, "CADET", "hint" if val else "missing", val or "-", "ColumnTypeData",
            "a library value, not measured on this packing: no porosity is computed from it")
    mix = sysd.get("mixer_m3")
    if mix is not None:
        t = "%s ml" % f(mix * 1e6, 3)
        add("Volumes and flow path", "Mixer volume", "CADET", "stored", t, "SystemData MixerInformation",
            "mixer CSTR; the caller's value wins")
        vol["mixer"] = vrec("stored", t, "stored: SystemData MixerInformation")
    else:
        add("Volumes and flow path", "Mixer volume", "CADET", "missing", "-", "-", "pass mixer_volume=")
        vol["mixer"] = vrec("missing", "not stored", "no MixerInformation")
    # delay volumes
    delays = []
    if ss is not None and len(ss):
        for _, row in ss.iterrows():
            if re.search(r"delay volume", str(row["instruction"]), re.I) or re.search(r"restrictor|cell volume", str(row["parameter"]), re.I):
                delays.append((row["group"], row["instruction"], row["parameter"], row["value"]))
    seen = set()
    delay_rec = {"outlet": None, "frac": None, "ph": []}
    for g, ins, par, val in delays:
        key = (ins, par)
        if key in seen:
            continue
        seen.add(key)
        pv = parse_volume_text(val)
        shown = "%g %s" % (pv[0], "µl" if pv[1] == "ul" else pv[1]) if pv else str(val)
        add("Volumes and flow path", "%s: %s" % (ins, par), "both", "stored", shown, "SystemSettingData (%s)" % g,
            "stored as a setting; today a hint only (not applied to the axes)")
        low = (ins + " " + par).lower()
        if "outlet" in low:
            delay_rec["outlet"] = shown
        elif "frac" in low:
            delay_rec["frac"] = shown
        elif "restrictor" in low or "ph" in low:
            delay_rec["ph"].append("%s %s" % (par.replace(" volume", ""), shown))
    if not delays:
        add("Volumes and flow path", "Delay volumes (monitor to outlet valve / fractions, pH cell)", "both", "missing", "-",
            "no 'Delay volume' setting in SystemSettingData", "fraction positions cannot be aligned to the UV by volume")
    vol["monitor_outlet"] = vrec("stored", delay_rec["outlet"], "SystemSettingData") if delay_rec["outlet"] else vrec("missing", "not stored", "no delay setting")
    vol["monitor_frac"] = vrec("stored", delay_rec["frac"], "SystemSettingData") if delay_rec["frac"] else vrec("missing", "not stored", "no delay setting")
    vol["ph"] = vrec("stored", "; ".join(delay_rec["ph"])[:34], "pH valve: restrictor and cell volumes, SystemSettingData") if delay_rec["ph"] else vrec("missing", "not stored", "no pH delay volumes")
    add("Volumes and flow path", "Column to UV tubing volume", "CADET", "caller", "-", "UNICORN stores none",
        "tubing_volume= (D27); the delay settings above start after the monitors")
    vol["column_uv"] = vrec("missing", "not stored", "UNICORN stores no tubing volume between column and UV; tubing_volume= (D27)")
    add("Volumes and flow path", "Pre-column tubing: pumps to mixer to injection valve to column", "CADET", "missing", "-",
        "UNICORN stores none", "the dead volume before the column is not modeled unless the caller gives it")
    for k, why in (("pump_mixer", "no volume stored between pump and mixer"), ("mixer_inj", "no volume stored between mixer and injection valve"),
                   ("inj_column", "no volume stored between injection valve and column"), ("inlet_line", "inlet tubing volume not stored"),
                   ("uv_ph", "UV to conductivity/pH tubing not stored")):
        vol[k] = vrec("missing", "not stored", why)
    sa = (hints.sample_application if hints and hints.available else []) or []
    loop_text = next((x.get("loop_type") for x in sa if x.get("loop_type")), None)
    if loop_text:
        add("Volumes and flow path", "Sample loop volume", "CADET", "hint", loop_text, "method: sample application dialog",
            "a template text, not what was fitted: loop_volume= from the caller (D19)")
        vol["loop"] = vrec("hint", loop_text, "method text only; caller gives loop_volume (D19)")
    else:
        add("Volumes and flow path", "Sample loop volume", "CADET", "caller", "-", "no method text", "loop_volume= (D19)")
        vol["loop"] = vrec("missing", "not stored", "no method text, caller gives loop_volume")
    add("Volumes and flow path", "Sample flow path volume (sample inlet to injection valve)", "CADET", "caller", "-",
        "UNICORN stores none", "sample_path_volume= (D21); matters for sample pump and direct inject")
    vol["sample_path"] = vrec("missing", "not stored", "sample_path_volume from the caller (D21)")
    add("Volumes and flow path", "UV flow-cell volume", "CADET", "missing", "-", "UNICORN stores none", "cell-to-cell volume (M13) is open")
    add("Volumes and flow path", "Conductivity cell volume", "CADET", "missing", "-", "UNICORN stores none", "")
    path_len = None
    if inst is not None and len(inst):
        m = inst[(inst["group"] == "UV") & inst["item"].astype(str).str.contains("Real length", na=False)]
        if len(m):
            path_len = str(m["value"].iloc[0])
    uvsig = v.primary_uv if v else None
    t = ("real %s cm" % f(float(path_len), 3)) if path_len else "-"
    add("Volumes and flow path", "UV cell path length", "both", "stored" if path_len or (uvsig and uvsig.uv_path_length_cm) else "missing",
        "%s; used %s cm (%s)" % (t, f(uvsig.uv_path_length_cm, 3) if uvsig else "-", uvsig.uv_path_basis if uvsig else "-"),
        "SystemData UVInformation and the 'UV cell path length' curve", "AU per cm needs it")
    vol["uv"] = vrec("stored" if path_len else "missing", ("path %s cm" % f(float(path_len), 2)) if path_len else "path not stored",
                     "UV cell: path length stored; cell volume not stored")

    # ---- axes
    ax = volume_axis(ch)
    add("Axes and clock", "Volume axis (counted volume per curve point)", "both", "stored" if ax is not None else "missing",
        "%d points" % len(ax[0]) if ax is not None else "-", "curve volume arrays", "the flow the pump delivered (Decide 6)")
    add("Axes and clock", "Time axis (method time)", "both", "stored", "end %s min" % f((A["end_elapsed"] or 0) / 60, 2), "curve time",
        "method time excludes pauses")
    tl = ch.timeline or {}
    add("Axes and clock", "Date and time (UTC) of the run", "both", "derived" if tl else "missing",
        ", ".join("%s" % k for k in tl.keys())[:60] if tl else "-", "method start + run-log pauses",
        "rebuilt, not stored per point")
    add("Axes and clock", "Pauses", "both", "stored" if all(p["source"] != "continue_without_pause" for p in A["pauses"]) else "derived",
        "%d (%s)" % (len(A["pauses"]), ", ".join(sorted({p["source"] for p in A["pauses"]})) or "none"), "run log / timeline",
        "inferred from a Continue without a logged Pause: the length is a timing estimate")
    add("Axes and clock", "CV axis (column volumes passed)", "both", "derived" if vc is not None else "missing",
        "needs Vc" if vc is not None else "-", "volume axis / Vc", "")

    # ---- flow and valves
    n_flow = len([e for e in A["r"]["entries"] if e["kind"] == "system_flow"])
    add("Flow, valves and composition", "Set flow per instruction", "both", "stored" if n_flow else "missing", "%d instruction(s)" % n_flow,
        "run log", "")
    fitted = len([x for x in A["rows"] if x and x["fitted"]])
    add("Flow, valves and composition", "Flow the pump delivered", "CADET", "derived", "%d of %d sections fitted" % (fitted, len([x for x in A["rows"] if x])),
        "volume axis", "Decide 6; display keeps the set flow")
    basis = method_flow_basis(ch)
    add("Flow, valves and composition", "Which pump the volume axis counts", "CADET", "stored" if basis else "derived",
        "named in the run log" if basis else "by rule (system pump, or sample pump when system flow is 0)", "(System) entry", "")
    for role, label in (("flow", "System flow curve (measured)"), ("sample_flow", "Sample flow curve (measured)")):
        cs = curves_with_role(ch, role, original_only=True)
        add("Flow, valves and composition", label, "both", "stored" if cs else "missing", cs[0].name if cs else "-", "curve", "")
    inj = sorted({s.injection_valve for s in S if s.injection_valve})
    add("Flow, valves and composition", "Injection valve positions", "both", "stored" if inj else "missing", ", ".join(inj) or "-", "run log", "")
    cvp = sorted({"%s%s" % (s.column_position or "-", " by-pass" if s.bypass else "") for s in S})
    add("Flow, valves and composition", "Column valve position / by-pass", "both", "stored" if any(s.column_position for s in S) else "missing",
        ", ".join(cvp), "run log", "column flow 0 at by-pass (D14)")
    outl = sorted({s.outlet_valve for s in S if s.outlet_valve})
    add("Flow, valves and composition", "Outlet valve positions", "both", "stored" if outl else "missing", ", ".join(outl) or "-", "run log", "")
    cfg = sysd.get("inlet_configuration") or {}
    for key, label in (("inlet_valve_a", "Inlet valve A"), ("inlet_valve_b", "Inlet valve B"), ("sample_inlet_valve", "Sample inlet valve"),
                       ("sample_pump", "Sample pump"), ("quaternary_valve", "Quaternary valve")):
        c = cfg.get(key, {})
        pres = c.get("present")
        add("Flow, valves and composition", "%s installed" % label, "both",
            "stored" if pres else ("absent" if pres is False else "missing"), c.get("state", "-"),
            "system configuration (%s)" % (", ".join(c.get("components", [])) or "-"), "")
    states = {}
    for s in S:
        for line, st in (s.inlet_states or {}).items():
            states.setdefault(line, set()).add(st.get("state", "?"))
    add("Flow, valves and composition", "Inlet positions per section", "both",
        "stored" if states and all(x == {"known"} for x in states.values()) else "derived",
        "; ".join("%s: %s" % (k, "/".join(sorted(map(str, x)))) for k, x in states.items()) or "-", "run log + system configuration",
        "known / assumed / not known (Decide 22)")
    ramps = len([s for s in S if not s.paused and abs(s.percent_b[1]) > 0])
    add("Flow, valves and composition", "%B ramps (gradient length > 0)", "both", "stored", "%d ramp section(s)" % ramps, "run log", "")
    held = len([s for s in S if s.held])
    add("Flow, valves and composition", "Holds (flow runs, %B frozen)", "both", "stored" if held else "absent", "%d section(s)" % held, "run log", "")
    add("Flow, valves and composition", "Buffer contents (components, concentrations)", "CADET", "caller", "-", "UNICORN stores only inlet and %B",
        "buffers= (D28)")
    bp = hints.buffer_prep if hints and hints.available else None
    add("Flow, valves and composition", "BufferPro recipe", "CADET", "hint" if bp else "absent", "present" if bp else "-", "NextBufferPrepData", "")
    add("Flow, valves and composition", "Sample concentration / what the load contains", "CADET", "caller", "-", "UNICORN does not store it", "feed=")
    # ---- detectors
    wl = []
    if ss is not None and len(ss):
        m = ss[(ss["instruction"] == "Wavelength")]
        wl = ["%s %s" % (r["parameter"], r["value"]) for _, r in m.iterrows()]
    add("Detectors and calibration", "UV wavelengths", "both", "stored" if wl else "missing", "; ".join(wl) or "-", "SystemSettingData", "")
    add("Detectors and calibration", "Extinction coefficient", "CADET", "caller", "-", "UNICORN stores none", "scope row 1, 2")
    cc = ""
    if cal is not None and len(cal):
        m = cal[cal["item"].astype(str).str.contains("cell constant", na=False)]
        cc = "cell constant %s %s, calibrated %s" % (f(float(m["value"].iloc[0]), 2), m["unit"].iloc[0] or "", str(m["date"].iloc[0])[:10]) if len(m) else ""
        ph = cal[cal["calibration"].astype(str).str.lower().str.startswith("ph")]
        phtxt = "slope %s %%" % f(float(ph[ph["item"].astype(str).str.contains("slope", na=False)]["value"].iloc[0]), 1) if len(ph[ph["item"].astype(str).str.contains("slope", na=False)]) else ""
    else:
        phtxt = ""
    add("Detectors and calibration", "Conductivity calibration", "both", "stored" if cc else "missing", cc or "-", "CalibrationSettingData", "")
    add("Detectors and calibration", "pH calibration", "both", "stored" if phtxt else "missing", phtxt or "-", "CalibrationSettingData", "")
    # ---- events and material
    inj_rows = [x for x in event_tables.rows(ch, "injection") if not x["pseudo"]]
    add("Events and material", "Injection marks", "both", "stored", "%d" % len(inj_rows), "Injection event curve", "")
    fr_rows = event_tables.rows(ch, "fraction")
    add("Events and material", "Fractions (tube, start, end, volume)", "both", "stored" if fr_rows else "missing",
        "%d rows (%d collected)" % (len(fr_rows), len([x for x in fr_rows if not x["pseudo"]])), "Fraction event curve", "")
    pk = getattr(ch, "peak_tables", None) or {}
    add("Events and material", "Peak tables (evaluation)", "display", "stored" if pk else "absent", "%d table(s)" % len(pk), "evaluation", "")
    pl = getattr(ch, "pool_tables", None) or {}
    add("Events and material", "Pools", "display", "stored" if pl else "absent", "%d table(s)" % len(pl), "evaluation", "")
    add("Events and material", "Material IDs (load, fractions, pools)", "both", "caller", "-", "UNICORN stores a Batch ID per run only",
        "a stored sample-ID variable is a hint at most")
    add("Events and material", "Individual column ID", "both", "missing", "-", "ColumnIndividualData (empty here)", "needed to link runs on one packing")
    # ---- above the operation
    G = "Above the operation: runs, links, identity"
    scout = (hints.scouting if hints and hints.available else None) or {}
    add(G, "Batch ID (the run's identity)", "both", "stored" if A["batch_id_present"] else "missing", "present (masked here)" if A["batch_id_present"] else "-", "run information", "the one key unique per run; the run name can repeat")
    add(G, "Result (procedure): one per method run", "both", "stored", "1", "Result.xml", "")
    add(G, "Chromatograms in the result", "both", "stored", ", ".join("%s (%s)" % (c["name"], c["kind"]) for c in A["chroms"]), "Manifest / Chrom.N.Xml",
        "imported and derived chromatograms are linked, never merged")
    add(G, "Settings carried across a New Chromatogram cut", "both", "missing", "unknown", "needs a sample with a cut", "without it, the second chromatogram's settings are not known until its first instruction")
    repeats = sorted({o["name"] for o in A["ops"] if not o["synthetic"] and o["repeats"] > 1})
    add(G, "Unit procedures / cycles", "both", "caller", "1 (default)" + ("; suggested rule: %s repeats" % ", ".join(repeats) if repeats else ""), "caller marks",
        "never inferred; by chromatogram, by phase or by series (section 3.3)")
    add(G, "Series link (scouting name, run number)", "both", "stored" if (scout.get("runs") or scout.get("name")) else "absent", scout.get("name") or "-", "method scouting", "")
    add(G, "Campaign ID", "both", "caller", "-", "UNICORN stores none", "")
    add(G, "Process stage (capture, polishing)", "both", "caller", "-", "UNICORN stores none", "ISA-88 process stage")
    add(G, "Plant unit / skid ID", "both", "caller", "system name masked", "SystemData Name", "the stored name is the instrument's, not the plant's")
    add(G, "Equipment module per phase (pump, sample feed, fractions)", "both", "derived", "from valve and event use", "run log", "ISA-88 equipment module")
    add(G, "process_operation (equilibrate, load, wash, elute ...)", "both", "caller", "(empty)", "UNICORN stores phase names only", "a phase called Wash 2 may elute: never guessed")
    add(G, "Material ID per load", "both", "caller", "-", "UNICORN stores none", "ISA-88 lot")
    add(G, "Material ID per fraction", "both", "derived", "run + tube id", "Fraction event curve", "")
    add(G, "Pool to next step's load link", "both", "caller", "-", "UNICORN stores none", "material genealogy")
    add(G, "Individual column ID and use count", "both", "missing", "-", "ColumnIndividualData (empty here)", "links runs on one packed column")
    add(G, "Buffer lot IDs", "both", "caller", "-", "UNICORN stores none", "")
    # ---- method
    nvar = len(run.variables()) if run.variables() is not None else 0
    add("Method", "Method data / variables", "display", "hint" if (hints and hints.available) else "missing", "%d variable(s)" % nvar,
        "MethodData", "the method is a template: hints only (D17)")
    add("Method", "Scouting", "display", "stored" if (hints and hints.scouting and hints.scouting.get("runs")) else "absent", "-", "method", "")
    # ---- instrument
    add("Instrument", "Instrument model, firmware, system type", "display", "stored" if A["instrument"] else "missing", A["instrument"] or "-",
        "InstrumentConfigurationData", "IP addresses and ports are never read")
    enabled = len(inst[inst["group"] == "enabled components"]) if inst is not None and len(inst) else 0
    add("Instrument", "Enabled components (modules)", "both", "stored" if enabled else "missing", "%d" % enabled, "SystemData", "")
    return dict(facts=facts, volumes=vol)


def inventory_html(inv):
    facts = inv["facts"]
    out = ['<div class="legend">%s</div>' % " ".join(str(badge(k)) + ' <span class="why">%s</span>' % E(STATUS[k][2]) for k in STATUS)]
    counts = OrderedDict((k, 0) for k in STATUS)
    for x in facts:
        counts[x["status"]] += 1
    out.append('<p class="tally">%s</p>' % " ".join("%s %d" % (str(badge(k)), n) for k, n in counts.items() if n))
    group = None
    rows = []
    for x in facts:
        if x["group"] != group:
            if rows:
                out.append(table(["Fact", "Needed by", "Status", "Value on this run", "Where", "Note", "Explained in"], rows, "inv"))
            out.append("<h4>%s</h4>" % E(x["group"]))
            group, rows = x["group"], []
        rows.append([x["fact"], x["by"], badge(x["status"]), x["value"], x["source"], x["note"], fact_doc(x["fact"])])
    out.append(table(["Fact", "Needed by", "Status", "Value on this run", "Where", "Note", "Explained in"], rows, "inv"))
    return "\n".join(out)


# ----------------------------------------------------------------------------------------------
# tables
# ----------------------------------------------------------------------------------------------

def ops_table(A):
    S, rows, ops = A["S"], A["rows"], A["ops"]
    out_rows, record_vals = [], {}
    for op in ops:
        set_ml = act_ml = feed_set = feed_act = 0.0
        for s, row in zip(S, rows):
            if s.paused:
                continue
            a, b = max(s.elapsed_start_s, op["a"]), min(s.elapsed_end_s, op["b"])
            if b <= a:
                continue
            col = s.column_flow_ml_min or 0.0
            sm = col * (b - a) / 60.0
            ratio = 1.0
            if row and row["logged_ml"] and row["axis_ml"] is not None and row["logged_ml"] > 0:
                ratio = row["axis_ml"] / row["logged_ml"]
            set_ml += sm
            act_ml += sm * ratio
            if s.feed:
                feed_set += sm
                feed_act += sm * ratio
        record_vals[op["id"]] = dict(set=set_ml, actual=act_ml, feed_set=feed_set, feed_actual=feed_act)
        diff = (act_ml - set_ml) / set_ml * 100.0 if set_ml > 0.0005 else None
        vu = op.get("volume_unit") or "ml"
        out_rows.append([op["id"], op["display_name"], op["instance"], op["original"], f(op["a"] / 60, 3), f(op["b"] / 60, 3),
                         f((op["b"] - op["a"]), 1), f(op["start_volume"], 2) if op["start_volume"] is not None else "-",
                         f(op["end_volume"], 2) if op["end_volume"] is not None else "-",
                         f(op["start_cv"], 2) if op["start_cv"] is not None else "-",
                         f(op["end_cv"], 2) if op["end_cv"] is not None else "-",
                         f(set_ml, 3), f(act_ml, 3), ("%+.1f %%" % diff) if diff is not None else "-",
                         f(feed_set, 3) if feed_set else "-", f(feed_act, 3) if feed_set else "-",
                         "(empty: caller maps)"])
    A["op_values"] = record_vals
    return table(["id", "display name", "instance", "UNICORN text (original)", "start min", "end min", "length s", "start vol",
                  "end vol", "start CV", "end CV", "column vol set point (ml)", "column vol actual (ml)", "diff",
                  "feed set (ml)", "feed actual (ml)", "process_operation"], out_rows, "wide")


def steps_table(A):
    rows = []
    n = 0
    for st in A["steps"]:
        if st["paused"]:
            rows.append(["-", st["op"], f(st["wa"], 1), f(st["wb"], 1), "-", "-", "-", "Held (pause)", "-"])
            continue
        n += 1
        flags = []
        if st["feed"]:
            flags.append("feed")
        if st["bypass"]:
            flags.append("by-pass")
        if st["held"]:
            flags.append("Hold")
        rows.append([n, st["op"], f(st["wa"], 1), f(st["wb"], 1), f(st["flow"], 3), f(st["b_pct"], 1) + (" (ramp %.3f %%/s)" % st["slope"] if abs(st["slope"]) > 0 else ""),
                     st["n_sections"], ", ".join(flags) or "-", "; ".join(st["notes"]) or "-"])
    return table(["step", "operation", "start s", "end s", "column flow ml/min", "%B", "sections joined", "activity", "note (folded set point)"],
                 rows, "wide")


def sections_table(A):
    rows = []
    for i, (s, row) in enumerate(zip(A["S"], A["rows"])):
        if s.paused:
            rows.append([i, "pause", f(s.start_s, 1), f(s.end_s, 1), f(s.end_s - s.start_s, 1), "0", "0", "-", "-", "-", "-", "-", "; ".join(s.changes)])
            continue
        lab = row["kind"] or "-" if row and row["fitted"] else "set"
        rows.append([i, s.block or s.phase or "-", f(s.start_s, 1), f(s.end_s, 1), f(s.end_s - s.start_s, 1),
                     f(row["set_ml_min"] if row else s.system_flow_ml_min, 3), f(s.column_flow_ml_min, 3),
                     f(row["logged_ml"], 4) if row and row["logged_ml"] is not None else "-",
                     f(row["axis_ml"], 4) if row and row["axis_ml"] is not None else "-",
                     (("%d piece(s), ramp %.1f s" % (len(row["pieces"]), row["ramp_s"])) if row and row["fitted"] else "as set"),
                     "%s %%B" % f(s.percent_b[0], 1), s.injection_valve or "-",
                     "; ".join(s.changes)[:140]])
    return table(["#", "block / phase", "start s", "end s", "length s", "pump set ml/min", "column set ml/min", "set x time (ml)",
                  "volume axis (ml)", "actual flow", "%B", "injection valve", "instructions that started it"], rows, "wide")


def materials_html(A):
    S, rows = A["S"], A["rows"]
    out = []
    groups, cur = [], []
    for s in S:
        if s.feed and not s.paused:
            cur.append(s)
        elif cur and not (s.paused and cur):
            groups.append(cur)
            cur = []
    if cur:
        groups.append(cur)
    feeds = []
    sa = (A["hints"].sample_application if A["hints"] and A["hints"].available else []) or []
    for i, g in enumerate(groups, 1):
        set_ml = act_ml = 0.0
        for s in g:
            k = S.index(s)
            sm = (s.column_flow_ml_min or 0) * (s.elapsed_end_s - s.elapsed_start_s) / 60.0
            r = rows[k]
            ratio = r["axis_ml"] / r["logged_ml"] if r and r["logged_ml"] and r["axis_ml"] is not None and r["logged_ml"] > 0 else 1.0
            set_ml += sm
            act_ml += sm * ratio
        feeds.append(["LOAD%d" % i, "%s -> %s s" % (f(g[0].start_s, 1), f(g[-1].end_s, 1)), g[0].injection_valve,
                      f(set_ml, 3), f(act_ml, 3), (sa[i - 1].get("sample_volume") if i - 1 < len(sa) else "-"),
                      "(caller gives the lot / sample ID)"])
    out.append("<h4>Loads (feeds)</h4>" + (table(["id", "span (wall clock)", "valve", "volume set point (ml)", "volume actual (ml)",
                                                   "method's planned sample volume", "material ID"], feeds, "")
                                           if feeds else "<p class='none'>No feed section (no injection valve at Inject / Direct inject).</p>"))
    fr = []
    for x in event_tables.rows(A["ch"], "fraction"):
        fr.append(["%s:%s" % ("RUN", x["name"]), x["name"], "waste / flow-through" if x["pseudo"] else "collected fraction",
                   f(x["start_time"], 3), f(x["end_time"], 3), f(x["length_volume"], 2), "(caller)"])
    out.append("<h4>Fractions and flow-through</h4>" + (table(["id (run + tube)", "UNICORN tube", "role", "start min", "end min", "volume ml", "material ID"], fr, "")
                                                          if fr else "<p class='none'>No fraction event curve.</p>"))
    pk = getattr(A["ch"], "peak_tables", None) or {}
    pl = getattr(A["ch"], "pool_tables", None) or {}
    out.append("<p>Peak tables: %d. Pool tables: %d (evaluation results; pools belong to the material layer, section 3.6.3).</p>" % (len(pk), len(pl)))
    return "\n".join(out)


def states_html(A):
    rows = [[b["state"], f(b["wa"], 1), f(b["wb"], 1), f(b["wb"] - b["wa"], 1), b["why"]] for b in A["states"] if b["state"] != "running"]
    if not rows:
        return "<p class='none'>Only Running: no Pause, Hold or fitted ramp around a stop in this run.</p>"
    return table(["ISA-88 state", "start s", "end s", "length s", "how it was found"], rows, "")


# ----------------------------------------------------------------------------------------------
# scope decisions
# ----------------------------------------------------------------------------------------------

def scope_html(A):
    v, run, ch, S = A["view"], A["run"], A["ch"], A["S"]
    rows = []
    cadet = "<span class='tag'>who owns it is open</span>"
    uvsig = v.primary_uv if v else None
    # 1
    if uvsig is not None and uvsig.export_values is not None:
        mx = float(np.nanmax(uvsig.export_values))
        r1 = "UV peak %s %s (%s). mg/ml needs the caller's extinction coefficient: not computed." % (f(mx, 3), uvsig.export_unit, uvsig.name)
    else:
        r1 = "no primary UV signal. Not computed."
    rows.append([1, "UV to mg/ml", badge("caller", "needs caller input"), r1, "Is the conversion a model input (CADET) or a display value (PyCORN)?"])
    # 2
    try:
        by = bypass_uv_readings(run, ch, sections=S)
        if by:
            r2 = "; ".join("window %s: %s, %s s, valve %s, absorbance/cm %s" % (b["window"], b["kind"], f(b["end_s"] - b["start_s"], 1), b["valve"], f(b["absorbance_per_cm"], 4)) for b in by[:3])
        else:
            r2 = "no window in which sample went to the by-pass."
        st2 = badge("caller", "needs caller input") if by else badge("absent")
    except Exception as exc:  # noqa: BLE001
        r2, st2 = "[FAIL] %s" % exc, badge("missing", "error")
    rows.append([2, "Extinction coefficient from the by-pass", st2, r2, "Detector calibration: PyCORN, CADET, or both?"])
    # 3
    if v and v.buffer_readings:
        b = v.buffer_readings[0]
        r3 = "%d by-pass buffer reading(s); first: conductivity %s S/m, pH %s, settled %s. Salt mol/m^3 needs the caller's buffer_salt." % (
            len(v.buffer_readings), f(b.get("conductivity_s_m"), 4), f(b.get("ph"), 2), b.get("conductivity_settled"))
        st3 = badge("derived")
    else:
        r3, st3 = "no buffer-only by-pass window, so no buffer reading and no salt calibration point.", badge("absent")
    rows.append([3, "Salt from conductivity", st3, r3, "Already CADET export only. Does it stay a PyCORN function?"])
    # 4
    ill = ""
    try:
        real = [o for o in A["ops"] if not o["synthetic"] and o["b"] - o["a"] > 90]
        uvc = curves_with_role(ch, "uv", original_only=True)
        if not real or not uvc:
            raise ValueError("needs a phase and a UV curve")
        op = real[-1]
        txt = "last_min=%s:end-1:end min" % op["name"]
        win = parse_windows([txt])
        stat = window_stats_rows(run, ch, win, [uvc[0].name])
        s0 = stat[-1] if stat else None
        ill = "Illustrative window chosen for this page, not a default: <code>%s</code> on %s: n %s, mean %s, max %s %s." % (
            E(txt), E(uvc[0].name), s0["n"], f(s0["mean"], 3), f(s0["max"], 3), E(s0["curve_unit"] or "")) if s0 else "window gave no row"
        st4 = badge("derived", "calculated on request")
    except Exception as exc:  # noqa: BLE001
        ill, st4 = "No example window: %s." % E(str(exc)), badge("absent")
    rows.append([4, "Phase window statistics (M22)", st4, H(ill), "Numerical summary of curves: display in PyCORN, or analysis in CADET?"])
    for n, what in ((5, "UV baseline, C/C0, breakthrough"), (6, "Dynamic binding capacity (M21)"), (7, "Nonlinear UV, polynomial calibration, cell-to-cell volume (M10-M13)")):
        rows.append([n, what, badge("absent", "not built"), "Nothing to show: proposed only.", "Build it in PyCORN at all, or hand the windows to CADET?"])
    fitted = [(s, r) for s, r in zip(S, A["rows"]) if r and r["fitted"]]
    if fitted:
        tot_set = sum(r["logged_ml"] or 0 for _, r in fitted)
        tot_ax = sum(r["axis_ml"] or 0 for _, r in fitted)
        r8 = "%d of %d sections fitted. In those, set flow x time %s ml, volume axis %s ml (%+.1f %%). See the set point vs actual figure." % (
            len(fitted), len([x for x in A["rows"] if x]), f(tot_set, 2), f(tot_ax, 2), (tot_ax - tot_set) / tot_set * 100.0 if tot_set else 0.0)
    else:
        r8 = "No section differs from the set flow beyond the tolerance."
    rows.append([8, "Flow the pump delivered (Decide 6)", badge("derived"), r8, "A fit of measured data: PyCORN or CADET? Wholly UNICORN-specific."])
    h = A["hints"]
    if h and h.available and h.blocks:
        hr = "; ".join("%s: planned %s %s, ran %s (%s)" % (b["block"], f(b["planned"], 2), b["base"], f(b["executed"], 2), b["status"]) for b in h.blocks[:6])
        st9 = badge("hint")
    else:
        hr, st9 = "no method in the result: no hints.", badge("absent")
    rows.append([9, "Planned vs run (method hints)", st9, hr, "Not stored by UNICORN as a result. Display aid or analysis?"])
    w = A["r"]["warnings"] + (v.warnings if v else [])
    rows.append([10, "Warnings (checks of curves against the run log)", badge("derived"), "%d warning(s) in total on this page's inputs (listed at the end)." % len(w),
                 "Borderline: a check of PyCORN's own reading, so it may stay."])
    scope = ref("roadmap table", RM_, "Scope decisions for CADET")
    row_docs = {1: links(ref("USER_GUIDE", UG, "Concentration (mg/ml) from UV")), 2: links(ref("USER_GUIDE", UG, "Extinction coefficient from the by-pass")),
                3: links(ref("CADET_GUIDE", CG, "Your model values")), 4: links(ref("USER_GUIDE", UG, "Curve statistics over phase windows"), ref("EXPORT_GUIDE", EG, "Phase window statistics")),
                5: links(ref("ROADMAP", RM_, "UV baseline and breakthrough")), 6: links(ref("ROADMAP", RM_, "UV baseline and breakthrough")),
                7: links(ref("ROADMAP", RM_, "UV concentration and signal calibration")), 8: links(decide(6), ref("CADET_GUIDE", CG, "What is written")),
                9: links(ref("ROADMAP", RM_, "Method hints"), ref("USER_GUIDE", UG, "Method hints")), 10: links(ref("CADET_GUIDE", CG, "Warnings"), ref("USER_GUIDE", UG, "Warnings and What to Check"))}
    rows = [r + [links(scope, row_docs.get(r[0], ""))] for r in rows]
    return table(["#", "Scope row", "Status on this run", "What it gives here", "Open question", "Explained in"], rows, "wide")


# ----------------------------------------------------------------------------------------------
# prototype record
# ----------------------------------------------------------------------------------------------

def val(value, unit, kind, basis, source, **extra):
    d = OrderedDict(value=None if value is None else round(float(value), 6), unit_of_measure=unit, kind=kind, basis=basis, source=source)
    d.update(extra)
    return d


def record(A):
    S, rows, ops = A["S"], A["rows"], A["ops"]
    vc = (A["view"].column or {}).get("vc_m3") if A["view"] else None
    rec = OrderedDict()
    rec["record_version"] = "prototype (docs/RUN_STRUCTURE_RECORD.md section 5)"
    rec["source_system"] = OrderedDict(name="UNICORN", version=A["run"].unicorn_version)
    rec["batch_id"] = "(UNICORN Batch ID: masked on this page)"
    rec["instrument"] = A["instrument"]
    rec["column"] = OrderedDict(vc=val(vc * 1e6 if vc else None, "mL", "set_point", "given" if False else "logged", "curves"))
    rec["procedure"] = OrderedDict(level="procedure", element_id="PR", original=OrderedDict(level="result", text=A["file"]),
                                   chromatograms=[OrderedDict(name=c["name"], kind=c["kind"]) for c in A["chroms"]],
                                   links=OrderedDict(campaign_id=None, series=None, process_stage=None, loads=[], fractions=[],
                                                     pools=[], note="all caller-supplied except fraction IDs (run + tube)"))
    ups = OrderedDict(level="unit_procedure", element_id="UP1", label="", boundary_source="default", operations=[])
    blocks_by_op = {}
    for k, (s, row) in enumerate(zip(S, rows)):
        mid = s.elapsed_start_s
        op = next((o for o in ops if o["a"] - 1e-6 <= mid < o["b"] - 1e-6), ops[-1])
        blocks_by_op.setdefault(op["id"], []).append((k, s, row))
    for op in ops:
        o = OrderedDict(level="operation", element_id=op["id"], display_name=op["display_name"], original=OrderedDict(level="phase", text=op["original"]),
                        instance=op["instance"], state="running", process_operation="",
                        start=OrderedDict(wall_s=round(op["wa"], 3), method_s=round(op["a"], 3)),
                        end=OrderedDict(wall_s=round(op["wb"], 3), method_s=round(op["b"], 3)),
                        column_volume=[val(A["op_values"][op["id"]]["set"], "mL", "set_point", "calculated", "run_log"),
                                       val(A["op_values"][op["id"]]["actual"], "mL", "actual", "calculated", "volume_axis")],
                        phases=[])
        phases = OrderedDict()
        for k, s, row in blocks_by_op.get(op["id"], []):
            pname = s.block or "(no block)"
            ph = phases.setdefault(pname, OrderedDict(level="phase", element_id="%s/PH%d" % (op["id"], len(phases) + 1),
                                                       original=OrderedDict(level="block", text=pname), equipment_module="system_pump", sections=[]))
            sec = OrderedDict(level="section", element_id="%s/SE%d" % (ph["element_id"], len(ph["sections"]) + 1), state="held" if s.paused else ("paused" if s.held else "running"),
                              start=OrderedDict(wall_s=round(s.start_s, 3), method_s=round(s.elapsed_start_s, 3)), end=OrderedDict(wall_s=round(s.end_s, 3), method_s=round(s.elapsed_end_s, 3)))
            if s.paused:
                sec["original"] = OrderedDict(level="instruction", state="pause", text="; ".join(s.changes))
            else:
                sec["column_flow"] = [val(s.column_flow_ml_min, "mL/min", "set_point", "logged", "run_log")]
                if row and row["fitted"]:
                    sec["column_flow"].append(val(row["axis_ml"] / ((s.end_s - s.start_s) / 60.0) if s.end_s > s.start_s else None, "mL/min", "actual", "fitted", "volume_axis",
                                                  pieces=[[round(a, 2), round(b, 2), round(q, 4), round(sl, 5)] for a, b, q, sl in row["pieces"]]))
                sec["percent_b"] = [val(s.percent_b[0], "%", "set_point", "logged", "run_log", slope_per_s=round(s.percent_b[1], 6))]
                sec["injection_valve"] = OrderedDict(value=s.injection_valve, original=s.injection_valve, sample_use=s.sample_use)
                sec["inlets"] = {k2: OrderedDict(position=v2, state=(s.inlet_states.get(k2) or {}).get("state")) for k2, v2 in (s.inlets or {}).items()}
                sec["bypass"] = s.bypass
            ph["sections"].append(sec)
        o["phases"] = list(phases.values())
        ups["operations"].append(o)
    rec["unit_procedures"] = [ups]
    rec["states"] = [OrderedDict(state=b["state"], start_wall_s=round(b["wa"], 3), end_wall_s=round(b["wb"], 3), how_found=b["why"]) for b in A["states"]]
    loads, cur = [], []
    for s in S:
        if s.feed and not s.paused:
            cur.append(s)
        elif cur and not s.paused:
            loads.append(cur)
            cur = []
    if cur:
        loads.append(cur)
    mats = OrderedDict(loads=[], fractions=[], pools=OrderedDict(peak_tables=len(getattr(A["ch"], "peak_tables", None) or {}),
                                                                  pool_tables=len(getattr(A["ch"], "pool_tables", None) or {})))
    for i, g in enumerate(loads, 1):
        set_ml = sum((s.column_flow_ml_min or 0) * (s.elapsed_end_s - s.elapsed_start_s) / 60.0 for s in g)
        mats["loads"].append(OrderedDict(
            element_id="LOAD%d" % i, injection_valve=g[0].injection_valve, start_wall_s=round(g[0].start_s, 3), end_wall_s=round(g[-1].end_s, 3),
            volume=[val(set_ml, "mL", "set_point", "calculated", "run_log")],
            material_id=None, material_id_source="caller (UNICORN stores none)"))
    for x in event_tables.rows(A["ch"], "fraction"):
        mats["fractions"].append(OrderedDict(element_id="FR:%s" % x["name"], role="waste_or_flow_through" if x["pseudo"] else "collected_fraction",
                                             start_min=round(x["start_time"], 4), end_min=round(x["end_time"], 4),
                                             volume=[val(x["length_volume"], "mL", "actual", "measured", "volume_axis")],
                                             material_id="RUN:%s" % x["name"], material_id_source="derived (run + tube)"))
    rec["materials"] = mats
    rec["volumes"] = OrderedDict((k, OrderedDict(status=v["status"], text=v["text"], source=v["why"])) for k, v in A["inv"]["volumes"].items())
    rec["facts"] = [OrderedDict(group=x["group"], fact=x["fact"], needed_by=x["by"], status=x["status"], value=x["value"], source=x["source"]) for x in A["inv"]["facts"]]
    rec["views"] = OrderedDict(display=OrderedDict(
        note="computed from the sections each time; never part of the stored record",
        steps=[OrderedDict(start_wall_s=round(st["wa"], 3), end_wall_s=round(st["wb"], 3), operation=st["op"],
                           state="held" if st["paused"] else "running",
                           column_flow=None if st["paused"] else val(st["flow"], "mL/min", "set_point", "logged", "run_log"),
                           percent_b=None if st["paused"] else val(st["b_pct"], "%", "set_point", "logged", "run_log"),
                           sections_joined=st.get("n_sections", 0), folded_note="; ".join(st["notes"]) or None) for st in A["steps"]]))
    rec["warnings"] = list(OrderedDict.fromkeys(list(A["r"]["warnings"]) + list(A["dwarn"]) + (list(A["view"].warnings) if A["view"] else [])))
    rec["not_known"] = [x["fact"] for x in A["inv"]["facts"] if x["status"] in ("missing", "caller")]
    return rec


# ----------------------------------------------------------------------------------------------
# checks and page
# ----------------------------------------------------------------------------------------------

def check_list(A):
    items = []
    S, rows = A["S"], A["rows"]
    inferred = [p for p in A["pauses"] if p["source"] == "continue_without_pause"]
    if inferred:
        items.append("%d pause(s) inferred from a Continue without a logged Pause (lengths %s s): is the inferred length trustworthy enough to show as a Held band?" % (
            len(inferred), ", ".join(f(p["duration_s"], 1) for p in inferred)))
    big = [(i, s, r) for i, (s, r) in enumerate(zip(S, rows)) if r and r["fitted"] and r["logged_ml"] and abs(r["axis_ml"] - r["logged_ml"]) > 0.1 * r["logged_ml"] and s.end_s - s.start_s > 5]
    if big:
        items.append("%d section(s) longer than 5 s where the pump delivered more than 10 %% off the set flow (e.g. section %d, %.1f to %.1f s): which flow should users see (Decide 15)?" % (
            len(big), big[0][0], big[0][1].start_s, big[0][1].end_s))
    ops = A["ops"]
    if ops and ops[0]["synthetic"]:
        items.append("The run has no phases: the record shows one synthetic operation. Is that the right reading of a manual run?")
    lead = [o for o in ops if o["name"].lower().startswith("method settings")]
    if lead:
        items.append("A '%s' operation of %.1f s exists only as a lead-in: keep it as an operation, or fold it?" % (lead[0]["name"], lead[0]["b"] - lead[0]["a"]))
    rep = [o for o in ops if o["repeats"] > 1]
    if rep:
        items.append("Repeated phases (%s) are named with an instance number: acceptable display wording (Decide 23)?" % ", ".join(sorted({o["display_name"] for o in rep})))
    fold = [x for x in A["steps"] if x["notes"]]
    if fold:
        items.append("%d display step(s) fold a previous-phase set point (orange): check the note wording and the 10 s rule (Decide 18)." % len(fold))
    states = {}
    for s in S:
        for line, st in (s.inlet_states or {}).items():
            states.setdefault(str(st.get("state")), set()).add(line)
    if "assumed" in states or "not known" in states:
        items.append("Inlet positions assumed or not known on line(s) %s: shown as such (Decide 22)." % ", ".join(sorted(set().union(*[v for k, v in states.items() if k != "known"]))))
    for w in (A["view"].warnings if A["view"] else []):
        m = re.match(r"feed from ([\d.]+) s to ([\d.]+) s.*no loop volume given", w)
        if m:
            items.append("The feed runs %.1f to %.1f s (%.1f min) because no loop volume is given: the whole Inject period counts as feed (D19). "
                         "For a loop inject the real feed is shorter." % (float(m.group(1)), float(m.group(2)), (float(m.group(2)) - float(m.group(1))) / 60.0))
    sc = (A["hints"].scouting if A["hints"] and A["hints"].available else None) or {}
    if sc.get("runs"):
        items.append("A scouting table is stored (%d run(s), %d variable(s)), although the coverage list still wants a scouting sample: is this one result of a "
                     "real series, and does a one-run table count as a series link (section 3.1)?" % (len(sc["runs"]), len(sc.get("variables") or [])))
    vc = (A["view"].column or {}).get("vc_m3") if A["view"] else None
    if vc is None:
        items.append("No Vc stored: no CV axis, no CADET column. Display steps and windows in CV are unavailable.")
    items.append("Volumes on the path UNICORN does not store (tubing, loop, sample path, cell volumes): see the flow path figure, red. These are the caller's inputs for CADET.")
    if A.get("frame_errors"):
        items.append("Reader errors: %s" % "; ".join(A["frame_errors"]))
    return items


CSS = """
body{font-family:Segoe UI,Helvetica,Arial,sans-serif;margin:0;background:#fcfcfb;color:#0b0b0b;line-height:1.45}
header{padding:18px 28px;background:#0b0b0b;color:#fff}
header h1{margin:0;font-size:20px} header p{margin:4px 0 0;color:#cfcfca;font-size:13px}
nav{position:sticky;top:0;background:#f1f0ec;border-bottom:1px solid #d6d5d0;padding:6px 28px;font-size:13px;z-index:5}
nav a{margin-right:14px;color:#1d4f9c;text-decoration:none} nav a:hover{text-decoration:underline}
main{padding:10px 28px 60px;max-width:1340px}
h2{margin:34px 0 6px;font-size:18px;border-bottom:2px solid #d6d5d0;padding-bottom:4px} h3{margin:18px 0 4px;font-size:15px} h4{margin:18px 0 4px;font-size:14px;color:#33322f}
p.lead{color:#52514e;margin:4px 0 10px;max-width:980px}
.tiles{display:flex;flex-wrap:wrap;gap:10px;margin:14px 0}
.tile{background:#fff;border:1px solid #d6d5d0;border-radius:6px;padding:8px 14px;min-width:110px}
.tile b{display:block;font-size:20px} .tile span{font-size:11.5px;color:#52514e}
table{border-collapse:collapse;font-size:12.5px;margin:6px 0 14px;background:#fff;width:100%}
th{background:#ecebe7;text-align:left;padding:4px 7px;border:1px solid #d6d5d0;position:sticky;top:30px}
td{padding:3px 7px;border:1px solid #e4e3df;vertical-align:top}
tr:nth-child(even) td{background:#faf9f7}
table.wide{font-size:11.5px}
.badge{color:#fff;border-radius:10px;padding:1px 8px;font-size:11px;white-space:nowrap}
.legend{margin:8px 0;font-size:12px} .legend .why{color:#52514e;margin-right:14px}
.tally{font-size:12.5px}
.checks li{margin:3px 0} .none{color:#52514e;font-style:italic}
.err{background:#fde8e8;border:1px solid #d03a3a;padding:8px;margin:8px 0} pre{font-size:11px;overflow:auto}
details{margin:8px 0} summary{cursor:pointer;font-weight:600}
code{background:#eeede9;padding:0 4px;border-radius:3px}
.fig{margin:8px 0;overflow:hidden;border:1px solid #e4e3df;border-radius:5px;background:#fcfcfb}
.zbar{padding:4px 8px;background:#f1f0ec;border-bottom:1px solid #e4e3df;font-size:12px;color:#52514e}
.zbar button{font-size:12px;margin-right:4px;min-width:28px;cursor:pointer}
pre.raw{max-height:340px;overflow:auto;background:#101418;color:#d7e0ea;padding:10px;font-size:11.5px;border-radius:5px;white-space:pre;margin:4px 0 10px}
details.rawbox{margin:8px 0;border:1px solid #d6d5d0;border-radius:5px;padding:4px 10px;background:#fff}
.src{font-weight:400;color:#52514e;font-size:11.5px;margin-left:8px} .rawnote{color:#52514e;font-size:12px;margin:2px 0}
.docs{color:#52514e;font-size:12px;margin:2px 0 8px} a.doc{background:#eef3fb;border-radius:8px;padding:0 7px;text-decoration:none;color:#1d4f9c;margin-right:3px;white-space:nowrap}
a.doc:hover{background:#d9e6fa}
.tree{background:#fff;border:1px solid #d6d5d0;border-radius:5px;padding:8px;max-height:520px;overflow:auto;font-size:12px}
.tree .ind{margin-left:16px;border-left:1px solid #e4e3df;padding-left:8px} .tree .leaf{margin:1px 0} .ty{color:#52514e;font-size:11px}
.tag{background:#eeede9;border-radius:8px;padding:0 7px;font-size:11px}
.note{background:#fff7e0;border:1px solid #e8d48a;padding:8px 12px;border-radius:5px;font-size:13px;max-width:980px}
ul.warn{font-size:12px;columns:1}
"""


ZOOM_JS = """
(function () {
  function init(box) {
    var svg = box.querySelector('svg');
    if (!svg) return;
    var vb = svg.getAttribute('viewBox').split(' ').map(Number), orig = vb.slice();
    var bar = document.createElement('div');
    bar.className = 'zbar';
    bar.innerHTML = '<button type="button" data-z="in" title="zoom in">+</button><button type="button" data-z="out" title="zoom out">&minus;</button>' +
      '<button type="button" data-z="reset" title="show the whole figure">reset</button>' +
      '<span>zoom: Ctrl+wheel, pinch or the buttons &middot; pan: drag &middot; reset: double-click</span>';
    box.insertBefore(bar, svg);
    svg.style.cursor = 'grab';
    svg.style.touchAction = 'pan-y';
    function set(v) { vb = v; svg.setAttribute('viewBox', v.join(' ')); }
    function zoom(f, cx, cy) {
      var w = Math.min(Math.max(vb[2] * f, orig[2] / 40), orig[2]), h = w * orig[3] / orig[2];
      var x = cx - (cx - vb[0]) * (w / vb[2]), y = cy - (cy - vb[1]) * (h / vb[3]);
      x = Math.min(Math.max(x, orig[0]), orig[0] + orig[2] - w);
      y = Math.min(Math.max(y, orig[1]), orig[1] + orig[3] - h);
      set([x, y, w, h]);
    }
    function point(e) {
      var r = svg.getBoundingClientRect();
      return [vb[0] + (e.clientX - r.left) / r.width * vb[2], vb[1] + (e.clientY - r.top) / r.height * vb[3]];
    }
    bar.addEventListener('click', function (e) {
      var k = e.target.getAttribute('data-z');
      if (!k) return;
      if (k === 'reset') set(orig.slice());
      else zoom(k === 'in' ? 0.6 : 1 / 0.6, vb[0] + vb[2] / 2, vb[1] + vb[3] / 2);
    });
    svg.addEventListener('wheel', function (e) {
      if (!e.ctrlKey) return;
      e.preventDefault();
      var p = point(e);
      zoom(e.deltaY < 0 ? 0.8 : 1.25, p[0], p[1]);
    }, { passive: false });
    svg.addEventListener('dblclick', function () { set(orig.slice()); });
    var drag = null;
    svg.addEventListener('pointerdown', function (e) {
      if (e.button !== 0 || vb[2] >= orig[2] - 1e-6) return;
      drag = { x: e.clientX, y: e.clientY, vb: vb.slice() };
      svg.setPointerCapture(e.pointerId);
      svg.style.cursor = 'grabbing';
    });
    svg.addEventListener('pointermove', function (e) {
      if (!drag) return;
      var r = svg.getBoundingClientRect();
      var x = drag.vb[0] - (e.clientX - drag.x) / r.width * drag.vb[2];
      var y = drag.vb[1] - (e.clientY - drag.y) / r.height * drag.vb[3];
      x = Math.min(Math.max(x, orig[0]), orig[0] + orig[2] - vb[2]);
      y = Math.min(Math.max(y, orig[1]), orig[1] + orig[3] - vb[3]);
      set([x, y, vb[2], vb[3]]);
    });
    function end() { drag = null; svg.style.cursor = 'grab'; }
    svg.addEventListener('pointerup', end);
    svg.addEventListener('pointercancel', end);
  }
  Array.prototype.forEach.call(document.querySelectorAll('.fig'), init);
})();
"""


def page(A, inv, rec_name):
    run, ch = A["run"], A["ch"]
    S = A["S"]
    vc = (A["view"].column or {}).get("vc_m3") if A["view"] else None
    steps_n = len([x for x in A["steps"] if not x["paused"]])
    tiles = [("%d" % len(A["ops"]), "operations"), ("%d" % len(A["blocks"]), "phases (blocks)"),
             ("%d" % len([s for s in S if not s.paused]), "sections"), ("%d" % steps_n, "display steps"),
             ("%d" % len(A["pauses"]), "pauses"), ("%d / %d" % (len([x for x in A["rows"] if x and x["fitted"]]), len([x for x in A["rows"] if x])), "sections fitted to the volume axis"),
             ("%s min" % f((A["end_wall"] or 0) / 60, 1), "wall-clock length"), ("%s ml" % f(vc * 1e6, 2) if vc else "none", "Vc"),
             ("%d" % len([x for x in inv["facts"] if x["status"] in ("missing", "caller")]), "facts the caller must give or UNICORN lacks")]
    warns = list(A["r"]["warnings"]) + list(A["dwarn"]) + (list(A["view"].warnings) if A["view"] else [])
    uniq = list(OrderedDict.fromkeys(warns))
    parts = ['<!doctype html><html lang="en"><head><meta charset="utf-8"><title>%s</title><style>%s</style></head><body>' % (E(A["file"]), CSS)]
    parts.append('<header><h1>%s</h1><p>%s &middot; UNICORN %s &middot; %s &middot; <a style="color:#9ec5ff" href="index.html">all examples</a> &middot; '
                 'prototype record: <a style="color:#9ec5ff" href="%s">%s</a></p></header>' % (
                     E(A["file"]), E(A["instrument"] or "instrument not stored"), E(str(run.unicorn_version)), E(SAMPLE_NOTES.get(A["file"], "")), E(rec_name), E(rec_name)))
    parts.append('<nav><a href="#checks">Check these</a><a href="#above">Above the run</a><a href="#levels">Levels</a><a href="#setact">Set point vs actual</a><a href="#curves">Curves</a>'
                 '<a href="#tables">Tables</a><a href="#path">Flow path and volumes</a><a href="#facts">Every fact</a><a href="#scope">Scope rows</a>'
                 '<a href="#record">The JSON</a><a href="#raw">Raw UNICORN output</a><a href="#warn">Warnings</a></nav><main>')
    parts.append('<p class="note">Prototype for review. The record in docs/RUN_STRUCTURE_RECORD.md is not built: every number here is what today\'s code '
                 'returns, laid out as the proposal describes. If something looks wrong, that is the finding: note the figure or table and the time.</p>')
    parts.append('<div class="tiles">%s</div>' % "".join('<div class="tile"><b>%s</b><span>%s</span></div>' % (E(a), E(b)) for a, b in tiles))
    checks = safe("checks", check_list, A)
    checks_html = checks if isinstance(checks, str) else "<ul class=\"checks\">%s</ul>" % "".join(
        "<li>%s <span class=\"docs\">%s</span></li>" % (E(x), text_doc(x)) for x in checks)

    def docs(*items):
        return '<p class="docs">Explained in: %s</p>' % links(*items)

    parts.append('<h2 id="checks">Check these first</h2><p class="lead">Generated from this run: each item is a place where a reading or a '
                 'scope choice shows up in the data. Links go to the markdown that explains the decision.</p>%s' % checks_html)
    parts.append('<h2 id="above">0. Above and beside the operations</h2><p class="lead">Linked runs, procedure, unit '
                 'procedure and cycles, chromatograms, equipment, material. Border: green UNICORN stores it, blue PyCORN works it out, amber a hint only, '
                 'orange dashed the caller supplies it, red dashed UNICORN lacks it.</p>%s<div class="fig">%s</div>' % (
                     docs(rs("3.1"), rs("3.2"), rs("3.3"), rs("3.4"), rst(3), rst(4), rst(5), rsf(2), rsf(3), rsf(6)), safe("above", fig_hierarchy, A)))
    parts.append('<h2 id="levels">1. Levels, states and display steps</h2><p class="lead">Operation = UNICORN phase, phase = '
                 'UNICORN block, section = one executed instruction. State follows ISA-88 (Pause is Held, Hold is Paused).</p>%s<div class="fig">%s</div>' % (
                     docs(rs("3.5"), rst(6), rsf(7), rs("3.7"), rst(11), rst(12), rsf(10), rst(22), rst(23), decide(23), decide(18)), safe("levels", fig_levels, A)))
    parts.append('<h2 id="setact">2. Set point next to actual</h2><p class="lead">What the method set, what the pump delivered, what UNICORN measured. '
                 'Measured traces break at pauses: no data is stored while the run is held.</p>%s<div class="fig">%s</div>' % (
                     docs(rs("3.8"), rst(13), rst(18), decide(6), decide(15), decide(18), ref("Scope row 8", RM_, "Scope decisions for CADET")), safe("setact", fig_setpoint_actual, A)))
    parts.append('<h2 id="curves">3. Measured curves by role</h2>%s<div class="fig">%s</div>' % (
        docs(ref("Finding curves by role", UG, "Finding Curves: by Role, Not by Name"), ref("Axes", UG, "Axes"), rs("5")), safe("curves", fig_curves, A)))
    parts.append('<h2 id="tables">4. Tables</h2>' + docs(rs("5"), rst(17), rst(18), rst(21), ref("Export guide", EG, "Phase window statistics")))
    parts.append("<h3>Operations, with set point and actual volumes</h3>" + safe("ops", ops_table, A))
    parts.append("<h3>Display steps (proposed table)</h3><p class='lead'>Sections joined where flow, %B, feed and activity are equal. Orange rows in the figure are the folded ones.</p>" + safe("steps", steps_table, A))
    parts.append("<h3>States other than Running</h3>" + safe("states", states_html, A))
    parts.append("<h3>Sections (the CADET side, 1:1 with today's table)</h3>" + safe("sections", sections_table, A))
    parts.append("<h3>Material: loads, fractions, pools</h3>" + safe("materials", materials_html, A))
    parts.append('<h2 id="path">5. Flow path and the volumes on it</h2><p class="lead">Which volumes UNICORN stores, which it does not. CADET needs the red ones from the caller.</p>%s<div class="fig">%s</div>' % (
        docs(rs("3.6.1"), rs("3.6.1.1"), rst(7), rsf("8a"), rsf("8b"), rs("3.6.1.2"), ref("D19, D21, D27, D28", RM_, "Decisions"), ref("CADET guide: model values", CG, "Your model values")),
        safe("path", fig_flowpath, A, inv)))
    parts.append('<h2 id="facts">6. Every fact UNICORN provides (or does not) that display or CADET could need</h2>%s%s' % (
        docs(rs("3.9"), rs("9"), rs("10")), safe("inventory", inventory_html, inv)))
    parts.append('<h2 id="scope">7. The ten scope rows on this run</h2><p class="lead">Nothing is decided; this shows what each row produces here.</p>%s%s' % (
        docs(ref("ROADMAP: Scope decisions for CADET", RM_, "Scope decisions for CADET")), safe("scope", scope_html, A)))
    parts.append('<h2 id="record">8. The JSON: what is in it</h2>%s%s' % (docs(rs("5"), rst(17), rst(18), rst(19), rs("7"), rs("8")), safe("json", json_section, A, rec_name)))
    parts.append('<h2 id="raw">9. Raw UNICORN output, as extracted</h2><p class="lead">The text and tables PyCORN read from the result, scrollable. Everything above is derived from these.</p>%s%s' % (
        docs(rs("3.2"), rs("3.4"), ref("Run structure review: evidence", RV, "5. Evidence")), safe("raw", raw_html, A)))
    parts.append('<h2 id="warn">10. Warnings from today\'s readers</h2><ul class="warn">%s</ul>' % "".join("<li>%s</li>" % E(w) for w in uniq))
    parts.append("</main><script>%s</script></body></html>" % ZOOM_JS)
    return "\n".join(parts)


def index_page(results):
    rows = []
    for A, ok in results:
        if not ok:
            rows.append([A["file"], H('<span class="err">%s</span>' % E(A.get("error", "")))] + [""] * 6)
            continue
        inv = A["inv"]
        rows.append([H('<a href="%s.html">%s</a>' % (E(os.path.splitext(A["file"])[0]), E(A["file"]))), SAMPLE_NOTES.get(A["file"], ""),
                     len(A["ops"]), len([s for s in A["S"] if not s.paused]), len(A["pauses"]),
                     "%d / %d" % (len([x for x in A["rows"] if x and x["fitted"]]), len([x for x in A["rows"] if x])),
                     len([x for x in inv["facts"] if x["status"] == "missing"]), len([x for x in inv["facts"] if x["status"] == "caller"])])
    p = ['<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Run structure: worked examples</title><style>%s</style></head><body>' % CSS,
         "<header><h1>Run structure and scope decisions: worked examples</h1><p>One page per example result in samples/. Prototype for the owner's review "
         "of docs/RUN_STRUCTURE_RECORD.md and ROADMAP \"Scope decisions for CADET\". Generated by tools/build_run_structure_examples.py.</p></header><main>",
         "<h2>How to read a page</h2><ol><li><b>Check these first</b>: places in this run that test a decision.</li>"
         "<li><b>Levels</b>: operation, phase, section, display step and ISA-88 state on one clock.</li>"
         "<li><b>Set point vs actual</b>: what the method set, what the pump delivered, what UNICORN measured.</li>"
         "<li><b>Flow path and volumes</b>, then <b>every fact</b>: stored, derived, hint only, caller gives, not stored.</li>"
         "<li><b>Scope rows</b>: the ten rows of the roadmap, as they come out on this run.</li></ol>",
         "<h2>Examples</h2>", table(["result", "what it covers", "operations", "sections", "pauses", "sections fitted", "facts not stored", "facts the caller gives"], rows, ""),
         "<h2>Status legend</h2><div class='legend'>%s</div>" % " ".join(str(badge(k)) + ' <span class="why">%s</span>' % E(STATUS[k][2]) for k in STATUS),
         "<p class='note'>sample.zip and u73_nonbinding_pulse.zip are the same run. u73_blank, u711_proa_day12 and u711_cadet_reference are the same method.</p></main></body></html>"]
    return "\n".join(p)


def main():
    results = []
    files = sorted(glob.glob(os.path.join(ARGS.samples, "*.zip")), key=lambda p: list(SAMPLE_NOTES).index(os.path.basename(p)) if os.path.basename(p) in SAMPLE_NOTES else 99)
    for path in files:
        name = os.path.splitext(os.path.basename(path))[0]
        try:
            A = analyze(path)
            A["inv"] = inventory(A)
            ops_table(A)  # fills A["op_values"]
            A["rec"] = record(A)
            rec_name = name + "_record_prototype.json"
            with open(os.path.join(OUT, rec_name), "w", encoding="utf-8", newline="\n") as fh:
                json.dump(A["rec"], fh, indent=1, default=str)
            html = page(A, A["inv"], rec_name)
            with open(os.path.join(OUT, name + ".html"), "w", encoding="utf-8", newline="\n") as fh:
                fh.write(html)
            results.append((A, True))
            print("[OK] %s: %d ops, %d sections, %d facts" % (name, len(A["ops"]), len(A["S"]), len(A["inv"]["facts"])))
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            results.append((OrderedDict(file=os.path.basename(path), error=str(exc)), False))
            print("[FAIL] %s: %s" % (name, exc))
    with open(os.path.join(OUT, "index.html"), "w", encoding="utf-8", newline="\n") as fh:
        fh.write(index_page(results))
    print("[OK] wrote", os.path.join(OUT, "index.html"))
    if UNLINKED:
        print("warning: %d doc link(s) did not resolve: %s" % (len(UNLINKED), sorted(set(UNLINKED))))


if __name__ == "__main__":
    main()
