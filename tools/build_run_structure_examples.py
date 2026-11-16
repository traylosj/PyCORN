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
C_WASTE = "#b9b8b3"  # fraction collector at a waste line: flow counted, not a sample
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
DECISIONS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "run_structure_decisions.json")


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


_IDS = {}


def _ids(fn):
    """Explicit anchors (`<a id="...">`) of one doc: a link to a table or figure must hit one."""
    if fn not in _IDS:
        with open(os.path.join(ROOT, "docs", fn), encoding="utf-8") as fh:
            _IDS[fn] = set(re.findall(r'<a id="([^"]+)"', fh.read()))
    return _IDS[fn]


def ref(label, fn, prefix=None, anchor=None):
    if anchor is not None and anchor.startswith(("table-", "figure-")) and anchor not in _ids(fn):
        UNLINKED.append((fn, anchor))
        return H('<span class="err">[unlinked: %s %s]</span>' % (E(fn), E(anchor)))
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
    ("procedure", "level PR: the result; chromatograms; links to campaign, series, stage, process inputs, process outputs, pools (all caller's except process output IDs)", "UNICORN + caller", "#above", [("3.1", None), ("3.4", None)]),
    ("unit_procedures[]", "level UP: label, boundary_source (cycles are the caller's)", "caller", "#above", [("3.3", None)]),
    ("  .equipment_phases[]", "level EP: sample feed, fractionation (collected fractions only), column by-pass; equipment_module, start, end, commanded_by (the phase that started it); may overlap and cross operations (names proposed)", "PyCORN", "#levels", [("3.5.1", None)]),
    ("  .operations[]", "level OP: id, label, label_source, original, instance, state, process_operation, start/end, column_volume set point and actual", "UNICORN + PyCORN", "#levels", [("3.5", None), ("3.8", None)]),
    ("    .phases[]", "level PH: one per task block run (a block the operation calls with a planned length), or the operation's own stretch, or the whole operation when it has no task block; label as logged, original, how_found, length (planned and executed); never overlapping", "UNICORN + method", "#levels", [("3.5.1", None), ("3.5", None)]),
    ("      .sections[]", "level SE: state, start/end (wall clock and method time), column_flow (set point + fitted actual with pieces), percent_b, injection_valve, inlets (known / assumed / not known), bypass", "UNICORN + volume axis", "#tables", [("3.5", None), ("3.7", None), ("3.8", None)]),
    ("states[]", "ISA-88 state bands: held, holding, restarting, paused", "PyCORN", "#levels", [("3.7", None)]),
    ("materials", "process inputs (material_use Consumed; a load has its set point volume, a buffer its inlet line, positions, %B range; contents and lot null = caller), process outputs (material_use Produced for collected fractions, Other for waste lines; ID = run + tube); material IDs null = caller", "UNICORN + caller", "#tables", [("3.6.3", None)]),
    ("volumes", "every volume on the flow path with status stored / not stored", "UNICORN", "#path", [("3.6.1.1", None)]),
    ("facts[]", "the inventory: every fact, who needs it, its status and source", "all", "#facts", [("3.9", None)]),
    ("views.display.steps[]", "display steps. A view, not part of the record: computed from sections each time", "PyCORN", "#levels", [("3.5", None)]),
    ("warnings[], not_known[]", "what was assumed, fitted or missing", "PyCORN", "#warn", [("4", None)]),
]

KEY_GLOSSARY = [
    ("level", "procedure, unit_procedure, operation, phase, section", "ISA-88 levels; section is CADET-Core's word", "table-17"),
    ("element_id", "PR, UP1, OP3, OP3/PH1, OP3/PH1/SE2", "the same key in tables, CADET and JSON, so they join; never shown", "table-17"),
    ("label, label_source", "Elution / UNICORN; Load / User input", "the name shown; the user may override it (BatchML Label)", "table-17"),
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
        ops = [OrderedDict(name="Manual run", original="(none: no method, no Phase lines)", instance=1,
                           repeats=1, a=0.0, b=r["end_elapsed_s"], start_volume=None, end_volume=None, start_cv=None,
                           end_cv=None, volume_unit=None, synthetic=True, display_name="Manual run")]
    for i, op in enumerate(ops, 1):
        op["id"] = "OP%d" % i  # positional key, never shown: the label carries the name
        op["label"] = op["name"]
        op["wa"], op["wb"] = wall(op["a"]), wall(op["b"])
    A["ops"] = ops
    A["chroms"] = run.describe_chromatograms()
    st = frames.get("instrument")
    A["system_type"] = ""
    if st is not None and len(st):
        m = st[(st["group"] == "instrument configuration") & (st["item"] == "SystemTypeName")]
        A["system_type"] = str(m["value"].iloc[0]) if len(m) else ""
    # SystemTypeName is UNICORN's internal code (e.g. NEXTAKTAchromatography); the configuration's Description names the system
    A["system_type"] = A["instrument"] or A["system_type"]
    rd = frames.get("run_details")
    A["batch_id_present"] = bool(rd is not None and len(rd) and rd.astype(str).apply(lambda col: col.str.contains("Batch ID", case=False, na=False)).any().any())
    A["phases"] = build_phases(A)
    A["equipment_phases"] = build_equipment_phases(A)
    A["phase_list"] = [p for ps in A["phases"].values() for p in ps]
    A["steps"] = build_steps(S, ops, A)
    name_steps(A["steps"], ops, A)
    A["lead_in"] = lead_in(S)
    A["states"] = build_states(S, rows)
    A["blocks"] = build_blocks(S)
    return A


STEP_KEYS = ("flow", "b_pct", "slope", "feed", "bypass", "held")


def _differs(sliver, target):
    """What a short piece had that the step it joins does not, in words."""
    out = []
    if sliver["flow"] != target["flow"]:
        out.append("%s ml/min" % f(sliver["flow"], 2))
    if sliver["b_pct"] != target["b_pct"] or sliver["slope"] != target["slope"]:
        out.append("%s %%B" % f(sliver["b_pct"], 1))
    for key, word in (("feed", "feed"), ("bypass", "by-pass"), ("held", "Hold")):
        if sliver[key] != target[key]:
            out.append("%s %s" % (word, "on" if sliver[key] else "off"))
    return ", ".join(out) or "same values"


def sec_ranges(secs):
    """Section numbers as '#3-6, #8': the '#' column of the sections table."""
    secs, out, i = sorted(secs), [], 0
    while i < len(secs):
        j = i
        while j + 1 < len(secs) and secs[j + 1] == secs[j] + 1:
            j += 1
        out.append("#%d" % secs[i] if i == j else "#%d-%d" % (secs[i], secs[j]))
        i = j + 1
    return ", ".join(out)


RUN_START_RE = re.compile(r"^(Method|Manual) Run\b")
PROCESS_INSTR = ("System flow", "Flow", "Sample flow", "Gradient", "Inlet", "Quaternary", "Column position", "Injection valve")
STRUCTURAL_INSTR = ("Base", "End_Block")
WAIT_INSTR = ("Hold until", "Pause", "Hold", "Message", "Next breakpoint")


def method_blocks(run):
    """The method's blocks by name: instruction names, own End_Block breakpoint and the phase name (from MethodData)."""
    md = run.metadata.get("MethodData")
    method = md.get("Method") if isinstance(md, dict) else None
    blocks = (method or {}).get("Blocks") or {}
    raw = blocks.get("Block") if isinstance(blocks, dict) else None
    raw = [raw] if isinstance(raw, dict) else (raw or [])
    out = OrderedDict()
    for b in raw:
        ins = (b.get("BlockInstructions") or {}).get("BlockInstruction") or []
        ins = [ins] if isinstance(ins, dict) else ins
        end_bp = None
        for i in ins:
            if (i.get("ReadableName") or "") == "End_Block":
                try:
                    end_bp = float(i.get("BreakPointValue"))
                except (TypeError, ValueError):
                    end_bp = None
        mp = b.get("MethodPhase")
        out[b.get("BlockName")] = OrderedDict(names=[i.get("ReadableName") or "" for i in ins], end_bp=end_bp,
                                              phase=mp.get("PhaseName") if isinstance(mp, dict) else None)
    return out


def block_class(info):
    """
    Task block or smaller logic, from the method block's own instructions (section 3.5.1 item 6, proposed):
    structural instructions and waits are ignored; a process instruction or a call of another block makes a
    task; no instruction of its own is a task (its length does the work); only equipment control is smaller logic.
    """
    names = [n for n in info["names"] if n not in STRUCTURAL_INSTR and not n.startswith(WAIT_INSTR)]
    if "Block" in names or "Loop" in names or any(n.startswith(PROCESS_INSTR) for n in names):
        return "task", "has a process instruction or calls a block"
    if not names:
        return "task", "no instruction of its own: its length does the work"
    return "smaller logic", "only equipment control (%s)" % ", ".join(sorted(set(names))[:3])


def executed_blocks(A):
    """Executed run-log blocks with their depth under the method run: [dict(name, kind, depth, a, b)], elapsed s."""
    out, stack = [], []
    for e in A["r"]["entries"]:
        t = e["text"]
        if e["kind"] == "block_start":
            if RUN_START_RE.match(t):
                kind, name = "run", t[:10]
            elif t.startswith("Phase "):
                kind, name = "phase", strip_phase(t)
            elif t.startswith("Block "):
                kind, name = "block", t[6:].strip()
            else:
                kind, name = "event", t
            node = dict(name=name, kind=kind, depth=len(stack), a=e["elapsed_s"], b=None)
            stack.append(node)
            out.append(node)
        elif e["kind"] == "block_end":
            if t.startswith("End Phase"):
                while stack:
                    n = stack.pop()
                    n["b"] = e["elapsed_s"]
                    if n["kind"] == "phase":
                        break
            elif t.startswith("End_Block") and stack:
                stack.pop()["b"] = e["elapsed_s"]
            elif t.startswith("End "):  # the run's end closes what is still open
                for n in stack:
                    n["b"] = e["elapsed_s"]
                stack = []
    for n in out:
        if n["b"] is None:
            n["b"] = A["r"]["end_elapsed_s"]
    return out


def build_phases(A):
    """
    Phases as section 3.5.1 reads them: per operation, the task blocks it calls (a planned length above 0 in this
    run, and not smaller logic by `block_class`), plus its own stretches when it has a planned length of its own;
    an operation with neither is one phase. A stretch with no planned length of its own joins the next phase (the
    last one at the end of the operation). Returns {operation id: [phase]} and the rows of every candidate block
    (`A["phase_candidates"]`). Watch-started blocks are not read as phases yet: no public sample fires one.
    """
    ops, hints = A["ops"], A["hints"]
    mb = method_blocks(A["run"])
    nodes = executed_blocks(A)
    rows = [r for r in (hints.blocks if hints and hints.available else [])]

    def planned_row(name, a):
        # a pause that starts at the block's start sits before or after it on the wall clock: try both
        after = A["wall"](a)
        before = after - sum(p["duration_s"] for p in A["pauses"] if abs(p["elapsed_s"] - a) <= 1e-6)
        return next((r for r in rows if r["block"] == name and min(abs(r["start_s"] - after), abs(r["start_s"] - before)) <= 2.0), None)

    phases, cand = OrderedDict(), []
    for op in ops:
        inside = [] if op["synthetic"] else [n for n in nodes if n["a"] >= op["a"] - 1e-6 and n["a"] < op["b"] - 1e-6
                                             and n["kind"] in ("block", "event") and n["depth"] >= 2]
        tasks = []
        for n in inside:
            if n["kind"] == "event":
                cand.append(dict(op=op["id"], block=n["name"][:60], depth=n["depth"], planned=None, cls="smaller logic",
                                 why="started by a watch action: a task only with a process instruction of its own (not read yet)", phase=None))
                continue
            if n["depth"] >= 3:
                cand.append(dict(op=op["id"], block=n["name"], depth=n["depth"], planned=None, cls="step of the outer block",
                                 why="called by a block that is not a phase", phase=None))
                continue
            row = planned_row(n["name"], n["a"])
            info = mb.get(n["name"])
            if row is None:
                cand.append(dict(op=op["id"], block=n["name"], depth=n["depth"], planned=None, cls="smaller logic",
                                 why="no planned length in this run (zero length)", phase=None))
                continue
            cls, why = block_class(info) if info else ("task", "no method block to read")
            cand.append(dict(op=op["id"], block=n["name"], depth=n["depth"], planned=row, cls=cls, why=why, phase=None))
            if cls == "task":
                tasks.append((n, row, cand[-1]))
        tasks.sort(key=lambda t: t[0]["a"])
        # the operation's own stretches: gaps between the task blocks, when the operation has a planned length of its own
        opinfo = next((v for v in mb.values() if v["phase"] and v["phase"].lower() == op["name"].lower()), None)
        own_len = bool(opinfo and opinfo["end_bp"])
        items, cursor = [], op["a"]
        for n, row, c in tasks:
            if own_len and n["a"] - cursor > CUTOFF_S:
                items.append(dict(a=cursor, b=n["a"], kind="own", node=None, row=None, cand=None))
            items.append(dict(a=n["a"], b=n["b"], kind="task", node=n, row=row, cand=c))
            cursor = n["b"]
        if own_len and tasks and op["b"] - cursor > CUTOFF_S:
            items.append(dict(a=cursor, b=op["b"], kind="own", node=None, row=None, cand=None))
        if not items:
            items = [dict(a=op["a"], b=op["b"], kind="operation", node=None, row=None, cand=None)]
        out, seen = [], {}
        for k, it in enumerate(items):
            start = op["a"] if k == 0 else items[k - 1]["b"]
            end = op["b"] if k == len(items) - 1 else it["b"]
            name = it["node"]["name"] if it["node"] else op["label"]
            seen[name] = seen.get(name, 0) + 1
            label = name if seen[name] == 1 else "%s (%d)" % (name, seen[name])
            row = it["row"]
            ph = OrderedDict(id="%s/PH%d" % (op["id"], k + 1), label=label, original=name, kind=it["kind"], a=start, b=end,
                             wa=A["wall"](start), wb=A["wall"](end), planned=row["planned"] if row else None,
                             base=row["base"] if row else None, executed=row["executed"] if row else None,
                             status=row["status"] if row else None, cause=row["cause"] if row else None)
            if it["cand"] is not None:
                it["cand"]["phase"] = ph["id"]
            out.append(ph)
        phases[op["id"]] = out
    A["phase_candidates"] = cand
    return phases


def build_equipment_phases(A):
    """Sample feed, fractionation (collected fractions, not waste lines) and column by-pass: equipment phases of the unit procedure."""
    S, eps = A["S"], []
    for a, b in _windows(S, lambda s: s.feed):
        eps.append(dict(label="Sample feed", module="sample_feed", wa=a, wb=b))
    for a, b in _windows(S, lambda s: s.bypass):
        eps.append(dict(label="Column by-pass", module="column_valve", wa=a, wb=b))
    try:
        fr = [x for x in event_tables.rows(A["ch"], "fraction") if not x["waste"] and x["end_time"] is not None]
    except Exception:  # noqa: BLE001
        fr = []
    if fr:
        eps.append(dict(label="Fractionation", module="fraction_collector", wa=A["wall"](min(x["start_time"] for x in fr) * 60),
                        wb=A["wall"](max(x["end_time"] for x in fr) * 60)))
    eps.sort(key=lambda e: e["wa"])
    flat = [p for ps in A["phases"].values() for p in ps]
    for k, e in enumerate(eps, 1):
        e["id"] = "EP%d" % k
        e["commanded_by"] = next((p["id"] for p in flat if p["wa"] - 1e-6 <= e["wa"] < p["wb"] - 1e-6), None)
    return eps


def phase_at(A, op_id, mid_elapsed):
    ps = A["phases"].get(op_id) or []
    return next((p for p in ps if p["a"] - 1e-6 <= mid_elapsed < p["b"] - 1e-6), ps[-1] if ps else None)


def build_steps(S, ops, A=None):
    """
    Display steps (local/DISPLAY_STEPS_PROPOSAL.md, rules 1 to 3). One step per operation, split only
    where flow, %B, feed, by-pass or Hold changes. Every pause is its own Held row. A running piece
    under CUTOFF_S joins the step after it when it starts the operation or follows a pause, else
    the step before it, unless it has a Hold, feed or by-pass the step lacks (those are never
    hidden). Each step keeps `parts`: the sections it joined,
    `absorbed` for the short pieces, and `notes` for what was absorbed.
    """
    all_steps = []
    for op in ops:
        pieces = []
        for i, s in enumerate(S):
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
                               bypass=bool(s.bypass), paused=s.paused, held=s.held, op=op["id"], notes=[],
                               ph=(phase_at(A, op["id"], (a + b) / 2.0) or {}).get("id") if A is not None else None,
                               n_sections=1, parts=[dict(sec=i, a=a, b=b, wa=wa, wb=wb, absorbed=False)]))
            pieces[-1]["parts"][0]["vals"] = {c: pieces[-1][c] for c in STEP_KEYS}
        steps = []

        def join_equal():
            """Rule 1: neighbouring running steps with the same values are one step."""
            k = 1
            while k < len(steps):
                x, y = steps[k - 1], steps[k]
                if not x["paused"] and not y["paused"] and x["ph"] == y["ph"] and all(x[c] == y[c] for c in STEP_KEYS):
                    x["b"], x["wb"] = y["b"], y["wb"]
                    x["parts"] += y["parts"]
                    x["notes"] += y["notes"]
                    x["n_sections"] += y["n_sections"]
                    del steps[k]
                else:
                    k += 1

        steps.extend(pieces)
        join_equal()
        absorbing = True
        while absorbing:  # rule 3
            absorbing = False
            for k, st in enumerate(steps):
                if st["paused"] or st["b"] - st["a"] >= CUTOFF_S:
                    continue
                # a step never crosses a phase boundary: only a neighbour in the same phase can take the piece
                before = steps[k - 1] if k and not steps[k - 1]["paused"] and steps[k - 1]["ph"] == st["ph"] else None
                after = steps[k + 1] if k + 1 < len(steps) and not steps[k + 1]["paused"] and steps[k + 1]["ph"] == st["ph"] else None
                starts = k == 0 or steps[k - 1]["paused"]
                target = (after or before) if starts else (before or after)
                # feed, by-pass and Hold are never hidden: the step it joins must have them on too
                if target is None or any(st[c] and not target[c] for c in ("feed", "bypass", "held")):
                    continue
                for p in st["parts"]:
                    p["absorbed"] = True
                if target is after:
                    target["a"], target["wa"] = st["a"], st["wa"]
                    target["parts"] = st["parts"] + target["parts"]
                else:
                    target["b"], target["wb"] = st["b"], st["wb"]
                    target["parts"] = target["parts"] + st["parts"]
                target["n_sections"] += st["n_sections"]
                del steps[k]
                join_equal()
                absorbing = True
                break
        for st in steps:  # one note per run of absorbed sections
            runs, cur = [], []
            for p in st["parts"]:
                if p["absorbed"]:
                    cur.append(p)
                elif cur:
                    runs.append(cur)
                    cur = []
            if cur:
                runs.append(cur)
            for run in runs:
                where = "first" if run[0] is st["parts"][0] else "last" if run[-1] is st["parts"][-1] else "middle"
                diffs = []
                for p in run:
                    diffs += [d for d in _differs(p["vals"], st).split(", ") if d not in diffs and d != "same values"]
                st["notes"].append("%s %.1f s (section%s %s) absorbed: %s" % (
                    where, run[-1]["b"] - run[0]["a"], "s" if len(run) > 1 else "",
                    sec_ranges([p["sec"] for p in run]), ", ".join(diffs) or "same values"))
        all_steps += steps
    return all_steps


def name_steps(steps, ops, A=None):
    """
    Step label (PyCORN, Table 15): the operation's label, then ": " and the phase label when the operation
    has more than one phase, then the activity words that are on (feed, by-pass, Hold), ", " between; a
    pause is "<label>, Held". Equal labels are counted in run order and the later ones show "(2)", "(3)":
    the rule that names repeated operations. Nothing in the label is a number from this run's values.
    """
    op_name = {o["id"]: o["name"] for o in ops}
    phases = (A or {}).get("phases") or {}
    ph_label = {p["id"]: p["label"] for ps in phases.values() for p in ps}
    seen = {}
    for st in steps:
        words = ["Held"] if st["paused"] else [w for k, w in (("feed", "feed"), ("bypass", "by-pass"), ("held", "Hold")) if st[k]]
        head = op_name[st["op"]]
        if len(phases.get(st["op"]) or []) > 1 and st.get("ph") in ph_label:
            head = "%s: %s" % (head, ph_label[st["ph"]])
        base = ", ".join([head] + words)
        seen[base] = seen.get(base, 0) + 1
        st["label"] = base if seen[base] == 1 else "%s (%d)" % (base, seen[base])
        st["label_source"] = "PyCORN"


def lead_in(S):
    """
    Rule 4 (CADET model view): the unpaused sections before the first flow that moves volume,
    with no feed, no by-pass and no %B ramp, merge into one zero-flow section. Returns the
    section numbers, the time range and the start of the last one (the valves at its end carry
    on), or None when fewer than two lead in or no flow follows.
    """
    n = 0
    while n < len(S):
        s = S[n]
        idle = (not s.paused and not s.feed and not s.bypass and not s.percent_b[1]
                and ((s.column_flow_ml_min or 0.0) == 0.0 or s.end_s - s.start_s <= 1e-6))
        if not idle:
            break
        n += 1
    if n < 2 or n == len(S):
        return None
    return dict(secs=list(range(n)), start_s=S[0].start_s, end_s=S[n - 1].end_s, last_start_s=S[n - 1].start_s)


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


def _windows(S, pred):
    """(start, end) of each stretch of neighbouring unpaused sections for which `pred(section)` holds."""
    out = []
    for s in S:
        if s.paused or not pred(s):
            continue
        if out and abs(out[-1][1] - s.start_s) < 1e-6:
            out[-1] = (out[-1][0], s.end_s)
        else:
            out.append((s.start_s, s.end_s))
    return out


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
    up = up_label(A)
    top, h = row("Unit procedure", "one by default")
    svg.rect(X(0), top, X(end_w) - X(0), h, "#d9d8d3", 3, "%s: one unit procedure by default. Several only when the caller marks cycles (section 3.3)." % up, stroke="#8d8c88", dash="4 3")
    svg.text(X(0) + 6, top + 17, "%s  whole run on one unit (%s); cycles: none marked (caller's rule)" % (up, A["system_type"] or "system"), 11.5)
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
        tip = "%s (element_id %s):%.1f - %.1f s (method time %.1f - %.1f)" % (op["display_name"], op["id"], op["wa"], op["wb"], op["a"], op["b"])
        svg.rect(X(op["wa"]), top, w - 1, h, LEVEL_FILLS[i % 2], 3, tip,
                 dash="4 3" if op["synthetic"] else None, stroke="#8d8c88" if op["synthetic"] else None)
        label = op["display_name"]
        if w > 110:
            svg.text(X(op["wa"]) + 5, top + 17, label, 11)
        elif w > 8:
            svg.text(X((op["wa"] + op["wb"]) / 2), top - 4 - 11 * (i % 2), label, 10, INK2, "middle")

    top, h = row("Phase", "task block (%d)" % len(A["phase_list"]))
    for i, bl in enumerate(A["phase_list"]):
        w = X(bl["wb"]) - X(bl["wa"])
        how = {"task": "task block", "own": "the operation's own stretch", "operation": "the operation has no task block: one phase"}[bl["kind"]]
        planned = (", planned %s %s, ran %s" % (f(bl["planned"], 2), bl["base"], f(bl["executed"], 2))) if bl["planned"] is not None else ""
        svg.rect(X(bl["wa"]), top, w - 1, h, LEVEL_FILLS[i % 2], 3, "%s (%s): %s, %.1f - %.1f s%s" % (bl["label"], bl["id"], how, bl["wa"], bl["wb"], planned),
                 dash="4 3" if bl["kind"] == "operation" else None, stroke="#8d8c88" if bl["kind"] == "operation" else None)
        if w > 80:
            svg.text(X(bl["wa"]) + 5, top + 17, bl["label"], 10.5)

    top, h = row("", "", h=68)
    svg.text(L - 12, top - 8, "Equipment phases", 12, INK, "end", "600")
    bar = 20  # bar height of the three activity rows (feed, by-pass, fractions), one row every 24
    for s in S:
        if s.paused:
            continue
        if s.feed:
            svg.rect(X(s.start_s), top, X(s.end_s) - X(s.start_s), bar, C_FEED, 2,
                     "sample feed: valve %s, %.1f - %.1f s" % (s.injection_valve, s.start_s, s.end_s))
        elif s.sample_use == "to_bypass":
            svg.rect(X(s.start_s), top, X(s.end_s) - X(s.start_s), bar, "#b9d9c9", 2,
                     "sample sent to the by-pass: %.1f - %.1f s" % (s.start_s, s.end_s))
        if s.bypass:
            svg.rect(X(s.start_s), top + 24, X(s.end_s) - X(s.start_s), bar, C_BYPASS, 2,
                     "column by-passed: %.1f - %.1f s (column flow 0)" % (s.start_s, s.end_s))
    for pred, name, row_y, ink in ((lambda s: s.feed, "feed", 0, "#ffffff"),
                                   (lambda s: s.sample_use == "to_bypass" and not s.feed, "sample to by-pass", 0, INK),
                                   (lambda s: s.bypass, "by-pass", 24, "#ffffff")):
        for a, b in _windows(S, pred):
            if X(b) - X(a) > 7 * len(name) + 10:
                svg.text(X(a) + 5, top + row_y + 14, name, 11.5, ink)
            else:  # too narrow for the word: write it beside the bar, with the length
                svg.text(X(b) + 6, top + row_y + 14, "%s %s s" % (name, f(b - a, 0)), 11.5, INK)
    try:
        for fr in event_tables.rows(A["ch"], "fraction"):
            if fr["pseudo"]:
                continue
            a, b = A["wall"](fr["start_time"] * 60), A["wall"](fr["end_time"] * 60)
            svg.rect(X(a), top + 48, X(b) - X(a), bar, C_WASTE if fr["waste"] else C_FRAC, 1, "%s %s: %.1f - %.1f s, %s ml"
                     % ("waste line (not a sample)" if fr["waste"] else "fraction", fr["name"], a, b, f(fr["length_volume"], 2)))
            if X(b) - X(a) > 7 * len(str(fr["name"])) + 4:
                svg.text((X(a) + X(b)) / 2, top + 62, fr["name"], 10.5, "#ffffff", "middle")
    except Exception:  # noqa: BLE001
        pass
    svg.text(L - 12, top + 14, "feed", 12, INK, "end")
    svg.text(L - 12, top + 38, "by-pass", 12, INK, "end")
    svg.text(L - 12, top + 62, "fractions", 12, INK, "end")

    top, h = row("Section", "%d; CADET-Process: Phase" % len([s for s in S if not s.paused]))
    for i, s in enumerate(S):
        if s.paused:
            svg.diamond(X(s.start_s), top + 6, "pause %.1f s at %.1f s wall clock" % (s.end_s - s.start_s, s.start_s))
            continue
        tip = "section #%d: %.1f - %.1f s | flow set %s ml/min (column %s) | %%B %s | valve %s | inlets %s | %s" % (
            i, s.start_s, s.end_s, f(s.system_flow_ml_min, 3), f(s.column_flow_ml_min, 3), f(s.percent_b[0], 1),
            s.injection_valve, s.inlets, "; ".join(s.changes))
        svg.rect(X(s.start_s), top, X(s.end_s) - X(s.start_s) - 1, h, STEP_FILLS[i % 2], 0, tip)
    li = A["lead_in"]
    if li:
        svg.rect(X(li["start_s"]) - 1, top - 2, X(li["end_s"]) - X(li["start_s"]) + 2, h + 4, "none", 2,
                 "CADET model view, rule 4: sections %s have no flow and merge into one zero-flow section "
                 "(%.1f - %.1f s). The valves as set at %.1f s carry into the first flow section."
                 % (sec_ranges(li["secs"]), li["start_s"], li["end_s"], li["last_start_s"]), stroke=C_BYPASS, dash="3 2")

    svg.line(20, y - 14, W - RM, y - 14, INK2, 1, "4 4")
    svg.text(20, y - 2, "Display (not in the record: computed from the sections each time)", 10.5, INK2)
    y += 8
    steps = A["steps"]
    top, h = row("Display step", "%d (sections joined)" % len([x for x in steps if not x["paused"]]), h=30)
    n = 0
    for st in steps:
        if st["paused"]:
            svg.diamond(X(st["wa"]), top + 8, "pause (Held): %.1f - %.1f s, section %s" % (
                st["wa"], st["wb"], sec_ranges([p["sec"] for p in st["parts"]])))
            continue
        n += 1
        w = X(st["wb"]) - X(st["wa"])
        listing = "; ".join("#%d %.1f-%.1f s%s" % (p["sec"], p["wa"], p["wb"], " (absorbed)" if p["absorbed"] else "") for p in st["parts"])
        tip = "step %d (%s): %.1f - %.1f s | %s ml/min | %s %%B | joins %d section(s): %s%s" % (
            n, st["op"], st["wa"], st["wb"], f(st["flow"], 3), f(st["b_pct"], 1), st["n_sections"], listing,
            " | " + "; ".join(st["notes"]) if st["notes"] else "")
        svg.rect(X(st["wa"]), top, w - 1, h, STEP_FILLS[n % 2], 0, tip)
        for p in st["parts"]:
            if p["absorbed"]:
                svg.rect(X(p["wa"]), top, max(X(p["wb"]) - X(p["wa"]) - 1, 3), h, C_PREV, 0, tip, opacity=0.8)
        for p in st["parts"][1:]:  # every place two original sections were joined
            svg.line(X(p["wa"]), top - 3, X(p["wa"]), top + h + 3, INK, 1.2, "2 2" if not p["absorbed"] else None)
        if w > 70:
            svg.text(X(st["wa"]) + 4, top + 12, "%s ml/min" % f(st["flow"], 2), 10)
            svg.text(X(st["wa"]) + 4, top + 25, "%s %%B" % f(st["b_pct"], 0), 10)
        if w > 26:
            svg.text(X(st["wa"]) + 2, top + h + 12, sec_ranges([p["sec"] for p in st["parts"]]), 9, INK2)

    axis_ticks(svg, A, X, y + 8)
    y += 26
    lx = L
    for fill, name in ((STEP_FILLS[1], "section / display step"), (C_PREV, "orange: short piece (under 10 s) absorbed into the step it touches"),
                       (C_FEED, "feed"), (C_BYPASS, "column by-pass"), (C_FRAC, "fraction (sample)"), (C_WASTE, "waste line (not a sample)")):
        svg.rect(lx, y, 13, 11, fill, 2)
        svg.text(lx + 19, y + 10, name, 10.5, INK2)
        lx += 28 + 6.3 * len(name)
    y += 18
    lx = L
    svg.line(lx + 6, y - 2, lx + 6, y + 13, INK, 1.2, "2 2")
    svg.text(lx + 19, y + 10, "dashed line: two sections with the same values joined (numbers under a step: the sections it joins, as in the sections table)", 10.5, INK2)
    y += 18
    svg.rect(L, y, 13, 11, "none", 2, stroke=C_BYPASS, dash="3 2")
    svg.text(L + 19, y + 10, "dashed box on the Section row: CADET merges these no-flow sections before the first flow into one (rule 4)", 10.5, INK2)
    y += 30
    svg.y = y
    return svg.out()


def clip(text, n):
    text = str(text)
    return text if len(text) <= n else text[:n - 3] + "..."


def section_details(s):
    """The original section as UNICORN's run log set it, one short line per fact (for the join figure)."""
    lines = ["wall %.1f-%.1f s, method time %.1f-%.1f s" % (s.start_s, s.end_s, s.elapsed_start_s, s.elapsed_end_s),
             "column flow %s ml/min, system flow %s" % (f(s.column_flow_ml_min or 0.0, 3), f(s.system_flow_ml_min, 3) if s.system_flow_ml_min is not None else "-"),
             "%%B %s, slope %s %%B/s" % (f(s.percent_b[0], 1) if s.percent_b[0] is not None else "-", f(s.percent_b[1], 4)),
             "valve %s, sample use %s, position %s" % (s.injection_valve or "-", s.sample_use or "-", s.column_position or "-"),
             "inlets %s" % (", ".join("%s %s" % kv for kv in s.inlets.items()) or "-")]
    flags = ", ".join(w for k, w in (("feed", "feed"), ("bypass", "by-pass"), ("held", "Hold")) if getattr(s, k))
    lines.append("on: %s" % (flags or "none"))
    lines.append("run log: %s" % ("; ".join(map(str, s.changes)) or "-"))
    return lines


def fig_joins(A):
    """
    Where original sections were joined, one row per display step that joined more than one.
    The cells are the sections, equal width (not to time scale): the figure above is to time scale,
    where a 1 s piece is invisible.
    """
    steps = [st for st in A["steps"] if not st["paused"] and len(st["parts"]) > 1]
    if not steps:
        return "<p class='lead'>No display step joined more than one section in this run.</p>"
    cw, ch, gap, left = 290, 132, 8, 330
    width = left + max(len(st["parts"]) for st in steps) * (cw + gap) + 20
    svg = Svg(max(width, 900))
    y = 24
    svg.text(20, y, "Where original sections were joined - %s" % A["file"], 15, weight="600")
    y += 18
    svg.text(20, y, "Each cell is one original section (# as in the sections table), not to time scale. "
                    "Blue: same values as the step. Orange: a short piece absorbed into the step.", 11.5, INK2)
    y += 22
    n = 0
    for st in A["steps"]:
        if st["paused"]:
            continue
        n += 1
        if len(st["parts"]) < 2:
            continue
        flags = ", ".join(w for k, w in (("feed", "feed"), ("bypass", "by-pass"), ("held", "Hold")) if st[k])
        op = next((o for o in A["ops"] if o["id"] == st["op"]), None)
        svg.text(20, y + 14, "Step %d: %s" % (n, st["label"]), 12.5, INK, weight="600")
        svg.text(20, y + 31, "%.1f - %.1f s, %s ml/min, %s %%B%s" % (st["wa"], st["wb"], f(st["flow"], 2), f(st["b_pct"], 0),
                                                              (", " + flags) if flags else ""), 11, INK2)
        svg.text(20, y + 46, "%d sections joined, %d absorbed" % (len(st["parts"]), len([p for p in st["parts"] if p["absorbed"]])), 11, INK2)
        x = left
        for p in st["parts"]:
            details = section_details(A["S"][p["sec"]])
            tip = "section #%d: %.1f - %.1f s (%.1f s)%s\n%s" % (p["sec"], p["wa"], p["wb"], p["b"] - p["a"],
                                                               " absorbed: " + _differs(p["vals"], st) if p["absorbed"] else "",
                                                               "\n".join(details))
            if p["absorbed"]:
                svg.rect(x, y, cw, ch, C_PREV, 3, tip, opacity=0.85)
            else:
                svg.rect(x, y, cw, ch, STEP_FILLS[1], 3, tip)
            ink = "#ffffff" if p["absorbed"] else INK
            svg.text(x + 7, y + 17, "#%d   %s s" % (p["sec"], f(p["b"] - p["a"], 1)), 12, ink, weight="600")
            svg.text(x + 7, y + 33, ("absorbed: " + _differs(p["vals"], st)) if p["absorbed"] else "same values", 10.5, ink)
            for k, line in enumerate(details):
                svg.text(x + 7, y + 49 + k * 13, clip(line, 50), 10, ink)
            x += cw + gap
        svg.line(left, y + ch + 6, x - gap, y + ch + 6, INK, 1.5)
        svg.text(left, y + ch + 19, "= display step %d" % n, 10.5, INK2)
        y += ch + 34
    svg.y = y + 4
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


def buffers_used(A):
    """The buffer side of the run: per inlet line the labels used (known / assumed / not known), the %B range, BufferPro."""
    lines = OrderedDict()
    pcts = []
    for s in A["S"]:
        if s.paused:
            continue
        for line, st in (s.inlet_states or {}).items():
            lines.setdefault(line, OrderedDict())[st.get("label")] = st.get("state")
        if s.percent_b[0] is not None:
            pcts += [s.percent_b[0], s.percent_b[0] + s.percent_b[1] * (s.end_s - s.start_s)]
    bp = A["hints"].buffer_prep if A["hints"] and A["hints"].available else None
    return dict(lines=lines, pct=(min(pcts), max(pcts)) if pcts else None, bufferpro=bool(bp))


def above_run(A):
    """What sits above and beside the operations for this run: [(row, title, lines, status, why)]."""
    ops, hints = A["ops"], A["hints"]
    real = [o for o in ops if not o["synthetic"]]
    repeats = sorted({o["name"] for o in real if o["repeats"] > 1})
    scout = (hints.scouting if hints and hints.available else None) or {}
    series = bool(scout.get("runs") or scout.get("name"))
    fr = [x for x in event_tables.rows(A["ch"], "fraction") if not x["waste"]]  # collected fractions, not waste lines
    inj = [x for x in event_tables.rows(A["ch"], "injection") if not x["pseudo"]]
    feeds = len([1 for i, s in enumerate(A["S"]) if s.feed and (i == 0 or not A["S"][i - 1].feed)])
    pk = getattr(A["ch"], "peak_tables", None) or {}
    pl = getattr(A["ch"], "pool_tables", None) or {}
    col = ((A["view"].column or {}).get("name") if A["view"] else None) or "(no column type named)"
    bl = len(A["phase_list"])
    return [
        ("chain", "Campaign / series", ["scouting table: %s" % ("%d run(s), %d variable(s)" % (len(scout.get("runs") or []), len(scout.get("variables") or [])) if series else "none"),
                                        "campaign ID: caller"], "stored" if series else "caller",
         "UNICORN stores the scouting link (name, run number) when the method has scouting; a production campaign is the caller's"),
        ("chain", "Process stage", ["capture, polishing ...", "UNICORN has none"], "caller", "ISA-88 process stage: not stored by UNICORN"),
        ("chain", "Procedure = result", ["1 per result (batch)", "Batch ID: %s (masked)" % ("stored" if A["batch_id_present"] else "not found")], "stored", "one method run; Batch ID is the run's identity"),
        ("chain", "Unit procedure", ["%s (1)" % up_label(A)[:24], "cycles: caller marks" + (("; hint: %s repeats" % ", ".join(repeats)) if repeats else "")], "derived",
         "one by default; a repeat in the logged phase order is only offered as a suggested rule, never applied (section 3.3)"),
        ("chain", "Operations", ["%d (UNICORN phases)" % len(ops), "synthetic: no phases" if ops[0]["synthetic"] else "from Phase lines"], "hint" if ops[0]["synthetic"] else "stored",
         "the run log's Phase lines"),
        ("chain", "Phases", ["%d (task blocks)" % bl, "equipment phases: %d" % len(A["equipment_phases"])], "derived", "task blocks of the method, run values applied (section 3.5.1)"),
        ("chain", "Sections", ["%d (executed instructions)" % len([s for s in A["S"] if not s.paused]), "CADET-Process: Phase"], "derived", "from what ran"),
        ("side", "Chromatograms", ["%d: %s" % (len(A["chroms"]), ", ".join("%s (%s)" % (c["name"], c["kind"]) for c in A["chroms"])[:44]), "cut settings: not known (needs a sample)"],
         "stored" if len(A["chroms"]) == 1 else "hint", "several chromatograms: how the run log records the cut is unverified (section 3.4)"),
        ("side", "Unit (equipment)", [A["instrument"][:24] or "-", "system type %s" % (A["system_type"] or "-"), "plant unit ID: caller"], "stored", "InstrumentConfigurationData; the plant's unit or skid ID is the caller's"),
        ("side", "Column (equipment)", [col[:26], "individual column ID: not stored", "use count: across runs"], "hint" if col.startswith("(") else "stored",
         "ColumnTypeData names the type; ColumnIndividualData is empty on these samples (section 3.6.1.2)"),
        ("side", "Process input: loads", ["%d feed(s), %d injection mark(s)" % (feeds, len(inj)), "material ID: caller"], "caller" if feeds else "absent", "a feed's lot or sample ID is the caller's"),
        ("side", "Process input: buffers", ["lines: " + "; ".join("%s %s" % (k, "/".join(v)) for k, v in buffers_used(A)["lines"].items() if k != "sample") if buffers_used(A)["lines"] else "lines: -",
                                           "%%B %s to %s" % (f(buffers_used(A)["pct"][0], 0), f(buffers_used(A)["pct"][1], 0)) if buffers_used(A)["pct"] else "%B: -",
                                           "BufferPro: %s" % ("recipe stored (hint)" if buffers_used(A)["bufferpro"] else "none"),
                                           "contents, lot IDs: caller"], "caller",
         "UNICORN stores the inlet line, position and %B per section, never what the buffer contains or its lot (sections 3.6.3.1, 3.6.3.2)"),
        ("side", "Process output: fractions, waste", ["%d collected" % len(fr), "ID = run + tube (derived)"], "derived" if fr else "absent", "IDs PyCORN can make itself"),
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
        add("Volumes and flow path", label, "both", "hint" if val else "missing", val or "-", "ColumnTypeData",
            "a library value, not measured on this packing: no porosity is computed from it. Display needs both for DBC; if CADET computes DBC, only CADET does")
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
    add("Detectors and calibration", "Extinction coefficient", "both", "caller", "-", "UNICORN stores none", "scope row 1, 2")
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
        "%d rows (%d collected fractions, %d waste lines)" % (len(fr_rows), len([x for x in fr_rows if not x["waste"]]),
                                                              len([x for x in fr_rows if x["waste"]])), "Fraction event curve", "")
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
    bu = buffers_used(A)
    add(G, "Buffers used: inlet lines and positions", "both", "derived",
        "; ".join("%s %s" % (k, "/".join(v)) for k, v in bu["lines"].items()) or "-", "run log + system configuration",
        "each position is known, assumed or not known (Decide 22)")
    add(G, "Buffer contents (components, concentrations) per inlet", "CADET", "caller", "-", "UNICORN stores none", "buffers= (D28)")
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
        out_rows.append([op["id"], op["label"], op["instance"], op["original"], f(op["a"] / 60, 3), f(op["b"] / 60, 3),
                         f((op["b"] - op["a"]), 1), f(op["start_volume"], 2) if op["start_volume"] is not None else "-",
                         f(op["end_volume"], 2) if op["end_volume"] is not None else "-",
                         f(op["start_cv"], 2) if op["start_cv"] is not None else "-",
                         f(op["end_cv"], 2) if op["end_cv"] is not None else "-",
                         f(set_ml, 3), f(act_ml, 3), ("%+.1f %%" % diff) if diff is not None else "-",
                         f(feed_set, 3) if feed_set else "-", f(feed_act, 3) if feed_set else "-",
                         "(empty: caller maps)"])
    A["op_values"] = record_vals
    return table(["element_id", "label", "instance", "UNICORN text (original)", "start min", "end min", "length s", "start vol",
                  "end vol", "start CV", "end CV", "column vol set point (ml)", "column vol actual (ml)", "diff",
                  "feed set (ml)", "feed actual (ml)", "process_operation"], out_rows, "wide")


def steps_table(A):
    rows = []
    n = 0
    for st in A["steps"]:
        secs = sec_ranges([p["sec"] for p in st["parts"]])
        if st["paused"]:
            rows.append(["-", st["label"], st["op"], f(st["wa"], 1), f(st["wb"], 1), "-", "-", secs, "-", "Held (pause), never joined", "-"])
            continue
        n += 1
        flags = []
        if st["feed"]:
            flags.append("feed")
        if st["bypass"]:
            flags.append("by-pass")
        if st["held"]:
            flags.append("Hold")
        absorbed = [p["sec"] for p in st["parts"] if p["absorbed"]]
        same = [p["sec"] for p in st["parts"] if not p["absorbed"]]
        if len(st["parts"]) == 1:
            how = "one section, not joined"
        else:
            how = "%d sections, same values: %s" % (len(same), sec_ranges(same)) if len(same) > 1 else "%d sections" % len(st["parts"])
            if absorbed:
                how += "; short pieces absorbed: %s" % sec_ranges(absorbed)
        cell = H("<b>%s</b>" % E(how)) if absorbed else how
        rows.append([n, st["label"], st["op"], f(st["wa"], 1), f(st["wb"], 1), f(st["flow"], 3), f(st["b_pct"], 1) + (" (ramp %.3f %%/s)" % st["slope"] if abs(st["slope"]) > 0 else ""),
                     secs, ", ".join(flags) or "-", cell, "; ".join(st["notes"]) or "-"])
    return table(["step", "label (PyCORN)", "operation", "start s", "end s", "column flow ml/min", "%B", "original sections (# in the sections table)", "activity",
                  "how they were joined", "note (what was absorbed)"], rows, "wide")


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
    out.append("<h4>Process inputs: loads (feeds)</h4>" + (table(["id", "span (wall clock)", "valve", "volume set point (ml)", "volume actual (ml)",
                                                   "method's planned sample volume", "material ID"], feeds, "")
                                           if feeds else "<p class='none'>No feed section (no injection valve at Inject / Direct inject).</p>"))
    bu = buffers_used(A)
    buf_rows = [["BUF:%s" % line, line, ", ".join(label if label.endswith((" (assumed)", " (not known)")) else "%s (%s)" % (label, state) for label, state in used.items()),
                 ("%s to %s %%B" % (f(bu["pct"][0], 0), f(bu["pct"][1], 0))) if line != "sample" and bu["pct"] else "-", "(caller)", "(caller)"]
                for line, used in bu["lines"].items()]
    out.append("<h4>Process inputs: buffers (inlet lines used)</h4>" + table(["id", "line", "positions used (state)", "%B range", "contents", "lot ID"], buf_rows, "") +
               "<p class='none'>%s</p>" % ("BufferPro recipe stored in the result (hint only)." if bu["bufferpro"] else "No BufferPro recipe stored. UNICORN never stores what a buffer contains."))
    fr = []
    waste_ml, sample_ml, per_line = 0.0, 0.0, OrderedDict()
    for x in event_tables.rows(A["ch"], "fraction"):
        vol = x["length_volume"] or 0.0
        if x["waste"]:
            waste_ml += vol
            per_line[x["name"]] = per_line.get(x["name"], 0.0) + vol
            role = "waste line (before the first tube)" if x["pseudo"] else "waste line (not a sample)"
        else:
            sample_ml += vol
            role = "collected fraction (sample)"
        fr.append(["%s:%s" % ("RUN", x["name"]), x["name"], "waste" if x["waste"] else "fraction collector", role,
                   f(x["start_time"], 3), f(x["end_time"], 3), f(x["length_volume"], 2), "-" if x["waste"] else "(caller)"])
    out.append("<h4>Process outputs (fraction collector and waste)</h4>" + (table(
        ["id (run + tube)", "UNICORN tube", "goes to", "role", "start min", "end min", "volume ml", "material ID"], fr, "")
        if fr else "<p class='none'>No fraction event curve.</p>"))
    if fr:
        out.append("<p>Volume to waste lines: <b>%s ml</b> (%s). Volume collected as fractions: <b>%s ml</b>. A waste line is a row with its flow, "
                   "not a sample: it gets no material ID.</p>" % (E(f(waste_ml, 2)), E(", ".join("%s %s ml" % (n, f(v, 2)) for n, v in per_line.items()) or "none"),
                                                              E(f(sample_ml, 2))))
    pk = getattr(A["ch"], "peak_tables", None) or {}
    pl = getattr(A["ch"], "pool_tables", None) or {}
    out.append("<p>Peak tables: %d. Pool tables: %d (evaluation results; pools belong to the material layer, section 3.6.3).</p>" % (len(pk), len(pl)))
    return "\n".join(out)


def states_html(A):
    rows = [[b["state"], f(b["wa"], 1), f(b["wb"], 1), f(b["wb"] - b["wa"], 1), b["why"]] for b in A["states"] if b["state"] != "running"]
    seen = {str(b["state"]).lower() for b in A["states"]}
    unseen = ", ".join(s for s in STATES_12 if s not in seen)
    note = "<p class='none'>ISA-88 example states (Table 12) not seen in this run: %s. The record carries only the states a run shows.</p>" % E(unseen)
    if not rows:
        return "<p class='none'>Only Running: no Pause, Hold or fitted ramp around a stop in this run.</p>" + note
    return table(["ISA-88 state", "start s", "end s", "length s", "how it was found"], rows, "") + note


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


def up_label(A):
    """Table 15: the chromatogram's name when the result holds exactly one, else the result name."""
    if len(A["chroms"]) == 1:
        return A["chroms"][0]["name"]
    return getattr(A["run"], "name", None) or A["file"]


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
                                   links=OrderedDict(campaign_id=None, series=None, process_stage=None, process_inputs=[], process_outputs=[],
                                                     pools=[], note="all caller-supplied except fraction IDs (run + tube)"))
    ups = OrderedDict(level="unit_procedure", element_id="UP1", label=up_label(A), label_source="UNICORN", boundary_source="default", operations=[])
    blocks_by_op = {}
    # the Fractionation phase is collecting fractions: a waste line is not a fractionation
    fractions = [x for x in event_tables.rows(A["ch"], "fraction") if not x["waste"] and x["end_time"] is not None]
    for k, (s, row) in enumerate(zip(S, rows)):
        mid = s.elapsed_start_s
        op = next((o for o in ops if o["a"] - 1e-6 <= mid < o["b"] - 1e-6), ops[-1])
        blocks_by_op.setdefault(op["id"], []).append((k, s, row))
    for op in ops:
        o = OrderedDict(level="operation", element_id=op["id"], label=op["label"], label_source="UNICORN", original=OrderedDict(level="phase", text=op["original"]),
                        instance=op["instance"], state="running", process_operation="",
                        start=OrderedDict(wall_s=round(op["wa"], 3), method_s=round(op["a"], 3)),
                        end=OrderedDict(wall_s=round(op["wb"], 3), method_s=round(op["b"], 3)),
                        column_volume=[val(A["op_values"][op["id"]]["set"], "mL", "set_point", "calculated", "run_log"),
                                       val(A["op_values"][op["id"]]["actual"], "mL", "actual", "calculated", "volume_axis")],
                        phases=[])
        # phases are task blocks (section 3.5.1): the blocks the operation calls with a planned length, run
        # values applied. Each section goes under the phase it falls in. Feed, fractionation and by-pass are
        # equipment phases, listed on the unit procedure.
        phase_objs = []
        for p in A["phases"][op["id"]]:
            ph = OrderedDict(level="phase", element_id=p["id"], label=p["label"], label_source="UNICORN",
                             original=OrderedDict(level="block" if p["kind"] == "task" else "phase", text=p["original"]),
                             how_found={"task": "a task block of the method (planned length above 0)",
                                        "own": "the operation's own stretch (it has a planned length of its own)",
                                        "operation": "the operation has no task block: one phase"}[p["kind"]],
                             start=OrderedDict(wall_s=round(p["wa"], 3), method_s=round(p["a"], 3)),
                             end=OrderedDict(wall_s=round(p["wb"], 3), method_s=round(p["b"], 3)), sections=[])
            if p["planned"] is not None:
                ph["length"] = [val(p["planned"], p["base"], "set_point", "logged", "method"),
                                val(p["executed"], p["base"], "actual", "calculated", "run_log", status=p["status"], cause=p["cause"])]
            phase_objs.append((p, ph))
        for k, s, row in blocks_by_op.get(op["id"], []):
            mid = (s.elapsed_start_s + s.elapsed_end_s) / 2.0 if not s.paused else s.elapsed_start_s
            p, ph = next(((p, ph) for p, ph in phase_objs if p["a"] - 1e-6 <= mid < p["b"] - 1e-6), phase_objs[-1])
            sec = OrderedDict(level="section", element_id="%s/SE%d" % (ph["element_id"], len(ph["sections"]) + 1), state="held" if s.paused else ("paused" if s.held else "running"),
                              start=OrderedDict(wall_s=round(s.start_s, 3), method_s=round(s.elapsed_start_s, 3)), end=OrderedDict(wall_s=round(s.end_s, 3), method_s=round(s.elapsed_end_s, 3)))
            if s.paused:
                sec["original"] = OrderedDict(level="instruction", block=s.block or None, state="pause", text="; ".join(s.changes))
            else:
                sec["original"] = OrderedDict(level="instruction", block=s.block or None, text="; ".join(s.changes))
                sec["column_flow"] = [val(s.column_flow_ml_min, "mL/min", "set_point", "logged", "run_log")]
                if row and row["fitted"]:
                    sec["column_flow"].append(val(row["axis_ml"] / ((s.end_s - s.start_s) / 60.0) if s.end_s > s.start_s else None, "mL/min", "actual", "fitted", "volume_axis",
                                                  pieces=[[round(a, 2), round(b, 2), round(q, 4), round(sl, 5)] for a, b, q, sl in row["pieces"]]))
                sec["percent_b"] = [val(s.percent_b[0], "%", "set_point", "logged", "run_log", slope_per_s=round(s.percent_b[1], 6))]
                sec["injection_valve"] = OrderedDict(value=s.injection_valve, original=s.injection_valve, sample_use=s.sample_use)
                sec["inlets"] = {k2: OrderedDict(position=v2, state=(s.inlet_states.get(k2) or {}).get("state")) for k2, v2 in (s.inlets or {}).items()}
                sec["bypass"] = s.bypass
            ph["sections"].append(sec)
        o["phases"] = [ph for p, ph in phase_objs]
        ups["operations"].append(o)
    # equipment phases (ISA-88.01 3.18): equipment control, not children of operations; names proposed (3.5.1)
    ups["equipment_phases"] = [OrderedDict(element_id=e["id"], label=e["label"], label_source="PyCORN", equipment_module=e["module"],
                                           start=OrderedDict(wall_s=round(e["wa"], 3)), end=OrderedDict(wall_s=round(e["wb"], 3)),
                                           commanded_by=e["commanded_by"]) for e in A["equipment_phases"]]
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
    bu = buffers_used(A)
    # ISA-88.01 5.3.2.2 formula: process inputs and process outputs. `material_use` is BatchML's MaterialUse
    # (B2MML-Common.xsd); a load or a buffer is told apart by its own fields and element_id (LOAD1, BUF:A)
    mats = OrderedDict(process_inputs=[], process_outputs=[], pools=OrderedDict(
        peak_tables=len(getattr(A["ch"], "peak_tables", None) or {}), pool_tables=len(getattr(A["ch"], "pool_tables", None) or {})))
    mats["process_inputs"] = [OrderedDict(
        material_use="Consumed", element_id="BUF:%s" % line, inlet_line=line,
        positions=[OrderedDict(original=label, state=state) for label, state in used.items()],
        percent_b_range=list(bu["pct"]) if line != "sample" and bu["pct"] else None,
        contents=None, lot_id=None, material_id=None, material_id_source="caller (UNICORN stores none)")
        for line, used in bu["lines"].items()]
    for i, g in enumerate(loads, 1):
        set_ml = sum((s.column_flow_ml_min or 0) * (s.elapsed_end_s - s.elapsed_start_s) / 60.0 for s in g)
        mats["process_inputs"].insert(i - 1, OrderedDict(
            material_use="Consumed", element_id="LOAD%d" % i, unit_procedure="UP1", injection_valve=g[0].injection_valve, start_wall_s=round(g[0].start_s, 3), end_wall_s=round(g[-1].end_s, 3),
            volume=[val(set_ml, "mL", "set_point", "calculated", "run_log")],
            material_id=None, material_id_source="caller (UNICORN stores none)"))
    for x in event_tables.rows(A["ch"], "fraction"):
        waste = bool(x["waste"])
        mats["process_outputs"].append(OrderedDict(element_id="OUT:%s" % x["name"], unit_procedure="UP1",
                                             material_use="Other" if waste else "Produced", other_value="waste" if waste else None,
                                             role="waste_line" if waste else "collected_fraction", goes_to="waste" if waste else "fraction_collector",
                                             is_sample=not waste,
                                             start_min=round(x["start_time"], 4), end_min=round(x["end_time"], 4),
                                             volume=[val(x["length_volume"], "mL", "actual", "measured", "volume_axis")],
                                             material_id=None if waste else "RUN:%s" % x["name"],
                                             material_id_source="none: a waste line is not a sample" if waste else "derived (run + tube)"))
    rec["materials"] = mats
    rec["method"] = recipe_block(A)
    rec["volumes"] = OrderedDict((k, OrderedDict(status=v["status"], text=v["text"], source=v["why"])) for k, v in A["inv"]["volumes"].items())
    rec["facts"] = [OrderedDict(group=x["group"], fact=x["fact"], needed_by=x["by"], status=x["status"], value=x["value"], source=x["source"]) for x in A["inv"]["facts"]]
    rec["views"] = OrderedDict(display=OrderedDict(
        note="computed from the sections each time; never part of the stored record",
        steps=[OrderedDict(label=st["label"], label_source=st["label_source"],
                           start_wall_s=round(st["wa"], 3), end_wall_s=round(st["wb"], 3), operation=st["op"],
                           state="held" if st["paused"] else "running",
                           column_flow=None if st["paused"] else val(st["flow"], "mL/min", "set_point", "logged", "run_log"),
                           percent_b=None if st["paused"] else val(st["b_pct"], "%", "set_point", "logged", "run_log"),
                           sections_joined=st.get("n_sections", 0), sections=[p["sec"] for p in st["parts"]],
                           absorbed_sections=[p["sec"] for p in st["parts"] if p["absorbed"]],
                           absorbed_note="; ".join(st["notes"]) or None) for st in A["steps"]]))
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
    rep = [o for o in ops if o["repeats"] > 1]
    if rep:
        items.append("Repeated phases (%s) are named with an instance number: acceptable display wording (Decide 23)?" % ", ".join(sorted({o["display_name"] for o in rep})))
    fold = [x for x in A["steps"] if x["notes"]]
    if fold:
        items.append("%d display step(s) absorb a short piece (orange, under 10 s): check the note wording and the 10 s rule (Decide 18)." % len(fold))
    if A["lead_in"]:
        li = A["lead_in"]
        items.append("CADET model view, rule 4: sections %s (%.1f to %.1f s, no flow) merge into one zero-flow section: right for the time axis?" % (
            sec_ranges(li["secs"]), li["start_s"], li["end_s"]))
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


# ----------------------------------------------------------------------------------------------
# recipe (the method as hints), edge cases, decisions to review
# ----------------------------------------------------------------------------------------------

def _op_at(A, t):
    return next((o for o in A["ops"] if o["wa"] - 1e-6 <= t < o["wb"] + 1e-6), None)


def recipe_block(A):
    """
    The method as the record would carry it (section 5, Method row): master recipe (the template),
    control recipe (the template with this run's variable values), and planned against run.
    Hints only (D17): nothing here is a result. Each planned block names the operation it ran in.
    """
    h = A["hints"]
    if not h or not h.available:
        return OrderedDict(available=False, note="no method in the result: no recipe")

    def op_ref(a, b):
        o = _op_at(A, (a + b) / 2.0) if a is not None and b is not None else None
        return OrderedDict(element_id=o["id"], label=o["label"]) if o else None

    planned = []
    for b in h.blocks:
        planned.append(OrderedDict(block=b["block"], element=op_ref(b.get("start_s"), b.get("end_s")), planned=b.get("planned"), base=b.get("base"),
                                   variable=b.get("variable"), ran=b.get("executed"), status=b.get("status"), cause=b.get("cause")))
    return OrderedDict(
        available=True, kind="hint (the method is a template, D17)",
        master_recipe=OrderedDict(name=h.method.get("name"), unicorn_version=h.method.get("unicorn_version"), technique=h.method.get("technique"),
                                  volume_unit=h.method.get("volume_unit"), flow_unit=h.method.get("flow_unit"),
                                  blocks=[x["block"] for x in h.blocks] + list(h.blocks_not_run)),
        control_recipe=OrderedDict(variables=[OrderedDict(variable=v["variable"], value=v["value"], unit=v.get("unit"), scouting=v.get("scouting"))
                                              for v in h.variables]),
        planned_vs_ran=planned, blocks_not_run=list(h.blocks_not_run),
        injections=[OrderedDict(block=i["block"], element=op_ref(i.get("start_s"), i.get("end_s")), planned=i.get("planned"), base=i.get("base"),
                                ran=i.get("executed"), variable=i.get("variable")) for i in h.injections],
        sample_application=h.sample_application, buffer_prep="stored (hint)" if h.buffer_prep else "not stored",
        scouting=OrderedDict(name=h.scouting.get("name"), runs=len(h.scouting.get("runs") or []), variables=h.scouting.get("variables") or []),
        warnings=list(h.warnings))


def recipe_html(A):
    r = recipe_block(A)
    if not r["available"]:
        return "<p class='none'>%s.</p>" % E(r["note"])
    m = r["master_recipe"]
    out = ["<p class='lead'>Hints only (D17): the method is a template, not a record of what ran. Master recipe = the method as stored. "
           "Control recipe = the same method with the values this run used. Each planned block is tied to the operation it ran in "
           "(the <code>element_id</code> the record uses), matched by time.</p>"]
    out.append("<h3>Master recipe</h3>" + table(["method", "UNICORN version", "technique", "volume unit", "flow unit", "blocks (planned order)"],
                                                [[m["name"], m["unicorn_version"], m["technique"], m["volume_unit"], m["flow_unit"], ", ".join(m["blocks"]) or "-"]], ""))
    rows = [[p["block"], ("%s (%s)" % (p["element"]["element_id"], p["element"]["label"])) if p["element"] else "-",
             "%s %s" % (f(p["planned"], 2), p["base"] or "") if p["planned"] is not None else "-", p["variable"] or "-",
             f(p["ran"], 2) if p["ran"] is not None else "-", p["status"] or "-", p["cause"] or "-"] for p in r["planned_vs_ran"]]
    out.append("<h3>Planned against run</h3>" + (table(["block", "ran in (element_id)", "planned", "set by variable", "ran (same base)", "status", "cause"], rows, "")
                                                  if rows else "<p class='none'>No block lengths could be compared.</p>"))
    if r["blocks_not_run"]:
        out.append("<p>Planned blocks that did not run: <b>%s</b>.</p>" % E(", ".join(r["blocks_not_run"])))
    if r["injections"]:
        out.append("<h3>Planned and executed injections</h3>" + table(
            ["block", "ran in (element_id)", "planned", "ran", "variable"],
            [[i["block"], ("%s (%s)" % (i["element"]["element_id"], i["element"]["label"])) if i["element"] else "-",
              "%s %s" % (f(i["planned"], 3), i["base"] or ""), f(i["ran"], 3), i["variable"] or "-"] for i in r["injections"]], ""))
    vrows = [[v["variable"], v["value"], v["unit"] or "-", "yes" if v["scouting"] else "-"] for v in r["control_recipe"]["variables"]]
    out.append("<h3>Control recipe: the values this run used</h3><details class='rawbox'><summary>%d variable(s) <span class='src'>search criteria and scouting flag</span></summary>%s</details>" % (
        len(vrows), table(["variable", "value", "unit", "scouting variable"], vrows, "")))
    if r["sample_application"]:
        sa = r["sample_application"][0]
        out.append("<p>Sample application dialog (planned): loop %s, sample volume %s ml, fill loop with %s ml, empty loop with %s ml.</p>" % (
            E(sa.get("loop_type") or "-"), E(f(sa.get("sample_volume"), 3)), E(f(sa.get("fill_loop_with"), 3)), E(f(sa.get("empty_loop_with"), 3))))
    out.append("<p>BufferPro recipe: %s. Scouting: %s.</p>" % (E(r["buffer_prep"]), E(("%d run(s), %d variable(s)" % (r["scouting"]["runs"], len(r["scouting"]["variables"])))
                                                                                if r["scouting"]["runs"] or r["scouting"]["variables"] else "none stored")))
    return "\n".join(out)


STATES_12 = ("idle", "running", "complete", "pausing", "paused", "holding", "held", "restarting", "stopping", "stopped", "aborting", "aborted")


def edge_cases_html(A):
    """Table 16: each edge case with whether this run meets it."""
    chroms, pauses = A["chroms"], A["pauses"]
    fitted = len([x for x in A["rows"] if x and x["fitted"]])
    feeds = len([1 for i, s in enumerate(A["S"]) if s.feed and (i == 0 or not A["S"][i - 1].feed)])
    scout = (A["hints"].scouting if A["hints"] and A["hints"].available else None) or {}
    byp = any(s.bypass for s in A["S"])
    sev = len(chroms) > 1
    cases = [
        ("A new chromatogram cuts inside a phase", "Chromatograms stay a field on the elements. Making them unit procedures or cycles fails with an error naming the phase that crosses the cut.",
         ("%d chromatograms: cut position not verified (needs a result that has one)" % len(chroms)) if sev else "does not apply: one chromatogram"),
        ("A pause or hold across a cycle boundary", "Belongs to the cycle in which it started.", "does not apply: no cycles marked (one unit procedure)"),
        ("A Holding or Restarting stretch the volume axis doesn't show clearly", "No transient section: a plain held stretch from the logged times.",
         ("%d pause(s); %d of %d sections fitted to the volume axis" % (len(pauses), fitted, len([x for x in A["rows"] if x]))) if pauses else "does not apply: no pause"),
        ("An instruction issued during Holding or Restarting", "Acts at the time it was logged; the transient is split there.", "not checked by this page"),
        ("A scouting run that varies settings rather than repeating cycles", "Linked as a series either way. Only the caller calls it cycles.",
         ("applies: scouting table with %d run(s)" % len(scout.get("runs") or [])) if scout.get("runs") else "does not apply: no scouting table"),
        ("Scouting runs that each hold several cycles", "Both levels: the series link and unit procedures within each.", "does not apply: no cycles marked"),
        ("Eluates pooled across cycles", "Material, outside the record. Fraction and pool rows carry the element_id of their unit procedure.",
         "one unit procedure: every fraction row carries UP1 (see the JSON)"),
        ("One run loads several materials", "One load per feed, each with its own material ID from the caller.", "%d load(s) in this run" % feeds),
        ("Pools combined outside UNICORN, then loaded on the next step", "A new material; its ID and parents come from the caller.", "does not apply: PyCORN never joins runs"),
        ("A by-pass measurement split from the column run by a new chromatogram", "Two operations in one unit procedure (same unit), unless the caller says otherwise.",
         ("applies: by-pass sections and %d chromatograms" % len(chroms)) if (byp and sev) else ("by-pass sections, one chromatogram" if byp else "does not apply: no by-pass")),
    ]
    return table(["edge case", "what the spec does with it", "this run"], cases, "")


def _text_data(A):
    """One short sentence of this run's data per data_key of the decisions file."""
    steps = [s for s in A["steps"] if not s["paused"]]
    n_secs = len([s for s in A["S"] if not s.paused])
    pieces = [p for st in A["steps"] for p in st["parts"] if p["absorbed"]]
    real = [o for o in A["ops"] if not o["synthetic"]]
    reps = sorted({o["name"] for o in real if o["repeats"] > 1})
    fitted = len([x for x in A["rows"] if x and x["fitted"]])
    tot = len([x for x in A["rows"] if x])
    big = [1 for s, r in zip(A["S"], A["rows"]) if r and r["fitted"] and r["logged_ml"] and abs(r["axis_ml"] - r["logged_ml"]) > 0.1 * r["logged_ml"] and s.end_s - s.start_s > 5]
    facts = {x["fact"]: x for x in A["inv"]["facts"]}
    vt, vo = facts.get("Total liquid volume Vt"), facts.get("Void volume Vo")
    noloop = [w for w in (A["view"].warnings if A["view"] else []) if "no loop volume given" in w]
    scout = (A["hints"].scouting if A["hints"] and A["hints"].available else None) or {}
    setml = sum((A["op_values"][o["id"]]["set"] or 0) for o in A["ops"] if o["id"] in A["op_values"])
    actml = sum((A["op_values"][o["id"]]["actual"] or 0) for o in A["ops"] if o["id"] in A["op_values"])
    bu = buffers_used(A)
    return {
        "up_cycles": "%d chromatogram(s). Default: one unit procedure, labelled '%s'. %s." % (
            len(A["chroms"]), up_label(A), ("Phase order repeats: %s (a suggested start phase)" % ", ".join(reps)) if reps else "No repeated phase"),
        "chromatograms": "%d chromatogram(s): %s. How the run log marks a cut: not verified." % (len(A["chroms"]), ", ".join(c["name"] for c in A["chroms"])),
        "display_steps": "%d running sections (pauses not counted) become %d running display steps." % (n_secs, len(steps)),
        "join_rule": "%d running sections become %d running display steps. %d short piece(s) under 10 s were absorbed%s." % (
            n_secs, len(steps), len(pieces), (" (%s s)" % ", ".join(f(p["b"] - p["a"], 1) for p in pieces[:6])) if pieces else ""),
        "repeats": ("Repeated: %s. Shown as an instance number." % ", ".join(reps)) if reps else "No repeated phase in this run.",
        "ramps_flow": "%d of %d sections fitted to the volume axis; %d section(s) over 5 s where the pump delivered more than 10 %% off the set flow." % (fitted, tot, len(big)),
        "transients": ("%d pause(s), lengths %s s." % (len(A["pauses"]), ", ".join(f(p["duration_s"], 1) for p in A["pauses"]))) if A["pauses"] else "No pause in this run.",
        "dbc_volumes": "Total liquid volume: %s. Void volume: %s. Both are library values from the column type, not measured on this packing." % (
            (vt["value"] if vt else "-"), (vo["value"] if vo else "-")),
        "feed_loop": ("The feed length rests on the whole Inject period: no loop volume given." if noloop else "No feed without a loop volume in this run."),
        "composition_order": "Buffer contents: from the caller (UNICORN stores none). BufferPro recipe: %s." % ("stored (hint)" if bu["bufferpro"] else "not stored"),
        "buffer_layers": "Buffer lot IDs, preparation and measurements: none in the result. BufferPro recipe: %s." % ("stored (hint)" if bu["bufferpro"] else "not stored"),
        "volume_precedence": "Volume axis against logged flow: set %s ml, actual %s ml over all operations." % (f(setml, 2), f(actml, 2)),
        "process_operation": "Empty: the caller maps phases to process operations.",
        "scouting_series": ("Scouting table: %d run(s)." % len(scout.get("runs") or [])) if scout.get("runs") else "No scouting table.",
        "calibration": "Extinction coefficient: from the caller (UNICORN stores none).",
        "none": "-",
        "phases": "%d phases in %d operations; %d blocks called by the operations are smaller logic or steps." % (
            len(A["phase_list"]), len(A["ops"]), len([c for c in A["phase_candidates"] if c["cls"] != "task"])),
        "t8": "; ".join("%s: %s" % (c["block"][:24], c["why"][:48]) for c in A["phase_candidates"] if c["cls"] == "smaller logic" and c["planned"] is not None) or "No block with a planned length is classified smaller logic in this run.",
    }


# options of these items are not stated in the spec; the page says so
NO_SPEC_OPTIONS = {"feed-wording", "gradient-wording", "section-lane-chart", "summary-beside-or-instead", "flow-users-see", "cycle-flag-name"}

DECIDED_LATER = {"dbc-volumes": "Display needs both volumes for DBC. If CADET computes DBC, only CADET needs them (owner)."}

# decided or proposed in review sessions, not (yet) in the decisions file
SESSION_DECISIONS = [
    dict(id="up-label", title="Unit procedure label", status="decided", data_key="up_cycles",
         question="What does a unit procedure show as its name?",
         options=["Empty when the run has one unit procedure.", "The chromatogram's name when the result holds exactly one, else the result name (UNICORN)."],
         proposed="Second option (owner chose a label so people know what it is).", links=["rst:15", "rs:3.3"]),
    dict(id="step-label", title="Display step label", status="decided", data_key="display_steps",
         question="What names a step cut from one operation?",
         options=["A counter ('Run (1 of 4)').", "The operation's name plus the activity words that are on (feed, by-pass, Hold; a pause is Held). Equal labels counted in run order.",
                  "The operation's name plus the changed value."],
         proposed="Second option (owner chose it).", links=["rst:15", "rs:3.5"]),
    dict(id="method-settings", title="Method Settings as an operation", status="decided", data_key="none",
         question="Is UNICORN's short first phase kept as an operation or folded into the next?",
         options=["Always kept as an operation.", "Folded into the next operation."], proposed="Kept (owner decision).", links=["rst:15"]),
    dict(id="first-position", title="Inlet first position (Decide 22)", status="open", data_key="inlets",
         question="When a valve has no position logged yet, is its first position known, assumed or not known?",
         options=["Assumed for every system type, labelled '(assumed)' (built).",
                  "Known for an AKTA model whose start-of-run positions are verified, assumed for any other model until it is verified."],
         proposed="Second option (owner, 2026-11-15). The system type is the AKTA model, not SystemTypeName. Assumed is correct for a model not yet verified. "
                  "The owner verifies the models that have test data. None is verified yet. Not built: it needs a map of verified models to first positions.",
         links=["decide:22", "rst:7"]),
    dict(id="extinction-needed-by", title="Who needs the extinction coefficient", status="decided", data_key="calibration",
         question="Does the display or CADET need the extinction coefficient?", options=["CADET only.", "Both: the display for mg/ml, CADET for its detector calibration."],
         proposed="Both (owner).", links=["rs:3.9", "rst:14"]),
    dict(id="phases-reading-d", title="Which blocks are phases (reading D)", status="decided", data_key="phases",
         question="What is a phase, and what are the operations?",
         options=["Operation = UNICORN phase, phase = equipment activity (the earlier reading).",
                  "Operation = each element the main block calls, phase = task block, equipment activities in their own list."],
         proposed="Second option (owner, 2026-11-15). Nested blocks are steps, loop iterations are repeated phases, a watch-started phase stays sequential, "
                  "the pump's whole-run flow is dropped, display steps never cross a phase boundary.", links=["rs:3.5.1", "rst:6", "rst:15"]),
    dict(id="task-block-test", title="Task block or smaller logic (test T8)", status="proposed", data_key="t8",
         question="How is a block with a planned length that only controls equipment (for example one that stops fractionation) told from a task?",
         options=["Every block with a planned length is a task.", "A block whose own instructions only control equipment is smaller logic (waits ignored); a block with a process instruction or a call is a task."],
         proposed="Second option, proposed on thin evidence: the public samples show only zero-length smaller logic. The caller can list block names to force either reading.",
         links=["rs:3.5.1"]),
    dict(id="steps-below-phase", title="Store steps and transitions below the phase", status="open", data_key="none",
         question="Does the record store IEC-style steps and transitions derived from breakpoints and zero-length blocks?",
         options=["No: sections stay the lowest stored level (today).", "Yes: store them under each phase."],
         proposed="Open for a possible future; the owner does not want it now.", links=["rs:3.5.1", "rs:11"]),
    dict(id="recipe-section", title="Recipe information on this page", status="proposed", data_key="none",
         question="Does the record carry the method as master and control recipe, as hints?",
         options=["Yes: a Method part in the record and a Recipe section here.", "No: the method stays raw text only."],
         proposed="Yes (section 5, Method row). Built in this prototype; nothing is built in the library.", links=["rs:5", "rs:3.2"]),
]


def phases_html(A):
    """Phases per operation, how every candidate block was classified, and the equipment phases (section 3.5.1)."""
    out = ["<p class='lead'>A phase is a task block: a block the operation reaches by a Block or Loop call, with a planned length above 0 "
           "(this run's variable values applied). The test that separates a task from smaller logic is proposed (3.5.1 item 6): a block that only "
           "controls equipment, or has no planned length in this run, is smaller logic. Blocks inside a task block are steps of it. A watch-started "
           "block is not read as a phase yet: no public sample fires one.</p>"]
    rows = []
    for op in A["ops"]:
        for p in A["phases"][op["id"]]:
            how = {"task": "task block", "own": "operation's own stretch", "operation": "operation has no task block"}[p["kind"]]
            rows.append([p["id"], op["label"], p["label"], how, f(p["wa"], 1), f(p["wb"], 1),
                         ("%s %s" % (f(p["planned"], 2), p["base"])) if p["planned"] is not None else "-",
                         f(p["executed"], 2) if p["executed"] is not None else "-", p["status"] or "-"])
    out.append("<h4>Phases</h4>" + table(["id", "operation", "phase", "how found", "start s", "end s", "planned", "ran (same base)", "status"], rows, ""))
    crow = []
    phase_of = {p["id"]: p["label"] for p in A["phase_list"]}
    for c in A["phase_candidates"]:
        pr = c["planned"]
        crow.append([c["op"], c["block"], c["depth"], ("%s %s" % (f(pr["planned"], 2), pr["base"])) if pr else "-", c["cls"], c["why"],
                     phase_of.get(c["phase"], "-")])
    out.append("<h4>Every block the operations called, and how it was classified</h4>" + (
        table(["operation", "block as logged", "depth", "planned length", "class", "why", "phase"], crow, "")
        if crow else "<p class='none'>The operations call no blocks: each is one phase (no method, or no Block lines).</p>"))
    erows = [[e["id"], e["label"], e["module"], f(e["wa"], 1), f(e["wb"], 1), e["commanded_by"] or "-"] for e in A["equipment_phases"]]
    out.append("<h4>Equipment phases (feed, fractionation, by-pass)</h4><p class='lead'>Equipment control, not children of operations: they may overlap "
               "and cross operation boundaries. Fractionation counts collected fractions only: a waste line is not a fractionation.</p>" + (
                   table(["id", "equipment phase", "equipment module", "start s", "end s", "commanded by (phase)"], erows, "")
                   if erows else "<p class='none'>No feed, fractionation or by-pass in this run.</p>"))
    return "\n".join(out)


def decisions_html(A):
    with open(DECISIONS_FILE, encoding="utf-8") as fh:
        items = json.load(fh)
    items = [dict(x) for x in items] + [dict(x) for x in SESSION_DECISIONS]
    for x in items:  # settled in review sessions after the file was extracted
        if x["id"] in DECIDED_LATER:
            x["status"], x["proposed"] = "decided", DECIDED_LATER[x["id"]]
    data = _text_data(A)
    data["inlets"] = "System type (AKTA model): %s, not verified: assumed is right for it. %s" % (A["instrument"] or "not stored", "; ".join(
        "%s: %s" % (line, ", ".join(used)) for line, used in buffers_used(A)["lines"].items()) or "-")
    rows = []
    for x in items:
        opts = "".join("<li>%s</li>" % E(o) for o in x["options"])
        note = "<br><i>The spec states no options for this item. These are placeholders.</i>" if x["id"] in NO_SPEC_OPTIONS else ""
        where = [link_code(c) for c in x["links"]]
        if x.get("spec_line"):
            where.append(H("<span class='docs'>spec line %s</span>" % E(str(x["spec_line"]))))
        rows.append([H("<b>%s</b><br>%s" % (E(x["title"]), E(x["question"]))), data.get(x["data_key"], "-"), H("<ol>%s</ol>%s" % (opts, note)), x["proposed"],
                     badge({"open": "caller", "decided": "stored", "proposed": "hint"}[x["status"]], x["status"]), links(*where)])
    return table(["decision", "this run", "options", "proposed answer", "status", "where"], rows, "wide")


def link_code(code):
    kind, _, arg = code.partition(":")
    if kind == "rs":
        return rs(arg)
    if kind == "rst":
        return rst(arg)
    if kind == "rsf":
        return rsf(arg)
    if kind == "decide":
        return decide(int(arg))
    if kind == "ref":
        fn, _, prefix = arg.partition("|")
        return ref(prefix, fn, prefix=prefix)
    raise ValueError("unknown link code %r" % code)


def page(A, inv, rec_name):
    run, ch = A["run"], A["ch"]
    S = A["S"]
    vc = (A["view"].column or {}).get("vc_m3") if A["view"] else None
    steps_n = len([x for x in A["steps"] if not x["paused"]])
    tiles = [("%d" % len(A["ops"]), "operations"), ("%d" % len(A["phase_list"]), "phases (task blocks)"), ("%d" % len(A["equipment_phases"]), "equipment phases"),
             ("%d" % len([s for s in S if not s.paused]), "sections (pauses not counted)"), ("%d" % steps_n, "running display steps"),
             ("%d" % len(A["pauses"]), "pauses"), ("%d / %d" % (len([x for x in A["rows"] if x and x["fitted"]]), len([x for x in A["rows"] if x])), "sections fitted to the volume axis"),
             ("%s min" % f((A["end_wall"] or 0) / 60, 1), "wall-clock length"), ("%s ml" % f(vc * 1e6, 2) if vc else "none", "Vc"),
             ("%d" % len([x for x in inv["facts"] if x["status"] in ("missing", "caller")]), "facts the caller must give or UNICORN lacks")]
    warns = list(A["r"]["warnings"]) + list(A["dwarn"]) + (list(A["view"].warnings) if A["view"] else [])
    uniq = list(OrderedDict.fromkeys(warns))
    parts = ['<!doctype html><html lang="en"><head><meta charset="utf-8"><title>%s</title><style>%s</style></head><body>' % (E(A["file"]), CSS)]
    parts.append('<header><h1>%s</h1><p>%s &middot; UNICORN %s &middot; %s &middot; <a style="color:#9ec5ff" href="index.html">all examples</a> &middot; '
                 'prototype record: <a style="color:#9ec5ff" href="%s">%s</a></p></header>' % (
                     E(A["file"]), E(A["instrument"] or "instrument not stored"), E(str(run.unicorn_version)), E(SAMPLE_NOTES.get(A["file"], "")), E(rec_name), E(rec_name)))
    parts.append('<nav><a href="#checks">Check these</a><a href="#decide">Decisions</a><a href="#above">Above the run</a><a href="#levels">Levels</a><a href="#setact">Set point vs actual</a><a href="#curves">Curves</a><a href="#recipe">Recipe</a>'
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
    parts.append('<h2 id="decide">Decisions to review</h2><p class="lead">Every open choice in the proposal, with what this run shows, the options, '
                 'the proposed answer and where the proposal explains it. Decided and proposed items from review sessions are listed too.</p>%s' % safe("decisions", decisions_html, A))
    parts.append('<h2 id="above">0. Above and beside the operations</h2><p class="lead">Linked runs, procedure, unit '
                 'procedure and cycles, chromatograms, equipment, material. Border: green UNICORN stores it, blue PyCORN works it out, amber a hint only, '
                 'orange dashed the caller supplies it, red dashed UNICORN lacks it.</p>%s<div class="fig">%s</div>' % (
                     docs(rs("3.1"), rs("3.2"), rs("3.3"), rs("3.4"), rst(3), rst(4), rst(5), rsf(2), rsf(3), rsf(6)), safe("above", fig_hierarchy, A)))
    parts.append('<h2 id="levels">1. Levels, states and display steps</h2><p class="lead">Operation = UNICORN phase (or a top-level block), phase = '
                 'task block (a block the operation calls with a planned length), section = one executed instruction. Feed, fractionation and by-pass are '
                 'equipment phases (bottom rows). State follows ISA-88 (Pause is Held, Hold is Paused).</p>%s<div class="fig">%s</div>' % (
                     docs(rs("3.5"), rst(6), rsf(7), rs("3.7"), rst(11), rst(12), rsf(10), rst(22), rst(23), decide(23), decide(18)), safe("levels", fig_levels, A)))
    parts.append('<h3>Where sections were joined</h3><p class="lead">The same display steps with every original section as a cell. '
                 'Long runs hide the short pieces in the figure above.</p><div class="fig">%s</div>' % safe("joins", fig_joins, A))
    parts.append('<h2 id="setact">2. Set point next to actual</h2><p class="lead">What the method set, what the pump delivered, what UNICORN measured. '
                 'Measured traces break at pauses: no data is stored while the run is held.</p>%s<div class="fig">%s</div>' % (
                     docs(rs("3.8"), rst(13), rst(18), decide(6), decide(15), decide(18), ref("Scope row 8", RM_, "Scope decisions for CADET")), safe("setact", fig_setpoint_actual, A)))
    parts.append('<h2 id="curves">3. Measured curves by role</h2>%s<div class="fig">%s</div>' % (
        docs(ref("Finding curves by role", UG, "Finding Curves: by Role, Not by Name"), ref("Axes", UG, "Axes"), rs("5")), safe("curves", fig_curves, A)))
    parts.append('<h2 id="recipe">Recipe: the method as downstream receives it</h2>%s%s' % (
        docs(rs("5"), rs("3.2"), ref("Method hints", UG, "Method hints")), safe("recipe", recipe_html, A)))
    parts.append('<h2 id="tables">4. Tables</h2>' + docs(rs("5"), rst(17), rst(18), rst(21), ref("Export guide", EG, "Phase window statistics")))
    parts.append("<h3>Operations, with set point and actual volumes</h3>" + safe("ops", ops_table, A))
    parts.append("<h3>Phases (reading D) and the blocks they come from</h3>" + docs(rs("3.5.1"), rst(6), rst(15), rst(16)) + safe("phases", phases_html, A))
    parts.append("<h3>Display steps (proposed table)</h3><p class='lead'>Rule 1: one step per phase (a step never crosses a phase boundary), split where flow, %B, feed, by-pass or Hold changes. Rule 2: every pause is its own Held row. "
                 "Rule 3: a piece under 10 s joins the step after it at the start of an operation or after a pause, else the step before it. A Hold, feed or by-pass is never hidden: a short piece that has one joins only a step that has it too. "
                 "Bold rows absorbed such a piece (orange in the figure). The sections table below has the same # numbers.</p>" + safe("steps", steps_table, A))
    parts.append("<h3>States other than Running</h3>" + safe("states", states_html, A))
    li = A["lead_in"]
    li_note = ("<p class='lead'>Rule 4 (CADET model view): sections %s (%.1f - %.1f s) have no flow, no feed, no by-pass and no ramp. "
               "They merge into one zero-flow section, so the time axis and the signals stay aligned. The valves as set at %.1f s carry into "
               "the first flow section. Pauses and later zero-flow stretches are not merged.</p>" % (
                   sec_ranges(li["secs"]), li["start_s"], li["end_s"], li["last_start_s"])) if li else ""
    parts.append("<h3>Sections (the CADET side, 1:1 with today's table)</h3>" + li_note + safe("sections", sections_table, A))
    parts.append("<h3>Material: process inputs, process outputs, pools</h3>" + safe("materials", materials_html, A))
    parts.append("<h3>Edge cases (Table 16) on this run</h3>" + docs(rst(16)) + safe("edge", edge_cases_html, A))
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
