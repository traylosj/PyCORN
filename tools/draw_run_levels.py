#!/usr/bin/env python
"""
Draw Figure 7 of docs/RUN_STRUCTURE_RECORD.md (section 3.5): the ISA-88 levels of one run.

    python tools/draw_run_levels.py [-o docs/images/u73_run_levels.svg]

Drawn by Claude (an AI assistant) from the public example result samples/u73_blank.zip.
Rows, top to bottom: the record's levels from the section 3.5 table, operations (UNICORN
phases), phases (UNICORN blocks, and the sample feed as one activity) and sections (the
run-log sections the CADET export uses); then the display steps (proposed, not in the
record: sections joined, the previous phase's set points folded in when replaced within
10 s, decided 2026-11-12 for display). Below them the column flow and %B set points from the
run log. Plain SVG, same look as docs/images/transient_states.svg.
"""
import argparse
import os
import sys
from html import escape

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from pycorn import open_run  # noqa: E402
from pycorn import event_tables  # noqa: E402
from pycorn.run_log_sections import run_log_sections  # noqa: E402

parser = argparse.ArgumentParser(description="Draw docs/images/u73_run_levels.svg.")
parser.add_argument("-o", "--output", default=os.path.join(ROOT, "docs", "images", "u73_run_levels.svg"),
                    help="the SVG to write (default: %(default)s)")
OUT = parser.parse_args().output

CUTOFF_S = 10.0  # display fold: a previous phase's set point replaced within this many seconds

FONT = "Segoe UI, Helvetica, Arial, sans-serif"
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"
LEVEL_FILLS = ("#e9e8e4", "#d6d5d0")
STEP_FILLS = ("#b7d3f6", "#86b6ef")
PREVIOUS = "#eb6834"
FEED = "#1baf7a"
LINE = "#2a78d6"

run = open_run(os.path.join(ROOT, "samples", "u73_blank.zip"))
chrom = run.chromatogram()
rls = run_log_sections(run, chrom)
sections = rls["sections"]
entries = rls["entries"]
operations = [r for r in event_tables.rows(chrom, "phase") if not r["pseudo"]]
for op in operations:
    op["a"], op["b"] = op["start_time"] * 60.0, op["end_time"] * 60.0
    op["label"] = op["name"].replace("Phase ", "")


def flow(s):
    return 0.0 if s.paused else (s.column_flow_ml_min or 0.0)


def pct(s):
    return s.percent_b[0] or 0.0


KIND = {"flow": "system_flow", "b": "gradient"}


def steps_for(op):
    """The operation's sections, the previous phase's set points folded in, equal pieces merged."""
    pieces = []
    for s in sections:
        a, b = max(s.elapsed_start_s, op["a"]), min(s.elapsed_end_s, op["b"])
        if s.paused:
            if not op["a"] - 1e-6 <= s.elapsed_start_s < op["b"] - 1e-6:
                continue
        elif not (s.elapsed_end_s > op["a"] + 1e-6 and s.elapsed_start_s < op["b"] - 1e-6):
            continue
        pieces.append(dict(a=a, b=b, flow=flow(s), b_pct=pct(s), slope=s.percent_b[1], feed=bool(s.feed),
                           paused=s.paused, previous=set()))
    for key, kind in KIND.items():
        first = next((e["elapsed_s"] for e in entries if e["kind"] == kind
                      and op["a"] - 1e-6 <= e["elapsed_s"] < op["b"] - 1e-6), None)
        if first is None or first - op["a"] > CUTOFF_S:
            continue
        after = next(x for x in pieces if x["a"] >= first - 1e-6 and not x["paused"])
        for x in pieces:
            if x["b"] <= first + 1e-6 and not x["paused"]:
                field = "flow" if key == "flow" else "b_pct"
                if x[field] != after[field]:
                    x["previous"].add(key)
                x[field] = after[field]
    steps = []
    for x in pieces:
        if steps and not x["paused"] and not steps[-1]["paused"] and all(
                steps[-1][k] == x[k] for k in ("flow", "b_pct", "slope", "feed")):
            steps[-1]["b"] = x["b"]
        else:
            steps.append(dict(x))
    return pieces, steps


PIECES, STEPS = [], []
for op in operations:
    p, s = steps_for(op)
    PIECES.extend(p)
    STEPS.extend(s)

parts = []


def text(x, y, s, size=12, fill=INK, anchor="start", weight="normal"):
    parts.append('<text x="%.1f" y="%.1f" font-family="%s" font-size="%g" fill="%s" text-anchor="%s" '
                 'font-weight="%s">%s</text>' % (x, y, FONT, size, fill, anchor, weight, escape(s)))


def rect(x, y, w, h, fill, rx=0):
    parts.append('<rect x="%.2f" y="%.2f" width="%.2f" height="%.2f" fill="%s" rx="%g"/>'
                 % (x, y, max(w, 1.5), h, fill, rx))


def pause_mark(x, y):
    parts.append('<path d="M %.1f %.1f l 6 7 l -6 7 l -6 -7 z" fill="%s"/>' % (x, y, INK))


def fmt(v):
    return "%g" % round(v, 2)


W, L, R = 1240, 230, 30
END = max(s.elapsed_end_s for s in sections)
X = lambda t: L + t * (W - L - R) / END  # noqa: E731

y = 34
text(20, y, "Operations, phases, sections and display steps of one run (samples/u73_blank.zip)", 16,
     weight="600")
y += 20
text(20, y, "Drawn by Claude from the example result. x: method time. A pause has no method time: "
            "drawn as a marker.", 12, INK2)
y += 36


def row(label, sub, h=26):
    global y
    top = y
    text(L - 12, top + h / 2 + 4, label, 12, INK, "end", "600")
    text(L - 12, top + h / 2 + 18, sub, 10.5, INK2, "end")
    y += h + 26
    return top, h


# operations = UNICORN phases
top, h = row("Operation", "UNICORN: phase")
for i, op in enumerate(operations):
    w = X(op["b"]) - X(op["a"])
    rect(X(op["a"]), top, w - 1, h, LEVEL_FILLS[i % 2], 3)
    if w > 90:
        text(X(op["a"]) + 6, top + 17, op["label"], 11.5)
    else:
        text(X((op["a"] + op["b"]) / 2), top - 4, op["label"], 10.5, INK2, "middle")

# phases = UNICORN blocks (zero-length blocks not drawn)
top, h = row("Phase", "UNICORN: block")
blocks, cur = [], None
for s in sections:
    if s.paused:
        continue
    if s.block and cur and cur["name"] == s.block and abs(cur["b"] - s.elapsed_start_s) < 1e-6:
        cur["b"] = s.elapsed_end_s
    elif s.block:
        cur = dict(name=s.block, a=s.elapsed_start_s, b=s.elapsed_end_s)
        blocks.append(cur)
for i, bl in enumerate(blocks):
    w = X(bl["b"]) - X(bl["a"])
    rect(X(bl["a"]), top, w - 1, h, LEVEL_FILLS[i % 2], 3)
    if w > 70:
        text(X(bl["a"]) + 6, top + 17, bl["name"], 11)

# one phase activity: the sample feed
top, h = row("Phase activity", "sample feed (valve at Inject)")
feed = [s for s in sections if s.feed and not s.paused]
for s in feed:
    rect(X(s.elapsed_start_s), top + 4, X(s.elapsed_end_s) - X(s.elapsed_start_s), h - 8, FEED, 2)
fa, fb = min(s.elapsed_start_s for s in feed), max(s.elapsed_end_s for s in feed)
text(X(fb) + 6, top + 17, "sample feed, %.1f s" % (fb - fa), 10.5, INK2)

# sections: the record's lowest level, as the model gets them
top, h = row("Section", "%d; CADET-Process: Phase" % len(sections))
for i, s in enumerate(sections):
    if s.paused:
        continue
    prev = any(s.elapsed_start_s >= pc["a"] - 1e-6 and s.elapsed_end_s <= pc["b"] + 1e-6 and pc["previous"]
               for pc in PIECES)
    rect(X(s.elapsed_start_s), top, X(s.elapsed_end_s) - X(s.elapsed_start_s) - 1, h,
         PREVIOUS if prev else STEP_FILLS[i % 2])
for s in sections:
    if s.paused:
        pause_mark(X(s.elapsed_start_s), top + 6)

# display steps (proposed, not in the record): sections joined, previous set points folded
parts.append('<line x1="20" x2="%d" y1="%.1f" y2="%.1f" stroke="%s" stroke-dasharray="4 4"/>'
             % (W - R, y - 13, y - 13, INK2))
text(20, y - 1, "Display (not in the record)", 10.5, INK2)
y += 10
top, h = row("Display step", "%d; sections joined (proposed)" % len(STEPS))
n = 0
for st in STEPS:
    if st["paused"]:
        pause_mark(X(st["a"]), top + 6)
        continue
    n += 1
    w = X(st["b"]) - X(st["a"])
    rect(X(st["a"]), top, w - 1, h, STEP_FILLS[n % 2])
    if w > 80:
        text(X(st["a"]) + 5, top + 17, "%s ml/min, %s %%B" % (fmt(st["flow"]), fmt(st["b_pct"])), 10.5)


def setpoint_panel(label, unit_max, ticks, value, unit):
    global y
    y += 4
    h = 90
    top = y
    text(L - 40, top + h / 2 + 4, label, 12, INK, "end", "600")
    for t in ticks:
        yy = top + h - t / unit_max * h
        parts.append('<line x1="%d" x2="%d" y1="%.1f" y2="%.1f" stroke="%s" stroke-width="1"/>'
                     % (L, W - R, yy, yy, GRID))
        text(L - 6, yy + 4, "%g" % t, 10.5, INK2, "end")
    pts = []
    for s in sections:
        if s.paused:
            continue
        v = top + h - value(s) / unit_max * h
        pts += [(X(s.elapsed_start_s), v), (X(s.elapsed_end_s), v)]
    parts.append('<path d="M %s" fill="none" stroke="%s" stroke-width="2" stroke-linejoin="round"/>'
                 % (" L ".join("%.1f %.1f" % p for p in pts), LINE))
    text(W - R, top - 4, unit, 10.5, INK2, "end")
    y += h + 26


setpoint_panel("Column flow", 5.0, (0, 1, 2.5, 5), flow, "ml/min, set point (run log)")
setpoint_panel("%B", 100.0, (0, 50, 100), pct, "%B, set point (run log)")
for m in range(0, int(END / 60) + 1, 2):
    xx = X(m * 60)
    parts.append('<line x1="%.1f" x2="%.1f" y1="%d" y2="%d" stroke="%s"/>' % (xx, xx, y - 26, y - 20, INK2))
    text(xx, y - 6, str(m), 10.5, INK2, "middle")
text(W - R, y + 10, "method time (min)", 11, INK2, "end")
y += 22

lx = L
for fill, name in ((STEP_FILLS[1], "section / display step (alternating shade)"),
                   (PREVIOUS, "previous phase's set point still active"), (FEED, "sample feed")):
    rect(lx, y, 14, 12, fill, 2)
    text(lx + 20, y + 10, name, 11, INK2)
    lx += 270
pause_mark(lx + 6, y - 1)
lengths = sorted({"%.0f s" % (s.end_s - s.start_s) for s in sections if s.paused})
text(lx + 18, y + 10, "pause, %s (ISA-88 Held)" % " / ".join(lengths), 11, INK2)
y += 30

svg = ('<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">'
       '<rect width="100%%" height="100%%" fill="%s"/>%s</svg>\n' % (W, y, W, y, SURFACE, "".join(parts)))
with open(OUT, "w", encoding="utf-8", newline="\n") as f:
    f.write(svg)
print("[OK] wrote", OUT, "height", y)
