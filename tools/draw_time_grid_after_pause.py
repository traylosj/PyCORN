#!/usr/bin/env python
"""
Draw the figure of docs/TIME_GRID_AFTER_PAUSE.md (docs/FACTS.md, F25) from
samples/u711_manual_run.zip.

    python tools/draw_time_grid_after_pause.py [-o docs/images/time_grid_after_pause.svg] [--table FILE.md]

Panel A: one slower curve's points around the first pause, grid time vs the time its stored
volume points to (read off the fastest curve). Panel B: that offset in each stretch between
pauses for three slower curves. Plain SVG, same look as the docs/images/logic_*.svg figures.
`--table` also writes panel B's numbers as a markdown table (the one in the page).
"""
import argparse
import os
import sys

import numpy as np

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from pycorn.sources import open_run  # noqa: E402

parser = argparse.ArgumentParser(description="Draw the figure of docs/TIME_GRID_AFTER_PAUSE.md from "
                                             "samples/u711_manual_run.zip.")
parser.add_argument("-o", "--output", default=os.path.join(ROOT, "docs", "images", "time_grid_after_pause.svg"),
                    help="the SVG to write (default: %(default)s)")
parser.add_argument("--table", help="also write panel B's numbers as a markdown table")
args = parser.parse_args()
SAMPLE = os.path.join(ROOT, "samples", "u711_manual_run.zip")
OUT = args.output
TABLE = args.table

FONT = "Segoe UI, Helvetica, Arial, sans-serif"
INK, INK2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"

# ---------------------------------------------------------------------------
# data
# ---------------------------------------------------------------------------
run = open_run(SAMPLE, adjust_axes=False)
chrom = run.chromatogram()
curves = [c for c in chrom.curves.values()
          if c.time is not None and c.volume is not None and c.values is not None and len(c.time) > 2]
ref = min(curves, key=lambda c: float(c.time[1] - c.time[0]))
rt = ref.time.astype(float) * 60.0  # s
rv = ref.volume.astype(float)
rising = np.concatenate([[True], np.diff(rv) > 1e-6])
it, iv = rt[rising], rv[rising]
slope = np.diff(rv) / np.diff(rt)

pauses = sorted({round(float(e.time) * 60.0, 3) for ec in chrom.event_curves.values() for e in ec.events
                 if e.time is not None and (e.text or "").startswith("Pause")})
t_end = float(ref.time[-1]) * 60.0


def offsets(curve):
    """Grid time (s), implied time (s), flowing mask."""
    t = curve.time.astype(float) * 60.0
    v = curve.volume.astype(float)
    inside = (v > iv[0]) & (v < iv[-1])
    implied = np.full(len(t), np.nan)
    implied[inside] = np.interp(v[inside], iv, it)
    flowing = inside & (np.interp(t, rt[1:], slope) > 0.1 * slope.max())
    return t, implied, flowing


def rate_text(curve):
    hz = 1.0 / (float(curve.time[1] - curve.time[0]) * 60.0)
    return "%g Hz" % round(hz, 2)


A_CURVE = "System flow"
B_CURVES = [("UV 2_0", BLUE), ("System flow", ORANGE), ("Cond temp", AQUA)]
a_color = dict(B_CURVES)[A_CURVE]

# stretches between pauses: (label, from s, to s)
edges = [0.0] + pauses + [t_end]
stretches = []
for k in range(len(edges) - 1):
    a_name = "start" if k == 0 else "P%d" % k
    b_name = "end" if k == len(edges) - 2 else "P%d" % (k + 1)
    stretches.append(("%s-%s" % (a_name, b_name), edges[k], edges[k + 1]))

levels = {}  # curve name -> list of (median offset s or None)
for name, _ in B_CURVES:
    t, implied, flowing = offsets(chrom.curves[name])
    off = implied - t
    row = []
    for _, a, b in stretches:
        m = flowing & (t > a) & (t <= b)
        row.append(float(np.median(off[m])) if m.sum() >= 3 else None)
    levels[name] = row

# ---------------------------------------------------------------------------
# svg helpers
# ---------------------------------------------------------------------------
parts = []


def text(x, y, s, size=12, fill=INK2, anchor="start", weight="normal"):
    s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    parts.append('<text x="%.1f" y="%.1f" font-family="%s" font-size="%d" fill="%s" text-anchor="%s" '
                 'font-weight="%s">%s</text>' % (x, y, FONT, size, fill, anchor, weight, s))


def line(x1, y1, x2, y2, stroke=AXIS, width=1, dash=None):
    d = ' stroke-dasharray="%s"' % dash if dash else ""
    parts.append('<line x1="%.2f" y1="%.2f" x2="%.2f" y2="%.2f" stroke="%s" stroke-width="%g"%s/>'
                 % (x1, y1, x2, y2, stroke, width, d))


def circle(x, y, r, fill, stroke=SURFACE, width=2):
    parts.append('<circle cx="%.2f" cy="%.2f" r="%g" fill="%s" stroke="%s" stroke-width="%g"/>'
                 % (x, y, r, fill, stroke, width))


W = 1000
L, R = 190, W - 40  # plot left / right edge

text(20, 30, "After a pause, slower curves drift from the time grid by up to one of their samples",
     16, INK, weight="600")
text(20, 50, "samples/u711_manual_run.zip. Reference: the fastest curve (%s, %s); its stored volume says when "
             "each point of a slower curve was taken." % (ref.name, rate_text(ref)))

# ---------------------------------------------------------------------------
# panel A
# ---------------------------------------------------------------------------
c = chrom.curves[A_CURVE]
t, implied, _ = offsets(c)
p0 = pauses[0]
lo, hi = p0 - 6.5, p0 + 6.5
top = 90
text(20, top, "A. %s (%s) around the first pause" % (A_CURVE, rate_text(c)), 13, INK, weight="600")
y_grid, y_imp = top + 50, top + 120
xa = lambda s: L + (s - lo) / (hi - lo) * (R - L)  # noqa: E731
for y, label1, label2 in ((y_grid, "Curve.time", "the grid; UNICORN plots here"),
                          (y_imp, "time from stored volume", "read off the fastest curve")):
    line(L, y, R, y, GRID)
    text(L - 14, y - 2, label1, 11, INK, "end")
    text(L - 14, y + 12, label2, 10, MUTED, "end")
line(xa(p0), top + 25, xa(p0), y_imp + 25, INK2, 1.5, "4 3")
text(xa(p0), top + 20, "Pause: method time stops, then Continue", 11, INK2, "middle")
sel = np.where((t >= lo) & (t <= hi) & np.isfinite(implied))[0]
for i in sel:
    line(xa(t[i]), y_grid, xa(implied[i]), y_imp, AXIS, 1.5)
for i in sel:
    circle(xa(t[i]), y_grid, 5, SURFACE, a_color, 2)
    circle(xa(implied[i]), y_imp, 5, a_color)
after = [i for i in sel if t[i] > p0]
before = [i for i in sel if t[i] < p0]
if before:
    k = before[len(before) // 2]
    text(xa(t[k]), y_imp + 24, "before: 0 s", 11, INK, "middle", "600")
if after:
    k = after[len(after) // 2]
    text(xa(t[k]), y_imp + 24, "after: %+.1f s" % (implied[k] - t[k]), 11, INK, "middle", "600")
ax_y = y_imp + 42
line(L, ax_y, R, ax_y, AXIS)
for s in range(int(np.ceil(lo)), int(np.floor(hi)) + 1, 2):
    line(xa(s), ax_y, xa(s), ax_y + 4, AXIS)
    text(xa(s), ax_y + 17, "%d" % s, 10, MUTED, "middle")
text((L + R) / 2, ax_y + 33, "method time (s)", 11, INK2, "middle")

# ---------------------------------------------------------------------------
# panel B: one group of dots per stretch between pauses
# ---------------------------------------------------------------------------
top = ax_y + 72
text(20, top, "B. Offset in each stretch between pauses (time from stored volume minus grid time)",
     13, INK, weight="600")
# legend row
lx = 20
for name, color in B_CURVES:
    circle(lx + 5, top + 22, 5, color)
    label = "%s (%s)" % (name, rate_text(chrom.curves[name]))
    text(lx + 15, top + 26, label, 11, INK)
    lx += 30 + 6.5 * len(label)
y0, y1 = top + 45, top + 245
vmin, vmax = -1.0, 1.0
yb = lambda v: y1 - (v - vmin) / (vmax - vmin) * (y1 - y0)  # noqa: E731
for v in (-1.0, -0.5, 0.0, 0.5, 1.0):
    line(L, yb(v), R, yb(v), AXIS if v == 0 else GRID, 1)
    text(L - 8, yb(v) + 4, "%+.1f" % v if v else "0", 10, MUTED, "end")
text(L - 45, (y0 + y1) / 2 - 6, "offset", 11, INK2, "end")
text(L - 45, (y0 + y1) / 2 + 8, "(s)", 11, INK2, "end")
n = len(stretches)
band = (R - L) / n
for k, (label, a, b) in enumerate(stretches):
    cx = L + band * (k + 0.5)
    if k:
        line(L + band * k, y0, L + band * k, y1, INK2, 1, "4 3")
    for j, (name, color) in enumerate(B_CURVES):
        v = levels[name][k]
        if v is None:
            continue
        circle(cx + (j - 1) * 16, yb(v), 6, color)
    text(cx, y1 + 17, label, 10, INK2, "middle")
    text(cx, y1 + 31, "%.1f-%.1f min" % (a / 60.0, b / 60.0), 10, MUTED, "middle")
line(L, y1, R, y1, AXIS)
text(L + band - 4, y0 + 12, "pause P1", 10, INK2, "end")
note_y = y1 + 60
text(20, note_y, "Each dot: one curve's median offset over the points where flow runs (missing: too few). "
                 "Dashed lines: pauses P1-P%d." % len(pauses), 11, INK2)
text(20, note_y + 17, "UNICORN's own time export uses the grid, and so does PyCORN's Curve.time. "
                      "Volume and CV axes are not affected.", 11, INK2)

H = note_y + 32
svg = ('<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">'
       '<rect width="100%%" height="100%%" fill="%s"/>%s</svg>\n' % (W, H, W, H, SURFACE, "".join(parts)))
with open(OUT, "w", encoding="utf-8", newline="\n") as f:
    f.write(svg)
print("[OK] wrote", OUT, "height", H)

if TABLE:
    head = "| Curve | " + " | ".join(s[0] for s in stretches) + " |"
    sep = "|---|" + "---:|" * len(stretches)
    rows = [head, sep]
    for name, _ in B_CURVES:
        cells = ["-" if v is None else ("0" if abs(v) < 0.005 else "%+.1f" % v) for v in levels[name]]
        rows.append("| %s (%s) | %s |" % (name, rate_text(chrom.curves[name]), " | ".join(cells)))
    with open(TABLE, "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(rows) + "\n")
    print("[OK] wrote", TABLE)
