#!/usr/bin/env python
"""
Draw the cycles-and-chromatograms figure of docs/RUN_STRUCTURE_S88_PLAN.md (section 3.3-3.4).

    python tools/draw_cycles_chromatograms.py [-o docs/images/cycles_chromatograms.svg]

A schematic, not data: one result whose method runs three cycles of the same phases, between
a prime and a final clean. Three rows show how the cycles are found: (A) a new chromatogram at
each cycle boundary, (B) a new chromatogram cut inside a phase (the error case), (C) one
chromatogram, cycles from the caller's rule. Plain SVG, same look as docs/images/logic_*.svg.
"""
import argparse
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parser = argparse.ArgumentParser(description="Draw docs/images/cycles_chromatograms.svg (a schematic).")
parser.add_argument("-o", "--output", default=os.path.join(ROOT, "docs", "images", "cycles_chromatograms.svg"),
                    help="the SVG to write (default: %(default)s)")
OUT = parser.parse_args().output

FONT = "Segoe UI, Helvetica, Arial, sans-serif"
INK, INK2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
BLUE, ORANGE, AQUA, RED = "#2a78d6", "#eb6834", "#1baf7a", "#c8102e"
PHASE_COLORS = {"Equil": "#dbe8f8", "Load": "#fde3d6", "Wash": "#d9f2e8", "Elute": "#c9dcf5", "Clean": "#ecebe5",
                "Prime": "#ecebe5", "Final clean": "#ecebe5"}

# the phases as run: (name, length in arbitrary time units, cycle or None)
PHASES = [("Prime", 3, None)]
for n in (1, 2, 3):
    PHASES += [("Equil", 2, n), ("Load", 4, n), ("Wash", 2, n), ("Elute", 2, n), ("Clean", 2, n)]
PHASES += [("Final clean", 3, None)]

parts = []


def text(x, y, s, size=12, fill=INK2, anchor="start", weight="normal"):
    s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    parts.append('<text x="%.1f" y="%.1f" font-family="%s" font-size="%d" fill="%s" text-anchor="%s" '
                 'font-weight="%s">%s</text>' % (x, y, FONT, size, fill, anchor, weight, s))


def line(x1, y1, x2, y2, stroke=AXIS, width=1, dash=None):
    d = ' stroke-dasharray="%s"' % dash if dash else ""
    parts.append('<line x1="%.2f" y1="%.2f" x2="%.2f" y2="%.2f" stroke="%s" stroke-width="%g"%s/>'
                 % (x1, y1, x2, y2, stroke, width, d))


def rect(x, y, w, h, fill, stroke=SURFACE, width=1.5, rx=3):
    parts.append('<rect x="%.2f" y="%.2f" width="%.2f" height="%.2f" rx="%g" fill="%s" stroke="%s" '
                 'stroke-width="%g"/>' % (x, y, w, h, rx, fill, stroke, width))


W = 1000
L, R = 200, W - 30
total = sum(p[1] for p in PHASES)
xs = lambda t: L + t / float(total) * (R - L)  # noqa: E731

starts, t = [], 0
for name, length, cycle in PHASES:
    starts.append(t)
    t += length
cycle_start = {c: starts[i] for i, (_, _, c) in enumerate(PHASES) if c and PHASES[i][0] == "Equil"}
cycle_end = {}
for i, (_, length, c) in enumerate(PHASES):
    if c:
        cycle_end[c] = starts[i] + length
load2 = next(starts[i] + PHASES[i][1] / 2.0 for i, p in enumerate(PHASES) if p[0] == "Load" and p[2] == 2)

text(20, 30, "One result, three cycles: how cycles and chromatograms relate", 16, INK, weight="600")
text(20, 50, "Schematic. A cycle (unit procedure) starts and ends between UNICORN phases.")
text(20, 65, "A chromatogram is only how the data was recorded.")

# row 0: the phases as run
y = 80
text(L - 14, y + 17, "UNICORN phases", 12, INK, "end", "600")
text(L - 14, y + 31, "(ISA-88 operations)", 10, MUTED, "end")
for (name, length, cycle), s in zip(PHASES, starts):
    rect(xs(s), y, xs(s + length) - xs(s), 26, PHASE_COLORS[name])
    label = name if length >= 2 else ""
    text((xs(s) + xs(s + length)) / 2, y + 17, label, 10, INK, "middle")


def cycles_band(yy, color):
    """The cycles above row yy, with 'before' / 'after' at the ends."""
    for c in (1, 2, 3):
        a, b = xs(cycle_start[c]), xs(cycle_end[c])
        rect(a, yy, b - a, 18, "none", color, 1.5)
        text((a + b) / 2, yy + 13, "cycle %d" % c, 10, color, "middle", "600")
    text(xs(1.5), yy + 13, "before", 10, MUTED, "middle")
    text(xs(total - 1.5), yy + 13, "after", 10, MUTED, "middle")


def note(yy, lines, fill=INK2):
    """Short notes under a row, left-aligned with the plot (kept short so no viewer crops them)."""
    for k, s in enumerate(lines):
        text(L, yy + 14 * k, s, 11, fill)


def chrom_band(yy, cuts, color=INK2):
    edges = [0] + cuts + [total]
    for k in range(len(edges) - 1):
        a, b = xs(edges[k]), xs(edges[k + 1])
        rect(a, yy, b - a, 18, "#f3f2ee", AXIS, 1)
        text((a + b) / 2, yy + 13, "Chrom.%d" % (k + 1), 10, color, "middle")


# row A
y = 140
text(20, y + 10, "A. New chromatogram", 13, INK, weight="600")
text(20, y + 26, "at each cycle boundary", 12, INK2)
cuts_a = [cycle_start[1], cycle_start[2], cycle_start[3], cycle_end[3]]
chrom_band(y + 28, cuts_a)
cycles_band(y, BLUE)
for c in cuts_a:
    line(xs(c), y - 4, xs(c), y + 50, BLUE, 1, "4 3")
note(y + 64, ["Cycles from the chromatograms, when the caller says so.",
              "Every cut falls between phases, so the check passes."])

# row B
y = 265
text(20, y + 10, "B. New chromatogram", 13, INK, weight="600")
text(20, y + 26, "cut inside a phase", 12, INK2)
cuts_b = [cycle_start[1], load2]
chrom_band(y + 28, cuts_b)
line(xs(load2), y + 4, xs(load2), y + 48, RED, 2)
text(xs(load2), y, "cut", 11, RED, "middle", "600")
note(y + 70, ["Load of cycle 2 crosses the cut: using these chromatograms as cycles fails.",
              "They stay data segments; cycles can still come from the caller's rule."], RED)

# row C
y = 390
text(20, y + 10, "C. One chromatogram", 13, INK, weight="600")
text(20, y + 26, "cycles from a rule", 12, INK2)
chrom_band(y + 28, [])
cycles_band(y, AQUA)
for c in (1, 2, 3):
    line(xs(cycle_start[c]), y - 4, xs(cycle_start[c]), y + 50, AQUA, 1, "4 3")
note(y + 64, ["The caller's rule: \"a cycle starts at each Equil\".",
              "A repeat found in the phase order is only a hint."])

# time axis
ax_y = y + 104
line(L, ax_y, R, ax_y, AXIS)
text(L, ax_y + 18, "Time: one clock for the whole result. Each chromatogram's own time starts at 0.", 11, INK2)
note_y = ax_y + 46
text(20, note_y, "Every segment carries its cycle (empty before and after) and its chromatogram,", 11, INK2)
text(20, note_y + 15, "so the views group by cycle however the cycles were recorded.", 11, INK2)

H = note_y + 30
svg = ('<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">'
       '<rect width="100%%" height="100%%" fill="%s"/>%s</svg>\n' % (W, H, W, H, SURFACE, "".join(parts)))
with open(OUT, "w", encoding="utf-8", newline="\n") as f:
    f.write(svg)
print("[OK] wrote", OUT, "height", H)
