#!/usr/bin/env python
"""
Draw the transient-states figure of docs/RUN_STRUCTURE_RECORD.md (section 3.7).

    python tools/draw_transient_states.py [-o docs/images/transient_states.svg]

A synthetic schematic drawn by Claude (an AI assistant), not data: the flow setpoint and the
actual flow (as the volume axis would show it) across Running, a Pause (Holding, Held,
Restarting) and Running again, then a Hold (Pausing, Paused) where the flow keeps running and
%B freezes. Placeholder numbers only. Plain SVG, same look as docs/images/logic_*.svg.
"""
import argparse
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parser = argparse.ArgumentParser(description="Draw docs/images/transient_states.svg (a schematic).")
parser.add_argument("-o", "--output", default=os.path.join(ROOT, "docs", "images", "transient_states.svg"),
                    help="the SVG to write (default: %(default)s)")
OUT = parser.parse_args().output

FONT = "Segoe UI, Helvetica, Arial, sans-serif"
INK, INK2, MUTED = "#0b0b0b", "#52514e", "#898781"
GRID, AXIS, SURFACE = "#e1e0d9", "#c3c2b7", "#fcfcfb"
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
STATE_FILL = {"Running": "#ecebe5", "Holding": "#fde3d6", "Held": "#f6c9b3", "Restarting": "#fde3d6",
              "Pausing": "#fbe9b0", "Paused": "#f5d77e"}

# (state, start, end) in arbitrary time units; Pausing is instantaneous for the flow, drawn narrow
STATES = [("Running", 0, 20), ("Holding", 20, 24), ("Held", 24, 40), ("Restarting", 40, 45),
          ("Running", 45, 60), ("Pausing", 60, 61), ("Paused", 61, 78), ("Running", 78, 90)]
T_END = 90
SET = 1.0  # setpoint (placeholder units)
# actual flow: piecewise linear points (t, flow)
ACTUAL = [(0, SET), (20, SET), (24, 0.0), (40, 0.0), (45, SET), (90, SET)]
SETPOINT = [(0, SET), (90, SET)]
# %B: ramps in Running, frozen while Held (clock stopped) and Paused
PCTB = [(0, 0.10), (20, 0.30), (40, 0.30), (60, 0.50), (78, 0.50), (90, 0.62)]

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


def polyline(points, stroke, width=2, dash=None):
    d = ' stroke-dasharray="%s"' % dash if dash else ""
    pts = " ".join("%.2f,%.2f" % p for p in points)
    parts.append('<polyline points="%s" fill="none" stroke="%s" stroke-width="%g"%s/>' % (pts, stroke, width, d))


W = 1000
L, R = 170, W - 40
xs = lambda t: L + t / float(T_END) * (R - L)  # noqa: E731

text(20, 30, "Flow ramps and ISA-88 states around a Pause and a Hold", 16, INK, weight="600")
text(20, 50, "Synthetic schematic (drawn by Claude), not data. The setpoint never changes in a transient;")
text(20, 65, "the actual flow is what the volume axis would show, fitted as stop, linear ramp, set flow.")

# state band
y = 90
text(L - 14, y + 17, "ISA-88 state", 12, INK, "end", "600")
text(L - 14, y + 31, "(UNICORN: Pause, Hold)", 10, MUTED, "end")
for name, a, b in STATES:
    rect(xs(a), y, xs(b) - xs(a), 26, STATE_FILL[name])
    if b - a >= 4:
        text((xs(a) + xs(b)) / 2, y + 17, name, 10, INK, "middle")
text(xs(22), y + 44, "Pause issued", 10, ORANGE, "middle")
text(xs(42.5), y + 44, "Continue", 10, ORANGE, "middle")
text(xs(60.5), y + 44, "Hold issued", 10, INK2, "middle")
text(xs(78), y + 44, "Continue", 10, INK2, "middle")

# flow panel
top, bot = 160, 280
fy = lambda v: bot - v / 1.2 * (bot - top)  # noqa: E731
text(L - 14, (top + bot) / 2, "Flow", 12, INK, "end", "600")
line(L, bot, R, bot, AXIS)
for _, a, _ in STATES[1:]:
    line(xs(a), top - 4, xs(a), bot, GRID, 1, "3 3")
polyline([(xs(t), fy(v)) for t, v in ACTUAL], ORANGE, 2.5)
polyline([(xs(t), fy(v)) for t, v in SETPOINT], BLUE, 2, "6 4")
text(xs(52), fy(SET) - 8, "setpoint (run log)", 10, BLUE, "middle")
text(xs(32), fy(0) - 8, "actual (volume axis): 0 while Held", 10, ORANGE, "middle")
text(xs(21.5), fy(0.55), "ramp down", 10, ORANGE, "end")
text(xs(43.5), fy(0.55), "ramp up", 10, ORANGE, "start")

# %B panel
top2, bot2 = 310, 400
by = lambda v: bot2 - v * (bot2 - top2)  # noqa: E731
text(L - 14, (top2 + bot2) / 2, "%B", 12, INK, "end", "600")
line(L, bot2, R, bot2, AXIS)
for _, a, _ in STATES[1:]:
    line(xs(a), top2 - 4, xs(a), bot2, GRID, 1, "3 3")
polyline([(xs(t), by(v)) for t, v in PCTB], AQUA, 2.5)
text(xs(32), by(0.30) - 8, "frozen (clock stopped)", 10, AQUA, "middle")
text(xs(69.5), by(0.50) - 8, "frozen (Hold)", 10, AQUA, "middle")

ax_y = bot2 + 24
text(L, ax_y, "Time (wall clock). Holding, Restarting: lengths inferred from the volume axis; "
              "UNICORN logs only when Pause and Continue were issued.", 11, INK2)
text(L, ax_y + 16, "Paused (a UNICORN Hold): the flow keeps running; only %B freezes. "
                   "Placeholder values throughout.", 11, INK2)

H = ax_y + 34
svg = ('<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">'
       '<rect width="100%%" height="100%%" fill="%s"/>%s</svg>\n' % (W, H, W, H, SURFACE, "".join(parts)))
with open(OUT, "w", encoding="utf-8", newline="\n") as f:
    f.write(svg)
print("[OK] wrote", OUT, "height", H)
