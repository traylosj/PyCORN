# -*- coding: utf-8 -*-
"""
Process sections from what actually ran, as recorded in the run (event) log.

Owner decisions (docs/ROADMAP.md, D13-D16, 2026-11-08):

- Only the run log is read; the method is not used. Every `(Completed)` instruction, `Method`
  and `Manual` (and `ManualAtInstrument`, `System`, ...) alike, is an executed command, and it
  holds until the next instruction of the same kind. `Next breakpoint`, skipped blocks and holds
  need no handling: the log shows what ran next.
- `Gradient <target> {%B} <length> {unit}`: length 0 is a step; otherwise %B ramps linearly from
  the value in force to the target over the length given (min/s as time; ml, or CV with the Vc
  in force, as volume pumped by the system pump), then holds. A %B instruction during a ramp
  takes over from the %B reached. `Quaternary gradient a {%} b {%} c {%} d {%} <length> {unit}`
  sets the Q1..Q4 percentages the same way (D15).
- Flow onto the column (D14): system flow, except when the injection valve is at
  'Direct inject' (sample flow) or 'System pump waste' (0, with a warning). While the column
  valve is at by-pass the column gets no flow (as for CV). Both set-points and the valve
  positions are kept per section.
- Feed (D16, D18, D21): sections where the injection valve is at 'Inject' or 'Direct inject'
  get a `sample_use`: 'feed'; 'to_bypass' while the column valve is at by-pass (D18: priming,
  purging, checking the sample's UV/cond/pH; not feed); 'buffer' for a direct inject from the
  sample inlet valve's buffer inlet (D21: sample-pump buffer, not feed). `feed` is
  `sample_use == 'feed'`. The loop volume and the sample flow path volume aren't logged, so
  cutting or shifting the feed is left to the caller (`pycorn.exporters.cadet`).
- Each pause in `chrom.timeline["pauses"]` (including UNICORN's unlogged stops) becomes a
  zero-flow section of its length (D7). Section times are given both as elapsed method time
  (stored time; stops during a pause) and as wall-clock time from method start (elapsed plus
  the pauses before), which is the exported time axis. Instructions at a pause's elapsed time
  take effect after the pause section; the pause keeps the state in force before it.
- A manual `Hold` (dated 'Hold <date>' entry, not 'Hold until ...') keeps the flow running but
  stops %-ramps, and %-instructions issued during it act at the `Continue` that ends it (tested
  against a lab environment: the Conc B curve changes at the Continue). A `Pause` during a Hold resumes
  into the Hold: its `Continue` ends the pause only. A Hold issued during a Pause takes over
  from it, and the next `Continue` ends the Hold.
- A setting with no instruction yet (e.g. flow before the first `System flow`) is taken from its
  curve: the median over that section, flagged `*_source='curve'`, with a warning when the
  section is longer than `INITIAL_GRACE_S`.
- Inlets (owner, 2026-11-11; Decide 22): per line (A, B, the sample inlet) a logged position
  is known; a line with no inlet valve in the result's stored system configuration
  (`pycorn.system_configuration`) is one fixed line, known, shown as the line alone ('B'); a
  line with a valve and no position logged yet takes the valve's first position, labelled
  assumed ('B1 (assumed)'; the sample inlet valve: 'Buffer (assumed)'); a configuration that
  doesn't say gives 'B (not known)' and a warning. `Section.inlet_states` holds label and
  state per line. Quaternary shares are listed as logged; for now the Q lines are taken as
  feeding the A and B pumps (provisional, a warning says so).
- The measured curves are only a check (D6): system/sample flow per section (median, relative
  tolerance), %B against the `conc_b` curve and Q1..Q4 against the `conc_q` curves (absolute
  tolerance). Mismatches are warnings; nothing is corrected.

Instruction texts are matched with `INSTRUCTIONS`; a completed instruction that is neither
matched nor listed in `IGNORED` is reported once, never guessed.
"""
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from .column_volume import BYPASS_POSITIONS, COLUMN_POSITION_PATTERNS
from .curve_roles import curves_with_role
from .event_tables import SPAN_COLUMNS, strip_status
from .model import imported_note, this_result_note
from .system_configuration import FIRST_POSITION, system_configuration
from .units import UnitError, conversion_factor, quantity

COMPLETED = re.compile(r"\(Completed\)")
NUMBER = r"[-+]?\d+(?:[.,]\d+)?"

# kind -> pattern on the status-stripped text
INSTRUCTIONS = [
    ("system_flow", re.compile(r"^System flow\s+(?P<value>%s)\s*\{(?P<unit>[^}]*)\}" % NUMBER)),
    ("sample_flow", re.compile(r"^Sample flow\s+(?P<value>%s)\s*\{(?P<unit>[^}]*)\}" % NUMBER)),
    ("gradient", re.compile(r"^Gradient\s+(?P<target>%s)\s*\{%%B\}\s+(?P<length>%s)\s*\{(?P<unit>[^}]*)\}"
                            % (NUMBER, NUMBER))),
    ("quaternary", re.compile(r"^Quaternary gradient\s+(?P<q1>{n})\s*\{{%\}}\s+(?P<q2>{n})\s*\{{%\}}\s+"
                              r"(?P<q3>{n})\s*\{{%\}}\s+(?P<q4>{n})\s*\{{%\}}\s+(?P<length>{n})\s*\{{(?P<unit>[^}}]*)\}}"
                              .format(n=NUMBER))),
    ("inlet", re.compile(r"^Inlet (?P<line>[A-Z])\s+(?P<position>\S+)\s*$")),
    ("sample_inlet", re.compile(r"^Sample inlet\s+(?P<position>.+?)\s*$")),
    ("injection_valve", re.compile(r"^Injection valve\s+(?P<position>.+?)\s*$")),
    ("outlet_valve", re.compile(r"^Outlet valve\s+(?P<position>.+?)\s*$")),
    ("wavelength", re.compile(r"^Wavelength\s+(?P<values>(?:(?:%s|Off)\s*\{nm\}\s*)+)$" % NUMBER)),
    ("base", re.compile(r"^Base\s+(?P<base>.+?)\s*$")),
    ("injection_mark", re.compile(r"^Injection mark\b")),
    # a manual Hold (dated text, not 'Hold until ...') runs until the next Continue or End
    ("hold", re.compile(r"^Hold\s+\d")),
    # 'Pause Infinite min <date>', 'Pause 5 min <date>': only to tell which Continue ends a Hold
    ("pause", re.compile(r"^Pause\s+(?:Infinite|\d)")),
    ("continue", re.compile(r"^Continue\b")),
    ("end", re.compile(r"^End\s+\d")),
]
COLUMN_POSITION = re.compile(r"^Column position\b")
RUN_START = re.compile(r"^(Method|Manual) Run\b")
BASE = re.compile(r"^Base\s")
VC_IN_BASE = re.compile(r"Vc=\s*(?P<value>%s)\s*\{(?P<unit>[^}]*)\}" % NUMBER)
# completed instructions that change nothing a section carries (alarms, monitors, fractionation,
# messages, ...); pauses come from the timeline, blocks and set marks from the event subtype
IGNORED = re.compile(
    r"^(Alarm|Noise reduction|Watch|Hold|Pause|Continue|End\b|End_Block|Next breakpoint|Message|pH valve|"
    r"Auto zero|Peak fractionation|Stop peak fractionation|Fractionation|Stop fractionation|Last tube|"
    r"Reset tube|Feed tube|Fraction collector|Timer|Loop|End Loop|Set mark|System wash|Pump wash|"
    r"Accumulator wash|Data sample frequency|Batch ID|Method Run|Manual Run|The )")

# injection valve positions (case-insensitive) that change the column flow (D14) or mark feed (D16)
DIRECT_INJECT = "direct inject"
SYSTEM_PUMP_WASTE = "system pump waste"
FEED_POSITIONS = ("inject", "direct inject")
# the sample inlet valve's buffer inlet ('Sample inlet Buffer'); the name differs slightly by system
BUFFER_INLET = re.compile(r"^\s*buff", re.IGNORECASE)

INITIAL_GRACE_S = 30.0  # a curve-filled section longer than this gets a warning
FLOW_TOLERANCE = 0.02  # relative (D6)
FLOW_FLOOR_ML_MIN = 0.01  # absolute tolerance for a 0 set-point
PERCENT_TOLERANCE = 1.0  # %B and %Q, absolute (D6)
CHECK_EDGE_S = 2.0  # curve points this close to a section boundary are not checked (1 s log resolution)
# the Conc Q curves are sampled every 2 s, staggered per valve, and lag the log by about one sample
Q_CHECK_EDGE_S = 6.0
FLOW_CHECK_MIN_S = 30.0  # shorter sections: pump ramps dominate, flow not checked
EPS_S = 1e-6


@dataclass
class Section:
    start_s: float  # wall-clock seconds from method start (elapsed + pauses before)
    end_s: float
    elapsed_start_s: float  # stored (elapsed) method time; a pause has start == end
    elapsed_end_s: float
    paused: bool
    phase: Optional[str]
    block: Optional[str]
    system_flow_ml_min: Optional[float]
    sample_flow_ml_min: Optional[float]
    column_flow_ml_min: Optional[float]  # D14; 0 in a pause and while the column is by-passed
    percent_b: tuple  # (%B at start, slope in %B per wall-clock s); (None, 0.0) if unknown
    percent_q: Optional[tuple]  # ((Q1..Q4 at start), (slopes per s)); None without a quaternary instruction
    injection_valve: Optional[str]
    feed: bool  # D16: sample_use == 'feed'
    sample_use: Optional[str]  # None (valve not at Inject/Direct inject), 'feed', 'to_bypass', 'buffer'
    column_position: Optional[str]
    bypass: bool
    inlets: dict  # {'A': 'A1', 'B': 'B1', 'sample': 'S3'} as last set
    outlet_valve: Optional[str]
    wavelengths_nm: Optional[tuple]  # UV channel n = n-th value; 0 = off
    vc_ml: Optional[float]
    held: bool  # in a manual Hold (flow runs; %-ramps stop)
    sources: dict = field(default_factory=dict)  # setting -> 'run_log' | 'curve' | 'default' | None
    set_marks: list = field(default_factory=list)
    changes: list = field(default_factory=list)  # instruction texts at the section's start
    # per line ('A', 'B', 'sample'): `inlet_state()` (label, position, state, why); empty: not
    # built here, read as a run with no system configuration
    inlet_states: dict = field(default_factory=dict)

    @property
    def duration_s(self):
        return self.end_s - self.start_s

    def percent_b_at(self, wall_s):
        b0, slope = self.percent_b
        return None if b0 is None else b0 + slope * (wall_s - self.start_s)


def _float(text):
    return float(text.replace(",", "."))


def _event_seconds(chrom):
    log = chrom.run_log()  # an imported chromatogram's: the source run's log ('ImportedLogbook')
    if log is None:
        return None, None
    return log, conversion_factor(log.time_unit, "s")


def parse_instruction(text):
    """(kind, match) for a status-stripped run-log text, ('column_position', match), or (None, None)."""
    for kind, pattern in INSTRUCTIONS:
        m = pattern.match(text)
        if m:
            return kind, m
    if COLUMN_POSITION.match(text):
        for pattern in COLUMN_POSITION_PATTERNS:
            m = pattern.match(text + " ")
            if m:
                return "column_position", m
        return "column_position", None
    return None, None


def executed_entries(chrom):
    """
    The executed run-log entries of `chrom` in log order: OrderedDicts with `index`, `elapsed_s`,
    `event_type`, `subtype`, `kind` (see `INSTRUCTIONS`, 'column_position', 'block_start',
    'block_end', 'set_mark' or None), `text` (status stripped) and `match`. Instructions count
    only at their '(Completed)' entry; block and set-mark entries count at any status.
    """
    log, to_s = _event_seconds(chrom)
    out = []
    if log is None:
        return out
    for index, e in enumerate(log.events):
        raw = e.text or ""
        if e.time is None:
            continue
        text = strip_status(raw) or ""
        if e.subtype in ("BlockStart", "BlockEnd"):
            kind = "block_start" if e.subtype == "BlockStart" else "block_end"
            m = None
        elif e.subtype == "SetMark":
            kind, m = "set_mark", re.search(r'"(.*)"', text)
        elif COMPLETED.search(raw) or BASE.match(raw):  # 'Base ...' entries have no status
            kind, m = parse_instruction(text)
        else:
            continue
        out.append(OrderedDict([("index", index), ("elapsed_s", e.time * to_s), ("event_type", e.type),
                                ("subtype", e.subtype), ("kind", kind), ("text", text), ("match", m)]))
    return out


class _Ramp:
    """Linear change of a vector of percentages over a length of time (s) or system-pumped volume (ml)."""

    def __init__(self, start, target, length, kind):
        self.start, self.target, self.length, self.kind, self.done = start, target, length, kind, 0.0

    def value(self):
        if self.start is None:
            return None
        return tuple(a + (b - a) * self.done / self.length for a, b in zip(self.start, self.target))

    def rate(self, system_flow_ml_min):
        """Progress per elapsed second."""
        if self.kind == "time":
            return 1.0
        return max(system_flow_ml_min or 0.0, 0.0) / 60.0

    def slopes(self, rate):
        if self.start is None:
            return tuple(0.0 for _ in self.target)
        return tuple((b - a) / self.length * rate for a, b in zip(self.start, self.target))


class _State:
    def __init__(self, vc_ml):
        self.phase = None
        self.blocks = []
        self.system_flow = None
        self.sample_flow = None
        self.percent_b = None  # tuple of one value
        self.percent_q = None  # tuple of four values
        self.ramp_b = None
        self.ramp_q = None
        self.injection_valve = None
        self.column_position = None
        self.bypass = False
        self.inlets = OrderedDict()
        self.outlet_valve = None
        self.wavelengths = None
        self.vc_ml = vc_ml
        self.held = False  # in a manual Hold: %-ramps stop, %-instructions wait for the Continue
        self.paused = False  # a Pause waiting for its Continue (a Continue then ends the pause, not a Hold)
        self.deferred = []
        self.sources = {"system_flow": None, "sample_flow": None, "percent_b": None, "percent_q": None}


def _length(m, state, warnings, text):
    """(length, 'time'|'volume') of a gradient instruction, or (None, None) if it can't be used."""
    length, unit = _float(m.group("length")), m.group("unit").strip()
    if unit == "CV":
        if state.vc_ml is None:
            warnings.append("no Vc for %r; %%-values during this ramp are unknown" % text)
            return None, None
        return length * state.vc_ml, "volume"
    q = quantity(unit)
    if q == "time":
        return length * conversion_factor(unit, "s"), "time"
    if q == "volume":
        return length * conversion_factor(unit, "ml"), "volume"
    warnings.append("gradient length unit %r not known in %r; %%-values during this ramp are unknown" % (unit, text))
    return None, None


def _flow_ml_min(m, state, warnings, text):
    value, unit = _float(m.group("value")), m.group("unit").strip()
    if quantity(unit) == "flow":
        return value * conversion_factor(unit, "ml/min")
    if quantity(unit) == "cv_rate" and state.vc_ml is not None:
        return value * conversion_factor(unit, "CV/h") * state.vc_ml / 60.0
    warnings.append("flow unit %r not usable in %r; flow taken from the curve" % (unit, text))
    return None


def _name(text, prefix):
    """'Block Wash' -> 'Wash' for prefix 'Block '."""
    return text[len(prefix):].strip() or None


def _apply(state, entry, warnings, unrecognized):
    """Apply one executed entry to `state`. Returns True if it is an instruction that sets something."""
    kind, m, text = entry["kind"], entry["match"], entry["text"]
    if kind == "block_start":
        if text.startswith("Phase "):
            state.phase, state.blocks = _name(text, "Phase "), []
        elif text.startswith("Block "):
            state.blocks.append(_name(text, "Block "))
        return False
    if kind == "block_end":
        if text.startswith("End Phase"):
            state.phase, state.blocks = None, []
        elif text.startswith("End_Block") and state.blocks:
            state.blocks.pop()
        return False
    if kind == "set_mark" or kind == "injection_mark":
        return False
    if kind == "hold":
        state.held, state.paused = True, False  # a Hold during a Pause takes over from it
        return False
    if kind == "pause":
        state.paused = True
        return False
    if kind == "continue" and state.paused:
        state.paused = False  # a Pause during a Hold resumes into the Hold
        return False
    if kind in ("continue", "end"):
        state.paused = False
        state.held, deferred, state.deferred = False, state.deferred, []
        for d in deferred:
            _apply(state, d, warnings, unrecognized)
        return bool(deferred)
    if kind in ("gradient", "quaternary") and state.held:
        state.deferred.append(entry)
        return False
    if kind == "system_flow":
        state.system_flow = _flow_ml_min(m, state, warnings, text)
        state.sources["system_flow"] = "run_log" if state.system_flow is not None else None
    elif kind == "sample_flow":
        state.sample_flow = _flow_ml_min(m, state, warnings, text)
        state.sources["sample_flow"] = "run_log" if state.sample_flow is not None else None
    elif kind in ("gradient", "quaternary"):
        target = ((_float(m.group("target")),) if kind == "gradient"
                  else tuple(_float(m.group(g)) for g in ("q1", "q2", "q3", "q4")))
        attr = "b" if kind == "gradient" else "q"
        ramp = getattr(state, "ramp_" + attr)
        current = ramp.value() if ramp is not None else getattr(state, "percent_" + attr)
        length, how = (0.0, None) if _float(m.group("length")) == 0 else _length(m, state, warnings, text)
        if length == 0:
            setattr(state, "percent_" + attr, target)
            setattr(state, "ramp_" + attr, None)
        elif length is None:
            setattr(state, "percent_" + attr, None)
            setattr(state, "ramp_" + attr, _Ramp(None, target, 1.0, "time"))  # unknown until the next instruction
            getattr(state, "ramp_" + attr).length = float("inf")
        else:
            if current is None:
                warnings.append("%r starts from an unknown %%-value; values during the ramp are unknown" % text)
            setattr(state, "ramp_" + attr, _Ramp(current, target, length, how))
            setattr(state, "percent_" + attr, current)
        state.sources["percent_" + attr] = "run_log"
    elif kind == "inlet":
        state.inlets[m.group("line")] = m.group("position")
    elif kind == "sample_inlet":
        state.inlets["sample"] = m.group("position")
    elif kind == "injection_valve":
        state.injection_valve = m.group("position")
    elif kind == "outlet_valve":
        state.outlet_valve = m.group("position")
    elif kind == "column_position":
        if m is None:
            unrecognized.add(text)
            return False
        state.column_position = m.group("position").strip()
        state.bypass = any(b.match(state.column_position) for b in BYPASS_POSITIONS)
    elif kind == "wavelength":
        state.wavelengths = tuple(0.0 if v == "Off" else _float(v)
                                  for v in re.findall(r"(%s|Off)\s*\{nm\}" % NUMBER, m.group("values")))
    elif kind == "base":
        vc = VC_IN_BASE.search(m.group("base"))
        if vc:
            try:
                state.vc_ml = _float(vc.group("value")) * conversion_factor(vc.group("unit").strip(), "ml")
            except UnitError as e:
                warnings.append("Vc in %r not read (%s)" % (text, e))
        return False
    else:
        if not IGNORED.match(text):
            unrecognized.add(" ".join(text.split()[:3]))
        return False
    return True


def column_flow(system_flow, sample_flow, injection_valve, bypass=False):
    """Flow through the column (ml/min) by injection valve position (D14); 0 while the column is by-passed."""
    position = (injection_valve or "").strip().lower()
    if bypass:
        return 0.0
    if position == DIRECT_INJECT:
        return sample_flow
    if position == SYSTEM_PUMP_WASTE:
        return 0.0
    return system_flow


ASSUMED = " (assumed)"  # a valve's first position, the run log has set none yet
NOT_KNOWN = " (not known)"  # the system configuration doesn't say whether the line has a valve
SAMPLE_PUMP_INLET = "Sample pump inlet"  # the sample pump's one line (no sample inlet valve)
SAMPLE_INLET = "Sample inlet"  # the sample line when its valve is not known
INLET_STATES = ("known", "assumed", "not known")  # `inlet_state()['state']`, best first


def inlet_state(line, logged, config):
    """
    Label and state of inlet `line` ('A', 'B' or 'sample') at a position the run log `logged`
    (None: none yet), with the run's `SystemConfiguration` `config` (None: not read). An
    OrderedDict: 'label' (as the tables show it), 'position' (logged, or the assumed first
    position; None for a fixed line or not known), 'state' ('known', 'assumed', 'not known')
    and 'why'. Logged always wins (owner, 2026-11-11):

    - logged: the position ('B2'), known
    - no valve on the line (configuration): the line alone ('B'; the sample pump's own inlet
      'Sample pump inlet'), known: nothing to log
    - a valve and no position logged yet: its first position (`FIRST_POSITION`: 'B1'; the
      sample inlet valve 'Buffer'), labelled ' (assumed)'
    - configuration not known: 'B (not known)' ('Sample inlet (not known)')
    """
    if logged:
        return OrderedDict([("label", logged), ("position", logged), ("state", "known"), ("why", "logged")])
    role = config.line(line) if config is not None else None
    name = SAMPLE_INLET if line == "sample" else line
    if role is not None and role.present is True:
        first = FIRST_POSITION[line]
        return OrderedDict([("label", first + ASSUMED), ("position", first), ("state", "assumed"),
                            ("why", "%s; no position logged yet: its first position" % role.text)])
    if role is not None and role.present is False:
        return OrderedDict([("label", SAMPLE_PUMP_INLET if line == "sample" else line), ("position", None),
                            ("state", "known"), ("why", role.text)])
    why = role.text if role is not None else "system configuration not read"
    return OrderedDict([("label", name + NOT_KNOWN), ("position", None), ("state", "not known"), ("why", why)])


def _state(section, line):
    states = getattr(section, "inlet_states", None) or {}
    return states.get(line) or inlet_state(line, section.inlets.get(line), None)


def lines_used(section):
    """
    The inlet lines `section` draws from, in order: 'A' while %B is below 100, 'B' while it is
    above 0 (both while %B is unknown), then 'sample' while the injection valve is at Direct
    inject.
    """
    b0, b1 = section.percent_b[0], section.percent_b_at(section.end_s)
    if b0 is None:
        a_used = b_used = True
    else:
        lo, hi = sorted((b0, b1))
        a_used, b_used = lo < 100 - PERCENT_TOLERANCE, hi > PERCENT_TOLERANCE
    used = [line for line, on in (("A", a_used), ("B", b_used)) if on]
    if (section.injection_valve or "").strip().lower() == DIRECT_INJECT:
        used.append("sample")
    return used


def _q_text(section):
    """'Q1 50%+Q2 50%' (constant) or 'Q ramp 100/0/0/0 to 0/100/0/0' from the logged Q1-Q4 shares."""
    if section.percent_q is None or section.percent_q[0] is None:
        return None
    q0, slopes = section.percent_q
    q1 = tuple(v + s * section.duration_s for v, s in zip(q0, slopes))
    if any(abs(a - b) > PERCENT_TOLERANCE for a, b in zip(q0, q1)):
        return "Q ramp %s to %s" % (_pct(q0), _pct(q1))
    parts = ["Q%d %g%%" % (n, round(v, 2)) for n, v in enumerate(q0, 1) if v > PERCENT_TOLERANCE]
    return "+".join(parts) or None


def inlets_text(section, ramp="ramp %s to %s"):
    """
    The inlets `section` draws from, as text ('A1', 'A1+B', 'ramp A1 to B2', 'B1 (assumed), Loop'):
    line A while %B is below 100, line B while it is above 0 (both, 'A1, B1', while %B is unknown),
    each labelled by `inlet_state` (logged position, the line alone when it has no valve, the
    valve's first position '(assumed)', '(not known)'); a constant %B between 0 and 100 joins them
    with '+'; `ramp` (as `percent_b` reads 'ramp 0 to 100'; the page's track: 'A1 -> B1', from to
    to) for a %B ramp, from the line it ramps away from; the logged quaternary shares ('Q1 50%+Q2
    50%'); then 'Loop' while the injection valve is at Inject, the sample inlet at Direct inject.
    None when nothing is known.
    """
    b0, b1 = section.percent_b[0], section.percent_b_at(section.end_s)
    used = lines_used(section)
    lines = [_state(section, line)["label"] for line in used if line != "sample"]
    if len(lines) == 2 and b0 is not None and abs(b1 - b0) > PERCENT_TOLERANCE:
        parts = [ramp % tuple(lines if b1 > b0 else lines[::-1])]
    elif len(lines) == 2 and b0 is not None:
        parts = ["+".join(lines)]  # a constant mix of A and B
    else:
        parts = [", ".join(lines)] if lines else []
    q = _q_text(section)
    if q:
        parts.append(q)
    valve = (section.injection_valve or "").strip().lower()
    if valve == "inject":
        parts.append("Loop")
    elif valve == DIRECT_INJECT:
        parts.append(_state(section, "sample")["label"])
    return ", ".join(parts) or None


def inlets_source(section):
    """
    How sure the inlets of `inlets_text(section)` are: 'known' (each logged, or a line with no
    valve), 'assumed' (a valve's first position, none logged yet) or 'not known' (the system
    configuration doesn't say); the least sure line decides. None when no line is used.
    """
    states = [_state(section, line)["state"] for line in lines_used(section)]
    return max(states, key=INLET_STATES.index) if states else None


QUATERNARY_NOTE = ("quaternary valve: Q1-Q4 shares listed as logged; for now its lines are taken as feeding "
                   "the A and B pumps (provisional, owner 2026-11-11)")


def inlet_warnings(sections):
    """
    'inlets: ...' warnings for `sections`: per line, where it is used at an assumed or not-known
    position (one warning per line and label, first start to last end, wall-clock s; from each
    section's `inlet_states`), and the provisional quaternary reading when Q shares are logged.
    """
    out = OrderedDict()
    for s in sections:
        if s.paused:
            continue
        for line in lines_used(s):
            st = _state(s, line)
            if st["state"] == "known":
                continue
            key = (line, st["label"])
            first = out.get(key, (s.start_s, s.end_s, st, False))
            # owner, 2026-11-11: an assumed sample inlet still counts as feed; say so
            out[key] = (first[0], s.end_s, st, first[3] or (line == "sample" and s.sample_use == "feed"))
    warnings = []
    for (line, label), (t0, t1, st, feed) in out.items():
        what = "sample inlet" if line == "sample" else "line %s" % line
        warnings.append("inlets: %s shown as %r between %.1f and %.1f s: %s" % (what, label, t0, t1, st["why"])
                        + ("; counted as feed there, though what was pumped may be buffer" if feed else ""))
    if any(s.percent_q is not None and not s.paused for s in sections):
        warnings.append("inlets: " + QUATERNARY_NOTE)
    return warnings


def is_buffer_inlet(sample_inlet):
    """True for the sample inlet valve's buffer inlet ('Buffer')."""
    return bool(sample_inlet) and bool(BUFFER_INLET.match(sample_inlet))


def sample_use(injection_valve, bypass=False, sample_inlet=None):
    """
    What the injection valve sends on (D16, D18, D21): None if it is not at 'Inject' or 'Direct
    inject'; 'buffer' for a direct inject from the sample inlet's buffer inlet; 'to_bypass' while
    the column is by-passed; else 'feed'.
    """
    position = (injection_valve or "").strip().lower()
    if position not in FEED_POSITIONS:
        return None
    if position == DIRECT_INJECT and is_buffer_inlet(sample_inlet):
        return "buffer"
    return "to_bypass" if bypass else "feed"


def _segments(entries, end_s, vc_ml, warnings, unrecognized):
    """Constant-state pieces in elapsed time (dicts: start, end, the settings, b/q with slopes, changes)."""
    state = _State(vc_ml)
    segments = []
    pending_changes = []

    def rate(ramp):
        return 0.0 if ramp is None or state.held else ramp.rate(state.system_flow)

    def emit(t0, t1):
        rate_b, rate_q = rate(state.ramp_b), rate(state.ramp_q)
        b = state.ramp_b.value() if state.ramp_b else state.percent_b
        q = state.ramp_q.value() if state.ramp_q else state.percent_q
        b_slope = state.ramp_b.slopes(rate_b) if state.ramp_b else (0.0,)
        q_slope = state.ramp_q.slopes(rate_q) if state.ramp_q else (0.0,) * 4
        segments.append(dict(start=t0, end=t1, b=b, b_slope=b_slope, q=q, q_slope=q_slope,
                             phase=state.phase, block=state.blocks[-1] if state.blocks else None,
                             system_flow=state.system_flow, sample_flow=state.sample_flow,
                             injection_valve=state.injection_valve, column_position=state.column_position,
                             bypass=state.bypass, inlets=dict(state.inlets), outlet_valve=state.outlet_valve,
                             wavelengths=state.wavelengths, vc_ml=state.vc_ml, held=state.held,
                             sources=dict(state.sources), changes=list(pending_changes)))
        pending_changes.clear()

    def advance(t0, t1):
        while t0 < t1 - EPS_S:
            t_end = t1
            for ramp in (state.ramp_b, state.ramp_q):
                r = rate(ramp)
                if r > 0 and np.isfinite(ramp.length):
                    t_end = min(t_end, t0 + (ramp.length - ramp.done) / r)
            t_end = max(t_end, t0)
            emit(t0, t_end)
            for attr in ("b", "q"):
                ramp = getattr(state, "ramp_" + attr)
                if ramp is None:
                    continue
                ramp.done += rate(ramp) * (t_end - t0)
                if ramp.done >= ramp.length - 1e-9:
                    setattr(state, "percent_" + attr, ramp.target)
                    setattr(state, "ramp_" + attr, None)
                    pending_changes.append("end of %s ramp" % ("%B" if attr == "b" else "%Q"))
            t0 = t_end
        return max(t0, t1)

    t = 0.0
    for entry in entries:
        at = min(max(entry["elapsed_s"], 0.0), end_s)
        t = advance(t, at)
        deferred = state.held and entry["kind"] in ("gradient", "quaternary")
        sets = _apply(state, entry, warnings, unrecognized)
        if deferred:
            pending_changes.append(entry["text"] + " (waits for Continue: Hold)")
        elif entry["kind"] in ("hold", "pause", "continue", "end"):
            if sets or entry["kind"] == "hold":
                pending_changes.append(entry["text"])
        elif sets or (entry["kind"] in ("block_start", "block_end") and not RUN_START.match(entry["text"])):
            pending_changes.append(entry["text"])
    advance(t, end_s)
    return segments


STATE_KEYS = ("phase", "block", "system_flow", "sample_flow", "injection_valve", "column_position", "bypass",
              "inlets", "outlet_valve", "wavelengths", "vc_ml", "held", "sources")


def _same(a, b):
    if any(a[k] != b[k] for k in STATE_KEYS):
        return False
    for key in ("b", "q"):
        x, y = a[key], b[key]
        if (x is None) != (y is None):
            return False
        if x is None:
            continue
        slope_a, slope_b = a[key + "_slope"], b[key + "_slope"]
        if any(abs(sa - sb) > 1e-12 for sa, sb in zip(slope_a, slope_b)):
            return False
        expected = [v + s * (a["end"] - a["start"]) for v, s in zip(x, slope_a)]
        if any(abs(e - v) > 1e-6 for e, v in zip(expected, y)):
            return False
    return True


def _merge(segments):
    """Join neighbouring pieces that change nothing (their instructions repeated what was in force)."""
    out = []
    for s in segments:
        if out and _same(out[-1], s):
            out[-1]["end"] = s["end"]
            out[-1]["changes"].extend(s["changes"])
        else:
            out.append(s)
    return out


def _curve_seconds(curve):
    return curve.time.astype(np.float64) * conversion_factor(curve.time_unit, "s")


def _curve_median(curve, unit, t0, t1, edge=0.0):
    """Median of `curve` (converted to `unit`) over elapsed [t0+edge, t1-edge] s, or None."""
    if curve is None or curve.time is None or curve.values is None:
        return None
    t = _curve_seconds(curve)
    sel = (t >= t0 + edge) & (t <= t1 - edge)
    if not sel.any():
        sel = (t >= t0) & (t <= t1)
    if not sel.any():
        return None
    try:
        factor = conversion_factor(curve.unit, unit) if unit else 1.0
    except UnitError:
        return None
    return float(np.median(curve.values[sel].astype(np.float64))) * factor


def _first(chrom, role):
    found = [c for c in curves_with_role(chrom, role) if c.time is not None]
    return found[0] if found else None


def _q_curves(chrom):
    found = [c for c in curves_with_role(chrom, "conc_q") if c.time is not None]
    found.sort(key=lambda c: int(re.sub(r"\D", "", c.name) or 0))
    return found if len(found) == 4 else None


def _fill_from_curves(segments, entries, flow_curve, sample_curve, b_curve, warnings):
    """
    Settings with no instruction in force, from their curves: before the first instruction of a
    kind, one median over [0, first instruction); later gaps (an instruction that couldn't be
    read), the median over each piece. Sample flow with neither instruction nor curve is 0.
    """
    first = {}
    for e in entries:
        first.setdefault(e["kind"], e["elapsed_s"])
    specs = (("system_flow", flow_curve, "ml/min", "system_flow", "system flow"),
             ("sample_flow", sample_curve, "ml/min", "sample_flow", "sample flow"),
             ("b", b_curve, None, "gradient", "%B"))
    for key, curve, unit, kind, label in specs:
        source_key = "percent_b" if key == "b" else key
        initial_end = first.get(kind, float("inf"))
        initial = None
        for s in segments:
            if s[key] is not None or s["sources"][source_key] is not None:
                continue
            if s["start"] < initial_end:
                if initial is None:
                    t1 = min(initial_end, segments[-1]["end"])
                    initial = (_curve_median(curve, unit, 0.0, t1), t1)
                    if initial[0] is None and key == "sample_flow" and curve is None:
                        initial = (0.0, t1)
                value, t0, t1 = initial[0], 0.0, initial[1]
            else:
                value, t0, t1 = _curve_median(curve, unit, s["start"], s["end"]), s["start"], s["end"]
            if value is None:
                continue
            s[key] = (value,) if key == "b" else value
            if key == "b":
                s["b_slope"] = (0.0,)
            s["sources"][source_key] = "default" if curve is None else "curve"
            s["fill"] = s.get("fill", []) + [(label, t0, t1, value, curve is None)]
    reported = set()
    for s in segments:
        for label, t0, t1, value, default in s.pop("fill", []):
            # sample flow only matters where it reaches the column (Direct inject)
            if label == "sample flow" and (s["injection_valve"] or "").strip().lower() != DIRECT_INJECT:
                continue
            if t1 - t0 <= INITIAL_GRACE_S or (label, t0) in reported:
                continue
            reported.add((label, t0))
            unit = "%B" if label == "%B" else "ml/min"
            warnings.append("%s at %.1f-%.1f s elapsed: no instruction in force; %.4g %s %s"
                            % (label, t0, t1, value, unit, "assumed (no curve)" if default else "from the curve"))
    for s in segments:
        for key, label in (("system_flow", "system flow"), ("b", "%B")):
            if s[key] is None and s["end"] - s["start"] > INITIAL_GRACE_S:
                warnings.append("%s at %.1f-%.1f s elapsed: unknown (no instruction, no curve)"
                                % (label, s["start"], s["end"]))


def run_log_sections(run, chrom=None, flow_tolerance=FLOW_TOLERANCE, percent_tolerance=PERCENT_TOLERANCE):
    """
    Sections of `chrom` (default: the run's first chromatogram) from its executed run-log
    entries. Returns an OrderedDict:

    - `sections`: list of `Section`, in time order, pauses included
    - `entries`: the executed entries used (see `executed_entries`; without the regex match)
    - `pauses`: the timeline pauses inserted (elapsed s, duration s, source)
    - `end_elapsed_s`, `end_s`: run end (last run-log entry), elapsed and wall clock
    - `checks`: per section, the measured flow and %-deviation found (D6)
    - `warnings`: everything assumed, unrecognized or not matched
    - `run_log_source`: None for the run's own log; for an imported chromatogram where its run
      log comes from (`Chromatogram.run_log_source()`: the source result's ID and name)

    An imported chromatogram's sections are read from its imported run log ('ImportedLogbook'):
    what ran in the source result. Its inlet labels use this result's system configuration; for
    one imported from another result (`Run.holds_run_of` False, `run` given) a warning says so.
    """
    chrom = chrom or run.chromatogram()
    warnings = []
    config = getattr(chrom, "system_configuration", None)
    if config is None and run is not None:
        config = system_configuration(run)
    log, to_s = _event_seconds(chrom)
    source = chrom.run_log_source()
    result = OrderedDict([("sections", []), ("entries", []), ("pauses", []), ("end_elapsed_s", None),
                          ("end_s", None), ("checks", []), ("warnings", warnings), ("run_log_source", source)])
    if log is None or not log.events:
        warnings.append("no run log; no sections")
        return result
    if source is not None:
        warnings.append(imported_note(source))
        if run is not None and config is not None and not run.holds_run_of(chrom):
            warnings.append(this_result_note(run, "the system configuration (inlet labels)"))
    entries = executed_entries(chrom)
    end_elapsed = max(e.time for e in log.events if e.time is not None) * to_s
    info = chrom.column_volume_info or {}
    vc_ml = None
    if info.get("value") is not None:
        vc_ml = info["value"] * conversion_factor(info["unit"], "ml")
    unrecognized = set()
    segments = _segments(entries, end_elapsed, vc_ml, warnings, unrecognized)
    for text in sorted(unrecognized):
        warnings.append("run-log instruction not recognized, ignored for sections: %r" % text)

    flow_curve, sample_curve = _first(chrom, "flow"), _first(chrom, "sample_flow")
    b_curve, q_curves = _first(chrom, "conc_b"), _q_curves(chrom)
    _fill_from_curves(segments, entries, flow_curve, sample_curve, b_curve, warnings)
    segments = _merge(segments)

    pauses = sorted((p for p in (chrom.timeline or {}).get("pauses") or []
                     if p["elapsed_s"] <= end_elapsed + EPS_S), key=lambda p: p["elapsed_s"])
    if chrom.timeline is None:
        warnings.append("no timeline; pauses not inserted")
    result["pauses"] = [OrderedDict([("elapsed_s", p["elapsed_s"]), ("duration_s", p["duration_s"]),
                                     ("source", p["source"])]) for p in pauses]

    sections = []
    offset = 0.0
    queue = list(pauses)
    zero_flow = {"system pump waste": False}

    def make(seg, t0, t1, pause=None):
        """A Section from piece `seg` over elapsed [t0, t1], or a pause of `pause` s at t0."""
        b0 = seg["b"][0] if seg["b"] is not None else None
        b_slope = seg["b_slope"][0] if b0 is not None else 0.0
        q0, q_slope = seg["q"], tuple(seg["q_slope"])
        if b0 is not None:
            b0 += b_slope * (t0 - seg["start"])
        if q0 is not None:
            q0 = tuple(v + sl * (t0 - seg["start"]) for v, sl in zip(q0, q_slope))
        paused = pause is not None
        if paused:
            b_slope, q_slope = 0.0, (0.0,) * 4
        valve = (seg["injection_valve"] or "").strip().lower()
        if valve == SYSTEM_PUMP_WASTE and not paused:
            zero_flow["system pump waste"] = True
        start = t0 + offset
        use = sample_use(seg["injection_valve"], seg["bypass"], seg["inlets"].get("sample"))
        return Section(start_s=start, end_s=start + (pause if paused else t1 - t0), elapsed_start_s=t0,
                       elapsed_end_s=t0 if paused else t1, paused=paused, phase=seg["phase"], block=seg["block"],
                       system_flow_ml_min=0.0 if paused else seg["system_flow"],
                       sample_flow_ml_min=0.0 if paused else seg["sample_flow"],
                       column_flow_ml_min=0.0 if paused else column_flow(seg["system_flow"], seg["sample_flow"],
                                                                         seg["injection_valve"], seg["bypass"]),
                       percent_b=(b0, b_slope), percent_q=(q0, q_slope) if q0 is not None else None,
                       injection_valve=seg["injection_valve"], feed=use == "feed", sample_use=use,
                       column_position=seg["column_position"], bypass=seg["bypass"], inlets=seg["inlets"],
                       outlet_valve=seg["outlet_valve"], wavelengths_nm=seg["wavelengths"], vc_ml=seg["vc_ml"],
                       held=seg["held"], sources=dict(seg["sources"], paused="timeline") if paused else dict(seg["sources"]),
                       changes=[] if paused else list(seg["changes"]),
                       inlet_states=OrderedDict((line, inlet_state(line, seg["inlets"].get(line), config))
                                                for line in FIRST_POSITION))

    def add_pause(seg, at, p):
        nonlocal offset
        sections.append(make(seg, at, at, pause=p["duration_s"]))
        sections[-1].changes = ["pause (%s, %.1f s)" % (p["source"], p["duration_s"])]
        offset += p["duration_s"]

    previous = None
    for seg in segments:
        t0 = seg["start"]
        # a pause at a piece's start keeps the state before it (instructions there act after the pause)
        while queue and queue[0]["elapsed_s"] <= t0 + EPS_S:
            add_pause(previous if previous is not None else seg, t0, queue.pop(0))
        piece_changes = seg["changes"]
        while queue and queue[0]["elapsed_s"] < seg["end"] - EPS_S:
            p = queue.pop(0)
            if p["elapsed_s"] > t0 + EPS_S:
                sections.append(make(seg, t0, p["elapsed_s"]))
                sections[-1].changes, piece_changes = piece_changes, []
            add_pause(seg, p["elapsed_s"], p)
            t0 = p["elapsed_s"]
        sections.append(make(seg, t0, seg["end"]))
        sections[-1].changes = piece_changes
        previous = seg
    for p in queue:  # pauses at the run end
        add_pause(segments[-1], end_elapsed, p)
    if zero_flow["system pump waste"]:
        warnings.append("injection valve at 'System pump waste': column flow taken as 0 there (D14)")
    for p in pauses:
        warnings.append("pause of %.1f s at %.1f s elapsed (%s) inserted as a zero-flow section"
                        % (p["duration_s"], p["elapsed_s"], p["source"]))

    for k, e in enumerate(entries):
        if e["kind"] == "set_mark":
            sec = _section_at(sections, e["elapsed_s"])
            if sec is not None:
                sec.set_marks.append(e["match"].group(1) if e["match"] else e["text"])
    warnings.extend(inlet_warnings(sections))
    result["sections"] = sections
    result["entries"] = [OrderedDict((k, v) for k, v in e.items() if k != "match") for e in entries]
    result["end_elapsed_s"], result["end_s"] = end_elapsed, end_elapsed + offset
    result["checks"] = _checks(sections, flow_curve, sample_curve, b_curve, q_curves, flow_tolerance,
                               percent_tolerance, warnings)
    return result


def _section_at(sections, elapsed_s):
    """The non-paused section holding elapsed time `elapsed_s` (the later one at a boundary)."""
    found = None
    for s in sections:
        if not s.paused and s.elapsed_start_s - EPS_S <= elapsed_s <= s.elapsed_end_s + EPS_S:
            found = s
    return found


def _checks(sections, flow_curve, sample_curve, b_curve, q_curves, flow_tolerance, percent_tolerance, warnings):
    checks = []
    b_t = _curve_seconds(b_curve) if b_curve is not None else None
    q_t = [_curve_seconds(c) for c in q_curves] if q_curves else None
    for k, s in enumerate(sections):
        row = OrderedDict([("section", k), ("start_s", s.start_s), ("end_s", s.end_s)])
        checks.append(row)
        if s.paused:
            continue
        t0, t1 = s.elapsed_start_s, s.elapsed_end_s
        label = "section %d (%.1f-%.1f s, %s)" % (k, s.start_s, s.end_s, s.block or s.phase or "no block")
        if t1 - t0 >= FLOW_CHECK_MIN_S:
            edge = min(15.0, 0.25 * (t1 - t0))
            for key, curve, setpoint in (("system_flow", flow_curve, s.system_flow_ml_min),
                                         ("sample_flow", sample_curve, s.sample_flow_ml_min)):
                if curve is None or setpoint is None or s.sources.get(key) != "run_log":
                    continue
                measured = _curve_median(curve, "ml/min", t0, t1, edge)
                if measured is None:
                    continue
                row[key + "_measured"] = measured
                limit = max(flow_tolerance * abs(setpoint), FLOW_FLOOR_ML_MIN)
                if abs(measured - setpoint) > limit:
                    warnings.append("%s: %s set to %.4g ml/min, curve median %.4g ml/min"
                                    % (label, key.replace("_", " "), setpoint, measured))
        if b_t is not None and s.percent_b[0] is not None:
            sel = (b_t >= t0 + CHECK_EDGE_S) & (b_t <= t1 - CHECK_EDGE_S)
            if sel.any():
                # slope is per wall-clock s; within a non-paused section wall and elapsed run together
                expected = s.percent_b[0] + s.percent_b[1] * (b_t[sel] - t0)
                dev = float(np.max(np.abs(b_curve.values[sel].astype(np.float64) - expected)))
                row["percent_b_max_deviation"] = dev
                if dev > percent_tolerance:
                    warnings.append("%s: %%B differs from the %s curve by up to %.2f %%B"
                                    % (label, b_curve.name, dev))
        if q_t is not None and s.percent_q is not None:
            devs = []
            for n, (curve, t) in enumerate(zip(q_curves, q_t)):
                sel = (t >= t0 + Q_CHECK_EDGE_S) & (t <= t1 - Q_CHECK_EDGE_S)
                if sel.any():
                    expected = s.percent_q[0][n] + s.percent_q[1][n] * (t[sel] - t0)
                    devs.append(float(np.max(np.abs(curve.values[sel].astype(np.float64) - expected))))
            if devs:
                row["percent_q_max_deviation"] = max(devs)
                if max(devs) > percent_tolerance:
                    warnings.append("%s: Q1-Q4 differ from the Conc Q curves by up to %.2f %%"
                                    % (label, max(devs)))
    return checks


def to_frame(result):
    """The sections of a `run_log_sections()` result as a pandas DataFrame (one row per section)."""
    import pandas as pd
    rows = []
    for s in result["sections"]:
        rows.append(OrderedDict([
            ("start_s", s.start_s), ("end_s", s.end_s), ("elapsed_start_s", s.elapsed_start_s),
            ("elapsed_end_s", s.elapsed_end_s), ("paused", s.paused), ("phase", s.phase), ("block", s.block),
            ("column_flow_ml_min", s.column_flow_ml_min), ("system_flow_ml_min", s.system_flow_ml_min),
            ("sample_flow_ml_min", s.sample_flow_ml_min), ("percent_b", s.percent_b[0]),
            ("percent_b_slope_per_s", s.percent_b[1]),
            ("percent_q", s.percent_q[0] if s.percent_q else None),
            ("injection_valve", s.injection_valve), ("feed", s.feed), ("sample_use", s.sample_use), ("column_position", s.column_position),
            ("bypass", s.bypass), ("inlets", inlets_text(s)), ("inlets_source", inlets_source(s)),
            ("outlet_valve", s.outlet_valve),
            ("wavelengths_nm", s.wavelengths_nm), ("set_marks", s.set_marks), ("changes", s.changes)]))
    return pd.DataFrame(rows)


# what ran in a phase / injection / fraction row (`event_tables.SPAN_COLUMNS`), from the sections
# as the CADET export reads them (D13-D18, D21); each setting lists its values in time order, a
# repeat of the one before left out
THEN = " -> "


def _sequence(values):
    out = []
    for v in values:
        if v is not None and v != "" and (not out or out[-1] != v):
            out.append(v)
    return THEN.join(out) if out else None


def _pct(values):
    return "/".join("%g" % round(v, 2) for v in values)


def _program(pieces):
    """'0 -> ramp 0 to 50 -> 100' from (start, end) pieces; a ramp split by other settings is one ramp."""
    items = []
    for start, end in pieces:
        if _pct(start) == _pct(end):
            items.append(("hold", start, end))
        elif items and items[-1][0] == "ramp" and _pct(items[-1][2]) == _pct(start):
            items[-1] = ("ramp", items[-1][1], end)
        else:
            items.append(("ramp", start, end))
    return _sequence(_pct(a) if kind == "hold" else "ramp %s to %s" % (_pct(a), _pct(b)) for kind, a, b in items)


def span_details(result, start_s, end_s, closed=False):
    """
    What ran between elapsed method seconds `start_s` and `end_s` (a phase, injection or fraction
    row), from a `run_log_sections()` result: block names, system and column flow (D14), %B and
    %Q programs (D13, D15), inlets, column position, injection valve, sample use (D16, D18, D21)
    and the set marks logged in the span (`closed`: one at `end_s` counts too). Pause sections
    give no values (D7). An OrderedDict keyed by `SPAN_COLUMNS`; text, None when nothing ran.
    """
    inside = [s for s in result["sections"] if not s.paused
              and s.elapsed_end_s > start_s + EPS_S and s.elapsed_start_s < end_s - EPS_S]

    def ends(s, value, slope):
        """`value` (at the section's start, per wall-clock s `slope`) where the span cuts the section."""
        t0 = max(0.0, start_s - s.elapsed_start_s)
        t1 = s.duration_s - max(0.0, s.elapsed_end_s - end_s)
        return value + slope * t0, value + slope * t1

    b = [ends(s, s.percent_b[0], s.percent_b[1]) for s in inside if s.percent_b[0] is not None]
    q = []
    for s in inside:
        if s.percent_q is not None:
            pairs = [ends(s, v, sl) for v, sl in zip(*s.percent_q)]
            q.append((tuple(p[0] for p in pairs), tuple(p[1] for p in pairs)))
    marks = []
    for e in result["entries"]:
        at = e["elapsed_s"]
        if e["kind"] == "set_mark" and start_s - EPS_S <= at and (at < end_s - EPS_S
                                                                  or closed and at <= end_s + EPS_S):
            m = re.search(r'"(.*)"', e["text"])
            marks.append(m.group(1) if m else e["text"])

    def flows(values):
        return _sequence("%g" % v for v in values if v is not None)

    return OrderedDict([
        ("blocks", _sequence(s.block for s in inside)),
        ("system_flow_ml_min", flows(s.system_flow_ml_min for s in inside)),
        ("column_flow_ml_min", flows(s.column_flow_ml_min for s in inside)),
        ("percent_b", _program([((x,), (y,)) for x, y in b])),
        ("percent_q", _program(q)),
        ("inlets", _sequence(inlets_text(s) for s in inside)),
        ("inlets_source", _sequence(inlets_source(s) for s in inside)),
        ("column_position", _sequence(s.column_position for s in inside)),
        ("injection_valve", _sequence(s.injection_valve for s in inside)),
        ("sample_use", _sequence(s.sample_use for s in inside)),
        ("set_marks", "; ".join(m for m in marks if m) or None)])
