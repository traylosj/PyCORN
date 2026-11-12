# -*- coding: utf-8 -*-
"""
`CadetExperiment`: a run as a neutral, all-SI description for CADET and CADET-Process
(docs/ROADMAP.md, "CADET export"). No CADET import; the CADET-Process objects and the
CADET-Core input file are built from this in later phases.

- Time axis (D5, D7): wall-clock seconds from method start (elapsed time plus the pauses before
  it), so pauses are zero-flow sections and the signals stay aligned with them. With
  `time_zero='injection'` every time is shifted so the first injection is at 0.
- Sections come from `pycorn.run_log_sections` (what ran, D13-D16). Flow is the flow through the
  column in m^3/s as CADET polynomial coefficients (constant, slope per s).
- Feed (D16, D18-D21): sections where the injection valve is at 'Inject' or 'Direct inject',
  labelled by `sample_use` (see `pycorn.run_log_sections.sample_use`).
  - D18: while the column is by-passed the sample goes to the by-pass ('to_bypass'), not feed.
    Every by-pass window, with sample or buffer (owner, rev 7e), is listed in
    `CadetExperiment.bypass` and marks UV and conductivity as not column outlet
    (`Signal.not_column_outlet`). Buffer-only windows give buffer conductivity and pH,
    checked against the column outlet (D22, `buffer_readings.py`).
  - D19: a loop injection ('Inject') ends after the caller's `loop_volume` of system flow
    through the injection valve, by-pass included; buffer follows. Without it the whole
    'Inject' period counts as feed, with a warning. (D20: a Superloop's feed is the valve
    period, so no loop volume is given for it.)
  - D21: a direct inject delivers what the sample pump drew one sample flow path volume
    earlier. With the caller's `sample_path_volume` the feed runs on that far after a switch
    to the sample inlet's buffer (Finalize sample injection) and starts that much later after
    a buffer wash. Without it the feed is what the sample inlet is set to: it ends at the
    switch, with a warning. A direct inject from the buffer inlet only is not feed.
- Column (D10, D11, D26): geometry from the column type the run links (`column_geometry`),
  usually None, and the caller's values as a fallback per value (or an override). Vc from the run's resolved column volume. Porosities only from the caller.
- Signals (D2, D8): every UV channel above 0 nm in AU/cm, the 280 nm channel primary (or the
  caller's wavelength); concentration only with a caller-given extinction coefficient.
  Conductivity in S/m, %B, and the measured column flow.
- Fractions (D4): times and volumes from the fraction table; masses only from the caller.
- Hints (D3): per section, inlet lines, %B, set marks and block/phase names. Report only.
  Inlets (owner, 2026-11-11): each section's `inlet_states` and the hints' `inlet_labels` /
  `inlet_sources` follow `run_log_sections.inlet_state` (logged and fixed lines known, a
  valve's first position assumed, else not known); `system['inlet_configuration']` is the
  run's installed valves and pumps (`pycorn.system_configuration`).
- Method hints (D17, D20): `pycorn.method_hints` (planned injections and block lengths
  against what ran, sample-application dialog, BufferPro, variables, documentation) in
  `CadetExperiment.method_hints`, its warnings prefixed 'method: '. They change no section.
"""
import base64
import binascii
import bisect
import csv
import os
import re
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from typing import Optional

import numpy as np

from ...column_geometry import column_geometry
from ...concentration import ACCURACY_NOTE
from ...curve_roles import curves_with_role
from ...method_hints import method_hints as read_method_hints
from ...model import this_result_note
from ...run_log_sections import DIRECT_INJECT, is_buffer_inlet, run_log_sections
from ...system_configuration import system_configuration
from ...timeline import _shift
from ...units import UnitError, conversion_factor, to_si
from .buffer_readings import buffer_readings
from .calibration import uv_au_per_cm, uv_concentration

TIME_ZEROS = ("method_start", "injection")
LOOP_INJECT = "inject"  # injection valve position that empties the loop onto the column
UV_CHANNEL_NAME = re.compile(r"(?P<channel>\d+)\D+(?P<nm>\d+(?:\.\d+)?)\s*$")  # 'UV 1_280'
ML_MIN_TO_M3_S = conversion_factor("ml/min", "m^3/s")
MIXER_ITEM = re.compile(r'Name="Volume"\s+Value="([^"]+)"\s+Unit="([^"]+)"')


@dataclass
class ExperimentSection:
    start_s: float
    end_s: float
    name: Optional[str]  # block, else phase
    phase: Optional[str]
    block: Optional[str]
    flow_m3_s: tuple  # (constant, slope per s) through the column
    percent_b: tuple  # (%B at start, slope per s); (None, 0.0) if unknown
    percent_q: Optional[tuple]  # ((Q1..Q4 at start), (slopes per s)) or None
    feed: bool
    feed_source: Optional[str]  # 'valve', 'loop_volume' (cut short), 'sample_path_volume' (shifted) or None
    sample_use: Optional[str]  # None, 'feed', 'to_bypass', 'buffer' (D18, D21)
    injection_valve: Optional[str]
    inlets: dict
    bypass: bool
    paused: bool
    held: bool
    wavelengths_nm: Optional[tuple]
    system_flow_m3_s: float  # set-points; 0 in a pause or when unknown
    sample_flow_m3_s: float
    sample_at_valve: Optional[str] = None  # direct inject: the sample inlet whose liquid reaches the valve (D21)
    feed_line: Optional[str] = None  # D27: 'A' or 'B' when the feed comes through a system pump line (mixer)
    # per line ('A', 'B', 'sample'): label, position, state (known / assumed / not known), why
    # (`pycorn.run_log_sections.inlet_state`, from the system configuration)
    inlet_states: dict = field(default_factory=dict)


@dataclass
class Signal:
    name: str
    role: str
    time_s: np.ndarray
    values: np.ndarray  # as stored
    unit: Optional[str]  # as stored
    export_values: Optional[np.ndarray]  # UV: AU/cm (AU without a path); others: SI where there is one
    export_unit: Optional[str]
    uv_path_length_cm: Optional[float] = None
    uv_path_basis: Optional[str] = None
    wavelength_nm: Optional[float] = None
    wavelength_source: Optional[str] = None
    channel: Optional[int] = None
    primary: bool = False
    concentration: Optional[dict] = None  # 'kg/m^3', 'mol/m^3', 'conversion'
    not_column_outlet: list = field(default_factory=list)  # (start_s, end_s): column by-passed (D18)


@dataclass
class Fraction:
    tube: Optional[str]
    start_s: Optional[float]
    end_s: Optional[float]
    volume_m3: Optional[float]
    mass_kg: Optional[dict] = None  # component -> kg, from the caller (D4)


@dataclass
class CadetExperiment:
    source: dict
    time_zero: dict
    column: dict
    sections: list
    events: list
    signals: list
    flow: Optional[dict]
    fractions: list
    components: dict
    hints: list
    warnings: list = field(default_factory=list)
    bypass: list = field(default_factory=list)  # column by-pass windows: start_s, end_s, sample (D18)
    buffer_readings: list = field(default_factory=list)  # buffer conductivity/pH at by-pass (D22)
    method_hints: object = None  # pycorn.method_hints.MethodHints (D17, D20)
    system: dict = field(default_factory=dict)  # D27: mixer and tubing volumes (m^3), sources, delay hints

    def signal(self, name):
        for s in self.signals:
            if s.name == name:
                return s
        raise KeyError("no signal %r; signals: %s" % (name, ", ".join(s.name for s in self.signals)))

    @property
    def primary_uv(self):
        return next((s for s in self.signals if s.primary), None)


def _version():
    """PyCORN's version and git commit (`pycorn.version`), so a CADET input can be traced."""
    from ...version import version_text
    return version_text()


def _wall_s(chrom, elapsed_s):
    tl = chrom.timeline or {"pauses": []}
    t = np.asarray(elapsed_s, dtype=np.float64)
    return t + _shift(tl, t)


def _curve_time_s(curve):
    return curve.time.astype(np.float64) * conversion_factor(curve.time_unit, "s")


def _valve(s):
    return (s.injection_valve or "").strip().lower()


def _periods(sections, keep):
    """Runs of neighbouring sections for which `keep(section)` holds."""
    out, run = [], []
    for s in sections:
        if keep(s):
            run.append(s)
        elif run:
            out.append(run)
            run = []
    if run:
        out.append(run)
    return out


def _split(s, t):
    """`s` cut at wall-clock time `t`: (head, tail), %B and %Q carried on to the tail."""
    dt = t - s.start_s
    b0, slope = s.percent_b
    q = s.percent_q
    tail = replace(s, start_s=t, percent_b=(None if b0 is None else b0 + slope * dt, slope),
                   percent_q=None if q is None else (tuple(v + sl * dt for v, sl in zip(*q)), q[1]))
    return replace(s, end_s=t), tail


def _set_use(s, use, source=None):
    s.sample_use, s.feed = use, use == "feed"
    s.feed_source = (source or "valve") if s.feed else None


def _cut_feed(sections, loop_volume, warnings):
    """
    D19: a loop injection ('Inject' period) uses up `loop_volume` (ml) by the system flow through
    the injection valve, by-pass sections included (D18); buffer follows.
    """
    out = []
    periods = {id(p[0]): p for p in _periods(sections, lambda s: _valve(s) == LOOP_INJECT)}
    k = 0
    while k < len(sections):
        period = periods.get(id(sections[k]))
        if period is None:
            out.append(sections[k])
            k += 1
            continue
        k += len(period)
        if loop_volume is None:
            if any(s.feed for s in period):
                warnings.append("feed from %.1f s to %.1f s: injection valve at Inject; no loop volume given, so the "
                                "whole period counts as feed (D19)" % (period[0].start_s, period[-1].end_s))
            out.extend(period)
            continue
        left = loop_volume * conversion_factor("ml", "m^3")
        for s in period:
            q = s.system_flow_m3_s
            if left <= 0:
                _set_use(s, "buffer")
                out.append(s)
                continue
            delivered = q * (s.end_s - s.start_s)
            if delivered <= left or q <= 0:
                left -= delivered
                out.append(s)
                continue
            head, tail = _split(s, s.start_s + left / q)
            if head.feed:
                head.feed_source = "loop_volume"
            _set_use(tail, "buffer")
            out.extend([head, tail])
            left = 0.0
        if left > 0:
            warnings.append("loop volume not used up in the Inject period ending at %.1f s (%.4g ml left)"
                            % (period[-1].end_s, left * conversion_factor("m^3", "ml")))
    return out


VALVE_FEED = ("inject", DIRECT_INJECT)
MARK_NEAR_S = 6.0  # an injection mark this close to a valve switch belongs to it
MARK_INLET_S = 60.0  # D27: an A/B inlet switched this close to a mark (no valve switch) is the sample line
SYSTEM_LINES = ("A", "B")


def _line_share_used(s, line):
    """Whether system pump line `line` ('A' or 'B') delivers anything in section `s`."""
    b0, slope = s.percent_b
    b0 = 0.0 if b0 is None else b0
    b1 = b0 + slope * (s.end_s - s.start_s)
    return min(b0, b1) < 100 if line == "A" else max(b0, b1) > 0


def _injection_marks_s(chrom):
    ec = chrom.event_curves.get("Injection")
    if ec is None or not ec.events:
        return []
    to_s = conversion_factor(ec.time_unit or "min", "s")
    t = np.asarray([e.time for e in ec.events if e.time is not None], dtype=np.float64) * to_s
    return sorted(_wall_s(chrom, t).tolist())


def _set_line_feed(s, line, source):
    s.sample_use, s.feed, s.feed_source, s.feed_line = "feed", True, source, line


def _no_pump_note(config):
    """D27 (4) with the system configuration: the words added when the system has no sample pump."""
    if config is None or config.present("sample_pump") is not False:
        return ""
    return ("; the system has no sample pump (system configuration), so sample reaches the column through the loop "
            "or an A/B line (inferred)")


def _line_feed(sections, chrom, sample_inlets, warnings, config=None):
    """
    D27 (4): feed loaded through a system pump line (A or B), so it passes the mixer. With the
    caller's `sample_inlets` (inlet positions such as 'A5'): every section that draws from one of
    them. Otherwise from each injection mark with no injection-valve switch within MARK_NEAR_S:
    only an A or B inlet **switched** within MARK_INLET_S of the mark is the sample line (owner:
    the inlet in force since method start says nothing; tested against a lab environment, most
    such marks had no switch near them); feed runs from that switch while the position stays selected. No switch
    near the mark: no feed, a warning. Sample loaded through a line with no mark is not seen
    (known limitation). The run's system configuration (`config`) only adds to the warnings: a
    system with no sample pump says so (sample through an A/B line, inferred); it never makes
    a feed window (owner, 2026-11-11).
    """
    note = _no_pump_note(config)
    if sample_inlets:
        wanted = {str(p) for p in sample_inlets}
        for s in sections:
            if s.feed or s.paused or s.bypass:
                continue
            line = next((ln for ln in SYSTEM_LINES if s.inlets.get(ln) in wanted and _line_share_used(s, ln)), None)
            if line is not None:
                _set_line_feed(s, line, "sample_inlets")
        if not any(s.feed_line for s in sections):
            warnings.append("sample_inlets %s: no section draws from them" % ", ".join(sorted(wanted)))
        return sections
    valve_starts = [p[0].start_s for p in _periods(sections, lambda s: _valve(s) in VALVE_FEED)]
    # inlet switches: (time, line, new position, index of the first section on it)
    switches = [(s.start_s, ln, s.inlets.get(ln), k) for k, (prev, s) in enumerate(zip(sections, sections[1:]), 1)
                for ln in SYSTEM_LINES if s.inlets.get(ln) is not None and s.inlets.get(ln) != prev.inlets.get(ln)]
    for t in _injection_marks_s(chrom):
        if any(abs(t - v) <= MARK_NEAR_S for v in valve_starts):
            continue
        near = [sw for sw in switches if abs(sw[0] - t) <= MARK_INLET_S]
        if not near:
            warnings.append("injection mark at %.1f s with no injection-valve switch and no inlet switch within %g s: "
                            "sample route unknown, no feed there (D27); pass sample_inlets= to name the sample inlets%s"
                            % (t, MARK_INLET_S, note))
            continue
        start, line, position, k = min(near, key=lambda sw: abs(sw[0] - t))
        end = start
        while k < len(sections) and sections[k].inlets.get(line) == position:
            s = sections[k]
            if s.percent_q is not None and not s.paused:
                warnings.append("feed through system pump line %s at %.1f s during quaternary mixing: not modelled "
                                "there" % (line, s.start_s))
            elif not (s.paused or s.feed or s.bypass) and _line_share_used(s, line):
                _set_line_feed(s, line, "injection_mark")
            end = s.end_s
            k += 1
        if not any(s.feed_line == line and start <= s.start_s < end for s in sections):
            warnings.append("injection mark at %.1f s: inlet %s switched to %s at %.1f s, but line %s delivers nothing "
                            "there (%%B); no feed" % (t, line, position, start, line))
            continue
        warnings.append("injection mark at %.1f s with no injection-valve switch: inlet %s switched to %s at %.1f s; "
                        "feed through system pump line %s and the mixer from %.1f s to %.1f s (D27); pass "
                        "sample_inlets= to name the sample inlets%s" % (t, line, position, start, line, start, end, note))
    return sections


def _decode_mixer(value):
    """Mixer volumes (value, unit) from a `MixerInformation` text: base64 XML `<Items><Item Name="Volume" ...>`."""
    text = str(value or "").strip()
    try:
        text = base64.b64decode(text + "=" * (-len(text) % 4)).decode("utf-8", "replace")
    except (ValueError, binascii.Error):
        pass
    return [(float(v), u) for v, u in MIXER_ITEM.findall(text)]


def _find_key(x, key):
    if isinstance(x, dict):
        for k, v in x.items():
            if k == key:
                yield v
            else:
                yield from _find_key(v, key)
    elif isinstance(x, list):
        for v in x:
            yield from _find_key(v, key)


def _delay_volume_hints(run):
    """The stored 'Delay volume ...' settings as text, e.g. 'Delay volume: Monitor to frac: Volume 435 ul'."""
    hints = []
    for group in _find_key((run.metadata or {}).get("SystemSettingData"), "Instructions"):
        for instr in group if isinstance(group, list) else [group]:
            name = instr.get("Instruction") if isinstance(instr, dict) else None
            if not name or "delay" not in str(name).lower():
                continue
            for params in _find_key(instr, "Parameter"):
                for p in params if isinstance(params, list) else [params]:
                    if "volume" in str(p).lower():
                        hints.append("%s: %s" % (name, re.sub(r"\s+", " ", str(p)).replace("µ", "u")))
    return hints


def _system_volumes(run, mixer_volume, tubing_volume, warnings):
    """D27: the mixer (stored, caller wins) and post-column tubing (caller only) volumes in m^3."""
    stored = []
    for v in _find_key((run.metadata or {}).get("SystemData"), "MixerInformation"):
        stored += _decode_mixer(v)
    stored_m3 = None
    for value, unit in stored:
        try:
            stored_m3 = value * conversion_factor(unit, "m^3")
            break
        except UnitError:
            continue
    if mixer_volume is not None:
        mixer_m3, source = mixer_volume[0] * conversion_factor(mixer_volume[1], "m^3"), "caller"
        if stored_m3 is not None:
            warnings.append("mixer: given %g %s used over the stored %g ml" % (mixer_volume[0], mixer_volume[1],
                                                                              stored_m3 * 1e6))
    elif stored_m3 is not None:
        mixer_m3, source = stored_m3, "SystemData MixerInformation"
    else:
        mixer_m3, source = None, None
        warnings.append("no mixer volume stored or given: no mixer CSTR (pass mixer_volume=)")
    if tubing_volume is not None:
        tubing_m3, tubing_source = tubing_volume[0] * conversion_factor(tubing_volume[1], "m^3"), "caller"
    else:
        tubing_m3, tubing_source = None, None
        warnings.append("column-to-UV tubing volume: the instrument doesn't store it, so no tubing CSTR after the "
                        "column (pass tubing_volume=, D27)")
    for v in (mixer_m3, tubing_m3):
        if v is not None and v <= 0:
            raise ValueError("system volumes must be above 0")
    return OrderedDict([("mixer_m3", mixer_m3), ("mixer_source", source), ("mixer_stored_m3", stored_m3),
                        ("tubing_m3", tubing_m3), ("tubing_source", tubing_source),
                        ("delay_volume_hints", _delay_volume_hints(run))])


def _pumped(sections):
    """
    The sample pump's history: cumulative volume drawn (m^3) at each section's start, and where
    each sample inlet's liquid starts in that volume (two lists: volumes, inlets).
    """
    starts, volumes, inlets, v = [], [], [], 0.0
    for s in sections:
        starts.append(v)
        q = s.sample_flow_m3_s
        if s.paused or q <= 0:
            continue
        inlet = s.inlets.get("sample")
        if not inlets or inlets[-1] != inlet:
            volumes.append(v)
            inlets.append(inlet)
        v += q * (s.end_s - s.start_s)
    return starts, volumes, inlets


def _drawn_at(volumes, inlets, v):
    """The sample inlet the pump drew from at cumulative volume `v` (the first one before any pumping)."""
    if not inlets:
        return None
    return inlets[max(bisect.bisect_right(volumes, v) - 1, 0)]


def _use_for(s, inlet):
    if is_buffer_inlet(inlet):
        return "buffer"
    return "to_bypass" if s.bypass else "feed"


def _sample_path(sections, path_ml, warnings):
    """
    D21: label direct-inject sections by the liquid that reaches the injection valve. With a
    sample flow path volume (ml) that is what the sample pump drew that volume earlier (sections
    split where it changes); without one it is the sample inlet in force.
    """
    starts, volumes, inlets = _pumped(sections)
    path = None if path_ml is None else path_ml * conversion_factor("ml", "m^3")
    out, unknown = [], False
    for s, v0 in zip(sections, starts):
        if _valve(s) != DIRECT_INJECT:
            out.append(s)
            continue
        now = s.inlets.get("sample")
        if path is None:
            s.sample_at_valve = now
            out.append(s)
            continue
        q = 0.0 if s.paused else s.sample_flow_m3_s
        u0 = v0 - path
        u1 = u0 + q * (s.end_s - s.start_s)
        unknown = unknown or (u0 < 0 and bool(inlets))
        pieces, rest = [], s
        for c in (c for c in volumes[1:] if u0 < c < u1):  # before the first, the first inlet
            head, rest = _split(rest, s.start_s + (c - u0) / q)
            pieces.append((head, u0 + q * (head.start_s - s.start_s)))
        pieces.append((rest, u0 + q * (rest.start_s - s.start_s)))
        for piece, u in pieces:
            inlet = _drawn_at(volumes, inlets, u)
            piece.sample_at_valve = inlet
            _set_use(piece, _use_for(piece, inlet), "valve" if inlet == now else "sample_path_volume")
            out.append(piece)
    if unknown:
        warnings.append("sample flow path content before the first sample pump flow is unknown; taken as the "
                        "first sample inlet pumped (%s) (D21)" % inlets[0])

    for period in _periods(out, lambda s: _valve(s) == DIRECT_INJECT):
        uses = [s.sample_use for s in period if not s.paused]
        if uses and all(u == "buffer" for u in uses):
            warnings.append("%.1f-%.1f s: direct inject from the sample inlet's buffer only; sample-pump buffer, "
                            "not feed (D21)" % (period[0].start_s, period[-1].end_s))
            continue
        if path is not None:
            continue
        live = [s for s in period if not s.paused]
        for a, b in zip(live, live[1:]):
            if a.sample_use != "buffer" and b.sample_use == "buffer":
                warnings.append("%.1f s: sample inlet switched to buffer during a direct inject; no "
                                "sample_path_volume given, so the feed ends at the switch and up to one sample "
                                "flow path volume of sample is missing (D21)" % b.start_s)
        first = next((s for s in live if s.sample_use != "buffer"), None)
        if first is not None:
            v0 = next(v for s, v in zip(sections, starts) if s is first)
            k =bisect.bisect_right(volumes, v0) - 1
            if k > 0 and volumes[k] == v0 and inlets[k - 1] != inlets[k]:
                warnings.append("%.1f s: the sample flow path held %s from earlier pumping, so the first path volume "
                                "onto the column is not %s; no sample_path_volume given, so the feed starts at the "
                                "valve switch (D21)" % (first.start_s, inlets[k - 1], inlets[k]))
    return out


def uv_wavelengths(chrom, sections, curves=None):
    """
    The D8 wavelength rule, shared by `build_experiment` and `pycorn export`: ([(curve, channel, nm,
    nm_source)], warnings) for `curves` (names), else every UV curve with data. `sections` are the
    `run_log_sections(...)['sections']`. Per channel: the run log's last Wavelength instruction
    before the first feed (else the last in the run); the curve name suffix only as a fallback,
    with a warning. Channel from 'UV <n>' in the name, else the stored order. Channels at 0 nm
    (off) are skipped with a note unless `curves` names them. `nm_source` is 'run_log',
    'curve_name' or None (unknown).
    """
    warnings = []
    if curves is not None:
        found = [chrom.curve(name) for name in curves]
    else:
        found = [c for c in curves_with_role(chrom, "uv") if c.time is not None and c.values is not None]
    settings = [s.wavelengths_nm for s in sections if s.wavelengths_nm is not None]
    feed_secs = [s for s in sections if s.feed and s.wavelengths_nm is not None]
    in_force = feed_secs[0].wavelengths_nm if feed_secs else (settings[-1] if settings else None)
    if len(set(settings)) > 1:
        warnings.append("UV wavelengths change during the run: %s; used %s"
                        % (", ".join(str(w) for w in dict.fromkeys(settings)), in_force))
    out = []
    for order, curve in enumerate(found):
        m = UV_CHANNEL_NAME.search(curve.name)
        channel = int(m.group("channel")) if m else order + 1
        if not m:
            warnings.append("%s: UV channel not in the name; taken as channel %d by stored order" % (curve.name, channel))
        if in_force is not None and channel <= len(in_force):
            nm, nm_source = in_force[channel - 1], "run_log"
        elif m:
            nm, nm_source = float(m.group("nm")), "curve_name"
            warnings.append("%s: no Wavelength instruction for channel %d; %g nm from the curve name"
                            % (curve.name, channel, nm))
        else:
            nm, nm_source = None, None
            warnings.append("%s: wavelength unknown" % curve.name)
        if nm == 0 and curves is None:
            warnings.append("%s: channel off (0 nm), skipped" % curve.name)
            continue
        out.append((curve, channel, nm, nm_source))
    return out, warnings


def _uv_signals(chrom, sections_result, uv_curves, primary_wavelength_nm, uv_path_length_cm, warnings,
                fallback_cm=None, fallback_basis="fallback"):
    channels, wl_warnings = uv_wavelengths(chrom, sections_result["sections"], uv_curves)
    warnings.extend(wl_warnings)
    signals = []
    for curve, channel, nm, nm_source in channels:
        values, unit, path, basis, warning = uv_au_per_cm(curve, uv_path_length_cm, fallback_cm, fallback_basis)
        if warning:
            warnings.append(warning)
        signals.append(Signal(name=curve.name, role="uv", time_s=_wall_s(chrom, _curve_time_s(curve)),
                              values=curve.values, unit=curve.unit, export_values=values, export_unit=unit,
                              uv_path_length_cm=path, uv_path_basis=basis, wavelength_nm=nm,
                              wavelength_source=nm_source, channel=channel))
    return _pick_primary(signals, primary_wavelength_nm, warnings)


def _pick_primary(signals, primary_wavelength_nm, warnings):
    """Mark the primary UV signal: the one at the primary wavelength, else the lowest channel (D8)."""
    if signals:
        primary = [s for s in signals if s.wavelength_nm == primary_wavelength_nm]
        if primary:
            primary[0].primary = True
        else:
            first = min(signals, key=lambda s: s.channel)
            first.primary = True
            warnings.append("no %g nm UV channel; %s (%s nm) is primary"
                            % (primary_wavelength_nm, first.name, first.wavelength_nm))
    return signals


def _other_signals(chrom):
    out = []
    for role in ("conductivity", "conc_b"):
        for curve in curves_with_role(chrom, role):
            if curve.time is None or curve.values is None:
                continue
            try:
                export, unit, _ = to_si(curve.values, curve.unit)
            except UnitError:
                export, unit = curve.values.astype(np.float64), curve.unit
            out.append(Signal(name=curve.name, role=role, time_s=_wall_s(chrom, _curve_time_s(curve)),
                              values=curve.values, unit=curve.unit, export_values=export, export_unit=unit))
    return out


def read_fraction_masses(path_or_rows):
    """
    Caller fraction assays (D4) from a CSV path or a list of dicts with `tube`, `component` and
    `mass_mg` or `conc_g_per_l`: {tube: {component: ('mass_kg', value) | ('conc_kg_m3', value)}}.
    """
    if isinstance(path_or_rows, (str, os.PathLike)):
        with open(path_or_rows, newline="", encoding="utf-8-sig") as f:  # Excel saves CSV with a BOM
            rows = list(csv.DictReader(f))
    else:
        rows = list(path_or_rows)
    out = OrderedDict()
    for row in rows:
        tube, component = str(row["tube"]).strip(), str(row["component"]).strip()
        if row.get("mass_mg") not in (None, ""):
            value = ("mass_kg", float(row["mass_mg"]) * 1e-6)
        elif row.get("conc_g_per_l") not in (None, ""):
            value = ("conc_kg_m3", float(row["conc_g_per_l"]))
        else:
            raise ValueError("fraction row for tube %r, component %r has no mass_mg or conc_g_per_l" % (tube, component))
        out.setdefault(tube, OrderedDict())[component] = value
    return out


def _fractions(chrom, masses, warnings):
    out = []
    used = set()
    for row in chrom.table_rows("fraction"):
        to_s = conversion_factor(row["time_unit"], "s") if row["time_unit"] else None
        to_m3 = conversion_factor(row["volume_unit"], "m^3") if row["volume_unit"] else None
        start = float(_wall_s(chrom, row["start_time"] * to_s)) if row["start_time"] is not None and to_s else None
        end = float(_wall_s(chrom, row["end_time"] * to_s)) if row["end_time"] is not None and to_s else None
        volume = None
        if to_m3 and row["start_volume"] is not None and row["end_volume"] is not None:
            volume = (row["end_volume"] - row["start_volume"]) * to_m3
        tube = row["name"]
        mass = None
        if masses and tube in masses:
            used.add(tube)
            mass = OrderedDict()
            for component, (kind, value) in masses[tube].items():
                if kind == "mass_kg":
                    mass[component] = value
                elif volume is not None:
                    mass[component] = value * volume
                else:
                    warnings.append("fraction %s: no volume, %s concentration not converted to mass" % (tube, component))
        out.append(Fraction(tube=tube, start_s=start, end_s=end, volume_m3=volume, mass_kg=mass))
    for tube in (masses or {}):
        if tube not in used:
            warnings.append("fraction assay for tube %r matches no fraction in the run" % tube)
    return out


def _events(chrom, sections):
    events = []
    for row in chrom.table_rows("injection"):
        if not row["pseudo"] and row["start_time"] is not None:
            t = row["start_time"] * conversion_factor(row["time_unit"], "s")
            events.append(OrderedDict([("time_s", float(_wall_s(chrom, t))), ("kind", "injection"),
                                       ("text", row["name"])]))
    for s in sections:
        for change in s.changes:
            kind = ("pause" if change.startswith("pause (") else "block_start" if change.startswith(("Block ", "Phase "))
                    else "block_end" if change.startswith(("End_Block", "End Phase")) else
                    "gradient_end" if change.startswith("end of ") else
                    "gradient" if change.startswith(("Gradient", "Quaternary")) else "instruction")
            events.append(OrderedDict([("time_s", s.start_s), ("kind", kind), ("text", change)]))
    events.sort(key=lambda e: e["time_s"])
    return events


def _hints(sections):
    hints = []
    for k, s in enumerate(sections):
        hints.append(OrderedDict([("section", k), ("start_s", s.start_s), ("end_s", s.end_s), ("phase", s.phase),
                                  ("block", s.block), ("inlets", dict(s.inlets)),
                                  ("inlet_labels", OrderedDict((k, v["label"]) for k, v in s.inlet_states.items())),
                                  ("inlet_sources", OrderedDict((k, v["state"]) for k, v in s.inlet_states.items())),
                                  ("percent_b", s.percent_b[0]),
                                  ("percent_q", s.percent_q[0] if s.percent_q else None),
                                  ("set_marks", list(s.set_marks))]))
    return hints


def build_experiment(run, chrom=None, time_zero="method_start", components=None, uv_curves=None,
                     primary_wavelength_nm=280.0, uv_path_length_cm=None, uv_path_length_fallback_cm=None,
                     uv_path_length_fallback_basis="fallback", epsilon=None, epsilon_unit="ml/(mg*cm)",
                     epsilon_molar=None, epsilon_molar_unit="1/(M*cm)", molecular_weight=None,
                     molecular_weight_unit="g/mol", epsilon_wavelength_nm=None, absorbance_limit_mau=None,
                     loop_volume=None, sample_path_volume=None, buffer_salt=None, fraction_masses=None, column_name=None, bed_height=None, diameter=None,
                     particle_diameter=None, geometry_override=False, geometry_fallback_sources=None, porosities=None,
                     mixer_volume=None, tubing_volume=None, sample_inlets=None, flow_tolerance=None,
                     percent_tolerance=None, response_factor=None):
    """
    `CadetExperiment` for `chrom` (default: the run's first chromatogram).

    - `time_zero`: 'method_start' (D5) or 'injection'.
    - `components`: whatever the caller knows (names, buffer A/B or Q-line concentrations in
      mol/m^3, ...); stored as given.
    - `uv_curves`: curve names to export instead of every live UV channel.
    - `uv_path_length_cm`: overrides the UV path of every plain absorbance curve.
      `uv_path_length_fallback_cm`: used only where the run gives no path (never over a stored
      path, never on `*/cm` curves), recorded as `uv_path_length_fallback_basis`, one warning per
      curve filled.
    - `epsilon` (mass) or `epsilon_molar` + `molecular_weight`: concentration of the UV channel
      at `epsilon_wavelength_nm` (default: the primary channel). `response_factor` (caller only,
      no default) scales the coefficient for this detector's response
      (`pycorn.concentration.concentration`). A concentration always adds `ACCURACY_NOTE` to
      the warnings (GitHub issue #1).
    - `loop_volume`: (value, unit) of a fixed sample loop, to end loop-injection feed (D19).
    - `sample_path_volume`: (value, unit) of the sample flow path from the sample inlet valve to
      the injection valve, to shift direct-inject feed by what the path holds (D21).
    - `buffer_salt`: {inlet position ('A1') or line ('A', 'B'): salt mol/m^3}, to give the
      by-pass buffer readings a salt value (D22, `salt_calibration_points`).
    - `fraction_masses`: CSV path or rows (see `read_fraction_masses`).
    - `column_name`, `bed_height`, `diameter`, `particle_diameter`: passed to `column_geometry`
      (D26: a fallback per value where the run stores none; `geometry_override=True` makes them win;
      `geometry_fallback_sources` labels the filled values, e.g. {'length': 'fallback (--bed-height)'});
      `porosities`: dict of caller values (e.g. {'column': 0.37, 'particle': 0.75}).
    - `mixer_volume`, `tubing_volume`: (value, unit) of the mixer (default: the stored mixer size) and of
      the column -> UV tubing (caller only); CSTRs before and after the column (D27).
    - `sample_inlets`: inlet positions (e.g. ['A5']) that held sample pumped through a system pump
      line; default: found from injection marks with no injection-valve switch (D27).
    """
    if time_zero not in TIME_ZEROS:
        raise ValueError("time_zero must be one of %s, not %r" % (", ".join(TIME_ZEROS), time_zero))
    chrom = chrom or run.chromatogram()
    kwargs = {}
    if flow_tolerance is not None:
        kwargs["flow_tolerance"] = flow_tolerance
    if percent_tolerance is not None:
        kwargs["percent_tolerance"] = percent_tolerance
    rls = run_log_sections(run, chrom, **kwargs)
    # W17: an imported chromatogram's sections are its source run's (its 'ImportedLogbook'); this
    # result's own column type and system settings are used for it, with a warning (owner)
    own = run.holds_run_of(chrom)
    if not rls["sections"]:
        # no run log (e.g. a derived chromatogram) or no executed entries: nothing ran to build from (D13)
        raise ValueError("%s: no run-log sections, so no CADET experiment (%s)"
                         % (chrom.name, "; ".join(rls["warnings"]) or "no executed run-log entries"))
    warnings = ["sections: %s" % w for w in rls["warnings"]]

    sections = [ExperimentSection(
        start_s=s.start_s, end_s=s.end_s, name=s.block or s.phase, phase=s.phase, block=s.block,
        flow_m3_s=((s.column_flow_ml_min or 0.0) * ML_MIN_TO_M3_S, 0.0), percent_b=s.percent_b,
        percent_q=s.percent_q, feed=s.feed, feed_source="valve" if s.feed else None, sample_use=s.sample_use,
        injection_valve=s.injection_valve, inlets=dict(s.inlets), bypass=s.bypass, paused=s.paused, held=s.held,
        wavelengths_nm=s.wavelengths_nm, system_flow_m3_s=(s.system_flow_ml_min or 0.0) * ML_MIN_TO_M3_S,
        sample_flow_m3_s=(s.sample_flow_ml_min or 0.0) * ML_MIN_TO_M3_S,
        inlet_states=OrderedDict((k, dict(v)) for k, v in s.inlet_states.items())) for s in rls["sections"]]
    for s in rls["sections"]:
        if s.column_flow_ml_min is None:
            warnings.append("section %.1f-%.1f s: column flow unknown; exported as 0" % (s.start_s, s.end_s))
    loop_ml = None
    if loop_volume is not None:
        loop_ml = loop_volume[0] * conversion_factor(loop_volume[1], "ml")
    sections = _cut_feed(sections, loop_ml, warnings)
    path_ml = None
    if sample_path_volume is not None:
        path_ml = sample_path_volume[0] * conversion_factor(sample_path_volume[1], "ml")
    sections = _sample_path(sections, path_ml, warnings)
    config = getattr(chrom, "system_configuration", None) or system_configuration(run)
    sections = _line_feed(sections, chrom, sample_inlets, warnings, config)
    system = _system_volumes(run, mixer_volume, tubing_volume, warnings)
    if not own:
        if system["mixer_source"] == "SystemData MixerInformation":
            warnings.append(this_result_note(run, "the mixer volume"))
        if system["delay_volume_hints"]:
            warnings.append(this_result_note(run, "the delay volume hints"))
    system["inlet_configuration"] = config.summary()  # installed valves and pumps, for the inlet labels
    for p in _periods(sections, lambda s: s.sample_use == "to_bypass"):
        warnings.append("%.1f-%.1f s: sample sent to the column by-pass, not feed; UV and conductivity there are "
                        "not column outlet (D18)" % (p[0].start_s, p[-1].end_s))
    bypass = [OrderedDict([("start_s", p[0].start_s), ("end_s", p[-1].end_s),
                           ("sample", any(s.sample_use == "to_bypass" for s in p))])
              for p in _periods(sections, lambda s: s.bypass)]
    buffer_only = [w for w in bypass if not w["sample"]]
    if buffer_only:
        warnings.append("%d column by-pass window(s) without sample (%s): UV and conductivity there are not column "
                        "outlet (D18)" % (len(buffer_only), ", ".join("%.1f-%.1f s" % (w["start_s"], w["end_s"])
                                                                      for w in buffer_only)))

    signals = _uv_signals(chrom, rls, uv_curves, primary_wavelength_nm, uv_path_length_cm, warnings,
                          uv_path_length_fallback_cm, uv_path_length_fallback_basis)
    if epsilon is not None or epsilon_molar is not None:
        nm = epsilon_wavelength_nm if epsilon_wavelength_nm is not None else None
        target = (next((s for s in signals if s.wavelength_nm == nm), None) if nm is not None
                  else next((s for s in signals if s.primary), None))
        if target is None:
            warnings.append("no UV channel at %s nm for the extinction coefficient; no concentration" % nm)
        elif target.uv_path_length_cm is None:
            warnings.append("%s: no path length; no concentration" % target.name)
        else:
            path_cm, path_basis = uv_path_length_cm, "caller"
            if uv_path_length_cm is None and target.uv_path_basis == uv_path_length_fallback_basis:
                path_cm, path_basis = target.uv_path_length_cm, uv_path_length_fallback_basis
            target.concentration = uv_concentration(
                chrom.curve(target.name), epsilon=epsilon, epsilon_unit=epsilon_unit, epsilon_molar=epsilon_molar,
                epsilon_molar_unit=epsilon_molar_unit, molecular_weight=molecular_weight,
                molecular_weight_unit=molecular_weight_unit, path_length_cm=path_cm,
                absorbance_limit=absorbance_limit_mau, path_length_basis=path_basis, response_factor=response_factor)
            warnings.append("%s: %s" % (target.name, ACCURACY_NOTE))
    if absorbance_limit_mau is not None:
        for s in signals:
            if s.role == "uv" and np.nanmax(s.values) > absorbance_limit_mau * conversion_factor("mAU", s.unit or "mAU"):
                warnings.append("%s exceeds the absorbance limit %g mAU" % (s.name, absorbance_limit_mau))
    signals += _other_signals(chrom)
    for sig in signals:
        if sig.role in ("uv", "conductivity"):
            sig.not_column_outlet = [(w["start_s"], w["end_s"]) for w in bypass]
    readings, reading_warnings = buffer_readings(chrom, sections, bypass, _wall_s, buffer_salt)
    warnings += reading_warnings
    hints = read_method_hints(run, chrom, rls["sections"])
    warnings += ["method: %s" % w for w in hints.warnings if hints.available]

    flow = None
    flow_curves = [c for c in curves_with_role(chrom, "flow") if c.time is not None]
    if flow_curves:
        c = flow_curves[0]
        flow = OrderedDict([("name", c.name), ("time_s", _wall_s(chrom, _curve_time_s(c))),
                            ("m3_s", to_si(c.values, c.unit)[0])])

    geometry = column_geometry(run, name=column_name, bed_height=bed_height, diameter=diameter,
                               particle_diameter=particle_diameter, override=geometry_override,
                               fallback_sources=geometry_fallback_sources)
    warnings += ["column: %s" % w for w in geometry.warnings]
    if not own and "ColumnTypeData" in geometry.sources.values():
        warnings.append(this_result_note(run, "the column type (%s)" % geometry.name))
    info = chrom.column_volume_info or {}
    vc_m3 = info["value"] * conversion_factor(info["unit"], "m^3") if info.get("value") is not None else None
    column = OrderedDict([("name", geometry.name), ("length_m", geometry.length_m),
                          ("diameter_m", geometry.diameter_m),
                          ("cross_section_area_m2", geometry.cross_section_area_m2),
                          ("particle_radius_m", geometry.particle_radius_m),
                          ("geometric_volume_m3", geometry.volume_m3), ("vc_m3", vc_m3),
                          ("vc_source", info.get("source")), ("sources", dict(geometry.sources)),
                          ("vc_check", geometry.vc_check), ("porosities", dict(porosities or {}))])

    masses = read_fraction_masses(fraction_masses) if fraction_masses is not None else None
    fractions = _fractions(chrom, masses, warnings)
    events = _events(chrom, rls["sections"])

    section_hints = _hints(rls["sections"])
    injections = [e["time_s"] for e in events if e["kind"] == "injection"]
    offset = 0.0
    if injections:
        tz = OrderedDict([("kind", time_zero), ("injection_offset_s", injections[0])])
    else:
        tz = OrderedDict([("kind", time_zero), ("injection_offset_s", None)])
        if time_zero == "injection":
            raise ValueError("time_zero='injection' but the run has no injection")
    if time_zero == "injection":
        offset = injections[0]
        for s in sections:
            s.start_s -= offset
            s.end_s -= offset
        for sig in signals:
            sig.time_s = sig.time_s - offset
            sig.not_column_outlet = [(a - offset, b - offset) for a, b in sig.not_column_outlet]
        if flow:
            flow["time_s"] = flow["time_s"] - offset
        for f in fractions:
            f.start_s = None if f.start_s is None else f.start_s - offset
            f.end_s = None if f.end_s is None else f.end_s - offset
        for e in events:
            e["time_s"] -= offset
        for w in bypass:
            w["start_s"] -= offset
            w["end_s"] -= offset
        for r in readings:
            for row in (r, r["outlet"]):
                for key in ("start_s", "end_s", "read_start_s", "read_end_s"):
                    if row is not None:
                        row[key] -= offset
        for row in hints.injections + hints.blocks + section_hints:
            for key in ("start_s", "end_s"):
                if row[key] is not None:
                    row[key] -= offset

    source = OrderedDict([("run", run.name), ("source", run.source), ("chromatogram", chrom.name),
                          ("unicorn_version", run.unicorn_version), ("pycorn_version", _version()),
                          ("end_s", rls["end_s"] - offset), ("checks", rls["checks"])])
    if rls.get("run_log_source") is not None:  # W17: the run log is the source result's (not on the original)
        source["run_log_source"] = OrderedDict(rls["run_log_source"])
    return CadetExperiment(source=source, time_zero=tz, column=column, sections=sections, events=events,
                           signals=signals, flow=flow, fractions=fractions, components=dict(components or {}),
                           hints=section_hints, warnings=warnings, bypass=bypass,
                           buffer_readings=readings, method_hints=hints, system=system)
