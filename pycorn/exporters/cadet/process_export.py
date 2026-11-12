# -*- coding: utf-8 -*-
"""
CADET-Process objects and CSV/JSON from a `CadetExperiment` (docs/ROADMAP.md, "CADET export":
phase C2, D24, D27).

- `to_csv(exp, directory)`: CSV like the CADET-Process examples (two columns, `time_s, value`, no
  header, X13), one file per signal, plus `experiment.json` with units, sources, sections,
  calibration and warnings, so the data can be used without importing PyCORN. No CADET import.
- `process_inputs(exp, ...)`: the Process skeleton as plain data (no CADET import): component
  names and, per section, the inlet flow and concentration polynomials.
- `to_references`, `to_fractionation_reference`, `to_process`, `to_cadet_process`: CADET-Process
  0.12 objects. `CADETProcess` is imported only there, with a clear error when it is missing.

Owner decisions (C2, 2026-11-08):
- One flow sheet with the CADET-Core input (C3). D27 replaced the single inlet: 'buffer' inlet ->
  'mixer' CSTR -> column -> 'tubing' CSTR -> outlet, and a 'feed' inlet -> column for valve feed
  (`system_units`, `system_flows`); the mixer holds during valve feed. Column flow is D14's, so
  pauses and by-passed sections have Q = 0 on the column; during a by-pass with flow the system
  flow goes through the mixer and around the column to the tubing / outlet (owner, M2).
- No caller concentrations at all: placeholder components, 'salt' following %B (0-100 mol/m^3)
  and 'sample' = 1 in feed sections, marked placeholder, so the Process simulates as a tracer run.
  Caller concentrations are all or nothing: given components need every buffer line the run
  uses and, when it has feed, the feed.
- By-pass windows (D18, D22) stay in the references as measured; each `ReferenceIO` lists them in
  `not_column_outlet`, with a warning, so the caller can set a metric's start/end.
- The Process needs `time_zero='method_start'` (D5): CADET-Process events can't be negative.
  References and CSV work with either time zero.

No simulation is run from PyCORN. CADET-Process 0.12 writes the CADET-Core 5 layout, so its
`Process` simulates on CADET-Core 5.1.2, not on Core 6 (X3); for Core 6 use the Core input (C3).
"""
import json
import os
import re
from collections import OrderedDict
from dataclasses import asdict, is_dataclass

import numpy as np

from ...units import conversion_factor

PLACEHOLDER_COMPONENTS = ("salt", "sample")
PLACEHOLDER_SALT_AT_100_B = 100.0  # mol/m^3 of 'salt' at 100 %B
QUATERNARY_LINES = ("Q1", "Q2", "Q3", "Q4")
COLUMN_MODELS = ("GeneralRateModel", "LumpedRateModelWithPores", "LumpedRateModelWithoutPores")
SIDECAR = "experiment.json"


def _cadet_process():
    try:
        import CADETProcess
    except ImportError as exc:
        raise ImportError("CADET-Process is needed for this (pip install CADET-Process==0.12.0, or "
                          "pip install pycorn[cadet]); to_csv() and process_inputs() work without it") from exc
    return CADETProcess


def _section_flow_at(sections, t):
    """Column flow (m^3/s) at times `t`, from the sections (D14); 0 outside them."""
    t = np.asarray(t, dtype=np.float64)
    q = np.zeros_like(t)
    for s in sections:
        inside = (t >= s.start_s) & (t < s.end_s)
        q[inside] = s.flow_m3_s[0] + s.flow_m3_s[1] * (t[inside] - s.start_s)
    if sections:
        last = sections[-1]
        q[t == last.end_s] = last.flow_m3_s[0] + last.flow_m3_s[1] * (last.end_s - last.start_s)
    return q


# --- Process inputs (no CADET import) -------------------------------------------------------------

def _vector(value, names, what):
    if isinstance(value, dict):
        unknown = [k for k in value if k not in names]
        if unknown:
            raise ValueError("%s: unknown component(s) %s; components: %s"
                             % (what, ", ".join(map(str, unknown)), ", ".join(names)))
        return np.array([float(value.get(n, 0.0)) for n in names])
    v = np.asarray(value, dtype=np.float64).reshape(-1)
    if len(v) != len(names):
        raise ValueError("%s: %d values for %d components (%s)" % (what, len(v), len(names), ", ".join(names)))
    return v


def _section_lines_used(s):
    """
    Buffer lines of section `s` whose share is above 0 ('A', 'B', 'Q1'..'Q4'); none in pauses
    and valve feed. In feed through a system pump line (D27) the other line still carries buffer.
    """
    feed_line = getattr(s, "feed_line", None)
    if s.paused or (s.feed and not feed_line) or s.end_s <= s.start_s:
        return set()
    b0, slope = s.percent_b
    b0 = 0.0 if b0 is None else b0
    b1 = b0 + slope * (s.end_s - s.start_s)
    here = set()
    if max(b0, b1) > 0:
        here.add("B")
    if min(b0, b1) < 100:
        if s.percent_q is None:
            here.add("A")
        else:
            for line, q0, qs in zip(QUATERNARY_LINES, *s.percent_q):
                if max(q0, q0 + qs * (s.end_s - s.start_s)) > 0:
                    here.add(line)
    return here - {feed_line}


def _lines_used(sections):
    """Buffer lines whose share is above 0 anywhere (`_section_lines_used`)."""
    used = set()
    for s in sections:
        used |= _section_lines_used(s)
    return used


BUFFER_LINES = ("A", "B") + QUATERNARY_LINES


def _inlet_positions(sections):
    """{line: [positions in order of first use]} from the sections' inlet switches ('Inlet A A1')."""
    out = OrderedDict()
    for s in sections:
        for line, position in (getattr(s, "inlets", None) or {}).items():
            if line in BUFFER_LINES and position and position not in out.setdefault(line, []):
                out[line].append(position)
    return out


def _section_buffers(s, given):
    """
    {line: concentrations} for section `s` from `given` (keyed by inlet position or line): the
    position the section ran on ('A2', as the run log names it) wins over its line ('A').
    """
    inlets = getattr(s, "inlets", None) or {}
    out = {}
    for line in BUFFER_LINES:
        position = inlets.get(line)
        if position is not None and position in given:
            out[line] = given[position]
        elif line in given:
            out[line] = given[line]
    return out


def _assumed_positions(sections):
    """{position: line} of the first positions taken as assumed (no position logged; `inlet_state`)."""
    out = {}
    for s in sections:
        for line, st in (getattr(s, "inlet_states", None) or {}).items():
            if line in BUFFER_LINES and st.get("state") == "assumed" and st.get("position"):
                out.setdefault(st["position"], line)
    return out


def _unsure_line_warnings(sections, given):
    """
    One warning per line keyed by its line letter ('B') in a section where its position is
    assumed (a valve, none logged yet) or not known (the system configuration doesn't say).
    """
    out = OrderedDict()
    for s in sections:
        states = getattr(s, "inlet_states", None) or {}
        for line in sorted(_section_lines_used(s)):
            st = states.get(line)
            if st is None or st.get("state") == "known" or line not in given:
                continue
            out.setdefault((line, st["label"]), s.start_s)
    return ["buffers[%r] used for line %s from %.1f s, where the run log has set no position (shown as %r): "
            "the composition of whatever that line held" % (line, line, t, label)
            for (line, label), t in out.items()]


def _where(s, line):
    position = (getattr(s, "inlets", None) or {}).get(line)
    what = "inlet %s (line %s)" % (position, line) if position else "line %s" % line
    return what, ("buffers[%r] or buffers[%r]" % (position, line) if position else "buffers[%r]" % line)


def _poly_mul(a, b):
    """Product of two coefficient lists (constant first)."""
    out = [0.0] * (len(a) + len(b) - 1)
    for i, x in enumerate(a):
        for j, y in enumerate(b):
            out[i + j] += x * y
    return out


def _mix(s, buffers, n_comp):
    """
    Concentration polynomial (n_comp x 4, constant first, time from the section start) of the
    buffer mix in `s`: A and B by %B; with quaternary shares the Q mix takes line A's place.
    """
    b0, bs = s.percent_b
    b = [0.0 if b0 is None else b0 / 100.0, bs / 100.0]
    if s.percent_q is None:
        side = [[buffers["A"][k], 0.0] for k in range(n_comp)] if "A" in buffers else [[0.0, 0.0]] * n_comp
    else:
        side = [[0.0, 0.0] for _ in range(n_comp)]
        for line, q0, qs in zip(QUATERNARY_LINES, *s.percent_q):
            if line in buffers:
                for k in range(n_comp):
                    side[k][0] += buffers[line][k] * q0 / 100.0
                    side[k][1] += buffers[line][k] * qs / 100.0
    a_share = [1.0 - b[0], -b[1]]
    out = np.zeros((n_comp, 4))
    for k in range(n_comp):
        poly = _poly_mul(a_share, side[k])
        if "B" in buffers:
            poly = [x + y for x, y in zip(poly, [buffers["B"][k] * b[0], buffers["B"][k] * b[1], 0.0])]
        out[k, :len(poly)] = poly
    return out


def process_inputs(exp, components=None, buffers=None, feed=None):
    """
    The Process skeleton as plain data: {'components', 'placeholder', 'sections', 'cycle_time_s',
    'valve_feed', 'warnings'}. Each section row: 'name', 'start_s', 'end_s', 'flow_m3_s' (column
    flow Q0, Q1), 'feed', 'feed_path' (None, 'valve', or 'line_A' / 'line_B' for sample drawn through a
    system pump line, D27), 'paused', and per inlet (D27: 'buffer' -> mixer -> column, 'feed' ->
    column): 'buffer_flow_m3_s', 'feed_flow_m3_s', 'c_buffer', 'c_feed' (n_comp x 4 polynomials in
    mol/m^3, time from the section start). In valve feed the buffer path has no flow (the mixer
    holds). 'c' is what enters: 'c_feed' in valve feed, else 'c_buffer'.

    - `components`: component names, in order.
    - `buffers`: {inlet: concentrations} (mol/m^3), each a list in component order or a
      {name: value} dict (missing names are 0). The key is an inlet position exactly as the run
      log names it ('A1', 'B2', or a process system's own names; `Inlet A A1` in the run log), or
      a pump line ('A', 'B', 'Q1'..'Q4') for every position on that line. Each section uses the
      position it ran on, else its line: with a buffer valve, a run that switches A1 -> A2 needs
      buffers['A1'] and buffers['A2'] if they differ. A position the run log never set is keyed
      by the line alone (owner, 2026-11-11): on a line with no inlet valve ('B' is known), and,
      with a warning, where the valve's first position is only assumed ('B1 (assumed)': key
      'B', never 'B1') or the system configuration doesn't say ('B (not known)').
    - `feed`: the feed concentrations (mol/m^3), list or dict; used in feed sections.
    Each falls back to `exp.components['names' | 'buffers' | 'feed']`. With none of them:
    placeholder components (owner, C2).
    """
    comp = exp.components or {}
    components = components if components is not None else comp.get("names")
    buffers = buffers if buffers is not None else comp.get("buffers")
    feed = feed if feed is not None else comp.get("feed")
    sections = [s for s in exp.sections if s.end_s > s.start_s]
    warnings = []
    has_feed = any(s.feed for s in sections)
    placeholder = components is None and buffers is None and feed is None
    if placeholder:
        names = list(PLACEHOLDER_COMPONENTS)
        lines = {"A": np.array([0.0, 0.0]), "B": np.array([PLACEHOLDER_SALT_AT_100_B, 0.0])}
        warnings.append("no component concentrations given: placeholder components 'salt' (%g mol/m^3 at 100 %%B) "
                        "and 'sample' (1 mol/m^3 in feed sections); a tracer setup, not the real buffers"
                        % PLACEHOLDER_SALT_AT_100_B)
        if any(s.percent_q is not None for s in sections):
            warnings.append("quaternary sections: placeholder 'salt' follows %B only; the Q lines carry nothing")
    else:
        if components is None:
            raise ValueError("buffers or feed given without components: pass components=[names]")
        names = [str(n) for n in components]
        lines = {str(k): _vector(v, names, "buffer %s" % k) for k, v in (buffers or {}).items()}
        positions = _inlet_positions(sections)
        known = set(BUFFER_LINES) | {p for ps in positions.values() for p in ps}
        unknown = sorted(set(lines) - known)
        if unknown:
            found = "; ".join("line %s: %s" % (line, ", ".join(ps)) for line, ps in positions.items()) or "none"
            assumed = _assumed_positions(sections)
            hint = "".join("; %s is never logged (line %s: its valve's first position, assumed where no position "
                           "is logged): key it %r" % (p, assumed[p], assumed[p]) for p in unknown if p in assumed)
            raise ValueError("buffers: %s is neither a pump line (%s) nor an inlet position of this run (run log: %s)%s"
                             % (", ".join(unknown), ", ".join(BUFFER_LINES), found, hint))
        missing = OrderedDict()
        for s in sections:
            have = _section_buffers(s, lines)
            for line in sorted(_section_lines_used(s) - set(have)):
                what, give = _where(s, line)
                missing.setdefault(what, (give, s.start_s))
        if missing:
            raise ValueError("the run uses %s; give their concentrations in buffers= (%s)" % (
                ", ".join("%s from %.1f s" % (what, t) for what, (_, t) in missing.items()),
                ", ".join(give for give, _ in missing.values())))
        for line, ps in positions.items():
            if line in lines and len(ps) > 1 and not all(p in lines for p in ps):
                warnings.append("buffers[%r] stands for inlets %s, which the run switches between: the same "
                                "composition is used for each; give buffers per inlet if they hold different "
                                "buffers" % (line, ", ".join(p for p in ps if p not in lines)))
        warnings += _unsure_line_warnings(sections, lines)
        if has_feed and feed is None:
            raise ValueError("the run has feed sections; give the feed concentrations in feed=")
    n = len(names)
    feed_c = None if placeholder or feed is None else _vector(feed, names, "feed")
    if any(s.percent_q is not None and (s.percent_b[0] or s.percent_b[1]) for s in sections):
        warnings.append("sections with both %B and quaternary shares: the Q mix is taken as line A's side of the "
                        "A/B mix")
    unknown_b = [s for s in sections if s.percent_b[0] is None]
    if unknown_b:
        warnings.append("%d section(s) with unknown %%B, taken as 0 %%B (first at %.1f s)"
                        % (len(unknown_b), unknown_b[0].start_s))

    rows = []
    zero_q = (0.0, 0.0)
    given = lines
    for s in sections:
        q = tuple(s.flow_m3_s)
        lines = given if placeholder else _section_buffers(s, given)  # the inlets this section ran on
        feed_line = getattr(s, "feed_line", None)
        if s.feed and feed_line:
            # D27: sample drawn through a system pump line passes the mixer; that line carries the feed
            mixed = dict(lines)
            if placeholder:
                sample = np.zeros(n)
                sample[names.index("sample")] = 1.0
                mixed[feed_line] = np.asarray(lines.get(feed_line, np.zeros(n))) + sample
            else:
                mixed[feed_line] = feed_c
            c_buffer, c_feed, q_buffer, q_feed, path = _mix(s, mixed, n), np.zeros((n, 4)), q, zero_q, "line_" + feed_line
        elif s.feed:
            # valve feed (loop, Superloop, direct inject): enters after the mixer, which holds (D27)
            c_buffer = _mix(s, lines, n)
            if placeholder:
                c_feed = c_buffer.copy()
                c_feed[names.index("sample"), 0] = 1.0
            else:
                c_feed = np.zeros((n, 4))
                c_feed[:, 0] = feed_c
            q_buffer, q_feed, path = zero_q, q, "valve"
        else:
            c_buffer, c_feed, q_buffer, q_feed, path = _mix(s, lines, n), np.zeros((n, 4)), q, zero_q, None
        q_bypass = zero_q
        if getattr(s, "bypass", False) and not s.paused and not s.feed and getattr(s, "system_flow_m3_s", 0.0):
            # owner, 2026-11-08 (M2): with the column by-passed the pumps still push the system flow
            # through the mixer, around the column to the UV; the column itself gets none
            q_bypass = (float(s.system_flow_m3_s), 0.0)
            q_buffer = (q_buffer[0] + q_bypass[0], q_buffer[1])
        rows.append(OrderedDict([("name", s.name), ("start_s", s.start_s), ("end_s", s.end_s), ("flow_m3_s", q),
                                 ("c", c_feed if path == "valve" else c_buffer), ("feed", s.feed),
                                 ("feed_path", path), ("buffer_flow_m3_s", q_buffer), ("feed_flow_m3_s", q_feed),
                                 ("bypass_flow_m3_s", q_bypass),
                                 ("c_buffer", c_buffer), ("c_feed", c_feed), ("paused", s.paused)]))
    return OrderedDict([("components", names), ("placeholder", placeholder), ("sections", rows),
                        ("cycle_time_s", sections[-1].end_s if sections else 0.0),
                        ("valve_feed", any(r["feed_path"] == "valve" for r in rows)),
                        ("bypass_flow", any(r["bypass_flow_m3_s"][0] for r in rows)), ("warnings", warnings)])


# --- CADET-Process objects ------------------------------------------------------------------------

def _reference_specs(exp):
    """(name, signal, values, unit, component name) for every reference: one per signal, plus one per
    UV concentration (mol/m^3)."""
    specs = []
    for sig in exp.signals:
        if sig.export_values is None:
            continue
        specs.append((sig.name, sig, np.asarray(sig.export_values, dtype=np.float64), sig.export_unit, sig.name))
        if sig.concentration and sig.concentration.get("mol/m^3") is not None:
            specs.append(("%s mol/m^3" % sig.name, sig, np.asarray(sig.concentration["mol/m^3"], dtype=np.float64),
                          "mol/m^3", sig.name))
    return specs


def to_references(exp):
    """
    One `ReferenceIO` per signal, in its export unit (UV AU/cm, conductivity S/m, %B in %), and one per
    UV concentration in mol/m^3; `flow_rate` is the column flow from the sections (D14). Each carries
    `unit`, `primary` and `not_column_outlet` (by-pass windows, D18/D22: kept as measured).
    Returns (references, warnings).
    """
    _cadet_process()
    from CADETProcess.processModel import ComponentSystem
    from CADETProcess.reference import ReferenceIO

    refs, warnings = [], []
    for name, sig, values, unit, component in _reference_specs(exp):
        ref = ReferenceIO(name, sig.time_s, values.reshape(-1, 1),
                          flow_rate=_section_flow_at(exp.sections, sig.time_s),
                          component_system=ComponentSystem([component]))
        ref.unit = unit
        ref.primary = sig.primary
        ref.not_column_outlet = list(sig.not_column_outlet)
        if sig.not_column_outlet:
            warnings.append("%s: %d by-pass window(s) kept as measured (%s); not column outlet, so set the metric's "
                            "start/end around them" % (name, len(sig.not_column_outlet),
                                                       ", ".join("%.1f-%.1f s" % w for w in sig.not_column_outlet)))
        refs.append(ref)
    return refs, warnings


def to_fractionation_reference(exp, molecular_weights=None, molecular_weight_unit="g/mol", name="fractions"):
    """
    `FractionationReference` from the caller's fraction assays (D4), in mol per component (CADET
    simulates mol/m^3), so `molecular_weights` ({component: MW}) is needed. Fractions without
    times, volume or a value for every component are left out, with a warning.
    Returns (reference or None, warnings).
    """
    warnings = []
    assayed = [f for f in exp.fractions if f.mass_kg]
    if not assayed:
        return None, ["no fraction assays given; no FractionationReference (D4)"]
    names = list(OrderedDict((c, None) for f in assayed for c in f.mass_kg))
    mw = molecular_weights if molecular_weights is not None else (exp.components or {}).get("molecular_weights")
    if not mw:
        return None, ["fraction assays without molecular_weights: no FractionationReference (CADET works in mol)"]
    missing = [c for c in names if c not in mw]
    if missing:
        raise ValueError("no molecular weight for component(s) %s" % ", ".join(missing))
    to_kg_mol = conversion_factor(molecular_weight_unit, "kg/mol")
    _cadet_process()
    from CADETProcess.fractionation import Fraction
    from CADETProcess.processModel import ComponentSystem
    from CADETProcess.reference import FractionationReference

    fractions = []
    for f in assayed:
        if f.start_s is None or f.end_s is None or not f.volume_m3 or f.end_s <= f.start_s:
            warnings.append("fraction %s: no times or volume; left out" % f.tube)
            continue
        if any(c not in f.mass_kg for c in names):
            warnings.append("fraction %s: no value for %s; left out"
                            % (f.tube, ", ".join(c for c in names if c not in f.mass_kg)))
            continue
        mol = np.array([f.mass_kg[c] / (float(mw[c]) * to_kg_mol) for c in names])
        fractions.append(Fraction(mol, f.volume_m3, f.start_s, f.end_s))
    if not fractions:
        return None, warnings + ["no usable fraction assay; no FractionationReference"]
    return FractionationReference(name, fractions, component_system=ComponentSystem(names)), warnings


def _set_column(column, exp, model, warnings):
    col = exp.column
    unset = []
    for attr, key in (("length", "length_m"), ("diameter", "diameter_m")):
        if col.get(key) is not None:
            setattr(column, attr, col[key])
        else:
            unset.append(attr)
    por = col.get("porosities") or {}
    if model == "LumpedRateModelWithoutPores":
        if "total" in por:
            column.total_porosity = por["total"]
        else:
            unset.append("total_porosity")
    else:
        if col.get("particle_radius_m") is not None:
            column.particle_radius = col["particle_radius_m"]
        else:
            unset.append("particle_radius")
        for attr, key in (("bed_porosity", "column"), ("particle_porosity", "particle")):
            if key in por:
                setattr(column, attr, por[key])
            else:
                unset.append(attr)
    warnings.append("column: left for the caller: %s, transport and binding (D10)"
                    % ", ".join(unset) if unset else "column: transport and binding left for the caller")


def system_units(exp, inputs, system_volumes=True):
    """
    D27 flow sheet as plain data: [(name, kind, volume_m3)] in order, kind 'inlet', 'cstr', 'column',
    'outlet'. 'buffer' inlet -> 'mixer' (when its volume is known) -> 'column'; 'feed' inlet ->
    'column' (only with valve feed); 'column' -> 'tubing' (caller volume) -> 'outlet'. With a column
    by-pass that has flow (M2), the mixer (else the buffer inlet) also connects past the column to
    'tubing' (else 'outlet'), see `system_flows`. `system_volumes=False` leaves both CSTRs out.
    """
    system = (exp.system or {}) if system_volumes else {}
    units = [("buffer", "inlet", None)]
    if inputs["valve_feed"]:
        units.append(("feed", "inlet", None))
    if system.get("mixer_m3"):
        units.append(("mixer", "cstr", system["mixer_m3"]))
    units.append(("column", "column", None))
    if system.get("tubing_m3"):
        units.append(("tubing", "cstr", system["tubing_m3"]))
    units.append(("outlet", "outlet", None))
    return units


def _minus(a, b):
    return (a[0] - b[0], a[1] - b[1])


def _plus(a, b):
    return (a[0] + b[0], a[1] + b[1])


def system_flows(units, row, bypass=False):
    """
    Connections (from, to, (Q0, Q1)) for one `process_inputs` section row and `system_units`.
    `bypass` (`inputs['bypass_flow']`): the run has a column by-pass with flow, so every row also
    has the connection past the column (owner, 2026-11-08, M2): the system flow goes through the
    mixer, around the column, and joins the column's outlet before the UV ('tubing', else 'outlet').
    It is 0 outside by-pass sections; the column gets the rest of the buffer flow.
    """
    names = {u[0] for u in units}
    qb, qf, qc = tuple(row["buffer_flow_m3_s"]), tuple(row["feed_flow_m3_s"]), tuple(row["flow_m3_s"])
    qy = tuple(row.get("bypass_flow_m3_s", (0.0, 0.0)))
    source = "mixer" if "mixer" in names else "buffer"
    after = "tubing" if "tubing" in names else "outlet"
    out = []
    if source == "mixer":
        out.append(("buffer", "mixer", qb))
    out.append((source, "column", _minus(qb, qy)))
    if bypass:
        out.append((source, after, qy))
    if "feed" in names:
        out.append(("feed", "column", qf))
    if after == "tubing":
        out += [("column", "tubing", qc), ("tubing", "outlet", _plus(qc, qy) if bypass else qc)]
    else:
        out.append(("column", "outlet", qc))
    return out


def check_process_overrides(overrides):
    """TypeError unless `overrides` is None or a dict (its content is CADET-Process's business, D28)."""
    if overrides is not None and not isinstance(overrides, dict):
        raise TypeError("process_overrides must be a dict in CADET-Process's own parameter layout (as "
                        "Process.parameters shows it), e.g. {'flow_sheet': {'column': {'bed_porosity': 0.37}}}")


def to_process(exp, components=None, buffers=None, feed=None, column_model="GeneralRateModel", name=None,
               system_volumes=True, process_overrides=None):
    """
    CADET-Process `Process` (D27): Inlet 'buffer' -> Cstr 'mixer' -> column 'column' -> Cstr 'tubing'
    -> Outlet 'outlet', and Inlet 'feed' -> 'column' for valve feed (see `system_units`). Per section
    one flow and one concentration event per inlet and one flow event per CSTR (outflow = inflow);
    `cycle_time` = last section end. Geometry and porosities from the experiment when it has them;
    transport and binding are left for the caller. Returns (process, inputs) with `inputs` from
    `process_inputs` plus 'units'.

    `process_overrides` (owner, 2026-11-08: one generic parameter block, no assumptions about what
    it holds): a nested dict in CADET-Process's own layout, as `process.parameters` shows it, e.g.
    {'flow_sheet': {'column': {'bed_porosity': 0.37}}}. Set last with `process.parameters = ...`,
    unchanged; CADET-Process checks the names (a wrong one raises). Objects such as a binding
    model are made with CADET-Process's API on the returned Process.
    """
    check_process_overrides(process_overrides)
    if exp.time_zero.get("kind") != "method_start":
        raise ValueError("the Process needs time_zero='method_start' (CADET-Process events can't be negative); "
                         "rebuild the experiment with it (references and CSV work with either)")
    if column_model not in COLUMN_MODELS:
        raise ValueError("column_model must be one of %s, not %r" % (", ".join(COLUMN_MODELS), column_model))
    inputs = process_inputs(exp, components, buffers, feed)
    if not inputs["sections"]:
        raise ValueError("the experiment has no sections")
    _cadet_process()
    from CADETProcess.processModel import ComponentSystem, FlowSheet, Inlet, Outlet, Process
    from CADETProcess.processModel import unitOperation

    from CADETProcess.processModel import Cstr

    cs = ComponentSystem(inputs["components"])
    units = system_units(exp, inputs, system_volumes)
    inputs["units"] = units
    sheet = FlowSheet(cs)
    for unit_name, kind, volume in units:
        if kind == "inlet":
            unit = Inlet(cs, name=unit_name)
        elif kind == "cstr":
            unit = Cstr(cs, name=unit_name)
            unit.init_liquid_volume = volume
        elif kind == "column":
            unit = getattr(unitOperation, column_model)(cs, name=unit_name)
            _set_column(unit, exp, column_model, inputs["warnings"])
        else:
            unit = Outlet(cs, name=unit_name)
        sheet.add_unit(unit, product_outlet=kind == "outlet")
    bypass = inputs["bypass_flow"]
    for a, b, _ in system_flows(units, inputs["sections"][0], bypass):
        sheet.add_connection(sheet[a], sheet[b])
    # the unit that feeds both the column and the by-pass path (M2): its outflow is split per section
    split = ("mixer" if any(u[0] == "mixer" for u in units) else "buffer") if bypass else None

    def poly(c):
        return c[:, :2].tolist() if not np.any(c[:, 2:]) else c.tolist()

    process = Process(sheet, name or _safe(exp.source.get("run") or "run"))
    process.cycle_time = inputs["cycle_time_s"]
    for k, row in enumerate(inputs["sections"]):
        t = row["start_s"]
        process.add_event("s%03d_buffer_flow" % k, "flow_sheet.buffer.flow_rate", list(row["buffer_flow_m3_s"]), t)
        process.add_event("s%03d_buffer_c" % k, "flow_sheet.buffer.c", poly(row["c_buffer"]), t)
        if inputs["valve_feed"]:
            process.add_event("s%03d_feed_flow" % k, "flow_sheet.feed.flow_rate", list(row["feed_flow_m3_s"]), t)
            process.add_event("s%03d_feed_c" % k, "flow_sheet.feed.c", poly(row["c_feed"]), t)
        flows = system_flows(units, row, bypass)
        for unit in ("mixer", "tubing"):
            out = [q for a, _, q in flows if a == unit]
            if out:  # a CSTR's flow rate is its whole outflow
                process.add_event("s%03d_%s_flow" % (k, unit), "flow_sheet.%s.flow_rate" % unit,
                                  [sum(q[0] for q in out), sum(q[1] for q in out)], t)
        if split:
            # all to the column, or (column by-passed) all past it; a section is one or the other
            to_column = 0.0 if row["bypass_flow_m3_s"][0] else 1.0
            process.add_event("s%03d_%s_split" % (k, split), "flow_sheet.output_states.%s" % split,
                              [to_column, 1.0 - to_column], t)
    if process_overrides:
        import copy
        try:
            # a copy: CADET-Process empties the dict it is given, and the caller's must stay as it is
            process.parameters = copy.deepcopy(process_overrides)
        except Exception as e:  # CADET-Process names the problem; say where it came from
            raise ValueError("process_overrides not accepted by CADET-Process (%s: %s); its layout is what "
                             "process.parameters shows" % (type(e).__name__, e)) from e
        inputs["warnings"].append("caller process_overrides set (CADET-Process names): %s"
                                  % ", ".join(_paths(process_overrides)))
    return process, inputs


def _paths(tree, path=()):
    """'a.b.c' for every leaf of a nested dict."""
    out = []
    for key, value in tree.items():
        here = path + (str(key),)
        out += _paths(value, here) if isinstance(value, dict) and value else [".".join(here)]
    return out


def to_cadet_process(exp, components=None, buffers=None, feed=None, column_model="GeneralRateModel",
                     molecular_weights=None, molecular_weight_unit="g/mol", process=True, system_volumes=True,
                     process_overrides=None):
    """
    Everything for CADET-Process: {'references', 'fractionation', 'process', 'inputs', 'warnings'}.
    `process=False` skips the Process (e.g. for a time_zero='injection' experiment).
    `process_overrides`: see `to_process`.
    """
    check_process_overrides(process_overrides)
    refs, warnings = to_references(exp)
    frac, frac_warnings = to_fractionation_reference(exp, molecular_weights, molecular_weight_unit)
    warnings += frac_warnings
    proc, inputs = None, None
    if process:
        proc, inputs = to_process(exp, components, buffers, feed, column_model, system_volumes=system_volumes,
                                  process_overrides=process_overrides)
        warnings += inputs["warnings"]
    return OrderedDict([("references", refs), ("fractionation", frac), ("process", proc), ("inputs", inputs),
                        ("warnings", warnings)])


# --- CSV and JSON (no CADET import) ---------------------------------------------------------------

def _safe(name):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(name)).strip("_") or "signal"


def _plain(value):
    """JSON-ready copy: numpy to lists and numbers, dataclasses to dicts, tuples to lists."""
    if is_dataclass(value) and not isinstance(value, type):
        return _plain(asdict(value))
    if isinstance(value, dict):
        return OrderedDict((str(k), _plain(v)) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    if isinstance(value, np.ndarray):
        return _plain(value.tolist())
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, float) and not np.isfinite(value):
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _write_two_columns(path, t, v):
    np.savetxt(path, np.column_stack([np.asarray(t, dtype=np.float64), np.asarray(v, dtype=np.float64)]),
               delimiter=",", fmt="%.10g")


def to_csv(exp, directory, components=None, buffers=None, feed=None, system_volumes=True, process_overrides=None):
    """
    Write the experiment to `directory` (created if needed): one `time_s,value` CSV per reference
    (as in `to_references`), `flow.csv` (measured flow, m^3/s), and `experiment.json` with every
    file's unit, the sections, events, fractions, column, buffer readings, method hints, the
    Process inputs (placeholder or caller components) and all warnings. `system_volumes=False`
    leaves the mixer and tubing CSTRs out of `process_inputs.units`, as in `to_core_input` (D27).
    `process_overrides` (see `to_process`) is stored unchanged as `process_overrides`, for
    `process.parameters = ...` where the Process is built. Returns the sidecar path.
    """
    check_process_overrides(process_overrides)
    os.makedirs(directory, exist_ok=True)
    files, used = [], set()
    for name, sig, values, unit, _ in _reference_specs(exp):
        base = _safe(name)
        k = 2
        while base in used:
            base = "%s_%d" % (_safe(name), k)
            k += 1
        used.add(base)
        fname = base + ".csv"
        _write_two_columns(os.path.join(directory, fname), sig.time_s, values)
        files.append(OrderedDict([("file", fname), ("name", name), ("signal", sig.name), ("role", sig.role),
                                  ("columns", ["time_s", unit]), ("unit", unit), ("stored_unit", sig.unit),
                                  ("primary", sig.primary), ("wavelength_nm", sig.wavelength_nm),
                                  ("wavelength_source", sig.wavelength_source),
                                  ("uv_path_length_cm", sig.uv_path_length_cm), ("uv_path_basis", sig.uv_path_basis),
                                  ("conversion", (sig.concentration or {}).get("conversion") if unit == "mol/m^3" else None),
                                  ("not_column_outlet", sig.not_column_outlet)]))
    if exp.flow is not None:
        _write_two_columns(os.path.join(directory, "flow.csv"), exp.flow["time_s"], exp.flow["m3_s"])
        files.append(OrderedDict([("file", "flow.csv"), ("name", exp.flow["name"]), ("role", "flow"),
                                  ("columns", ["time_s", "m^3/s"]), ("unit", "m^3/s")]))
    warnings = list(exp.warnings)
    inputs = None
    try:
        inputs = process_inputs(exp, components, buffers, feed)
        warnings += ["process: %s" % w for w in inputs["warnings"]]
        inputs = OrderedDict([("components", inputs["components"]), ("placeholder", inputs["placeholder"]),
                              ("cycle_time_s", inputs["cycle_time_s"]),
                              ("units", system_units(exp, inputs, system_volumes)),
                              ("sections", [OrderedDict([(k, r[k]) for k in ("name", "start_s", "end_s", "flow_m3_s",
                                                                             "feed", "feed_path", "paused",
                                                                             "buffer_flow_m3_s", "feed_flow_m3_s",
                                                                             "bypass_flow_m3_s")]
                                                        + [("c_mol_m3", r["c"]), ("c_buffer_mol_m3", r["c_buffer"]),
                                                           ("c_feed_mol_m3", r["c_feed"])])
                                            for r in inputs["sections"]])])
    except ValueError as exc:
        warnings.append("process: no inputs (%s)" % exc)
    sidecar = OrderedDict([
        ("format", "pycorn-cadet-experiment"), ("format_version", 1),
        ("units", "SI: s, m, m^2, m^3, m^3/s, mol/m^3, S/m; UV in AU/cm (AU without a path length); %B in %"),
        ("source", exp.source), ("time_zero", exp.time_zero), ("files", files), ("column", exp.column),
        ("system", exp.system),
        ("components", exp.components), ("process_inputs", inputs), ("sections", exp.sections),
        ("bypass", exp.bypass), ("events", exp.events), ("fractions", exp.fractions),
        ("buffer_readings", exp.buffer_readings), ("hints", exp.hints), ("method_hints", exp.method_hints),
        ("process_overrides", process_overrides), ("warnings", warnings)])
    path = os.path.join(directory, SIDECAR)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_plain(sidecar), f, indent=1, ensure_ascii=True)
    return path
