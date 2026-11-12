# -*- coding: utf-8 -*-
"""
Method hints (docs/ROADMAP.md, "Method hints", D17 as amended by D20): what the method
planned, read only where the run log is silent, and compared with what ran. Nothing here
changes a section; precedence is caller > run log > method, and every value read from the
method is marked `source='method'`.

Sources: `run.metadata['MethodData']` (the method as programmed) and
`run.metadata['MethodDocumentationData']`, in ZIP, .UFol and database runs alike. The method
is a template: a breakpoint or setting tied to a method variable takes the value the run used,
from the result's variables (`pycorn.variables`, the start-protocol and scouting values).
Field names are matched as stored; raw values are kept next to parsed ones.

- Breakpoints: a block's instructions sit at breakpoint positions (`BreakPointValue`, in the
  block's base: volume, CV or time). A breakpoint variable belongs to the position, so it moves
  every instruction stored there (e.g. a sample volume variable on 'Injection valve Manual load'
  also moves the `End_Block` beside it).
- Base: the block's `Base` instruction; 'SameAsMain' takes the main block's. Volume is in the
  method's `NormalVolumeUnit`, time in min, CV against the Vc the run log gives.
- Injections (D20): the planned valve period runs from the breakpoint that turns the injection
  valve to Inject (or Direct inject) to the next valve instruction in the block, else to the
  block's `End_Block`. It is compared with the executed valve period of each run of that block,
  counted as UNICORN counts the base: system flow set-points, pauses excluded. The valve turns
  a few seconds after the breakpoint (about 2.7 s measured on an AKTA avant) while the flows set
  before it run (the pre-switch gap, docs/FACTS.md "Method and injection facts"). That gap is measured from
  the log (set-point amount from the breakpoint to the logged switch) and allowed: short
  beyond gap + `TOLERANCE` (at least `MIN_SHORT_ML`), or over the plan by more than
  `TOLERANCE`, is a warning (owner, rev 7f; a planned 0 warns whenever anything flowed). The
  feed itself stays the logged valve period (D20); this only checks it. Direct inject is
  reported, not checked.
- Blocks: one row per executed block with a fixed length (`End_Block` past 0), planned against
  executed, with a cause where the log or the method shows one ('next_breakpoint',
  'watch_in_block', 'hold_or_pause'). Report only, no warnings. Phases are logged as phases, not
  blocks, and are not rows. Method blocks that never ran are listed by name.
- Sample application: the phase dialog fields (`m...` in the phase's `ConfigurationProperties`).
  The dialog's sample volume can be stale (the block breakpoint is what ran); a difference is a
  warning.
- BufferPro: when `IsBufferPrepEnabled` is true, the recipe fields the UNICORN help documents
  (recipe name, pH, buffer and salt concentration, stock solution per Q inlet) matched by field
  name, raw values kept. No run with BufferPro on was available: the layout is unchecked and
  says so in a warning. Never used as buffer concentrations (D3).
- Variables and documentation: every variable value the run used, the method's scouting flags
  and scouting runs, documentation columns, phase texts and start-protocol items. Labels only.
"""
import re
from collections import OrderedDict
from dataclasses import dataclass, field

from .run_log_sections import _event_seconds, executed_entries, run_log_sections
from .variables import VARIABLE_KINDS
from .variables import rows as variable_rows

TOLERANCE = 0.02  # valve period off the plan (after the pre-switch gap) by more than this (relative) warns
MIN_SHORT_ML = 0.005  # ... and short by at least this much (log times are 0.1-0.2 s apart)
BLOCK_TOLERANCE = 0.05  # block length within this (relative) is 'as_planned'; set-points miss pump ramps
INJECT_POSITIONS = ("inject", "direct inject")
TIME_UNIT = "min"
DIALOG_FIELD = re.compile(r"<(m[A-Z]\w*)(?:\s[^>]*)?>([^<]*)</\1>")
DIALOG_NAMES = OrderedDict([("mLoopType", "loop_type"), ("mSampleVolumeForSuperloop", "sample_volume"),
                            ("mFillLoopWith", "fill_loop_with"), ("mEmptyLoopWith", "empty_loop_with"),
                            ("mLoopLoading", "loop_loading"), ("mInjectAllSample", "inject_all_sample"),
                            ("mFixedVolume", "fixed_volume"), ("mSetMaxVolume", "set_max_volume"),
                            ("mMaxVolume", "max_volume"), ("mDirectSampleInlet", "direct_sample_inlet")])
HOLD_INSTRUCTIONS = ("Hold", "Hold until", "Pause")
# BufferPro fields as the UNICORN help names them, matched against stored element names
BUFFERPRO_ROOT = re.compile(r"bufferpro|recipe|bufferprep", re.I)
STOCK = re.compile(r"stock", re.I)


@dataclass
class MethodHints:
    available: bool  # the run has MethodData
    source: str = "method"
    method: dict = field(default_factory=dict)  # name, UNICORN version, units, technique
    variables: list = field(default_factory=list)  # every variable value the run used (source 'run')
    scouting: dict = field(default_factory=dict)
    documentation: dict = field(default_factory=dict)
    buffer_prep: dict = None  # None when BufferPro is off
    sample_application: list = field(default_factory=list)  # phase dialog settings
    injections: list = field(default_factory=list)  # planned vs executed valve periods (D20)
    blocks: list = field(default_factory=list)  # planned vs executed block lengths
    blocks_not_run: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


def _list(x):
    return x if isinstance(x, list) else [] if x is None else [x]


def _number(text):
    try:
        return float(str(text).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None


def _bool(text):
    return None if text is None else str(text).strip().lower() == "true"


def _text(node):
    """Element text: a plain string, or the '#text' of an element with attributes."""
    if isinstance(node, dict):
        node = node.get("#text")
    return None if node in (None, "None") else str(node)


PARAMETER_NAME_KEYS = ("ParameterName", "GenericCommandParameterName", "ConditionParameterName")


def _params(instruction, hidden=True):
    """
    {parameter name: (readable value, unit, variable name)} of one block instruction, in stored
    order. Conditions (Hold until, Watch) name theirs `ConditionParameterName`; a parameter with
    no name is keyed by its position ('parameter 2'). `hidden=False` leaves out the parameters
    UNICORN stores as hidden and never shows (`IsHidden`).
    """
    out = OrderedDict()
    for kind in ("GenericCommandInstruction", "StrategyInstruction"):
        body = instruction.get(kind)
        if not isinstance(body, dict):
            continue
        for n, p in enumerate(_list(body.get("GenericCommandParameter")) + _list(body.get("StrategyParameter"))):
            if not hidden and _bool(p.get("IsHidden")):
                continue
            name = next((p[k] for k in PARAMETER_NAME_KEYS if p.get(k)), None) or "parameter %d" % (n + 1)
            out[name] = (_text(p.get("ParameterReadableValue")), _text(p.get("ParameterUnit")), p.get("VariableName"))
    return out


def _called_block(instruction):
    """
    The block an instruction calls, else None: the `CalledBlockName` of any of its parameters
    (a Block call's `Block`, a Watch's `Action`), else a Block call's `Block` value.
    """
    for kind in ("GenericCommandInstruction", "StrategyInstruction"):
        body = instruction.get(kind)
        if not isinstance(body, dict):
            continue
        for p in _list(body.get("GenericCommandParameter")) + _list(body.get("StrategyParameter")):
            name = _text(p.get("CalledBlockName")) if isinstance(p, dict) else None
            if name:
                return name
    if instruction.get("ReadableName") == "Block":
        return (_params(instruction).get("Block") or (None,))[0]
    return None


def _leaves(node, path=""):
    """(path, text) for every leaf under `node`."""
    if isinstance(node, dict):
        for k, v in node.items():
            yield from _leaves(v, path + "/" + k if path else k)
    elif isinstance(node, list):
        for k, v in enumerate(node):
            yield from _leaves(v, "%s[%d]" % (path, k))
    elif node is not None:
        yield path, str(node)


class _Block:
    """One method block: base, breakpoint groups (with the run's variable values), instructions."""

    def __init__(self, raw, used, warnings):
        self.raw, self.name = raw, raw.get("BlockName")
        self.instructions = _list((raw.get("BlockInstructions") or {}).get("BlockInstruction"))
        self.phase = (raw.get("MethodPhase") or {}).get("PhaseName") if isinstance(raw.get("MethodPhase"), dict) else None
        self.main = _bool(raw.get("IsMainBlock"))
        self.unused = _bool(raw.get("IsUnused"))
        self.groups = []
        for ins in self.instructions:
            stored = ins.get("BreakPointValue")
            if not self.groups or self.groups[-1]["stored"] != stored:
                self.groups.append(OrderedDict([("stored", stored), ("value", _number(stored)), ("variable", None),
                                                ("instructions", [])]))
            g = self.groups[-1]
            g["instructions"].append(ins)
            var = ins.get("BreakPointVariable")
            if var:
                if g["variable"] not in (None, var):
                    warnings.append("block %r: breakpoint %s has variables %r and %r; the first is used"
                                    % (self.name, stored, g["variable"], var))
                    continue
                g["variable"] = var
                row = used.get(var)
                if row is not None and row.number == row.number:  # not NaN
                    g["value"] = row.number
        self.length = None
        if self.groups and self.groups[-1]["instructions"][-1].get("ReadableName") == "End_Block":
            self.length = self.groups[-1]["value"]
        self.length_variable = self.groups[-1]["variable"] if self.groups else None

    def base(self):
        """(kind, method Vc in ml or None) from the block's own Base instruction."""
        for ins in self.instructions:
            if ins.get("ReadableName") == "Base":
                p = _params(ins)
                return (p.get("Base") or (None,))[0], _number((p.get("Volume") or (None,))[0])
        return None, None

    def has(self, names):
        return any((ins.get("ReadableName") or "") in names for ins in self.instructions)

    def calls(self):
        return [(_params(i).get("Block") or (None,))[0] for i in self.instructions if i.get("ReadableName") == "Block"]

    def planned_injections(self):
        """[(position, start, end, end_at)] planned valve periods in base units; end_at 'valve' or 'End_Block'."""
        out, current = [], None
        for g in self.groups:
            for ins in g["instructions"]:
                if ins.get("ReadableName") != "Injection valve":
                    continue
                pos = (_params(ins).get("Position") or (None,))[0]
                if current is not None and pos != current[0]:
                    out.append((current[0], current[1], g["value"], "valve"))
                    current = None
                if current is None and (pos or "").lower() in INJECT_POSITIONS:
                    current = (pos, g["value"])
        if current is not None:
            out.append((current[0], current[1], self.length, "End_Block"))
        return out


def _block_runs(entries):
    """[(name, start elapsed s, end elapsed s)] of every executed block, by the log's start/end nesting."""
    runs, stack = [], []
    for e in entries:
        if e["kind"] == "block_start" and e["text"].startswith("Block "):
            stack.append((e["text"][len("Block "):].strip(), e["elapsed_s"]))
        elif e["kind"] == "block_end" and e["text"].startswith("End_Block") and stack:
            name, t0 = stack.pop()
            runs.append((name, t0, e["elapsed_s"]))
    runs.sort(key=lambda r: r[1])
    return runs


def _wall(sections, elapsed_s):
    """Wall-clock seconds for an elapsed time (pauses before it added)."""
    for s in sections:
        if s.elapsed_start_s <= elapsed_s < s.elapsed_end_s or (s.elapsed_end_s == elapsed_s and not s.paused):
            return s.start_s + (elapsed_s - s.elapsed_start_s)
    return elapsed_s if not sections else sections[-1].end_s + (elapsed_s - sections[-1].elapsed_end_s)


def _amount(sections, t0, t1, base, keep=None):
    """
    (amount in the base, ml) between elapsed `t0` and `t1`, from system flow set-points, pauses
    excluded. Amount is None for a CV base without a logged Vc.
    """
    amount, ml = 0.0, 0.0
    for s in sections:
        if s.paused or (keep is not None and not keep(s)):
            continue
        a, b = max(t0, s.elapsed_start_s), min(t1, s.elapsed_end_s)
        if b <= a:
            continue
        v = (s.system_flow_ml_min or 0.0) * (b - a) / 60
        ml += v
        if base == "Time":
            amount += (b - a) / 60
        elif base == "CV":
            amount = None if amount is None or not s.vc_ml else amount + v / s.vc_ml
        else:
            amount += v
    return amount, ml


def _base_unit(kind, volume_unit):
    return {"Volume": volume_unit, "CV": "CV", "Time": TIME_UNIT}.get(kind)


def _variables(run, method_vars):
    flags = {v.get("VariableName"): v for v in method_vars}
    out = []
    for r in variable_rows(run):
        if r.kind not in VARIABLE_KINDS:
            continue
        m = flags.get(r.variable) or {}
        out.append(OrderedDict([("variable", r.variable), ("value", r.value), ("number", r.number), ("unit", r.unit),
                                ("scouting", r.scouting), ("details_only", _bool(m.get("IsVisibleInDetailsOnly"))),
                                ("source", "run")]))
    return out


def _scouting(method, documentation):
    flagged = [v.get("VariableName") for v in _list((method.get("Variables") or {}).get("Variable"))
               if _bool(v.get("IsScoutingVariable"))]
    runs = []
    scouting_runs = method.get("ScoutingRuns")
    for sr in _list(scouting_runs.get("ScoutingRun") if isinstance(scouting_runs, dict) else None):
        data = _list((sr.get("ScoutingRunDatas") or {}).get("ScoutingRunData"))
        runs.append(OrderedDict([("position", _number(sr.get("MethodPos"))), ("included", _bool(sr.get("IsIncluded"))),
                                 ("values", OrderedDict((d.get("VariableName"), _text(d.get("ReadableValue")))
                                                        for d in data))]))
    setting = (documentation.get("ResultSetting") or {}) if isinstance(documentation, dict) else {}
    return OrderedDict([("variables", flagged), ("runs", runs), ("name", _text(setting.get("ScoutingName"))),
                        ("source", "method")])


def _documentation(doc):
    if not isinstance(doc, dict):
        return OrderedDict()
    columns = doc.get("Columns")
    names = [_text(c.get("ColumnName")) for c in _list(columns.get("Column") if isinstance(columns, dict) else None)]
    texts = OrderedDict((p, t) for p, t in _leaves(doc.get("PhaseInformation")) if "@" not in p)
    start = doc.get("StartProtocol") or {}
    items = [_text(i.get("StartProtocolItem")) if isinstance(i, dict) else _text(i)
             for i in _list(start.get("StartProtocolItems"))] if isinstance(start, dict) else []
    questions = OrderedDict((p, t) for p, t in _leaves(doc.get("Questions")) if "@" not in p and t != "None")
    return OrderedDict([("columns", [n for n in names if n]), ("phase_texts", texts),
                        ("start_protocol", [i for i in items if i]), ("questions", questions),
                        ("folder", _text((doc.get("ResultSetting") or {}).get("FolderPath"))
                         if isinstance(doc.get("ResultSetting"), dict) else None),
                        ("source", "method")])


def _dialogs(blocks, volume_unit):
    out = []
    for b in blocks.values():
        phase = b.raw.get("MethodPhase")
        props = phase.get("ConfigurationProperties") if isinstance(phase, dict) else None
        if not isinstance(props, str) or "mSampleVolumeForSuperloop" not in props and "mLoopType" not in props:
            continue
        raw = OrderedDict(DIALOG_FIELD.findall(props))
        row = OrderedDict([("block", b.name), ("phase", b.phase)])
        for stored, name in DIALOG_NAMES.items():
            value = raw.get(stored)
            if name in ("sample_volume", "fill_loop_with", "empty_loop_with", "max_volume"):
                value = _number(value)
            elif name in ("loop_loading", "inject_all_sample", "fixed_volume", "set_max_volume"):
                value = _bool(value)
            row[name] = value
        row["volume_unit"] = volume_unit
        row["raw"] = raw
        row["source"] = "method"
        out.append(row)
    return out


def _buffer_prep(method, documentation, warnings):
    if not _bool(method.get("IsBufferPrepEnabled")):
        return None
    leaves = []
    for root in (method, documentation if isinstance(documentation, dict) else {}):
        for path, text in _leaves(root):
            if BUFFERPRO_ROOT.search(path) and "IsBufferPrepEnabled" not in path:
                leaves.append((path, text))

    def pick(pattern, among=leaves):
        rx = re.compile(pattern, re.I)
        return next((t for p, t in among if rx.search(p.rsplit("/", 1)[-1])), None)

    stock = OrderedDict()
    for path, text in leaves:
        m = re.match(r"(.*%s[^/]*?(?:\[\d+\])?)/(.*)$" % STOCK.pattern, path, re.I)
        if m:
            stock.setdefault(m.group(1), []).append((m.group(2), text))
    solutions = []
    for items in stock.values():
        solutions.append(OrderedDict([("inlet", pick(r"inlet|position", items)),
                                      ("concentration_M", _number(pick(r"concentration", items))),
                                      ("substance", pick(r"substance|name", items)),
                                      ("description", pick(r"description", items)),
                                      ("raw", OrderedDict(items))]))
    out = OrderedDict([("enabled", True), ("final_concentration", _number(method.get("BufferFinalConcentration"))),
                       ("recipe", pick(r"recipe.*name|^name$")), ("ph", _number(pick(r"^(target)?ph$"))),
                       ("buffer_concentration", _number(pick(r"buffer.*conc"))),
                       ("salt_concentration", _number(pick(r"salt.*conc"))),
                       ("stock_solutions", solutions), ("raw", OrderedDict(leaves)), ("source", "method")])
    warnings.append("BufferPro is on: recipe fields matched by name, not checked against a real run (layout "
                    "unknown); raw fields kept. Hints only, never buffer concentrations (D3)")
    return out


def method_hints(run, chrom=None, sections=None):
    """
    `MethodHints` for `chrom` (default: the run's first chromatogram). `sections`: the
    `run_log_sections(run, chrom)['sections']` already built, to save a second pass.
    """
    md = run.metadata.get("MethodData")
    method = md.get("Method") if isinstance(md, dict) else None
    if not isinstance(method, dict):
        return MethodHints(available=False, warnings=["no MethodData: no method hints"] if md is None
                           else ["MethodData has no Method element: layout unknown, not read"])
    chrom = chrom or run.chromatogram()
    if sections is None:
        sections = run_log_sections(run, chrom)["sections"]
    hints = MethodHints(available=True)
    warnings = hints.warnings
    if not run.holds_run_of(chrom):  # W17: an imported chromatogram's run is another result's
        from .model import this_result_note
        warnings.append(this_result_note(run, "the method"))
    volume_unit = _text(method.get("NormalVolumeUnit")) or "ml"
    hints.method = OrderedDict([("name", _text(method.get("Description"))),
                                ("unicorn_version", method.get("@UNICORNVersion")),
                                ("technique", _text(method.get("TechniqueName"))), ("volume_unit", volume_unit),
                                ("flow_unit", _text(method.get("NormalFlowUnit"))), ("source", "method")])
    used = OrderedDict()
    for r in variable_rows(run):
        if r.kind in VARIABLE_KINDS and (r.variable not in used or r.scouting):
            used[r.variable] = r
    method_vars = _list((method.get("Variables") or {}).get("Variable"))
    documentation = run.metadata.get("MethodDocumentationData")
    documentation = documentation.get("MethodDocumentation") if isinstance(documentation, dict) else None
    hints.variables = _variables(run, method_vars)
    hints.scouting = _scouting(method, documentation)
    hints.documentation = _documentation(documentation)
    hints.buffer_prep = _buffer_prep(method, documentation, warnings)

    blocks = OrderedDict()
    for raw in _list((method.get("Blocks") or {}).get("Block")):
        b = _Block(raw, used, warnings)
        blocks[b.name] = b
    main = next((b for b in blocks.values() if b.main), None)

    def base(b):
        kind, vc = b.base()
        if kind == "SameAsMain" and main is not None and b is not main:
            kind, vc = main.base()
        return kind, vc

    hints.sample_application = _dialogs(blocks, volume_unit)
    entries = executed_entries(chrom)
    runs = _block_runs(entries)
    log, to_s = _event_seconds(chrom)
    next_bp = [e.time * to_s for e in (log.events if log is not None else [])
               if e.time is not None and (e.text or "").startswith("Next breakpoint")]
    ran = {name for name, _, _ in runs}
    hints.blocks_not_run = [b.name for b in blocks.values()
                            if not b.main and not b.unused and not b.phase and b.name not in ran]

    for name, t0, t1 in runs:
        b = blocks.get(name)
        if b is None:
            continue
        kind, _ = base(b)
        unit = _base_unit(kind, volume_unit)
        nb = any(t0 <= t <= t1 for t in next_bp)
        if b.length and unit:
            got, ml = _amount(sections, t0, t1, kind)
            row = OrderedDict([("block", name), ("start_s", _wall(sections, t0)), ("end_s", _wall(sections, t1)),
                               ("base", unit), ("planned", b.length), ("variable", b.length_variable),
                               ("executed", got), ("executed_ml", ml), ("status", None), ("cause", None)])
            if got is not None:
                rel = (got - b.length) / b.length
                row["status"] = "as_planned" if abs(rel) <= BLOCK_TOLERANCE else "shorter" if rel < 0 else "longer"
                if row["status"] == "shorter":
                    row["cause"] = "next_breakpoint" if nb else "watch_in_block" if _has_watch(b) else None
                elif row["status"] == "longer":
                    held = any(s.held or s.paused for s in sections
                               if s.elapsed_end_s > t0 and s.elapsed_start_s < t1)
                    row["cause"] = "hold_or_pause" if held or b.has(HOLD_INSTRUCTIONS) else None
            hints.blocks.append(row)
        for position, start, end, end_at in b.planned_injections():
            hints.injections.append(_injection(sections, b, name, t0, t1, kind, unit, position, start, end, end_at,
                                               nb, warnings))
    _check_dialogs(hints, blocks, warnings)
    return hints


def _has_watch(block):
    """True when the block sets a watch ('Watch UV parameters' and the like only set its parameters)."""
    return any((i.get("ReadableName") or "").startswith("Watch") and
               not (i.get("ReadableName") or "").endswith("parameters") for i in block.instructions)


def _injection(sections, b, name, t0, t1, kind, unit, position, start, end, end_at, nb, warnings):
    """Planned vs executed valve period for one run of block `name` (elapsed t0..t1)."""
    at_position = lambda s: (s.injection_valve or "").lower() == position.lower()  # noqa: E731
    inside = [s for s in sections if not s.paused and at_position(s) and s.elapsed_end_s > t0 and s.elapsed_start_s < t1]
    planned = None if start is None or end is None else end - start
    row = OrderedDict([("block", name), ("position", position), ("base", unit), ("planned", planned),
                       ("planned_start", start), ("planned_end", end), ("planned_end_at", end_at),
                       ("variable", next((g["variable"] for g in b.groups if g["value"] == end and g["variable"]), None)),
                       ("start_s", None), ("end_s", None), ("executed", None), ("executed_ml", None),
                       ("bypass_ml", None), ("pre_switch_s", None), ("pre_switch", None), ("gap_allowance", None),
                       ("status", None),
                       ("next_breakpoint", nb), ("source", "method")])
    if not inside:
        row["status"] = "not_executed"
        warnings.append("block %r ran at %.1f s but the injection valve never reached %s in it (planned %s %s)"
                        % (name, _wall(sections, t0), position, planned, unit))
        return row
    v0 = max(t0, inside[0].elapsed_start_s)
    v1 = min(t1, inside[-1].elapsed_end_s)
    got, ml = _amount(sections, v0, v1, kind, keep=at_position)
    _, bypass_ml = _amount(sections, v0, v1, kind, keep=lambda s: at_position(s) and s.bypass)
    pre, pre_ml = _amount(sections, t0, v0, kind)
    row.update([("start_s", _wall(sections, v0)), ("end_s", _wall(sections, v1)), ("executed", got),
                ("executed_ml", ml), ("bypass_ml", bypass_ml), ("pre_switch_s", v0 - t0),
                ("pre_switch", None if pre is None or start is None else pre - start)])
    if position.lower() != "inject" or planned is None or got is None or unit is None:
        return row  # direct inject (sample pump) and unknown bases: reported, not checked
    if row["pre_switch"] is None:
        return row  # CV base without a logged Vc
    # the measured gap, plus TOLERANCE of the plan, at least MIN_SHORT_ML (in the base: ml scaled as the valve period)
    gap = max(row["pre_switch"], 0.0)
    slack = max(TOLERANCE * planned, MIN_SHORT_ML * got / ml if ml else 0.0)
    row["gap_allowance"] = gap + slack
    if got < planned - gap - slack - 1e-9:
        row["status"] = "short"
    elif got > planned * (1 + TOLERANCE) + 1e-9:
        row["status"] = "over"
    else:
        row["status"] = "as_planned"
    if row["status"] != "as_planned":
        warnings.append("injection in block %r at %.1f-%.1f s: valve at %s for %.4g %s, planned %.4g %s%s "
                        "(pre-switch gap %.3g %s; allowed %.3g %s short, %g %% over)%s; the feed is the logged "
                        "valve period (D20)"
                        % (name, row["start_s"], row["end_s"], position, got, unit, planned, unit,
                           " (variable %r)" % row["variable"] if row["variable"] else "", gap, unit,
                           gap + slack, unit, TOLERANCE * 100, "; Next breakpoint in the block" if nb else ""))
    return row


def _check_dialogs(hints, blocks, warnings):
    """Sample application dialog volume vs the planned valve period of the blocks the phase calls."""
    for d in hints.sample_application:
        if d["sample_volume"] is None:
            continue
        todo, seen, planned = [d["block"]], set(), []
        while todo:
            b = blocks.get(todo.pop(0))
            if b is None or b.name in seen:
                continue
            seen.add(b.name)
            planned += [r for r in hints.injections if r["block"] == b.name and r["planned"] is not None
                        and r["base"] == d["volume_unit"]]
            todo += b.calls()
        d["planned_injection"] = planned[0]["planned"] if planned else None
        if planned and abs(planned[0]["planned"] - d["sample_volume"]) > 1e-6:
            warnings.append("phase %r: dialog sample volume %g %s differs from the planned valve period %g %s of "
                            "block %r; the block breakpoint is what ran"
                            % (d["phase"] or d["block"], d["sample_volume"], d["volume_unit"], planned[0]["planned"],
                               d["volume_unit"], planned[0]["block"]))
