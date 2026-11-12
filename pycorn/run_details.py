# -*- coding: utf-8 -*-
"""
Run-level tables of what a result stores besides curves and events (owner, 2026-11-09): the
method, the instrument's calibration, the instrument itself, its system settings and the audit
log. Every export path writes them: csv files, xlsx sheets, the in-browser page. Read the same
way from a .zip, a .UFol member or the database (`run.metadata`, `run.result`,
`run.evaluation_log`). Field names are taken as stored, never from a fixed list; a table the
run has no data for is empty.

Method information is laid out as UNICORN's own report prints it (0.31.0): the method text
in call order and indented, the variable list by phase and block, the column type in its
three groups, calibrations as references then constants, dates with their UTC offset.

- `run_details`: section, field, value. Result (name, system, created / modified with their
  UTC offset and by whom, run type, the notes as the report names them: Method, Start, Run,
  Evaluation notes) and method (name, dates, technique, units, flags, scouting name,
  start-protocol items, result folder and naming, the naming variable, documentation), then
  the method's evaluation procedures ('selected' or 'not selected').
- `method_steps`: one row per instruction of the method as programmed, in the order UNICORN
  prints it: from the main block, each called block's instructions right after the call (a
  Block instruction, or a Watch whose action calls a block), one level deeper (`depth`,
  `path`, `called_from`: the calling step), with the phase of the phase block that calls it.
  `line` is UNICORN's method text ('0.00 System flow: (5.000)#Flow rate {ml/min}, ...',
  indented by depth; a breakpoint variable on the instruction that stores it: '(22.00)#SampleVol
  Injection valve: Manual load', '(5.00)#Equilibration volume End_Block'; a column volume set
  by a variable '(Vc=0.170)#ColVolume {ml}'). Also base, breakpoint (stored, and as the
  run's variables set it), instruction, parameters ('name: value unit', the linked variable
  in brackets) and the values the run used for those variables. Parameters UNICORN stores as
  hidden (`IsHidden`) are left out, as UNICORN does. The method is a template (D17, D23): the
  run log says what ran.
- `method_variables_frame(run)` (the 'variables' table of every export): UNICORN's variable
  list, by phase and block in method order, the unit with 'base' replaced by the block's base.
- `scouting_runs`: one row per scouting run of the method (`run` from 1, the stored
  `position`), a column per scouted variable in variable-list order.
- `start_questions`: the start-protocol questions: number, question, answer type, answer.
- `column`: the stored column type: run parameters, details, ordering information, then every
  other stored field; column volume and linear flows calculated from diameter and bed height.
- `calibration`: per sensor its reference values, then its constants (pH slope and asymmetry,
  cell constant, ...; pressure sensors have a date only): sensor, date as stored, its UTC
  offset, UTC, operator, locally calibrated, kind, position, item, value, unit.
- `instrument`: group, item, value, unit. Instrument configuration (model, firmware, library
  versions), strategy, system (name, computer, serial, ...), mixer, UV cell (nominal and real
  path length), every module's statistics (serial, versions, run hours, pump strokes) and the
  enabled components. IP addresses and ports are never written (owner, 2026-11-09).
- `system_settings`: group, instruction, parameter, value (alarm limits, wavelengths, ...).
- `audit_log`: one row per audit-trail entry: trail, time as stored, UTC offset, UTC, type,
  group, operator, entry.

`Masker(run)` replaces what identifies people, hosts and instruments (logins, computer name,
system name, serial numbers) with User1, Computer1, System1, Serial1, ... in every text cell
of every table (`pycorn export --mask-identifying`). Built-in accounts (Default, System) stay.
Free text people type, which may name anyone (the method, start, run and evaluation notes,
start-protocol answers), is replaced as a whole by `FREE_TEXT_MASK`.
"""
import base64
import binascii
import datetime as dt
import re
import xml.etree.ElementTree as ET
from collections import OrderedDict

from .method_hints import (_Block, _base_unit, _bool, _called_block, _documentation, _list, _number, _params, _scouting,
                           _text)
from .variables import VARIABLE_KINDS, parse_number
from .variables import rows as variable_rows

TABLES = ("run_details", "method_steps", "scouting_runs", "start_questions", "column", "calibration", "instrument",
          "system_settings", "audit_log")
# never written: ICUIPAddress, VCUIpAddress, RemotingPort, VIDPort, ...; 'IP address' / 'port' items
NETWORK = re.compile(r"(IP|Ip)\s?[Aa]ddress$|Port$|\b(?i:ip address|port)\b")
BASE64_ITEMS = ("MixerInformation", "UVInformation", "OperationalStatistics")
BUILTIN_ACCOUNTS = {"default", "system"}
MIN_SERIAL = 4
SERIAL = re.compile(r"serial", re.I)
METHOD_RUN = re.compile(r"Method Run .*? User (.*?) at computer (.*?) is in control")
FREE_TEXT_MASK = "(free text, masked)"  # a note or start-protocol answer under --mask-identifying


def _doc(run, key, root):
    """`run.metadata[key][root]` as a dict, else {}."""
    data = run.metadata.get(key)
    data = data.get(root) if isinstance(data, dict) else None
    return data if isinstance(data, dict) else {}


def _scalar(value):
    return value is not None and not isinstance(value, (dict, list)) and str(value) not in ("", "None")


def _number_or_text(text):
    n = parse_number(text) if isinstance(text, str) else float("nan")
    return text if n != n else n


def _utc(local, offset_minutes):
    """Naive UTC from a stored local time text and its UTC offset in minutes, else None."""
    try:
        when = dt.datetime.fromisoformat(str(local))
        return when - dt.timedelta(minutes=float(offset_minutes))
    except (TypeError, ValueError):
        return None


def _b64_xml(text):
    """The XML root that base64 `text` holds, else None."""
    try:
        raw = base64.b64decode(re.sub(r"\s+", "", str(text)), validate=True)
    except (binascii.Error, ValueError):
        return None
    for payload in (raw, re.sub(rb"^\s*<\?xml[^>]*\?>", b"", raw)):  # UTF-8 text that declares utf-16
        try:
            return ET.fromstring(payload)
        except ET.ParseError:
            pass
    return None


def _attr(el, name):
    return next((v for k, v in el.attrib.items() if k.lower() == name), None)


def _frame(rows, columns):
    import pandas as pd
    return pd.DataFrame(rows, columns=columns)


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

def local_time_text(doc, key):
    """
    `doc[key]`, a stored local time, with the UTC offset stored next to it (`<key>UtcOffsetMinutes`)
    as UNICORN prints it: '2025-11-18T11:17:39.581 -05:00'; the stored text alone without one.
    """
    value = doc.get(key) if isinstance(doc, dict) else None
    if not _scalar(value):
        return None
    try:
        minutes = int(float(str(doc.get(key + "UtcOffsetMinutes"))))
    except (TypeError, ValueError):
        return str(value)
    sign = "-" if minutes < 0 else "+"
    return "%s %s%02d:%02d" % (value, sign, abs(minutes) // 60, abs(minutes) % 60)


DATE_FIELDS = ("Created", "LastModified")
# the result's notes (`ResultDocumentation`) as UNICORN's report names them, in its order
NOTE_LABELS = OrderedDict([("MethodNote", "Method notes"), ("StartNote", "Start notes"), ("RunNote", "Run notes"),
                           ("EvaluationNote", "Evaluation notes")])


def _camel_words(text):
    """'MethodName' -> 'Method name'."""
    words = re.findall(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+", str(text))
    return " ".join([words[0]] + [w.lower() if not w.isupper() else w for w in words[1:]]) if words else str(text)


def _result_naming(setting):
    """UNICORN's 'Result name' line: 'Method name, without unique identifier', from `ResultSetting`."""
    kind = setting.get("ResultSettingUniqueIdentifier")
    kind = kind.get("@ResultSettingUniqueIdentifierType") if isinstance(kind, dict) else None
    name = _text(setting.get("ResultName"))
    head = ", ".join(x for x in (name, _camel_words(kind) if kind else None) if x)
    unique = _bool(setting.get("IsAddUniqueIdentifier"))
    if unique is None:
        return head or None
    return "%s%s unique identifier" % (head + ", " if head else "", "with" if unique else "without")


def run_details_frame(run):
    """section, field, value: the result and the method (see the module docstring)."""
    rows = []
    result = run.result or {}
    for field, key in (("name", "Name"), ("system", "SystemName"), ("system type", "SystemTypeName"),
                       ("folder", "FolderPath"), ("batch id", "BatchId"), ("run type", "RunType"),
                       ("result purpose", "ResultPurpose"), ("state", "ResultState"), ("created", "Created"),
                       ("created by", "CreatedBy"), ("last modified", "LastModified"),
                       ("last modified by", "LastModifiedBy")):
        if _scalar(result.get(key)):
            rows.append(("result", field, local_time_text(result, key) if key in DATE_FIELDS else str(result[key])))
    notes = result.get("ResultDocumentation")
    notes = notes if isinstance(notes, dict) else {}
    for key in sorted(notes, key=lambda k: list(NOTE_LABELS).index(k) if k in NOTE_LABELS else len(NOTE_LABELS)):
        if _scalar(notes[key]):  # as UNICORN's report: Method notes, Start notes, Run notes, Evaluation notes
            rows.append(("result", NOTE_LABELS.get(key, key), str(notes[key])))
    method = _doc(run, "MethodData", "Method")
    for field, key in (("name", "Description"), ("system", "SystemName"), ("created", "Created"),
                       ("created by", "CreatedBy"), ("last modified", "LastModified"),
                       ("last modified by", "LastModifiedBy"), ("UNICORN version", "@UNICORNVersion"),
                       ("technique", "TechniqueName"), ("volume unit", "NormalVolumeUnit"),
                       ("flow unit", "NormalFlowUnit"), ("pressure unit", "NormalPressureUnit"),
                       ("CIP method", "IsCIP"), ("HETP method", "IsHETP"),
                       ("performance test", "IsPerformanceTest"), ("BufferPro on", "IsBufferPrepEnabled"),
                       ("buffer final concentration", "BufferFinalConcentration")):
        if _scalar(method.get(key)):
            rows.append(("method", field, local_time_text(method, key) if key in DATE_FIELDS else str(method[key])))
    documentation = _doc(run, "MethodDocumentationData", "MethodDocumentation")
    if documentation:
        doc = _documentation(documentation)
        setting = documentation.get("ResultSetting")
        if isinstance(setting, dict) and _scalar(setting.get("ScoutingName")):
            rows.append(("method", "scouting name", str(setting["ScoutingName"])))
        if isinstance(setting, dict):  # UNICORN's 'Result name and location'
            if _scalar(setting.get("FolderPath")):
                rows.append(("method", "result folder", str(setting["FolderPath"])))
            naming = _result_naming(setting)
            if naming:
                rows.append(("method", "result name", naming))
            kind = setting.get("ResultSettingUniqueIdentifier")
            variable = _text(kind.get("VariableName")) if isinstance(kind, dict) else None
            if variable:  # 'Result name: RunName (Type: Variable)'
                rows.append(("method", "result name variable", variable))
        if doc.get("start_protocol"):
            rows.append(("method", "start protocol", ", ".join(doc["start_protocol"])))
        if doc.get("columns"):
            rows.append(("method", "columns", ", ".join(doc["columns"])))
        procedures = documentation.get("EvaluationProcedures")
        for p in _list(procedures.get("EvaluationProcedure") if isinstance(procedures, dict) else None):
            if not isinstance(p, dict):
                continue
            name = _text(p.get("DisplayName")) or _text(p.get("EvaluationProcedureName"))
            if name:  # UNICORN's report: '<procedure name> {Selected}'
                rows.append(("evaluation procedure", name, "selected" if _bool(p.get("IsSelected")) else "not selected"))
        for path, text in doc.get("phase_texts", {}).items():
            rows.append(("method documentation", path, text))
    return _frame(rows, ["section", "field", "value"])


QUESTION_COLUMNS = ["number", "question", "answer_type", "answer", "stored_answer_type", "mandatory",
                    "requires_signature", "visible_in_chromatogram", "method_position"]
# answer types as UNICORN's report prints them; others are given as stored
ANSWER_TYPES = {"InputField": "Textual input"}


def start_questions_frame(run):
    """
    The method's start-protocol questions, one row each, as UNICORN's report lists them
    ('No. 1: ...', 'Answer type: Textual input'): number (from 1, in stored order), the
    question, its answer type (as the report prints it; the stored name where the report's
    text is not known), and the answer the run was started with (the result's
    `QuestionAnswer` search criterion of that question; empty when none was given), then
    the stored flags.
    """
    documentation = _doc(run, "MethodDocumentationData", "MethodDocumentation")
    questions = documentation.get("Questions")
    questions = _list(questions.get("Question")) if isinstance(questions, dict) else []
    answers = {}
    for r in variable_rows(run):
        if r.kind == "QuestionAnswer" and r.variable not in answers:
            answers[r.variable] = r.value if r.value not in ("", None) else None
    rows = []
    for q in questions:
        if not isinstance(q, dict):
            continue
        name = _text(q.get("Name"))
        answer = q.get("Answer")
        stored = answer.get("@AnswerType") if isinstance(answer, dict) else None
        flag = lambda key: _bool(q.get(key)) if q.get(key) is not None else None  # noqa: E731
        rows.append([len(rows) + 1, name, ANSWER_TYPES.get(stored, stored), answers.get(name), stored,
                     flag("IsMandatory"), flag("IsRequiresSignature"), flag("IsVisibleInChromatogram"),
                     _number(q.get("MethodPos"))])
    return _frame(rows, QUESTION_COLUMNS)


def _used_variables(run):
    used = OrderedDict()
    for r in variable_rows(run):
        if r.kind in VARIABLE_KINDS and (r.variable not in used or r.scouting):
            used[r.variable] = r
    return used


# the page's default columns first, in the order shown (export_tables), then the details
STEP_COLUMNS = ["step", "phase", "block", "line", "depth", "path", "called_from", "base", "breakpoint",
                "breakpoint_variable",
                "breakpoint_used", "instruction", "parameters", "run_values", "main_block", "unused"]
INDENT = "    "  # one level of the method text, as UNICORN's report indents it


def _method_blocks(run):
    """(blocks, main, base unit by block name, volume unit) of the method as programmed."""
    method = _doc(run, "MethodData", "Method")
    volume_unit = _text(method.get("NormalVolumeUnit")) or "ml"
    blocks = [_Block(raw, _used_variables(run), []) for raw in _list((method.get("Blocks") or {}).get("Block"))]
    main = next((b for b in blocks if b.main), None)
    bases = {}
    for b in blocks:
        kind = b.base()[0]
        if kind == "SameAsMain" and main is not None and b is not main:
            kind = main.base()[0]
        bases[b.name] = _base_unit(kind, volume_unit) or kind
    return blocks, main, bases, volume_unit


def _method_order(blocks, main):
    """
    (block, depth, phase, path, group, instruction, called_from) in the order UNICORN prints
    the method: from the main block, each called block's instructions right after its call
    (a Block instruction, or a Watch whose action calls a block), one level deeper; a block
    takes the phase of the phase block that calls it. `called_from` is the 1-based place, in
    this order, of the instruction that called the block (None at depth 0). Blocks no call
    reaches follow, in stored order.
    """
    by_name = {b.name: b for b in blocks}
    reached = set()
    count = [0]

    def walk(b, depth, phase, path, stack, caller):
        reached.add(b.name)
        phase = b.phase or phase
        for group in b.groups:
            for ins in group["instructions"]:
                count[0] += 1
                here = count[0]
                yield b, depth, phase, path, group, ins, caller
                callee = by_name.get(_called_block(ins))
                if callee is not None and callee.name not in stack:
                    yield from walk(callee, depth + 1, phase, path + " > " + callee.name, stack | {callee.name}, here)

    for start in ([main] if main is not None else []) + blocks:
        if start.name not in reached:
            yield from walk(start, 0, None, start.name, {start.name}, None)


def _breakpoint_text(stored):
    n = parse_number(stored) if isinstance(stored, str) else float("nan")
    return "%.2f" % n if n == n else (stored or "")


def _instruction_text(ins, by_name):
    """One instruction as UNICORN's method text prints it: 'System flow: (5.000)#Flow rate {ml/min}, ...'."""
    name = ins.get("ReadableName") or ""
    if name == "Block":  # a call: 'Phase: Equilibration' for a phase block, else 'Block: Wash'
        callee = by_name.get((_params(ins).get("Block") or (None,))[0])
        if callee is not None:
            return "Phase: %s" % callee.phase if callee.phase else "Block: %s" % callee.name
    parts = []
    for pname, (value, unit, var) in _params(ins, hidden=False).items():
        if value is None and var is None:
            continue
        vc = "Vc=" if name == "Base" and pname == "Volume" else ""  # '(Vc=0.170)#ColVolume', 'Vc=5.027'
        text = "(%s%s)#%s" % (vc, value or "", var) if var else vc + (value or "")
        parts.append(text + (" {%s}" % unit if unit else ""))
    return name + (": " + ", ".join(parts) if parts else "")


def method_steps_frame(run):
    """One row per instruction of the method as programmed, in UNICORN's order (see the module docstring)."""
    blocks, main, bases, _ = _method_blocks(run)
    by_name = {b.name: b for b in blocks}
    used = _used_variables(run)
    rows = []
    for b, depth, phase, path, group, ins, called_from in _method_order(blocks, main):
        params, run_values = [], []
        for name, (value, unit, var) in _params(ins, hidden=False).items():
            if value is None and var is None:
                continue
            text = "%s: %s" % (name, " ".join(x for x in (value, unit) if x))
            if var:
                text += " [%s]" % var
                r = used.get(var)
                if r is not None:
                    run_values.append("%s = %s" % (var, " ".join(x for x in (r.value, r.unit) if x)))
            params.append(text)
        text = _instruction_text(ins, by_name)
        if ins.get("ReadableName") == "Block":  # 'Phase: Equilibration' belongs to the phase it opens
            callee = by_name.get((_params(ins).get("Block") or (None,))[0])
            phase = callee.phase if callee is not None and callee.phase else phase
        # the breakpoint variable on the instruction that stores it, as UNICORN prints it:
        # '(22.00)#SampleVol Injection valve: Manual load', '(5.00)#Equilibration volume End_Block'
        if ins.get("BreakPointVariable"):
            line = "(%s)#%s %s" % (_breakpoint_text(group["stored"]), ins["BreakPointVariable"], text)
        else:
            line = "%s %s" % (_breakpoint_text(group["stored"]), text)
        rows.append([len(rows) + 1, phase, b.name, INDENT * depth + line, depth, path, called_from, bases[b.name],
                     _number_or_text(group["stored"]), group["variable"],
                     group["value"] if group["variable"] else None, ins.get("ReadableName"), "; ".join(params),
                     "; ".join(run_values), b.main, b.unused])
    frame = _frame(rows, STEP_COLUMNS)
    frame["called_from"] = frame["called_from"].astype("Int64")  # empty at depth 0
    return frame


VARIABLE_LIST_COLUMNS = ["phase", "block", "variable", "value", "unit", "number", "kind", "scouting", "stored_unit"]


def _method_variable_order(run):
    """
    {variable: (phase, block, unit)} in UNICORN's variable-list order: as the method text
    uses them, a breakpoint variable before the parameters of the instruction that stores it
    ('(15.00)#PurgeVol System flow: (0.350)#EquiFlow'); the unit with 'base' replaced by the
    block's base.
    """
    blocks, main, bases, _ = _method_blocks(run)
    found = OrderedDict()
    for b, depth, phase, path, group, ins, _ in _method_order(blocks, main):
        var = ins.get("BreakPointVariable")
        if var and var not in found:
            found[var] = (phase, b.name, bases[b.name])
        for _, (value, unit, var) in _params(ins, hidden=False).items():
            if var and var not in found:
                found[var] = (phase, b.name, bases[b.name] if unit in ("base", "{base}") else unit)
    return found


def method_variables_frame(run):
    """
    The method variables as UNICORN's variable list shows them: in method order, each with the
    phase and block that first uses it and its unit with 'base' replaced by that block's base
    (CV, ml, min); values as the run used them (`run.variables()`, stored unit in
    `stored_unit`). Stored variables the method does not use (or a run without its method)
    follow in stored order.
    """
    found = _method_variable_order(run)
    order = {name: i for i, name in enumerate(found)}
    rows = []
    for r in sorted(variable_rows(run), key=lambda r: order.get(r.variable, len(order))):  # stable
        phase, block, unit = found.get(r.variable, (None, None, None))
        if unit is None:
            unit = r.unit or None
        if unit in ("base", "{base}"):
            unit = None
        rows.append([phase, block, r.variable, r.value, unit, r.number, r.kind, r.scouting, r.unit])
    frame = _frame(rows, VARIABLE_LIST_COLUMNS)
    frame["number"] = frame["number"].astype(float)
    frame["scouting"] = frame["scouting"].astype(bool)
    return frame


def scouting_runs_frame(run):
    """
    One row per scouting run, as UNICORN's report lists them: `run` (numbered from 1 in stored
    order, UNICORN's 'Run 1'), the stored `position` (`MethodPos`, from 0), included, then the
    value of each scouted variable, in the method's variable-list order.
    """
    method = _doc(run, "MethodData", "Method")
    documentation = _doc(run, "MethodDocumentationData", "MethodDocumentation")
    runs = _scouting(method, documentation)["runs"] if method else []
    names = []
    for r in runs:
        names += [n for n in r["values"] if n not in names]
    order = {name: i for i, name in enumerate(_method_variable_order(run))} if runs else {}
    names.sort(key=lambda n: order.get(n, len(order)))  # stable: unused names keep their stored order
    return _frame([[k + 1, r["position"], r["included"]] + [r["values"].get(n) for n in names]
                   for k, r in enumerate(runs)], ["run", "position", "included"] + names)


CALIBRATION_COLUMNS = ["calibration", "date", "utc_offset_min", "date_utc", "operator", "locally_calibrated",
                       "kind", "position", "item", "value", "unit"]
# 'Calibrated electrode slope; %': UNICORN's constant descriptions end in '; <unit>'
_DESCRIPTION_UNIT = re.compile(r"^(?P<item>.*);\s*(?P<unit>[^;\s]{1,12})$")


def calibration_frame(run):
    """
    As UNICORN's report lists a calibration: per sensor, its reference values, then its
    constants (see the module docstring); `item` names each ('Reference value 1', or the
    stored constant description without its '; <unit>' ending, which goes to `unit`).
    """
    rows = []
    for c in _list(_doc(run, "CalibrationSettingData", "CalibrationSettings").get("CalibrationSetting")):
        if not isinstance(c, dict):
            continue
        head = [_text(c.get("CalibrationName")), _text(c.get("CalibrationDate")),
                _number_or_text(_text(c.get("CalibrationDateUtcOffsetMinutes"))),
                _utc(c.get("CalibrationDate"), c.get("CalibrationDateUtcOffsetMinutes")), _text(c.get("Operator")),
                _bool(c.get("IsLocallyCalibrated")) if c.get("IsLocallyCalibrated") is not None else None]
        items = []
        for p in _list((c.get("CalibrationPointSettings") or {}).get("CalibrationPointSetting")):
            pos = _text(p.get("CalibrationPos"))
            items.append(["reference", _number_or_text(pos), "Reference value %s" % (pos or ""),
                          _number_or_text(_text(p.get("ReferenceValue"))), _text(p.get("Unit")) or None])
        for p in _list((c.get("CalibrationConstantSettings") or {}).get("CalibrationConstantSetting")):
            description = _text(p.get("ConstantDescription")) or ""
            m = _DESCRIPTION_UNIT.match(description)
            items.append(["constant", _number_or_text(_text(p.get("CalibrationPos"))),
                          m.group("item") if m else description or None,
                          _number_or_text(_text(p.get("ConstantValue"))), m.group("unit") if m else None])
        rows += [head + item for item in items] or [head + [None] * 5]
    frame = _frame(rows, CALIBRATION_COLUMNS)
    if len(frame):
        import pandas as pd
        frame["date_utc"] = pd.to_datetime(frame["date_utc"])
    return frame


COLUMN_COLUMNS = ["column", "group", "parameter", "value", "unit", "source"]
# (group, parameter, stored path, unit path) as UNICORN's report lists a column type
_COLUMN_FIELDS = [
    ("Run parameters", "Technique", "Media/TechniqueName", None),
    ("Run parameters", "Column volume", None, None),  # calculated, below
    ("Run parameters", "Max pre-column pressure", "Hardware/PreColumnPressure", "Hardware/PreColumnPressureUnit"),
    ("Run parameters", "Max delta column pressure", "DeltaColumnPressure", "DeltaColumnPressureUnit"),
    ("Run parameters", "Default flow rate", "DefaultFlowrate", "ColumnFlowrateUnit"),
    ("Run parameters", "Max flow rate", "MaxFlowrate", "ColumnFlowrateUnit"),
    ("Run parameters", "Default linear flow rate", None, None),
    ("Run parameters", "Max linear flow rate", None, None),
    ("Run parameters", "Min pH value (short term)", "Media/MinpHShortTerm", None),
    ("Run parameters", "Max pH value (short term)", "Media/MaxpHShortTerm", None),
    ("Run parameters", "Min pH value (long term)", "Media/MinpHLongTerm", None),
    ("Run parameters", "Max pH value (long term)", "Media/MaxpHLongTerm", None),
    ("Details", "Hardware diameter", "Hardware/Diameter", "Hardware/DiameterUnit"),
    ("Details", "Bed height", "BedHeight", "BedHeightUnit"),
    ("Details", "Typical loading range", None, None),
    ("Details", "Total liquid volume (Vt)", "TotalLiquidVolume", "ColumnVolumeUnitName"),
    ("Details", "Void volume (Vo)", "VoidVolume", "ColumnVolumeUnitName"),
    ("Details", "Typical peak width at base", "TypicalWidthAtBase", "TypicalWidthAtBaseUnit"),
    ("Details", "Average particle diameter", "Media/AverageParticleDiameter", "Media/AverageParticleDiameterUnit"),
    ("Ordering information", "Name", "Name", None),
    ("Ordering information", "Code number", "ArticleNumber", None),
    ("Ordering information", "Resin name", "Media/Name", None),
    ("Ordering information", "Resin code number", "Media/ArticleNumber", None),
    ("Ordering information", "Hardware name", "Hardware/Name", None),
    ("Ordering information", "Hardware code number", "Hardware/ArticleNumber", None),
]
_LOADING = "ColumnTypicalLoadingRanges/ColumnTypicalLoadingRange"
OTHER_GROUP = "Other (as stored)"


def _get(node, path):
    for part in path.split("/"):
        node = node.get(part) if isinstance(node, dict) else None
    return node


def _unit(text):
    """A stored unit name as UNICORN prints it: 'ml_per_min' -> 'ml/min'."""
    return str(text).replace("_per_", "/") if _scalar(text) else None


def _column_rows(ct):
    """[(group, parameter, value, unit, source)] of one stored column type."""
    from .method_hints import _leaves
    used, rows = {"PackingData", "Media/PackingData"}, []
    diameter, height = _number(_text(_get(ct, "Hardware/Diameter"))), _number(_text(_get(ct, "BedHeight")))
    cm = _get(ct, "Hardware/DiameterUnit") == "cm" and _get(ct, "BedHeightUnit") == "cm"
    area = 3.141592653589793 * (diameter / 2) ** 2 if cm and diameter and height else None
    for group, parameter, path, unit_path in _COLUMN_FIELDS:
        if parameter == "Column volume":
            if area:
                rows.append((group, parameter, area * height, "ml",
                             "calculated: pi x (Hardware/Diameter / 2)^2 x BedHeight"))
            continue
        if parameter.endswith("linear flow rate"):
            key = "DefaultFlowrate" if parameter.startswith("Default") else "MaxFlowrate"
            flow = _number(_text(ct.get(key)))
            if area and flow is not None and _unit(ct.get("ColumnFlowrateUnit")) == "ml/min":
                rows.append((group, parameter, flow / area * 60, "cm/h",
                             "calculated: %s / (pi x (Hardware/Diameter / 2)^2) x 60" % key))
            continue
        if parameter == "Typical loading range":
            for r in _list(_get(ct, _LOADING)):
                lo, hi = _text(r.get("TypicalLoadingRangeMin")), _text(r.get("TypicalLoadingRangeMax"))
                if lo or hi:
                    rows.append((group, parameter, "%s-%s" % (lo or "", hi or ""),
                                 _unit(r.get("TypicalLoadingRangeUnit")), _LOADING))
            used.update(_LOADING + "/" + k for k in ("TypicalLoadingRangeMin", "TypicalLoadingRangeMax",
                                                    "TypicalLoadingRangeUnit"))
            continue
        value = _text(_get(ct, path))
        used.update(p for p in (path, unit_path) if p)
        if value is not None and value != "":
            rows.append((group, parameter, _number_or_text(value), _unit(_get(ct, unit_path)) if unit_path else None,
                         path))
    for path, text in _leaves(ct):
        if re.sub(r"\[\d+\]", "", path) not in used and not path.startswith("@") and text not in ("", "None"):
            rows.append((OTHER_GROUP, path, _number_or_text(text), None, path))
    return rows


def column_frame(run):
    """
    The column types stored with the run, laid out as UNICORN's report lists them: run
    parameters, details, ordering information, then every other stored field ('Other (as
    stored)'; the packing instructions left out). Column volume and linear flows are
    calculated from diameter and bed height (`source` says how); every other value is stored.
    """
    rows = []
    for ct in _list(_doc(run, "ColumnTypeData", "ColumnTypes").get("ColumnType")):
        if isinstance(ct, dict):
            rows += [[_text(ct.get("Name"))] + list(r) for r in _column_rows(ct)]
    return _frame(rows, COLUMN_COLUMNS)


def _items(root):
    """(group, item, value, unit) of <Item Name= Value= Unit=> elements, by <Module name=> when present."""
    out = []
    modules = [m for m in root.iter() if m.tag == "Module"]
    for module, group in ([(m, _attr(m, "name")) for m in modules] or [(root, None)]):
        for item in module.iter("Item"):
            out.append((group, _attr(item, "name"), _attr(item, "value"), _attr(item, "unit") or None))
    return out


def instrument_frame(run):
    """group, item, value, unit (see the module docstring); never IP addresses or ports."""
    rows = []
    config = _doc(run, "InstrumentConfigurationData", "InstrumentConfigurationDesc")
    rows += [("instrument configuration", k, str(v), None) for k, v in config.items()
             if not k.startswith("@") and _scalar(v)]
    strategy = _doc(run, "StrategyData", "StrategyInformation")
    rows += [("strategy", k, str(v), None) for k, v in strategy.items() if not k.startswith("@") and _scalar(v)]
    system = _doc(run, "SystemData", "Systems").get("System")
    system = system if isinstance(system, dict) else {}
    for k, v in system.items():
        if NETWORK.search(k) or k in BASE64_ITEMS or k.startswith("@xmlns") or not _scalar(v):
            continue
        rows.append(("system", k.lstrip("@"), str(v), None))
    for key, group in (("MixerInformation", "mixer"), ("UVInformation", "UV"), ("OperationalStatistics", None)):
        root = _b64_xml(system[key]) if _scalar(system.get(key)) else None
        if root is not None:
            rows += [(g or group, item, value, unit) for g, item, value, unit in _items(root)]
    components = (system.get("EnabledComponents") or {}) if isinstance(system.get("EnabledComponents"), dict) else {}
    for c in _list(components.get("EnabledComponent")):
        if isinstance(c, dict) and _bool(c.get("IsEnabled")):
            rows.append(("enabled components", _text(c.get("Name")), "enabled", None))
    return _frame([list(r) for r in rows if not NETWORK.search(str(r[1] or ""))], ["group", "item", "value", "unit"])


def system_settings_frame(run):
    """group, instruction, parameter, value of the system settings the run was started with."""
    rows = []
    for g in _list(_doc(run, "SystemSettingData", "SystemSettings").get("InstructionGroup")):
        for ins in _list(g.get("Instructions")):
            for p in _list(ins.get("Parameters")):
                text = _text(p.get("Parameter") if isinstance(p, dict) else p) or ""
                name, _, value = text.partition("\t")
                rows.append([_text(g.get("InstructionGroupName")), _text(ins.get("Instruction")), name, value])
    return _frame(rows, ["group", "instruction", "parameter", "value"])


def audit_log_frame(run):
    """One row per audit-trail entry of the result (`run.evaluation_log`)."""
    columns = ["trail", "time", "utc_offset_min", "time_utc", "type", "group", "operator", "entry", "has_details"]
    rows = []
    for trail in run.evaluation_log or []:
        if not isinstance(trail, dict):
            continue
        for e in _list(trail.get("AuditTrailEntries")):
            offset = e.get("LogTimeUtcOffsetMinutes")
            rows.append([trail.get("AuditTrailType"), e.get("LogTime"), offset, _utc(e.get("LogTime"), offset),
                         e.get("AuditTrailEntryType"), e.get("GroupName"), e.get("Operator"), e.get("LogEntry"),
                         e.get("HasDetails")])
    frame = _frame(rows, columns)
    if len(frame):
        import pandas as pd
        frame["time_utc"] = pd.to_datetime(frame["time_utc"])
    return frame


BUILDERS = OrderedDict([("run_details", run_details_frame), ("method_steps", method_steps_frame),
                        ("scouting_runs", scouting_runs_frame), ("start_questions", start_questions_frame),
                        ("column", column_frame),
                        ("calibration", calibration_frame),
                        ("instrument", instrument_frame), ("system_settings", system_settings_frame),
                        ("audit_log", audit_log_frame)])


def summary_items(run):
    """[(label, value)] for a summary: method name and instrument model, when stored."""
    method = _doc(run, "MethodData", "Method")
    config = _doc(run, "InstrumentConfigurationData", "InstrumentConfigurationDesc")
    return [("Method", _text(method.get("Description")) or ""), ("Instrument", _text(config.get("Description")) or "")]


# ---------------------------------------------------------------------------
# Masking
# ---------------------------------------------------------------------------

class Masker:
    """
    Pseudonyms for what identifies people, hosts and instruments in `run` (see the module
    docstring). `text(value)` masks one value; `frame(df)` every text cell of a DataFrame.
    """

    def __init__(self, run):
        found = OrderedDict()

        def add(value, prefix):
            value = str(value or "").strip()
            if prefix == "Serial" and len(value) < MIN_SERIAL:
                return  # '56' would mask every 56 in the tables
            if value and value.lower() not in BUILTIN_ACCOUNTS and value not in found:
                found[value] = prefix
                if prefix == "User":
                    for part in re.split(r"[\\/@]+", value):
                        if part != value:
                            add(part, prefix)

        docs = [run.result or {}] + [v for v in run.metadata.values() if isinstance(v, dict)] + \
            [t for t in run.evaluation_log or [] if isinstance(t, dict)]
        for doc in docs:
            for key, value in _walk(doc):
                if key in ("CreatedBy", "LastModifiedBy", "Operator"):
                    add(value, "User")
                elif key == "ComputerName":
                    add(value, "Computer")
                elif key == "SystemName":
                    add(value, "System")
                elif key.endswith("SerialNumber"):
                    add(value, "Serial")
                elif key in BASE64_ITEMS:
                    root = _b64_xml(value)
                    for _, item, item_value, _ in (_items(root) if root is not None else []):
                        if SERIAL.search(item or ""):
                            add(item_value, "Serial")
        system = _doc(run, "SystemData", "Systems").get("System")
        if isinstance(system, dict):
            add(system.get("Name"), "System")
        for chrom in run.chromatograms.values():
            for ec in chrom.event_curves.values():
                for e in ec.events:
                    m = METHOD_RUN.search(e.text or "")
                    if m:
                        add(m.group(1), "User")
                        add(m.group(2), "Computer")
        # free text people type (notes, start-protocol answers) may name anyone: masked as a whole
        self._free = set()
        notes = (run.result or {}).get("ResultDocumentation")
        documentation = _doc(run, "MethodDocumentationData", "MethodDocumentation")
        for value in list(notes.values() if isinstance(notes, dict) else []) + \
                [documentation.get(k) for k in NOTE_LABELS] + \
                [r.value for r in variable_rows(run) if r.kind == "QuestionAnswer"]:
            text = str(value).strip() if _scalar(value) else ""
            if text and parse_number(text) != parse_number(text):  # a number names nobody: '5' stays everywhere
                self._free.add(text)
        counters, self.names = {}, OrderedDict()
        for value, prefix in found.items():
            counters[prefix] = counters.get(prefix, 0) + 1
            self.names[value] = "%s%d" % (prefix, counters[prefix])
        keys = sorted(self.names, key=len, reverse=True)
        self._pattern = re.compile(r"(?<![A-Za-z0-9])(%s)(?![A-Za-z0-9])" % "|".join(map(re.escape, keys)),
                                   re.I) if keys else None
        self._lower = {k.lower(): v for k, v in self.names.items()}

    def text(self, value):
        if not isinstance(value, str):
            return value
        if value.strip() in self._free:
            return FREE_TEXT_MASK
        if self._pattern is None:
            return value
        return self._pattern.sub(lambda m: self._lower[m.group(1).lower()], value)

    def deep(self, value):
        """`value` (dicts, lists, tuples) with every string masked; numpy arrays left as they are."""
        if isinstance(value, str):
            return self.text(value)
        if isinstance(value, dict):
            return type(value)((self.deep(k), self.deep(v)) for k, v in value.items())
        if isinstance(value, (list, tuple)):
            return type(value)(self.deep(v) for v in value)
        return value

    def frame(self, frame):
        if frame is None or (self._pattern is None and not self._free):
            return frame
        import pandas as pd
        text = [c for c in frame.columns if frame[c].dtype == object or pd.api.types.is_string_dtype(frame[c])]
        if not text:
            return frame
        out = frame.copy()
        for column in text:
            out[column] = [self.text(v) for v in out[column]]
        return out


def _walk(node, key=""):
    """(key, scalar) for every leaf under `node`; dict keys without '@'."""
    if isinstance(node, dict):
        for k, v in node.items():
            yield from _walk(v, str(k).lstrip("@"))
    elif isinstance(node, list):
        for v in node:
            yield from _walk(v, key)
    elif node is not None:
        yield key, node
