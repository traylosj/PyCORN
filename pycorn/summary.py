# -*- coding: utf-8 -*-
"""
What a run holds, without exporting it (Track 9, A3): `pycorn inspect [--json] [--no-mask]`.

`run_summary(run, mask=True)` returns one JSON-ready dict per run: name, source, folder, run
start, system, column, method variables, every chromatogram (kind, source result, Vc and its
source, curves with role, unit, number of points and axes, and the phase, injection and
fraction tables as positions only) and the run's warnings. Never curve values, never IP
addresses or ports (nothing from the instrument's network settings is read), never server or
database names (a database run is named by the label the caller gives, e.g. 'alias#1', and its
ResultID).

Masked by default (owner, 2026-11-11), since the output often goes to a remote model:
- people, computers, instruments and serial numbers become User1, Computer1, System1, Serial1
  (`run_details.Masker`, as `pycorn export --mask-identifying`), typed notes '(free text,
  masked)';
- run names become Run1, Run2, ..., folder paths Folder1, ... (the same name or folder, the same
  label), file names File1.zip, ... (extension kept), a .UFol member the run's label, the name
  of the result an imported chromatogram came from that run's label when it is a run of the same
  document, else Result1, ...;
- the method name becomes Method1, ... (also as the run log's 'Method: <name>'); imported and
  derived chromatograms' names Chrom1, ... (the original chromatogram's stays); curve names a
  user set (non-original curves whose name is not one UNICORN recorded in the run) Curve1, ...;
  method variable values that are text '(text, masked)' (numbers and variable names stay);
- every text (warnings, chromatogram labels, phase names, the run log's 'Result: <path>') has
  these names replaced too. Result IDs are numbers and stay; column, phase and fraction names,
  the system type and the database label stay as stored.
`mask=False` (`--no-mask`) shows everything as stored.

Labels are per document: pass one `SummaryNames` to every `run_summary` call of a document
(`SummaryNames(run_names)` with the names of the runs to come, in order, so an imported
chromatogram can point at a run summarised later). Without one, a run is masked on its own
(Run1, Folder1, ...).

`document(runs, masked, ...)` wraps summaries as the one JSON document `--json` prints:
`"schema": 1`; a key is only ever added, a renamed or removed key raises the number. The
envelope, the schema number and `plain` live in `pycorn.json_output` (shared by every
command's --json); they are imported here so the names keep working.
"""
import datetime as dt
import os
import re
from collections import OrderedDict

from .json_output import SCHEMA_VERSION, plain  # noqa: F401  (SCHEMA_VERSION: re-exported)

MIN_NAME = 3  # a shorter name is not replaced inside other texts (it would hit unrelated words and numbers)
# values that are structure, not text a person chose: never searched for names
STRUCTURE_KEYS = {"kind", "role", "data_type", "unit", "time_unit", "volume_unit", "axes", "type", "local", "utc",
                  "start_utc", "end_utc", "unicorn_version"}
RESULT_PATH = re.compile(r"(Result: )(.*?)( User |$)")
METHOD_TEXT = re.compile(r"\bMethod: (.*?)(?= Result: |$)")  # the run log's 'Method Run ... Method: <name> Result: ...'
TEXT_MASK = "(text, masked)"  # a method variable's text value in masked output


class SummaryNames:
    """
    Run, folder, file, result, method, chromatogram and curve names -> labels (Run1, Folder1,
    File1.zip, Result1, Method1, Chrom1, Curve1), shared by the summaries of one document so the
    same name gets the same label everywhere. `run_names`: names of the runs the document will
    hold, in order (labels Run1, Run2, ...).
    """

    def __init__(self, run_names=()):
        self.runs, self.folders, self.files, self.results = OrderedDict(), OrderedDict(), OrderedDict(), OrderedDict()
        self.methods, self.chroms, self.curves = OrderedDict(), OrderedDict(), OrderedDict()
        for name in run_names:
            self.run(name)

    @staticmethod
    def _text(value):
        return value.strip() if isinstance(value, str) and value.strip() else None

    def _label(self, table, value, prefix):
        value = self._text(value)
        if value is None:
            return None
        if value not in table:
            table[value] = "%s%d" % (prefix, len(table) + 1)
        return table[value]

    def run(self, name):
        return self._label(self.runs, name, "Run")

    def folder(self, path):
        return self._label(self.folders, path, "Folder")

    def method(self, name):
        return self._label(self.methods, name, "Method")

    def chromatogram(self, name):
        """An imported or derived chromatogram's name (the original chromatogram's stays)."""
        return self._label(self.chroms, name, "Chrom")

    def curve(self, name):
        """A curve name a user set (evaluated curves; curves as UNICORN recorded them stay)."""
        return self._label(self.curves, name, "Curve")

    def file(self, name):
        name = self._text(name)
        if name is None:
            return None
        if name not in self.files:
            self.files[name] = "File%d%s" % (len(self.files) + 1, os.path.splitext(name)[1])
        return self.files[name]

    def result(self, name):
        """A source result's name: the run's label when it is a run of this document, else Result1, ..."""
        name = self._text(name)
        if name is None:
            return None
        if name in self.runs:
            return self.runs[name]
        if name not in self.results:
            self.results[name] = "Result%d" % (len(self.results) + 1)
        return self.results[name]

    def _pairs(self):
        pairs = {}
        # later tables win for a name in several: runs last
        for table in (self.curves, self.chroms, self.methods, self.results, self.files, self.folders, self.runs):
            pairs.update(table)
        return pairs

    def learn(self, value):
        """Learn the names a text of the run log carries: 'Result: /folder/run', 'Method: <name>'."""
        if isinstance(value, str):
            for m in RESULT_PATH.finditer(value):
                self._path(m.group(2))
            for m in METHOD_TEXT.finditer(value):
                self.method(m.group(1))

    def text(self, value, keep=()):
        """
        `value` with every known name replaced by its label (longest first, whole words, any
        case); names in `keep` (a run's original chromatogram and curve names) are left alone.
        """
        if not isinstance(value, str) or not value:
            return value
        self.learn(value)
        # one pass over the text as stored, so a label is never replaced again
        pairs = {k: v for k, v in self._pairs().items() if len(k) >= MIN_NAME and k not in keep}
        if not pairs:
            return value
        lower = {k.lower(): v for k, v in pairs.items()}
        pattern = re.compile(r"(?<![A-Za-z0-9])(%s)(?![A-Za-z0-9])" % "|".join(
            map(re.escape, sorted(pairs, key=len, reverse=True))), re.I)
        return pattern.sub(lambda m: lower[m.group(1).lower()], value)

    def _path(self, path):
        """Learn a result path of the run log ('/folder/run'): its folder and its run name."""
        folder, _, name = path.strip().rpartition("/")
        if folder:
            self.folder(folder)
        self.run(name)

    def deep(self, value, key="", keep=()):
        """`value` (dicts, lists) with `text` on every string except structure values (`STRUCTURE_KEYS`)."""
        if isinstance(value, dict):
            return type(value)((k, self.deep(v, k, keep)) for k, v in value.items())
        if isinstance(value, list):
            return [self.deep(v, key, keep) for v in value]
        if isinstance(value, str) and key not in STRUCTURE_KEYS:
            return self.text(value, keep)
        return value


def _strings(value):
    if isinstance(value, dict):
        for v in value.values():
            yield from _strings(v)
    elif isinstance(value, list):
        for v in value:
            yield from _strings(v)
    elif isinstance(value, str):
        yield value


def _chromatogram_label(kind, source_name, source_id, copy):
    """`model.describe_chromatograms`' label, rebuilt from the masked source name."""
    if kind != "imported":
        return kind
    label = "imported from %s" % (source_name or "result %s" % (source_id if source_id is not None else "?"))
    return label + " (a copy of this run)" if copy else label


def _mask_names(summary, names, method=None):
    """
    `summary` with run, folder, file, result, method, user-set chromatogram and curve names
    replaced by `names`' labels and text variable values masked (module docstring). `method`:
    the run's method name, when known.
    """
    run_label = names.run(summary["name"])
    folder_label = names.folder(summary["folder"])
    names.method(method)
    source = summary["source"]
    file_label = names.file(source.get("file"))
    for text in _strings(summary):
        names.learn(text)
    chroms = summary["chromatograms"]
    # names UNICORN recorded in this run: the original chromatogram(s), original curves (also as
    # copied into imported chromatograms); they stay, here and in this run's texts
    keep = {c["name"] for c in chroms if c["kind"] == "original"}
    keep |= {cu["name"] for c in chroms for cu in c["curves"] if cu["original"] is not False}
    chrom_names = [c["name"] if c["name"] in keep else names.chromatogram(c["name"]) for c in chroms]
    curve_names = [[cu["name"] if cu["name"] in keep else names.curve(cu["name"]) for cu in c["curves"]]
                   for c in chroms]
    sources = [names.result(c["source_result_name"]) for c in chroms]
    out = names.deep(summary, keep=keep)  # every name is known now: replace them in every text
    out["name"], out["folder"] = run_label, folder_label
    if "file" in source:
        out["source"]["file"] = file_label
    if source.get("member"):
        ext = os.path.splitext(source["member"])[1]
        out["source"]["member"] = (run_label or "Run") + ext
    for chrom, name, curves, label in zip(out["chromatograms"], chrom_names, curve_names, sources):
        chrom["name"], chrom["source_result_name"] = name, label
        chrom["label"] = _chromatogram_label(chrom["kind"], label, chrom["source_result_id"], chrom["copy_of_run"])
        for curve, curve_name in zip(chrom["curves"], curves):
            curve["name"] = curve_name
    for variable in out["variables"]:  # a number stays; text may name anything
        if variable["number"] is None and isinstance(variable["value"], str) and variable["value"].strip():
            variable["value"] = TEXT_MASK
    return out
EVENT_TABLES = (("phases", "phase"), ("injections", "injection"), ("fractions", "fraction"))
# positions only: no run-log text, no 'what ran' columns (pycorn export writes those)
EVENT_FIELDS = ("number", "name", "pseudo", "start_time", "end_time", "start_volume", "end_volume", "start_cv",
                "end_cv", "start_utc", "end_utc", "time_unit", "volume_unit")
AXES = ("time", "volume", "cv", "utc", "time_adjusted", "volume_adjusted")


def _iso(value):
    return plain(value) if value is not None else None


def _source(run, label):
    """Where the run was read from: kind, label, file name and member, or the ResultID (never a server or database)."""
    origin = run.origin or {}
    out = OrderedDict([("kind", run.source), ("label", label or run.source)])
    if run.source == "sql":
        out["result_id"] = plain(run.result_id)
    else:
        path = origin.get("path") or origin.get("file_name")
        out["file"] = os.path.basename(str(path)) if path else None
        if run.source == "ufol":
            out["member"] = origin.get("member")
            out["original_id"] = plain((run.container or {}).get("original_id"))
    return out


def _run_start(run):
    """{'local', 'utc', 'utc_offset_minutes', 'source'}: RunStartDate as the file catalog reads it, else the timeline's start."""
    from .timeline import _run_start_date
    start = _run_start_date(run)
    if start is not None:
        local, offset, utc = start
        return OrderedDict([("local", _iso(local)), ("utc", _iso(utc)), ("utc_offset_minutes", plain(offset)),
                            ("source", "RunStartDate")])
    timeline = (run.timeline if run.chromatograms else None) or {}
    utc, offset = timeline.get("start_utc"), timeline.get("local_utc_offset")
    local = None
    if utc is not None and offset is not None:
        try:
            local = (utc + dt.timedelta(minutes=float(offset))).replace(tzinfo=None)
        except (TypeError, ValueError):
            local = None
    return OrderedDict([("local", _iso(local)), ("utc", _iso(utc)), ("utc_offset_minutes", plain(offset)),
                        ("source", timeline.get("start_source") if utc is not None else None)])


def _column(run):
    from .column_geometry import used_columns
    return [OrderedDict([("name", name), ("volume", plain(volume)), ("volume_unit", unit)])
            for name, volume, unit in used_columns(run)]


def _variables(run):
    """Method variables as the run used them (start-protocol answers are not variables: left out)."""
    from .variables import VARIABLE_KINDS, rows
    return [OrderedDict([("name", r.variable), ("value", r.value), ("number", plain(r.number)), ("unit", r.unit),
                         ("scouting", bool(r.scouting))]) for r in rows(run) if r.kind in VARIABLE_KINDS]


def _role(curve):
    from .curve_roles import ROLES, curve_has_role
    return next((role for role in ROLES if curve_has_role(curve, role)), None)


def _curves(chrom):
    out = []
    for curve in chrom.curves.values():
        points = None
        for axis in ("values", "time", "volume"):
            array = getattr(curve, axis, None)
            if array is not None:
                points = int(len(array))
                break
        out.append(OrderedDict([
            ("name", curve.name), ("role", _role(curve)), ("data_type", curve.data_type), ("unit", curve.unit),
            ("points", points), ("axes", [a for a in AXES if getattr(curve, a, None) is not None]),
            ("time_unit", curve.time_unit), ("volume_unit", curve.volume_unit),
            ("original", curve.is_original),
        ]))
    return out


def _event_rows(chrom, kind, notes):
    try:
        found = chrom.table_rows(kind)
    except Exception as e:  # one table that cannot be built does not hide the rest
        from .messages import plain_error
        notes.append("%s: %s table not built: %s" % (chrom.name, kind, plain_error(e)))
        return []
    return [OrderedDict((f, plain(row.get(f))) for f in EVENT_FIELDS) for row in found]


def _chromatograms(run, notes):
    default = set(run.select_chromatograms()) if run.chromatograms else set()
    out = []
    for described in run.describe_chromatograms():
        chrom = run.chromatograms[described["name"]]
        vc = chrom.column_volume_info or {}
        item = OrderedDict([
            ("name", described["name"]), ("kind", described["kind"]), ("label", described["label"]),
            ("source_result_id", plain(described["source_result_id"])),
            ("source_result_name", described["source_result_name"]), ("copy_of_run", described["copy_of_run"]),
            ("default", described["name"] in default),
            ("column_volume", OrderedDict([("value", plain(vc.get("value"))), ("unit", vc.get("unit")),
                                           ("source", vc.get("source"))])),
            ("curves", _curves(chrom)),
        ])
        for key, kind in EVENT_TABLES:
            item[key] = _event_rows(chrom, kind, notes)
        out.append(item)
    return out


def run_summary(run, mask=True, label=None, names=None):
    """
    What `run` holds, as one JSON-ready dict (module docstring): no curve values, no network
    details, no server or database names. `mask` (default True): people, computers,
    instruments, serial numbers, run, folder, file and source result names replaced by labels
    (module docstring). `label`: how to name the run's source (e.g. a database label 'alias#1');
    default its kind ('zip', 'ufol', 'sql'). `names`: a `SummaryNames` shared by every run of
    one document (default: a new one, so this run is Run1).
    """
    notes = []
    result = run.result or {}
    summary = OrderedDict([
        ("name", run.name),
        ("source", _source(run, label)),
        ("folder", result.get("FolderPath") or (run.container or {}).get("folder")),
        ("run_start", _run_start(run)),
        ("system", OrderedDict([("name", result.get("SystemName")), ("type", result.get("SystemTypeName"))])),
        ("unicorn_version", run.unicorn_version),
        ("column", _column(run)),
        ("variables", _variables(run)),
        ("chromatograms", _chromatograms(run, notes)),
        ("warnings", [str(w) for w in run.warnings or []] + notes),
        ("masked", bool(mask)),
    ])
    summary = plain(summary)
    if mask:
        from .run_details import Masker, _doc
        method = _doc(run, "MethodData", "Method").get("Description") if run.metadata else None
        summary = _mask_names(summary, names if names is not None else SummaryNames(),
                              method if isinstance(method, str) else None)
        summary = Masker(run).deep(summary)
        summary["masked"] = True
    return summary


def document(runs, masked, command="inspect", **fields):
    """
    The one JSON document `pycorn inspect --json` prints: {'schema': 1, 'version', 'command',
    'masked', 'runs', ...`fields`} (e.g. 'not_read', 'problems'). `command` names the command
    that wrote it (`json_output.document`, the envelope every command's --json takes).
    """
    from .json_output import document as envelope
    return envelope(command, masked, runs=list(runs), **fields)


def _range(row, axis, unit):
    a, b = row.get("start_" + axis), row.get("end_" + axis)
    if a is None and b is None:
        return None
    text = "%s-%s" % ("?" if a is None else "%.4g" % a, "?" if b is None else "%.4g" % b)
    return "%s %s" % (text, unit) if unit else text


def summary_lines(summary):
    """`run_summary` for people (`pycorn inspect` without --json): plain lines."""
    s = summary
    src = s["source"]
    where = src.get("file") or ("ResultID %s" % src.get("result_id") if src.get("result_id") is not None else "")
    if src.get("member"):
        where += " / " + src["member"]
    lines = ["Run: %s" % s["name"], "  source: %s%s" % (src["label"], " (%s)" % where if where else "")]
    lines.append("  folder: %s" % (s["folder"] or "-"))
    start = s["run_start"]
    lines.append("  run start: %s" % ("%s (UTC %s; %s)" % (start["local"] or "?", start["utc"], start["source"])
                                      if start["utc"] else "not known"))
    system = s["system"]
    lines.append("  system: %s%s" % (system["name"] or "-", " (%s)" % system["type"] if system["type"] else ""))
    lines.append("  column: %s" % (", ".join("%s%s" % (c["name"], " (%s %s)" % (c["volume"], c["volume_unit"] or "")
                                                       if c["volume"] is not None else "") for c in s["column"])
                                   or "-"))
    lines.append("  chromatograms: %d" % len(s["chromatograms"]))
    for c in s["chromatograms"]:
        lines.append("  [%s] %s%s" % (c["name"], c["label"], ", the default" if c["default"] else ""))
        vc = c["column_volume"]
        lines.append("    Vc: %s" % ("%.4g %s (%s)" % (vc["value"], vc["unit"] or "", vc["source"])
                                     if vc["value"] is not None else "not known (no CV)"))
        lines.append("    curves: %d" % len(c["curves"]))
        for curve in c["curves"]:
            lines.append("      %s; role %s; %s; %s points; axes %s%s" % (
                curve["name"], curve["role"] or "-", curve["unit"] or "no unit",
                "?" if curve["points"] is None else curve["points"], ", ".join(curve["axes"]) or "-",
                "" if curve["original"] is not False else "; not original"))
        for key, _ in EVENT_TABLES:
            rows = c[key]
            lines.append("    %s: %d" % (key, len(rows)))
            for row in rows:
                parts = [p for p in (_range(row, "time", row["time_unit"]), _range(row, "volume", row["volume_unit"]),
                                     _range(row, "cv", "CV")) if p]
                lines.append("      %s %s%s: %s" % (row["number"], row["name"], " (before the first)" if row["pseudo"]
                                                   else "", "; ".join(parts) or "-"))
    lines.append("  variables: %d" % len(s["variables"]))
    for v in s["variables"]:
        lines.append("    %s = %s%s" % (v["name"], v["value"], " %s" % v["unit"] if v["unit"] else ""))
    lines.append("  warnings: %d" % len(s["warnings"]))
    lines += ["    %s" % w for w in s["warnings"]]
    return lines
