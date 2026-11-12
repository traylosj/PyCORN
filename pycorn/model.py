# -*- coding: utf-8 -*-
"""
Structured model of a UNICORN run, independent of where it was loaded from.

Every object keeps the source XML element as a dict (`xml`, from xmltodict, all
values as the original strings), so fields without a typed attribute are still
available. Typed attributes are conveniences on top of that.
"""
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Optional

import numpy as np


@dataclass
class Entry:
    """One file in the bundle (outer entry or a member of a nested zip)."""
    name: str
    raw: bytes
    kind: str  # 'xml', 'zip', 'nrbf', 'png', 'text', 'empty' or 'unknown'
    # (for 'png', `value` is the image without the null padding UNICORN adds)
    data_type: Optional[str] = None  # .NET type name, for members of nested zips
    value: Any = None  # decoded value: str, list of numbers, or dict for parsed XML
    children: "OrderedDict[str, Entry]" = field(default_factory=OrderedDict)
    error: Optional[str] = None  # why decoding failed; `raw` is still kept


@dataclass
class CurvePointSet:
    """One stored point set of a curve (a curve can have several resolutions)."""
    file_name: str
    is_full_resolution: Optional[bool]
    volume: Optional[np.ndarray]  # float32, as stored; float64 built from the grid for volume-grid curves; None for time-only curves
    values: Optional[np.ndarray]  # float32, as stored


@dataclass
class Curve:
    name: str
    data_type: str  # CurveDataType attribute, e.g. 'UV', 'Conduction', 'Other'
    unit: Optional[str]  # AmplitudeUnit, exactly as stored
    precision: Optional[int]  # AmplitudePrecision: decimals UNICORN displays; `values` are not rounded
    curve_number: Optional[int]
    # elapsed method time in `time_unit`; None if it cannot be built, and for curves on a
    # regular volume grid (isochrone 'Volume'), which have no time axis
    time: Optional[np.ndarray]
    # None if it cannot be built, and for amplitude-only curves on a regular time grid with
    # no VolumeUnit (e.g. UV ratio curves from evaluation), which have no volume axis
    volume: Optional[np.ndarray]
    values: Optional[np.ndarray]
    time_unit: Optional[str]
    volume_unit: Optional[str]
    isochrone: Optional[str]  # 'Time' or 'Volume': which axis is on a regular grid
    is_original: Optional[bool]
    column_volume: Optional[float]
    column_volume_unit: Optional[str]
    uv_info: Optional[dict]  # CurveUVInfo with typed values
    point_sets: list
    xml: dict
    # axes as UNICORN plots them: zero at the injection given by `Chromatogram.retention_zero`
    # (equal to `time`/`volume` when retention is not adjusted; None if the zero is unknown)
    time_adjusted: Optional[np.ndarray] = None
    volume_adjusted: Optional[np.ndarray] = None
    # column volumes through the column since method start (by-pass doesn't count); None
    # without a column volume or a volume axis (see `pycorn.column_volume`)
    cv: Optional[np.ndarray] = None
    # wall clock, numpy datetime64[us] in UTC; NaT where it can't be known (see `pycorn.timeline`)
    utc: Optional[np.ndarray] = None
    # set on converted or derived copies only (`pycorn.units.normalized`, `pycorn.concentration`)
    conversion: Optional[dict] = None

    def to_frame(self):
        """Return the curve as a DataFrame with raw and injection-adjusted axes, CV, UTC and the values."""
        import pandas as pd
        return pd.DataFrame({"time": self.time, "volume": self.volume,
                             "time_adjusted": self.time_adjusted, "volume_adjusted": self.volume_adjusted,
                             "cv": self.cv, "utc": self.utc, "value": self.values})


@dataclass
class Event:
    time: Optional[float]
    volume: Optional[float]
    text: Optional[str]
    type: Optional[str]
    subtype: Optional[str]
    instruction_feedback: Optional[int]
    xml: dict
    time_adjusted: Optional[float] = None
    volume_adjusted: Optional[float] = None
    cv: Optional[float] = None
    utc: Optional[Any] = None  # numpy datetime64[us], naive UTC (as Curve.utc); None when unknown


@dataclass
class EventCurve:
    name: str
    type: Optional[str]  # EventCurveType attribute, e.g. 'Fraction', 'Injection', 'Logbook'
    curve_number: Optional[int]
    time_unit: Optional[str]
    volume_unit: Optional[str]
    is_original: Optional[bool]
    column_volume: Optional[float]
    column_volume_unit: Optional[str]
    events: list
    xml: dict

    def to_frame(self):
        import pandas as pd
        columns = ["time", "volume", "time_adjusted", "volume_adjusted", "cv", "utc", "text", "type", "subtype",
                   "instruction_feedback"]
        return pd.DataFrame([{c: getattr(e, c) for c in columns} for e in self.events], columns=columns)


@dataclass
class PeakTable:
    name: Optional[str]
    curve_number: Optional[int]  # the curve the peaks were integrated on
    info: dict  # table-level fields (typed), without the peaks
    peaks: list  # one dict of typed fields per peak
    xml: dict

    def to_frame(self):
        import pandas as pd
        return pd.DataFrame(self.peaks)


# What a chromatogram of a result is (W17, owner 2026-11-11): 'original' (the run's own: original
# curves, the run log), 'imported' (copied in from another result in Evaluation: it stores
# OriginalResultID / OriginalResultName, every curve non-original, an imported run log) or
# 'derived' (made in Evaluation: non-original curves and events only, no original result). Exports
# and visuals show the original chromatogram(s) by default; every other one is selectable.
CHROMATOGRAM_KINDS = ("original", "imported", "derived")
# the stored EventCurveType of a run log: the run's own, and the one an imported chromatogram
# carries (the source result's run log, copied in with it; owner, 2026-11-11: read it, labelled
# as from the source result)
RUN_LOG, IMPORTED_RUN_LOG = "Logbook", "ImportedLogbook"
# what a chromatogram selection can name besides a chromatogram's name (`select_chromatograms`)
CHROMATOGRAM_WORDS = ("all",) + CHROMATOGRAM_KINDS


def chromatogram_kind(info, curve_flags=(), event_flags=()):
    """
    'original', 'imported' or 'derived' (`CHROMATOGRAM_KINDS`) from what a chromatogram stores:
    its `info` (an `OriginalResultID` means imported) and the `IsOriginalData` flags of its curves
    and event curves (True, False or None for not stored). Derived: every flag is False. A
    chromatogram with no curve and no event curve, or any flag not False, counts as original.
    Used by `Chromatogram.kind` and by the light file read (`file_catalog.read_headers`).
    """
    source = (info or {}).get("OriginalResultID")
    if source is not None and str(source).strip() != "":
        return "imported"
    flags = list(curve_flags) + list(event_flags)
    if flags and all(f is False for f in flags):
        return "derived"
    return "original"


def source_result(info):
    """{'id', 'name'} of the result an imported chromatogram came from (`info`'s OriginalResultID / OriginalResultName); None when it has none."""
    info = info or {}
    rid, name = info.get("OriginalResultID"), info.get("OriginalResultName")
    if (rid is None or str(rid).strip() == "") and not name:
        return None
    return {"id": rid, "name": name}


def _stored(value):
    """A stored text field, stripped; None when empty or not stored."""
    text = str(value).strip() if value is not None else ""
    return text or None


def imported_note(source):
    """
    The note every table read from an imported chromatogram's run log carries (warnings),
    from `Chromatogram.run_log_source()`: what ran in the source result, not in this one.
    """
    name, rid = source.get("source_result_name"), source.get("source_result_id")
    what = "%s (ResultID %s)" % (name, rid) if name and rid not in (None, "") else name or "result %s" % rid
    return ("imported chromatogram: run log %r (%s) is the source result's, %s: what ran there, not in this result"
            % (source.get("event_curve"), source.get("type"), what))


def this_result_note(run, what):
    """
    The warning for run-level data of this result used for a chromatogram imported from another
    result (`Run.holds_run_of` False; owner, 2026-11-11: used, with a warning, since it is often
    the same system and column): `what` was taken from this result, not the source's run.
    """
    rid = run.result_id if run is not None else None
    where = "this result (ResultID %s)" % rid if rid not in (None, "") else "this result"
    return "imported chromatogram: %s taken from %s, which may differ from the source result's run" % (what, where)


def describe_chromatograms(items, result_id=None, result_name=None):
    """
    One dict per chromatogram of a result, in stored order: {'name', 'kind', 'source_result_id',
    'source_result_name', 'copy_of_run', 'label'}. `items`: (name, kind, info) per chromatogram;
    `result_id` / `result_name`: the result's own (to tell when an imported chromatogram is a copy
    of this same run: the same result ID, or the same name where the result's ID is not known).
    'copy_of_run' is None for chromatograms that are not imported. 'label': English text, e.g.
    'imported from <name> (a copy of this run)'.
    """
    out = []
    for name, kind, info in items:
        source = source_result(info) if kind == "imported" else None
        copy = None
        if source is not None:
            if result_id is not None and source["id"] is not None:
                copy = str(source["id"]).strip() == str(result_id).strip()
            else:
                copy = bool(result_name) and source["name"] == result_name
        if kind == "imported":
            label = "imported from %s" % (source["name"] if source and source["name"] else
                                          "result %s" % (source["id"] if source else "?"))
            if copy:
                label += " (a copy of this run)"
        else:
            label = kind
        out.append({"name": name, "kind": kind, "source_result_id": source["id"] if source else None,
                    "source_result_name": source["name"] if source else None, "copy_of_run": copy, "label": label})
    return out


def select_chromatograms(described, which=None):
    """
    (names, unmatched) of a chromatogram selection over `describe_chromatograms` rows. `which`:
    None (the default: the original chromatogram(s); the first chromatogram when none is
    original), or a text or a list of texts, each a chromatogram's name (as stored, exact) or
    one of `CHROMATOGRAM_WORDS`: 'all', 'original', 'imported', 'derived'. A name wins over a
    word. `names` keeps the stored order; `unmatched`: the texts that matched no chromatogram.
    """
    names = [d["name"] for d in described]
    if which is None:
        picked = [d["name"] for d in described if d["kind"] == "original"] or names[:1]
        return picked, []
    if isinstance(which, str):
        which = [which]
    wanted, unmatched = set(), []
    for item in which:
        item = str(item)
        if item in names:
            found = [item]
        elif item.strip().lower() in CHROMATOGRAM_WORDS:
            word = item.strip().lower()
            found = [d["name"] for d in described if word == "all" or d["kind"] == word]
        else:
            found = []
        if not found:
            unmatched.append(item)
        wanted.update(found)
    return [n for n in names if n in wanted], unmatched


@dataclass
class Chromatogram:
    name: str
    chromatogram_id: Optional[int]
    info: dict  # chromatogram-level fields (typed); embedded XML documents are parsed
    curves: "OrderedDict[str, Curve]"
    event_curves: "OrderedDict[str, EventCurve]"
    peak_tables: list
    pool_tables: Any  # PoolTables element as a dict (empty in all samples so far)
    # Curve and event-curve names depend on the instrument and its configuration (e.g. 'UV 1_280'
    # vs 'UV_280'), and curve numbers differ between runs. Prefer the type lookups below; use
    # `curve(name)` only for names known to exist in this run.
    # where UNICORN puts zero on the x axis: {'adjusted', 'injection_number', 'time', 'volume',
    # 'source'}; 'source' is 'layout', 'ui_settings' (7.11+), 'peak_table', 'default' (no setting:
    # injection 1, as UNICORN's export does), 'none' (no injection) or 'disabled' (adjust_axes=False);
    # 'ui_settings_phase' (zero at a phase start, unverified) adds 'phase' and 'phase_occurrence'
    retention_zero: Optional[dict] = None
    # Vc used for CV: 'value', 'unit', 'source', 'location', every 'candidates' found and
    # 'disagreements' (`pycorn.column_volume.resolve_column_volume`)
    column_volume_info: Optional[dict] = None
    # column valve switches and by-pass intervals from the run log (`pycorn.column_volume.column_path`)
    column_path: Optional[dict] = None
    # UTC reconstruction: start, end, anchors, pauses, quality (`pycorn.timeline.build_timeline`)
    timeline: Optional[dict] = None
    # the run's inlet valves and pumps from its stored configuration, set at load like the axes
    # (`pycorn.system_configuration`); None: not read (the inlet labels then say 'not known')
    system_configuration: Any = None

    def curve(self, name):
        """Return the curve with exactly this name; the error lists the names this run has."""
        try:
            return self.curves[name]
        except KeyError:
            raise KeyError("no curve %r in %s; available: %s" % (name, self.name, ", ".join(self.curves))) from None

    @property
    def kind(self):
        """'original', 'imported' or 'derived' (`chromatogram_kind`, W17)."""
        return chromatogram_kind(self.info, [c.is_original for c in self.curves.values()],
                                 [ec.is_original for ec in self.event_curves.values()])

    @property
    def source_result(self):
        """{'id', 'name'} of the result an imported chromatogram came from; None for the others."""
        return source_result(self.info) if self.kind == "imported" else None

    def only_original(self, original_only=None):
        """
        Whether curve lookups skip non-original curves: `original_only` as given, or for None
        (the default of the lookups) True on an original chromatogram (its derived curves are
        left out, as before W17) and False on an imported or derived one, whose curves are all
        non-original.
        """
        return self.kind == "original" if original_only is None else bool(original_only)

    def curves_of_type(self, data_type, original_only=None):
        """
        Curves whose CurveDataType is `data_type` (as stored, e.g. 'UV', 'Conduction', 'pH').
        Types are not validated against a fixed list: other instruments may store others.
        `original_only`: see `only_original` (None: original curves of an original chromatogram,
        every curve of an imported or derived one).
        """
        only = self.only_original(original_only)
        return [c for c in self.curves.values()
                if c.data_type == data_type and (c.is_original is not False or not only)]

    def curves_with_role(self, role, original_only=None):
        """
        Curves that measure `role` ('uv', 'conductivity', 'ph', 'conc_b', ...; see
        `pycorn.curve_roles.ROLES`), whatever the instrument names them. `original_only`: see
        `only_original`.
        """
        from .curve_roles import curves_with_role
        return curves_with_role(self, role, original_only)

    def event_curves_of_type(self, type):
        """Event curves whose EventCurveType is `type` (as stored, e.g. 'Injection', 'Fraction', 'Logbook')."""
        return [ec for ec in self.event_curves.values() if ec.type == type]

    def run_log_curves(self):
        """
        The event curves that hold this chromatogram's run log, by stored type: 'Logbook'; for an
        imported chromatogram 'ImportedLogbook' (the source run's log, copied in with it; times
        and volumes on the source run's axes, as its curves), 'Logbook' only when it has none.
        Every run-log reader (phases, sections, Vc, column path, UTC) takes the first.
        """
        if self.kind == "imported":
            found = self.event_curves_of_type(IMPORTED_RUN_LOG)
            if found:
                return found
        return self.event_curves_of_type(RUN_LOG)

    def run_log(self):
        """The first of `run_log_curves()`, or None."""
        found = self.run_log_curves()
        return found[0] if found else None

    def run_log_source(self):
        """
        Where this chromatogram's run log comes from: None for the run's own ('Logbook'); for an
        imported one {'event_curve', 'type', 'source_result_id', 'source_result_name'}: the
        run log of the result it was copied from, so what ran there, not in this result.
        """
        log = self.run_log()
        if log is None or self.kind != "imported":
            return None
        source = self.source_result or {}
        return OrderedDict([("event_curve", log.name), ("type", log.type), ("source_result_id", source.get("id")),
                            ("source_result_name", source.get("name"))])

    def own_event_curves(self, type):
        """
        The event curves of `type` ('Injection', 'Fraction', ...) that record what ran: the
        original ones of an original or derived chromatogram (evaluation-made ones left out, as
        before W17). An imported chromatogram stores every event curve as non-original, so there
        they are told apart by what they store: left out are pooled-fraction curves
        (`pycorn.pools`: made in the source's evaluation) and curves of another method run (a
        stored `MethodStartTime` other than the run log's: marks the source result had itself
        imported). Curves that store no `MethodStartTime` are kept.
        """
        found = [ec for ec in self.event_curves_of_type(type) if ec.events]
        if self.kind != "imported":
            return [ec for ec in found if ec.is_original is not False]
        from .pools import is_pooled_curve
        return [ec for ec in found if not self.from_other_run(ec) and not (type == "Fraction" and is_pooled_curve(ec))]

    def from_other_run(self, item):
        """
        True when curve or event curve `item` of an imported chromatogram is of another method
        run than its run log: a stored `MethodStartTime` other than the run log's (curves the
        source result had itself imported, as overlays). False for an original or derived
        chromatogram, without a run log, or when either stores no `MethodStartTime`.
        """
        if self.kind != "imported":
            return False
        log = self.run_log()
        start = _stored((log.xml or {}).get("MethodStartTime")) if log is not None else None
        own = _stored((getattr(item, "xml", None) or {}).get("MethodStartTime"))
        return start is not None and own is not None and own != start

    def other_run_curves(self):
        """The curves of an imported chromatogram that are of another method run (`from_other_run`)."""
        return [c for c in self.curves.values() if self.from_other_run(c)]

    def table_rows(self, kind):
        """Rows of the 'phase', 'injection' or 'fraction' table (see `pycorn.event_tables`)."""
        from .event_tables import rows
        return rows(self, kind)

    def _table(self, kind, sections):
        from .event_tables import to_frame, with_details
        return to_frame(with_details(self, self.table_rows(kind), sections))

    def phase_table(self, sections=None):
        """
        Phases with start, end and length in time, volume, CV and UTC, and what ran in each (flow,
        %B, inlets, set marks, ...: `event_tables.SPAN_COLUMNS`), as a DataFrame. `sections`: a
        `run_log_sections()` result of this chromatogram when already built.
        """
        return self._table("phase", sections)

    def injection_table(self, sections=None):
        """Injections, with the same columns as `phase_table`."""
        return self._table("injection", sections)

    def fraction_table(self, sections=None):
        """Fractions, with the same columns as `phase_table`."""
        return self._table("fraction", sections)

    def zero_offsets(self, kind, number):
        """
        What to subtract from time, volume, CV and UTC to put zero at the start of row `number`
        of the `kind` table (0 is the leading pseudo-row), as UNICORN adjusts its x axis.
        """
        from .event_tables import zero_offsets
        for row in self.table_rows(kind):
            if row["number"] == number:
                return zero_offsets(row)
        raise KeyError("no %s number %r in %s" % (kind, number, self.name))


@dataclass
class DbTable:
    """Rows of one database table that belong to, or are referenced by, a result."""
    name: str  # schema-qualified, e.g. 'dbo.Curve'
    # 'owned': below dbo.Result through foreign keys (curves, events, peaks, ...);
    # 'linked': owned through a link table this result alone uses (e.g. its audit trail);
    # 'referenced': rows the others point at, which other results can share (folders, lookups, ...)
    relation: str
    columns: dict  # column name -> SQL data type
    rows: list  # one dict per row, values as the driver returns them (blobs as bytes)

    def to_frame(self):
        import pandas as pd
        return pd.DataFrame(self.rows, columns=list(self.columns))


@dataclass
class Run:
    """Everything loaded from one result."""
    source: str  # 'zip', 'ufol' for a result read from a folder export, or 'sql' for the database
    locator: str  # file path ('<.UFol path>/<member path>' for a folder export; 'sql:ResultID=<id>')
    # Result.xml (typed values; for a database run, dbo.Result laid out the same way);
    # `run_information` and `search_criteria` decoded
    result: dict
    chromatograms: "OrderedDict[str, Chromatogram]"
    metadata: "OrderedDict[str, Any]"  # nested *Data entries: parsed XML dict, text or None
    evaluation_log: list  # audit trails from EvaluationLog.xml (database: dbo.AuditTrail and its entries)
    manifest: list  # Manifest.xml entries (empty for a database run)
    # every file in the bundle, raw bytes included; for a database run, the curve and *Data
    # blobs under the names they have in a bundle
    entries: "OrderedDict[str, Entry]"
    warnings: list = field(default_factory=list)
    # where the bundle sat in a folder export (member, folder, name, original_id,
    # original_path); None for a standalone bundle
    container: Optional[dict] = None
    # where the run was read from: 'file_name' and 'path' of the .zip (for a folder export: of
    # the .UFol, plus the 'member' inside it), or 'server', 'database' and 'result_id'.
    # Holds connection details for database runs: don't print or log it.
    origin: Optional[dict] = None
    # database runs only: every row tied to the result, by table ('dbo.Curve', ...)
    tables: "OrderedDict[str, DbTable]" = field(default_factory=OrderedDict)

    @property
    def name(self):
        return self.result.get("Name")

    @property
    def created(self):
        return self.result.get("Created")

    @property
    def unicorn_version(self):
        return self.result.get("@UNICORNVersion")

    # Vc and the timeline of the first chromatogram (each chromatogram has its own)
    def _first_info(self, attr, key):
        if not self.chromatograms:
            return None
        info = getattr(self.chromatogram(), attr)
        return info.get(key) if info else None

    @property
    def column_volume(self):
        return self._first_info("column_volume_info", "value")

    @property
    def column_volume_unit(self):
        return self._first_info("column_volume_info", "unit")

    @property
    def column_volume_source(self):
        return self._first_info("column_volume_info", "source")

    @property
    def column_volume_candidates(self):
        return self._first_info("column_volume_info", "candidates")

    @property
    def timeline(self):
        return self.chromatogram().timeline if self.chromatograms else None

    def variables(self):
        """Method variables as the run used them, one row per stored row (`pycorn.variables`)."""
        from .variables import to_frame
        return to_frame(self)

    def variable(self, name):
        """One method variable as the run used it; the error lists the names this run has."""
        from .variables import variable
        return variable(self, name)

    def system_configuration(self):
        """
        Inlet valves and pumps from the result's stored system configuration: per role
        (inlet valve A / B, sample inlet valve, sample pump, quaternary valve) installed, not
        installed or not known (`pycorn.system_configuration`).
        """
        from .system_configuration import system_configuration
        return system_configuration(self)

    def normalized(self, units=None, uv_path_length_cm=None):
        """A converted copy; see `pycorn.units.normalized`."""
        from .units import normalized
        return normalized(self, units, uv_path_length_cm)

    def chromatogram(self, name=None):
        """
        Return a chromatogram by name, or (no name) the run's first original chromatogram (W17:
        the default everywhere; the first chromatogram when none is original).
        """
        if name is None:
            return self.chromatograms[self.select_chromatograms()[0]]
        return self.chromatograms[name]

    @property
    def result_id(self):
        """The result's own ID where the source gives one (database: ResultID; .UFol member: its original id), else None."""
        rid = (self.result or {}).get("ResultID")
        if rid is None:
            rid = (self.origin or {}).get("result_id")
        if rid is None:
            rid = (self.container or {}).get("original_id")
        return rid

    def describe_chromatograms(self):
        """
        One dict per chromatogram, in stored order: {'name' (the key in `chromatograms`), 'kind'
        ('original', 'imported', 'derived'), 'source_result_id', 'source_result_name' (imported
        only), 'copy_of_run' (an imported copy of this same run; None when not imported), 'label'}.
        See `pycorn.model.describe_chromatograms`.
        """
        return describe_chromatograms([(key, c.kind, c.info) for key, c in self.chromatograms.items()],
                                      self.result_id, self.name)

    def holds_run_of(self, chrom):
        """
        True when the result's own run-level data (method, system configuration, column, run
        information, run name) describe the run `chrom` holds: every chromatogram except one
        imported from another result (an imported copy of this same run counts, see
        `describe_chromatograms`). For the others the source run's are not stored here: readers
        use this result's (often the same system and column; owner, 2026-11-11), each with one
        warning (`this_result_note`).
        """
        if chrom is None or chrom.kind != "imported":
            return True
        key = next((k for k, c in self.chromatograms.items() if c is chrom), chrom.name)
        found = next((d for d in self.describe_chromatograms() if d["name"] == key), None)
        return bool(found and found["copy_of_run"])

    def select_chromatograms(self, which=None, strict=True):
        """
        Names (keys of `chromatograms`, stored order) of a selection: None (default) the original
        chromatogram(s); else a name or a word ('all', 'original', 'imported', 'derived'), or a
        list of them. `strict`: KeyError, listing the run's chromatograms, for an item that
        matches none (else it is skipped; `chromatogram_selection` returns those too).
        """
        names, unmatched = self.chromatogram_selection(which)
        if strict and unmatched:
            raise KeyError("no chromatogram %s; the run has %s; or give one of: %s"
                           % (", ".join(repr(u) for u in unmatched), ", ".join(
                               "%s (%s)" % (d["name"], d["label"]) for d in self.describe_chromatograms()),
                              ", ".join(CHROMATOGRAM_WORDS)))
        return names

    def chromatogram_selection(self, which=None):
        """(names, unmatched texts) of `select_chromatograms` without raising."""
        if not self.chromatograms:
            return [], ([] if which is None else [which] if isinstance(which, str) else list(which))
        return select_chromatograms(self.describe_chromatograms(), which)

    def to_res3_dict(self):
        """
        Return the legacy res3-like dict that `PcUni6.load_all_xml()` produces:
        {'Chrom.N': {name: {'data': [(volume, value), ...], ...}}}.
        Only original event curves are included, as in `PcUni6`.
        """
        out = OrderedDict()
        for chrom in self.chromatograms.values():
            run_name = chrom.name + ".Xml"
            block = {}
            for ec in chrom.event_curves.values():
                if not ec.is_original:
                    continue
                e_name = "Fractions" if ec.name == "Fraction" else ec.name
                block[e_name] = {"run_name": run_name, "data": [(e.volume, e.text) for e in ec.events],
                                 "data_name": e_name, "magic_id": 0}
            for curve in chrom.curves.values():
                if curve.volume is None or curve.values is None:
                    continue
                d_name = "xUV cell path length" if curve.name == "UV cell path length" else curve.name
                block[d_name] = {"run_name": run_name,
                                 "data": list(zip(curve.volume.tolist(), curve.values.tolist())),
                                 "unit": curve.unit, "data_name": d_name, "data_type": curve.data_type,
                                 "magic_id": 0}
            out[chrom.name] = block
        return out
