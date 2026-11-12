# -*- coding: utf-8 -*-
"""
Column volume (Vc) and the CV axis.

Vc: `ColumnVolume` is not always stored (Phase 0: NULL on 55% of one database's curves), so
every source is collected as a candidate, and the first usable one by priority is used
(docs/ROADMAP.md, "Finding the column volume (Vc)"):

    1 override            the caller's value
    2 curves              Curve/EventCurve ColumnVolume (> 0)
    3 run_information     ResultRunInformation 'ColumnInformation' usedColumns (written at run time)
    4 run_log             run-log 'Base CV, Vc=<x> {<unit>}' texts
    5 flow_ratio          '<X>' (volumetric flow) / '<X> (CV/h)' curves
    6 peak_table          PeakTable ColumnVolume (only after evaluation)
    8 method              MethodData mColumnVolume: low confidence (wrong in samples/sample.zip),
                          recorded but never used for CV
    9 name_hint           a volume in the run or folder name: only with use_name_hint=True
   10 geometry            pi*d^2/4*L of a column geometry the caller gave (`geometry_volume=`):
                          a last resort, used only when no other source is usable

Column records (`ColumnTypeData`, `ColumnIndividualData`, priority 7) are not a source of Vc
(owner, 2026-11-08). `ColumnTypeData` holds the geometry of the column type the run named;
`column_geometry.py` reads it and only checks it against Vc. A geometry the user gives
(`pycorn export --bed-height/--diameter`) may give Vc as a last resort (owner, 2026-11-08),
with a warning.

CV (owner, 2026-11-08) = volume that went through the column / Vc, counted from method
start. Volume while the column valve is in by-pass doesn't count, so CV stays flat then.
By-pass is read from the run log's 'Column position ...' texts (`COLUMN_POSITION_PATTERNS`);
the switch happens at the '(Completed)' entry. Before the first switch the column is taken
to be in line. Pauses need no handling: stored volume stops during a pause.
"""
import re
from collections import OrderedDict, namedtuple

import numpy as np

from .model import this_result_note
from .units import UnitError, conversion_factor, quantity

SOURCES = ("override", "curves", "run_information", "run_log", "flow_ratio", "peak_table", "method", "name_hint",
           "geometry")
LAST_RESORT = "last_resort"  # confidence of the 'geometry' candidate
# candidates from the result's own run-level data (not the chromatogram's): for a chromatogram
# imported from another result they describe this result's run, so using one is warned (W17)
RUN_LEVEL_SOURCES = ("run_information", "method", "name_hint")
DISAGREEMENT = 0.01  # relative difference between usable candidates that is reported

# run log: 'Base CV, Vc=0.700 {ml}, Any'
BASE_CV_TEXT = re.compile(r"^Base CV\b")
BASE_CV = re.compile(r"^Base CV,\s*Vc=\s*(?P<value>\d+(?:\.\d+)?)\s*\{(?P<unit>[^}]*)\}")
METHOD_COLUMN_VOLUME = re.compile(r"<mColumnVolume>\s*([-+0-9.eE]+)\s*</mColumnVolume>")
NAME_HINT = re.compile(r"(?<![\d.])(\d+(?:\.\d+)?)\s?(ml|mL|L)\b")
# a curve name with a trailing unit in brackets: 'System flow (CV/h)' -> 'System flow'
UNIT_SUFFIX = re.compile(r"\s*\([^()]*\)\s*$")

# Column valve texts. Several patterns can be listed for other instruments; a 'Column
# position' text that none matches is reported, never guessed.
COLUMN_POSITION_TEXT = re.compile(r"^Column position\b")
COLUMN_POSITION_PATTERNS = [
    # 'Column position 1 Down flow (Completed)', 'Column position Position 2 Up flow (Issued) (Processing)',
    # 'Column position By-pass Down flow (Completed)'
    re.compile(r"^Column position\s+(?:Position\s+)?(?P<position>.+?)\s+(?P<direction>Down|Up)\s+flow\b(?P<status>.*)$"),
]
BYPASS_POSITIONS = [re.compile(r"^by-?pass$", re.I)]
COMPLETED = re.compile(r"\(Completed\)")
# volume before the first column position entry that is worth a warning when the run later
# goes to by-pass (owner, 2026-11-08); tuned against a lab environment, where the start-up volume is far smaller
PRE_SWITCH_WARN_ML = 0.1

Candidate = namedtuple("Candidate", "source value unit location confidence")


def _positive(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0


def _volume_unit(unit):
    return quantity(unit) == "volume"


def _distinct(values, rel=1e-6):
    out = []
    for v in values:
        if not any(abs(v - o) <= rel * max(abs(v), abs(o)) for o in out):
            out.append(v)
    return out


def _walk_strings(obj):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _walk_strings(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            yield from _walk_strings(v)


def _as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _flow_ratio_candidates(chrom):
    """Vc from '<X>' (ml/min, L/h, ...) / '<X> (CV/h)' at points with non-trivial flow."""
    found = []
    curves = list(chrom.curves.values())
    imported = chrom.kind == "imported"  # its curves are the source run's, all stored non-original
    for rate in curves:
        if quantity(rate.unit) != "cv_rate" or rate.values is None or not (rate.is_original or imported):
            continue
        base = UNIT_SUFFIX.sub("", rate.name or "")
        partner = next((c for c in curves if c is not rate and c.name == base and quantity(c.unit) == "flow"
                        and c.values is not None and len(c.values) == len(rate.values)), None)
        if partner is None:
            continue
        flow_per_h = partner.values.astype(np.float64) * conversion_factor(partner.unit, "ml/min") * 60.0
        cv_per_h = rate.values.astype(np.float64)
        mask = (np.abs(cv_per_h) > 0.01 * np.nanmax(np.abs(cv_per_h))) if np.any(cv_per_h) else None
        if mask is None or mask.sum() < 3:
            continue
        ratio = flow_per_h[mask] / cv_per_h[mask]
        median = float(np.median(ratio))
        spread = float(np.percentile(ratio, 90) - np.percentile(ratio, 10))
        if median > 0 and spread <= DISAGREEMENT * median:
            found.append(Candidate("flow_ratio", median, "ml", "%s / %s" % (partner.name, rate.name), "high"))
    return found


def collect_candidates(run, chrom, column_volume=None, column_volume_unit=None, geometry_volume=None):
    """
    Every Vc candidate for `chrom` (a chromatogram of `run`), in priority order.

    `column_volume` overrides everything; its unit is `column_volume_unit`, else the
    chromatogram's volume unit. `geometry_volume` = (value, unit, location) of a caller's
    column geometry, the last candidate.
    """
    cands = []
    if column_volume is not None:
        unit = column_volume_unit or chrom.info.get("VolumeUnit")
        cands.append(Candidate("override", float(column_volume), unit, "argument", "high"))

    # one candidate per distinct (value, unit), naming the curves that store it
    stored = OrderedDict()
    for kind, items in (("curve", chrom.curves.values()), ("event curve", chrom.event_curves.values())):
        for item in items:
            if _positive(item.column_volume):
                key = (float(item.column_volume), item.column_volume_unit)
                stored.setdefault(key, []).append("%s %s" % (kind, item.name))
    for (value, unit), where in stored.items():
        location = where[0] if len(where) == 1 else "%s and %d more" % (where[0], len(where) - 1)
        cands.append(Candidate("curves", value, unit, location, "high"))

    for ri in run.result.get("run_information") or []:
        if ri.get("type") != "ColumnInformation" or not isinstance(ri.get("value"), dict):
            continue
        used = ri["value"].get("usedColumns") or {}
        for column in _as_list(used.get("column") if isinstance(used, dict) else None):
            if isinstance(column, dict) and _positive(column.get("volume")):
                cands.append(Candidate("run_information", float(column["volume"]), column.get("volumeUnit"),
                                       "usedColumns %s" % (column.get("name"),), "high"))

    for ec in chrom.run_log_curves():  # an imported chromatogram's: the source run's log
        for e in ec.events:
            text = e.text or ""
            if not BASE_CV_TEXT.match(text):
                continue
            m = BASE_CV.match(text)
            if m and float(m.group("value")) > 0:
                cands.append(Candidate("run_log", float(m.group("value")), m.group("unit").strip(),
                                       "%s at %s %s" % (ec.name, e.volume, ec.volume_unit or ""), "high"))

    cands.extend(_flow_ratio_candidates(chrom))

    for table in chrom.peak_tables:
        if _positive(table.info.get("ColumnVolume")):
            unit = (table.info.get("PeakUnit") or {}).get("ColumnVolumeUnit")
            cands.append(Candidate("peak_table", float(table.info["ColumnVolume"]), unit,
                                   "peak table %s" % (table.name,), "high"))

    for text in _walk_strings(run.metadata.get("MethodData")):
        for m in METHOD_COLUMN_VOLUME.finditer(text):
            try:
                value = float(m.group(1))
            except ValueError:
                continue
            if value > 0:
                cands.append(Candidate("method", value, None, "MethodData mColumnVolume", "low"))

    for where, text in (("run name", run.name), ("folder", run.result.get("FolderPath"))):
        for m in NAME_HINT.finditer(text or ""):
            cands.append(Candidate("name_hint", float(m.group(1)), m.group(2), where, "hint"))

    if geometry_volume is not None:
        value, unit, location = geometry_volume
        if _positive(value):
            cands.append(Candidate("geometry", float(value), unit, location, LAST_RESORT))
    return cands


def resolve_column_volume(run, chrom, column_volume=None, column_volume_unit=None, use_name_hint=False,
                          geometry_volume=None):
    """
    Vc for `chrom`: a dict with `value`, `unit`, `source`, `location`, `candidates` (every
    `Candidate` found), `disagreements` and `warnings`. `value` is None if no usable
    candidate exists (low-confidence ones are kept as candidates but never used).
    `geometry_volume` (value, unit, location) is used only when nothing else is usable; when
    a stored Vc wins, a warning says the given geometry was not used.
    """
    warnings = []
    cands = collect_candidates(run, chrom, column_volume, column_volume_unit, geometry_volume)
    usable = [c for c in cands
              if (c.confidence == "high" or (c.source == "name_hint" and use_name_hint)) and _volume_unit(c.unit)]
    for c in cands:
        if c.confidence in ("high", LAST_RESORT) and not _volume_unit(c.unit):
            warnings.append("Vc %g from %s (%s) has unit %r, not a known volume unit; not used"
                            % (c.value, c.source, c.location, c.unit))
    result = OrderedDict([("value", None), ("unit", None), ("source", None), ("location", None),
                          ("candidates", cands), ("disagreements", []), ("warnings", warnings)])
    if not usable:
        usable = [c for c in cands if c.confidence == LAST_RESORT and _volume_unit(c.unit)]
        if usable:
            chosen = usable[0]
            result.update(value=chosen.value, unit=chosen.unit, source=chosen.source, location=chosen.location)
            warnings.append("no column volume stored; Vc %.4g %s from the column geometry given (%s), as a last resort"
                            % (chosen.value, chosen.unit, chosen.location))
            return result
        low = [c for c in cands if c.confidence != "high"]
        if low:
            warnings.append("no reliable column volume; low-confidence candidate(s) not used: %s; pass "
                            "column_volume= to set one"
                            % ", ".join("%g %s from %s" % (c.value, c.unit or "(no unit)", c.source) for c in low))
        else:
            warnings.append("no column volume found; CV not computed (pass column_volume= to set one)")
        return result
    chosen = usable[0]
    result.update(value=chosen.value, unit=chosen.unit, source=chosen.source, location=chosen.location)
    if chosen.source in RUN_LEVEL_SOURCES and not run.holds_run_of(chrom):
        # W17: an imported chromatogram's Vc from this result's own data (owner: used, with a warning)
        warnings.append(this_result_note(run, "Vc (%s)" % chosen.source))
    for c in cands:
        if c.source == "geometry":
            warnings.append("Vc %.4g %s from the column geometry given (%s) not used: the run has Vc %g %s (%s); "
                            "the geometry is only a last resort" % (c.value, c.unit, c.location, chosen.value,
                                                                   chosen.unit, chosen.source))
    chosen_ml = chosen.value * conversion_factor(chosen.unit, "ml")
    same_source = _distinct([c.value * conversion_factor(c.unit, "ml") for c in usable if c.source == chosen.source])
    if len(same_source) > 1:
        warnings.append("%s has %d different column volumes (%s ml); using the first, %g %s"
                        % (chosen.source, len(same_source), ", ".join("%g" % v for v in same_source),
                           chosen.value, chosen.unit))
    for c in usable:
        if c.source == chosen.source:
            continue
        c_ml = c.value * conversion_factor(c.unit, "ml")
        if abs(c_ml - chosen_ml) > DISAGREEMENT * max(c_ml, chosen_ml):
            result["disagreements"].append(c)
    if result["disagreements"]:
        warnings.append("column volume %g %s (%s) disagrees with %s"
                        % (chosen.value, chosen.unit, chosen.source,
                           ", ".join("%g %s from %s" % (c.value, c.unit, c.source) for c in result["disagreements"])))
    return result


# ---------------------------------------------------------------------------
# Column valve: by-pass intervals
# ---------------------------------------------------------------------------

def column_path(chrom):
    """
    Column valve switches from the run log of `chrom`: a dict with `switches` (time, volume,
    position, direction, bypass, text), `bypass` (by-pass intervals as (start, end) volumes in
    ml; end is inf if the run ends in by-pass), `positions` (distinct non-by-pass positions)
    and `warnings`.
    """
    warnings, switches = [], []
    unrecognized = set()
    log_curves = chrom.run_log_curves()  # an imported chromatogram's: the source run's log
    for ec in log_curves:
        try:
            to_ml = conversion_factor(ec.volume_unit, "ml")
        except UnitError as e:
            if any(COLUMN_POSITION_TEXT.match(ev.text or "") for ev in ec.events):
                warnings.append("%s: column positions not read (%s)" % (ec.name, e))
            continue
        for e in ec.events:
            text = e.text or ""
            if not COLUMN_POSITION_TEXT.match(text):
                continue
            m = next((p.match(text) for p in COLUMN_POSITION_PATTERNS if p.match(text)), None)
            if m is None:
                unrecognized.add(text)
                continue
            if not COMPLETED.search(m.group("status")) or e.volume is None:
                continue
            position = m.group("position").strip()
            switches.append(OrderedDict([("time", e.time), ("volume", e.volume * to_ml), ("position", position),
                                         ("direction", m.group("direction")),
                                         ("bypass", any(b.match(position) for b in BYPASS_POSITIONS)),
                                         ("text", text)]))
    for text in sorted(unrecognized):
        warnings.append("column position text not recognized, ignored for CV: %r" % text)
    switches.sort(key=lambda s: s["volume"])
    bypass, start = [], None
    for s in switches:
        if s["bypass"] and start is None:
            start = s["volume"]
        elif not s["bypass"] and start is not None:
            bypass.append((start, s["volume"]))
            start = None
    if start is not None:
        bypass.append((start, float("inf")))
    positions = []
    for s in switches:
        if not s["bypass"] and s["position"] not in positions:
            positions.append(s["position"])
    if len(positions) > 1:
        warnings.append("the run uses %d column positions (%s); CV counts the volume through all of them"
                        % (len(positions), ", ".join(positions)))
    if bypass and switches and switches[0]["volume"] > PRE_SWITCH_WARN_ML:
        warnings.append("%g ml ran before the first column position entry and is counted as through the column"
                        % switches[0]["volume"])
    return OrderedDict([("switches", switches), ("bypass", bypass), ("positions", positions),
                        ("warnings", warnings)])


def in_column_volume(volume_ml, bypass):
    """Volume (ml, number or array) that went through the column up to `volume_ml`."""
    v = np.asarray(volume_ml, dtype=np.float64)
    out = v.copy()
    for start, end in bypass:
        out -= np.clip(v - start, 0.0, end - start)
    return out if out.ndim else float(out)


def apply_cv(chrom, vc, vc_unit, path):
    """Set `cv` on the curves and events of `chrom` (None everywhere if `vc` is None)."""
    warnings = []
    vc_ml = vc * conversion_factor(vc_unit, "ml") if vc is not None else None
    bypass = path["bypass"]
    for curve in chrom.curves.values():
        curve.cv = None
        if vc_ml is None or curve.volume is None:
            continue
        try:
            to_ml = conversion_factor(curve.volume_unit, "ml")
        except UnitError as e:
            warnings.append("%s: no CV (%s)" % (curve.name, e))
            continue
        curve.cv = in_column_volume(curve.volume.astype(np.float64) * to_ml, bypass) / vc_ml
    for ec in chrom.event_curves.values():
        to_ml = None
        if vc_ml is not None:
            try:
                to_ml = conversion_factor(ec.volume_unit, "ml")
            except UnitError as e:
                warnings.append("%s: no CV (%s)" % (ec.name, e))
        for e in ec.events:
            e.cv = (in_column_volume(e.volume * to_ml, bypass) / vc_ml
                    if to_ml is not None and e.volume is not None else None)
    return warnings
