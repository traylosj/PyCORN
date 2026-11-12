# -*- coding: utf-8 -*-
"""
Read a UNICORN 6+ result bundle (.zip) into a `pycorn.model.Run`, keeping everything.

Nothing in the bundle is dropped:
- `run.entries` holds every file (and every member of every nested zip) with its raw
  bytes and decoded value; entries that cannot be decoded keep `raw` and set `error`.
- The structured fields (`result`, `chromatograms`, `metadata`, `evaluation_log`,
  `manifest`) are built from those entries.
- Anything unexpected (undecodable entry, missing curve file, manifest mismatch,
  unusable time grid) is recorded in `run.warnings` instead of being skipped silently.

Values in `run.entries[...].value` and in each object's `xml` dict are the original
strings. `run.result`, chromatogram `info`, event and peak fields, and the audit log
are converted to bool/int/float where the text is unambiguous.
"""
import base64
import binascii
import os
import re
from collections import OrderedDict
from zipfile import ZipFile

import numpy as np
import xmltodict

from ..axes import add_axes
from ..bundle_codecs import (NrbfError, decode_nrbf, decode_text, read_nested_zip,
                             strip_xml_declaration)
from ..model import (Chromatogram, Curve, CurvePointSet, Entry, Event, EventCurve,
                     PeakTable, Run)

_INT = re.compile(r"^-?\d+$")
_FLOAT = re.compile(r"^-?(\d+\.\d*|\.\d+|\d+(\.\d*)?[eE][-+]?\d+|\d+\.\d*[eE][-+]?\d+)$")
_DATA_TYPE_SUFFIX = "DataType"
_STRUCTURED_XML = ("Result.xml", "Manifest.xml", "EvaluationLog.xml")
# a zip with neither is some other zip
NOT_A_RESULT = ("not a UNICORN result (no Result.xml and no chromatogram), only result exports (.zip) from "
                "UNICORN 6 or later are read")
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_PNG_END = b"IEND\xaeB`\x82"  # IEND chunk type + its CRC


def _as_list(value):
    """xmltodict gives a dict for one child and a list for several; always return a list."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _typed_scalar(text):
    if not isinstance(text, str):
        return text
    if text == "true":
        return True
    if text == "false":
        return False
    if _INT.match(text) and not (len(text.lstrip("-")) > 1 and text.lstrip("-").startswith("0")):
        return int(text)
    if _FLOAT.match(text):
        return float(text)
    return text


def _typed(obj):
    """
    Copy of an xmltodict tree with unambiguous leaf strings converted to bool/int/float.
    XML attributes (`@...` keys, e.g. FormatVersion) stay as text.
    """
    if isinstance(obj, dict):
        return OrderedDict((k, v if k.startswith("@") else _typed(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return [_typed(v) for v in obj]
    return _typed_scalar(obj)


def _parse_xml_text(text):
    return xmltodict.parse(strip_xml_declaration(text))


def _expand_embedded_xml(obj, warnings, where):
    """Copy of a tree in which string values holding a whole XML document are parsed."""
    if isinstance(obj, dict):
        return OrderedDict((k, _expand_embedded_xml(v, warnings, where)) for k, v in obj.items())
    if isinstance(obj, list):
        return [_expand_embedded_xml(v, warnings, where) for v in obj]
    if isinstance(obj, str) and obj.lstrip().startswith("<?xml"):
        try:
            return _parse_xml_text(obj)
        except Exception as e:
            warnings.append("%s: embedded XML not parsed (%s); kept as text" % (where, e))
    return obj


def _float(value):
    value = _typed_scalar(value)
    return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else None


def _int(value):
    value = _typed_scalar(value)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _bool(value):
    value = _typed_scalar(value)
    return value if isinstance(value, bool) else None


# ---------------------------------------------------------------------------
# Entries
# ---------------------------------------------------------------------------

def _decode_member(name, raw, data_type, warnings, where):
    """Decode one member of a nested zip (a BinaryFormatter value)."""
    try:
        value = decode_nrbf(raw)
    except NrbfError as e:
        warnings.append("%s/%s: not decoded (%s); raw bytes kept" % (where, name, e))
        return Entry(name, raw, "unknown", data_type=data_type, error=str(e))
    if isinstance(value, str) and value.lstrip().startswith("<"):
        try:
            value = _parse_xml_text(value)
        except Exception as e:
            warnings.append("%s/%s: XML not parsed (%s); kept as text" % (where, name, e))
    elif isinstance(value, str) and value == "":
        value = None
    return Entry(name, raw, "nrbf", data_type=data_type, value=value)


def _read_entry(name, raw, warnings):
    if len(raw) == 0:
        return Entry(name, raw, "empty")
    members = read_nested_zip(raw)
    if members is not None:
        entry = Entry(name, raw, "zip")
        for member, member_raw in members.items():
            if member.endswith(_DATA_TYPE_SUFFIX) and member[:-len(_DATA_TYPE_SUFFIX)] in members:
                text = member_raw.decode("utf-8", "replace").strip()
                entry.children[member] = Entry(member, member_raw, "text", value=text)
                continue
            type_entry = members.get(member + _DATA_TYPE_SUFFIX)
            data_type = type_entry.decode("utf-8", "replace").strip() if type_entry is not None else None
            entry.children[member] = _decode_member(member, member_raw, data_type, warnings, name)
        return entry
    if raw.startswith(_PNG_SIGNATURE):
        # padded with nulls to a fixed size (128 KiB in all samples); the image ends at IEND
        end = raw.find(_PNG_END)
        if end < 0:
            warnings.append("%s: PNG without IEND chunk; raw bytes kept" % name)
            return Entry(name, raw, "png", value=raw, error="no IEND chunk")
        return Entry(name, raw, "png", value=raw[:end + len(_PNG_END)])
    try:
        text = decode_text(raw)
    except UnicodeDecodeError as e:
        warnings.append("%s: unknown binary format; raw bytes kept" % name)
        return Entry(name, raw, "unknown", error=str(e))
    if text.lstrip().startswith("<"):
        try:
            return Entry(name, raw, "xml", value=_parse_xml_text(text))
        except Exception as e:
            warnings.append("%s: XML not parsed (%s); kept as text" % (name, e))
            return Entry(name, raw, "text", value=text, error=str(e))
    return Entry(name, raw, "text", value=text)


# ---------------------------------------------------------------------------
# Result.xml, Manifest.xml, EvaluationLog.xml
# ---------------------------------------------------------------------------

def _run_information(type_, raw, text, warnings, where):
    """One run-information item: `text` is XML or plain text; `raw` is the stored form."""
    decoded = {"type": type_, "raw": raw, "text": text, "value": None}
    if text is None:
        return decoded
    if text.lstrip().startswith("<"):
        try:
            decoded["value"] = _typed(_parse_xml_text(text))
        except Exception as e:
            warnings.append("%s: RunInformation %r XML not parsed (%s)" % (where, type_, e))
    else:
        decoded["value"] = text
    return decoded


def _decode_run_information(item, warnings):
    """ResultRunInformation: base64 text, which is XML or plain text once decoded."""
    encoded = item.get("RunInformation") or ""
    type_ = item.get("@RunInformationType")
    try:
        text = decode_text(base64.b64decode(encoded, validate=False))
    except (binascii.Error, UnicodeDecodeError) as e:
        warnings.append("Result.xml: RunInformation %r not decoded (%s)" % (type_, e))
        text = None
    return _run_information(type_, encoded, text, warnings, "Result.xml")


def _result_from_root(root, run_information, warnings, where):
    """`run.result` from the Result element: typed values, decoded run information, search criteria."""
    result = _typed(_expand_embedded_xml(root, warnings, where))
    result["run_information"] = run_information
    criteria = (root.get("ResultSearchCriterias") or {}).get("ResultSearchCriteria")
    # Keyword2 is the variable's value; keep it as text (e.g. '0.070', 'In-line')
    result["search_criteria"] = [OrderedDict((k, v) for k, v in c.items()) for c in _as_list(criteria)]
    return result


def _build_result(entry, warnings):
    if entry is None or not isinstance(entry.value, dict):
        warnings.append("Result.xml missing or unreadable")
        return OrderedDict()
    root = entry.value.get("Result") or OrderedDict()
    run_information = [_decode_run_information(item, warnings)
                       for item in _as_list(root.get("ResultRunInformation"))]
    return _result_from_root(root, run_information, warnings, "Result.xml")


def _build_manifest(entry, names, warnings):
    if entry is None or not isinstance(entry.value, dict):
        warnings.append("Manifest.xml missing or unreadable")
        return []
    root = entry.value.get("ExportManifest") or {}
    manifest = [OrderedDict(d) for d in _as_list(root.get("Details"))]
    listed = {d.get("FileName") for d in manifest}
    present = set(names) - {"Manifest.xml"}
    for missing in sorted(listed - present):
        warnings.append("Manifest.xml lists %s, but it is not in the bundle" % missing)
    for extra in sorted(present - listed):
        warnings.append("%s is in the bundle but not listed in Manifest.xml" % extra)
    return manifest


def _build_evaluation_log(entry):
    if entry is None or not isinstance(entry.value, dict):
        return []
    return _evaluation_log_from_trails((entry.value.get("AuditTrails") or {}).get("AuditTrail"))


def _evaluation_log_from_trails(trails_xml):
    trails = []
    for trail in _as_list(trails_xml):
        trail = _typed(trail)
        entries = (trail.get("AuditTrailEntries") or {}).get("AuditTrailEntry")
        trail["AuditTrailEntries"] = _as_list(entries)
        trails.append(trail)
    return trails


# ---------------------------------------------------------------------------
# Chromatograms
# ---------------------------------------------------------------------------

def _array(entry, member):
    child = entry.children.get(member) if entry is not None else None
    if child is None or not isinstance(child.value, list):
        return None
    return np.asarray(child.value, dtype=np.float32)


def _regular_grid(xml, n_points, where, warnings):
    """
    The axis named by IsoChroneType ('Time' or 'Volume'), for curves stored on a regular
    grid: point i is at DistanceToStartPoint + i * DistanceBetweenPoints, on the method
    (unadjusted) axis.
    """
    axis = xml.get("IsoChroneType")
    if axis not in ("Time", "Volume"):
        warnings.append("%s: IsoChroneType is %r; regular axis not built" % (where, axis))
        return None
    if xml.get("CompressionDesc"):
        warnings.append("%s: compressed curve (%r); %s axis not built" % (where, xml.get("CompressionDesc"),
                                                                          axis.lower()))
        return None
    start = _float(xml.get("DistanceToStartPoint"))
    step = _float(xml.get("DistanceBetweenPoints"))
    if start is None or step is None:
        warnings.append("%s: no DistanceToStartPoint/DistanceBetweenPoints; %s axis not built"
                        % (where, axis.lower()))
        return None
    return start + np.arange(n_points, dtype=np.float64) * step


def _build_curve(xml, entries, where, warnings):
    # Curves on a regular volume grid (e.g. derived baselines from evaluation, UNICORN 7.11)
    # store only amplitudes; the volume axis comes from the grid and there is no time axis.
    volume_grid = xml.get("IsoChroneType") == "Volume"
    # Curves on a regular time grid with an empty VolumeUnit (e.g. UV ratio curves made in
    # evaluation) also store only amplitudes: the time axis comes from the grid, no volume axis.
    time_only = xml.get("IsoChroneType") == "Time" and not xml.get("VolumeUnit")
    point_sets = []
    for cp in _as_list((xml.get("CurvePoints") or {}).get("CurvePoint")):
        file_name = cp.get("BinaryCurvePointsFileName")
        entry = entries.get(file_name)
        if entry is None:
            warnings.append("%s: point file %s not in the bundle" % (where, file_name))
        volume = _array(entry, "CoordinateData.Volumes")
        values = _array(entry, "CoordinateData.Amplitudes")
        if values is not None and volume is None and volume_grid:
            volume = _regular_grid(xml, len(values), where, warnings)
        elif values is not None and volume is None and time_only:
            pass
        elif entry is not None and (volume is None or values is None):
            warnings.append("%s: %s has no decodable Volumes/Amplitudes" % (where, file_name))
        if volume is not None and values is not None and len(volume) != len(values):
            warnings.append("%s: %s has %d volumes but %d values" % (where, file_name, len(volume), len(values)))
        point_sets.append(CurvePointSet(file_name, _bool(cp.get("IsFullResolution")), volume, values))

    primary = next((ps for ps in point_sets if ps.is_full_resolution), point_sets[0] if point_sets else None)
    volume = primary.volume if primary else None
    values = primary.values if primary else None
    n_points = len(values) if values is not None else 0
    time = None
    if values is not None and not volume_grid:
        time = _regular_grid(xml, n_points, where, warnings)

    uv_info = xml.get("CurveUVInfo")
    return Curve(
        name=xml.get("Name"),
        data_type=xml.get("@CurveDataType"),
        unit=xml.get("AmplitudeUnit"),
        precision=_int(xml.get("AmplitudePrecision")),
        curve_number=_int(xml.get("CurveNumber")),
        time=time,
        volume=volume,
        values=values,
        time_unit=xml.get("TimeUnit"),
        volume_unit=xml.get("VolumeUnit"),
        isochrone=xml.get("IsoChroneType"),
        is_original=_bool(xml.get("IsOriginalData")),
        column_volume=_float(xml.get("ColumnVolume")),
        column_volume_unit=xml.get("ColumnVolumeUnitName"),
        uv_info=_typed(uv_info) if isinstance(uv_info, dict) else None,
        point_sets=point_sets,
        xml=xml,
    )


def _build_event_curve(xml):
    events = []
    for e in _as_list((xml.get("Events") or {}).get("Event")):
        events.append(Event(
            time=_float(e.get("EventTime")),
            volume=_float(e.get("EventVolume")),
            text=e.get("EventText"),
            type=e.get("@EventType"),
            subtype=e.get("@EventSubType"),
            instruction_feedback=_int(e.get("InstructionFeedback")),
            xml=e,
        ))
    return EventCurve(
        name=xml.get("Name"),
        type=xml.get("@EventCurveType"),
        curve_number=_int(xml.get("CurveNumber")),
        time_unit=xml.get("TimeUnit"),
        volume_unit=xml.get("VolumeUnit"),
        is_original=_bool(xml.get("IsOriginalData")),
        column_volume=_float(xml.get("ColumnVolume")),
        column_volume_unit=xml.get("ColumnVolumeUnitName"),
        events=events,
        xml=xml,
    )


def _build_peak_table(xml):
    info = _typed(OrderedDict((k, v) for k, v in xml.items() if k != "Peaks"))
    peaks = [_typed(p) for p in _as_list((xml.get("Peaks") or {}).get("Peak"))]
    return PeakTable(
        name=xml.get("Name"),
        curve_number=_int((xml.get("DataCurve") or {}).get("CurveNumber")),
        info=info,
        peaks=peaks,
        xml=xml,
    )


def _unique_key(mapping, name, number, where, warnings):
    if name not in mapping:
        return name
    key = "%s (%s)" % (name, number)
    warnings.append("%s: duplicate name %r stored as %r" % (where, name, key))
    return key


def _build_chromatogram(entry_name, root, entries, warnings, adjust_axes=True):
    chrom_name = root.get("ChromatogramName") or entry_name.rsplit(".", 1)[0]
    curves = OrderedDict()
    for xml in _as_list((root.get("Curves") or {}).get("Curve")):
        where = "%s/%s" % (chrom_name, xml.get("Name"))
        curve = _build_curve(xml, entries, where, warnings)
        curves[_unique_key(curves, curve.name, curve.curve_number, chrom_name, warnings)] = curve
    event_curves = OrderedDict()
    for xml in _as_list((root.get("EventCurves") or {}).get("EventCurve")):
        ec = _build_event_curve(xml)
        event_curves[_unique_key(event_curves, ec.name, ec.curve_number, chrom_name, warnings)] = ec
    peak_tables = [_build_peak_table(xml)
                   for xml in _as_list((root.get("PeakTables") or {}).get("PeakTable"))]
    collections = ("Curves", "EventCurves", "PeakTables", "PoolTables")
    info = OrderedDict((k, v) for k, v in root.items() if k not in collections)
    info = _typed(_expand_embedded_xml(info, warnings, entry_name))
    chrom = Chromatogram(
        name=chrom_name,
        chromatogram_id=_int(root.get("ChromatogramID")),
        info=info,
        curves=curves,
        event_curves=event_curves,
        peak_tables=peak_tables,
        pool_tables=root.get("PoolTables"),
    )
    chrom.retention_zero = _retention_zero(chrom, warnings, adjust_axes)
    _apply_retention_zero(chrom)
    return chrom


def _get(tree, *keys):
    for key in keys:
        if not isinstance(tree, dict):
            return None
        tree = tree.get(key)
    return tree


def _injection_events(chrom):
    """Events of the injection event curve the curves refer to, else of the first one of type Injection."""
    linked = {_int(_get(c.xml, "InjectionEventCurve", "CurveNumber")) for c in chrom.curves.values()}
    injection_curves = [ec for ec in chrom.event_curves.values() if ec.curve_number in linked]
    injection_curves += chrom.event_curves_of_type("Injection")
    return injection_curves[0].events if injection_curves else []


def _retention_zero(chrom, warnings, adjust_axes=True):
    """
    Where UNICORN puts zero on the x axis. Its chromatogram layout can adjust retention
    so that injection N is at zero (pre-injection data then plots at negative volume).
    The layout setting is used; the UI settings (UNICORN 7.11+) and then the peak table's
    setting are the fallbacks. With no setting at all, zero is at injection 1, as in
    UNICORN's text export (source 'default').

    With `adjust_axes=False` nothing is adjusted (source 'disabled'): the adjusted axes
    equal the stored ones.
    """
    if not adjust_axes:
        return OrderedDict([("adjusted", False), ("injection_number", None), ("time", 0.0),
                            ("volume", 0.0), ("source", "disabled")])
    events = _injection_events(chrom)
    x_scale = _get(chrom.info, "ChromatogramLayoutXml", "ChromatogramLayout", "Axis", "AxisScales",
                   "XAxisScale")
    adjusted = _get(x_scale, "AdjustRetentionZeroToInjectionNumber")
    number = _get(x_scale, "InjectionNumberForRetentionAdjustment")
    source = "layout"
    # 7.11 UI settings: flags are 'True'/'False' text, which `_typed` leaves as strings
    ui_axis = _get(chrom.info, "UISettings", "ChromatogramLayoutSettings", "ScaleSettings", "XAxis")
    if _get(ui_axis, "IsAdjustToZeroAtPhaseSelected") == "True":
        return _phase_zero(chrom, ui_axis, warnings)
    if not isinstance(adjusted, bool) and _get(ui_axis, "IsAdjustToZeroAtInjectionSelected") in ("True", "False"):
        adjusted = _get(ui_axis, "IsAdjustToZeroAtInjectionSelected") == "True"
        number = _get(ui_axis, "AdjustRetentionToZeroAtInjection")
        source = "ui_settings"
    if not isinstance(adjusted, bool):
        numbers = [t.info.get("ZeroAdjustedToInjectionNumber") for t in chrom.peak_tables]
        numbers = [n for n in numbers if isinstance(n, int) and not isinstance(n, bool) and n > 0]
        if numbers:
            adjusted, number, source = True, numbers[0], "peak_table"
        elif events:
            warnings.append("%s: no retention-zero setting found; axes zeroed at injection 1 "
                            "(UNICORN's default)" % chrom.name)
            adjusted, number, source = True, 1, "default"
        else:
            warnings.append("%s: no retention-zero setting and no injection; axes start at method start"
                            % chrom.name)
            adjusted, source = False, "none"
    zero = OrderedDict([("adjusted", adjusted), ("injection_number", number if adjusted else None),
                        ("time", 0.0), ("volume", 0.0), ("source", source)])
    if not adjusted:
        return zero

    if not isinstance(number, int) or not 1 <= number <= len(events):
        warnings.append("%s: retention is adjusted to injection %r, but the run has %d injection(s); "
                        "adjusted axes not built" % (chrom.name, number, len(events)))
        zero["time"] = zero["volume"] = None
        return zero
    zero["time"] = events[number - 1].time
    zero["volume"] = events[number - 1].volume
    return zero


def _phase_matches(row_name, wanted):
    """A phase-table name ('Phase Elution') matches 'Phase Elution' or 'Elution'."""
    if not row_name or not wanted:
        return False
    return wanted.strip() in (row_name, row_name[len("Phase "):] if row_name.startswith("Phase ") else row_name)


def _phase_zero(chrom, ui_axis, warnings):
    """
    Retention zero at a phase start (UNICORN 7.11 UI settings). No sample has this set:
    `AdjustRetentionToZeroAtPhase` is read as the phase name and `AdjustRetentionToZeroAtPhaseX`
    as which occurrence of it (0 = first). Unverified, so it always warns.
    """
    from ..event_tables import phase_rows
    name = _get(ui_axis, "AdjustRetentionToZeroAtPhase")
    occurrence = _get(ui_axis, "AdjustRetentionToZeroAtPhaseX")
    occurrence = occurrence if isinstance(occurrence, int) and not isinstance(occurrence, bool) else 0
    zero = OrderedDict([("adjusted", True), ("injection_number", None), ("time", None), ("volume", None),
                        ("source", "ui_settings_phase"), ("phase", name), ("phase_occurrence", occurrence)])
    matches = [r for r in phase_rows(chrom) if not r["pseudo"] and _phase_matches(r["name"], name)]
    if 0 <= occurrence < len(matches):
        zero["time"], zero["volume"] = matches[occurrence]["start_time"], matches[occurrence]["start_volume"]
        warnings.append("%s: retention zero at phase %r (occurrence %d) from the UI settings; reading of "
                        "this setting is unverified" % (chrom.name, name, occurrence))
    else:
        warnings.append("%s: retention zero is set to phase %r (occurrence %d), but the run log has %d such "
                        "phase(s); adjusted axes not built" % (chrom.name, name, occurrence, len(matches)))
    return zero


def _apply_retention_zero(chrom):
    zero_time = chrom.retention_zero["time"]
    zero_volume = chrom.retention_zero["volume"]
    for curve in chrom.curves.values():
        if zero_time is not None and curve.time is not None:
            curve.time_adjusted = curve.time - zero_time
        if zero_volume is not None and curve.volume is not None:
            curve.volume_adjusted = curve.volume.astype(np.float64) - zero_volume
    for ec in chrom.event_curves.values():
        for e in ec.events:
            if zero_time is not None and e.time is not None:
                e.time_adjusted = e.time - zero_time
            if zero_volume is not None and e.volume is not None:
                e.volume_adjusted = e.volume - zero_volume


def _build_metadata(entries, used, warnings):
    """The *Data entries (method, system, strategy, ...): every entry not in `used`, decoded."""
    metadata = OrderedDict()
    for name, entry in entries.items():
        if name in used:
            continue
        if entry.kind == "zip":
            values = OrderedDict((m, c.value) for m, c in entry.children.items()
                                 if c.kind != "text" or not m.endswith(_DATA_TYPE_SUFFIX))
            value = values["Xml"] if list(values) == ["Xml"] else values
        else:
            value = entry.value
        if entry.kind == "unknown":
            warnings.append("%s: kept only as raw bytes in run.entries" % name)
        metadata[name] = _expand_embedded_xml(value, warnings, name)
    return metadata


# ---------------------------------------------------------------------------
# Public reader
# ---------------------------------------------------------------------------

def read_zip(path, adjust_axes=True, axes_options=None):
    """
    Read a UNICORN 6+ result bundle into a `Run`.

    `adjust_axes=False` skips the retention-zero adjustment: `time_adjusted`/`volume_adjusted`
    then equal the stored axes and `retention_zero["source"]` is 'disabled'.
    `axes_options` are passed to `pycorn.axes.add_axes` (CV and UTC), e.g. a column volume.
    """
    warnings = []
    entries = OrderedDict()
    with ZipFile(path) as bundle:
        for info in bundle.infolist():
            entries[info.filename] = _read_entry(info.filename, bundle.read(info), warnings)

    used = set(_STRUCTURED_XML)
    chromatograms = OrderedDict()
    for name, entry in entries.items():
        if entry.kind == "xml" and isinstance(entry.value, dict) and "Chromatogram" in entry.value:
            chrom = _build_chromatogram(name, entry.value["Chromatogram"], entries, warnings, adjust_axes)
            # chrom.name is exactly as named; a repeated name gets a unique key (and a warning)
            chromatograms[_unique_key(chromatograms, chrom.name, name, "result", warnings)] = chrom
            used.add(name)
            for curve in chrom.curves.values():
                used.update(ps.file_name for ps in curve.point_sets)
    if "Result.xml" not in entries and not chromatograms:
        raise ValueError("%s: %s" % (os.path.basename(str(path)) if isinstance(path, (str, os.PathLike))
                                     else "this file", NOT_A_RESULT))

    run = Run(
        source="zip",
        locator=str(path),
        result=_build_result(entries.get("Result.xml"), warnings),
        chromatograms=chromatograms,
        metadata=_build_metadata(entries, used, warnings),
        evaluation_log=_build_evaluation_log(entries.get("EvaluationLog.xml")),
        manifest=_build_manifest(entries.get("Manifest.xml"), list(entries), warnings),
        entries=entries,
        origin=file_origin(path),
    )
    run.warnings.extend(warnings)
    add_axes(run, **(axes_options or {}))
    return run


def file_origin(path):
    """`run.origin` for a file: its name and absolute path (both None for an in-memory file)."""
    if isinstance(path, (str, os.PathLike)):
        return OrderedDict([("file_name", os.path.basename(path)), ("path", os.path.abspath(path))])
    return OrderedDict([("file_name", None), ("path", None)])
