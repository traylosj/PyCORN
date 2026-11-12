# -*- coding: utf-8 -*-
"""
Column geometry of a run: bed height, inner diameter and particle diameter, in SI, for exports
such as CADET.

Source: the run's own copy of its column type (`ColumnTypeData`; the database stores the same
bytes in `ResultAuditTrail.ColumnTypeData`, so ZIP, .UFol and database runs read alike). The
column the run used is the one UNICORN named at run time in the 'ColumnInformation' run
information (`usedColumns`). The name 'Any' means no column type was chosen: no geometry.

Tested against a lab environment (2026-11-08): few results name a column; those that do hold
its record, with BedHeight (cm), Hardware/Diameter (cm) and
Media/AverageParticleDiameter (um), and pi*d^2/4*L agrees with the run's Vc within 0.5 %.
Most results use 'Any' or name no column. The database's column library
(`ColumnType`/`ColumnHardware`/`ColumnMedia`) is linked to results by name only and holds
today's values, not those at run time, so it is not read.

Users are not expected to reference column definitions (owner, 2026-11-08): geometry is read
only when the run links a column type and the run holds its record. Otherwise the run has CV
only and no geometry; nothing is looked up by name, in the library, from a volume in the column
name or from Vc.

Stored geometry is never a source of Vc (owner, 2026-11-08): the geometric volume is only
checked against the run's Vc (`run.column_volume`). Owner, D26 (2026-11-08): geometry the
**caller** gives may give Vc as a last resort, only when no other Vc source is usable
(`column_volume.py`, source 'geometry'); then there is nothing to check here.

Caller values (D26): a fallback per value by default (used only where the run stores none; a
stored value wins and the unused caller value is always noted), or `override=True` (the
caller wins, for an instrument that was set up wrong). Each value records where it came from. `TotalLiquidVolume` and `VoidVolume` are kept as stored, as hints
only: no porosity is computed from them.
"""
import math
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Optional

from .column_volume import DISAGREEMENT
from .units import UnitError, conversion_factor

NO_COLUMN = "Any"  # UNICORN's column name when no column type was chosen

# stored record fields kept as hints: (label, path in the ColumnType record, unit path)
HINT_FIELDS = (
    ("article_number", ("ArticleNumber",), None),
    ("media", ("Media", "Name"), None),
    ("technique", ("Media", "TechniqueName"), None),
    ("hardware", ("Hardware", "Name"), None),
    ("total_liquid_volume", ("TotalLiquidVolume",), ("ColumnVolumeUnitName",)),
    ("void_volume", ("VoidVolume",), ("ColumnVolumeUnitName",)),
)


@dataclass
class ColumnGeometry:
    name: Optional[str]  # column type name, or None
    length_m: Optional[float] = None
    diameter_m: Optional[float] = None
    particle_radius_m: Optional[float] = None
    cross_section_area_m2: Optional[float] = None
    volume_m3: Optional[float] = None  # pi * d^2 / 4 * L; a cross-check, never Vc
    sources: dict = field(default_factory=dict)  # 'length' / 'diameter' / 'particle_diameter' -> source
    stored: dict = field(default_factory=OrderedDict)  # values and units as stored
    hints: dict = field(default_factory=OrderedDict)  # see HINT_FIELDS
    vc_check: Optional[dict] = None  # run Vc vs volume_m3
    warnings: list = field(default_factory=list)


def _as_list(value):
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _get(record, path):
    for key in path:
        if not isinstance(record, dict):
            return None
        record = record.get(key)
    return record


def _positive_float(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 and math.isfinite(value) else None


def used_columns(run):
    """The columns UNICORN named at run time: [(name, volume, volume unit)], 'Any' included."""
    out = []
    for ri in (run.result or {}).get("run_information") or []:
        if ri.get("type") != "ColumnInformation" or not isinstance(ri.get("value"), dict):
            continue
        used = ri["value"].get("usedColumns") or {}
        for column in _as_list(used.get("column") if isinstance(used, dict) else None):
            if isinstance(column, dict):
                out.append((column.get("name"), column.get("volume"), column.get("volumeUnit")))
    return out


def column_types(run):
    """The ColumnType records in the run's `ColumnTypeData` (usually zero or one)."""
    data = (run.metadata or {}).get("ColumnTypeData")
    types = _get(data, ("ColumnTypes", "ColumnType")) if isinstance(data, dict) else None
    return [t for t in _as_list(types) if isinstance(t, dict)]


def _to_m(value, unit, what, warnings):
    try:
        return value * conversion_factor(unit, "m")
    except UnitError as e:
        warnings.append("column %s not used (%s)" % (what, e))
        return None


CALLER_VALUES = (("length", "bed height"), ("diameter", "diameter"), ("particle_diameter", "particle diameter"))
CALLER_ARGS = {"length": "bed_height", "diameter": "diameter", "particle_diameter": "particle_diameter"}


def column_geometry(run, name=None, bed_height=None, diameter=None, particle_diameter=None, override=False,
                    fallback_sources=None):
    """
    `ColumnGeometry` of the column `run` used.

    `name` picks a column type by name when the run names more than one, or none.
    `bed_height`, `diameter` and `particle_diameter` are the caller's values, each a (value, unit)
    pair, e.g. (2.5, 'cm'), (85, 'um'). By default (D26) each is a fallback: used only where the
    run stores none (no linked column, or that one value missing), with the source
    `fallback_sources.get('length' | 'diameter' | 'particle_diameter')`, by default
    'fallback (bed_height=)' / 'fallback (diameter=)' / 'fallback (particle_diameter=)' (the
    argument given, as the export's 'fallback (--bed-height)'), and a warning;
    a stored value wins, and the unused caller value is always noted. `override=True` makes the
    caller's values win (source 'caller'), also noted, for an instrument that was set up wrong.
    Missing values stay None, with a warning.
    """
    warnings = []
    named = [n for n, _, _ in used_columns(run) if n and n != NO_COLUMN]
    if name is None:
        if len(named) == 1:
            name = named[0]
        elif len(named) > 1:
            warnings.append("the run names %d columns (%s); pass name= to pick one" % (len(named), ", ".join(named)))
        elif used_columns(run):
            warnings.append("no column type was chosen for the run (column %r)" % NO_COLUMN)
        else:
            warnings.append("the run has no ColumnInformation naming its column")
    geometry = ColumnGeometry(name=name, warnings=warnings)

    record = None
    if name is not None:
        record = next((t for t in column_types(run) if t.get("Name") == name), None)
        if record is None:
            warnings.append("column type %r is not in the run's ColumnTypeData" % name)

    values = OrderedDict()  # what -> (value, unit, source)
    if record is not None:
        for what, path, unit_path in (("length", ("BedHeight",), ("BedHeightUnit",)),
                                      ("diameter", ("Hardware", "Diameter"), ("Hardware", "DiameterUnit")),
                                      ("particle_diameter", ("Media", "AverageParticleDiameter"),
                                       ("Media", "AverageParticleDiameterUnit"))):
            raw, unit = _get(record, path), _get(record, unit_path)
            geometry.stored[what] = (raw, unit)
            value = _positive_float(raw)
            if value is not None:
                values[what] = (value, unit, "ColumnTypeData")
        for label, path, unit_path in HINT_FIELDS:
            raw = _get(record, path)
            if raw not in (None, ""):
                geometry.hints[label] = (raw, _get(record, unit_path)) if unit_path else raw
    given_values = {"length": bed_height, "diameter": diameter, "particle_diameter": particle_diameter}
    for what, label in CALLER_VALUES:
        given = given_values[what]
        if given is None:
            continue
        value, unit = given
        if _positive_float(value) is None:
            raise ValueError("%s must be a positive number, got %r" % (what, value))
        stored = values.get(what)
        if override:
            if stored is not None:
                warnings.append("column %s: given %g %s used over the stored %g %s (override)"
                                % (label, float(value), unit, stored[0], stored[1]))
            values[what] = (float(value), unit, "caller")
        elif stored is not None:
            warnings.append("column %s: stored %g %s used; given %g %s not used (fallback only where the run "
                            "stores none; override to use it)" % (label, stored[0], stored[1], float(value), unit))
        else:
            source = (fallback_sources or {}).get(what, "fallback (%s=)" % CALLER_ARGS[what])
            warnings.append("column %s: none stored; given %g %s used (%s)" % (label, float(value), unit, source))
            values[what] = (float(value), unit, source)

    si = {}
    for what in ("length", "diameter", "particle_diameter"):
        if what not in values:
            warnings.append("no column %s" % what.replace("_", " "))
            continue
        value, unit, source = values[what]
        si[what] = _to_m(value, unit, what.replace("_", " "), warnings)
        if si[what] is not None:
            geometry.sources[what] = source
    geometry.length_m = si.get("length")
    geometry.diameter_m = si.get("diameter")
    if si.get("particle_diameter") is not None:
        geometry.particle_radius_m = si["particle_diameter"] / 2
    if geometry.diameter_m is not None:
        geometry.cross_section_area_m2 = math.pi * geometry.diameter_m ** 2 / 4
        if geometry.length_m is not None:
            geometry.volume_m3 = geometry.cross_section_area_m2 * geometry.length_m
    geometry.vc_check = _vc_check(run, geometry, warnings)
    return geometry


def _vc_check(run, geometry, warnings):
    """Compare the geometric volume with the run's Vc; warn above DISAGREEMENT."""
    if geometry.volume_m3 is None or run.column_volume is None:
        return None
    if run.column_volume_source == "geometry":
        warnings.append("the run's Vc was computed from the given geometry (last resort, D26); nothing to check")
        return None
    try:
        vc_m3 = run.column_volume * conversion_factor(run.column_volume_unit, "m^3")
    except UnitError as e:
        warnings.append("column volume not compared with the geometry (%s)" % e)
        return None
    rel = (geometry.volume_m3 - vc_m3) / vc_m3
    if abs(rel) > DISAGREEMENT:
        warnings.append("column geometry gives %.4g ml but the run's Vc is %.4g ml (%+.1f %%)"
                        % (geometry.volume_m3 * 1e6, vc_m3 * 1e6, rel * 100))
    return OrderedDict([("run_vc_m3", vc_m3), ("run_vc_source", run.column_volume_source),
                        ("geometry_m3", geometry.volume_m3), ("relative_difference", rel)])
