# -*- coding: utf-8 -*-
"""
Units: a lookup table from unit spellings to a quantity and a factor to that quantity's
canonical unit, conversion between spellings, SI conversion, and UV path lengths.

Stored values and unit strings are never changed: conversions return new values. Units
not in `UNITS` are unknown; nothing here guesses a conversion for them. Add a unit by adding
one line to `UNITS` (a new quantity also needs its canonical unit in `CANONICAL`).

Only the units in docs/PHASE0_FINDINGS.md ("Units inventory") occur in real data so far;
the others (L, L/h, AU, ...) are tested on synthetic values only.

`to_si()` converts to the SI unit of a quantity (`SI_UNITS`), for exports such as CADET.
Quantities without an SI form (%, absorbance, extinction, temperature in degrees C) raise
UnitError there; the export decides how to handle them.

Display precision (owner, 2026-11-10): how many decimals a value is shown with comes from what
UNICORN specifies, not from the digits of the value: a curve's `AmplitudePrecision`
(`Curve.precision`), and for time, volume, CV, flow and %B the decimals UNICORN prints in its
run log (`DISPLAY_DECIMALS`). Every conversion carries the decimals with the value
(`convert_decimals()`): ml -> L adds 3 (same significant figures), min -> s takes 2 to 1.
Values are never rounded here; only the page and Excel's cell formats round
(`pycorn.display_precision`).
"""
import math
from collections import OrderedDict, namedtuple

import numpy as np

UnitInfo = namedtuple("UnitInfo", "unit quantity factor canonical")

# quantity -> canonical unit (the unit with factor 1)
CANONICAL = {
    "volume": "ml",
    "time": "min",
    "flow": "ml/min",
    "linear_flow": "cm/h",
    "cv_rate": "CV/h",
    "absorbance": "mAU",
    "absorbance_per_length": "mAU/cm",  # already divided by the path length in cm
    "conductivity": "mS/cm",
    "pressure": "MPa",
    "length": "cm",
    "percent": "%",
    "ratio": "Ratio",
    "temperature": "°C",
    "mass_concentration": "mg/ml",
    "molar_concentration": "mM",  # = mol/m^3
    "mass_extinction": "ml/(mg*cm)",  # 'A 0.1%' / 'E 1 mg/ml': absorbance of 1 mg/ml over 1 cm
    "molar_extinction": "1/(M*cm)",
    "molar_mass": "g/mol",
    "volume_absorbance": "ml*mAU",  # peak areas
    "mass": "mg",  # injected amounts (`pycorn.concentration.epsilon_from_injection`)
}

# spelling -> (quantity, factor to the canonical unit). Case matters: 'mS' is not 'MS'.
UNITS = {
    "ml": ("volume", 1.0), "mL": ("volume", 1.0),
    "l": ("volume", 1000.0), "L": ("volume", 1000.0),
    "ul": ("volume", 1e-3), "uL": ("volume", 1e-3), "µl": ("volume", 1e-3), "µL": ("volume", 1e-3),
    "μl": ("volume", 1e-3), "μL": ("volume", 1e-3),
    "min": ("time", 1.0), "s": ("time", 1.0 / 60), "sec": ("time", 1.0 / 60), "h": ("time", 60.0),
    "m^3": ("volume", 1e6),
    "ml/min": ("flow", 1.0), "mL/min": ("flow", 1.0), "ml/h": ("flow", 1.0 / 60), "mL/h": ("flow", 1.0 / 60),
    "l/min": ("flow", 1000.0), "L/min": ("flow", 1000.0), "l/h": ("flow", 1000.0 / 60), "L/h": ("flow", 1000.0 / 60),
    "ul/min": ("flow", 1e-3), "µl/min": ("flow", 1e-3), "μl/min": ("flow", 1e-3),
    "m^3/s": ("flow", 6e7),
    "cm/h": ("linear_flow", 1.0), "m/s": ("linear_flow", 360000.0),
    "CV/h": ("cv_rate", 1.0), "1/s": ("cv_rate", 3600.0),
    "mAU": ("absorbance", 1.0), "AU": ("absorbance", 1000.0),
    "mAU/cm": ("absorbance_per_length", 1.0), "AU/cm": ("absorbance_per_length", 1000.0),
    "mS/cm": ("conductivity", 1.0), "uS/cm": ("conductivity", 1e-3), "µS/cm": ("conductivity", 1e-3),
    "S/m": ("conductivity", 10.0),
    "μS/cm": ("conductivity", 1e-3),
    "MPa": ("pressure", 1.0), "kPa": ("pressure", 1e-3), "Pa": ("pressure", 1e-6), "bar": ("pressure", 0.1), "psi": ("pressure", 0.00689475729),
    "cm": ("length", 1.0), "mm": ("length", 0.1), "m": ("length", 100.0),
    "um": ("length", 1e-4), "µm": ("length", 1e-4), "μm": ("length", 1e-4),
    "%": ("percent", 1.0),
    "Ratio": ("ratio", 1.0),
    "°C": ("temperature", 1.0),
    "mg/ml": ("mass_concentration", 1.0), "mg/mL": ("mass_concentration", 1.0),
    "g/l": ("mass_concentration", 1.0), "g/L": ("mass_concentration", 1.0),
    "kg/m^3": ("mass_concentration", 1.0),
    "ug/ml": ("mass_concentration", 1e-3), "ug/mL": ("mass_concentration", 1e-3),
    "µg/ml": ("mass_concentration", 1e-3), "µg/mL": ("mass_concentration", 1e-3),
    "μg/ml": ("mass_concentration", 1e-3), "μg/mL": ("mass_concentration", 1e-3),
    "mM": ("molar_concentration", 1.0), "mol/m^3": ("molar_concentration", 1.0),
    "mmol/l": ("molar_concentration", 1.0), "mmol/L": ("molar_concentration", 1.0),
    "M": ("molar_concentration", 1000.0), "mol/l": ("molar_concentration", 1000.0),
    "mol/L": ("molar_concentration", 1000.0),
    "uM": ("molar_concentration", 1e-3), "µM": ("molar_concentration", 1e-3), "μM": ("molar_concentration", 1e-3),
    "ml/(mg*cm)": ("mass_extinction", 1.0), "mL/(mg*cm)": ("mass_extinction", 1.0),
    "l/(g*cm)": ("mass_extinction", 1.0), "L/(g*cm)": ("mass_extinction", 1.0),
    "1/(M*cm)": ("molar_extinction", 1.0), "M^-1 cm^-1": ("molar_extinction", 1.0),
    "l/(mol*cm)": ("molar_extinction", 1.0), "L/(mol*cm)": ("molar_extinction", 1.0),
    "g/mol": ("molar_mass", 1.0), "Da": ("molar_mass", 1.0), "kDa": ("molar_mass", 1000.0),
    "kg/mol": ("molar_mass", 1000.0),
    "ml*mAU": ("volume_absorbance", 1.0), "mL*mAU": ("volume_absorbance", 1.0),
    "l*mAU": ("volume_absorbance", 1000.0), "L*mAU": ("volume_absorbance", 1000.0),
    "mg": ("mass", 1.0), "g": ("mass", 1000.0), "kg": ("mass", 1e6),
    "ug": ("mass", 1e-3), "µg": ("mass", 1e-3), "μg": ("mass", 1e-3),
}

# quantity -> SI unit (a spelling in UNITS). 'cv_rate' (CV/h) becomes column volumes per second.
SI_UNITS = {
    "volume": "m^3",
    "time": "s",
    "flow": "m^3/s",
    "linear_flow": "m/s",
    "cv_rate": "1/s",
    "conductivity": "S/m",
    "pressure": "Pa",
    "length": "m",
    "mass_concentration": "kg/m^3",
    "molar_concentration": "mol/m^3",
    "molar_mass": "kg/mol",
    "mass": "kg",
}

# UNICORN stores CurveUVInfo path lengths without a unit; they are mm (F9, Phase 0: 2 mm and
# 0.5 mm flow cells), while the 'UV cell path length' curve is in cm
UV_INFO_PATH_UNIT = "mm"

# unit -> decimals UNICORN prints values in this unit with in its run log ('Length 5.00 {CV}',
# '8.000 {ml/min}', 'Vc=0.700 {ml}': ml is printed with 0-3, the most is kept). UNICORN stores no
# precision for the time and volume axes or for CV (docs/FACTS.md F15); these are the rule
# (owner, 2026-11-10). Other units of the same quantity get theirs from `convert_decimals()`
# (s: 1, h: 4, L: 6, L/h: 5); 'CV' is not in UNITS (no other unit to convert to).
DISPLAY_DECIMALS = {"min": 2, "ml": 3, "ml/min": 3, "CV": 2, "%": 1}


class UnitError(ValueError):
    """A unit is unknown, or two units measure different quantities."""


def unit_info(unit):
    """`UnitInfo` for a unit spelling, or None if the unit is missing or not in `UNITS`."""
    if unit is None:
        return None
    spec = UNITS.get(unit.strip())
    if spec is None:
        return None
    quantity, factor = spec
    return UnitInfo(unit, quantity, factor, CANONICAL[quantity])


def quantity(unit):
    """The quantity a unit measures ('volume', 'flow', ...), or None if unknown."""
    info = unit_info(unit)
    return info.quantity if info else None


def conversion_factor(from_unit, to_unit):
    """Multiply values in `from_unit` by this to get `to_unit`. Raises UnitError."""
    a, b = unit_info(from_unit), unit_info(to_unit)
    for unit, info in ((from_unit, a), (to_unit, b)):
        if info is None:
            raise UnitError("unknown unit %r (known: %s)" % (unit, ", ".join(sorted(UNITS))))
    if a.quantity != b.quantity:
        raise UnitError("cannot convert %r (%s) to %r (%s)" % (from_unit, a.quantity, to_unit, b.quantity))
    return a.factor / b.factor


def convert(values, from_unit, to_unit):
    """`values` (a number or array) converted from `from_unit` to `to_unit`. Raises UnitError."""
    factor = conversion_factor(from_unit, to_unit)
    if values is None:
        return None
    if isinstance(values, np.ndarray):
        return values.astype(np.float64) * factor
    return values * factor


def convert_decimals(decimals, factor):
    """
    The decimals for values multiplied by `factor`, given `decimals` before: the step one last
    decimal stands for, converted, and never shown coarser (ceil(decimals - log10(factor)), at
    least 0). Powers of 10 shift exactly (ml -> L: 3 -> 6, the same significant figures); 0.01
    min is 0.6 s, so min -> s takes 2 to 1. None when `decimals` or `factor` is missing or 0.
    """
    if decimals is None or not factor:
        return None
    # the small tolerance keeps exact powers of 10 exact (log10(0.001) is -2.9999999999999996)
    return max(0, int(math.ceil(decimals - math.log10(abs(factor)) - 1e-9)))


def display_decimals(unit):
    """
    Decimals values in `unit` are shown with: `DISPLAY_DECIMALS`, or for another unit of the
    same quantity converted by `convert_decimals()`. None for units without a rule (absorbance,
    conductivity, ...: their curves carry their own `precision`) and unknown units.
    """
    if not isinstance(unit, str) or not unit.strip():
        return None
    unit = unit.strip()
    if unit in DISPLAY_DECIMALS:
        return DISPLAY_DECIMALS[unit]
    info = unit_info(unit)
    if info is None:
        return None
    for base, decimals in DISPLAY_DECIMALS.items():
        if quantity(base) == info.quantity:
            return convert_decimals(decimals, conversion_factor(base, unit))
    return None


def si_unit(unit):
    """The SI unit for `unit`'s quantity (see `SI_UNITS`). Raises UnitError if unknown or not SI-convertible."""
    info = unit_info(unit)
    if info is None:
        raise UnitError("unknown unit %r (known: %s)" % (unit, ", ".join(sorted(UNITS))))
    if info.quantity not in SI_UNITS:
        raise UnitError("%r (%s) has no SI unit here (SI quantities: %s)"
                        % (unit, info.quantity, ", ".join(sorted(SI_UNITS))))
    return SI_UNITS[info.quantity]


def to_si(values, unit):
    """(`values` in SI, SI unit, factor) for values in `unit`. Raises UnitError (see `si_unit()`)."""
    target = si_unit(unit)
    factor = conversion_factor(unit, target)
    return convert(values, unit, target), target, factor


def uv_path_length_cm(curve):
    """
    The path length (cm) a UV curve's values are relative to, and where it came from:
    (value, basis), or (None, reason).

    - `mAU/cm`-type units are already per cm: (1.0, 'per_cm_unit'). Never scale them again.
    - Plain `mAU`/`AU`: from the curve's own CurveUVInfo, the nominal path if
      `IsUVValuesNormalizedToNominalUVPathLength`, else the actual `UVPathLength` (mm -> cm).
    """
    info = unit_info(curve.unit)
    if info is not None and info.quantity == "absorbance_per_length":
        return 1.0, "per_cm_unit"
    if info is None or info.quantity != "absorbance":
        return None, "unit %r is not an absorbance" % (curve.unit,)
    uv = curve.uv_info
    if not uv:
        return None, "no CurveUVInfo"
    normalized = uv.get("IsUVValuesNormalizedToNominalUVPathLength")
    key = "NominalUVPathLength" if normalized is True else "UVPathLength"
    value = uv.get(key)
    if not isinstance(value, (int, float)) or isinstance(value, bool) or value <= 0:
        return None, "CurveUVInfo has no usable %s" % key
    return float(value) * conversion_factor(UV_INFO_PATH_UNIT, "cm"), key


_uv_path_length_cm = uv_path_length_cm  # `normalized()` has a parameter of the same name


def _convert_or_keep(values, unit, target, what, warnings):
    """(converted values, new unit, factor); unchanged with a warning if not convertible."""
    try:
        factor = conversion_factor(unit, target)
    except UnitError as e:
        warnings.append("%s: not converted to %s (%s)" % (what, target, e))
        return values, unit, None
    if values is None:
        return None, target, factor
    if isinstance(values, np.ndarray):
        return values.astype(np.float64) * factor, target, factor
    return values * factor, target, factor


def check_units(units):
    """`units` ({quantity: unit}) as a dict; UnitError for an unknown quantity or a unit of another one."""
    units = dict(units or {})
    for q, target in units.items():
        if q not in CANONICAL:
            raise UnitError("unknown quantity %r (known: %s)" % (q, ", ".join(sorted(CANONICAL))))
        if quantity(target) != q:
            raise UnitError("%r is not a unit of %s" % (target, q))
    return units


def normalized(run, units=None, uv_path_length_cm=None):
    """
    A copy of `run` with converted curves, axes, events and column volume. The original run
    is not changed.

    `units` maps a quantity to the unit wanted, e.g. {'volume': 'L', 'flow': 'L/h',
    'absorbance': 'AU', 'time': 'h'}. A curve whose unit is in a listed quantity is converted;
    'volume' and 'time' also convert the volume and time axes of curves and events (CV and UTC
    don't change). `uv_path_length_cm` rescales plain absorbance curves from the path their
    values are relative to (see `uv_path_length_cm()`) to this path; `*/cm` curves are never
    rescaled. A rescaled curve's `uv_info` then says its values are normalized to that nominal
    path (the actual `UVPathLength` is kept), so concentration and CADET UV use the new path.
    Curves that can't be converted are kept as they are, with a warning.

    Each changed curve records what was done in `curve.conversion`: for the values `raw_unit`,
    `unit`, `factor` (factor 1 and the same unit when only the axes changed) and, for UV,
    `path_length_cm_from` / `path_length_cm_to`; for converted axes `axes` = {'volume' / 'time':
    {raw_unit, unit, factor}}. A converted curve's `precision` (decimals shown) is carried with
    its values (`convert_decimals()`: mAU -> AU, 3 -> 6); the axes' decimals follow their unit
    (`display_decimals()`). The chromatogram
    timelines keep their stored time unit (`timeline['time_unit']`).
    """
    import copy
    import dataclasses

    units = check_units(units)
    out = copy.copy(run)
    out.warnings = list(run.warnings)
    warnings = out.warnings
    out.chromatograms = OrderedDict()
    for key, chrom in run.chromatograms.items():
        new_chrom = copy.copy(chrom)
        new_chrom.curves = OrderedDict()
        for c_key, curve in chrom.curves.items():
            where = "%s/%s" % (chrom.name, curve.name)
            changes = {}
            conversion = OrderedDict([("raw_unit", curve.unit), ("unit", curve.unit), ("factor", 1.0)])
            values, unit = curve.values, curve.unit
            q = quantity(unit)
            if q in units and unit != units[q]:
                values, unit, factor = _convert_or_keep(values, unit, units[q], where, warnings)
                if factor is not None:
                    conversion["unit"], conversion["factor"] = unit, factor
            if uv_path_length_cm is not None and q == "absorbance":
                path, basis = _uv_path_length_cm(curve)
                if path is None:
                    warnings.append("%s: not rescaled to %g cm (%s)" % (where, uv_path_length_cm, basis))
                else:
                    scale = uv_path_length_cm / path
                    values = None if values is None else values.astype(np.float64) * scale
                    conversion["factor"] *= scale
                    conversion["path_length_cm_from"] = path
                    conversion["path_length_cm_to"] = uv_path_length_cm
                    # The values are now relative to the new path: say so, or a later
                    # `uv_path_length_cm()` (concentration, CADET) divides by the old one again
                    uv = OrderedDict(curve.uv_info)
                    uv["IsUVValuesNormalizedToNominalUVPathLength"] = True
                    uv["NominalUVPathLength"] = uv_path_length_cm / conversion_factor(UV_INFO_PATH_UNIT, "cm")
                    changes["uv_info"] = uv
            if conversion["factor"] != 1.0 or conversion["unit"] != conversion["raw_unit"]:
                changes.update(values=values, unit=unit,
                               precision=convert_decimals(curve.precision, conversion["factor"]))
            axes = OrderedDict()
            for axis, q_axis in (("volume", "volume"), ("time", "time")):
                axis_unit = getattr(curve, axis + "_unit")
                if q_axis in units and axis_unit and axis_unit != units[q_axis]:
                    for name in (axis, axis + "_adjusted"):
                        arr = getattr(curve, name)
                        new, new_unit, factor = _convert_or_keep(arr, axis_unit, units[q_axis],
                                                                 "%s %s axis" % (where, axis), warnings)
                        changes[name] = new
                    changes[axis + "_unit"] = new_unit
                    if factor is not None:
                        axes[axis] = OrderedDict([("raw_unit", axis_unit), ("unit", new_unit), ("factor", factor)])
            if axes:
                conversion["axes"] = axes
            if "values" in changes or axes:
                changes["conversion"] = conversion
            if "volume" in units and curve.column_volume is not None and curve.column_volume_unit:
                cv, cv_unit, _ = _convert_or_keep(curve.column_volume, curve.column_volume_unit, units["volume"],
                                                  where + " column volume", warnings)
                changes.update(column_volume=cv, column_volume_unit=cv_unit)
            new_chrom.curves[c_key] = dataclasses.replace(curve, **changes) if changes else curve
        new_chrom.event_curves = OrderedDict()
        for e_key, ec in chrom.event_curves.items():
            ec_changes, factors = {}, {}
            for axis in ("volume", "time"):
                axis_unit = getattr(ec, axis + "_unit")
                if axis in units and axis_unit and axis_unit != units[axis]:
                    _, new_unit, factor = _convert_or_keep(None, axis_unit, units[axis],
                                                           "%s/%s %s axis" % (chrom.name, ec.name, axis), warnings)
                    if factor is not None:
                        factors[axis] = factor
                        ec_changes[axis + "_unit"] = new_unit
            if factors:
                events = []
                for e in ec.events:
                    e_changes = {}
                    for axis, factor in factors.items():
                        for name in (axis, axis + "_adjusted"):
                            if getattr(e, name) is not None:
                                e_changes[name] = getattr(e, name) * factor
                    events.append(dataclasses.replace(e, **e_changes))
                ec_changes["events"] = events
            new_chrom.event_curves[e_key] = dataclasses.replace(ec, **ec_changes) if ec_changes else ec
        if chrom.retention_zero and chrom.retention_zero.get("source") != "disabled":
            zero = OrderedDict(chrom.retention_zero)
            for axis in ("volume", "time"):
                unit = chrom.info.get(axis.capitalize() + "Unit")
                if axis in units and zero.get(axis) is not None and unit and unit != units[axis]:
                    zero[axis], _, _ = _convert_or_keep(zero[axis], unit, units[axis],
                                                        "%s retention zero" % chrom.name, warnings)
            new_chrom.retention_zero = zero
        if "volume" in units and chrom.column_volume_info and chrom.column_volume_info.get("value") is not None:
            info = OrderedDict(chrom.column_volume_info)
            info["value"], info["unit"], _ = _convert_or_keep(info["value"], info["unit"], units["volume"],
                                                              "%s column volume" % chrom.name, warnings)
            new_chrom.column_volume_info = info
        if "volume" in units or "time" in units:
            new_chrom.info = OrderedDict(chrom.info)
            for axis in ("volume", "time"):
                if axis in units and new_chrom.info.get(axis.capitalize() + "Unit"):
                    new_chrom.info[axis.capitalize() + "Unit"] = units[axis]
        out.chromatograms[key] = new_chrom
    return out
