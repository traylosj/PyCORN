# -*- coding: utf-8 -*-
"""
Signal conversions for CADET exports, from caller inputs only (docs/ROADMAP.md, D2).

- UV is always exported as absorbance per path length, AU/cm: stored mAU / 1000 / path (cm).
  The path is the one the values are relative to (`pycorn.units.uv_path_length_cm`; `*/cm`
  curves are already per cm), or the caller's. Without a path the values stay in AU, with a
  warning.
- Concentration only with a caller-given extinction coefficient (Beer-Lambert, through
  `pycorn.concentration`): kg/m^3 (= mg/ml), and mol/m^3 when a molecular weight is given.
  One wavelength describes one absorbing component.
- Conductivity to salt only with a caller-given calibration (a function, or a table of
  (conductivity mS/cm, salt mol/m^3) points interpolated linearly); never a built-in formula.

CADET-Core and CADET-Process have no detector model, so these conversions happen here.
"""
import dataclasses
from collections import OrderedDict

import numpy as np

from ...concentration import concentration
from ...units import conversion_factor, quantity, uv_path_length_cm


def uv_au_per_cm(curve, path_length_cm=None, fallback_path_cm=None, fallback_basis="fallback"):
    """
    (values, unit, path_cm, basis, warning) for the UV curve `curve`: values in 'AU/cm', or in
    'AU' when no path length is known (then `warning` says so). `path_length_cm` overrides the
    stored path for plain absorbance curves. `fallback_path_cm` is used only where the run gives
    no path (never over a stored one, never on `*/cm` curves); its basis is `fallback_basis` and
    `warning` says it was used.
    """
    q = quantity(curve.unit)
    if q not in ("absorbance", "absorbance_per_length"):
        raise ValueError("%s: unit %r is not an absorbance" % (curve.name, curve.unit))
    stored = curve.values.astype(np.float64)
    if q == "absorbance_per_length":
        return stored * conversion_factor(curve.unit, "AU/cm"), "AU/cm", 1.0, "per_cm_unit", None
    au = stored * conversion_factor(curve.unit, "AU")
    if path_length_cm is not None:
        path, basis = float(path_length_cm), "caller"
    else:
        path, basis = uv_path_length_cm(curve)
        if (path is None or path <= 0) and fallback_path_cm is not None:
            if float(fallback_path_cm) <= 0:
                raise ValueError("fallback UV path length must be above 0 cm, not %r" % fallback_path_cm)
            reason = basis
            path, basis = float(fallback_path_cm), fallback_basis
            return au / path, "AU/cm", path, basis, "%s: no UV path length in the run (%s); %g cm used (%s)" % (
                curve.name, reason, path, basis)
    if path is None or path <= 0:
        return au, "AU", None, basis, "%s: no UV path length (%s); exported in AU, no concentration" % (curve.name, basis)
    return au / path, "AU/cm", path, basis, None


def uv_concentration(curve, epsilon=None, epsilon_unit="ml/(mg*cm)", epsilon_molar=None,
                     epsilon_molar_unit="1/(M*cm)", molecular_weight=None, molecular_weight_unit="g/mol",
                     path_length_cm=None, absorbance_limit=None, absorbance_limit_unit="mAU",
                     path_length_basis="caller", response_factor=None):
    """
    OrderedDict with 'kg/m^3' (array), 'mol/m^3' (array, or None without a molecular weight) and
    'conversion' (what was used) for the UV curve `curve`. Raises ValueError if something
    needed is missing (see `pycorn.concentration.concentration`, also for `response_factor`).
    A given `path_length_cm` is recorded with `path_length_basis` ('caller', or a fallback label).
    """
    if path_length_cm is not None and quantity(curve.unit) == "absorbance":
        uv = dict(curve.uv_info or {})
        uv.update(IsUVValuesNormalizedToNominalUVPathLength=False,
                  UVPathLength=path_length_cm / conversion_factor("mm", "cm"))
        curve = dataclasses.replace(curve, uv_info=uv)
    mg_ml = concentration(curve, epsilon=epsilon, epsilon_unit=epsilon_unit, epsilon_molar=epsilon_molar,
                          epsilon_molar_unit=epsilon_molar_unit, molecular_weight=molecular_weight,
                          molecular_weight_unit=molecular_weight_unit, absorbance_limit=absorbance_limit,
                          absorbance_limit_unit=absorbance_limit_unit, response_factor=response_factor)
    kg_m3 = mg_ml.values * conversion_factor("mg/ml", "kg/m^3")
    mol_m3 = None
    if molecular_weight is not None:
        mol_m3 = kg_m3 / (molecular_weight * conversion_factor(molecular_weight_unit, "kg/mol"))
    conversion = OrderedDict(mg_ml.conversion)
    if path_length_cm is not None:
        conversion["path_length_basis"] = path_length_basis
    return OrderedDict([("kg/m^3", kg_m3), ("mol/m^3", mol_m3), ("conversion", conversion)])


def salt_from_conductivity(values_ms_cm, calibration):
    """
    Salt concentration (mol/m^3) from conductivity values (mS/cm) with the caller's
    `calibration`: a function of a numpy array, or a table of (mS/cm, mol/m^3) points
    (linear interpolation; values outside the table are clipped to its ends).
    """
    x = np.asarray(values_ms_cm, dtype=np.float64)
    if callable(calibration):
        return np.asarray(calibration(x), dtype=np.float64)
    table = np.asarray(calibration, dtype=np.float64)
    if table.ndim != 2 or table.shape[1] != 2 or len(table) < 2:
        raise ValueError("conductivity calibration table must be at least two (mS/cm, mol/m^3) rows")
    order = np.argsort(table[:, 0])
    return np.interp(x, table[order, 0], table[order, 1])
