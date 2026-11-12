# -*- coding: utf-8 -*-
"""
Concentration (mg/ml) from a UV curve: c = A [AU] / (epsilon [ml mg^-1 cm^-1] * l [cm]).

A derived view: the stored curve is never changed, and `to_res3_dict()` never includes it.
epsilon is the caller's (per curve: it depends on the wavelength), either a mass coefficient
or a molar coefficient plus molecular weight. UNICORN's stored coefficients
(Peak.ExtCoeff, Pool.ExtCoeff, ProteinInfo) were empty in every run tested against a lab environment and
apply only inside a peak or pool, so they are not read here. No default epsilon, path,
absorbance limit or response factor is ever assumed.

A concentration from UV is an estimate (GitHub issue #1): every result carries
`ACCURACY_NOTE`, and the exports write it to their warnings, also for a calibrated
coefficient or a response factor. `pycorn.uv_calibration` measures the coefficient this
detector gives from sample sent through the column by-pass.
"""
import dataclasses
from collections import OrderedDict

import numpy as np

from .units import conversion_factor, convert_decimals, quantity, uv_path_length_cm

ACCURACY_NOTE = ("concentration from UV (Beer-Lambert) is an estimate; treat it with care. It can be off by "
                 "several percent or more: the extinction coefficient (theoretical or measured) against this "
                 "detector's response, detector saturation and non-linear absorbance at high signal, baseline "
                 "drift and the path length all add error, and so can a calibration or response factor. Check "
                 "it against a by-pass reading of the sample (pycorn.uv_calibration) or an offline assay")


def concentration(curve, epsilon=None, epsilon_unit="ml/(mg*cm)", epsilon_molar=None,
                  epsilon_molar_unit="1/(M*cm)", molecular_weight=None, molecular_weight_unit="g/mol",
                  absorbance_limit=None, absorbance_limit_unit="mAU", fallback_path_cm=None,
                  fallback_basis="fallback", response_factor=None):
    """
    A new `Curve` in mg/ml from the UV curve `curve`.

    Give `epsilon` (mass coefficient, e.g. 1.0 for 'A 0.1% = 1.0'), or `epsilon_molar` and
    `molecular_weight` (epsilon_mass = epsilon_molar / MW). The path length is the one the
    values are relative to (`pycorn.units.uv_path_length_cm`); `*/cm` curves use 1 cm.
    `fallback_path_cm` is used only where the run gives no path (never over a stored path,
    never on `*/cm` curves), with the basis `fallback_basis` (as
    `exporters.cadet.calibration.uv_au_per_cm`).
    Points above `absorbance_limit` (in the stored, unscaled absorbance) become NaN.
    `response_factor` (caller only, no default): what this detector reads against what the
    coefficient predicts, e.g. 0.92 for a detector that reads 8 % low; the coefficient used
    is epsilon * response_factor.

    The result's `conversion` records the input curve, epsilon as given and as used, its unit
    and source, the response factor, the path length and its basis, and `ACCURACY_NOTE`.
    Raises ValueError if something needed is missing.
    """
    if curve.values is None:
        raise ValueError("%s has no values" % curve.name)
    q = quantity(curve.unit)
    if q not in ("absorbance", "absorbance_per_length"):
        raise ValueError("%s: unit %r is not an absorbance" % (curve.name, curve.unit))
    if epsilon is not None:
        eps = epsilon * conversion_factor(epsilon_unit, "ml/(mg*cm)")
        eps_source = OrderedDict([("source", "mass"), ("value", epsilon), ("unit", epsilon_unit)])
    elif epsilon_molar is not None and molecular_weight is not None:
        mw = molecular_weight * conversion_factor(molecular_weight_unit, "g/mol")
        # (1/(M*cm)) / (g/mol) = l/(g*cm) = ml/(mg*cm)
        eps = epsilon_molar * conversion_factor(epsilon_molar_unit, "1/(M*cm)") / mw
        eps_source = OrderedDict([("source", "molar"), ("value", epsilon_molar), ("unit", epsilon_molar_unit),
                                  ("molecular_weight", molecular_weight),
                                  ("molecular_weight_unit", molecular_weight_unit)])
    else:
        raise ValueError("%s: no extinction coefficient: give epsilon, or epsilon_molar and molecular_weight"
                         % curve.name)
    if eps <= 0:
        raise ValueError("%s: extinction coefficient must be positive" % curve.name)
    given = eps
    if response_factor is not None:
        if not float(response_factor) > 0:
            raise ValueError("response factor must be above 0, not %r" % (response_factor,))
        eps = given * float(response_factor)
    path, basis = uv_path_length_cm(curve)
    if path is None and fallback_path_cm is not None:
        if not float(fallback_path_cm) > 0:
            raise ValueError("fallback UV path length must be above 0 cm, not %r" % (fallback_path_cm,))
        path, basis = float(fallback_path_cm), fallback_basis
    if path is None:
        raise ValueError("%s: no path length (%s)" % (curve.name, basis))
    to_au = conversion_factor(curve.unit, "AU/cm" if q == "absorbance_per_length" else "AU")
    stored = curve.values.astype(np.float64)
    values = stored * to_au / (eps * path)
    if absorbance_limit is not None:
        limit = absorbance_limit * conversion_factor(absorbance_limit_unit, curve.unit)
        values[stored > limit] = np.nan
    conversion = OrderedDict([("input_curve", curve.name), ("raw_unit", curve.unit), ("unit", "mg/ml"),
                              ("epsilon_ml_per_mg_cm", eps), ("epsilon_given_ml_per_mg_cm", given),
                              ("response_factor", float(response_factor) if response_factor is not None else None),
                              ("epsilon", eps_source),
                              ("path_length_cm", path), ("path_length_basis", basis),
                              ("absorbance_limit", absorbance_limit),
                              ("absorbance_limit_unit", absorbance_limit_unit if absorbance_limit is not None else None),
                              ("note", ACCURACY_NOTE)])
    # the UV curve's decimals carried through the factor (`units.convert_decimals`), for display only
    precision = convert_decimals(curve.precision, to_au / (eps * path))
    return dataclasses.replace(curve, name="%s (mg/ml)" % curve.name, unit="mg/ml", values=values, precision=precision,
                               is_original=False, uv_info=None, point_sets=[], conversion=conversion)

