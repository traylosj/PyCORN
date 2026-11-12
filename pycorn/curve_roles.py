# -*- coding: utf-8 -*-
"""
Find curves by what they measure ("role"), not by name.

Curve names differ between instruments (lab and process scale) and configurations, and curve
numbers differ between runs. A role is matched on the stored CurveDataType and AmplitudeUnit;
only where those cannot tell curves apart (e.g. 'Conc B' vs 'Conc Q1', both 'Other' in %) is a
name pattern used, matched on the name with case, spaces and punctuation ignored.

A role with no data_type matches any stored type.

`ROLES` can be extended or changed by callers for names not seen yet.
"""
import re

from .units import quantity

# role -> data_type (CurveDataType), unit test, name pattern (on the normalized name) or None
ROLES = {
    "uv": {"data_type": "UV"},
    # conductivity in any unit except % ('% Cond' is the same sensor as a percentage)
    "conductivity": {"data_type": "Conduction", "unit": lambda u: u != "%"},
    "conductivity_percent": {"data_type": "Conduction", "unit": lambda u: u == "%"},
    "ph": {"data_type": "pH"},
    "pressure": {"data_type": "Pressure"},
    "temperature": {"data_type": "Temperature"},
    # gradient %B: stored as 'Other' in %, like the quaternary valves (Conc Q1-Q4), so only the
    # name tells it apart: 'Conc B', 'Conc_B', 'ConcB', 'Concentration B', '%B'
    "conc_b": {"data_type": "Other", "unit": lambda u: u == "%",
               "name": r"^(conc(entration)?b|%b)$"},
    # quaternary valve composition: 'Conc Q1' .. 'Conc Q4' (several curves)
    "conc_q": {"data_type": "Other", "unit": lambda u: u == "%", "name": r"^conc(entration)?q\d$"},
    # volumetric flow (ml/min, L/h, ...): the system pump vs the sample pump. Both are stored as
    # 'Other' with the same unit, so the name tells them apart ('Sample flow'); every other
    # volumetric flow curve is taken as system flow. The CV/h and cm/h curves are not flow.
    "flow": {"unit": lambda u: quantity(u) == "flow", "name": r"^(?!sample)"},
    "sample_flow": {"unit": lambda u: quantity(u) == "flow", "name": r"^sample"},
}

# keys used by older code (e.g. `import_xml_as_df` defaults) -> role
LEGACY_KEYS = {"UV": "uv", "Cond": "conductivity", "% Cond": "conductivity_percent",
               "pH": "ph", "Conc B": "conc_b"}


def normalize_name(name):
    """Lower case, without spaces, underscores and punctuation other than '%'."""
    return re.sub(r"[^0-9a-z%]", "", (name or "").lower())


def curve_has_role(curve, role):
    spec = ROLES[role]
    if spec.get("data_type") is not None and curve.data_type != spec["data_type"]:
        return False
    if "unit" in spec and not spec["unit"](curve.unit or ""):
        return False
    if spec.get("name") and not re.search(spec["name"], normalize_name(curve.name)):
        return False
    return True


def curves_with_role(chromatogram, role, original_only=None):
    """
    Curves of `chromatogram` with this role, in stored order (several can match).
    `original_only`: True skips non-original curves, False keeps them; None (default) skips them
    on the run's original chromatogram only, so an imported or derived chromatogram (every curve
    non-original, W17) finds its curves when the caller picked it (`Chromatogram.only_original`).
    An imported chromatogram's curves of another method run (`Chromatogram.from_other_run`:
    overlays its source result had itself imported) have no role: they stay in its curves.
    """
    if role not in ROLES:
        raise KeyError("unknown curve role %r; known roles: %s" % (role, ", ".join(ROLES)))
    only = chromatogram.only_original(original_only) if hasattr(chromatogram, "only_original") else \
        (True if original_only is None else original_only)
    other = getattr(chromatogram, "from_other_run", None)
    return [c for c in chromatogram.curves.values()
            if curve_has_role(c, role) and (c.is_original is not False or not only)
            and not (other is not None and other(c))]


def resolve_curve_keys(chromatogram, keys):
    """
    Curve names for `keys`. Each key is an exact curve name in this run, a role name
    (see `ROLES`), or a legacy key ('UV', 'Cond', 'Conc B', ...) that stands for a role; a role
    expands to every curve that has it (original curves only on the run's original chromatogram,
    `curves_with_role`). Raises KeyError, listing the run's curves,
    for a key that matches nothing.
    """
    names = []
    for key in keys:
        if key in chromatogram.curves:
            found = [key]
        else:
            role = key if key in ROLES else LEGACY_KEYS.get(key)
            found = [c.name for c in curves_with_role(chromatogram, role)] if role else []
        if not found:
            raise KeyError("no curve or role %r in %s; curves: %s; roles: %s"
                           % (key, chromatogram.name, ", ".join(chromatogram.curves), ", ".join(ROLES)))
        names.extend(n for n in found if n not in names)
    return names
