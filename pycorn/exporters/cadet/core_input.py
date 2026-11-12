# -*- coding: utf-8 -*-
"""
CADET-Core input from a `CadetExperiment`, in the CADET-Core 5 or 6 layout (docs/ROADMAP.md,
"CADET export": phase C3, D9, D25).

- `to_core_input(exp, core_version, ...)`: the input as a plain nested dict
  `{'meta': ..., 'input': {'model', 'solver', 'return'}}`, groups lower case, fields upper case, as
  `cadet-cli` reads them. `core_version` is '5' (layout of 5.1.2) or '6' (layout of 6.0.0a5), required.
- `write_json(core, path)` (no dependency; `cadet-cli` reads `.json`, X4) and `write_hdf5(core, path)`
  (`h5py`, `pip install pycorn[cadet-core]`).

Structure (D24, D27, the same as the CADET-Process `Process`): unit_000 'buffer' INLET
(`PIECEWISE_CUBIC_POLY`) -> 'mixer' CSTR -> unit_001 column -> 'tubing' CSTR -> unit_002 OUTLET, and a
'feed' INLET -> column for valve feed; the optional units follow as unit_003.. (`/meta/UNIT_NAMES`).
Sections, inlet polynomials and flows come from `process_inputs()` and `system_flows()`, so the Core
file and the Process share one source. Inlets, CSTRs, outlet, connections, sections, solver and
return are written once; only the column unit differs (`_column_v5`, `_column_v6`).

Layouts, from the sources (X16, X17, rev 9):
- v5: `GENERAL_RATE_MODEL` / `LUMPED_RATE_MODEL_WITH_PORES` / `LUMPED_RATE_MODEL_WITHOUT_PORES`,
  `COL_LENGTH`, particle fields (`PAR_DIFFUSION`, `PAR_SURFDIFFUSION`, ...) and `adsorption` on the
  unit; FV only (`NCOL`, `NPAR`, WENO).
- v6: `COLUMN_MODEL_1D` (or the legacy name), `GEOMETRY = AXIAL_FLOW_CYLINDER`, `BED_LENGTH`,
  `NPARTYPE = 1`, particle fields (`PORE_DIFFUSION`, `SURFACE_DIFFUSION`, `HAS_*_DIFFUSION`) and
  `adsorption` in `particle_type_000`. Owner (C3): DG by default (16 elements, degree 4; particles 1
  element, degree 4, the CADET-Process DG defaults).

Model parameters: from the experiment (geometry from the run's linked column or the caller's
`build_experiment` values, D10/D26; porosities from `build_experiment(porosities=)`), else with
`placeholders=True` the CADET-Core GRM benchmark values, each listed in `/meta` and warned, and binding
`NONE` (tracer). Geometry never gets a placeholder (D10): missing geometry is an error.

Everything else of the model (D28): `overrides=`, in CADET-Core's own names and layout, merged in last and
unchanged. pycorn doesn't mirror CADET's models and parameters; it builds what the run gives. (The
earlier `parameters=` / `binding=` arguments, pycorn names translated to CADET's, were removed for D28.)
"""
import json
from collections import OrderedDict

import numpy as np

from .process_export import COLUMN_MODELS, _plain, process_inputs, system_flows, system_units

CORE_VERSIONS = OrderedDict([("5", "5.1.2"), ("6", "6.0.0a5")])

# CADET-Core's GRM benchmark (test/data/model_COL1D_GRM_dynLin_1comp_benchmark1.json in 6.0.0a5)
PLACEHOLDER_SOURCE = "CADET-Core GRM benchmark (model_COL1D_GRM_dynLin_1comp_benchmark1)"
PLACEHOLDERS = OrderedDict([("bed_porosity", 0.37), ("particle_porosity", 0.75), ("axial_dispersion", 5.75e-8),
                            ("film_diffusion", 6.9e-6), ("pore_diffusion", 6.07e-11), ("particle_radius", 4.5e-5)])
PLACEHOLDERS["total_porosity"] = (PLACEHOLDERS["bed_porosity"]
                                  + (1 - PLACEHOLDERS["bed_porosity"]) * PLACEHOLDERS["particle_porosity"])

# Parameters each model needs (CADET-Process attribute names); surface_diffusion is optional (0).
NEEDS = OrderedDict([
    ("GeneralRateModel", ("length", "cross_section_area", "bed_porosity", "particle_porosity", "particle_radius",
                          "axial_dispersion", "film_diffusion", "pore_diffusion")),
    ("LumpedRateModelWithPores", ("length", "cross_section_area", "bed_porosity", "particle_porosity",
                                  "particle_radius", "axial_dispersion", "film_diffusion")),
    ("LumpedRateModelWithoutPores", ("length", "cross_section_area", "total_porosity", "axial_dispersion")),
])
PER_COMPONENT = ("film_diffusion", "pore_diffusion")
GEOMETRY = ("length", "cross_section_area")

UNIT_TYPES_V5 = OrderedDict([("GeneralRateModel", "GENERAL_RATE_MODEL"),
                             ("LumpedRateModelWithPores", "LUMPED_RATE_MODEL_WITH_PORES"),
                             ("LumpedRateModelWithoutPores", "LUMPED_RATE_MODEL_WITHOUT_PORES")])

# CADET-Process 0.12 defaults (processModel/discretization.py)
FV = OrderedDict([("ncol", 100), ("npar", 5), ("weno_order", 3)])
DG = OrderedDict([("nelem", 16), ("polydeg", 4), ("par_nelem", 1), ("par_polydeg", 4)])
TIME_INTEGRATOR = OrderedDict([("ABSTOL", 1e-8), ("ALGTOL", 1e-12), ("RELTOL", 1e-6), ("INIT_STEP_SIZE", 1e-6),
                               ("MAX_STEPS", 1000000)])
LINEAR_SOLVER = OrderedDict([("GS_TYPE", 1), ("MAX_KRYLOV", 0), ("MAX_RESTARTS", 10), ("SCHUR_SAFETY", 1e-8)])


class _Placeholder(float):
    """A placeholder value that remembers its parameter name until `_placeholder_paths` strips it."""

    def __new__(cls, value, name):
        obj = float.__new__(cls, value)
        obj.name = name
        return obj


class _PlaceholderText(str):
    def __new__(cls, value, name):
        obj = str.__new__(cls, value)
        obj.name = name
        return obj


def _placeholder_paths(tree, path=()):
    """
    {placeholder name: [paths]} in `tree`; each marked value is replaced by its plain float / str in
    place, so the written input is unchanged. A path is a tuple of keys (list items are part of
    their field's path).
    """
    found = OrderedDict()
    for key, item in tree.items():
        here = path + (key,)
        if isinstance(item, dict):
            for name, paths in _placeholder_paths(item, here).items():
                found.setdefault(name, []).extend(paths)
        elif isinstance(item, (_Placeholder, _PlaceholderText)):
            found.setdefault(item.name, []).append(here)
            tree[key] = str(item) if isinstance(item, str) else float(item)
        elif isinstance(item, list) and any(isinstance(i, (_Placeholder, _PlaceholderText)) for i in item):
            found.setdefault(next(i.name for i in item if isinstance(i, (_Placeholder, _PlaceholderText))),
                             []).append(here)
            tree[key] = [str(i) if isinstance(i, str) else float(i) for i in item]
    return found


def _placeholder_warning(used, where):
    """The placeholder warning, with the CADET-Core field(s) each value sits in (what to override)."""
    def fields(name):
        paths = sorted({p[-1] for p in where.get(name, [])})
        return " (%s)" % ", ".join(paths) if paths else ""
    return ("placeholder parameters (%s): %s; not this column's values"
            % (PLACEHOLDER_SOURCE, ", ".join("%s=%g%s" % (k, v, fields(k)) for k, v in used.items())))


def _merge_overrides(core, overrides):
    """
    Merge `overrides` (CADET-Core's own layout, from the root: {'input': {...}}) into `core` unchanged
    (D28): dicts merge key by key, anything else replaces. No name is translated or checked. Returns
    (paths set, paths of them that were not in `core` before: new fields, or a misspelt name).
    """
    check_overrides(overrides)
    paths, new = [], []

    def merge(target, given, path):
        for key, item in given.items():
            key = str(key)
            here = path + (key,)
            if isinstance(item, dict) and isinstance(target.get(key), dict):
                merge(target[key], item, here)
            else:
                if key not in target:
                    new.append(here)
                target[key] = _plain(item)
                paths.append(here)

    merge(core, overrides, ())
    return paths, new


def check_overrides(overrides):
    """Raise when `overrides` isn't a dict with only the 'input' group (the only structure checked, D28)."""
    if not isinstance(overrides, dict):
        raise TypeError("overrides must be a dict in CADET-Core's input layout, e.g. {'input': {'model': ...}}")
    wrong = sorted(str(k) for k in overrides if k != "input")
    if wrong:
        raise ValueError("overrides: only the 'input' group can be set (CADET-Core reads /input), not %s"
                         % ", ".join(wrong))


def _resolve_parameters(exp, model, n_comp, placeholders, warnings):
    """(values, placeholder names) for `model`, experiment > placeholder; error on what's missing."""
    col = exp.column or {}
    por = col.get("porosities") or {}
    from_exp = {"length": col.get("length_m"), "cross_section_area": col.get("cross_section_area_m2"),
                "particle_radius": col.get("particle_radius_m"), "bed_porosity": por.get("column"),
                "particle_porosity": por.get("particle"), "total_porosity": por.get("total")}
    values, used_placeholders, missing = OrderedDict(), OrderedDict(), []
    for name in NEEDS[model]:
        if from_exp.get(name) is not None:
            values[name] = from_exp[name]
        elif placeholders and name not in GEOMETRY:
            values[name] = used_placeholders[name] = PLACEHOLDERS[name]
        else:
            missing.append(name)
    if missing:
        geometry = [m for m in missing if m in GEOMETRY]
        raise ValueError("%s needs %s%s" % (
            model, ", ".join(missing),
            "; geometry comes only from the run's linked column or build_experiment's bed_height / diameter "
            "(D10, D26)" if geometry else
            "; pass placeholders=True and set your values with overrides= in CADET-Core's names (D28)"))
    for name in values:
        values[name] = float(values[name])
    for name in PER_COMPONENT:
        if name in values:
            values[name] = [values[name]] * n_comp
    for name in used_placeholders:  # marked so to_core_input finds where each one lands (D28)
        v = values[name]
        values[name] = [_Placeholder(x, name) for x in v] if isinstance(v, list) else _Placeholder(v, name)
    if used_placeholders:
        warnings.append("placeholder parameters (%s): %s; not this column's values"
                        % (PLACEHOLDER_SOURCE, ", ".join("%s=%g" % kv for kv in used_placeholders.items())))
    return values, used_placeholders


def _binding(n_comp, placeholders, warnings):
    """(model name, NBOUND): the placeholder binding, NONE (a tracer); the caller's comes in `overrides` (D28)."""
    if not placeholders:
        raise ValueError("no binding model: pass placeholders=True (binding NONE) and set yours with overrides= in "
                         "CADET-Core's names (D28)")
    warnings.append("placeholder binding: ADSORPTION_MODEL = NONE (tracer, nothing binds)")
    return _PlaceholderText("NONE", "ADSORPTION_MODEL"), [0] * n_comp


def _column_v5(model, n_comp, p, bind, discretization):
    """Column unit, CADET-Core 5 layout (X16): everything on the unit, FV only."""
    name, nbound = bind
    disc = OrderedDict(FV)
    disc.update(discretization or {})
    if disc.get("method", "FV") != "FV":
        raise ValueError("CADET-Core 5 has FV only; discretization method %r" % disc["method"])
    u = OrderedDict([("UNIT_TYPE", UNIT_TYPES_V5[model]), ("NCOMP", n_comp), ("COL_LENGTH", p["length"]),
                     ("CROSS_SECTION_AREA", p["cross_section_area"]), ("COL_DISPERSION", p["axial_dispersion"])])
    if model == "LumpedRateModelWithoutPores":
        u["TOTAL_POROSITY"] = p["total_porosity"]
    else:
        u["COL_POROSITY"] = p["bed_porosity"]
        u["PAR_POROSITY"] = p["particle_porosity"]
        u["PAR_RADIUS"] = p["particle_radius"]
        u["FILM_DIFFUSION"] = p["film_diffusion"]
        u["NPARTYPE"] = 1
        u["PAR_TYPE_VOLFRAC"] = 1.0
        if model == "GeneralRateModel":
            u["PAR_DIFFUSION"] = p["pore_diffusion"]
        u["INIT_CP"] = [0.0] * n_comp
    u["INIT_C"] = [0.0] * n_comp
    u["ADSORPTION_MODEL"] = name
    u["NBOUND"] = nbound
    d = OrderedDict([("NCOL", int(disc["ncol"]))])
    if model == "GeneralRateModel":
        d["NPAR"] = int(disc["npar"])
        d["PAR_DISC_TYPE"] = "EQUIDISTANT_PAR"
    d["USE_ANALYTIC_JACOBIAN"] = 1
    d.update(LINEAR_SOLVER)
    d["RECONSTRUCTION"] = "WENO"
    d["weno"] = OrderedDict([("BOUNDARY_MODEL", 0), ("WENO_EPS", 1e-10), ("WENO_ORDER", int(disc["weno_order"]))])
    u["discretization"] = d
    return u, "FV"


def _column_v6(model, n_comp, p, bind, discretization, legacy_unit_type):
    """Column unit, CADET-Core 6 layout (X1, X2, X17): GEOMETRY, particle_type_000; DG unless asked."""
    name, nbound = bind
    disc = OrderedDict([("method", "DG")])
    disc.update(discretization or {})
    method = disc["method"]
    if method not in ("DG", "FV"):
        raise ValueError("discretization method must be 'DG' or 'FV', not %r" % method)
    defaults = DG if method == "DG" else FV
    disc = OrderedDict(list(defaults.items()) + list(disc.items()))
    unit_type = UNIT_TYPES_V5[model] if legacy_unit_type else "COLUMN_MODEL_1D"
    u = OrderedDict([("UNIT_TYPE", unit_type), ("GEOMETRY", "AXIAL_FLOW_CYLINDER"), ("NCOMP", n_comp),
                     ("BED_LENGTH", p["length"]), ("CROSS_SECTION_AREA", p["cross_section_area"]),
                     ("COL_DISPERSION", p["axial_dispersion"]), ("FORWARD_FLOW", 1)])  # required in 6.0.0a5 (C4, X21)
    if model == "LumpedRateModelWithoutPores":
        u["TOTAL_POROSITY"] = p["total_porosity"]
    else:
        u["COL_POROSITY"] = p["bed_porosity"]
    u["INIT_C"] = [0.0] * n_comp
    u["NPARTYPE"] = 1
    u["PAR_TYPE_VOLFRAC"] = 1.0
    pt = OrderedDict([("HAS_FILM_DIFFUSION", int(model != "LumpedRateModelWithoutPores")),
                      ("HAS_PORE_DIFFUSION", int(model == "GeneralRateModel")),
                      ("HAS_SURFACE_DIFFUSION", 0)])
    if model != "LumpedRateModelWithoutPores":
        pt["PAR_POROSITY"] = p["particle_porosity"]
        pt["PAR_RADIUS"] = p["particle_radius"]
        pt["FILM_DIFFUSION"] = p["film_diffusion"]
        pt["INIT_CP"] = [0.0] * n_comp
    if model == "GeneralRateModel":
        pt["PORE_DIFFUSION"] = p["pore_diffusion"]
    pt["ADSORPTION_MODEL"] = name
    pt["NBOUND"] = nbound
    d =OrderedDict([("SPATIAL_METHOD", method)])
    if method == "DG":
        d["NELEM"] = int(disc["nelem"])
        d["POLYDEG"] = int(disc["polydeg"])
        d["USE_ANALYTIC_JACOBIAN"] = 1
    else:
        d["NCOL"] = int(disc["ncol"])
        d["USE_ANALYTIC_JACOBIAN"] = 1
        d.update(LINEAR_SOLVER)
        d["RECONSTRUCTION"] = "WENO"
        d["weno"] = OrderedDict([("BOUNDARY_MODEL", 0), ("WENO_EPS", 1e-10),
                                 ("WENO_ORDER", int(disc["weno_order"]))])
    u["discretization"] = d
    if model == "GeneralRateModel":
        if method == "DG":
            pt["discretization"] = OrderedDict([("SPATIAL_METHOD", "DG"), ("PAR_NELEM", int(disc["par_nelem"])),
                                                ("PAR_POLYDEG", int(disc["par_polydeg"]))])
        else:
            pt["discretization"] = OrderedDict([("SPATIAL_METHOD", "FV"), ("NCELLS", int(disc["npar"])),
                                                ("PAR_DISC_TYPE", "EQUIDISTANT")])
    u["particle_type_000"] = pt
    return u, method


def _solution_times(exp, solution_times, t0, t1, warnings):
    if solution_times is not None:
        t = np.asarray(solution_times, dtype=np.float64)
    else:
        primary = exp.primary_uv
        if primary is None:
            warnings.append("no primary UV signal: no USER_SOLUTION_TIMES (CADET picks its own output times)")
            return None
        t = np.asarray(primary.time_s, dtype=np.float64)
    t = np.unique(t[np.isfinite(t) & (t >= t0) & (t <= t1)])
    return t.tolist() if len(t) else None


def to_core_input(exp, core_version, components=None, buffers=None, feed=None, column_model="GeneralRateModel",
                  placeholders=False, discretization=None, time_integrator=None, solution_times=None,
                  legacy_unit_type=False, system_volumes=True, overrides=None, **removed):
    """
    CADET-Core input dict for `exp` in the layout of `core_version` ('5' or '6', required, D9).

    - `components`, `buffers`, `feed`: as in `process_inputs` (none of them: placeholder 'salt' and
      'sample', D24).
    - `column_model`: 'GeneralRateModel', 'LumpedRateModelWithPores' or 'LumpedRateModelWithoutPores'.
    - `placeholders=True`: the transport values the experiment doesn't give and the binding become
      placeholders (non-binding tracer, CADET-Core benchmark values), listed in `/meta/placeholders`.
      Never geometry (D10). Without it, anything missing is an error. Your own model values go in
      `overrides` (D28; the earlier `parameters=` / `binding=` are gone).
    - `discretization`: v5 {'ncol', 'npar', 'weno_order'}; v6 {'method': 'DG' (default) or 'FV', 'nelem',
      'polydeg', 'par_nelem', 'par_polydeg' | 'ncol', 'npar', 'weno_order'}.
    - `time_integrator`: overrides for `/input/solver/time_integrator` (CADET names).
    - `solution_times`: output times in s; default the primary UV grid within the sections.
    - `legacy_unit_type` (v6): write `GENERAL_RATE_MODEL` etc. instead of `COLUMN_MODEL_1D`.
    - `system_volumes` (D27): the mixer CSTR before and the tubing CSTR after the column, from
      `exp.system` (each only when its volume is known); False leaves both out.
    - `overrides` (D28): a nested dict in CADET-Core's own input layout, from the root
      (`{'input': {'model': {'unit_001': {'COL_POROSITY': 0.37}}}}`), merged in last and unchanged:
      dicts merge key by key, anything else replaces. pycorn doesn't translate or check the names, so
      every field the installed CADET-Core reads can be set; keep the case CADET uses (groups lower,
      fields upper). The column is always `unit_001`. A placeholder it replaces leaves
      `/meta/placeholders`; the paths set are in `/meta/OVERRIDES`.
    Warnings are in `core['meta']['warnings']`.
    """
    if removed:
        gone = sorted(set(removed) & {"parameters", "binding"})
        if gone:
            raise TypeError("to_core_input: %s removed (D28): pass placeholders=True and set model values with "
                            "overrides= in CADET-Core's own names, e.g. {'input': {'model': {'unit_001': "
                            "{'COL_POROSITY': 0.37}}}}" % " and ".join("%s=" % g for g in gone))
        raise TypeError("to_core_input: unexpected argument(s) %s" % ", ".join(sorted(removed)))
    core_version = str(core_version) if core_version is not None else None
    if core_version not in CORE_VERSIONS:
        raise ValueError("core_version must be %s, not %r" % (" or ".join(repr(v) for v in CORE_VERSIONS),
                                                              core_version))
    if column_model not in COLUMN_MODELS:
        raise ValueError("column_model must be one of %s, not %r" % (", ".join(COLUMN_MODELS), column_model))
    if exp.time_zero.get("kind") != "method_start":
        raise ValueError("the Core input needs time_zero='method_start' (sections start at the method start, D24); "
                         "rebuild the experiment with it")
    inputs = process_inputs(exp, components, buffers, feed)
    rows = inputs["sections"]
    if not rows:
        raise ValueError("the experiment has no sections")
    warnings = list(inputs["warnings"])
    names = inputs["components"]
    n_comp = len(names)
    p, used = _resolve_parameters(exp, column_model, n_comp, placeholders, warnings)
    bind = _binding(n_comp, placeholders, warnings)
    binding_placeholder = True  # always NONE until overrides replace it (D28)

    # Shared parts (X18): inlets, CSTRs (D27), outlet, connections, sections, solver, return.
    # unit_000 'buffer' inlet, unit_001 column, unit_002 outlet, then 'feed' inlet, 'mixer', 'tubing'
    # as present, so the column and the outlet keep their numbers.
    units = system_units(exp, inputs, system_volumes)
    order = ["buffer", "column", "outlet"] + [u[0] for u in units if u[0] in ("feed", "mixer", "tubing")]
    index = {name: k for k, name in enumerate(order)}
    kinds = {u[0]: u for u in units}

    def inlet_unit(key):
        u = OrderedDict([("UNIT_TYPE", "INLET"), ("NCOMP", n_comp), ("INLET_TYPE", "PIECEWISE_CUBIC_POLY")])
        for k, row in enumerate(rows):
            c = np.asarray(row[key], dtype=np.float64)
            u["sec_%03d" % k] = OrderedDict([("CONST_COEFF", c[:, 0].tolist()), ("LIN_COEFF", c[:, 1].tolist()),
                                             ("QUAD_COEFF", c[:, 2].tolist()), ("CUBE_COEFF", c[:, 3].tolist())])
        return u

    def cstr_unit(volume):
        # No NBOUND (C4, X22): with NBOUND all 0 both Cores make one particle type and reject it ("Particle type 0
        # does not have a bound state"); without it the CSTR has no solid phase
        return OrderedDict([("UNIT_TYPE", "CSTR"), ("NCOMP", n_comp), ("INIT_C", [0.0] * n_comp),
                            ("INIT_LIQUID_VOLUME", float(volume)), ("CONST_SOLID_VOLUME", 0.0),
                            ("FLOWRATE_FILTER", 0.0)])

    # One switch per section, as CADET-Process writes it (C4, X20): both Cores pick the switch at a discontinuous
    # section change from `section index % NSWITCHES` and move at most one switch, so fewer switches than
    # sections (or a continuous boundary) leaves later switches unused (the feed never reached the column).
    connections = OrderedDict([("CONNECTIONS_INCLUDE_PORTS", 1), ("CONNECTIONS_INCLUDE_DYNAMIC_FLOW", 1)])
    switches = [(k, [(a, b, (float(q[0]), float(q[1]))) for a, b, q in system_flows(units, row, inputs["bypass_flow"])])
                for k, row in enumerate(rows)]
    for i, (k, flows) in enumerate(switches):
        # [from, to, port from, port to, component from, component to, Q0..Q3]; Q = 0 rows kept (pauses,
        # the mixer holding during valve feed)
        table = []
        for a, b, (q0, q1) in flows:
            table += [index[a], index[b], -1, -1, -1, -1, q0, q1, 0.0, 0.0]
        connections["switch_%03d" % i] = OrderedDict([("SECTION", k), ("CONNECTIONS", table)])
    connections["NSWITCHES"] = len(switches)
    zero = [r for r in rows if r["flow_m3_s"][0] == 0 and r["flow_m3_s"][1] == 0]
    if zero:
        warnings.append("%d of %d section(s) with no column flow (pauses, by-pass): written as Q = 0"
                        % (len(zero), len(rows)))

    if core_version == "5":
        column, method = _column_v5(column_model, n_comp, p, bind, discretization)
    else:
        column, method = _column_v6(column_model, n_comp, p, bind, discretization, legacy_unit_type)
    model = OrderedDict([("NUNITS", len(order))])
    for name in order:
        if name == "buffer":
            unit = inlet_unit("c_buffer")
        elif name == "feed":
            unit = inlet_unit("c_feed")
        elif name == "column":
            unit = column
        elif name == "outlet":
            unit = OrderedDict([("UNIT_TYPE", "OUTLET"), ("NCOMP", n_comp)])
        else:
            unit = cstr_unit(kinds[name][2])
        model["unit_%03d" % index[name]] = unit
    model["connections"] = connections
    model["solver"] = OrderedDict(LINEAR_SOLVER)

    times = [rows[0]["start_s"]] + [r["end_s"] for r in rows]
    # SECTION_CONTINUITY all 0 (C4, X20): at a continuous boundary the Cores don't advance the switch, so one
    # switch per section needs every boundary discontinuous (CADET-Process writes all 0 too)
    sections = OrderedDict([("NSEC", len(rows)), ("SECTION_TIMES", times),
                            ("SECTION_CONTINUITY", [0] * (len(rows) - 1))])
    ti = OrderedDict(TIME_INTEGRATOR)
    ti.update({str(k).upper(): v for k, v in (time_integrator or {}).items()})
    solver = OrderedDict([("NTHREADS", 1), ("CONSISTENT_INIT_MODE", 1)])
    user_times = _solution_times(exp, solution_times, times[0], times[-1], warnings)
    if user_times is not None:
        solver["USER_SOLUTION_TIMES"] = user_times
    solver["sections"] = sections
    solver["time_integrator"] = ti
    ret = OrderedDict([("WRITE_SOLUTION_TIMES", 1), ("SPLIT_COMPONENTS_DATA", 0), ("SPLIT_PORTS_DATA", 0),
                       ("unit_002", OrderedDict([("WRITE_SOLUTION_OUTLET", 1), ("WRITE_SOLUTION_INLET", 0),
                                                 ("WRITE_SOLUTION_BULK", 0)]))])

    tree = OrderedDict([("model", model), ("solver", solver), ("return", ret)])
    where = _placeholder_paths(tree, ("input",))  # also strips the markers
    if used:  # now that the paths are known: name the CADET-Core field of each placeholder
        warnings[:] = [_placeholder_warning(used, where) if w.startswith("placeholder parameters (") else w
                       for w in warnings]
    overridden = None
    if overrides is not None:
        overridden, added = _merge_overrides(OrderedDict([("input", tree)]), overrides)

        def replaced(path):
            return any(path[:len(p)] == p for p in overridden)

        gone = [name for name, paths in where.items() if all(replaced(p) for p in paths)]
        if gone:
            used = OrderedDict((k, v) for k, v in used.items() if k not in gone)
            if "ADSORPTION_MODEL" in gone:
                binding_placeholder = False
            # rewrite the placeholder warnings for what is left
            warnings[:] = [w for w in warnings if not w.startswith(("placeholder parameters (",
                                                                    "placeholder binding:"))]
            if used:
                warnings.append(_placeholder_warning(used, where))
            if binding_placeholder:
                warnings.append("placeholder binding: ADSORPTION_MODEL = NONE (tracer, nothing binds)")
        warnings.append("caller overrides (CADET-Core names, not checked by pycorn): %s"
                        % ", ".join("/".join(p) for p in overridden))
        if added:
            # D28: names aren't checked; a new field is normal for model values, a misspelt name looks the same
            warnings.append("overrides added field(s) this file didn't have: %s; check the spelling (a misspelt "
                            "name is ignored or rejected by CADET-Core and the placeholder stays)"
                            % ", ".join("/".join(p) for p in added))

    src = exp.source or {}
    meta = OrderedDict([
        ("FORMAT", "pycorn-cadet-core-input"), ("FORMAT_VERSION", 1),
        ("CORE_VERSION", core_version), ("TARGET_VERSION", CORE_VERSIONS[core_version]),
        ("COLUMN_MODEL", column_model), ("SPATIAL_METHOD", method),
        ("RUN", src.get("run")), ("CHROMATOGRAM", src.get("chromatogram")),
        ("UNICORN_VERSION", src.get("unicorn_version")), ("PYCORN_VERSION", src.get("pycorn_version")),
        ("TIME_ZERO", "method_start"), ("INJECTION_OFFSET_S", exp.time_zero.get("injection_offset_s")),
        ("COMPONENTS", names), ("PLACEHOLDER_COMPONENTS", int(inputs["placeholder"])),
        ("placeholders", OrderedDict([("SOURCE", PLACEHOLDER_SOURCE)] + [(k.upper(), v) for k, v in used.items()]
                                     + ([("ADSORPTION_MODEL", "NONE")] if binding_placeholder else []))
         if used or binding_placeholder else None),
        # where each placeholder sits, in CADET-Core's names: what to override
        ("PLACEHOLDER_PATHS", OrderedDict((k.upper(), ["/".join(p) for p in where.get(k, [])]) for k in used)
         if used else None),
        ("UNIT_NAMES", order),
        ("MIXER_VOLUME_M3", kinds["mixer"][2] if "mixer" in kinds else None),
        ("MIXER_SOURCE", (exp.system or {}).get("mixer_source") if "mixer" in kinds else None),
        ("TUBING_VOLUME_M3", kinds["tubing"][2] if "tubing" in kinds else None),
        ("SECTION_NAMES", [r["name"] or "" for r in rows]),
        ("SECTION_FEED", [int(bool(r["feed"])) for r in rows]),
        ("SECTION_FEED_PATH", [r["feed_path"] or "" for r in rows]),
        ("SECTION_PAUSED", [int(bool(r["paused"])) for r in rows])]
        + ([("OVERRIDES", ["/".join(p) for p in overridden])] if overridden is not None else [])
        + [("WARNINGS", warnings)])
    return OrderedDict([("meta", meta), ("input", tree)])


def _drop_none(value):
    if isinstance(value, dict):
        return OrderedDict((k, _drop_none(v)) for k, v in value.items() if v is not None)
    return value


def write_json(core, path):
    """Write the dict from `to_core_input` as JSON (`cadet-cli` reads `.json` input, X4). Returns `path`."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(_plain(_drop_none(core)), f, indent=1, ensure_ascii=True)
    return path


def write_hdf5(core, path):
    """Write the dict from `to_core_input` as HDF5, laid out as CADET-Python writes it (strings as
    bytes, lists as arrays). Needs `h5py` (pip install pycorn[cadet-core]). Returns `path`."""
    try:
        import h5py
    except ImportError as exc:
        raise ImportError("h5py is needed for HDF5 (pip install h5py, or pip install pycorn[cadet-core]); "
                          "write_json() works without it") from exc

    def save(group, d):
        for key, item in d.items():
            if item is None:
                continue
            if isinstance(item, dict):
                save(group.create_group(key), item)
            elif isinstance(item, str):
                group[key] = np.array(item.encode("utf-8"))
            elif isinstance(item, (list, tuple)) and item and all(isinstance(i, str) for i in item):
                group[key] = np.array([i.encode("utf-8") for i in item])
            elif isinstance(item, (list, tuple)) and not item:
                group[key] = np.array([], dtype=np.float64)
            else:
                group[key] = np.array(item)

    with h5py.File(path, "w") as f:
        save(f, _plain(core))
    return path
