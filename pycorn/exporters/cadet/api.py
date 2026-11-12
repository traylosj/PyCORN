# -*- coding: utf-8 -*-
"""
`pycorn.export_cadet()`: the Python entry point of the CADET export (docs/ROADMAP.md, C5, D28).

It writes what `pycorn export --cadet` / `--cadet-core 5|6` write, through the same code
(`run_export.cadet_outputs`), for the original chromatogram(s) or the ones named (W17). The options are the CLI flags;
model values go in `core_overrides`, in CADET-Core's own names (D28).
"""
import os
import shutil
from pathlib import Path

# owner, 2026-11-10 (W15): written to warnings.csv and the CLI output whenever CADET files are written, shown
# on the page's CADET-Core download (`browser.names()`), at the top of docs/CADET_GUIDE.md and on the README's CADET line
CADET_EXPERIMENTAL_NOTE = ("the CADET export is experimental: its assumptions are incomplete and not yet tested, "
                           "and will change significantly as development continues")

OPTION_NAMES = ("epsilon", "epsilon_molar", "molecular_weight", "uv_response", "absorbance_limit", "path_length_cm",
                "bed_height", "diameter",
                "particle_diameter", "override_geometry", "mixer_volume", "tubing_volume", "sample_inlets",
                "system_volumes",
                # provenance only: recorded in the files' PyCORN metadata (default 'Python: pycorn.export_cadet')
                "route", "command")
# options given as (value, unit), with an example for the error message
QUANTITY_OPTIONS = {"molecular_weight": '(150000, "g/mol")', "absorbance_limit": '(2000, "mAU")',
                    "bed_height": '(10, "cm")', "diameter": '(0.5, "cm")', "particle_diameter": '(34, "um")',
                    "mixer_volume": '(0.6, "ml")', "tubing_volume": '(50, "ul")'}
PER_WAVELENGTH = {"epsilon": '{280: (1.4, "ml/(mg*cm)")}', "epsilon_molar": '{280: (43824, "1/(M*cm)")}'}


def _check_shapes(options):
    """TypeError, with an example, for an option given in another shape than the CLI flag's."""
    for name, example in QUANTITY_OPTIONS.items():
        value = options.get(name)
        if value is not None and not (isinstance(value, (tuple, list)) and len(value) == 2):
            raise TypeError("export_cadet: %s is (value, unit), e.g. %s=%s; got %r" % (name, name, example, value))
    for name, example in PER_WAVELENGTH.items():
        value = options.get(name)
        if value is None:
            continue
        ok = isinstance(value, dict) and all(isinstance(v, (tuple, list)) and len(v) == 2 for v in value.values())
        if not ok:
            raise TypeError("export_cadet: %s is per wavelength, {nm: (value, unit)}, e.g. %s=%s; got %r"
                            % (name, name, example, value))


def export_cadet(run, folder, core_version=None, cadet_process=True, chromatogram=None, core_format="json",
                 core_overrides=None, process_overrides=None, overwrite=False, **options):
    """
    Write the CADET export of the chromatograms of `run` into `folder` (created if needed), per
    chromatogram: `<stem>_cadet/` (CADET-Process CSVs and `experiment.json`) when `cadet_process`, and
    `<stem>_core_v<n>.json|.h5` (CADET-Core input) when `core_version` is '5' or '6'.

    - `chromatogram`: which chromatograms (W17): None (default) the run's original one(s); a name,
      'all', 'original', 'imported' or 'derived', or a list of them (KeyError for a name the run
      lacks). Several selected are all exported, with a warning (D28 (6), amended: until users
      settle on a rule); chromatograms the default leaves out are named in the warnings.
    - `core_format`: 'json' or 'hdf5' (needs h5py).
    - `core_overrides` (D28): a nested dict in CADET-Core's own input layout, from the root
      (`{'input': {'model': {'unit_001': {...}}}}`), merged into the Core input unchanged; the
      porosities, transport, binding, discretization and solver the run doesn't give. What it
      doesn't set gets the placeholders listed in `/meta/placeholders`.
    - `process_overrides`: the same for CADET-Process, in its own parameter layout (as
      `process.parameters` shows it); stored unchanged in `experiment.json` as `process_overrides`,
      for `process.parameters = ...` where the Process is built (`to_cadet_process(...,
      process_overrides=)` sets it). Owner, 2026-11-08: one generic block per target, no
      assumptions about what it holds.
    - `options`: the `pycorn export` flags, quantities as (value, unit): epsilon / epsilon_molar
      ({nm: (value, unit)}), molecular_weight, uv_response (detector response factor, issue #1), absorbance_limit, path_length_cm, bed_height, diameter, particle_diameter,
      override_geometry, mixer_volume, tubing_volume, sample_inlets, system_volumes.
    - `overwrite`: replace these CADET files if they exist (else FileExistsError, nothing written).

    Caller geometry that gives Vc (D26) recomputes the axes of `run` itself, as `pycorn export` does.
    Returns {'chromatograms': [names], 'written': [paths], 'warnings': [text]}.
    """
    from ...run_export import ExportOptions, _geometry_kwargs, _geometry_vc, _given_geometry, cadet_outputs, \
        chromatogram_stems
    from .core_input import CORE_VERSIONS, check_overrides
    from .process_export import check_process_overrides
    unknown = sorted(set(options) - set(OPTION_NAMES))
    if unknown:
        raise TypeError("export_cadet: unknown option(s) %s; options: %s"
                        % (", ".join(unknown), ", ".join(OPTION_NAMES)))
    _check_shapes(options)
    if core_version is not None:
        core_version = str(core_version)
        if core_version not in CORE_VERSIONS:
            raise ValueError("core_version must be %s, not %r" % (" or ".join(repr(v) for v in CORE_VERSIONS),
                                                                  core_version))
    if core_format not in ("json", "hdf5"):
        raise ValueError("core_format must be 'json' or 'hdf5', not %r" % core_format)
    if not cadet_process and core_version is None:
        raise ValueError("nothing to write: give core_version='5' or '6', or cadet_process=True")
    if core_overrides is not None:
        if core_version is None:
            raise ValueError("core_overrides set the CADET-Core input; give core_version='5' or '6'")
        check_overrides(core_overrides)
    if process_overrides is not None:
        if not cadet_process:
            raise ValueError("process_overrides go with the CADET-Process files; give cadet_process=True")
        check_process_overrides(process_overrides)
    if not run.chromatograms:
        raise ValueError("the run has no chromatogram")
    problems = []  # (chromatogram, source, message), as for warnings.csv
    # W17: the original chromatogram(s) unless `chromatogram` picks others; KeyError for a name the run lacks
    run.select_chromatograms(chromatogram)
    stems = chromatogram_stems(run, chromatogram, problems)
    chosen = list(stems)
    if len(chosen) > 1:
        # D28 (6), amended: warn and export all (of those selected) until users settle on a rule
        problems.append(("", "cadet", "%d chromatograms selected (%s): all exported; pick one with "
                                      "chromatogram=" % (len(chosen), ", ".join(chosen))))

    options.setdefault("route", "Python: pycorn.export_cadet")
    opts = ExportOptions(cadet=bool(cadet_process), cadet_core=core_version, core_format=core_format,
                         core_overrides=core_overrides, process_overrides=process_overrides, **options)
    targets = {}
    for name in chosen:
        targets[name] = []
        if cadet_process:
            targets[name].append(os.path.join(folder, stems[name] + "_cadet"))
        if core_version is not None:
            targets[name].append(os.path.join(folder, "%s_core_v%s.%s" % (
                stems[name], core_version, "h5" if core_format == "hdf5" else "json")))
    present = [t for paths in targets.values() for t in paths if os.path.exists(t)]
    if present and not overwrite:
        raise FileExistsError("%s (overwrite=True replaces it)" % ", ".join(present))
    for t in present:
        if os.path.isdir(t):
            shutil.rmtree(t)  # no stale CSVs from an earlier export
        else:
            os.remove(t)
    os.makedirs(folder, exist_ok=True)

    if _given_geometry(opts):
        from ...column_geometry import column_geometry
        geometry = column_geometry(run, **_geometry_kwargs(opts))
        _geometry_vc(run, opts, geometry)
    written = []
    for name in chosen:
        done = cadet_outputs(run, run.chromatograms[name], opts, Path(folder), stems[name], problems)
        written += done
        if core_version is not None and not any(w.startswith(stems[name] + "_core_v") for w in done):
            why = [m for c, s, m in problems if s == "cadet_core" and c == name]
            for paths in targets.values():  # all or nothing
                for t in paths:
                    if os.path.isdir(t):
                        shutil.rmtree(t, ignore_errors=True)
                    elif os.path.exists(t):
                        os.remove(t)
            raise ValueError("%s: %s" % (name, why[-1] if why else "CADET-Core input not written"))
    warnings = ["%s: %s" % (source, message) for _, source, message in problems]
    return {"chromatograms": chosen, "written": [os.path.join(folder, w.rstrip("/")) for w in written],
            "warnings": warnings}
