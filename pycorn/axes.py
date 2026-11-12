# -*- coding: utf-8 -*-
"""
CV and UTC axes for a loaded `Run`, shared by every reader (ZIP, folder export, database).

`add_axes(run)` runs at load (it also puts the run's `system_configuration` on each
chromatogram, for the inlet labels). Call it again to recompute, e.g. with a column volume
override: `add_axes(run, column_volume=0.7, column_volume_unit="ml")`.

An imported chromatogram (W17) reads its imported run log ('ImportedLogbook', the source run's,
on the same axes as its curves) for Vc, the column path and the UTC timeline. Where the result's
own run-level data (run information, method, run name, system configuration, run start date)
are used for one imported from another result (`Run.holds_run_of` False), a warning says so:
they describe this result's run, often but not always the same system and column.
"""
from .column_volume import apply_cv, column_path, resolve_column_volume
from .system_configuration import system_configuration
from .timeline import apply_utc, build_timeline

CATEGORIES = ("Vc", "CV", "UTC")


def _tagged(chrom_name, category, messages):
    return ["%s: %s: %s" % (chrom_name, category, m) for m in messages]


def add_axes(run, column_volume=None, column_volume_unit=None, use_name_hint=False, geometry_volume=None):
    """
    Resolve Vc, read the column valve path and rebuild the UTC timeline of each chromatogram,
    then set `cv` and `utc` on its curves and events. Warnings go to `run.warnings`, as
    '<chromatogram>: Vc|CV|UTC: ...' (earlier ones from this function are replaced).

    `column_volume` (in `column_volume_unit`, else the chromatogram's volume unit) overrides
    every stored Vc. `use_name_hint=True` also accepts a volume in the run or folder name.
    `geometry_volume` = (value, unit, location) of a column geometry the caller gave: Vc only
    as a last resort, when no stored source has one.
    """
    names = set(run.chromatograms) | {c.name for c in run.chromatograms.values()}
    prefixes = tuple("%s: %s: " % (n, c) for n in names for c in CATEGORIES)
    run.warnings[:] = [w for w in run.warnings if not w.startswith(prefixes)]
    config = system_configuration(run)  # inlet valves and pumps, for the inlet labels
    for chrom in run.chromatograms.values():
        # also for a chromatogram imported from another result (its sections warn: W17)
        chrom.system_configuration = config
        info = resolve_column_volume(run, chrom, column_volume, column_volume_unit, use_name_hint, geometry_volume)
        path = column_path(chrom)
        cv_warnings = apply_cv(chrom, info["value"], info["unit"], path)
        tl = build_timeline(run, chrom)
        utc_warnings = apply_utc(chrom, tl)
        run.warnings.extend(_tagged(chrom.name, "Vc", info.pop("warnings")))
        run.warnings.extend(_tagged(chrom.name, "CV", path.pop("warnings") + cv_warnings))
        # e.g. a curve UNICORN 7.11 evaluated on a volume grid: it has no time to put UTC on
        no_time = ["%s: no time axis stored (an evaluated curve?); no time or UTC" % c.name
                   for c in chrom.curves.values() if c.time is None and c.values is not None]
        run.warnings.extend(_tagged(chrom.name, "UTC", tl.pop("warnings") + utc_warnings + no_time))
        chrom.column_volume_info, chrom.column_path, chrom.timeline = info, path, tl
    return run


def is_axes_warning(warning):
    """True for a warning `add_axes` wrote (Vc, CV or UTC)."""
    parts = warning.split(": ", 2)
    return len(parts) == 3 and parts[1] in CATEGORIES
