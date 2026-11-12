# -*- coding: utf-8 -*-
"""
`pycorn` command line (run search step 5; also `python -m pycorn`).

    pycorn find --folder "/DefaultHome/Projects/**" --var "Flow rate>=1 ml/min" --csv runs.csv
    pycorn export --from runs.csv --out exports --format csv,xlsx
    pycorn export results/*.zip folder.UFol --out exports
    pycorn bypass-uv results/*.zip --sample-conc "1.2 mg/ml" --csv bypass.csv
    pycorn inspect results/*.zip --json  (what each run holds, no export; masked unless --no-mask)
    pycorn windows results/*.zip --window "load=Sample Application:1:3 CV" --window-curves uv  (M22, no export)
    pycorn schema --json            (tables, columns, groups, roles, units, exit codes)
    pycorn sources --test --json    (database sources: set / empty, connected; never a value)
    pycorn --version
    pycorn find ... --json | --jsonl | --csv -   (also export, bypass-uv, inspect, windows: for programs, Track 9 A4)

`find` prints the number of matches and the first rows; the whole page (or every page
with --all) goes to --csv, which `export --from` reads back. Console output is ASCII and
names a database only by its label ('alias#n'), never by server or database name.
--json / --jsonl / --csv - put the result on stdout for a program (`json_output`; human lines
on stderr; people masked unless --no-mask; errors as one JSON object on stderr).
Owner decisions: docs/ROADMAP.md, "Command line".
"""
import argparse
import os
import re
import shlex
import sys
import warnings
from collections import OrderedDict

from .messages import REPORT, SETUP_HINT, plain_error
from .run_export import CSV_ENCODING, FORMATS, LABEL, _file_rows, existing_status  # noqa: F401 (existing_status)

SHOW = 20  # rows find prints


def ascii_text(value):
    """Text safe on any console: non-ASCII characters as escapes."""
    return str(value).encode("ascii", "backslashreplace").decode("ascii")


def _say(text="", file=None):
    print(ascii_text(text), file=file or sys.stdout)


_SHOWN = set()  # warnings printed by this command (count and page apply the same filters: once is enough)
_MACHINE = [False]  # --json / --jsonl on the command line: usage errors as a JSON object too (main sets it)


# ---------------------------------------------------------------------------
# Machine-readable output (Track 9, A4): --json, --jsonl, --csv -, structured errors
# ---------------------------------------------------------------------------

def machine_mode(args):
    """'json', 'jsonl' or None: how the command writes its result for a program."""
    if getattr(args, "jsonl", False):
        return "jsonl"
    if getattr(args, "json", False):
        return "json"
    return None


def _to_stdout(args):
    """True when the result goes to stdout for a program (--json, --jsonl, --csv -): human lines go to stderr."""
    return machine_mode(args) is not None or getattr(args, "csv", None) == "-"


def _masker(args):
    """A `json_output.PeopleMasker` for one document, or None under --no-mask."""
    if getattr(args, "no_mask", False):
        return None
    from .json_output import PeopleMasker
    return PeopleMasker()


_ACTIVE_MASKER = [None]  # the masker of the command writing for programs: the error object's text too


class _MaskedStream:
    """A text stream writing through `masker.text` (people as User1, System1, ...) to `stream`."""

    def __init__(self, stream, masker):
        self._stream, self._masker = stream, masker

    def write(self, text):
        return self._stream.write(self._masker.text(text))

    def __getattr__(self, name):
        return getattr(self._stream, name)


class _MachineOutput:
    """
    For a command writing to stdout for a program: `out` is the real stdout; inside the block
    sys.stdout is stderr, so every human line (progress, notes, `_say`) lands there. With
    `masker` (`people=True` and no --no-mask; owner, 2026-11-11) stderr is masked like stdout,
    with the same labels, and warnings are held to the end of the command, when every name
    of the runs loaded is known, then printed masked.
    """

    def __init__(self, args=None, people=False):
        self.masker = _masker(args) if people and args is not None else None

    def __enter__(self):
        import contextlib
        self.out = sys.stdout
        self._stack = contextlib.ExitStack()
        err = sys.stderr
        if self.masker is not None:
            err = _MaskedStream(sys.stderr, self.masker)
            self._stack.enter_context(contextlib.redirect_stderr(err))
            self._held = []
            self._showwarning = warnings.showwarning
            warnings.showwarning = lambda message, category, filename, lineno, file=None, line=None: \
                self._held.append((message, category, filename, lineno))
            _ACTIVE_MASKER[0] = self.masker
        self._stack.enter_context(contextlib.redirect_stdout(err))
        return self

    def __exit__(self, *exc):
        try:
            if self.masker is not None:
                warnings.showwarning = self._showwarning
                for message, category, filename, lineno in self._held:
                    _show_warning(message, category, filename, lineno)
        finally:
            self._stack.close()
        return False

    def document(self, doc):
        from .json_output import dumps_document
        self.out.write(dumps_document(doc) + "\n")
        self.out.flush()


def _json_error(e, args, code, text=None):
    """In --json / --jsonl mode: the error as one JSON object on stderr ({'error': {'text', 'key', 'exit'}})."""
    from .json_output import dumps_line, error_document
    from .messages import error_key
    more = {}
    if isinstance(e, ConnectionError):
        more["hint"] = SETUP_HINT
    elif code == 3 and not getattr(args, "debug", False):
        more["hint"] = REPORT
    text = text or plain_error(e)
    if _ACTIVE_MASKER[0] is not None:  # people masked as on stdout (not under --no-mask)
        text = _ACTIVE_MASKER[0].text(text)
    print(dumps_line(error_document(text, error_key(e) if code != 3 else "unexpected", code, **more)),
          file=sys.stderr)


class _Parser(argparse.ArgumentParser):
    """argparse with usage errors as one JSON object on stderr when --json / --jsonl was given (exit 2)."""

    def error(self, message):
        if _MACHINE[0]:
            from .json_output import dumps_line, error_document
            print(dumps_line(error_document("%s: %s" % (self.prog, message), "bad_flag", 2)), file=sys.stderr)
            self.exit(2)
        super().error(message)


def _show_warning(message, category, filename, lineno, file=None, line=None):
    text = str(message)
    if text in _SHOWN:
        return
    _SHOWN.add(text)
    _say("warning: %s" % text, sys.stderr)


# ---------------------------------------------------------------------------
# Filters from the command line
# ---------------------------------------------------------------------------

_OPS = (">=", "<=", "==", "!=", "<", ">", "=")


def parse_var(text):
    """
    (name, spec) from one --var: 'Flow rate>=1 ml/min', 'UV1==280', 'Column position=Position 2'
    (text, wildcards), 'Sample_ID' (has it). The first operator splits name and value.
    """
    best = None
    for op in _OPS:
        i = text.find(op)
        if i >= 0 and (best is None or i < best[0] or (i == best[0] and len(op) > len(best[1]))):
            best = (i, op)
    if best is None:
        name = text.strip()
        if not name:
            raise ValueError("--var: empty")
        return name, None
    i, op = best
    name, value = text[:i].strip(), text[i + len(op):].strip()
    if not name:
        raise ValueError("--var %r: no variable name before %s" % (text, op))
    if op == "=":
        return name, value
    number, _, unit = value.partition(" ")
    try:
        number = float(number)
    except ValueError:
        raise ValueError("--var %r: %s needs a number (then an optional unit)" % (text, op)) from None
    unit = unit.strip()
    return name, (op, number, unit) if unit else (op, number)


def parse_range(text, convert=str):
    """'a..b' -> (a, b) (either end may be empty: None); one value v -> (v, v)."""
    if ".." in text:
        start, end = (t.strip() for t in text.split("..", 1))
        return (convert(start) if start else None, convert(end) if end else None)
    return convert(text.strip()), convert(text.strip())


def parse_ints(text, flag="value"):
    """'3' -> 3, '3,5,8' -> [3, 5, 8] (any of them), '3..8' -> (3, 8) (a range)."""
    def whole(v):
        try:
            return int(v)
        except ValueError:
            raise ValueError("%s %r: give whole numbers: 3, 3,5,8 or a range 3..8" % (flag, text)) from None

    if ".." in text:
        return parse_range(text, whole)
    values = [whole(v) for v in text.split(",") if v.strip()]
    if not values:
        raise ValueError("%s %r: give whole numbers: 3, 3,5,8 or a range 3..8" % (flag, text))
    return values[0] if len(values) == 1 else values


def parse_sources(values):
    """--sources values -> find()'s sources=: 'discover', aliases, or alias:db1,db2 / alias:*."""
    if not values:
        return None
    if len(values) == 1 and values[0].strip().lower() == "discover":
        return "discover"
    plan, plain = {}, []
    for value in values:
        alias, sep, dbs = value.partition(":")
        if sep:
            plan[alias.strip()] = dbs.strip() if dbs.strip() == "*" else [d.strip() for d in dbs.split(",") if d.strip()]
        else:
            plain += [a.strip() for a in alias.split(",") if a.strip()]
    if plan:
        for alias in plain:
            plan[alias] = None
        return plan
    return plain


def _texts(values):
    """Repeated text flags: one value as is, several as a list (any of them)."""
    if not values:
        return None
    return values[0] if len(values) == 1 else list(values)


def filters_from(args):
    """find()'s filters from parsed arguments."""
    variables = {}
    for text in args.var or []:
        name, spec = parse_var(text)
        if name in variables:  # all must hold
            old = variables[name]
            variables[name] = (old if isinstance(old, list) else [old]) + [spec]
        else:
            variables[name] = spec
    filters = {
        "folder": args.folder, "name": _texts(args.name), "system": _texts(args.system),
        "system_type": _texts(args.system_type), "run_type": _texts(args.run_type), "state": _texts(args.state),
        "batch_id": _texts(args.batch_id), "created_by": _texts(args.created_by), "column": _texts(args.column),
        "run_start": parse_range(args.run_start) if args.run_start else None,
        "run_index": parse_ints(args.run_index, "--run-index") if args.run_index else None,
        "result_ids": parse_ints(args.result_id, "--result-id") if args.result_id else None,
        "n_chromatograms": parse_ints(args.chromatograms, "--chromatograms") if args.chromatograms else None,
        "variables": variables or None,
        "has_curve_roles": [r.strip() for v in args.has_role or [] for r in v.split(",") if r.strip()] or None,
        "has_peak_tables": args.has_peaks,
        "include_hidden": args.include_hidden or None,
    }
    return {k: v for k, v in filters.items() if v is not None}


# ---------------------------------------------------------------------------
# Export options from the command line
# ---------------------------------------------------------------------------

def parse_units(values):
    """--units 'volume=L,flow=L/h' (comma list or repeated) -> {quantity: unit}, checked."""
    from .units import check_units
    units = {}
    for value in values or []:
        for part in value.split(","):
            if not part.strip():
                continue
            q, sep, unit = part.partition("=")
            if not sep or not q.strip() or not unit.strip():
                raise ValueError("--units %r: give quantity=unit, e.g. volume=L,flow=L/h" % part.strip())
            units[q.strip()] = unit.strip()
    return check_units(units)


def parse_quantity(text, flag, target, default_unit=None):
    """'10 cm' -> (10.0, 'cm'): a positive number and a unit convertible to `target`."""
    from .units import UnitError, conversion_factor
    number, _, unit = text.strip().partition(" ")
    unit = unit.strip() or default_unit
    try:
        number = float(number)
    except ValueError:
        raise ValueError("%s %r: give a number and a unit, e.g. '10 %s'" % (flag, text, target)) from None
    if not number > 0:
        raise ValueError("%s %r: must be above 0" % (flag, text))
    if not unit:
        raise ValueError("%s %r: no unit (e.g. '%g %s')" % (flag, text, number, target))
    try:
        conversion_factor(unit, target)
    except UnitError as e:
        raise ValueError("%s %r: %s" % (flag, text, e)) from None
    return number, unit


def parse_factor(text, flag):
    """'0.92' -> 0.92: a plain number above 0."""
    try:
        value = float(text)
    except ValueError:
        raise ValueError("%s %r: give a number, e.g. 0.92" % (flag, text)) from None
    if not value > 0:
        raise ValueError("%s %r: must be above 0" % (flag, text))
    return value


def parse_list(values):
    """--tables a,b --tables c (comma list or repeated) -> ['a', 'b', 'c']; empty items dropped."""
    return [part.strip() for value in values or [] for part in value.split(",") if part.strip()]


def parse_count(text, flag):
    """'4000' -> 4000: a whole number."""
    try:
        value = int(text)
    except ValueError:
        raise ValueError("%s %r: give a whole number, e.g. 4000" % (flag, text)) from None
    return value


def parse_number_arg(text, flag):
    """'0.5' -> 0.5: any number (the choices that need a sign or range say so themselves)."""
    try:
        return float(text)
    except ValueError:
        raise ValueError("%s %r: give a number, e.g. 0.5" % (flag, text)) from None


def parse_per_wavelength(values, flag, target):
    """--epsilon '280=1.4' or '280=1.4 <unit>', repeatable -> {nm: (value, unit)}."""
    out = {}
    for value in values or []:
        nm, sep, rest = value.partition("=")
        try:
            nm = float(nm)
        except ValueError:
            nm = None
        if not sep or nm is None or not nm > 0:
            raise ValueError("%s %r: give wavelength=value, e.g. 280=1.4" % (flag, value))
        if nm in out:
            raise ValueError("%s: %g nm given twice" % (flag, nm))
        out[nm] = parse_quantity(rest, flag, target, default_unit=target)
    return out


INLET = re.compile(r"^[A-Za-z][0-9]*$")


def parse_inlets(values):
    """--sample-inlets 'A5,B2' (comma list or repeated) -> ['A5', 'B2']: inlet positions as the run log names them."""
    out = []
    for value in values or []:
        for part in value.split(","):
            part = part.strip()
            if not part:
                continue
            if not INLET.match(part):
                raise ValueError("--sample-inlets %r: give inlet positions, e.g. A5 or A5,B2" % part)
            if part.upper() not in out:
                out.append(part.upper())
    return out


def bypass_uv_args(args):
    """--sample-conc, --loop-volume, --absorbance-limit -> (value, unit) keywords (issue #1)."""
    return {
        "sample_concentration": (parse_quantity(args.sample_conc, "--sample-conc", "mg/ml")
                                 if args.sample_conc else None),
        "loop_volume": parse_quantity(args.loop_volume, "--loop-volume", "ml") if args.loop_volume else None,
        "absorbance_limit": (parse_quantity(args.absorbance_limit, "--absorbance-limit", "mAU", "mAU")
                             if args.absorbance_limit else None),
    }


def options_file(path):
    """`run_export.ExportOptions` from an options JSON file (`ExportOptions.to_dict()` form; `--options`)."""
    from .run_export import ExportOptions
    block = _json_block(path, "--options")
    try:
        return ExportOptions.from_dict(block)
    except ValueError as e:
        text = str(e)
        text = text[len("options: "):] if text.startswith("options: ") else text
        raise ValueError("--options %s: %s" % (path, text)) from None


def export_options(args):
    """
    `run_export.ExportOptions` from parsed export arguments; ValueError before anything is loaded.
    With `--options FILE.json` the file's values are the start and every flag given on the command
    line replaces the file's value of its option (a flag can set or change an option, not clear one).
    """
    import dataclasses
    import importlib.util
    from .run_export import PROVENANCE_ONLY, ExportOptions
    epsilon = parse_per_wavelength(args.epsilon, "--epsilon", "ml/(mg*cm)")
    molar = parse_per_wavelength(args.epsilon_molar, "--epsilon-molar", "1/(M*cm)")
    # D29 (3): --mw that nothing uses is a note in warnings.csv (run_export.write_run), not an error
    options = ExportOptions(
        units=parse_units(args.units) or None, method_hints=args.method_hints,
        mask_identifying=args.mask_identifying,
        path_length_cm=parse_quantity(args.path_length, "--path-length", "cm", "cm")[0] if args.path_length else None,
        epsilon=epsilon or None, epsilon_molar=molar or None,
        molecular_weight=parse_quantity(args.mw, "--mw", "g/mol", "g/mol") if args.mw else None,
        uv_response=parse_factor(args.uv_response, "--uv-response") if args.uv_response else None,
        bypass_uv=bool(args.bypass_uv or args.sample_conc or args.loop_volume), **bypass_uv_args(args),
        windows=list(args.window) if args.window else None,
        window_curves=parse_list(args.window_curves) or None,
        bed_height=parse_quantity(args.bed_height, "--bed-height", "m") if args.bed_height else None,
        diameter=parse_quantity(args.diameter, "--diameter", "m") if args.diameter else None,
        particle_diameter=(parse_quantity(args.particle_diameter, "--particle-diameter", "m")
                           if args.particle_diameter else None),
        override_geometry=args.override_geometry, cadet=args.cadet, cadet_core=args.cadet_core,
        core_format=args.core_format or "json",
        mixer_volume=parse_quantity(args.mixer_volume, "--mixer-volume", "ml") if args.mixer_volume else None,
        tubing_volume=parse_quantity(args.tubing_volume, "--tubing-volume", "ml") if args.tubing_volume else None,
        sample_inlets=parse_inlets(args.sample_inlets) or None, system_volumes=not args.no_system_volumes,
        tables=parse_list(args.tables) or None, curves=parse_list(args.curves) or None,
        curve_roles=parse_list(args.curve_roles) or None,
        max_points=parse_count(args.max_points, "--max-points") if args.max_points else None, method=args.method,
        every_n=parse_count(args.every, "--every") if args.every else None,
        derivative_share=(parse_number_arg(args.derivative_share, "--derivative-share")
                          if args.derivative_share is not None else None),
        grid_step=parse_number_arg(args.grid_step, "--grid-step") if args.grid_step else None,
        grid_axis=args.grid_axis,
        chromatograms=list(args.chromatogram) if args.chromatogram else None,
        route="CLI: pycorn export", command=getattr(args, "command_line", None))
    if getattr(args, "options", None):
        start, defaults = options_file(args.options), ExportOptions()
        for name in (f.name for f in dataclasses.fields(ExportOptions) if f.name not in PROVENANCE_ONLY):
            flag = getattr(options, name)
            given = args.core_format is not None if name == "core_format" else flag != getattr(defaults, name)
            if not given:
                setattr(options, name, getattr(start, name))
    both = sorted(set(options.epsilon or {}) & set(options.epsilon_molar or {}))
    if both:
        raise ValueError("%s nm: give --epsilon or --epsilon-molar, not both" % ", ".join("%g" % n for n in both))
    if options.epsilon_molar and not options.molecular_weight:
        raise ValueError("--epsilon-molar needs --mw (molecular weight, e.g. '66000 g/mol')")
    cadet_only = [flag for flag, given in (("--mixer-volume", args.mixer_volume),
                                           ("--tubing-volume", args.tubing_volume),
                                           ("--sample-inlets", args.sample_inlets),
                                           ("--no-system-volumes", args.no_system_volumes)) if given]
    if cadet_only and not (options.cadet or options.cadet_core):
        raise ValueError("%s: only used with --cadet or --cadet-core" % ", ".join(cadet_only))
    if options.override_geometry and not (options.bed_height or options.diameter or options.particle_diameter):
        raise ValueError("--override-geometry needs --bed-height, --diameter or --particle-diameter")
    if options.core_format == "hdf5":
        if not options.cadet_core:
            raise ValueError("--core-format is only used with --cadet-core")
        if not importlib.util.find_spec("h5py"):
            raise ImportError("--core-format hdf5 needs h5py: pip install pycorn[cadet-core] (or write JSON)")
    if args.core_overrides:
        if not options.cadet_core:
            raise ValueError("--core-overrides is only used with --cadet-core")
        from .exporters.cadet.core_input import check_overrides
        options.core_overrides = _json_block(args.core_overrides, "--core-overrides")
        check_overrides(options.core_overrides)
    elif options.core_overrides:  # from --options
        from .exporters.cadet.core_input import check_overrides
        check_overrides(options.core_overrides)
    if args.process_overrides:
        if not options.cadet:
            raise ValueError("--process-overrides is only used with --cadet")
        options.process_overrides = _json_block(args.process_overrides, "--process-overrides")
    from .run_export import check_options
    check_options(options)
    return options


def _json_block(path, flag):
    """A JSON object from `path`, as given (D28: its content is CADET's business, not checked here)."""
    import json
    try:
        with open(path, encoding="utf-8-sig") as f:
            block = json.load(f)
    except OSError as e:
        raise ValueError("%s: cannot read %s (%s)" % (flag, path, e.strerror or e)) from None
    except ValueError as e:
        raise ValueError("%s: %s is not valid JSON (%s)" % (flag, path, e)) from None
    if not isinstance(block, dict):
        raise ValueError("%s: %s must hold one JSON object ({...}), not %s" % (flag, path, type(block).__name__))
    return block


# ---------------------------------------------------------------------------
# find
# ---------------------------------------------------------------------------

def _labelled(page, labels):
    out = page.copy()
    out.insert(0, LABEL, [labels.get((s, str(d).lower()), "%s#?" % s) for s, d in zip(out["source"], out["database"])])
    return out


def _after(text, labels):
    """--after 'alias#n:ResultID' (or a plain ResultID) -> find()'s after=."""
    label, sep, rid = text.rpartition(":")
    try:
        rid = int(rid)
    except ValueError:
        raise ValueError("--after %r: give 'label:ResultID' as the console prints it (e.g. --after %s:120), "
                         "or a ResultID" % (text, next(iter(sorted(labels.values())), "default#1"))) from None
    if not sep:
        return rid
    for (source, database), value in labels.items():
        if value.lower() == label.strip().lower():
            return source, database, rid
    raise ValueError("--after: no database labelled %r in this search (labels: %s)"
                     % (label, ", ".join(sorted(labels.values()))))


def _table(page, show):
    """The first `show` rows of a page as plain text."""
    columns = [(LABEL, "db"), ("result_id", "id"), ("name", "name"), ("run_start_local", "run start"),
               ("system", "system"), ("state", "state"), ("n_copies", "copies")]
    rows = [[h for _, h in columns]]
    for _, row in page.head(show).iterrows():
        cells = []
        for c, _ in columns:
            v = row.get(c)
            if v is None or (not isinstance(v, str) and _isna(v)):
                v = ""
            elif c == "run_start_local":
                v = str(v)[:19]
            v = ascii_text(v)
            cells.append(v if len(v) <= 40 else v[:37] + "...")
        rows.append(cells)
    widths = [max(len(r[i]) for r in rows) for i in range(len(columns))]
    return "\n".join("  ".join(cell.ljust(w) for cell, w in zip(r, widths)).rstrip() for r in rows)


def _isna(value):
    import pandas as pd
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _unknown_variables(db, variables, sources, strict):
    """After a search with --var matched nothing: say which names no run has, with close names."""
    import difflib
    try:
        known = [str(v) for v in db.pick_list("variable", limit=5000, sources=sources, strict=strict)["value"]]
    except Exception:  # only a hint: the search itself worked
        return
    lower = {k.lower(): k for k in known}
    for name in variables:
        if name.lower() in lower:
            continue
        close = difflib.get_close_matches(name.lower(), list(lower), n=3, cutoff=0.6)
        _say("--var %r: no run in this search has a variable of that name%s" % (
            name, "; close: %s" % ", ".join(repr(lower[c]) for c in close) if close else
            " (names: pycorn find --csv with --variables, or db.pick_list('variable'))"))


def run_find(args, db):
    with warnings.catch_warnings():
        # said once at the end ('not searched: ...'), not in a warning per query
        warnings.filterwarnings("ignore", message=r"source '[^']*': cannot connect")
        if _to_stdout(args):
            with _MachineOutput(args, people=True) as output:
                return _find_for_programs(args, db, output)
        return _find(args, db)


class _Search:
    """One `find` search: filters, labels, the total and its pages (labelled, wide with --variables), lazily."""

    def __init__(self, args, db):
        self.args, self.db = args, db
        self.filters = filters_from(args)
        self.sources = parse_sources(args.sources)
        self.labels = db.database_labels(self.sources, strict=args.strict)
        self.total = db.count(sources=self.sources, strict=args.strict, **self.filters)
        self.more, self.last = False, None
        if args.all:
            self._pages = db.find_pages(page_size=args.limit, sources=self.sources, strict=args.strict,
                                        dedupe=args.dedupe, **self.filters)
        else:
            after = _after(args.after, self.labels) if args.after else None
            # find() itself, without its 'more results' warning: the console says it below
            page, self.more, self.last = db._find_page(args.limit, after, self.sources, args.strict, self.filters,
                                                       args.dedupe)
            self._pages = [page]

    def pages(self):
        from .search import wide
        for page in self._pages:
            page = _labelled(page, self.labels)
            if self.args.variables and len(page):
                page = wide(page, self.db.variables(page))
            yield page

    def next_after(self):
        """'label:ResultID' to continue after this page (--after), or None."""
        if not (self.more and self.last is not None):
            return None
        label = self.labels.get((self.last[0], str(self.last[1]).lower()), "%s#?" % self.last[0])
        return "%s:%d" % (label, int(self.last[2]))


def _find(args, db):
    import pandas as pd
    search = _Search(args, db)
    filters, sources, labels = search.filters, search.sources, search.labels
    total, more, last = search.total, search.more, search.last
    frames = list(search.pages())
    catalog = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    unreachable = dict(getattr(db, "unreachable", None) or {})
    _say("%d result(s) match%s%s." % (total, "" if not args.dedupe else " (before removing copies)",
                                      " in the databases reached" if unreachable else ""))
    if not total and filters.get("variables"):
        _unknown_variables(db, filters["variables"], sources, args.strict)
    if len(catalog) and args.show > 0:
        _say(_table(catalog, args.show))
        if len(catalog) > args.show:
            _say("... %d more row(s) in this %s" % (len(catalog) - args.show, "selection" if args.all else "page"))
    if more and last is not None:
        label = labels.get((last[0], str(last[1]).lower()), "%s#?" % last[0])
        _say("More results follow: --after %s:%d, a larger --limit, or --all" % (label, int(last[2])))
    if args.csv:
        if os.path.dirname(os.path.abspath(args.csv)):
            os.makedirs(os.path.dirname(os.path.abspath(args.csv)), exist_ok=True)
        catalog.to_csv(args.csv, index=False, encoding=CSV_ENCODING)
        _say("%d row(s) written to %s" % (len(catalog), args.csv))
    if unreachable:
        for alias, why in unreachable.items():
            _say("not searched: source %r: %s" % (alias, why), sys.stderr)
        _say("the search is incomplete; %s" % SETUP_HINT, sys.stderr)
        return 1
    return 0


def _find_for_programs(args, db, output):
    """
    `find` with --json, --jsonl or --csv -: the rows on stdout (`database` left out, the label
    names it; people masked unless --no-mask), the human lines on stderr without the table.
    --csv FILE still writes the file as stored.
    """
    import pandas as pd
    from .json_output import JsonLines, catalog_rows, document, stdout_frame, write_csv
    mode, masker = machine_mode(args), output.masker
    search = _Search(args, db)
    stream = JsonLines(output.out, "find", masker is not None) if mode == "jsonl" else None
    rows, frames = [], []
    for page in search.pages():
        frames.append(page)
        for row in catalog_rows(page, masker):
            if stream:
                stream.write("row", row)
            else:
                rows.append(row)
    catalog = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    unreachable = dict(getattr(db, "unreachable", None) or {})
    _say("%d result(s) match%s%s." % (search.total, "" if not args.dedupe else " (before removing copies)",
                                      " in the databases reached" if unreachable else ""))
    if not search.total and search.filters.get("variables"):
        _unknown_variables(db, search.filters["variables"], search.sources, args.strict)
    if search.next_after():
        _say("More results follow: --after %s, a larger --limit, or --all" % search.next_after())
    if args.csv == "-":
        write_csv(stdout_frame(catalog, masker), output.out)
    elif args.csv:
        if os.path.dirname(os.path.abspath(args.csv)):
            os.makedirs(os.path.dirname(os.path.abspath(args.csv)), exist_ok=True)
        catalog.to_csv(args.csv, index=False, encoding=CSV_ENCODING)
        _say("%d row(s) written to %s" % (len(catalog), args.csv))
    problems = ["source %r not searched" % alias for alias in unreachable]
    for alias, why in unreachable.items():
        _say("not searched: source %r: %s" % (alias, why), sys.stderr)
    if unreachable:
        _say("the search is incomplete; %s" % SETUP_HINT, sys.stderr)
    fields = OrderedDict([("total", int(search.total)), ("rows", len(catalog)), ("more", bool(search.next_after())),
                          ("next_after", search.next_after()),
                          ("unreachable", [OrderedDict([("source", a), ("reason", w)]) for a, w in unreachable.items()]),
                          ("problems", problems)])
    if stream:
        stream.summary(**fields)
    elif mode == "json":
        output.document(document("find", masker is not None, total=fields["total"], more=fields["more"],
                                 next_after=fields["next_after"], rows=rows, unreachable=fields["unreachable"],
                                 problems=problems))
    return 1 if unreachable else 0


# ---------------------------------------------------------------------------
# export
# ---------------------------------------------------------------------------

def read_selection(path):
    """
    A find CSV, a catalog_from_files CSV or both mixed back as a catalog (types as find() /
    catalog_from_files() give them, as far as export needs).
    """
    import pandas as pd
    if not os.path.isfile(path):
        raise FileNotFoundError("--from: no such file: %s" % path)
    frame = pd.read_csv(path, dtype={"source": str, "database": str, "tstamp": str, LABEL: str, "name": str,
                                     "folder_path": str, "path": str, "member": str, "match_key": str})
    columns = set(frame.columns)
    if "database" not in columns and {LABEL, "result_id"} <= columns:
        # written to stdout (find --csv -): the database named by its label only, found again at load
        frame.insert(list(frame.columns).index(LABEL) + 1, "database", pd.Series([None] * len(frame), dtype=object))
        columns.add("database")
    if "source" not in columns or not ({"database", "result_id"} <= columns or "path" in columns):
        raise ValueError("%s is not a pycorn find or file catalog CSV (needs source plus database and result_id, "
                         "or path)" % path)
    files = _file_rows(frame)
    if "result_id" in columns:
        db_rows = [not f for f in files]
        no_database = frame.loc[db_rows, "database"].isna()
        if LABEL in columns:
            no_database &= frame.loc[db_rows, LABEL].isna()
        if frame.loc[db_rows, "result_id"].isna().any() or no_database.any():
            raise ValueError("%s: a row has neither a database and result_id nor a path" % path)
        frame["result_id"] = frame["result_id"].astype(int) if not any(files) else frame["result_id"].astype("Int64")
    elif not all(files):
        raise ValueError("%s: a row has no path and the CSV has no database/result_id columns" % path)
    return _drop_repeats(frame, files, path)


def _drop_repeats(frame, files, path):
    """`frame` without rows naming a run a row above already names (one warning)."""
    keys, keep = set(), []
    for i, row in enumerate(frame.itertuples(index=False)):
        if files[i]:
            key = ("file", os.path.normcase(os.path.abspath(str(row.path))), str(getattr(row, "member", "") or ""))
        elif _text(row.database) is None:  # named by its label only (find --csv -)
            key = (row.source, "label:%s" % str(getattr(row, LABEL)).lower(), int(row.result_id))
        else:
            key = (row.source, str(row.database).lower(), int(row.result_id))
        keep.append(key not in keys)
        keys.add(key)
    if all(keep):
        return frame
    warnings.warn("%s: %d row(s) name a run listed above; each run is exported once" % (path, keep.count(False)))
    return frame[keep].reset_index(drop=True)


def resolve_labels(catalog, database):
    """
    `catalog` with the database of every row that names it by its label only (a CSV `find --csv -`
    wrote) filled in from the settings (`database()`: the PyCORNDatabase); ValueError for a label
    the settings do not have. Other rows as they are.
    """
    if LABEL not in catalog.columns or "database" not in catalog.columns:
        return catalog
    files = _file_rows(catalog)
    need = [i for i, (f, d) in enumerate(zip(files, catalog["database"])) if not f and _text(d) is None]
    if not need:
        return catalog
    known = {}

    def lookup(scope):
        for (source, name), label in database().database_labels(scope).items():
            known.setdefault(label.lower(), (source, name))

    lookup(None)
    if any(str(catalog[LABEL].iloc[i]).lower() not in known for i in need):
        lookup("discover")
    catalog = catalog.copy()
    catalog["database"] = catalog["database"].astype(object)
    for i in need:
        label, source = str(catalog[LABEL].iloc[i]), catalog["source"].iloc[i]
        hit = known.get(label.lower())
        if hit is None or (isinstance(source, str) and hit[0] != source):
            raise ValueError("no database labelled %r for source %r in the settings (labels: %s)" % (
                label, source, ", ".join(sorted(known)) or "none"))
        catalog.iloc[i, catalog.columns.get_loc("database")] = hit[1]
    return catalog


def _file_selection(paths):
    from .file_catalog import catalog_from_files
    catalog = catalog_from_files(paths)
    return catalog, list(catalog["source"])


def _not_read(catalog):
    """How many files catalog_from_files could not read (each already warned about)."""
    return len(catalog.attrs.get("not_read") or [])


def _runs(catalog, db_factory, strict):
    """
    (row number, run) for each loaded row, in catalog order: database rows through
    db.load_many, file rows through load_files (a .UFol read once); what can't be read is
    skipped with a warning (`run_export._selected_runs`, which `pycorn export` uses too).
    """
    from . import run_export as rx
    return rx._selected_runs(catalog, db_factory, strict)


def _text(value):
    return None if value is None or (not isinstance(value, str) and _isna(value)) else str(value)


def run_export(args, db_factory):
    if _to_stdout(args):
        with _MachineOutput(args, people=True) as output:
            return _export_for_programs(args, db_factory, output)
    return _export(args, db_factory)[0]


class _ExportReport:
    """
    `export --json` / `--jsonl`: the index rows on stdout (people masked unless --no-mask), one
    per run as its status is known (--jsonl) or all in one document (--json).
    """

    def __init__(self, args, output):
        from .json_output import JsonLines
        self.output, self.masker = output, output.masker
        self.stream = JsonLines(output.out, "export", self.masker is not None) if machine_mode(args) == "jsonl" else None
        self.rows = {}

    def row(self, i, catalog, label, fields):
        """Index row `i` (`fields`: the export_* columns, then the catalog row) as it goes out."""
        from .json_output import catalog_rows
        frame = catalog.iloc[[i]].copy()
        for k, (column, value) in enumerate(fields.items()):
            frame.insert(k, column, [value])
        row = catalog_rows(frame, self.masker, labels=[label])[0]
        row["position"] = i
        self.rows[i] = row
        if self.stream:
            self.stream.write("run", row)

    def finish(self, out, status, not_read, problems):
        from .json_output import document, home_tilde
        counts = OrderedDict([("selected", len(status)), ("written", status.count("written")),
                              ("incomplete", status.count("incomplete")),
                              ("skipped", sum(1 for s in status if s.startswith("skipped"))),
                              ("failed", status.count("failed")), ("not_loaded", status.count("not loaded")),
                              ("not_read", len(not_read))])
        index = home_tilde(os.path.join(os.path.abspath(out), "index.csv"))
        folder = home_tilde(os.path.abspath(out))
        not_read = [OrderedDict([("file", home_tilde(p)), ("reason", r)]) for p, r in not_read]
        if self.stream:
            self.stream.summary(totals=counts, out=folder, index=index, not_read=not_read, problems=problems)
        else:
            self.output.document(document("export", self.masker is not None, out=folder, index=index, totals=counts,
                                          rows=[self.rows[i] for i in sorted(self.rows)], not_read=not_read,
                                          problems=problems))


def _export_for_programs(args, db_factory, output):
    report = _ExportReport(args, output)
    code, status, not_read = _export(args, db_factory, report)
    problems = ["%d %s" % (status.count(s), s) for s in ("failed", "not loaded", "incomplete") if s in status]
    if not_read:
        problems.append("%d file(s) not read" % len(not_read))
    report.finish(args.out, status, not_read, problems)
    return code


def _export(args, db_factory, report=None):
    """
    The export (human lines through `_say`); (exit code, out folder, status per run, files not
    read [(path, reason)]). `report` (`_ExportReport`) gets every index row as its status is known.
    """
    import pandas as pd
    from . import run_export as rx
    formats = [f.strip().lower() for v in args.format or ["csv"] for f in v.split(",") if f.strip()]
    unknown = [f for f in formats if f not in FORMATS]
    if unknown:
        raise ValueError("--format: unknown %s (known: %s)" % (", ".join(unknown), ", ".join(FORMATS)))
    if "xlsx" in formats:
        rx.excel_engine()  # fail before loading anything
    options = export_options(args)
    if args.update and args.overwrite:
        raise ValueError("--update re-exports changed runs, --overwrite every run: give one of them")
    if bool(args.selection) == bool(args.files):
        raise ValueError("export: give --from <find CSV> or result files (.zip/.UFol), not both or neither")
    if os.path.exists(args.out) and not os.path.isdir(args.out):
        raise ValueError("--out %s is a file: give a folder (it is made if missing)" % args.out)
    db = None

    def database():
        nonlocal db
        db = db or db_factory()
        return db

    if args.selection:
        catalog = resolve_labels(read_selection(args.selection), database)
    else:
        catalog = _file_selection(args.files)[0]
    if report is not None:
        from .json_output import learn_catalog
        learn_catalog(catalog, report.masker)  # labels in catalog order, before a run adds its own
    out = rx.long_path(args.out)
    state = {}

    def on_run(event):
        kind = event["event"]
        if kind == "selected":
            state["labels"] = event["labels"]
            stale = event["changed"]
            _say("%d run(s) selected, %d to export, %d skipped (already there); %d changed since export or not "
                 "checkable%s" % (event["selected"], event["to_export"], event["skipped"], stale,
                                  "" if args.overwrite or args.update or not stale else ": --update exports them again"))
        elif kind == "loaded":
            if report is not None and report.masker is not None:
                report.masker.absorb(event["run"])  # its people, also as the run log quotes them in warnings
        elif kind == "failed":
            _debug_traceback(args)
        else:
            i, row = event["position"], event["row"]
            if row["export_status"] == "incomplete":  # asked for and not written: say so, and exit 1
                _say("incomplete: %s (%d file(s); not written: %s; see warnings.csv)" % (
                    row["export_folder"], len(event["written"]),
                    ", ".join("%s for %s" % (what, chrom) for chrom, what in event["missing"])))
            elif row["export_status"] == "written":
                _say("written: %s (%d file(s))" % (row["export_folder"], len(event["written"])))
            if report is not None:
                report.row(i, catalog, state["labels"][i], row)

    index = rx.export_selection(catalog, args.out, formats, options=options, overwrite=args.overwrite,
                                update=args.update, strict=args.strict, db=database, on_run=on_run)
    status = list(index["export_status"])
    if (options.cadet or options.cadet_core) and any(s in ("written", "incomplete") for s in status):
        from .exporters.cadet.api import CADET_EXPERIMENTAL_NOTE
        _say("note: " + CADET_EXPERIMENTAL_NOTE)  # W15: also a line in every warnings table
    counts = pd.Series(status).value_counts()
    not_read = _not_read(catalog)
    parts = ["%d %s" % (n, s) for s, n in counts.items()] + (["%d file(s) not read" % not_read] if not_read else [])
    _say("done: %s; index: %s" % (", ".join(parts) or "no run to export", out / rx.INDEX_FILE))
    code = 0 if not not_read and not any(s in counts for s in ("failed", "not loaded", "incomplete")) else 1
    return code, status, list(catalog.attrs.get("not_read") or [])


# ---------------------------------------------------------------------------
# bypass-uv (issue #1)
# ---------------------------------------------------------------------------

def _fmt(value, spec="%.4g"):
    return "" if value is None else spec % value


def _once(factory):
    """`factory` called at most once (a database opened only when needed, then reused)."""
    made = []

    def get():
        if not made:
            made.append(factory())
        return made[0]
    return get


# reading columns named by the run, not by a person: never masked (the run is picked by name)
_READING_NAMES = ("source", "run", "chromatogram", "curve")


def _shown_reading(row, masker):
    """A by-pass reading as it goes to stdout: people in its texts masked by `masker` (None: as stored)."""
    if masker is None:
        return OrderedDict(row)
    out = OrderedDict()
    for key, value in row.items():
        if key in _READING_NAMES:
            out[key] = value
        elif isinstance(value, list):
            out[key] = [masker.text(v) for v in value]
        else:
            out[key] = masker.text(value)
    return out


def _reading_frame(rows):
    """The readings as `bypass-uv --csv` writes them (warnings as a JSON list per cell)."""
    import json
    import pandas as pd
    from .uv_calibration import READING_COLUMNS
    frame = pd.DataFrame(rows, columns=["source", "result_id", "run", "chromatogram"] + READING_COLUMNS)
    frame["warnings"] = [json.dumps(w) for w in frame["warnings"]]
    return frame


def run_bypass_uv(args, db_factory):
    """Print (and with --csv write) `uv_calibration.bypass_uv_readings` for every run selected."""
    if _to_stdout(args):
        with _MachineOutput(args, people=True) as output:
            return _bypass_uv(args, db_factory, output)
    return _bypass_uv(args, db_factory)


def _bypass_uv(args, db_factory, output=None):
    """
    The by-pass readings: as text (`output` None), or with `output` (`_MachineOutput`) as one
    JSON document, JSON lines or CSV on stdout (people masked unless --no-mask; human lines on
    stderr without the table). --csv FILE writes the file as stored either way.
    """
    from .json_output import JsonLines, document, home_tilde, learn_catalog, write_csv
    from .uv_calibration import ACCURACY_NOTE, bypass_uv_readings
    kwargs = {}
    for name, given in bypass_uv_args(args).items():
        if given is not None:
            kwargs.update({name: given[0], name + "_unit": given[1]})
    if bool(args.selection) == bool(args.files):
        raise ValueError("bypass-uv: give --from <find CSV> or result files (.zip/.UFol), not both or neither")
    database = _once(db_factory)
    if args.selection:
        catalog = resolve_labels(read_selection(args.selection), database)
        labels = [label if isinstance(label, str) else source for label, source in
                  zip(catalog[LABEL] if LABEL in catalog.columns else catalog["source"], catalog["source"])]
    else:
        catalog, labels = _file_selection(args.files)
    mode = machine_mode(args) if output is not None else None
    masker = output.masker if output is not None else None
    learn_catalog(catalog, masker)  # labels in catalog order, before a run adds its own
    stream = JsonLines(output.out, "bypass-uv", masker is not None) if mode == "jsonl" else None
    rows, shown, failed, loaded = [], [], 0, 0
    for i, run in _runs(catalog, database, args.strict):
        loaded += 1
        if masker is not None:
            masker.absorb(run)
        where = labels[i] if i is not None else run.source
        rid = (run.origin or {}).get("result_id") if run.source == "sql" else None
        # W17: the original chromatogram(s) unless --chromatogram picks others
        picked, unmatched = run.chromatogram_selection(args.chromatogram)
        if unmatched:
            warnings.warn("%s: no chromatogram %s (it has %s)" % (run.name, ", ".join(map(repr, unmatched)), ", ".join(
                "%s (%s)" % (d["name"], d["label"]) for d in run.describe_chromatograms())))
        for chrom in (run.chromatograms[name] for name in picked):
            try:
                readings = bypass_uv_readings(run, chrom, **kwargs)
            except Exception as e:
                if args.strict:
                    raise
                _debug_traceback(args)
                warnings.warn("%s %s: no by-pass reading: %s" % (run.name, chrom.name, plain_error(e)))
                failed += 1
                continue
            for r in readings:
                rows.append(OrderedDict([("source", where), ("result_id", rid), ("run", run.name),
                                         ("chromatogram", chrom.name)] + list(r.items())))
                if output is not None:
                    shown.append(_shown_reading(rows[-1], masker))
                    if stream:
                        stream.write("reading", shown[-1])
    _say("%d reading(s) of sample sent to the column by-pass" % len(rows))
    head = ["run", "chrom", "win", "curve", "nm", "kind", "max", "A/cm", "epsilon"]
    table = [head]
    for r in rows:
        table.append([str(r["run"])[:30], r["chromatogram"], str(r["window"]), r["curve"],
                      _fmt(r["wavelength_nm"], "%g"), r["kind"], _fmt(r["max"], "%.0f"),
                      _fmt(r["absorbance_per_cm"]), _fmt(r["epsilon_ml_per_mg_cm"])])
    if rows:
        if output is None:  # the table is the result: on stdout for programs
            widths = [max(len(ascii_text(row[k])) for row in table) for k in range(len(head))]
            for row in table:
                _say("  ".join(ascii_text(c).ljust(w) for c, w in zip(row, widths)).rstrip())
            for r in rows:
                for w in r["warnings"]:
                    _say("  %s %s w%d %s: %s" % (str(r["run"])[:30], r["chromatogram"], r["window"], r["curve"], w))
        _say("note: " + ACCURACY_NOTE)
    if args.csv == "-":
        write_csv(_reading_frame(shown), output.out)
    elif args.csv:
        _reading_frame(rows).to_csv(args.csv, index=False, encoding=CSV_ENCODING)
        _say("written: %s" % args.csv)
    not_read = list(catalog.attrs.get("not_read") or [])
    problems = ["%d file(s) not read" % len(not_read)] if not_read else []
    if loaded < len(catalog):
        problems.append("%d run(s) not loaded" % (len(catalog) - loaded))
    if failed:
        problems.append("%d chromatogram(s) not read" % failed)
    if problems:
        _say("incomplete: %s (see the warnings above)" % ", ".join(problems))
    if mode:
        not_read = [OrderedDict([("file", home_tilde(p)), ("reason", r)]) for p, r in not_read]
        if stream:
            stream.summary(readings=len(shown), runs=loaded, not_read=not_read, problems=problems)
        else:
            output.document(document("bypass-uv", masker is not None, readings=shown, runs=loaded,
                                     not_read=not_read, problems=problems))
    return 1 if problems else 0


# ---------------------------------------------------------------------------
# windows (M22 per-phase curve statistics without an export; Track 9, A7)
# ---------------------------------------------------------------------------

# row columns named by the run or the caller, not by a person: never masked
_WINDOW_NAMES = ("source", "run", "chromatogram", "window", "phase", "curve")
WINDOW_ROW_COLUMNS = ["source", "result_id"]  # then window_stats.COLUMNS (the export's table)


def window_flags(args):
    """
    (windows, curves) of `pycorn windows`: `window_stats.Window` objects and curve keys; ValueError
    (a '--' message: `bad_flag`) when either is missing or a window does not parse (D28: no default).
    """
    from .window_stats import SYNTAX, parse_windows
    if not args.window:
        raise ValueError("--window: give at least one window (no default window): %s" % SYNTAX)
    curves = parse_list(args.window_curves)
    if not curves:
        raise ValueError("--window-curves: give curve names as the run stores them or roles (uv, conductivity, "
                         "ph, ...) (no default curves)")
    try:
        windows = parse_windows(args.window)
    except ValueError as e:
        raise ValueError("--window: %s" % e) from None
    return windows, curves


def _shown_window_row(row, masker):
    """A window row as it goes to stdout: people in its texts masked by `masker` (None: as stored)."""
    if masker is None:
        return OrderedDict(row)
    out = OrderedDict()
    for key, value in row.items():
        if key in _WINDOW_NAMES:
            out[key] = value
        elif isinstance(value, list):
            out[key] = [masker.text(v) for v in value]
        else:
            out[key] = masker.text(value)
    return out


def window_frame(rows):
    """The rows as `pycorn windows --csv` writes them: the export's table columns after source and
    result_id, warnings as JSON text per cell (as the export writes them)."""
    import pandas as pd
    from .run_export import _json
    from .window_stats import COLUMNS
    frame = pd.DataFrame(rows, columns=WINDOW_ROW_COLUMNS + COLUMNS)
    frame["warnings"] = [_json(list(w)) for w in frame["warnings"]]
    return frame


def _window_range(row):
    axis = row["axis"]
    unit = "CV" if axis == "cv" else row["time_unit"] if axis == "time" else row["volume_unit"]
    lo, hi = row["start_" + axis], row["end_" + axis]
    return "%s-%s %s" % (_fmt(lo, "%.4g"), _fmt(hi, "%.4g"), unit or "")


def window_problems(rows, windows, chrom_name, run_name, missing_curves):
    """
    What leaves a run's chromatogram short (exit 1): a curve key that matches no curve, a window
    that gave no row (phase not in the run, CV without Vc, ...), rows with n = 0 (no point inside).
    """
    out = ["%s %s: no curve %r" % (run_name, chrom_name, key) for key in missing_curves]
    for w in windows:
        mine = [r for r in rows if r["window"] == w.name]
        if not mine:
            out.append("%s %s: window %s gave no rows" % (run_name, chrom_name, w.name))
        elif any(not r["n"] for r in mine):
            out.append("%s %s: window %s: %d row(s) with no point inside (n = 0)"
                       % (run_name, chrom_name, w.name, sum(1 for r in mine if not r["n"])))
    return out


def run_windows(args, db_factory):
    """
    `pycorn windows`: `window_stats.window_stats_rows` for every run and chromatogram selected,
    the rows `pycorn export --window` writes, without an export. Exit 0, or 1 when a file was not
    read, a run not loaded, a chromatogram not read, a curve key or window gave no rows, or a
    row has n = 0 (`window_problems`).
    """
    if _to_stdout(args):
        with _MachineOutput(args, people=True) as output:
            return _windows(args, db_factory, output)
    return _windows(args, db_factory)


def _windows(args, db_factory, output=None):
    from .curve_roles import resolve_curve_keys
    from .json_output import JsonLines, document, home_tilde, learn_catalog, write_csv
    from .window_stats import INTERIM_RULE, window_stats_rows
    windows, curves = window_flags(args)
    if bool(args.selection) == bool(args.files):
        raise ValueError("windows: give --from <find CSV> or result files (.zip/.UFol), not both or neither")
    database = _once(db_factory)
    if args.selection:
        catalog = resolve_labels(read_selection(args.selection), database)
        labels = [label if isinstance(label, str) else source for label, source in
                  zip(catalog[LABEL] if LABEL in catalog.columns else catalog["source"], catalog["source"])]
    else:
        catalog, labels = _file_selection(args.files)
    mode = machine_mode(args) if output is not None else None
    masker = output.masker if output is not None else None
    learn_catalog(catalog, masker)  # labels in catalog order, before a run adds its own
    stream = JsonLines(output.out, "windows", masker is not None) if mode == "jsonl" else None
    rows, shown, notes, row_notes, short, failed, loaded = [], [], [], [], [], 0, 0
    for i, run in _runs(catalog, database, args.strict):
        loaded += 1
        if masker is not None:
            masker.absorb(run)
        where = labels[i] if i is not None else run.source
        rid = (run.origin or {}).get("result_id") if run.source == "sql" else None
        # W17: the original chromatogram(s) unless --chromatogram picks others
        picked, unmatched = run.chromatogram_selection(args.chromatogram)
        if unmatched:
            warnings.warn("%s: no chromatogram %s (it has %s)" % (run.name, ", ".join(map(repr, unmatched)), ", ".join(
                "%s (%s)" % (d["name"], d["label"]) for d in run.describe_chromatograms())))
        if not picked:
            short.append("%s: no chromatogram selected" % run.name)
        for chrom in (run.chromatograms[name] for name in picked):
            try:
                found, said = window_stats_rows(run, chrom, windows, curves)
            except Exception as e:
                if args.strict:
                    raise
                _debug_traceback(args)
                warnings.warn("%s %s: no window statistics: %s" % (run.name, chrom.name, plain_error(e)))
                failed += 1
                continue
            missing = []
            for key in curves:
                try:
                    resolve_curve_keys(chrom, [key])
                except KeyError:
                    missing.append(key)
            short += window_problems(found, windows, chrom.name, run.name, missing)
            for text in said:  # the export's warnings.csv lines (phase_windows), with the run named
                notes.append(OrderedDict([("run", run.name), ("chromatogram", chrom.name), ("text", text)]))
                warnings.warn("%s %s: %s" % (run.name, chrom.name, text))
            seen = set()
            for r in found:
                for w in r["warnings"]:  # once per window, not per curve (as the export's warnings.csv)
                    text = "window %s, %s #%d: %s" % (r["window"], r["phase"], r["occurrence"], w)
                    if text not in seen:
                        seen.add(text)
                        notes.append(OrderedDict([("run", run.name), ("chromatogram", chrom.name), ("text", text)]))
                        row_notes.append(notes[-1])
                        if output is not None:
                            warnings.warn("%s %s: %s" % (run.name, chrom.name, text))
                rows.append(OrderedDict([("source", where), ("result_id", rid)] + list(r.items())))
                if output is not None:
                    shown.append(_shown_window_row(rows[-1], masker))
                    if stream:
                        stream.write("row", shown[-1])
    _say("%d row(s) of window statistics" % len(rows))
    head = ["run", "chrom", "window", "phase", "#", "range", "curve", "n", "mean", "median", "sd", "min", "max"]
    table = [head]
    for r in rows:
        table.append([str(r["run"])[:30], r["chromatogram"], r["window"], str(r["phase"])[:30], str(r["occurrence"]),
                      _window_range(r), r["curve"], str(r["n"]), _fmt(r["mean"]), _fmt(r["median"]), _fmt(r["sd"]),
                      _fmt(r["min"]), _fmt(r["max"])])
    if rows and output is None:  # the table is the result: on stdout for people
        widths = [max(len(ascii_text(row[k])) for row in table) for k in range(len(head))]
        for row in table:
            _say("  ".join(ascii_text(c).ljust(w) for c, w in zip(row, widths)).rstrip())
        for note in row_notes:  # each window's own notes (clipping, empty), as bypass-uv lists its readings'
            _say("  %s %s %s" % (str(note["run"])[:30], note["chromatogram"], note["text"]))
    if rows:
        _say("note: " + INTERIM_RULE)
    if args.csv == "-":
        write_csv(window_frame(shown), output.out)
    elif args.csv:
        window_frame(rows).to_csv(args.csv, index=False, encoding=CSV_ENCODING)
        _say("written: %s" % args.csv)
    not_read = list(catalog.attrs.get("not_read") or [])
    problems = ["%d file(s) not read" % len(not_read)] if not_read else []
    if loaded < len(catalog):
        problems.append("%d run(s) not loaded" % (len(catalog) - loaded))
    if failed:
        problems.append("%d chromatogram(s) not read" % failed)
    problems += short
    if problems:
        _say("incomplete: %s (see the warnings above)" % "; ".join(problems))
    if mode:
        not_read = [OrderedDict([("file", home_tilde(p)), ("reason", r)]) for p, r in not_read]
        if masker is not None:
            problems = [masker.text(p) for p in problems]
            notes = [_shown_window_row(n, masker) for n in notes]
        extra = OrderedDict([("runs", loaded), ("not_read", not_read), ("problems", problems), ("warnings", notes),
                             ("notes", [INTERIM_RULE] if rows else [])])
        if stream:
            stream.summary(rows=len(shown), **extra)
        else:
            output.document(document("windows", masker is not None, rows=shown, **extra))
    return 1 if problems else 0


def _windows_command(sub, common):
    w = sub.add_parser("windows", parents=[common],
                       help="curve statistics over windows relative to a phase (M22), without an export",
                       description="For each run and chromatogram: n, mean, median, SD, min and max of the named "
                                   "curves over each window, one row per phase occurrence x window x curve: the "
                                   "rows 'pycorn export --window' writes (<chromatogram>_phase_window_statistics). "
                                   "No default window or curve.")
    w.add_argument("files", nargs="*", help=".zip / .UFol files or folders holding them")
    w.add_argument("--from", dest="selection", help="a CSV written by 'pycorn find --csv' or a file catalog")
    w.add_argument("--window", action="append", metavar="NAME=PHASE:FROM:TO UNIT",
                   help="a window relative to a phase, e.g. 'load=Sample Application:1:3 CV'; FROM/TO count from "
                        "the phase start, or from its end written end, end-N, end+N ('tail=Elution:end-2:end CV'); "
                        "UNIT s/min/h, ml/L or CV; repeatable; required")
    w.add_argument("--window-curves", action="append", metavar="NAMES",
                   help="curves: names as the run stores them or roles (uv, conductivity, ph, ...); comma list or "
                        "repeated; required")
    w.add_argument("--csv", help="write every row to this CSV (as stored); '-' writes it to stdout (people masked "
                                 "unless --no-mask)")
    w.add_argument("--strict", action="store_true", help="stop at the first run that cannot be loaded or read")
    _json_flags(w, "the rows", "one row per line", _PEOPLE_MASK_HELP)
    _chromatogram_flag(w)
    w.set_defaults(handler=run_windows)


# ---------------------------------------------------------------------------
# For scripts and agents (Track 9): --version, schema, sources
# ---------------------------------------------------------------------------

class _Version(argparse.Action):
    """--version: print `version_text()` (version and commit) and stop; read only when asked."""

    def __init__(self, option_strings, dest=argparse.SUPPRESS, default=argparse.SUPPRESS, help=None):
        super().__init__(option_strings, dest=dest, default=default, nargs=0, help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        from .version import version_text
        _say(version_text())
        parser.exit()


def _print_json(document):
    """One JSON document on stdout (ASCII: non-ASCII characters as \\u escapes)."""
    import json
    print(json.dumps(document, indent=1))


def run_schema(args, db_factory=None):
    """`pycorn schema`: tables, columns, groups, roles, units and exit codes (`pycorn.schema`); exit 0."""
    from .schema import build_schema, schema_lines
    schema = build_schema()
    if args.json:
        _print_json(schema)
    else:
        for line in schema_lines(schema):
            _say(line)
    return 0


def run_sources(args, db_factory=None):
    """
    `pycorn sources`: the database sources the settings name, each setting set or empty (never a
    value); with --test a connection to each. Exit 0, or 1 when a tested source did not connect.
    """
    from .setup_env import source_status, status_lines
    status = source_status(args.env_file, test=args.test)
    if args.json:
        _print_json(status)
    else:
        for line in status_lines(status):
            _say(line)
    return 1 if any(row["connected"] is False for row in status["sources"]) else 0


def run_inspect(args, db_factory):
    """
    `pycorn inspect` (Track 9, A3): `summary.run_summary` of every run selected, as text or one
    JSON document; masked unless --no-mask (run, folder and file names too, with labels shared
    across the document; the warnings on stderr are masked the same way). Exit 0, or 1 when a
    file was not read, a run not loaded or not summarised.
    """
    if machine_mode(args):
        with _MachineOutput() as output:
            return _inspect_masked(args, db_factory, output)
    return _inspect_masked(args, db_factory, None)


def _inspect_masked(args, db_factory, output):
    if args.no_mask:
        return _inspect(args, db_factory, None, output)
    from .summary import SummaryNames
    names = SummaryNames()
    with warnings.catch_warnings(record=True) as caught:  # said at the end, with the names masked
        warnings.simplefilter("always")
        try:
            return _inspect(args, db_factory, names, output)
        finally:
            for w in caught:
                _show_warning(names.text(str(w.message)), w.category, w.filename, w.lineno)


def _inspect(args, db_factory, names, output=None):
    from .json_output import JsonLines
    from .summary import document, run_summary, summary_lines
    if bool(args.selection) == bool(args.files):
        raise ValueError("inspect: give --from <find CSV> or result files (.zip/.UFol), not both or neither")
    database = _once(db_factory)
    if args.selection:
        catalog = resolve_labels(read_selection(args.selection), database)
        labels = [label if isinstance(label, str) else source for label, source in
                  zip(catalog[LABEL] if LABEL in catalog.columns else catalog["source"], catalog["source"])]
    else:
        catalog, labels = _file_selection(args.files)
    mask = names is not None
    if mask:  # Run1, Run2, ... in catalog order, so an imported chromatogram can name a run listed later
        for name in (catalog["name"] if "name" in catalog.columns else []):
            names.run(_text(name))
        for path in (catalog["path"] if "path" in catalog.columns else []):
            if _text(path):
                names.file(os.path.basename(str(path)))
    summaries, loaded, failed = [], 0, 0
    stream = JsonLines(output.out, "inspect", mask) if machine_mode(args) == "jsonl" else None
    for i, run in _runs(catalog, database, args.strict):
        loaded += 1
        try:
            summaries.append(run_summary(run, mask=mask, label=labels[i] if i is not None else None, names=names))
            if stream:
                stream.write("run", summaries[-1])
        except Exception as e:
            if args.strict:
                raise
            _debug_traceback(args)
            warnings.warn("run %d not summarised: %s" % (loaded, plain_error(e)))
            failed += 1
    not_read = catalog.attrs.get("not_read") or []
    if mask:
        not_read = [(names.file(os.path.basename(str(p))), names.text(r)) for p, r in not_read]
    problems = ["%d file(s) not read" % len(not_read)] if not_read else []
    if loaded < len(catalog):
        problems.append("%d run(s) not loaded" % (len(catalog) - loaded))
    if failed:
        problems.append("%d run(s) not summarised" % failed)
    not_read_rows = [OrderedDict([("file", os.path.basename(str(p))), ("reason", r)]) for p, r in not_read]
    if stream:
        stream.summary(runs=len(summaries), not_read=not_read_rows, problems=problems)
    elif args.json:
        output.document(document(summaries, mask, not_read=not_read_rows, problems=problems))
    else:
        for k, summary in enumerate(summaries):
            if k:
                _say()
            for line in summary_lines(summary):
                _say(line)
        if summaries:
            _say()
        _say("%d run(s)%s" % (len(summaries), "; masked: people, computers, instruments, serial numbers, run, "
                                                 "folder and file names (--no-mask shows them)" if mask else
                              "; not masked"))
    if problems:
        _say("incomplete: %s (see the warnings)" % ", ".join(problems), sys.stderr if args.json else None)
    return 1 if problems else 0


def _inspect_command(sub, common):
    i = sub.add_parser("inspect", parents=[common],
                       help="show what runs hold (chromatograms, curves, phases, injections, fractions, "
                            "variables, Vc) without exporting; masked by default",
                       description="For each run: name, source, folder, run start, system, column, every "
                                   "chromatogram with its curves (role, unit, points, axes) and its phase, "
                                   "injection and fraction positions, Vc and its source, method variables and "
                                   "warnings. Never curve values, IP addresses or ports. People, computers, "
                                   "instruments, serial numbers, run, folder and file names are masked (User1, "
                                   "Computer1, System1, Serial1, Run1, Folder1, File1.zip) unless --no-mask.")
    i.add_argument("files", nargs="*", help=".zip / .UFol files or folders holding them")
    i.add_argument("--from", dest="selection",
                   help="a CSV written by 'pycorn find --csv' or catalog_from_files(...).to_csv(), or both mixed")
    _json_flags(i, "a list of runs", "one run per line")
    i.add_argument("--no-mask", action="store_true",
                   help="show logins, computer and instrument names, serial numbers, run, folder and file "
                        "names as stored")
    i.add_argument("--strict", action="store_true", help="stop at the first run that cannot be loaded")
    i.set_defaults(handler=run_inspect)


def _json_flags(parser, document, lines, mask_help=None):
    """--json / --jsonl (one of them) for a command writing its result for programs; --no-mask with `mask_help`."""
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--json", action="store_true",
                       help="one JSON document on stdout (\"schema\": 1, %s); human lines go to stderr" % document)
    group.add_argument("--jsonl", action="store_true",
                       help="JSON lines on stdout as they are produced: a header, %s, a summary last" % lines)
    if mask_help:
        parser.add_argument("--no-mask", action="store_true", help=mask_help)


# what --no-mask shows for find, export and bypass-uv (run, folder, method names and variables always show)
_PEOPLE_MASK_HELP = ("--json, --jsonl and --csv - mask logins, computer and instrument names and serial numbers "
                     "(User1, System1, ...); --no-mask shows them as stored")


def _agent_commands(sub, common):
    """The subcommands for scripts and agents; each runs through `args.handler`."""
    s = sub.add_parser("schema", parents=[common],
                       help="list the exported tables and columns, download groups, curve roles, units and exit codes",
                       description="What PyCORN writes and understands, read from the code (no run data, no "
                                   "settings). --json: one JSON document with \"schema\": 1.")
    s.add_argument("--json", action="store_true", help="one JSON document on stdout, every column included")
    s.set_defaults(handler=run_schema)
    d = sub.add_parser("sources", parents=[common],
                       help="list the database sources the settings name (set or empty, never a value); --test connects",
                       description="The database sources 'pycorn find' would search, from the same settings "
                                   "(--env-file, PYCORN_ENV_FILE, the first .env found, UNICORN_* environment "
                                   "variables). Only whether each setting is set or empty is shown, never a value.")
    d.add_argument("--test", action="store_true", help="connect to each source once; exit 1 when one fails")
    d.add_argument("--json", action="store_true", help="one JSON document on stdout")
    d.set_defaults(handler=run_sources)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def build_parser():
    parser = _Parser(prog="pycorn", description="Find UNICORN runs and export them to files.")
    parser.add_argument("--env-file", help="the .env with the database settings (default: the usual lookup)")
    parser.add_argument("--debug", action="store_true", help="show the full traceback of errors (for reports)")
    parser.add_argument("--version", action=_Version, help="print the version and commit, then stop")
    sub = parser.add_subparsers(dest="command", required=True)
    # --debug also after the command ('pycorn export ... --debug'); SUPPRESS keeps one given before
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--debug", action="store_true", default=argparse.SUPPRESS,
                        help="show the full traceback of errors (for reports)")

    f = sub.add_parser("find", parents=[common],
                       help="search the databases .env lists; print the matches, save them with --csv",
                       description="Search the databases .env lists. Text filters take UNICORN wildcards (* and ?); "
                                   "a repeated text flag means any of its values. Ranges are 'a..b' (either end may be "
                                   "empty).")
    f.add_argument("--folder", help="folder path from the root, e.g. '/DefaultHome/Projects/**' (** = subfolders)")
    f.add_argument("--name", action="append", help="result name")
    f.add_argument("--run-start", help="run date range on the run's own clock, e.g. 2019-01-01..2019-06-30 "
                                       "(one date = that day)")
    f.add_argument("--system", action="append", help="equipment name")
    f.add_argument("--system-type", action="append", help="equipment type")
    f.add_argument("--run-type", action="append", help="run type, e.g. Method")
    f.add_argument("--state", action="append", help="result state, e.g. Evaluated")
    f.add_argument("--batch-id", action="append", help="batch id")
    f.add_argument("--run-index", help="run index: 3, 3,5 or 3..8")
    f.add_argument("--result-id", help="ResultIDs: 4711, 4711,4712 or 4700..4800")
    f.add_argument("--created-by", action="append", help="user who created the result")
    f.add_argument("--column", action="append", help="column name UNICORN recorded at run time")
    f.add_argument("--var", action="append", metavar="SPEC",
                   help="method variable, repeatable: 'Flow rate>=1 ml/min', 'UV1==280', "
                        "'Column position=Position 2' (text), 'Sample_ID' (has it); "
                        "operators = == != < <= > >=")
    f.add_argument("--has-role", action="append", help="curve roles the run must have, e.g. uv,conductivity")
    peaks = f.add_mutually_exclusive_group()
    peaks.add_argument("--has-peaks", dest="has_peaks", action="store_const", const=True, help="has peak tables")
    peaks.add_argument("--no-peaks", dest="has_peaks", action="store_const", const=False, help="has no peak tables")
    f.add_argument("--chromatograms", help="number of chromatograms: 1, 1,2 or 2..")
    f.add_argument("--include-hidden", action="store_true", help="include results in hidden folders")
    f.add_argument("--sources", action="append",
                   help="where to search (default: everything .env lists): 'discover', an alias, "
                        "or alias:db1,db2 / alias:* ; repeatable")
    f.add_argument("--dedupe", action="store_true", help="keep one copy of each run (the most recently changed)")
    f.add_argument("--limit", type=int, default=500, help="rows per page (default 500, at most 5000)")
    f.add_argument("--after", help="continue after this row: 'alias#n:ResultID' as find printed it")
    f.add_argument("--all", action="store_true", help="every page, not just the first")
    f.add_argument("--variables", action="store_true", help="add a column per method variable to --csv")
    f.add_argument("--show", type=int, default=SHOW, help="rows to print (default %d)" % SHOW)
    f.add_argument("--csv", help="write the selection to this CSV (input for export --from); '-' writes it to "
                                 "stdout (the database named by its label, people masked unless --no-mask)")
    f.add_argument("--strict", action="store_true", help="stop at a database that cannot be searched")
    _json_flags(f, "the rows", "one row per line", _PEOPLE_MASK_HELP)

    e = sub.add_parser("export", parents=[common], help="write runs to files, one per chromatogram",
                       description="Write runs to <out>/<alias#n>/<folder tree>/<run name>/<chromatogram>.<ext>. "
                                   "Runs already there are skipped; an index.csv lists every run.")
    e.add_argument("files", nargs="*", help=".zip / .UFol files or folders holding them")
    e.add_argument("--from", dest="selection",
                   help="a CSV written by 'pycorn find --csv' or catalog_from_files(...).to_csv(), or both mixed")
    e.add_argument("--out", required=True, help="output folder")
    e.add_argument("--format", action="append",
                   help="csv, xlsx, pickle; comma list or repeated (default csv)")
    e.add_argument("--overwrite", action="store_true", help="replace runs already exported")
    e.add_argument("--update", action="store_true",
                   help="export again the runs changed since they were exported (or not checkable)")
    e.add_argument("--strict", action="store_true", help="stop at the first run that cannot be loaded or written")
    e.add_argument("--options", metavar="FILE.json",
                   help="the export options from a JSON file (the options.json an export writes next to index.csv, "
                        "or the page's); flags given here replace the file's value of their option")
    _json_flags(e, "the index rows and totals", "one index row per run", _PEOPLE_MASK_HELP +
                "; the files written are not changed (--mask-identifying masks those)")
    _chromatogram_flag(e)
    o = e.add_argument_group("extra outputs (opt-in)")
    o.add_argument("--units", action="append", metavar="Q=UNIT",
                   help="convert before writing, e.g. volume=L,flow=L/h,absorbance=AU,time=h")
    o.add_argument("--method-hints", action="store_true",
                   help="planned vs executed blocks and injections from the method (hints only)")
    o.add_argument("--mask-identifying", action="store_true",
                   help="write logins, computer and instrument names and serial numbers as User1, Computer1, "
                        "System1, Serial1 (for files you share)")
    o.add_argument("--path-length", metavar="CM",
                   help="UV path length (cm) used only where the run gives none (mg/ml and CADET)")
    o.add_argument("--epsilon", action="append", metavar="NM=VALUE",
                   help="mass extinction coefficient per wavelength, ml/(mg*cm) unless a unit follows, e.g. "
                        "280=1.4; repeatable; adds mg/ml curves")
    o.add_argument("--epsilon-molar", action="append", metavar="NM=VALUE",
                   help="molar extinction coefficient per wavelength, 1/(M*cm), e.g. 280=43824 (needs --mw)")
    o.add_argument("--mw", help="molecular weight, e.g. '66000 g/mol': for --epsilon-molar, and with --epsilon for "
                                "the CADET export's mol/m^3")
    o.add_argument("--uv-response", metavar="FACTOR",
                   help="detector response factor on the extinction coefficient, e.g. 0.92 for a detector that "
                        "reads 8%% low; no default (mg/ml and CADET)")
    o.add_argument("--bypass-uv", action="store_true",
                   help="UV of sample sent to the column by-pass, per window and channel (<chromatogram>_by_pass_uv);"
                        "implied by --sample-conc / --loop-volume")
    _bypass_flags(o)
    o.add_argument("--window", action="append", metavar="NAME=PHASE:FROM:TO UNIT",
                   help="curve statistics over a window relative to a phase (<chromatogram>_phase_window_statistics), "
                        "e.g. 'load=Sample Application:1:3 CV'; FROM/TO count from the phase start, or from its "
                        "end written end, end-N, end+N ('tail=Elution:end-2:end CV'); UNIT s/min/h, ml/L or CV; "
                        "repeatable; needs --window-curves; no default window")
    o.add_argument("--window-curves", action="append", metavar="NAMES",
                   help="curves for --window: names as the run stores them or roles (uv, conductivity, ph, ...); "
                        "comma list or repeated; no default")
    o.add_argument("--bed-height", help="column bed height, e.g. '10 cm'; used where the run stores none")
    o.add_argument("--diameter", help="column inner diameter, e.g. '1 cm'; used where the run stores none")
    o.add_argument("--particle-diameter", help="particle diameter, e.g. '34 um'; used where the run stores none")
    o.add_argument("--override-geometry", action="store_true",
                   help="the given column geometry wins over the stored one")
    o.add_argument("--cadet", action="store_true", help="CADET-Process CSVs in <chromatogram>_cadet/")
    o.add_argument("--cadet-core", choices=("5", "6"), help="CADET-Core 5.1.2 or 6.0 input file per chromatogram")
    o.add_argument("--core-format", choices=("json", "hdf5"),
                   help="file type of --cadet-core (default json; hdf5 needs h5py)")
    o.add_argument("--mixer-volume", help="system mixer volume, e.g. '0.6 ml'; wins over the stored mixer size "
                                          "(CADET outputs)")
    o.add_argument("--tubing-volume", help="column -> UV tubing volume, e.g. '0.2 ml'; the run stores none "
                                           "(CADET outputs)")
    o.add_argument("--sample-inlets", action="append", metavar="POS",
                   help="inlet positions holding sample pumped through system pump line A/B, e.g. A5; comma list "
                        "or repeated; default: found from injection marks (CADET outputs)")
    o.add_argument("--no-system-volumes", action="store_true",
                   help="leave the mixer and tubing CSTRs out of the CADET outputs")
    o.add_argument("--core-overrides", metavar="FILE.json",
                   help="your model values in CADET-Core's own input layout, from the root ({\"input\": {...}}); "
                        "merged into every --cadet-core file unchanged")
    o.add_argument("--process-overrides", metavar="FILE.json",
                   help="your model values in CADET-Process's own parameter layout (as process.parameters shows "
                        "it); stored unchanged in each <chromatogram>_cadet/experiment.json (--cadet)")

    s = e.add_argument_group("smaller files (opt-in; the CADET files and the stored run are not changed)")
    s.add_argument("--tables", action="append", metavar="LIST",
                   help="write only these groups of tables, as the web page groups them: key (phases, "
                        "injections, fractions, pooled fractions), peaks, method, details, audit, runlog (events, "
                        "run log sections, pauses); a group keeps the curves table (which curves: --curves). Or "
                        "single tables, e.g. curves,events,phases. Comma list or repeated; README, column and "
                        "timeline, warnings and column guide are always written (sheets of the workbook for "
                        "xlsx); default all")
    s.add_argument("--curves", action="append", metavar="NAMES",
                   help="curves table: only these curves, by name as the run stores them (comma list or repeated)")
    s.add_argument("--curve-roles", action="append", metavar="ROLES",
                   help="curves table: only the curves of these roles, e.g. uv,conductivity (works across "
                        "instruments; with --curves both)")
    s.add_argument("--max-points", metavar="N",
                   help="thin each curve on its own axis to about N points (needs --method); step transitions stay")
    s.add_argument("--method", choices=("interval", "derivative", "minmax", "hybrid"),
                   help="how --max-points picks points: evenly spaced, more where the curve changes, the "
                        "smallest and largest value per bucket, or hybrid (half derivative, half evenly "
                        "spaced, plus min/max and step transitions; the page's rule)")
    s.add_argument("--every", metavar="N",
                   help="instead of --max-points: keep every Nth stored point of each curve (step transitions stay)")
    s.add_argument("--derivative-share", metavar="0-1",
                   help="with --method interval or derivative: the share of points placed where the curve "
                        "changes (1 = all; 0.5 = half, half evenly spaced)")
    s.add_argument("--grid-step", metavar="STEP",
                   help="instead of --max-points: every curve on one evenly spaced grid of this step, in the "
                        "unit of --grid-axis; each curve's other axes follow it; pauses are bridged")
    s.add_argument("--grid-axis", choices=("time", "volume", "cv"), help="axis of --grid-step (default time)")

    b = sub.add_parser("bypass-uv", parents=[common], help="UV of sample sent to the column by-pass, and the extinction coefficient "
                                         "it gives",
                       description="For each run: every window where sample went to the column by-pass (run log) "
                                   "and every live UV channel there. Plateau: coefficient = (max - before) / "
                                   "(path * --sample-conc); pulse: area / (path * --loop-volume * --sample-conc). "
                                   "An estimate: check the detector's linear range (--absorbance-limit).")
    b.add_argument("files", nargs="*", help=".zip / .UFol files or folders holding them")
    b.add_argument("--from", dest="selection", help="a CSV written by 'pycorn find --csv' or a file catalog")
    b.add_argument("--csv", help="write every reading to this CSV; '-' writes it to stdout (people masked unless "
                                 "--no-mask)")
    b.add_argument("--strict", action="store_true", help="stop at the first run that cannot be loaded")
    _json_flags(b, "the readings", "one reading per line", _PEOPLE_MASK_HELP)
    _chromatogram_flag(b)
    _bypass_flags(b)
    _agent_commands(sub, common)
    _inspect_command(sub, common)
    _windows_command(sub, common)
    return parser


def _chromatogram_flag(parser):
    # W17 (owner, 2026-11-11); not --chromatograms, which `find` uses for the number of chromatograms
    parser.add_argument("--chromatogram", action="append", metavar="NAME|all|imported|derived",
                        help="which chromatograms of each run: a name as the run stores it, or all, original, "
                             "imported (copied in from another result in Evaluation) or derived (made in "
                             "Evaluation); repeatable; default: the run's original chromatogram only")


def _bypass_flags(group):
    group.add_argument("--sample-conc", help="sample concentration from an offline assay, e.g. '1.2 mg/ml': gives the "
                                             "by-pass extinction coefficient")
    group.add_argument("--loop-volume", help="fixed loop volume, e.g. '20 ul': needed for a coefficient from a pulse "
                                             "(the valve period only bounds it)")
    group.add_argument("--absorbance-limit", metavar="MAU",
                       help="detector linear limit in stored units, mAU unless a unit follows: no by-pass coefficient "
                            "and mg/ml NaN above it; no default")


def _debug_traceback(args):
    """Under --debug: the traceback of the exception being handled, to stderr."""
    if getattr(args, "debug", False):
        import traceback
        traceback.print_exc()


# errors with a sentence of their own (exit 2); anything else is unexpected (exit 3)
_KNOWN_ERRORS = (ValueError, KeyError, LookupError, ImportError, ConnectionError, OSError, MemoryError)


def main(argv=None):
    """
    Run one command. Exit 0 when everything asked for was done, 1 when it ran but left
    something out (said on screen), 2 for an error in plain words, 3 for an unexpected one,
    130 when interrupted. --debug adds the traceback. With --json / --jsonl an error is one
    JSON object on stderr ({'error': {'text', 'key', 'exit'}}, `messages.ERROR_KEYS`), usage
    errors too.
    """
    given = list(sys.argv[1:] if argv is None else argv)
    _MACHINE[0] = any(a in ("--json", "--jsonl") for a in given)
    try:
        args = build_parser().parse_args(given)
    finally:
        _MACHINE[0] = False
    # recorded with every export (README.txt, provenance.json, Excel summary) to reproduce it
    args.command_line = "pycorn " + " ".join(shlex.quote(a) for a in given)
    _SHOWN.clear()
    _ACTIVE_MASKER[0] = None
    machine = machine_mode(args) is not None

    def db_factory():
        from .db_connector import PyCORNDatabase
        return PyCORNDatabase(env_file=args.env_file)

    with warnings.catch_warnings():  # filters and display for this command only
        warnings.showwarning = _show_warning
        try:
            if machine and getattr(args, "csv", None) == "-":
                raise ValueError("--csv - and --%s both write to stdout: give one of them" % machine_mode(args))
            if getattr(args, "handler", None):  # commands added in their own functions (_agent_commands)
                return args.handler(args, db_factory)
            if args.command == "find":
                return run_find(args, db_factory())
            if args.command == "bypass-uv":
                return run_bypass_uv(args, db_factory)
            return run_export(args, db_factory)
        except KeyboardInterrupt as e:
            if machine:
                _json_error(e, args, 130, "interrupted")
            else:
                _say("interrupted", sys.stderr)
            return 130
        except _KNOWN_ERRORS as e:
            _debug_traceback(args)
            if args.debug and getattr(e, "detail", None):
                _say("detail: %s" % e.detail, sys.stderr)
            if machine:
                _json_error(e, args, 2)
                return 2
            _say("error: %s" % plain_error(e), sys.stderr)
            if isinstance(e, ConnectionError):
                _say(SETUP_HINT, sys.stderr)
            return 2
        except Exception as e:
            _debug_traceback(args)
            if machine:
                _json_error(e, args, 3, "unexpected error (%s: %s)" % (type(e).__name__, plain_error(e)))
                return 3
            _say("unexpected error (%s: %s)" % (type(e).__name__, plain_error(e)), sys.stderr)
            if not args.debug:
                _say(REPORT, sys.stderr)
            return 3


if __name__ == "__main__":
    sys.exit(main())
