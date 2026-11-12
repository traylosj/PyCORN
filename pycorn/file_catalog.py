# -*- coding: utf-8 -*-
"""
The run catalog of result files: the same columns `PyCORNDatabase.find()` returns, built
from `.zip` / `.Result` exports and `.UFol` folder exports, so files and databases can be
compared, matched and filtered alike (docs/ROADMAP.md, "Run search").

Files are read light (owner, 2026-11-08): only `Result.xml` and the chromatogram XML of
each bundle, never the curve data, so a large folder of exports is listed quickly.
`open_run()` / `open_folder()` load the full runs.

A file row has `source` 'zip' or 'ufol' (as `run.source`), no `database`, `result_id`,
`folder_id` or `tstamp`, and three more columns: `path` (absolute path of the file),
`member` (the result inside a .UFol, '' for a .zip) and `original_id` (the ResultID the
.UFol export recorded, else None). (`path`, `member`) identifies a file row (`FILE_KEY`).

`match_key` is built as for the database (`pycorn.search.match_key`); `n_copies` and
`duplicate_group` mark the same run appearing more than once among the files listed.
`PyCORNDatabase.match_files()` finds each file's results in the databases.
"""
import datetime as dt
import io
import os
import re
import tarfile
import warnings
from collections import OrderedDict
from types import SimpleNamespace
from zipfile import ZipFile

from .column_geometry import used_columns
from .messages import plain_error
from .model import chromatogram_kind
from .search import CATALOG_COLUMNS, COPY_COLUMNS, _add_match_keys, duplicate_group, match_key
from .sources.zip_source import NOT_A_RESULT, _STRUCTURED_XML, _as_list, _bool, _build_result, _read_entry, _typed
from .timeline import _run_start_date, parse_iso
from .variables import to_frame as _variables_frame

FILE_COLUMNS = ["path", "member", "original_id"]
FILE_CATALOG_COLUMNS = CATALOG_COLUMNS + FILE_COLUMNS
FILE_KEY = ["path", "member"]
FILE_VARIABLE_COLUMNS = ["path", "member", "kind", "variable", "value", "number", "unit", "scouting"]

_BUNDLE_EXT = (".zip", ".result")
_FOLDER_EXT = (".ufol",)
_ORIGINAL_ID = re.compile(r"\.(\d+)\.Result$")


# other UNICORN files a folder may hold: skipped, with a note saying why
_NOT_READ = {".asc": "UNICORN text exports (curves only): export the result as .zip",
             ".res": "UNICORN 3-5 results: read them with pycorn.PcRes3"}


def find_files(paths):
    """Result files under `paths` (files, or folders searched recursively), sorted, once each."""
    out = []
    for path in [paths] if isinstance(paths, (str, os.PathLike)) else paths:
        path = os.path.abspath(str(path))
        if os.path.isdir(path):
            skipped = {}
            for folder, _, names in os.walk(path):
                for n in names:
                    ext = os.path.splitext(n)[1].lower()
                    if ext in _BUNDLE_EXT + _FOLDER_EXT:
                        out.append(os.path.join(folder, n))
                    elif ext in _NOT_READ:
                        skipped[ext] = skipped.get(ext, 0) + 1
            for ext, n in sorted(skipped.items()):
                warnings.warn("%s: %d %s file(s) skipped (%s)" % (path, n, ext, _NOT_READ[ext]), stacklevel=3)
        elif os.path.isfile(path):
            if os.path.splitext(path)[1].lower() not in _BUNDLE_EXT + _FOLDER_EXT:
                raise ValueError("not a UNICORN result file (.zip, .Result or .UFol): %s" % path)
            out.append(path)
        else:
            raise FileNotFoundError("no such file or folder: %s" % path)
    return sorted(dict.fromkeys(out))


def _bundles(path):
    """(member, original_id, bundle bytes or path) of every result in one file."""
    if os.path.splitext(path)[1].lower() in _FOLDER_EXT:
        with tarfile.open(path, mode="r:") as archive:
            for member in archive:
                if member.isfile() and member.name.endswith(".Result"):
                    match = _ORIGINAL_ID.search(member.name)
                    yield member.name, int(match.group(1)) if match else None, \
                        io.BytesIO(archive.extractfile(member).read())
    else:
        yield "", None, path


def read_headers(bundle):
    """
    The light read of one result bundle (path or file object): `run.result` as
    `open_run()` builds it, and the `info` of each chromatogram (no curves, events or
    peaks; `has_peak_tables` says whether it has any; `kind`: original, imported or derived,
    `model.chromatogram_kind`). Returns a namespace with `result`,
    `chromatograms` (list of {'name', 'info', 'has_peak_tables', 'kind'}) and `warnings`.
    """
    notes, result_entry, chromatograms = [], None, []
    with ZipFile(bundle) as z:
        for info in z.infolist():
            name = info.filename
            if not name.lower().endswith(".xml"):
                continue
            entry = _read_entry(name, z.read(info), notes)
            if name == "Result.xml":
                result_entry = entry
            elif name not in _STRUCTURED_XML and isinstance(entry.value, dict) and "Chromatogram" in entry.value:
                root = entry.value["Chromatogram"] or OrderedDict()
                header = OrderedDict((k, v) for k, v in root.items()
                                     if k not in ("Curves", "EventCurves", "PeakTables", "PoolTables"))
                flags = [[_bool(x.get("IsOriginalData")) for x in _as_list((root.get(k) or {}).get(item))
                          if isinstance(x, dict)] for k, item in (("Curves", "Curve"), ("EventCurves", "EventCurve"))]
                chromatograms.append({
                    "entry": name,  # the bundle entry: a repeated name is keyed as `open_run` keys it
                    "name": root.get("ChromatogramName") or name.rsplit(".", 1)[0],
                    "info": _typed(header),
                    "has_peak_tables": bool(_as_list((root.get("PeakTables") or {}).get("PeakTable"))),
                    "kind": chromatogram_kind(_typed(header), *flags),  # W17, as `Chromatogram.kind`
                })
    if result_entry is None and not chromatograms:
        raise ValueError(NOT_A_RESULT)
    return SimpleNamespace(result=_build_result(result_entry, notes), chromatograms=chromatograms, warnings=notes)


def file_chromatograms(path, member=""):
    """
    `model.describe_chromatograms` of one result in a file (a .zip, or `member` of a .UFol) from
    the light read (W17): name (keyed as `open_run` keys it), kind, source result, copy of this run.
    """
    from .model import describe_chromatograms
    member = member if isinstance(member, str) else ""
    for m, original_id, bundle in _bundles(path):
        if (m or "") != member:
            continue
        headers = read_headers(bundle)
        items, keys = [], set()
        for c in headers.chromatograms:
            key = c["name"] if c["name"] not in keys else "%s (%s)" % (c["name"], c["entry"])
            keys.add(key)
            items.append((key, c["kind"], c["info"]))
        return describe_chromatograms(items, original_id, headers.result.get("Name"))
    raise ValueError("%s: no result %s in this file" % (os.path.basename(str(path)), member))


def _local_utc(info, field):
    """(local naive datetime, UTC aware datetime) of a stored time and its '<field>UtcOffsetMinutes'."""
    local = parse_iso(str(info.get(field) or ""))
    offset = info.get(field + "UtcOffsetMinutes")
    if local is None or offset is None:
        return None, None
    try:
        offset = int(float(offset))
    except (TypeError, ValueError):
        return None, None
    return local, (local - dt.timedelta(minutes=offset)).replace(tzinfo=dt.timezone.utc)


def _row(headers, path, member, original_id, source):
    result = headers.result
    starts = [t for t in (_local_utc(c["info"], "Created") for c in headers.chromatograms) if t[1] is not None]
    first = min(starts, key=lambda t: t[1]) if starts else (None, None)
    run_start = _run_start_date(headers)
    if run_start is not None:
        start_local, start_utc, start_source = run_start[0], run_start[2], "RunStartDate"
    else:
        start_local, start_utc = first
        start_source = "Chromatogram.Created" if start_utc is not None else None
    columns = used_columns(headers)
    run_index = result.get("RunIndex")
    return {
        "source": source, "database": None, "result_id": None,
        "name": result.get("Name"), "folder_path": result.get("FolderPath"), "folder_id": None,
        "run_start_local": start_local, "run_start_utc": start_utc, "run_start_source": start_source,
        "added_utc": _local_utc(result, "Created")[1], "modified_utc": _local_utc(result, "LastModified")[1],
        "system": result.get("SystemName"), "system_type": result.get("SystemTypeName"),
        "run_type": result.get("RunType"), "state": result.get("ResultState"),
        "batch_id": None if result.get("BatchId") is None else str(result.get("BatchId")),
        "run_index": None if run_index is None else int(run_index),
        "created_by": result.get("CreatedBy"),
        "column": columns[0][0] if columns else None,
        "n_chromatograms": len(headers.chromatograms),
        "has_peak_tables": any(c["has_peak_tables"] for c in headers.chromatograms),
        "first_chromatogram_utc": first[1], "tstamp": None,
        "path": path, "member": member, "original_id": original_id,
    }


def catalog_from_files(paths, variables=False):
    """
    The run catalog of the result files under `paths` (files or folders, searched
    recursively): one row per result, `FILE_CATALOG_COLUMNS`, in path order and, inside a
    .UFol, archive order. A file that cannot be read is left out with a warning, and listed
    in `catalog.attrs['not_read']` as (path, plain reason).

    With `variables=True`, returns (catalog, variables): the method variables of every
    row as one long table (`FILE_VARIABLE_COLUMNS`, as `run.variables()` gives them).
    """
    import pandas as pd
    rows, tables, not_read = [], [], []
    for path in find_files(paths):
        source = "ufol" if os.path.splitext(path)[1].lower() in _FOLDER_EXT else "zip"
        member = ""
        try:
            for member, original_id, bundle in _bundles(path):
                headers = read_headers(bundle)
                rows.append(_row(headers, path, member, original_id, source))
                if variables:
                    tables.append(_variables_frame(headers).assign(path=path, member=member))
        except Exception as e:  # one bad file doesn't stop the listing
            reason = plain_error(e) + (" (member %s)" % member if member else "")
            not_read.append((path, reason))
            warnings.warn("%s not read: %s" % (os.path.basename(path), reason), stacklevel=2)
    data = {c: [r[c] for r in rows] for c in FILE_CATALOG_COLUMNS if c not in COPY_COLUMNS}
    for c in ("run_start_utc", "added_utc", "modified_utc", "first_chromatogram_utc"):
        data[c] = pd.to_datetime(data[c], utc=True)
    data["run_start_local"] = pd.to_datetime(data["run_start_local"])
    _add_match_keys(data)
    catalog = pd.DataFrame({c: pd.Series(data[c], dtype=object) if not len(rows) else data[c]
                            for c in FILE_CATALOG_COLUMNS})
    catalog = mark_file_copies(catalog)
    catalog.attrs["not_read"] = not_read
    if not variables:
        return catalog
    if tables:
        table = pd.concat(tables, ignore_index=True)[FILE_VARIABLE_COLUMNS]
    else:
        table = pd.DataFrame({c: pd.Series(dtype=object) for c in FILE_VARIABLE_COLUMNS})
    for c in ("kind", "variable", "unit"):
        table[c] = table[c].astype(object).astype("category")
    return catalog, table


def run_match_key(run):
    """The `match_key` of a loaded run, built as `catalog_from_files` builds it for the file row."""
    starts = [u for u in (_local_utc(c.info or {}, "Created")[1] for c in run.chromatograms.values()) if u is not None]
    run_index = (run.result or {}).get("RunIndex")
    return match_key(min(starts) if starts else None, (run.result or {}).get("SystemName"),
                     None if run_index is None else int(run_index))


def is_file_row(path, database):
    """True for a catalog row of a file (a `path`, no `database`)."""
    return not _missing(path) and _missing(database)


def file_row_key(path, member):
    """(absolute path, member) of a file row; the same as `run_file_key` of the run loaded from it."""
    return os.path.abspath(str(path)), member if isinstance(member, str) else ""


def run_file_key(run):
    """(absolute path, member) a file run was read from (`run.origin`), else None."""
    origin = run.origin or {}
    if not origin.get("path"):
        return None
    return file_row_key(origin["path"], origin.get("member"))


def load_files(catalog, strict=False, check_key=True, adjust_axes=True, column_volume=None, column_volume_unit=None,
               use_name_hint=False, on_skip=None):
    """
    Load every file row of a catalog (`catalog_from_files()`, filtered further or
    deduplicated as you like), one `Run` at a time: a generator, so only one run is in memory.
    Rows are loaded in catalog order; consecutive rows of the same .UFol are read in one pass
    over it, in archive order, each member once.

    A file or member that cannot be read (moved, deleted, broken) is skipped with a warning
    naming the file; `strict=True` raises instead. When the catalog has `match_key`, a run
    whose key differs from its row's (the file was replaced since the catalog was made) is
    loaded as it is now, with a warning (`check_key=False` skips the check). Other
    arguments as for `open_run()`. `PyCORNDatabase.load_many()` takes catalogs that mix
    database and file rows. `on_skip(key, message)`: called for each row skipped, `key`
    ('file', path, member) as `file_row_key` gives it, `message` the warning's reason.
    """
    from .sources import _axes_options, open_run
    from .sources.ufol_source import iter_ufol
    if "path" not in catalog.columns:
        raise ValueError("load_files: the catalog needs a path column (catalog_from_files rows)")
    databases = catalog["database"] if "database" in catalog.columns else [None] * len(catalog)
    if not all(is_file_row(p, d) for p, d in zip(catalog["path"], databases)):
        raise ValueError("load_files: database rows (no path) are loaded by PyCORNDatabase.load_many")
    members = catalog["member"] if "member" in catalog.columns else [""] * len(catalog)
    keys = catalog["match_key"] if check_key and "match_key" in catalog.columns else [None] * len(catalog)
    groups = []  # consecutive rows of one file: one open, one pass
    for path, member, key in zip(catalog["path"], members, keys):
        path, member = file_row_key(path, member)
        if not groups or groups[-1][0] != path:
            groups.append((path, OrderedDict()))
        groups[-1][1].setdefault(member, None if _missing(key) else key)
    axes = _axes_options(column_volume, column_volume_unit, use_name_hint)

    def skip(message, path, members):
        if strict:
            raise LookupError(message)
        warnings.warn(message + "; skipped", stacklevel=3)
        if on_skip is not None:
            for member in members:
                on_skip(("file", path, member), message)

    def checked(run, key, where):
        if key is not None and run_match_key(run) != key:
            warnings.warn("%s changed since the catalog (another run in the file now); loading it as "
                          "it is now" % where, stacklevel=3)
        return run

    for path, wanted in groups:
        name = os.path.basename(path)
        if not os.path.isfile(path):
            skip("%s not found (moved or deleted since the catalog was made?)" % name, path, list(wanted))
            continue
        found = set()
        try:
            if os.path.splitext(path)[1].lower() in _FOLDER_EXT:
                for run in iter_ufol(path, adjust_axes=adjust_axes, axes_options=axes, members=set(wanted)):
                    member = run.container["member"]
                    found.add(member)
                    yield checked(run, wanted[member], "%s member %s" % (name, member))
                for member in wanted:
                    if member not in found:
                        skip("%s has no member %s (the folder export changed since the catalog was made?)"
                             % (name, member), path, [member])
            else:
                yield checked(open_run(path, adjust_axes=adjust_axes, column_volume=column_volume,
                                       column_volume_unit=column_volume_unit, use_name_hint=use_name_hint),
                              next(iter(wanted.values())), name)
        except Exception as e:
            if strict:
                raise
            skip("%s not read: %s" % (name, plain_error(e)), path, [m for m in wanted if m not in found])


def _missing(value):
    import pandas as pd
    return value is None or (not isinstance(value, str) and pd.isna(value))


def match_zip_to_db(paths, db, sources=None, strict=False):
    """Which database results the files under `paths` are: `db.match_files(paths)` (plan section 5)."""
    return db.match_files(paths, sources=sources, strict=strict)


def mark_file_copies(catalog):
    """`n_copies` / `duplicate_group` among the file rows themselves (the same run exported twice)."""
    import pandas as pd
    out = catalog.copy()
    if not len(out):
        return out
    counts = out["match_key"].value_counts()
    n = [None if pd.isna(k) else int(counts[k]) for k in out["match_key"]]
    out["n_copies"] = pd.array(n, dtype="Int64")
    out["duplicate_group"] = [duplicate_group(k) if c is not None and c > 1 else None  # c None: no key
                              for k, c in zip(out["match_key"], n)]
    return out
