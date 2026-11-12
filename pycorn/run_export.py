# -*- coding: utf-8 -*-
"""
Write runs to files, one file per chromatogram (`pycorn export`, run search step 5).

Layout (owner, 2026-11-08):

    <out>/<alias>#<n>/<UNICORN folder tree>/<run name>/<chromatogram>.<ext>

`alias#n` is the database label warnings use (`PyCORNDatabase.database_labels()`); file
runs use 'zip' or 'ufol' and the folder tree stored in the file. Names are cleaned to
characters every file system accepts. When several runs of one export would get the same
folder, all of them get `_<ResultID>` (file runs: the original id of a .UFol member, else
the file name). A run folder that already exists is skipped unless `overwrite=True`.

Table names (owner, 2026-11-09): one name per table, the same in the in-browser page, the
Excel sheets and the CSV files (`pycorn.export_tables`: `title(key)` for sheets, `file_stem(key)`
for files); code keys (`hint_blocks`, `run_info`, ...) stay internal. Every run folder also gets
`README.txt` (what each file holds, its origin, how it was made). With csv (or pickle, or only
CADET files) it also gets the run-level CSVs, `provenance.json` (`provenance()`: version,
commit, route, command, options) and `column_guide.csv` (every column: UNICORN's data or
PyCORN's analysis). With xlsx and no csv (owner, 2026-11-10, W4) the folder holds the
workbook(s) and `README.txt` only: the run-level tables, the column guide and the provenance
are sheets of the workbook.

Chromatograms (W17, owner 2026-11-11): by default only the run's original chromatogram(s) are
written; imported and derived ones (Evaluation) are written when `ExportOptions.chromatograms`
(`--chromatogram NAME|all|original|imported|derived`, repeatable) selects them, under their own
stems (`_file_stems`: the same stem whichever chromatograms are written). When the default leaves
chromatograms out, warnings.csv names them.

Formats, each one file per chromatogram:

- 'csv': `<chrom>.csv` (every curve with its time, volume, CV
  and UTC, raw values), `<chrom>_events.csv`, and when not empty `<chrom>_peaks.csv`,
  `_peak_table_summary.csv` (each peak table's header, as UNICORN's report prints it), `_phases.csv`, `_injections.csv`, `_fractions.csv` (start/end on every axis, row 0 the
  leading pseudo-row; fractions with the pool they are in), `_pooled_fractions.csv` (`pycorn.pools`),
  `_run_log_sections.csv` (process sections from the executed run log,
  with the curve checks), `_pauses.csv` (the timeline's pauses); per run
  `method_variables.csv`, `column_and_timeline.csv` (one row per chromatogram: Vc and its
  candidates, column geometry, timeline, provenance), `warnings.csv` and `column_guide.csv`
  (owner, 2026-11-08: these extra tables by default), and when not empty `run_and_method.csv`,
  `method_steps.csv`, `scouting_runs.csv`, `start_questions.csv`, `column.csv`, `calibration.csv`, `instrument.csv`,
  `system_settings.csv`, `audit_log.csv` (`pycorn.run_details`; owner, 2026-11-09: in every
  export path, IP addresses and ports never).
- 'xlsx': `<run name>_<chrom>.xlsx`: a Summary sheet (with how it was made), a chromatogram
  chart, then a sheet per non-empty table above, formatted for reading
  (`pycorn.excel_export`; xlsxwriter, a core dependency; with only openpyxl, the plain tables);
  the run-level tables (variables, column and timeline, warnings, column guide, ...) are sheets
  of it, not files.
- 'pickle': `<chrom>.pkl`, the chromatogram's part of `Run.to_res3_dict()`
  ({chromatogram name: {curve or event name: {...}}}). Only open pickles you trust.

UTC columns are UTC without a zone (Excel cannot store zones). Cells holding lists or
dicts (section inlets, Vc candidates, ...) are JSON text. An extra table that cannot be
built is left out with a line in warnings.csv; the curves are still written.

Opt-in outputs (`ExportOptions`, the `pycorn export` flags; owner decisions in
docs/ROADMAP.md, "Default outputs and flag decisions"):

- `units`: the run converted (`run.normalized`) before anything is written.
- `mask_identifying`: logins, computer and system names and serial numbers as User1,
  Computer1, System1, Serial1 in every table and pickle written (`run_details.Masker`); the
  CADET files hold none of them.
- `epsilon` / `epsilon_molar` + `molecular_weight` per wavelength: a mg/ml curve for each UV
  curve at a listed wavelength, as extra '<curve> (mg/ml): ...' columns, and a
  `_concentration` table (epsilon, response factor, path and their sources). Each mg/ml curve
  adds the concentration accuracy note to warnings.csv (issue #1).
- `uv_response`: detector response factor on epsilon (caller only, no default; issue #1).
- `bypass_uv` (+ `sample_concentration`, `loop_volume`): `_by_pass_uv` table, the UV of sample
  sent to the column by-pass and the coefficient it gives (`pycorn.uv_calibration`, issue #1).
- `windows` + `window_curves` (M22): `_phase_window_statistics` table, n, mean, median, SD, min
  and max of the named curves over windows relative to a phase (`pycorn.window_stats`; no
  default window or curve).
- `absorbance_limit`: mg/ml NaN and no by-pass coefficient above it (caller only).
- `path_length_cm`: UV path used only where the run gives none (mg/ml and CADET).
- `bed_height`, `diameter`, `particle_diameter` (+ `override_geometry`): the caller's column
  geometry (D26); a run without any Vc gets Vc from it as a last resort (source 'geometry').
- `method_hints`: `_planned_vs_run_blocks` / `_planned_vs_run_injections` (planned vs executed;
  hints only, D17).
- `route`, `command`: provenance only, recorded with the export (`provenance()`).
- `cadet`: CADET-Process CSVs and experiment.json in `<chrom>_cadet/`.
- `cadet_core` ('5' or '6'): CADET-Core input `<chrom>_core_v<n>.json` (or `.h5` with
  `core_format='hdf5'`, needs h5py).
"""
import dataclasses
import datetime as dt
import hashlib
import json
import os
import pickle
import re
import shutil
import zipfile
from pathlib import Path
from xml.etree import ElementTree

from .export_tables import column_guide, file_stem
from .messages import Message
from .model import CHROMATOGRAM_WORDS
from .run_details import TABLES as DETAIL_TABLES
from .version import version_text

FORMATS = ("csv", "xlsx", "pickle")
# UTF-8 with a byte order mark: Excel then reads units such as °C right; pandas skips the mark
CSV_ENCODING = "utf-8-sig"
EXCEL_ROWS = 1048576  # rows per sheet, header included

_BAD = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_RESERVED = {"CON", "PRN", "AUX", "NUL"} | {"COM%d" % i for i in range(1, 10)} | {"LPT%d" % i for i in range(1, 10)}
_PARTIAL = ".partial"


def clean_name(text):
    """`text` as one path part valid on Windows, macOS and Linux ('_' for what is not)."""
    text = _BAD.sub("_", "" if text is None else str(text)).strip().rstrip(". ")
    if not text:
        return "_"
    if text.split(".")[0].upper() in _RESERVED:
        text = "_" + text
    return text


def folder_parts(folder_path):
    """The cleaned parts of a UNICORN folder path ('/DefaultHome/Projects/X')."""
    if folder_path is None or (not isinstance(folder_path, str)):
        return []
    return [clean_name(p) for p in folder_path.split("/") if p.strip()]


def _missing(value):
    import pandas as pd
    return value is None or (not isinstance(value, str) and pd.isna(value))


def _row_id(row):
    """What tells colliding runs apart: ResultID, else a .UFol original id, else the file name."""
    for key in ("result_id", "original_id"):
        if key in row and not _missing(row[key]):
            return str(int(row[key]))
    if "path" in row and not _missing(row["path"]):
        return clean_name(os.path.splitext(os.path.basename(str(row["path"])))[0])
    return None


def run_folders(catalog, labels):
    """
    The folder of each catalog row, relative to the output folder (list of Paths, in row
    order). `labels` gives each row's top folder ('alias#n', 'zip', 'ufol'). Runs whose
    folder would be the same (case ignored, as on Windows), or the same as a UNICORN folder
    of the export, all get `_<id>` (`_row_id`); a counter settles what is left.
    """
    rows = [row for _, row in catalog.iterrows()]
    bases = [[clean_name(label)] + folder_parts(row.get("folder_path")) for row, label in zip(rows, labels)]
    names = [clean_name(row.get("name")) for row in rows]
    folders = {tuple(p.lower() for p in base[:i]) for base in bases for i in range(2, len(base) + 1)}
    keys = [tuple(p.lower() for p in base) + (name.lower(),) for base, name in zip(bases, names)]
    seen = {}
    for key in keys:
        seen[key] = seen.get(key, 0) + 1
    out, taken = [], set()
    for row, base, name, key in zip(rows, bases, names, keys):
        if seen[key] > 1 or key in folders:
            ident = _row_id(row)
            if ident:
                name = "%s_%s" % (name, ident)
        final, n = name, 1
        while tuple(p.lower() for p in base) + (final.lower(),) in taken:
            n += 1
            final = "%s_%d" % (name, n)
        taken.add(tuple(p.lower() for p in base) + (final.lower(),))
        out.append(Path(*base, final))
    return out


def row_stamp(row, content=False):
    """
    What tells a later export that a catalog row's run changed: the result's `tstamp` for a
    database row; for a file row '<size>:<modified ns>', plus ':<sha256>' of the content with
    `content=True` (owner, 2026-11-08: size and time first, the hash only when they differ;
    `match_key` says which run it is, not whether it changed). None when neither is known.
    """
    path = row.get("path")
    if not _missing(path):
        try:
            st = os.stat(str(path))
            stamp = "%d:%d" % (st.st_size, st.st_mtime_ns)
            return stamp + ":" + _sha256(path) if content else stamp
        except OSError:
            return None
    tstamp = row.get("tstamp")
    return None if _missing(tstamp) else str(tstamp)


def loaded_stamp(run):
    """
    The `tstamp` of a database run as loaded (from its `dbo.Result` row), in `row_stamp`'s
    form; None for files or a database without `tstamp`. A run that changed since the search
    is stored with this, not the search CSV's older one.
    """
    from .search import _tstamp
    table = (getattr(run, "tables", None) or {}).get("dbo.Result")
    if run.source != "sql" or table is None or not table.rows:
        return None
    value = table.rows[0].get("tstamp")
    return None if value is None else _tstamp(value)


def _sha256(path):
    h = hashlib.sha256()
    with open(str(path), "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def same_stamp(then, now, row):
    """
    (same, stamp now) for a stored stamp `then` and the row's stamp `now`. Database stamps
    must be equal. File stamps ('size:mtime[:sha256]') are the same when size and modified time
    are; otherwise the content is hashed and compared with a stored hash (the returned stamp
    then holds it). A stored stamp without a hash, from before hashes, can only match on time.
    """
    if ":" not in then or ":" not in now:
        return then == now, now
    t, n = then.split(":"), now.split(":")
    if t[:2] == n[:2]:
        return True, then if len(t) > 2 else now
    if len(t) < 3 or t[0] != n[0]:
        return False, now
    full = row_stamp(row, content=True)
    return full is not None and full.split(":")[2] == t[2], full or now


def stored_stamp(folder):
    """
    (has_stamp, stamp) of an exported run folder, from its `column_and_timeline.csv`
    (`run_info.csv` before 0.21.0): (False, None) for a folder written before stamps were
    recorded (or without a readable file).
    """
    import pandas as pd
    path = long_path(folder) / (file_stem("run_info") + ".csv")
    if not path.exists():
        path = long_path(folder) / "run_info.csv"  # exported by PyCORN before 0.21.0
    if not path.exists():  # an xlsx-only folder (0.32.0): the stamp is on the workbook's own sheet
        for book in sorted(long_path(folder).glob("*.xlsx")):
            try:
                return True, _workbook_stamp(book)
            except (OSError, KeyError, ValueError, zipfile.BadZipFile, ElementTree.ParseError):
                continue
        return False, None
    try:
        frame = pd.read_csv(path, dtype=str, usecols=lambda c: c == "export_stamp", nrows=1)
    except (OSError, ValueError, pd.errors.EmptyDataError):
        return False, None
    if "export_stamp" not in frame.columns:
        return False, None
    value = frame["export_stamp"].iloc[0] if len(frame) else None
    return True, None if _missing(value) else value


def _workbook_stamp(book):
    """
    The `export_stamp` of the 'Column and timeline' sheet of workbook `book` (None when empty), read
    from the .xlsx's XML (an .xlsx is a zip; no openpyxl). KeyError / ValueError when the workbook has
    no such sheet or column (written before stamps were recorded).
    """
    from .export_tables import title
    main = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    rel = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"
    with zipfile.ZipFile(book) as z:
        sheets = ElementTree.fromstring(z.read("xl/workbook.xml")).find(main + "sheets")
        found = next(s.get(rel + "id") for s in sheets if s.get("name") == title("run_info"))
        links = ElementTree.fromstring(z.read("xl/_rels/workbook.xml.rels"))
        target = next(r.get("Target") for r in links if r.get("Id") == found).lstrip("/")
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            shared = ["".join(t.text or "" for t in si.iter(main + "t"))
                      for si in ElementTree.fromstring(z.read("xl/sharedStrings.xml")).iter(main + "si")]
        rows = ElementTree.fromstring(z.read(target if target.startswith("xl/") else "xl/" + target)
                                      ).find(main + "sheetData")

    def text(cell):
        kind = cell.get("t")
        if kind == "inlineStr":
            return "".join(t.text or "" for t in cell.iter(main + "t"))
        v = cell.find(main + "v")
        if v is None or v.text is None:
            return None
        return shared[int(v.text)] if kind == "s" else v.text

    def column(cell):
        return re.match(r"[A-Z]+", cell.get("r")).group(0)

    rows = list(rows)[:2]
    head = {text(c): column(c) for c in rows[0]} if rows else {}
    if "export_stamp" not in head:
        raise ValueError("no export_stamp column")
    cells = {column(c): text(c) for c in rows[1]} if len(rows) > 1 else {}
    value = cells.get(head["export_stamp"])
    return None if _missing(value) else value


def long_path(path):
    """`path` made absolute; on Windows with the \\\\?\\ prefix when it is long."""
    path = os.path.abspath(str(path))
    if os.name == "nt" and len(path) > 240 and not path.startswith("\\\\?\\"):
        path = "\\\\?\\" + path
    return Path(path)


def exists(folder):
    """A run folder that is already there (and not empty)."""
    folder = long_path(folder)
    return folder.is_dir() and any(folder.iterdir())


def excel_engine():
    """The pandas engine for .xlsx (xlsxwriter, else openpyxl); ImportError when neither is there."""
    import importlib.util
    for engine in ("xlsxwriter", "openpyxl"):
        if importlib.util.find_spec(engine):
            return engine
    raise ImportError("Excel export needs xlsxwriter or openpyxl: pip install xlsxwriter")


# ---------------------------------------------------------------------------
# Tables of one chromatogram
# ---------------------------------------------------------------------------

def _header(curve, what, unit):
    return "%s: %s (%s)" % (curve, what, unit) if unit else "%s: %s" % (curve, what)


def _naive_utc(values):
    """Aware or naive UTC times as naive UTC (Excel stores no zone)."""
    import pandas as pd
    times = pd.to_datetime(pd.Series(values, dtype=object), utc=True, errors="coerce")
    return times.dt.tz_localize(None)


def curves_frame(chrom, extra=(), options=None, problems=None):
    """
    Every curve of a chromatogram side by side: a column per axis and the values, NaN-padded.
    `extra`: derived curves (e.g. mg/ml) added after the stored ones. `options`: which curves
    and how many points (`curve_sampling`); a warning line about the thinning goes to `problems`.
    """
    import pandas as pd
    from . import curve_sampling as cs
    options = options or ExportOptions()
    lacking = []
    curves = cs.chosen_curves(chrom, options.curves, options.curve_roles,
                              lacking if options.curves_missing_ok else None)
    if lacking and problems is not None:
        problems.append((chrom.name, "curves", "curve(s) not in this chromatogram, left out of the curves table: %s"
                         % ", ".join(lacking)))
    curves.update({c.name: c for c in extra})
    text = None
    if options.max_points is not None or options.every_n is not None:
        columns, text = cs.thinned(curves, options, _header)
    elif options.grid_step is not None:
        columns, text = cs.on_grid(curves, options, _header)
    else:
        columns = {}
        for name, c in curves.items():
            if c.values is None:
                continue
            for what, values, unit in cs._columns(c):
                if values is not None:
                    columns[_header(name, what, unit)] = pd.Series(values)
    if text and problems is not None:
        problems.append((chrom.name, "sampling", text))
    return pd.concat(columns, axis=1) if columns else pd.DataFrame()


def events_frame(chrom):
    """Every event of a chromatogram, one row each, with its event curve's name and type."""
    import pandas as pd
    frames = []
    for name, ec in chrom.event_curves.items():
        frame = ec.to_frame()
        frame.insert(0, "event_curve", name)
        frame.insert(1, "event_curve_type", ec.type)
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["utc"] = _naive_utc(out["utc"])
    return out


def _peak_units(table):
    units = table.info.get("PeakUnit")
    return units if isinstance(units, dict) else {}


def _zero_shift(chrom, table):
    """What the run's stored axis adds to the table's retentions: the retention zero's volume or time, else None."""
    zero = chrom.retention_zero or {}
    if not table.info.get("ZeroAdjustedToInjectionNumber"):
        return 0.0
    if zero.get("injection_number") != table.info.get("ZeroAdjustedToInjectionNumber"):
        return None
    return {"Volume": zero.get("volume"), "Time": zero.get("time")}.get(table.info.get("CalculationRetention"))


PEAK_COLUMNS_FIRST = ["peak_table", "number", "MaxPeakRetention", "retention_unit", "Area", "area_unit",
                      "PercentOfTotalPeakArea", "Height", "height_unit", "StartPeakVial", "MaxPeakVial", "EndPeakVial",
                      "max_peak_on_run_axis", "retention_zero_injection"]


def peaks_frame(chrom):
    """
    Every peak of a chromatogram's peak tables, with its table's name, its number in the table
    (UNICORN's 'No'), the units, the injection its retentions count from, and the peak maximum
    on the run's stored axis.
    """
    import pandas as pd
    frames = []
    for table in chrom.peak_tables:
        frame = table.to_frame()
        units = _peak_units(table)
        frame.insert(0, "peak_table", table.name)
        frame.insert(1, "number", range(1, len(frame) + 1))
        frame["retention_unit"] = units.get("RetentionUnit")
        frame["area_unit"] = units.get("AreaUnit")
        frame["height_unit"] = units.get("HeightUnit")
        frame["retention_zero_injection"] = table.info.get("ZeroAdjustedToInjectionNumber")
        shift = _zero_shift(chrom, table)
        if "MaxPeakRetention" in frame.columns:
            frame["max_peak_on_run_axis"] = (pd.to_numeric(frame["MaxPeakRetention"], errors="coerce") + shift
                                             if shift is not None else float("nan"))
        # UNICORN's peak table first (No, Retention, Area, % of total peak area, Height, tubes), each
        # unit after its value, then every other stored column in stored order
        first = [c for c in PEAK_COLUMNS_FIRST if c in frame.columns]
        frames.append(frame[first + [c for c in frame.columns if c not in first]])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# (field as UNICORN's report names it, stored key, unit key in PeakUnit) of a peak table's header
_PEAK_FIELDS = [
    ("Calculation made in", "CalculationRetention", None),
    ("Total number of detected peaks", "NumberOfDetectedPeaks", None),
    ("Total area", "TotalPeakArea", "AreaUnit"),
    ("Total peak width", "TotalPeakWidth", "RetentionUnit"),
    ("Area in evaluated peaks", "TotalPeakAreaEvaluatedPeaks", "AreaUnit"),
    ("Ratio peak area / total area", "RatioPeakAreaTotalArea", None),
    ("Asymmetry level", "AssymetryLevel", None),
    ("Calculated from", "DataCurve", None),
    ("Baseline name", "BaseLine", None),
    ("Baseline type", "BaselineType", None),
    ("Separation technique", "TechniqueName", None),
    ("Column height", "ColumnHeight", "ColumnHeightUnit"),
    ("Column volume", "ColumnVolume", "ColumnVolumeUnit"),
    ("Column liquid volume", "ColumnVt", "ColumnVolumeUnit"),
    ("Created", "Created", None),
    ("Peaks rejected if peak is less than the N largest", "MaxNumberOfPeaks", None),
    ("Resolution algorithm", "ResolutionAlgorithm", None),
    ("Resolution algorithm id", "ResolutionAlgorithmID", None),
    ("Resolution algorithm name", None, None),  # readable, below
    ("Retention zero at injection", "ZeroAdjustedToInjectionNumber", None),
]
PEAK_SUMMARY_COLUMNS = ["peak_table", "field", "value", "unit", "stored_as"]
# readable names exactly as UNICORN's report prints them (never guessed: an id or type not seen
# in a report gets no name); the report prints a zero baseline's name as 'Zero Baseline'
RESOLUTION_ALGORITHMS = {3: "1.177 * ((Ret2-Ret1) / (WidthHalfHeight2 + WidthHalfHeight1))"}
BASELINE_NAMES = {"ZeroBaseline": "Zero Baseline"}
UV_NORMALIZED_FIELD = "UV normalized to 1 cm path length"
UV_NORMALIZED_LINE = "UV signal is normalized to 1 cm path length"
PEAK_LOG_MATCH_S = 1.0  # an evaluation-log 'Peak Integration' entry this close to the table's Created made it


def _algorithm_id(info):
    """(id, stored field) of the resolution algorithm: `ResolutionAlgorithmID`, else the number in 'Algorithm3'."""
    stored = info.get("ResolutionAlgorithmID")
    if stored is not None and str(stored).strip().lstrip("-").isdigit():
        return int(str(stored).strip()), "ResolutionAlgorithmID"
    m = re.match(r"^\s*Algorithm\s*(\d+)\s*$", str(info.get("ResolutionAlgorithm") or ""))
    return (int(m.group(1)), "ResolutionAlgorithm") if m else (None, None)


def _peak_integration_entry(run, chrom, table):
    """The evaluation log's 'Peak Integration' entry that made `table` (same chromatogram, logged at its Created)."""
    from .run_details import _utc
    created = _utc(table.info.get("Created"), table.info.get("CreatedUtcOffsetMinutes") or 0)
    best = None
    for trail in (run.evaluation_log or []) if run is not None and created is not None else []:
        for e in (trail.get("AuditTrailEntries") or []) if isinstance(trail, dict) else []:
            text = str(e.get("LogEntry") or "")
            if not text.lstrip().startswith("Peak Integration"):
                continue
            m = re.search(r"Chromatogram name:\s*(.*)", text)
            if m and m.group(1).strip() != chrom.name:
                continue
            when = _utc(e.get("LogTime"), e.get("LogTimeUtcOffsetMinutes") or 0)
            gap = abs((when - created).total_seconds()) if when is not None else None
            if gap is not None and gap <= PEAK_LOG_MATCH_S and (best is None or gap < best[0]):
                best = (gap, e, text)
    return (best[1], best[2]) if best else (None, None)


def peak_uv_normalized(run, chrom, table):
    """
    (normalized, stored_as) for a peak table of a UV curve: whether its heights and areas are
    of the UV signal normalized to 1 cm path length (UNICORN's evaluation setting). From a
    stored field of the table when there is one (none seen so far), else from the
    evaluation log's 'Peak Integration' entry that made it (same chromatogram, logged at the
    table's Created): True when it says 'UV signal is normalized to 1 cm path length', False
    when it does not. (None, None) when neither tells, and for a table of a curve that is not
    UV (the setting is UNICORN's for UV only); ('n/a', None) for the latter.
    """
    from .curve_roles import curve_has_role
    number = (table.info.get("DataCurve") or {}).get("CurveNumber") if isinstance(table.info.get("DataCurve"),
                                                                                  dict) else None
    curve = next((c for c in chrom.curves.values() if number is not None and c.curve_number == number), None)
    if curve is not None and not curve_has_role(curve, "uv"):
        return "n/a", None
    for key, value in table.info.items():
        if re.search(r"normali[sz]", str(key), re.I) and not isinstance(value, (dict, list)) and value is not None:
            return str(value).strip().lower() in ("true", "1", "yes"), key
    entry, text = _peak_integration_entry(run, chrom, table)
    if entry is None:
        return None, None
    return UV_NORMALIZED_LINE.lower() in text.lower(), "evaluation log: Peak Integration at %s" % entry.get("LogTime")


def peak_summary_frame(chrom, run=None, problems=None):
    """
    The header of each peak table, as UNICORN's report prints it under the peaks: one row per
    field (total area, peak width, baseline, column, ...), curve numbers given as curve names,
    the baseline type and resolution algorithm with UNICORN's readable names, whether a UV
    peak table is normalized to 1 cm path length (`peak_uv_normalized`; `run` gives the
    evaluation log, and when it cannot tell, a line goes to `problems`), then every other
    stored field under its stored name.
    """
    import pandas as pd
    from .run_details import local_time_text
    names = {c.curve_number: n for n, c in chrom.curves.items() if c.curve_number is not None}
    rows = []
    for table in chrom.peak_tables:
        info, units = table.info, _peak_units(table)
        done = {"Name", "CurveNumber", "PeakUnit", "CreatedUtcOffsetMinutes"}
        for field, key, unit in _PEAK_FIELDS:
            done.add(key)
            value, stored_as = info.get(key) if key else None, key
            if isinstance(value, dict):  # DataCurve / BaseLine: {'CurveNumber': n}
                number = value.get("CurveNumber")
                value = names.get(number, "curve %s" % number if number is not None else None)
            elif key == "Created":
                value = local_time_text(info, key)
            elif key == "BaseLine" and value is None:  # no baseline curve: UNICORN prints the type's name
                value, stored_as = BASELINE_NAMES.get(info.get("BaselineType")), "BaselineType"
            elif key == "ResolutionAlgorithmID":
                value, stored_as = _algorithm_id(info)
            elif field == "Resolution algorithm name":
                number, stored_as = _algorithm_id(info)
                value = RESOLUTION_ALGORITHMS.get(number)
            if value is not None and value != "":
                rows.append([table.name, field, value, units.get(unit) if unit else None, stored_as])
        normalized, stored_as = peak_uv_normalized(run, chrom, table)
        if normalized in (True, False):
            rows.append([table.name, UV_NORMALIZED_FIELD, normalized, None, stored_as])
            done.add(stored_as)
        elif normalized is None and problems is not None:
            problems.append((chrom.name, "peaks", "peak table %r: whether its heights and areas are of UV normalized "
                                                  "to 1 cm path length is neither stored nor in the evaluation log; "
                                                  "they may be on another scale than the UV curve" % table.name))
        for key, value in info.items():
            if key not in done and value is not None and not isinstance(value, (dict, list)):
                rows.append([table.name, key, value, None, key])
    return pd.DataFrame(rows, columns=PEAK_SUMMARY_COLUMNS)


def _plain(value):
    """`value` as JSON-ready Python: named tuples and dataclasses as dicts, times as ISO text."""
    import numpy as np
    if hasattr(value, "_asdict"):
        value = value._asdict()
    elif dataclasses.is_dataclass(value) and not isinstance(value, type):
        value = dataclasses.asdict(value)
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, np.ndarray)):
        return [_plain(v) for v in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _json(value):
    """JSON text for a list or dict cell; other values as they are."""
    if isinstance(value, (list, tuple, dict)) or hasattr(value, "_asdict"):
        return json.dumps(_plain(value), ensure_ascii=False)
    return value


def _json_columns(frame):
    for column in frame.columns:
        if frame[column].dtype == object:
            frame[column] = [_json(v) for v in frame[column]]
    return frame


def _naive_utc_columns(frame, columns):
    for column in columns:
        if column in frame.columns:
            frame[column] = _naive_utc(frame[column])
    return frame


TABLES = (("phases", "phase"), ("injections", "injection"), ("fractions", "fraction"), ("pools", "pool"))


def other_run_notes(chrom):
    """
    One warning when an imported chromatogram holds curves of another method run
    (`Chromatogram.other_run_curves`): they are written with its curves but have no role, so
    plots, window statistics and CADET leave them out. Empty for every other chromatogram.
    """
    other = chrom.other_run_curves()
    if not other:
        return []
    return ["imported chromatogram: %d curve(s) from another method run (a different stored method start: overlays "
            "its source result had imported): written with its curves, left out of role lookups (plots, window "
            "statistics, CADET): %s" % (len(other), ", ".join(c.name for c in other))]


def event_table_frame(chrom, kind, sections=None, problems=None):
    """
    The 'phase', 'injection' or 'fraction' table of a chromatogram (`chrom.phase_table()` etc.:
    what ran in each row included), UTC without a zone. `sections`: `run_log_sections(run, chrom)`
    when already built; False when building it failed (those columns stay empty). Fractions
    get a `pool` column (the pool each is in, `pycorn.pools`); kind 'pool' is the pooled
    fractions table (`pools.pool_rows`; a pool whose fractions do not match goes to `problems`).
    """
    import pandas as pd
    from . import pools
    if kind == "pool":
        found = []
        frame = _json_columns(pools.to_frame(pools.pool_rows(chrom, found)))
        if problems is not None:
            problems += [(chrom.name, "pools", w) for w in found]
    else:
        from .event_tables import to_frame, with_details
        rows = with_details(chrom, chrom.table_rows(kind), sections)
        frame = to_frame(rows)
        if kind == "fraction":
            frame.insert(int(frame.columns.get_loc("name")) + 1, "pool", pools.fraction_pools(chrom, rows))
    return _naive_utc_columns(frame, ("start_utc", "end_utc")) if len(frame) else pd.DataFrame(columns=frame.columns)


def sections_frame(run, chrom, result=None):
    """
    (frame, warnings): the process sections of `chrom` from its executed run log
    (`pycorn.run_log_sections`; `result` when already built), one row per section, with the
    curve checks (measured flow, largest %B and %Q deviation) as extra columns.
    """
    from .run_log_sections import run_log_sections, to_frame
    result = result if result is not None else run_log_sections(run, chrom)
    frame = to_frame(result)
    if len(frame):
        for column in ("system_flow_measured", "sample_flow_measured", "percent_b_max_deviation",
                       "percent_q_max_deviation"):
            frame[column] = [check.get(column) for check in result["checks"]]
    return _json_columns(frame), list(result["warnings"])


PAUSE_COLUMNS = ["elapsed_s", "start_utc", "end_utc", "duration_s", "source", "quality", "pause_index",
                 "resume_index", "cause"]


def pauses_frame(chrom):
    """The pauses UNICORN's clock stopped for (`chrom.timeline`), UTC without a zone."""
    import pandas as pd
    pauses = (chrom.timeline or {}).get("pauses") or []
    frame = pd.DataFrame([{k: p.get(k) for k in PAUSE_COLUMNS} for p in pauses], columns=PAUSE_COLUMNS)
    return _naive_utc_columns(frame, ("start_utc", "end_utc")) if len(frame) else frame


def run_info_frame(run, geometry, stamp=None, prov=None, chromatograms=None):
    """
    One row per chromatogram written (`chromatograms`: their names; None: every chromatogram):
    what it is (W17: original, imported with its source result, derived), run name and UNICORN version, Vc (value, unit, source, every
    candidate, disagreements), column positions used, column geometry (SI; a cross-check of
    Vc, never Vc), the UTC timeline (quality, start, end, paused time) and the export's
    `stamp` (`export_stamp`, `match_key`; empty when not given), and the PyCORN version and
    commit that wrote it, with the export's route and options (`provenance`, `prov`).
    """
    import pandas as pd
    stamp = stamp or {}
    prov = prov or {}
    rows = []
    described = {d["name"]: d for d in run.describe_chromatograms()}
    for key, chrom in run.chromatograms.items():
        if chromatograms is not None and key not in chromatograms:
            continue
        d = described[key]
        vc = chrom.column_volume_info or {}
        tl = chrom.timeline or {}
        zero = chrom.retention_zero or {}
        check = (geometry.vc_check or {}) if geometry is not None else {}
        g = geometry
        rows.append([
            ("chromatogram", chrom.name), ("chromatogram_kind", d["kind"]),
            ("source_result_id", d["source_result_id"]), ("source_result_name", d["source_result_name"]),
            ("copy_of_run", d["copy_of_run"]),
            ("run_name", run.name), ("unicorn_version", run.unicorn_version),
            ("vc", vc.get("value")), ("vc_unit", vc.get("unit")), ("vc_source", vc.get("source")),
            ("vc_location", vc.get("location")), ("vc_candidates", vc.get("candidates") or []),
            ("vc_disagreements", vc.get("disagreements") or []),
            ("column_positions", (chrom.column_path or {}).get("positions") or []),
            ("column_name", g.name if g else None), ("column_length_m", g.length_m if g else None),
            ("column_diameter_m", g.diameter_m if g else None),
            ("particle_radius_m", g.particle_radius_m if g else None),
            ("geometry_volume_m3", g.volume_m3 if g else None), ("geometry_sources", g.sources if g else {}),
            ("geometry_vs_vc", check.get("relative_difference")), ("column_hints", g.hints if g else {}),
            ("timeline_quality", tl.get("quality")), ("start_utc", tl.get("start_utc")),
            ("start_source", tl.get("start_source")), ("local_utc_offset_min", tl.get("local_utc_offset")),
            ("end_utc", tl.get("end_utc")), ("end_elapsed_s", tl.get("end_elapsed_s")),
            ("total_paused_s", tl.get("total_paused_s")), ("n_pauses", len(tl.get("pauses") or [])),
            ("residual_s", tl.get("residual_s")), ("unplaced_s", tl.get("unplaced_s")),
            ("valid_until_s", tl.get("valid_until_s")),
            ("retention_zero_adjusted", zero.get("adjusted")), ("retention_zero_injection", zero.get("injection_number")),
            ("retention_zero_time", zero.get("time")), ("retention_zero_volume", zero.get("volume")),
            ("retention_zero_source", zero.get("source")),
            ("export_stamp", stamp.get("export_stamp")), ("match_key", stamp.get("match_key")),
            ("pycorn_version", version_text()), ("export_route", prov.get("route")),
            ("export_options", prov.get("options") or {}),
        ])
    frame = pd.DataFrame([[v for _, v in r] for r in rows], columns=[k for k, _ in rows[0]] if rows else None)
    return _json_columns(_naive_utc_columns(frame, ("start_utc", "end_utc")))


WARNING_COLUMNS = ["chromatogram", "source", "category", "message"]


def warnings_frame(run, extra=()):
    """
    Every warning of a run, one row each: `run.warnings` (load and axes; 'chromatogram:
    category: message' split up) and `extra` (chromatogram, source, message) triples from
    the export (sections, geometry, tables not built).
    """
    import pandas as pd
    from .axes import CATEGORIES
    names = set(run.chromatograms) | {c.name for c in run.chromatograms.values()}
    rows = []
    for text in run.warnings:
        chrom, category, message = "", "", text
        head, sep, rest = text.partition(": ")
        if sep and head in names:
            chrom, message = head, rest
            cat, sep, rest = rest.partition(": ")
            if sep and cat in CATEGORIES:
                category, message = cat, rest
        rows.append((chrom, "run", category, message))
    rows += [(chrom, source, "", message) for chrom, source, message in extra]
    return pd.DataFrame(rows, columns=WARNING_COLUMNS)


def details_frames(run, problems):
    """
    [(name, frame)] of the run-level tables of `pycorn.run_details` (method, calibration,
    instrument, system settings, audit log); one that cannot be built is left out with a line
    in `problems`.
    """
    from .run_details import BUILDERS
    out = []
    for name, build in BUILDERS.items():
        frame = _built(name, "", lambda build=build: _naive_utc_columns(build(run), ("date_utc", "time_utc")),
                       problems)
        if frame is not None:
            out.append((name, frame))
    return out


def _built(what, chrom_name, build, problems):
    """`build()`, or None with a line in `problems` when it raises (the run is still written)."""
    try:
        return build()
    except Exception as e:
        problems.append((chrom_name, "export", "%s not written (%s: %s)" % (what, type(e).__name__, e)))
        return None


@dataclasses.dataclass
class ExportOptions:
    """
    The opt-in outputs of `write_run` (the `pycorn export` flags). Quantities are (value, unit);
    `epsilon` / `epsilon_molar` map a wavelength in nm to (value, unit).
    """
    units: dict = None  # {quantity: unit}, for run.normalized()
    method_hints: bool = False
    # people, computer, instrument names and serials as User1, Computer1, System1, Serial1 (`run_details.Masker`)
    mask_identifying: bool = False
    path_length_cm: float = None  # UV path where the run gives none
    epsilon: dict = None  # {nm: (value, unit)}, mass coefficient
    epsilon_molar: dict = None  # {nm: (value, unit)}, with molecular_weight
    molecular_weight: tuple = None
    uv_response: float = None  # detector response factor on epsilon (issue #1), caller only
    bypass_uv: bool = False  # issue #1: UV of sample sent to the column by-pass (`_by_pass_uv` table)
    sample_concentration: tuple = None  # (value, unit) from an offline assay: by-pass epsilon
    loop_volume: tuple = None  # (value, unit) of the fixed loop: by-pass pulse epsilon
    absorbance_limit: tuple = None  # (value, unit): no by-pass epsilon and NaN mg/ml above it
    # M22: curve statistics over windows relative to a phase (`pycorn.window_stats`); no default window or curve
    windows: list = None  # 'NAME=PHASE:FROM:TO UNIT' texts or `window_stats.Window` objects
    window_curves: list = None  # curve names or roles (`curve_roles.ROLES`) for the windows
    bed_height: tuple = None
    diameter: tuple = None
    particle_diameter: tuple = None
    override_geometry: bool = False
    cadet: bool = False
    cadet_core: str = None  # '5' or '6'
    core_format: str = "json"  # or 'hdf5' (needs h5py)
    mixer_volume: tuple = None  # D27: over the stored mixer size
    tubing_volume: tuple = None  # D27: column -> UV tubing (the run stores none)
    sample_inlets: list = None  # D27: inlet positions ('A5') holding sample on a system pump line
    system_volumes: bool = True  # False: no mixer / tubing CSTRs (Core input, experiment.json units)
    # one generic parameter block per target (D28; owner, 2026-11-08: no assumptions about its content):
    # merged into the Core input unchanged / stored unchanged in experiment.json (--core-overrides,
    # --process-overrides: JSON files)
    core_overrides: dict = None
    process_overrides: dict = None
    # smaller exports (owner, 2026-11-09): all off unless asked (D28); CADET files and the stored run unchanged
    tables: list = None  # keys of the tables to write (`export_tables.TABLES`); None: all
    curves: list = None  # curve names for the curves table (with curve_roles: both)
    curve_roles: list = None  # 'uv', 'conductivity', ... (`curve_roles.ROLES`)
    curves_missing_ok: bool = False  # the page's ticked names, for runs that lack some: skip them with a warning
    max_points: int = None  # each curve thinned on its own axis: at most about this many points ...
    method: str = None  # ... by 'interval', 'derivative', 'minmax' or 'hybrid' (`curve_sampling`)
    every_n: int = None  # instead: every Nth stored point of each curve
    derivative_share: float = None  # 0..1: derivative-spaced share for interval/derivative (1 = all)
    grid_step: float = None  # all curves on one even grid, in the axis' unit (instead of max_points)
    grid_axis: str = None  # 'time' (default), 'volume' or 'cv'
    # W17 (owner, 2026-11-11): which chromatograms are written: None, the run's original one(s); else
    # names and / or 'all', 'original', 'imported', 'derived' (`Run.select_chromatograms`); a name a run
    # lacks is a line in warnings.csv
    chromatograms: list = None
    # provenance only (owner, 2026-11-09): how the export was asked for, recorded with every export
    route: str = None  # 'CLI: pycorn export', 'web page', ...; None: a Python call of write_run
    command: str = None  # the command line, when there is one

    def to_dict(self):
        """
        The options as one JSON-ready dict (Track 9, A5): every option by its field name, in field
        order, defaults included; quantities as [value, "unit"], `epsilon` / `epsilon_molar` as
        {"<nm>": [value, "unit"]}, windows as their text. `route` and `command` (provenance only:
        how an export was asked for, not what it writes) are left out. `from_dict` reads it back;
        `pycorn export --options FILE.json` and the page's options.json use this form.
        """
        from collections import OrderedDict
        out = OrderedDict()
        for f in dataclasses.fields(self):
            if f.name in PROVENANCE_ONLY:
                continue
            value = getattr(self, f.name)
            if value is not None and f.name in PER_WAVELENGTH:
                value = OrderedDict(("%g" % float(nm), [q[0], q[1]]) for nm, q in value.items())
            elif value is not None and f.name == "windows":
                value = [w if isinstance(w, str) else w.text() for w in value]
            elif value is not None and f.type is tuple:
                value = [value[0], value[1]]
            out[f.name] = _plain(value)
        return out

    @classmethod
    def from_dict(cls, d):
        """
        `ExportOptions` from `to_dict`'s form (a partial dict too: missing keys and null values
        keep the defaults; the "options" of a provenance.json read the same). ValueError naming
        the key for an unknown key or a value of the wrong kind; then `check_options`.
        """
        if not isinstance(d, dict):
            raise ValueError("options: give one JSON object ({...}), not %s" % type(d).__name__)
        fields = {f.name: f for f in dataclasses.fields(cls)}
        unknown = [k for k in d if k not in fields]
        if unknown:
            raise ValueError("options: unknown key(s) %s; the keys are: %s" % (
                ", ".join(repr(k) for k in unknown), ", ".join(k for k in fields if k not in PROVENANCE_ONLY)))
        kwargs = {}
        for key, value in d.items():
            if value is not None:
                kwargs[key] = _option_value(key, fields[key].type, value)
        options = cls(**kwargs)
        check_options(options)
        return options


PROVENANCE_ONLY = ("route", "command")
PER_WAVELENGTH = ("epsilon", "epsilon_molar")
# the unit each quantity option converts to: a unit that does not is an error naming the key
QUANTITY_TARGETS = {"molecular_weight": "g/mol", "sample_concentration": "mg/ml", "loop_volume": "ml",
                    "absorbance_limit": "mAU", "bed_height": "m", "diameter": "m", "particle_diameter": "m",
                    "mixer_volume": "ml", "tubing_volume": "ml", "epsilon": "ml/(mg*cm)",
                    "epsilon_molar": "1/(M*cm)"}


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _quantity(key, value):
    """[value, "unit"] -> (value, unit), the unit convertible to `QUANTITY_TARGETS[key]`; ValueError naming `key`."""
    from .units import UnitError, conversion_factor
    if not (isinstance(value, (list, tuple)) and len(value) == 2 and _number(value[0])
            and isinstance(value[1], str) and value[1].strip()):
        raise ValueError("options: %s must be [value, \"unit\"], e.g. [10, \"%s\"]; got %r"
                         % (key, QUANTITY_TARGETS[key], value))
    if not value[0] > 0:
        raise ValueError("options: %s must be above 0, got %r" % (key, value[0]))
    try:
        conversion_factor(value[1], QUANTITY_TARGETS[key])
    except UnitError as e:
        raise ValueError("options: %s: %s" % (key, e)) from None
    return value[0], value[1]


def _option_value(key, kind, value):
    """One `to_dict` value back as the `ExportOptions` field `key` (of type `kind`) holds it."""
    def wrong(what):
        return ValueError("options: %s must be %s, got %r" % (key, what, value))

    if key in PER_WAVELENGTH:
        if not isinstance(value, dict):
            raise wrong('{"<nm>": [value, "unit"]}')
        out = {}
        for nm, q in value.items():
            try:
                wavelength = float(nm)
            except (TypeError, ValueError):
                wavelength = None
            if wavelength is None or not wavelength > 0:
                raise ValueError("options: %s: %r is not a wavelength in nm" % (key, nm))
            out[wavelength] = _quantity(key, q)
        return out
    if kind is tuple:
        return _quantity(key, value)
    if kind is bool:
        if not isinstance(value, bool):
            raise wrong("true or false")
        return value
    if kind is float:
        if not _number(value):
            raise wrong("a number")
        return value
    if kind is int:
        if not (_number(value) and float(value).is_integer()):
            raise wrong("a whole number")
        return int(value)
    if kind is str:
        if key == "cadet_core" and _number(value) and float(value).is_integer():
            value = "%d" % value
        if not isinstance(value, str):
            raise wrong("text")
        return value
    if kind is list:
        if key == "chromatograms" and isinstance(value, str):
            return value  # one word or name, as `Run.select_chromatograms` takes it
        if not isinstance(value, (list, tuple)) or not all(isinstance(v, str) for v in value):
            raise wrong("a list of texts")
        return list(value)
    if kind is dict:
        if not isinstance(value, dict):
            raise wrong("a JSON object")
        if key == "units":
            from .units import check_units
            if not all(isinstance(k, str) and isinstance(v, str) for k, v in value.items()):
                raise wrong('{"quantity": "unit"}')
            try:
                return check_units(dict(value))
            except ValueError as e:
                raise ValueError("options: units: %s" % e) from None
        return dict(value)
    return value


def provenance(options, formats=None):
    """
    What reproduces an export (owner, 2026-11-09): app, version and git commit, repository,
    route and command, formats, and every option that differs from this version's defaults
    (JSON-ready). Recorded in README.txt, provenance.json, the Excel summary, the
    column-and-timeline table and the CADET files' PyCORN metadata.
    """
    from collections import OrderedDict
    from .export_tables import REPOSITORY
    from .version import __version__, commit
    options = options or ExportOptions()
    defaults = ExportOptions()
    changed = OrderedDict((f.name, _plain(getattr(options, f.name))) for f in dataclasses.fields(options)
                          if f.name not in PROVENANCE_ONLY and getattr(options, f.name) != getattr(defaults, f.name))
    return OrderedDict([
        ("app", "PyCORN"), ("version", __version__), ("commit", commit()), ("repository", REPOSITORY),
        ("route", options.route or "Python: pycorn.run_export.write_run"), ("command", options.command),
        ("formats", list(formats) if formats else None), ("options", changed),
        ("exported_utc", dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
    ])


PATH_BASIS = "fallback (--path-length)"
GEOMETRY_FLAGS = (("length", "bed_height", "--bed-height"), ("diameter", "diameter", "--diameter"),
                  ("particle_diameter", "particle_diameter", "--particle-diameter"))
PRIMARY_NM = 280.0  # build_experiment's primary wavelength


def _geometry_kwargs(options):
    """column_geometry()'s caller values and labels from `options` (D26)."""
    return dict(bed_height=options.bed_height, diameter=options.diameter,
                particle_diameter=options.particle_diameter, override=options.override_geometry,
                fallback_sources={what: "fallback (%s)" % flag for what, _, flag in GEOMETRY_FLAGS})


def _given_geometry(options):
    return any(getattr(options, attr) is not None for _, attr, _ in GEOMETRY_FLAGS)


def _geometry_vc(run, options, geometry):
    """
    D26 point 4: when a chromatogram has no Vc and the caller's geometry gives a volume, run
    `add_axes` again with it as the last-resort Vc (source 'geometry'). True when it did.
    """
    if geometry is None or geometry.volume_m3 is None:
        return False
    caller = {w: s for w, s in geometry.sources.items() if w in ("length", "diameter") and s != "ColumnTypeData"}
    if not caller or all(c.column_volume_info and c.column_volume_info.get("value") is not None
                         for c in run.chromatograms.values()):
        return False
    from .axes import add_axes
    given = {"length": options.bed_height, "diameter": options.diameter}
    location = ", ".join("%s %g %s" % (flag, given[w][0], given[w][1]) if w in caller else "stored %s" % w
                         for w, _, flag in GEOMETRY_FLAGS[:2])
    add_axes(run, geometry_volume=(geometry.volume_m3 * 1e6, "ml", location))  # it warns (Vc) itself
    return True


def uv_wavelengths(run, chrom, sections_result=None):
    """
    ([(curve, channel, nm, nm source)], warnings) for the live UV curves of `chrom`, by the
    CADET export's rule (D8, `pycorn.exporters.cadet.uv_wavelengths`): the run log's Wavelength
    instruction per channel, the curve-name suffix only as a fallback (warned); 0 nm channels
    are left out. Owner, 2026-11-08: the same rule as `build_experiment`.
    """
    from .exporters.cadet import uv_wavelengths as d8
    from .run_log_sections import run_log_sections
    rls = sections_result if sections_result is not None else run_log_sections(run, chrom)
    return d8(chrom, rls["sections"])


def _primary_nm(channels):
    """build_experiment's primary wavelength among `channels` (280 nm, else the lowest channel)."""
    if not channels:
        return None
    if any(_same_nm(nm, PRIMARY_NM) for _, _, nm, _ in channels):
        return PRIMARY_NM
    return min(channels, key=lambda c: c[1])[2]


def _same_nm(a, b):
    return a is not None and b is not None and abs(float(a) - float(b)) < 1e-6


CONCENTRATION_COLUMNS = ["curve", "mg_ml_curve", "wavelength_nm", "wavelength_source", "epsilon_ml_per_mg_cm",
                         "epsilon", "response_factor", "path_length_cm", "path_length_basis"]


def concentration_curves(run, chrom, options, problems, sections_result=None):
    """
    (mg/ml curves, `_concentration` frame) for the UV curves of `chrom` at a wavelength the
    caller gave an extinction coefficient for. Notes go to `problems`.
    """
    import pandas as pd
    from .concentration import concentration
    wanted = sorted(set(options.epsilon or {}) | set(options.epsilon_molar or {}))
    if not wanted:
        return [], None
    channels, notes = uv_wavelengths(run, chrom, sections_result)
    problems += [(chrom.name, "concentration", w) for w in notes]
    curves, rows = [], []
    for nm in wanted:
        hits = [c for c in channels if _same_nm(c[2], nm)]
        if not hits:
            problems.append((chrom.name, "concentration", "no UV curve at %g nm (UV channels: %s); no mg/ml for it"
                             % (nm, ", ".join("%s %s nm" % (c[0].name, c[2]) for c in channels) or "none")))
            continue
        for curve, _, wl, wl_source in hits:
            kwargs = {}
            if nm in (options.epsilon or {}):
                kwargs.update(epsilon=options.epsilon[nm][0], epsilon_unit=options.epsilon[nm][1])
            else:
                kwargs.update(epsilon_molar=options.epsilon_molar[nm][0], epsilon_molar_unit=options.epsilon_molar[nm][1],
                              molecular_weight=options.molecular_weight[0],
                              molecular_weight_unit=options.molecular_weight[1])
            try:
                if options.absorbance_limit is not None:
                    kwargs.update(absorbance_limit=options.absorbance_limit[0],
                                  absorbance_limit_unit=options.absorbance_limit[1])
                mg = concentration(curve, fallback_path_cm=options.path_length_cm, fallback_basis=PATH_BASIS,
                                   response_factor=options.uv_response, **kwargs)
            except Exception as e:
                problems.append((chrom.name, "concentration", "%s: no mg/ml (%s)" % (curve.name, e)))
                continue
            conv = mg.conversion
            problems.append((chrom.name, "concentration", "%s: %s" % (mg.name, conv["note"])))
            if conv["path_length_basis"] == PATH_BASIS:
                problems.append((chrom.name, "concentration", "%s: no UV path length in the run; %g cm used (%s)"
                                 % (curve.name, conv["path_length_cm"], PATH_BASIS)))
            curves.append(mg)
            rows.append([curve.name, mg.name, wl, wl_source, conv["epsilon_ml_per_mg_cm"], _json(conv["epsilon"]),
                         conv["response_factor"], conv["path_length_cm"], conv["path_length_basis"]])
    return curves, pd.DataFrame(rows, columns=CONCENTRATION_COLUMNS)


def bypass_uv_frame(run, chrom, options, problems, sections_result=None):
    """
    `_bypass_uv` frame of `chrom` (`pycorn.uv_calibration.bypass_uv_readings`, issue #1): one row per
    sample-to-by-pass window and UV channel; None when the run sent no sample to the by-pass.
    Row warnings go to `problems`, with the accuracy note once.
    """
    import pandas as pd
    from .uv_calibration import READING_COLUMNS, bypass_uv_readings
    kwargs = {}
    for name, given in (("sample_concentration", options.sample_concentration),
                        ("loop_volume", options.loop_volume), ("absorbance_limit", options.absorbance_limit)):
        if given is not None:
            kwargs.update({name: given[0], name + "_unit": given[1]})
    rows = bypass_uv_readings(run, chrom, sections=sections_result["sections"] if sections_result else None, **kwargs)
    if not rows:
        problems.append((chrom.name, "bypass_uv", "no sample sent to the column by-pass in the run log"))
        return None
    for r in rows:
        problems += [(chrom.name, "bypass_uv", "window %d, %s: %s" % (r["window"], r["curve"], w))
                     for w in r["warnings"]]
    problems.append((chrom.name, "bypass_uv", rows[0]["note"]))
    frame = pd.DataFrame([[_json(r[c]) if c == "warnings" else r[c] for c in READING_COLUMNS if c != "note"]
                          for r in rows], columns=[c for c in READING_COLUMNS if c != "note"])
    return frame


def phase_windows_frame(run, chrom, options, problems):
    """
    `_phase_window_statistics` frame of `chrom` (M22, `pycorn.window_stats.window_stats_rows`):
    one row per phase occurrence x window x curve; None when no row was made. Missing phases and
    curves, and each window's clipping, go to `problems` (once per window, not per curve).
    """
    from .window_stats import INTERIM_RULE, to_frame, window_stats_rows
    rows, notes = window_stats_rows(run, chrom, options.windows, options.window_curves)
    problems += [(chrom.name, "phase_windows", w) for w in notes]
    seen = set()
    for r in rows:
        for w in r["warnings"]:
            text = "window %s, %s #%d: %s" % (r["window"], r["phase"], r["occurrence"], w)
            if text not in seen:
                seen.add(text)
                problems.append((chrom.name, "phase_windows", text))
    if not rows:
        return None
    problems.append((chrom.name, "phase_windows", INTERIM_RULE))
    frame = to_frame(rows)
    frame["warnings"] = [_json(w) for w in frame["warnings"]]
    return frame


def hint_frames(run, chrom, sections, problems):
    """(blocks, injections) frames of `pycorn.method_hints` (planned vs executed; hints only, D17)."""
    import pandas as pd
    from .method_hints import method_hints
    hints = method_hints(run, chrom, sections=sections)
    problems += [(chrom.name, "method_hints", w) for w in hints.warnings]
    frames = []
    for rows in (hints.blocks, hints.injections):
        frame = pd.DataFrame([_plain(r) for r in rows])
        frames.append(_json_columns(frame))
    return frames


def cadet_outputs(run, chrom, options, folder, stem, problems, sections_result=None):
    """
    Build the CADET experiment of `chrom` (from the stored run, time zero at the method start)
    and write what `options` asks for into `folder`: `<stem>_cadet/` (CADET-Process CSVs and
    experiment.json) and `<stem>_core_v<n>.json|.h5` (CADET-Core input). Returns the names written.
    `sections_result`: `run_log_sections(run, chrom)` when already built (saves building it again).
    Whenever files are written, `problems` gets `CADET_EXPERIMENTAL_NOTE` once (W15).
    """
    from .exporters.cadet.api import CADET_EXPERIMENTAL_NOTE
    written = _cadet_outputs(run, chrom, options, folder, stem, problems, sections_result)
    if written and not any(m == CADET_EXPERIMENTAL_NOTE for _, _, m in problems):
        problems.append(("", "cadet", CADET_EXPERIMENTAL_NOTE))
    return written


def _cadet_outputs(run, chrom, options, folder, stem, problems, sections_result=None):
    from .exporters.cadet import build_experiment, to_core_input, to_csv, write_hdf5, write_json
    kwargs = {}
    eps = dict(options.epsilon or {})
    molar = dict(options.epsilon_molar or {})
    if eps or molar:
        channels, _ = uv_wavelengths(run, chrom, sections_result)
        nm = _primary_nm(channels)
        key = next((k for k in list(eps) + list(molar) if _same_nm(k, nm)), None)
        if key is None:
            problems.append((chrom.name, "cadet", "no --epsilon (epsilon=) for the primary UV wavelength (%s nm): "
                                                  "the CADET experiment has no concentration" % nm))
        elif key in eps:
            kwargs.update(epsilon=eps[key][0], epsilon_unit=eps[key][1], epsilon_wavelength_nm=nm)
            # D29: CADET works in mol/m^3; a mass coefficient gives it only with the molecular weight
            if options.molecular_weight is not None:
                kwargs.update(molecular_weight=options.molecular_weight[0],
                              molecular_weight_unit=options.molecular_weight[1])
            else:
                problems.append((chrom.name, "cadet", "--epsilon without --mw (molecular_weight=): no molecular "
                                                      "weight, so no UV concentration in mol/m^3 (CADET works in "
                                                      "mol/m^3)"))
        else:
            kwargs.update(epsilon_molar=molar[key][0], epsilon_molar_unit=molar[key][1],
                          molecular_weight=options.molecular_weight[0],
                          molecular_weight_unit=options.molecular_weight[1], epsilon_wavelength_nm=nm)
        if key is not None and options.uv_response is not None:
            kwargs.update(response_factor=options.uv_response)
    if options.absorbance_limit is not None:
        from .units import conversion_factor
        kwargs.update(absorbance_limit_mau=options.absorbance_limit[0]
                      * conversion_factor(options.absorbance_limit[1], "mAU"))
    geometry = _geometry_kwargs(options)
    exp = build_experiment(run, chrom, time_zero="method_start", uv_path_length_fallback_cm=options.path_length_cm,
                           uv_path_length_fallback_basis=PATH_BASIS, bed_height=geometry["bed_height"],
                           diameter=geometry["diameter"], particle_diameter=geometry["particle_diameter"],
                           geometry_override=geometry["override"],
                           geometry_fallback_sources=geometry["fallback_sources"],
                           mixer_volume=options.mixer_volume, tubing_volume=options.tubing_volume,
                           sample_inlets=options.sample_inlets, **kwargs)
    problems += [(chrom.name, "cadet", w) for w in exp.warnings]
    # PyCORN's own metadata (experiment.json 'source', Core 'meta'), never CADET's names
    prov = provenance(options)
    exp.source = dict(exp.source or {}, export=prov)
    written = []
    if options.cadet:
        to_csv(exp, str(folder / (stem + "_cadet")), system_volumes=options.system_volumes,
               process_overrides=options.process_overrides)
        written.append(stem + "_cadet/")
    if options.cadet_core:
        try:
            core = to_core_input(exp, core_version=options.cadet_core, placeholders=True,
                                 system_volumes=options.system_volumes, overrides=options.core_overrides)
        except ValueError as e:
            problems.append((chrom.name, "cadet_core", "CADET-Core input not written: %s (give the column with "
                                                       "--bed-height and --diameter; in Python bed_height=(value, "
                                                       "unit), diameter=(value, unit))" % e))
            return written
        problems += [(chrom.name, "cadet_core", w) for w in core["meta"].get("WARNINGS") or []]
        core["meta"]["EXPORT"] = json.dumps(prov, ensure_ascii=False)  # JSON text: HDF5 holds it as one string
        hdf5 = options.core_format == "hdf5"
        name = "%s_core_v%s.%s" % (stem, options.cadet_core, "h5" if hdf5 else "json")
        (write_hdf5 if hdf5 else write_json)(core, str(folder / name))
        written.append(name)
    return written


RUN_TABLES = ("variables",) + DETAIL_TABLES + ("run_info", "warnings", "column_guide")
README = "README.txt"
RUN_FILES = tuple(file_stem(k) for k in RUN_TABLES) + ("readme", "provenance")  # run-level file stems


def _file_stems(run):
    """
    A cleaned, unique file stem per chromatogram name, over every chromatogram of the run in
    stored order, so a chromatogram's stem is the same whichever chromatograms are written (W17).
    """
    stems, taken = {}, set()
    for name in run.chromatograms:
        stem, n = clean_name(name), 1
        while stem.lower() in taken or stem.lower() in RUN_FILES:
            n += 1
            stem = "%s_%d" % (clean_name(name), n)
        taken.add(stem.lower())
        stems[name] = stem
    return stems


def chromatogram_stems(run, which=None, problems=None):
    """
    {name: file stem} of the chromatograms `which` selects (W17, `Run.select_chromatograms`:
    None, the default, the original chromatogram(s); names or 'all' / 'original' / 'imported' /
    'derived'), in stored order; stems from `_file_stems`. With `problems` (a list of
    (chromatogram, source, message)): an item that matches no chromatogram of this run is a line
    there (else KeyError), and so are the chromatograms the default leaves out, with how to pick them.
    """
    names, unmatched = run.chromatogram_selection(which)
    if unmatched:
        if problems is None:
            run.select_chromatograms(which)  # raises, listing the run's chromatograms
        listed = ", ".join("%s (%s)" % (d["name"], d["label"]) for d in run.describe_chromatograms())
        problems.append(("", "chromatograms", "no chromatogram %s in this run (it has %s)"
                         % (", ".join(repr(u) for u in unmatched), listed or "none")))
    if which is None and problems is not None:
        left = [d for d in run.describe_chromatograms() if d["name"] not in names]
        if left:
            problems.append(("", "chromatograms", "%d other chromatogram(s) not written: %s; pick them with "
                                                  "--chromatogram NAME|all|imported|derived (Python: "
                                                  "chromatograms= / export_cadet(chromatogram=))"
                             % (len(left), ", ".join("%s (%s)" % (d["name"], d["label"]) for d in left))))
    stems = _file_stems(run)
    return {name: stems[name] for name in names}


def missing_outputs(run, options, written):
    """
    [(chromatogram, what)] for the outputs `options` asked for that `written` (from
    `write_run`) doesn't hold: CADET files (e.g. no geometry for the Core input or no run log
    to build from), and mg/ml curves for an --epsilon wavelength no UV curve of the run has
    (when csv or xlsx tables were written); `warnings.csv` says why. Only the chromatograms
    `options.chromatograms` selects (W17; default: the original ones).
    """
    options = options or ExportOptions()
    done = set(written)
    missing = []
    wanted = sorted(set(options.epsilon or {}) | set(options.epsilon_molar or {}))
    tables = any(str(f).lower().endswith((".csv", ".xlsx")) and os.path.splitext(str(f))[0].lower() not in RUN_FILES
                 for f in written)
    for name, stem in chromatogram_stems(run, options.chromatograms, []).items():
        if wanted and tables:
            try:
                channels = uv_wavelengths(run, run.chromatograms[name])[0]
            except Exception:  # no sections: no mg/ml was made at all (warnings.csv says why)
                channels = []
            missing += [(name, "mg/ml at %g nm" % nm) for nm in wanted
                        if not any(_same_nm(c[2], nm) for c in channels)]
        if options.cadet and stem + "_cadet/" not in done:
            missing.append((name, "CADET-Process files"))
        if options.cadet_core:
            ext = "h5" if options.core_format == "hdf5" else "json"
            if "%s_core_v%s.%s" % (stem, options.cadet_core, ext) not in done:
                missing.append((name, "CADET-Core %s input" % options.cadet_core))
    return missing


# the tables --tables can pick (warnings, the column guide, README and provenance are always written)
SELECTABLE_TABLES = (("curves", "events", "peaks", "peak_summary") + tuple(sheet for sheet, _ in TABLES)
                     + ("sections", "pauses", "concentration", "bypass_uv", "phase_windows", "hint_blocks",
                        "hint_injections")
                     + tuple(t for t in RUN_TABLES if t not in ("warnings", "column_guide")))


# W16 (owner, 2026-11-10): the tables by the page's groups, for the page's Download panel and
# `--tables <groups>`; a sixth group, the run log, for the tables the page does not show (owner,
# 2026-11-11). Column and timeline, warnings, the column guide, README and provenance are always
# written; which curves: --curves (D28).
TABLE_GROUPS = (("key", ("phases", "injections", "fractions", "pools")),
                ("peaks", ("peaks", "peak_summary")),
                ("method", ("hint_blocks", "hint_injections", "variables", "method_steps", "scouting_runs",
                            "start_questions")),
                ("details", ("run_details", "column", "calibration", "instrument", "system_settings")),
                ("audit", ("audit_log",)),
                ("runlog", ("events", "sections", "pauses")))
GROUP_NAMES = tuple(g for g, _ in TABLE_GROUPS)
ALWAYS_TABLES = ("run_info", "warnings", "column_guide")
# a group name keeps these: the curves (which ones: --curves) and the tables of their own flags
WITH_GROUPS = ("curves", "concentration", "bypass_uv", "phase_windows")


def wanted_tables(names):
    """
    The table keys `--tables` names (table keys and `TABLE_GROUPS` names, a group being all its
    tables; 'peaks' is the group), with `ALWAYS_TABLES`, and `WITH_GROUPS` when a group is named;
    None (every table) for None.
    """
    if names is None:
        return None
    groups = dict(TABLE_GROUPS)
    wanted = set(ALWAYS_TABLES)
    for name in names:
        wanted.update(groups.get(name, (name,)))
    if any(name in groups for name in names):
        wanted.update(WITH_GROUPS)
    return wanted


def check_options(options):
    """ValueError for a table or group name that does not exist, or size choices that contradict each other."""
    from .curve_sampling import check_sampling
    unknown = [t for t in options.tables or [] if t not in SELECTABLE_TABLES and t not in GROUP_NAMES]
    if unknown:
        raise ValueError("unknown table(s) %s; the groups are: %s; the tables are: %s"
                         % (", ".join(unknown), ", ".join(GROUP_NAMES), ", ".join(SELECTABLE_TABLES)))
    check_sampling(options)
    if options.chromatograms is not None:
        if not isinstance(options.chromatograms, str) and not list(options.chromatograms):
            raise ValueError("chromatograms= is empty: give names or words (%s), or None for the original "
                             "chromatogram(s)" % ", ".join(CHROMATOGRAM_WORDS))
    if options.windows or options.window_curves:
        from .window_stats import SYNTAX, parse_windows
        if not options.windows:
            raise ValueError("--window-curves (window_curves=) needs --window (windows=): %s" % SYNTAX)
        if not options.window_curves:
            raise ValueError("--window (windows=) needs --window-curves (window_curves=): curve names or roles, "
                             "e.g. conductivity,ph (no default curves)")
        parse_windows(options.windows)  # ValueError for a window that does not parse


def write_run(run, folder, formats=("csv",), overwrite=False, options=None, stamp=None):
    """
    Write `run` into `folder` (one file per chromatogram per format). Written into
    '<folder>.partial' first and renamed when done, so an interrupted export leaves no
    half-written run folder (a failed one leaves none at all). Returns the file names written;
    an existing folder raises FileExistsError unless `overwrite` (then it is replaced).
    `options`: the opt-in outputs (`ExportOptions`). `stamp`: {'export_stamp': ..., 'match_key':
    ...} for `column_and_timeline.csv`, what a later export compares to see the run changed
    (`row_stamp`). README.txt, provenance.json and the run-level tables are written in every format.
    Vc from the caller's geometry (D26) recomputes the axes of `run` itself.
    """
    options = options or ExportOptions()
    formats = [f.lower() for f in formats]
    unknown = [f for f in formats if f not in FORMATS]
    if unknown:
        raise ValueError("unknown format(s) %s; known: %s" % (", ".join(unknown), ", ".join(FORMATS)))
    check_options(options)
    folder = long_path(folder)
    if exists(folder) and not overwrite:
        raise FileExistsError(str(folder))
    partial = folder.with_name(folder.name + _PARTIAL)
    if partial.exists():
        shutil.rmtree(partial)
    partial.mkdir(parents=True)
    try:
        written = _write_into(run, partial, formats, options, stamp)
    except BaseException:
        shutil.rmtree(partial, ignore_errors=True)
        raise
    if folder.exists():
        shutil.rmtree(folder)
    partial.rename(folder)
    return written


PROVENANCE_FILE = "provenance.json"


def only_workbook(formats):
    """True when `formats` hold xlsx but not csv: the folder is the workbook(s) and README.txt (W4, 0.32.0)."""
    return "xlsx" in formats and "csv" not in formats


def _write_into(run, partial, formats, options, stamp):
    """`write_run`'s files, written into the (new, empty) folder `partial`."""
    from .excel_export import _source
    from .export_tables import readme_text
    prov = provenance(options, formats)
    options = dataclasses.replace(options, route=prov["route"])  # CADET metadata records the same
    written = []
    described = []  # (file, table key or format, chromatogram) for README.txt
    tables = "csv" in formats or "xlsx" in formats
    problems = []  # (chromatogram, source, message) for warnings.csv
    if not tables:
        for flag, given, what in (("--epsilon", options.epsilon or options.epsilon_molar, "mg/ml curves"),
                                  ("--method-hints", options.method_hints, "method hints"),
                                  ("--bypass-uv", options.bypass_uv, "by-pass UV readings"),
                                  ("--window", options.windows, "phase window statistics")):
            if given:
                problems.append(("", "export", "%s not used: only csv and xlsx hold %s" % (flag, what)))
    geometry = None
    if run.chromatograms:
        from .column_geometry import column_geometry
        geometry = _built("column geometry", "", lambda: column_geometry(run, **_geometry_kwargs(options)), problems)
        if _given_geometry(options) and _geometry_vc(run, options, geometry):
            geometry = column_geometry(run, **_geometry_kwargs(options))  # notes the Vc it gave
        if geometry is not None:
            # without a column type and caller values, 'no column length' etc. only repeat that
            problems += [("", "geometry", w) for w in geometry.warnings
                         if geometry.name is not None or _given_geometry(options) or not w.startswith("no column ")]
    stored = run
    if options.units:
        run = run.normalized(units=options.units)
    if options.path_length_cm is not None and not (options.epsilon or options.epsilon_molar or options.cadet
                                                   or options.cadet_core):
        problems.append(("", "export", "--path-length not used: only --epsilon, --cadet and --cadet-core use a "
                                       "UV path length"))
    cadet = options.cadet or options.cadet_core
    if options.uv_response is not None and not (options.epsilon or options.epsilon_molar):
        problems.append(("", "export", "--uv-response not used: it scales an extinction coefficient "
                                       "(--epsilon / --epsilon-molar)"))
    if options.molecular_weight is not None and not (options.epsilon_molar or (options.epsilon and cadet)):
        # D29 (3): a note, not an error
        problems.append(("", "export", "--mw not used: only --epsilon-molar, and --epsilon with --cadet or "
                                       "--cadet-core (mol/m^3), use a molecular weight"))
    # W17: the chromatograms written (default: the run's original one(s)); stems as for every chromatogram
    stems = chromatogram_stems(run, options.chromatograms, problems)
    if cadet and len(stems) > 1:
        # D28 (6), amended: warn and export all (of those selected) until users settle on a rule
        problems.append(("", "cadet", "%d chromatograms selected (%s): CADET files written for all"
                                      % (len(stems), ", ".join(stems))))
    from .run_details import method_variables_frame
    variables = method_variables_frame(stored)
    res3 =run.to_res3_dict() if "pickle" in formats else None
    wanted = wanted_tables(options.tables)
    per_chrom, extras = {}, {}
    for name, stem in stems.items():
        chrom = run.chromatograms[name]
        rls = None
        if tables:
            from .run_log_sections import run_log_sections
            rls = _built("sections", chrom.name, lambda: run_log_sections(run, chrom), problems)
        extra, conc = [], None
        if tables and (options.epsilon or options.epsilon_molar):
            built = _built("mg/ml curves", chrom.name,
                           lambda: concentration_curves(run, chrom, options, problems, rls), problems)
            extra, conc = built if built is not None else ([], None)
        extras[name] = extra
        problems += [(chrom.name, "curves", w) for w in other_run_notes(chrom)]
        frames = [("curves", curves_frame(chrom, extra, options, problems)), ("events", events_frame(chrom)),
                  ("peaks", peaks_frame(chrom)),
                  ("peak_summary", _built("peak table summary", chrom.name,
                                          lambda: peak_summary_frame(chrom, run, problems), problems))]
        if tables:
            built = rls if rls is not None else False  # failed sections: the table, its run-log columns empty
            for sheet, kind in TABLES:
                frames.append((sheet, _built(sheet, chrom.name,
                                             lambda: event_table_frame(chrom, kind, built, problems), problems)))
            sections = None
            if rls is not None:
                sections = _built("sections", chrom.name, lambda: sections_frame(run, chrom, rls), problems)
            if sections is not None:
                problems += [(chrom.name, "sections", w) for w in sections[1]]
            frames.append(("sections", sections[0] if sections is not None else None))
            frames.append(("pauses", _built("pauses", chrom.name, lambda: pauses_frame(chrom), problems)))
            frames.append(("concentration", conc))
            if options.bypass_uv:
                frames.append(("bypass_uv", _built("by-pass UV", chrom.name,
                                                   lambda: bypass_uv_frame(run, chrom, options, problems, rls),
                                                   problems)))
            if options.windows:
                frames.append(("phase_windows", _built("phase window statistics", chrom.name,
                                                       lambda: phase_windows_frame(run, chrom, options, problems),
                                                       problems)))
            if options.method_hints:
                hints = _built("method hints", chrom.name,
                               lambda: hint_frames(run, chrom, rls["sections"] if rls else None, problems), problems)
                if hints is not None:
                    frames += [("hint_blocks", hints[0]), ("hint_injections", hints[1])]
        per_chrom[name] = [(sheet, frame) for sheet, frame in frames
                           if frame is not None and (wanted is None or sheet in wanted)]
        if options.cadet or options.cadet_core:
            # from the stored run (build_experiment converts to SI itself); the sections built
            # above only when they are of that run (not unit-converted)
            same = rls if run is stored else None
            cadet = _built("CADET export", chrom.name, lambda: cadet_outputs(
                stored, stored.chromatograms[name], options, partial, stem, problems, sections_result=same), problems)
            written += cadet or []
    info = _built("run_info", "", lambda: run_info_frame(run, geometry, stamp, prov, list(stems)), problems)
    run_frames = [("variables", variables)] + details_frames(stored, problems)
    run_frames += [("run_info", info)] if info is not None else []
    if wanted is not None:  # column and timeline, warnings and the column guide are always written
        run_frames = [(s, f) for s, f in run_frames if s in wanted]
    run_frames.append(("warnings", warnings_frame(run, problems)))
    if options.mask_identifying:
        from .run_details import Masker
        masker = Masker(stored)
        run_frames = [(s, masker.frame(f)) for s, f in run_frames]
        per_chrom = {name: [(s, masker.frame(f)) for s, f in frames] for name, frames in per_chrom.items()}
    # what is UNICORN's and what PyCORN's, per column of the tables written (empty ones left out)
    shown = lambda frames: [(s, f) for s, f in frames if not f.empty or s in ("curves", "events")]  # noqa: E731
    run_frames.append(("column_guide", column_guide(
        [p for frames in per_chrom.values() for p in shown(frames)],
        [(s, f) for s, f in run_frames if not (s in DETAIL_TABLES and f.empty)])))
    if "xlsx" in formats:  # before any table is written
        for name, stem in stems.items():
            too_long = [s for s, f in per_chrom[name] + run_frames if len(f) >= EXCEL_ROWS]
            if too_long:
                raise Message("msg.excel_rows", "%s: %s has more rows than an Excel sheet holds; use csv"
                              % (stem, ", ".join(too_long)), name=stem, tables=", ".join(too_long))
    for name, stem in stems.items():
        frames = per_chrom[name]
        if "csv" in formats:
            for sheet, frame in frames:
                if sheet != "curves" and sheet != "events" and frame.empty:
                    continue
                suffix = "" if sheet == "curves" else "_" + file_stem(sheet)
                frame.to_csv(partial / (stem + suffix + ".csv"), index=False, encoding=CSV_ENCODING)
                written.append(stem + suffix + ".csv")
                described.append((stem + suffix + ".csv", sheet, name))
        if "xlsx" in formats:
            from .excel_export import workbook_name, write_workbook
            book = workbook_name(run.name, stem, clean_name)
            write_workbook(partial / book, run, run.chromatograms[name], frames, run_frames, excel_engine(), prov,
                           extra_curves=extras.get(name, ()))
            written.append(book)
            described.append((book, "xlsx", name))
        if "pickle" in formats:
            with open(partial / (stem + ".pkl"), "wb") as f:
                part = {name: res3.get(name, {})}
                pickle.dump(masker.deep(part) if options.mask_identifying else part, f,
                            protocol=pickle.HIGHEST_PROTOCOL)
            written.append(stem + ".pkl")
            described.append((stem + ".pkl", "pickle", name))
    if not only_workbook(formats):  # W4: the workbook holds the run-level tables and the provenance
        for key, frame in run_frames:  # what the run is and what to check
            if key in DETAIL_TABLES and frame.empty:
                continue
            frame.to_csv(partial / (file_stem(key) + ".csv"), index=False, encoding=CSV_ENCODING)
            written.append(file_stem(key) + ".csv")
            described.append((file_stem(key) + ".csv", key, None))
    described += [(f, "cadet", None) for f in written if f not in {d[0] for d in described}]
    if not only_workbook(formats):
        with open(partial / PROVENANCE_FILE, "w", encoding="utf-8") as f:
            json.dump({"run": run.name, "read_from": _source(run), **prov}, f, indent=2, ensure_ascii=False)
        written.append(PROVENANCE_FILE)
        described.append((PROVENANCE_FILE, "provenance", None))
    (partial / README).write_text(readme_text(run.name, _source(run), described, prov), encoding="utf-8")
    written.append(README)
    return written


# ---------------------------------------------------------------------------
# Batch export (Track 9, A5): the loop of `pycorn export`, for any caller
# ---------------------------------------------------------------------------

LABEL = "db_label"  # find CSV column: the database label, the export's top folder
INDEX_FILE = "index.csv"
OPTIONS_FILE = "options.json"  # `ExportOptions.to_dict()` of the export, next to index.csv
# index.csv: these columns, then the catalog's (`pycorn export` and the page's Download all)
INDEX_COLUMNS = ("export_status", "export_folder", "export_files", "export_error", "export_changed", "export_stamp")


def export_error(error=None, missing=None):
    """
    index.csv's `export_error`: one sentence why a run was not loaded or written
    (`messages.plain_error` of the exception, or a skip's reason), or for an incomplete one
    'not written: ...' (`missing_outputs`, as `pycorn export` says it); '' when fine. The home
    folder is written '~'.
    """
    from .json_output import tilde_text
    from .messages import plain_error
    if error is not None:
        return tilde_text(error if isinstance(error, str) else plain_error(error))
    if missing:
        return "not written: " + ", ".join("%s for %s" % (what, chrom) for chrom, what in missing)
    return ""


def mask_index(index, masker=None):
    """
    index.csv with people masked (`mask_identifying`; the same for `pycorn export` and the page):
    the catalog's `system` and `created_by` (`json_output.PEOPLE_COLUMNS`) as System1, User1, ...
    (built-in accounts stay; a login's DOMAIN\\name parts masked too), and those values inside
    `match_key` and `export_error`. `masker`: a `json_output.PeopleMasker` that has learned the
    catalog (and the runs loaded); None: one learning the index's people columns, in row order.
    """
    from .json_output import DERIVED_PEOPLE_COLUMNS, PEOPLE_COLUMNS, PeopleMasker, learn_catalog
    if masker is None:
        masker = PeopleMasker()
        learn_catalog(index, masker)
    index = index.copy()
    for column, prefix in PEOPLE_COLUMNS.items():
        if column in index.columns:
            index[column] = [masker.add(v, prefix) for v in index[column]]
    for column in DERIVED_PEOPLE_COLUMNS:  # match_key, export_error
        if column in index.columns:
            index[column] = [masker.text(v) for v in index[column]]
    return index


def _text(value):
    """`value` as text; None for None / NaN."""
    if value is None:
        return None
    if not isinstance(value, str):
        import pandas as pd
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
    return str(value)


def _file_rows(catalog):
    """True per row of a file (catalog_from_files: a path, no database)."""
    from .file_catalog import is_file_row
    paths = catalog["path"] if "path" in catalog.columns else [None] * len(catalog)
    databases = catalog["database"] if "database" in catalog.columns else [None] * len(catalog)
    return [is_file_row(p, d) for p, d in zip(paths, databases)]


def _lazy_database(db):
    """A function giving the database: `db` itself, or what `db()` returns (called once, when first needed)."""
    held = []

    def database():
        if not held:
            if db is None:
                raise ValueError("the selection has database runs: give db= (a PyCORNDatabase)")
            held.append(db() if callable(db) and not hasattr(db, "load_many") else db)
        return held[0]
    return database


def selection_labels(catalog, db=None):
    """
    The top folder of each catalog row: a file row its source ('zip' / 'ufol'); a database row
    the catalog's `db_label` when it has one, else its label from the settings
    (`PyCORNDatabase.database_labels()`, discovered databases too; 'alias#?' when unknown).
    `db`: a PyCORNDatabase, or a function returning one (called only when needed).
    """
    database = _lazy_database(db)
    given = list(catalog[LABEL]) if LABEL in catalog.columns else [None] * len(catalog)
    sources = catalog["source"] if "source" in catalog.columns else ["zip"] * len(catalog)
    databases = catalog["database"] if "database" in catalog.columns else [None] * len(catalog)
    labels, known = [], None
    for s, d, label, is_file in zip(sources, databases, given, _file_rows(catalog)):
        if is_file:
            labels.append(s)  # 'zip' / 'ufol', as for files named on the command line
            continue
        if isinstance(label, str):
            labels.append(label)
            continue
        if known is None:
            known = database().database_labels(None)
        if (s, str(d).lower()) not in known:
            known.update(database().database_labels("discover"))
        labels.append(known.get((s, str(d).lower()), "%s#?" % s))
    return labels


def existing_status(out, folders, stamps, rows, overwrite, update):
    """
    (status, changed) per run before anything is loaded. A run folder already there is
    compared with its `column_and_timeline.csv` stamp (`same_stamp`; `rows` for hashing a
    file whose size or time changed, and `stamps` updated in place to what is known then):
    'skipped (exists)' when unchanged, 'skipped (changed since export)' (changed 'yes') or
    'skipped (exists, not checked)' (changed 'unknown': no stamp then or now). `update`
    exports the changed and unchecked ones again (status ''), `overwrite` every one.
    """
    status, changed = [], []
    for i, folder in enumerate(folders):
        now = stamps[i]
        if overwrite or not exists(out / folder):
            status.append("")
            changed.append("")
            continue
        has, then = stored_stamp(out / folder)
        if not has or then is None or now is None:
            state, flag = "skipped (exists, not checked)", "unknown"
        else:
            same, stamps[i] = same_stamp(then, now, rows[i])
            state, flag = ("skipped (exists)", "") if same else ("skipped (changed since export)", "yes")
        status.append("" if update and flag else state)
        changed.append(flag)
    return status, changed


def _selected_runs(catalog, database, strict, on_skip=None):
    """
    (row number, run) for each loaded row, in catalog order: database rows through
    db.load_many (`database()`: the PyCORNDatabase), file rows through load_files (a .UFol read
    once); what can't be read is skipped with a warning, and `on_skip(row number, reason)`.
    """
    from .file_catalog import file_row_key, load_files, run_file_key
    files = _file_rows(catalog)
    where = {}
    for i, row in enumerate(catalog.itertuples(index=False)):
        key = (("file",) + file_row_key(row.path, getattr(row, "member", "")) if files[i] else
               (row.source, str(row.database).lower(), int(row.result_id)))
        where[key] = i

    def skipped(key, message):
        if on_skip is not None and key is not None:
            key = tuple(key) if key[0] == "file" else (key[0], str(key[1]).lower(), int(key[2]))
            if where.get(key) is not None:
                on_skip(where[key], message)

    runs = (load_files(catalog, strict=strict, on_skip=skipped) if all(files)
            else database().load_many(catalog, strict=strict, on_skip=skipped))
    for run in runs:
        if run.source == "sql":
            o = run.origin or {}
            key = (o.get("source"), str(o.get("database")).lower(), int(o.get("result_id")))
        else:
            key = ("file",) + (run_file_key(run) or ("", ""))
        yield where.get(key), run


def loaded_row_stamp(run, row, stamp):
    """
    The `export_stamp` of a catalog row once its run is loaded: a file's size and time plus its
    content hash; a database run the `tstamp` it was loaded with (it may have changed since the
    search), else `stamp`.
    """
    if stamp is not None and stamp.count(":") == 1:  # a file: store its hash too
        return row_stamp(row, content=True)
    return loaded_stamp(run) or stamp


def write_selected(run, row, folder, formats, options, overwrite, stamp):
    """
    `write_run` for catalog row `row` (stamp and match key recorded); (status, files written,
    missing) with status 'written', or 'incomplete' when an output asked for is not written
    (`missing_outputs`: [(chromatogram, what)]). Raises what write_run raises.
    """
    written = write_run(run, folder, formats, overwrite=overwrite, options=options,
                        stamp={"export_stamp": stamp, "match_key": _text(row.get("match_key"))})
    missing = missing_outputs(run, options, written)
    return ("incomplete" if missing else "written"), written, missing


def index_frame(catalog, status, folders, files, changed, stamps, errors=None):
    """
    index.csv as a DataFrame: `INDEX_COLUMNS` (one value per catalog row each; `errors` the
    `export_error` texts, None: all empty), then the catalog's columns.
    """
    index = catalog.copy()
    errors = [""] * len(index) if errors is None else list(errors)
    values = dict(export_status=list(status), export_folder=[str(f) for f in folders], export_files=list(files),
                  export_error=errors, export_changed=list(changed), export_stamp=list(stamps))
    for k, column in enumerate(INDEX_COLUMNS):
        index.insert(k, column, values[column])
    return index


def write_options_file(folder, options):
    """`OPTIONS_FILE` in `folder`: `options.to_dict()`, readable by `ExportOptions.from_dict` and `--options`."""
    with open(os.path.join(str(folder), OPTIONS_FILE), "w", encoding="utf-8") as f:
        json.dump((options or ExportOptions()).to_dict(), f, indent=2, ensure_ascii=False)
        f.write("\n")


def export_selection(catalog, out, formats=("csv",), options=None, overwrite=False, update=False, strict=False,
                     db=None, labels=None, on_run=None):
    """
    Export the runs of `catalog` (`db.find()`, `catalog_from_files()` or both mixed) into `out`,
    as `pycorn export` does: one folder per run (`run_folders`), runs already there skipped
    unless changed since (`update=True` exports those again, `overwrite=True` every run), and
    `index.csv` (`index_frame`: `INDEX_COLUMNS` then the catalog's) and `options.json`
    (`options.to_dict()`) in `out`. Returns the index rows as a DataFrame.

    `formats`: 'csv', 'xlsx', 'pickle'; `options`: `ExportOptions` (None: the defaults); `db`:
    a PyCORNDatabase, or a function returning one, for database rows (file rows need none);
    `labels`: each row's top folder (None: `selection_labels`). A run that cannot be loaded is
    'not loaded', one that cannot be written 'failed' (a warning each) unless `strict` (raised).
    `on_run(event)` is called with a dict as the export goes, its 'event' one of:
    'selected' (once: 'selected', 'to_export', 'skipped', 'changed' counts, 'folders',
    'labels'), 'loaded' ('position', 'run': before it is written), 'failed' ('position',
    'run', 'error': while the exception is handled, before its warning) and 'row' (once per
    run, as its status is known: 'position', 'row' (the `INDEX_COLUMNS` values), 'run' (None
    when not loaded), 'written' (file names), 'missing'). `export_error` says why a run is not
    loaded, failed or incomplete (`export_error()`). With `options.mask_identifying` index.csv
    is masked as the run files are (`mask_index`); the events carry the values as stored.
    """
    import warnings
    from collections import OrderedDict
    from .messages import plain_error
    formats = [str(f).strip().lower() for f in ([formats] if isinstance(formats, str) else formats)]
    unknown = [f for f in formats if f not in FORMATS]
    if unknown:
        raise ValueError("unknown format(s) %s; known: %s" % (", ".join(unknown), ", ".join(FORMATS)))
    if "xlsx" in formats:
        excel_engine()  # fail before loading anything
    options = options or ExportOptions()
    check_options(options)
    if update and overwrite:
        raise ValueError("update=True re-exports changed runs, overwrite=True every run: give one of them")
    if os.path.exists(str(out)) and not os.path.isdir(str(out)):
        raise ValueError("out %s is a file: give a folder (it is made if missing)" % out)
    catalog = catalog.reset_index(drop=True)
    database = _lazy_database(db)
    labels = selection_labels(catalog, database) if labels is None else list(labels)
    folders = run_folders(catalog, labels)
    out = long_path(out)
    rows = [row for _, row in catalog.iterrows()]
    stamps = [row_stamp(row) for row in rows]  # files: size and time only, hashed when needed
    status, changed = existing_status(out, folders, stamps, rows, overwrite, update)
    todo = catalog[[s == "" for s in status]]
    files = [""] * len(catalog)
    errors = [""] * len(catalog)
    reasons = {}  # local row number of `todo` -> why it was not loaded (the skip warning's reason)
    masker = None
    if options.mask_identifying:  # index.csv masked as the files are; catalog people first (labels in row order)
        from .json_output import PeopleMasker, learn_catalog
        masker = PeopleMasker()
        learn_catalog(catalog, masker)

    def tell(event, **info):
        if on_run is not None:
            on_run(dict(event=event, **info))

    def told(i, run=None, written=None, missing=None):
        tell("row", position=i, run=run, written=written or [], missing=missing or [], row=OrderedDict([
            ("export_status", status[i] or "not loaded"), ("export_folder", str(folders[i])),
            ("export_files", files[i]), ("export_error", errors[i]), ("export_changed", changed[i]),
            ("export_stamp", stamps[i])]))

    tell("selected", selected=len(catalog), to_export=len(todo), skipped=len(catalog) - len(todo),
         changed=sum(1 for c in changed if c), folders=folders, labels=labels)
    for i, s in enumerate(status):
        if s:  # skipped: known before anything is loaded
            told(i)
    if len(todo):
        positions = list(todo.index)
        for local, run in _selected_runs(todo, database, strict, on_skip=reasons.__setitem__):
            if local is None:
                continue
            i = positions[local]
            if masker is not None:
                masker.absorb(run)
            tell("loaded", position=i, run=run)
            stamps[i] = loaded_row_stamp(run, rows[i], stamps[i])
            try:
                status[i], written, missing = write_selected(run, rows[i], out / folders[i], formats, options,
                                                             overwrite or update, stamps[i])
            except Exception as e:
                if strict:
                    raise
                status[i] = "failed"
                errors[i] = export_error(e)
                tell("failed", position=i, run=run, error=e)
                warnings.warn("%s not written: %s" % (folders[i], plain_error(e)))
                told(i, run)
                continue
            files[i] = ";".join(written)
            errors[i] = export_error(missing=missing)
            told(i, run, written, missing)
        for local, reason in reasons.items():
            errors[positions[local]] = export_error(reason)
    for i, s in enumerate(status):
        if not s:  # not loaded (each said in a warning)
            errors[i] = errors[i] or "not loaded (see the warnings)"
            told(i)
    status = [s or "not loaded" for s in status]
    index = index_frame(catalog, status, folders, files, changed, stamps, errors)
    if masker is not None:
        index = mask_index(index, masker)
    out.mkdir(parents=True, exist_ok=True)
    index.to_csv(out / INDEX_FILE, index=False, encoding=CSV_ENCODING)
    write_options_file(out, options)
    return index
