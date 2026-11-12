# -*- coding: utf-8 -*-
"""
Find results in a UNICORN database: a catalog with one row per result, one page at a time.

`PyCORNDatabase.find(**filters)` returns a page of the catalog (`CATALOG_COLUMNS`), at most
`limit` rows ordered by ResultID; `after=` continues after the last ResultID of a page, and
`find_pages()` walks every page. `count()` counts matches without fetching them.
`variables(catalog)` returns the method variables of a page as one long table; `wide()`
pivots them into columns for display or export. `pick_list(field)` lists the values a
filter can take, with result counts.

Built to keep the database and the client light: every filter runs on the server, on
structured, indexed tables (never blobs), and no query reads the whole database. Folder
paths are built only for the folders on the page.

Several databases (`pycorn.db_sources`; every one `.env` lists by default, `sources=` to
narrow or widen): a page fills from one database at a time (`page_across`), and a result
is identified by (source, database, result_id) (`RESULT_KEY`), as ResultIDs repeat
between databases. `count()` and `pick_list()` sum over databases unless asked for
`by_database=True` (`merge_pick_lists`).

Other UNICORN versions may lack columns: each database's SchemaProfile says which it has.
Only `REQUIRED_COLUMNS` (Result.ResultID) is needed; a catalog column whose source is
missing is NULL there, and a filter that needs a missing column matches nothing there,
with a warning (`build_query(columns=, label=)`, `resolve_blocks`).

Filters (all optional; all given filters must match; a list means any of its values):

- Text fields take UNICORN-style wildcards (`*` any text, `?` one character). Whether
  case counts follows the database's collation.
- `folder`: a path from the root, `/<root>/<sub>/...`, as `run.result["FolderPath"]` and
  `pick_list("folder")` show it. Each part may hold wildcards; a final `/**` adds every
  subfolder. Results in hidden folders (or below them) are left out unless
  `include_hidden=True`.
- `run_start`: `(start, end)`, either may be None. Plain dates and times are on the run's
  own local clock (as UNICORN shows it); a plain date as `end` takes in that whole day.
  Times with a time zone compare in UTC. The run start is `RunStartDate`, else the first
  `Chromatogram.Created`.
- `column`: the column name UNICORN wrote at run time (`ColumnInformation`, usedColumns;
  `Any` = no column type chosen). Limits: only that record is searched, not method
  variables (use `variables=` for a variable such as 'Column type'); tested against a lab
  environment, most runs say `Any`. A result with several columns matches if any name does.
- `variables`: `{name: spec}` on the method variables (search criteria; the value the run
  used, scouting value included). Names are the method author's: give them exactly.
  A spec is: None (the variable is present), text (wildcards on the stored text), a number
  (== the stored number), `(op, number)` (raw stored numbers, whatever their unit),
  `(op, number, unit)` (each stored value converted to `unit` through `pycorn.units`
  first; values whose unit does not convert are left out, with a warning), or a list of
  specs (all must hold). `op` is one of `==, !=, <, <=, >, >=`. Numbers follow SQL
  Server's `TRY_CAST(... AS float)`, as `run.variables()` does.
- `has_curve_roles`: role names from `pycorn.curve_roles.ROLES`; the result must have an
  original curve with each role (in any chromatogram). Roles are matched on curve type,
  unit and name exactly as `Chromatogram.curves_with_role()` does: the distinct
  (type, unit, name) combinations are read once per connector (one grouped query over
  the curve table, the one filter step that reads all curve rows) and turned into SQL.
- `name`, `system`, `system_type`, `run_type`, `state`, `batch_id`, `created_by`: text
  (wildcards) or a list.
- `result_ids`, `run_index`, `n_chromatograms`: an int, a list of ints (any of them) or a
  `(min, max)` tuple, either end None. (On the command line: `--result-id`, `--run-index`,
  `--chromatograms`, with `3`, `3,5,8` or `3..8`.)
- `has_peak_tables`: True / False. `include_hidden`: see `folder`.
`FILTERS` lists the names; another name raises TypeError.
"""
import datetime as dt
import html
import math
import re
import warnings
from pathlib import Path

from .curve_roles import ROLES, curve_has_role
from .units import UNITS, unit_info
from .variables import SCOUTING_KIND, VARIABLE_KINDS, parse_number

FIND_SQL_FILE = Path(__file__).parent / "sql" / "results" / "find_results.sql"
# the filters find() / find_pages() / count() take (module docstring)
FILTERS = ("folder", "name", "run_start", "system", "system_type", "run_type", "state", "batch_id", "run_index",
           "result_ids", "created_by", "column", "variables", "has_curve_roles", "has_peak_tables",
           "n_chromatograms", "include_hidden")


def check_filters(filters, where="find"):
    """TypeError for a filter name `FILTERS` doesn't hold, with the closest name."""
    import difflib
    unknown = [k for k in filters if k not in FILTERS]
    if unknown:
        hints = []
        for k in unknown:
            close = difflib.get_close_matches(k, FILTERS, n=1)
            hints.append("%r%s" % (k, " (did you mean %r?)" % close[0] if close else ""))
        raise TypeError("%s(): unknown filter %s; filters: %s" % (where, ", ".join(hints), ", ".join(FILTERS)))

DEFAULT_LIMIT = 500
MAX_LIMIT = 5000  # rows per page; also the most results variables() takes at once
_ID_CHUNK = 1000  # parameters per statement (SQL Server allows 2100)

# the number SQL Server reads from a stored value; empty text is NULL (not 0), as in run.variables()
NUMBER_SQL = "TRY_CAST(NULLIF(LTRIM(RTRIM(sc.KeyWord2)), '') AS float)"
# unit spellings differ only in case ('M' molar, 'm' metre): compare them exactly
_UNIT_SQL = "sc.ExtraDisplayInformation COLLATE Latin1_General_BIN2"
_OPS = ("==", "!=", "<", "<=", ">", ">=")
_SQL_OPS = {"==": "=", "!=": "<>", "<": "<", "<=": "<=", ">": ">", ">=": ">="}
# unit-converted equality: relative tolerance for the float conversion
_REL = 1e-9

_COLUMN_NAME_SQL = (
    "CASE WHEN CHARINDEX('<name>', colinfo.stored) > 0 "
    "AND CHARINDEX('</name>', colinfo.stored) > CHARINDEX('<name>', colinfo.stored) "
    "THEN SUBSTRING(colinfo.stored, CHARINDEX('<name>', colinfo.stored) + 6, "
    "CHARINDEX('</name>', colinfo.stored) - CHARINDEX('<name>', colinfo.stored) - 6) END")

# Columns that other UNICORN versions may lack. Each piece of SQL lists the (table, column)
# pairs it reads; when the database lacks one, a catalog column is NULL and a filter
# matches nothing there (with a warning), instead of the database being left out.
_RUN_INFO = (("ResultRunInformation", "ResultID"), ("ResultRunInformation", "RunInformationTypeName"),
             ("ResultRunInformation", "RunInformation"))
_CHROM_RESULT = (("Chromatogram", "ResultID"),)
_PEAK_TABLES = (("Chromatogram", "ChromatogramID"), ("Chromatogram", "ResultID"), ("PeakTable", "ChromatogramID"))
_FOLDER_TREE = (("Folder", "FolderID"), ("Folder", "ParentFolderID"), ("Folder", "Description"))
_CRITERIA = (("ResultSearchCriteria", "ResultID"), ("ResultSearchCriteria", "SearchCriteriaName"),
             ("ResultSearchCriteria", "KeyWord1"), ("ResultSearchCriteria", "KeyWord2"))
_UNIT_COLUMN = (("ResultSearchCriteria", "ExtraDisplayInformation"),)
_CURVES = (("Curve", "ChromatogramID"), ("Curve", "CurveDataTypeName"), ("Curve", "AmplitudeUnit"),
           ("Curve", "Description"), ("Chromatogram", "ChromatogramID"), ("Chromatogram", "ResultID"))


def _r(column):
    return (("Result", column),)


# (catalog column, SQL expression, columns it reads), in catalog order; folder_path is
# filled in per page. The run start, column and chromatogram counts come from APPLY blocks
# in find_results.sql that turn into NULL by themselves when their columns are missing.
_SELECT = [
    ("database", "DB_NAME()", ()),
    ("result_id", "r.ResultID", ()),
    ("name", "r.Description", _r("Description")),
    ("folder_id", "r.FolderID", _r("FolderID")),
    ("run_start_local", "CONVERT(datetime2, rs.run_start)", ()),
    ("run_start_utc", "SWITCHOFFSET(rs.run_start, '+00:00')", ()),
    ("run_start_source", "rs.run_start_source", ()),
    ("added_utc", "SWITCHOFFSET(r.Created, '+00:00')", _r("Created")),
    ("modified_utc", "SWITCHOFFSET(r.LastModified, '+00:00')", _r("LastModified")),
    ("system", "r.SystemName", _r("SystemName")),
    ("system_type", "r.SystemTypeName", _r("SystemTypeName")),
    ("run_type", "r.RunTypeName", _r("RunTypeName")),
    ("state", "r.ResultStateName", _r("ResultStateName")),
    ("batch_id", "r.BatchId", _r("BatchId")),
    ("run_index", "r.RunIndex", _r("RunIndex")),
    ("created_by", "r.CreatedBy", _r("CreatedBy")),
    ("column", _COLUMN_NAME_SQL, ()),
    ("n_chromatograms", "chs.n", ()),
    ("has_peak_tables", "CASE WHEN EXISTS (SELECT 1 FROM dbo.Chromatogram ch JOIN dbo.PeakTable pt "
                        "ON pt.ChromatogramID = ch.ChromatogramID WHERE ch.ResultID = r.ResultID) "
                        "THEN CAST(1 AS bit) ELSE CAST(0 AS bit) END", _PEAK_TABLES),
    ("first_chromatogram_utc", "SWITCHOFFSET(chs.first_created, '+00:00')", ()),
    ("tstamp", "r.tstamp", _r("tstamp")),
]
# The same run in several places (plan section 5): `match_key` (first chromatogram time in
# UTC cut to whole seconds, system, run index; not the name), how many results of this search
# share it (`n_copies`, counted over every database searched, with the same filters), and a
# short id for the group when there are several (`duplicate_group`; None for a single copy).
COPY_COLUMNS = ["match_key", "n_copies", "duplicate_group"]
# 'source' is the alias of the `.env` source the row came from (filled in by the connector)
CATALOG_COLUMNS = (["source", "database", "result_id", "name", "folder_path"] + [c for c, _, _ in _SELECT[3:]]
                   + COPY_COLUMNS)
# a result is identified by these together (ResultIDs repeat across databases)
RESULT_KEY = ["source", "database", "result_id"]
# what the copy lookup reads of each result
_COPY_SELECT = ("result_id", "name", "system", "run_index", "first_chromatogram_utc", "modified_utc")

# The only columns a search cannot do without: a database lacking one is left out.
# Every other column falls back (see _SELECT and the filters).
REQUIRED_COLUMNS = {"Result": ("ResultID",)}


# Catalog columns filled from APPLY blocks or follow-up queries, with what they read
_DERIVED_NEEDS = {
    "folder_path": _FOLDER_TREE + _r("FolderID"),
    "column": _RUN_INFO,
    "n_chromatograms": _CHROM_RESULT,
    "first_chromatogram_utc": _CHROM_RESULT + (("Chromatogram", "Created"),),
    "match_key": _CHROM_RESULT + (("Chromatogram", "Created"),),
}
# the copy lookup (duplicates) needs these; without them a database is not searched for copies
_COPY_NEEDS = _DERIVED_NEEDS["match_key"] + _r("SystemName") + _r("RunIndex")


def empty_catalog_columns(columns):
    """
    {catalog column: missing 'Table.Column' list} for the catalog columns that stay empty
    in a database (`columns`: its SchemaProfile) because it lacks what they are read from.
    """
    out = {}
    for name, _, needs in _SELECT:
        if not _has(columns, needs):
            out[name] = _lacking(columns, needs)
    for name, needs in _DERIVED_NEEDS.items():
        if not _has(columns, needs):
            out[name] = _lacking(columns, needs)
    start = _CHROM_RESULT + (("Chromatogram", "Created"),)
    if not _has(columns, _RUN_INFO) and not _has(columns, start):
        for name in ("run_start_local", "run_start_utc", "run_start_source"):
            out[name] = _lacking(columns, _RUN_INFO + start)
    return out


class _AllColumns:
    """Stands in for a SchemaProfile when none is given: every column is there."""

    @staticmethod
    def has(table, *columns):
        return True


def _has(columns, needs):
    """True when the database (`columns`: a SchemaProfile or None) has every (table, column) in `needs`."""
    columns = columns or _AllColumns
    return all(columns.has(table, column) for table, column in needs)


def _lacking(columns, needs):
    columns = columns or _AllColumns
    return ["%s.%s" % (t, c) for t, c in needs if not columns.has(t, c)]


def _warn_missing(label, what, columns, needs, effect):
    warnings.warn("database %s lacks %s: %s %s" % (label or "?", ", ".join(_lacking(columns, needs)), what, effect),
                  stacklevel=4)


_BLOCK = re.compile(r"/\*(IF [^*]*|ELSE|END)\*/")


def resolve_blocks(sql, columns=None):
    """
    Keep or drop the column-dependent blocks of a SQL file ('IF Table.Column ...', 'ELSE',
    'END' in block comments; they nest): the IF part when the database (`columns`, a
    SchemaProfile; None = every column) has every listed column, else the ELSE part.
    """
    out, stack, pos = [], [], 0  # stack: [condition holds, in ELSE part, parent keeps]
    keeping = True
    for match in _BLOCK.finditer(sql):
        if keeping:
            out.append(sql[pos:match.start()])
        pos = match.end()
        token = match.group(1)
        if token.startswith("IF "):
            needs = [tuple(part.split(".", 1)) for part in token[3:].split()]
            if any(len(n) != 2 for n in needs):
                raise ValueError("SQL block %r: list columns as Table.Column" % token)
            stack.append([_has(columns, needs), False, keeping])
        elif not stack:
            raise ValueError("SQL block: %s without IF" % token)
        elif token == "ELSE":
            stack[-1][1] = True
        else:
            keeping = stack.pop()[2]
            continue
        holds, in_else, parent = stack[-1]
        keeping = parent and (holds != in_else)
    if stack:
        raise ValueError("SQL block: IF without END")
    out.append(sql[pos:])
    return "".join(out)

# Result columns a filter or pick list can use directly: argument -> SQL column
_RESULT_TEXT = {"name": "r.Description", "system": "r.SystemName", "system_type": "r.SystemTypeName",
                "run_type": "r.RunTypeName", "state": "r.ResultStateName", "batch_id": "r.BatchId",
                "created_by": "r.CreatedBy"}

PICK_LISTS = ("folder", "system", "system_type", "run_type", "state", "created_by", "batch_id", "column",
              "variable", "variable_value", "curve_role")


# ---------------------------------------------------------------------------
# Parameters and wildcards
# ---------------------------------------------------------------------------

class _Params(dict):
    """pymssql parameters with generated names."""

    def add(self, value):
        name = "p%d" % len(self)
        self[name] = value
        return "%%(%s)s" % name


def like_pattern(text):
    """A LIKE pattern (ESCAPE '\\') for UNICORN-style wildcards: '*' any text, '?' one character."""
    escaped = re.sub(r"([\\%_\[])", r"\\\1", str(text))
    return escaped.replace("*", "%").replace("?", "_")


def _as_list(value):
    return list(value) if isinstance(value, (list, tuple, set, frozenset)) else [value]


def _any_like(column, values, params):
    parts = ["%s LIKE %s ESCAPE '\\'" % (column, params.add(like_pattern(v))) for v in _as_list(values)]
    return "(" + " OR ".join(parts) + ")"


def _int_filter(column, value, params, name="filter"):
    """An int, a list of ints (any of them) or a (min, max) range (either end may be None)."""
    def whole(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            raise ValueError("%s=%r: give an int, a list of ints or a (min, max) tuple (either end None)"
                             % (name, value)) from None

    if isinstance(value, tuple) and len(value) == 2:
        parts = ["%s %s %s" % (column, op, params.add(whole(v))) for op, v in ((">=", value[0]), ("<=", value[1]))
                 if v is not None]
        return "(" + " AND ".join(parts) + ")" if parts else None
    values = [whole(v) for v in _as_list(value)]
    if len(values) > _ID_CHUNK:
        raise ValueError("at most %d values in one filter (got %d)" % (_ID_CHUNK, len(values)))
    return "%s IN (%s)" % (column, ", ".join(params.add(v) for v in values))


# ---------------------------------------------------------------------------
# Run start
# ---------------------------------------------------------------------------

def _parse_when(value):
    """(datetime, is_date_only) from a date, datetime or ISO text."""
    if isinstance(value, dt.datetime):
        return value, False
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day), True
    text = str(value).strip()
    try:
        return dt.datetime.combine(dt.date.fromisoformat(text), dt.time()), True
    except ValueError:
        pass
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")), False
    except ValueError:
        raise ValueError("run_start: cannot read %r as a date or time (use ISO, e.g. '2019-06-30' or "
                         "'2019-06-30T14:00:00+00:00')" % (value,)) from None


def _run_start_filter(run_start, params):
    if not isinstance(run_start, (tuple, list)) or len(run_start) != 2:
        raise ValueError("run_start must be (start, end); either may be None")
    parts = []
    for bound, value in zip(("start", "end"), run_start):
        if value is None:
            continue
        when, date_only = _parse_when(value)
        if bound == "end" and date_only:
            when, op = when + dt.timedelta(days=1), "<"  # the whole end day
        else:
            op = ">=" if bound == "start" else "<="
        if when.tzinfo is not None:  # a moment: compare in UTC
            text = when.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "+00:00"
            parts.append("rs.run_start %s CONVERT(datetimeoffset, %s, 127)" % (op, params.add(text)))
        else:  # the run's own local clock
            text = when.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            parts.append("CONVERT(datetime2, rs.run_start) %s CONVERT(datetime2, %s, 126)" % (op, params.add(text)))
    return "(" + " AND ".join(parts) + ")" if parts else None


# ---------------------------------------------------------------------------
# Folders
# ---------------------------------------------------------------------------

def _folder_ctes(folder, params):
    """
    CTEs for one folder path pattern: `folder_hit` holds the matched folders (plus every
    subfolder after a final '**'). The recursion follows only folders whose name matches
    the path part at their depth, so it never walks the whole tree.
    """
    parts = [p for p in str(folder).strip().strip("/").split("/")]
    subtree = bool(parts) and parts[-1] == "**"
    if subtree:
        parts = parts[:-1]
    if not parts or any(p in ("", "**") for p in parts):
        raise ValueError("folder: give a path from the root, '/<root>/<sub>', optionally ending in '/**' "
                         "(got %r)" % (folder,))
    patterns = [params.add(like_pattern(p)) for p in parts]
    depth = len(parts)
    step = " ".join("WHEN %d THEN %s" % (i + 1, pat) for i, pat in enumerate(patterns) if i)
    ctes = ["folder_match (FolderID, depth) AS ("
            "SELECT f.FolderID, 1 FROM dbo.Folder f WHERE f.ParentFolderID IS NULL "
            "AND f.Description LIKE %s ESCAPE '\\'" % patterns[0]]
    if depth > 1:
        ctes[0] += (" UNION ALL SELECT f.FolderID, m.depth + 1 FROM dbo.Folder f "
                    "JOIN folder_match m ON f.ParentFolderID = m.FolderID WHERE m.depth < %d "
                    "AND f.Description LIKE (CASE m.depth + 1 %s END) ESCAPE '\\'" % (depth, step))
    ctes[0] += ")"
    hit = "folder_hit (FolderID) AS (SELECT FolderID FROM folder_match WHERE depth = %d" % depth
    if subtree:
        hit += " UNION ALL SELECT f.FolderID FROM dbo.Folder f JOIN folder_hit h ON f.ParentFolderID = h.FolderID"
    ctes.append(hit + ")")
    return ctes


_HIDDEN_CTE = ("hidden_folder (FolderID) AS (SELECT FolderID FROM dbo.Folder WHERE IsHidden = 1 "
               "UNION ALL SELECT f.FolderID FROM dbo.Folder f JOIN hidden_folder h ON f.ParentFolderID = h.FolderID)")


def folder_paths(cursor, folder_ids, columns=None):
    """
    {FolderID: '/<root>/<sub>/...'} for these folders, reading only them and their parents;
    empty when the database lacks the folder columns (`columns`: its SchemaProfile).
    """
    if not _has(columns, _FOLDER_TREE):
        return {}
    ids = sorted({int(i) for i in folder_ids if i is not None and i == i})
    rows = {}
    for start in range(0, len(ids), _ID_CHUNK):
        chunk = ids[start:start + _ID_CHUNK]
        params = _Params()
        cursor.execute(
            "WITH up (FolderID, ParentFolderID, Description) AS ("
            "SELECT FolderID, ParentFolderID, Description FROM dbo.Folder WHERE FolderID IN (%s) "
            "UNION ALL SELECT f.FolderID, f.ParentFolderID, f.Description FROM dbo.Folder f "
            "JOIN up ON f.FolderID = up.ParentFolderID) "
            "SELECT DISTINCT FolderID, ParentFolderID, Description FROM up"
            % ", ".join(params.add(i) for i in chunk), params)
        for folder_id, parent, name in cursor.fetchall():
            rows[folder_id] = (parent, name)
    paths = {}
    for folder_id in ids:
        names, current, seen = [], folder_id, set()
        while current is not None and current in rows and current not in seen:
            seen.add(current)
            parent, name = rows[current]
            names.append(name or "")
            current = parent
        paths[folder_id] = "/" + "/".join(reversed(names)) if folder_id in rows else None
    return paths


# ---------------------------------------------------------------------------
# Method variables
# ---------------------------------------------------------------------------

def _variable_exists(name, condition, params):
    kinds = ", ".join(params.add(k) for k in VARIABLE_KINDS)
    return ("EXISTS (SELECT 1 FROM dbo.ResultSearchCriteria sc WHERE sc.ResultID = r.ResultID "
            "AND sc.SearchCriteriaName IN (%s) AND sc.KeyWord1 = %s%s)"
            % (kinds, params.add(name), " AND " + condition if condition else ""))


def _number_condition(op, value, params, unit=None, notes=None, name=None):
    if op not in _OPS:
        raise ValueError("variables: unknown operator %r (use one of %s)" % (op, ", ".join(_OPS)))
    value = float(value)
    if unit is None:
        return "%s %s %s" % (NUMBER_SQL, _SQL_OPS[op], params.add(value))
    target = unit_info(unit)
    if target is None:
        raise ValueError("variables: unknown unit %r for %r (known: %s)" % (unit, name, ", ".join(sorted(UNITS))))
    # every stored unit of the same quantity, with the value in that unit
    parts = []
    for spelling, (quantity, factor) in UNITS.items():
        if quantity != target.quantity:
            continue
        there = value * target.factor / factor
        if op == "==":
            test = "ABS(%s - %s) <= %s" % (NUMBER_SQL, params.add(there), params.add(abs(there) * _REL))
        elif op == "!=":
            test = "ABS(%s - %s) > %s" % (NUMBER_SQL, params.add(there), params.add(abs(there) * _REL))
        else:
            test = "%s %s %s" % (NUMBER_SQL, _SQL_OPS[op], params.add(there))
        parts.append("(%s = %s AND %s)" % (_UNIT_SQL, params.add(spelling), test))
    if notes is not None:
        notes.append((name, target.unit, [u for u, (q, _) in UNITS.items() if q == target.quantity]))
    return "(" + " OR ".join(parts) + ")"


def _variable_filters(variables, params, notes, columns=None, label=None):
    if not _has(columns, _CRITERIA):
        _warn_missing(label, "the variables filter", columns, _CRITERIA, "matches nothing there")
        return ["1 = 0"]
    clauses = []
    for name, spec in variables.items():
        for one in (spec if isinstance(spec, list) else [spec]):
            if one is None:
                clauses.append(_variable_exists(name, None, params))
            elif isinstance(one, str):
                clauses.append(_variable_exists(name, "sc.KeyWord2 LIKE %s ESCAPE '\\'"
                                                % params.add(like_pattern(one)), params))
            elif isinstance(one, bool):
                raise ValueError("variables: %r: give text or a number, not %r" % (name, one))
            elif isinstance(one, (int, float)):
                clauses.append(_variable_exists(name, _number_condition("==", one, params), params))
            elif isinstance(one, tuple) and len(one) in (2, 3):
                op, value = one[0], one[1]
                unit = one[2] if len(one) == 3 else None
                if isinstance(value, str):
                    if op not in ("==", "!=") or unit is not None:
                        raise ValueError("variables: %r: text compares only with '==' or '!=', without a unit" % name)
                    test = "sc.KeyWord2 %sLIKE %s ESCAPE '\\'" % ("NOT " if op == "!=" else "", params.add(like_pattern(value)))
                    clauses.append(_variable_exists(name, test, params))
                elif unit is not None and not _has(columns, _UNIT_COLUMN):  # no stored units to convert from
                    _warn_missing(label, "the variables filter with a unit (%r)" % name, columns, _UNIT_COLUMN,
                                  "matches nothing there")
                    clauses.append("1 = 0")
                else:
                    clauses.append(_variable_exists(name, _number_condition(op, value, params, unit, notes, name), params))
            else:
                raise ValueError("variables: %r: unsupported spec %r (see pycorn.search)" % (name, one))
    return clauses


def _warn_unconverted(cursor, notes):
    """Warn when some stored values of a unit-filtered variable have a unit that does not convert."""
    for name, unit, spellings in notes:
        params = _Params()
        cursor.execute(
            "SELECT TOP 1 1 FROM dbo.ResultSearchCriteria sc WHERE sc.KeyWord1 = %s AND sc.SearchCriteriaName IN (%s) "
            "AND (sc.ExtraDisplayInformation IS NULL OR %s NOT IN (%s))"
            % (params.add(name), ", ".join(params.add(k) for k in VARIABLE_KINDS), _UNIT_SQL,
               ", ".join(params.add(u) for u in spellings)), params)
        if cursor.fetchall():
            warnings.warn("variables: some values of %r have a unit that does not convert to %s; "
                          "they are left out" % (name, unit), stacklevel=3)


# ---------------------------------------------------------------------------
# Curve roles
# ---------------------------------------------------------------------------

class _CurveKind:
    """What curve_has_role() looks at, for one stored (type, unit, name) combination."""
    __slots__ = ("data_type", "unit", "name")

    def __init__(self, data_type, unit, name):
        self.data_type, self.unit, self.name = data_type, unit, name


def _original_curve(columns, alias="c"):
    """SQL keeping original curves only; every curve counts when the database has no IsOriginalData."""
    return "ISNULL(%s.IsOriginalData, 1) = 1" % alias if _has(columns, (("Curve", "IsOriginalData"),)) else "1 = 1"


def read_curve_kinds(cursor, columns=None):
    """Distinct (CurveDataTypeName, AmplitudeUnit, Description) of original curves ([] without those columns)."""
    if not _has(columns, _CURVES):
        return []
    cursor.execute("SELECT DISTINCT c.CurveDataTypeName, c.AmplitudeUnit, c.Description FROM dbo.Curve c "
                   "WHERE %s" % _original_curve(columns))
    return [tuple(row) for row in cursor.fetchall()]


def _role_condition(role, kinds, params):
    """SQL on curve `c` for this role, from the stored combinations; None when no curve has it."""
    if role not in ROLES:
        raise KeyError("unknown curve role %r; known roles: %s" % (role, ", ".join(ROLES)))
    matched = [k for k in kinds if curve_has_role(_CurveKind(*k), role)]
    if not matched:
        return None
    by_type_unit = {}
    for data_type, unit, name in kinds:
        by_type_unit.setdefault((data_type, unit), []).append(name)
    whole, names = [], {}
    for data_type, unit, name in matched:
        if (data_type, unit) in whole:
            continue
        if all(curve_has_role(_CurveKind(data_type, unit, n), role) for n in by_type_unit[(data_type, unit)]):
            whole.append((data_type, unit))  # every name of this type and unit has the role
        else:
            names.setdefault((data_type, unit), []).append(name)
    parts = []
    for (data_type, unit), these in [(key, None) for key in whole] + list(names.items()):
        test = "c.CurveDataTypeName = %s AND %s" % (
            params.add(data_type), "c.AmplitudeUnit IS NULL" if unit is None
            else "c.AmplitudeUnit COLLATE Latin1_General_BIN2 = %s" % params.add(unit))
        if these:
            test += " AND c.Description IN (%s)" % ", ".join(params.add(n) for n in these)
        parts.append("(" + test + ")")
    return "(" + " OR ".join(parts) + ")"


def _role_filters(roles, kinds, params, columns=None):
    clauses = []
    for role in _as_list(roles):
        condition = _role_condition(role, kinds, params)
        clauses.append("1 = 0" if condition is None else
                       "EXISTS (SELECT 1 FROM dbo.Chromatogram ch JOIN dbo.Curve c ON c.ChromatogramID = ch.ChromatogramID "
                       "WHERE ch.ResultID = r.ResultID AND %s AND %s)" % (_original_curve(columns), condition))
    return clauses


def roles_present(kinds):
    """Roles that at least one stored original curve has."""
    return [role for role in ROLES if any(curve_has_role(_CurveKind(*k), role) for k in kinds)]


# ---------------------------------------------------------------------------
# The catalog query
# ---------------------------------------------------------------------------

def build_query(mode="find", limit=DEFAULT_LIMIT, after=None, curve_kinds=None, folder=None, folder_ids=None,
                name=None, run_start=None, system=None, system_type=None, run_type=None, state=None,
                batch_id=None, run_index=None, result_ids=None, created_by=None, column=None, variables=None,
                has_curve_roles=None, has_peak_tables=None, n_chromatograms=None, include_hidden=False,
                sql_file=None, columns=None, label=None, match_keys=None):
    """
    (sql, params, notes) for `mode` 'find' (one page, `limit` + 1 rows), 'count', or
    'copies' (`_COPY_SELECT` of every result matching the filters and one of
    `match_keys`, from `copy_keys`). `notes` lists unit-converted variable filters, for
    `_warn_unconverted`. `curve_kinds` (from `read_curve_kinds`) is needed only with
    `has_curve_roles`.

    `columns` is the database's SchemaProfile (None = every column is there). A catalog
    column whose source column is missing is NULL; a filter that needs a missing column
    matches nothing in this database, with a warning naming `label` (e.g. 'lab2#2') and
    the columns.
    """
    params, ctes, where, notes = _Params(), [], [], []

    def usable(what, needs):
        if _has(columns, needs):
            return True
        _warn_missing(label, "the %s filter" % what, columns, needs, "matches nothing there")
        where.append("1 = 0")
        return False

    if folder is not None:
        if isinstance(folder, (list, tuple)):
            raise ValueError("folder: give one path pattern (a list is not supported)")
        if usable("folder", _FOLDER_TREE + _r("FolderID")):
            ctes += _folder_ctes(folder, params)
            where.append("r.FolderID IN (SELECT FolderID FROM folder_hit)")
    if not include_hidden:
        hidden = (("Folder", "FolderID"), ("Folder", "ParentFolderID"), ("Folder", "IsHidden")) + _r("FolderID")
        if _has(columns, hidden):
            ctes.append(_HIDDEN_CTE)
            where.append("r.FolderID NOT IN (SELECT FolderID FROM hidden_folder)")
        else:
            _warn_missing(label, "hidden folders", columns, hidden, "cannot be told apart; their results are included")
    if folder_ids is not None and usable("folder_ids", _r("FolderID")):
        where.append(_int_filter("r.FolderID", folder_ids, params))
    for arg, value in (("name", name), ("system", system), ("system_type", system_type), ("run_type", run_type),
                       ("state", state), ("batch_id", batch_id), ("created_by", created_by)):
        if value is not None and usable(arg, _r(_RESULT_TEXT[arg][2:])):
            where.append(_any_like(_RESULT_TEXT[arg], value, params))
    if run_start is not None:
        # the run start is RunStartDate, else the first Chromatogram.Created
        if _has(columns, _RUN_INFO) or usable("run_start", _CHROM_RESULT + (("Chromatogram", "Created"),)):
            clause = _run_start_filter(run_start, params)
            if clause:
                where.append(clause)
    if run_index is not None and usable("run_index", _r("RunIndex")):
        where.append(_int_filter("r.RunIndex", run_index, params, "run_index"))
    if result_ids is not None:
        where.append(_int_filter("r.ResultID", result_ids, params, "result_ids"))
    if column is not None and usable("column", _RUN_INFO):
        names = ["%<name>" + like_pattern(html.escape(str(v), quote=False)) + "</name>%" for v in _as_list(column)]
        where.append("(" + " OR ".join("colinfo.stored LIKE %s ESCAPE '\\'" % params.add(n) for n in names) + ")")
    if variables:
        where += _variable_filters(variables, params, notes, columns, label)
    if has_curve_roles and usable("has_curve_roles", _CURVES):
        if curve_kinds is None:
            raise ValueError("has_curve_roles needs curve_kinds (read_curve_kinds)")
        where += _role_filters(has_curve_roles, curve_kinds, params, columns)
    if has_peak_tables is not None and usable("has_peak_tables", _PEAK_TABLES):
        where.append("%sEXISTS (SELECT 1 FROM dbo.Chromatogram ch JOIN dbo.PeakTable pt ON pt.ChromatogramID = "
                     "ch.ChromatogramID WHERE ch.ResultID = r.ResultID)" % ("" if has_peak_tables else "NOT "))
    if n_chromatograms is not None and usable("n_chromatograms", _CHROM_RESULT):
        where.append(_int_filter("chs.n", n_chromatograms, params, "n_chromatograms"))
    if match_keys is not None:
        where.append(_match_key_filter(match_keys, params))

    sql = resolve_blocks(Path(sql_file or FIND_SQL_FILE).read_text(encoding="utf-8"), columns)
    if mode == "copies":
        select = ", ".join("%s AS [%s]" % (expr if _has(columns, needs) else "NULL", col)
                           for col, expr, needs in _SELECT if col in _COPY_SELECT)
        top, order = "", "ORDER BY r.ResultID"
    elif mode == "find":
        limit = int(limit)
        if not 1 <= limit <= MAX_LIMIT:
            raise ValueError("limit must be 1-%d (use find_pages() for more)" % MAX_LIMIT)
        if after is not None:
            where.append("r.ResultID > %s" % params.add(int(after)))
        # 'database', 'column' are keywords; a column the database lacks is NULL
        select = ", ".join("%s AS [%s]" % (expr if _has(columns, needs) else "NULL", col)
                           for col, expr, needs in _SELECT)
        top, order = "TOP (%s)" % params.add(limit + 1), "ORDER BY r.ResultID"
    elif mode == "count":
        select, top, order = "COUNT(*) AS n", "", ""
    else:
        raise ValueError("mode must be 'find', 'count' or 'copies'")
    sql = (sql.replace("/*CTES*/", "WITH " + ",\n".join(ctes) if ctes else "")
              .replace("/*TOP*/", top).replace("/*SELECT*/", select)
              .replace("/*FILTERS*/", "".join("\n    AND " + w for w in where)).replace("/*ORDER*/", order))
    return sql, params, notes


def _column_name(stored):
    return html.unescape(stored) if isinstance(stored, str) else None  # pandas may hand back NaN for NULL


def _tstamp(value):
    return value.hex() if isinstance(value, (bytes, bytearray)) else value


def _utc(value):
    """pymssql returns datetimeoffset as an aware datetime: make it UTC (pandas then keeps one zone)."""
    return value.astimezone(dt.timezone.utc) if isinstance(value, dt.datetime) and value.tzinfo else value


def fetch_page(cursor, limit=DEFAULT_LIMIT, source=None, **filters):
    """
    (catalog DataFrame, more) for one page of the database the cursor is in; `more` is
    True when another page follows there. `source` fills the 'source' column.
    """
    import pandas as pd
    sql, params, notes = build_query("find", limit=limit, **filters)
    cursor.execute(sql, params)
    columns = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    more = len(rows) > int(limit)
    rows = rows[:int(limit)]
    _warn_unconverted(cursor, notes)
    data = {c: [row[i] for row in rows] for i, c in enumerate(columns)}
    data["source"] = [source] * len(rows)
    paths = folder_paths(cursor, data["folder_id"], filters.get("columns"))
    data["folder_path"] = [paths.get(f) for f in data["folder_id"]]
    data["column"] = [_column_name(v) for v in data["column"]]
    data["tstamp"] = [_tstamp(v) for v in data["tstamp"]]
    for c in ("run_start_utc", "added_utc", "modified_utc", "first_chromatogram_utc"):
        data[c] = pd.to_datetime([_utc(v) for v in data[c]], utc=True)
    data["run_start_local"] = pd.to_datetime(data["run_start_local"])
    _add_match_keys(data)
    data["n_copies"] = pd.array(data["n_copies"], dtype="Int64")
    frame = pd.DataFrame({c: data[c] for c in CATALOG_COLUMNS})
    # unknown (NULL, the database lacks peak tables) stays <NA> rather than becoming False
    peaks = frame["has_peak_tables"]
    frame["has_peak_tables"] = peaks.astype("boolean") if peaks.isna().any() else peaks.astype(bool)
    return frame, more


def empty_catalog():
    import pandas as pd
    return pd.DataFrame({c: pd.Series(dtype=object) for c in CATALOG_COLUMNS})


# ---------------------------------------------------------------------------
# The same run in several places (plan section 5)
# ---------------------------------------------------------------------------

def _missing(value):
    import pandas as pd
    return value is None or (not isinstance(value, str) and pd.isna(value))


def _second(utc):
    """A chromatogram time as UTC cut to the whole second (some exports store whole seconds only)."""
    import pandas as pd
    t = pd.Timestamp(utc)
    return (t.tz_localize("UTC") if t.tzinfo is None else t.tz_convert("UTC")).floor("s")


def match_key(first_chromatogram_utc, system, run_index):
    """
    The key that says two results are the same run (plan section 5): first chromatogram
    time in UTC cut to whole seconds, system, run index, as one text. None without a
    chromatogram time.

    The name is not part of it (owner, 2026-11-08): UNICORN saves an evaluated result as a
    new result under a new name with the same chromatogram (18 such groups in the local
    databases, 'Normal' original plus 'Evaluated' copies), and those count as copies.
    `Result.Created` is not used either: importing a result into another database resets it.
    """
    if _missing(first_chromatogram_utc):
        return None
    return "%s|%s|%s" % (_second(first_chromatogram_utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "" if _missing(system) else system, "" if _missing(run_index) else int(run_index))


def duplicate_group(key):
    """A short id for the results sharing `key` (the same in every page and database)."""
    import hashlib
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]


def _add_match_keys(data):
    """Fill COPY_COLUMNS of a catalog under construction (dict of column lists): one copy each."""
    data["match_key"] = [match_key(*v) for v in zip(data["first_chromatogram_utc"], data["system"], data["run_index"])]
    # without a key, whether there are copies is unknown
    data["n_copies"] = [None if k is None else 1 for k in data["match_key"]]
    data["duplicate_group"] = [None] * len(data["match_key"])


def copy_keys(catalog):
    """
    The distinct lookup keys (system, run index, first chromatogram second as
    'YYYY-MM-DDTHH:MM:SS' UTC) of the catalog rows that have a `match_key`.
    """
    keys = []
    for utc, system, run_index, key in zip(catalog["first_chromatogram_utc"], catalog["system"],
                                           catalog["run_index"], catalog["match_key"]):
        if key is None or _missing(key):
            continue
        keys.append((None if _missing(system) else system, None if _missing(run_index) else int(run_index),
                     _second(utc).strftime("%Y-%m-%dT%H:%M:%S")))
    return list(dict.fromkeys(keys))


_KEYS_PER_QUERY = 400  # 3 parameters per key, beside the filters' own (SQL Server allows 2100)


def _match_key_filter(keys, params):
    """
    Results whose system, run index and first chromatogram second equal one of `keys`
    (`copy_keys`). The system is indexed (IX_Result_SystemName); the exact comparison
    (case) is done by the caller on what comes back (`match_key`).
    """
    if not keys:
        return "1 = 0"
    rows = ", ".join("(%s, %s, %s)" % (params.add(s), params.add(i), params.add(t)) for s, i, t in keys)
    return ("EXISTS (SELECT 1 FROM (VALUES %s) k(s, i, t) "
            "WHERE (r.SystemName = k.s OR (r.SystemName IS NULL AND k.s IS NULL)) "
            "AND (r.RunIndex = k.i OR (r.RunIndex IS NULL AND k.i IS NULL)) "
            "AND chs.first_created >= TODATETIMEOFFSET(CONVERT(datetime2, k.t, 126), 0) "
            "AND chs.first_created < TODATETIMEOFFSET(DATEADD(second, 1, CONVERT(datetime2, k.t, 126)), 0))" % rows)


COPY_LOOKUP_COLUMNS = ["source", "database", "result_id", "name", "match_key", "modified_utc"]


def fetch_copies(cursor, keys, source=None, database=None, **filters):
    """
    Every result in the cursor's database that matches `filters` and has one of `keys`
    (`copy_keys`): a DataFrame of `COPY_LOOKUP_COLUMNS` (`source` and `database` fill
    those columns). Empty when the database lacks
    what the key is read from (`_COPY_NEEDS`). One query per 400 keys, each an index seek
    on the system name (then the chromatograms of that system's results).
    """
    import pandas as pd
    rows, columns = [], list(_COPY_SELECT)
    if keys and _has(filters.get("columns"), _COPY_NEEDS):
        for start in range(0, len(keys), _KEYS_PER_QUERY):
            sql, params, _ = build_query("copies", match_keys=keys[start:start + _KEYS_PER_QUERY], **filters)
            cursor.execute(sql, params)
            columns = [d[0] for d in cursor.description]
            rows += cursor.fetchall()
    data = {c: [row[i] for row in rows] for i, c in enumerate(columns)}
    for c in ("first_chromatogram_utc", "modified_utc"):
        data[c] = pd.to_datetime([_utc(v) for v in data[c]], utc=True)
    data["database"] = [database] * len(rows)
    data["source"] = [source] * len(rows)
    _add_match_keys(data)
    return pd.DataFrame({c: data[c] for c in COPY_LOOKUP_COLUMNS})


def mark_copies(catalog, copies, dedupe=False):
    """
    Fill `n_copies` and `duplicate_group` of a catalog page from `copies` (the
    `fetch_copies` frames of every database searched, in search order, the page's own
    rows included). With `dedupe`, keep one row per group: the most recently changed copy
    (`modified_utc`, owner 2026-11-08), the first in search order on a tie. A row whose
    kept copy lies on another page or in another database is dropped here, so over all
    pages every run appears once; a deduplicated page can be shorter than `limit`.
    """
    import pandas as pd
    out = catalog.copy()
    if not len(out):
        return out
    copies = copies[copies["match_key"].notna()].reset_index(drop=True)
    # newest first (unknown change time last), then search order
    ranked = copies.assign(_order=range(len(copies))).sort_values(
        ["modified_utc", "_order"], ascending=[False, True], na_position="last", kind="stable")
    kept, counts = {}, {}
    for key, group in ranked.groupby("match_key", sort=False):
        best = group.iloc[0]
        kept[key] = (best["source"], str(best["database"]).lower(), int(best["result_id"]))
        counts[key] = len(group)
    n, groups, keep = [], [], []
    for _, row in out.iterrows():
        key = None if _missing(row["match_key"]) else row["match_key"]
        if key is None or key not in counts:
            n.append(None if key is None else 1)
            groups.append(None)
            keep.append(True)
            continue
        n.append(max(counts[key], 1))
        groups.append(duplicate_group(key) if counts[key] > 1 else None)
        keep.append(kept[key] == (row["source"], str(row["database"]).lower(), int(row["result_id"])))
    out["n_copies"] = pd.array(n, dtype="Int64")
    out["duplicate_group"] = groups
    if dedupe:
        out = out[keep].reset_index(drop=True)
    return out


def after_key(after):
    """(source, database, result_id) to continue after, from a page, a tuple, or None."""
    if after is None:
        return None
    if hasattr(after, "columns"):  # a catalog page: continue after its last row
        if not len(after):
            return None
        last = after.iloc[-1]
        return last["source"], last["database"], int(last["result_id"])
    if isinstance(after, tuple) and len(after) == 3:
        return after[0], after[1], int(after[2])
    raise ValueError("after: give the last page, or (source, database, result_id)")


def page_across(scopes, fetch, limit=DEFAULT_LIMIT, after=None):
    """
    One page over several databases, filled one database at a time in scope order
    (owner, 2026-11-08), each in ResultID order. `scopes` yields objects with `.source`
    and `.database` (in order); `fetch(scope, limit, after_id)` returns (page, more) for
    one database. `after` = (source, database, result_id) continues after that row
    (database None: in the source's first database).
    Returns (catalog, more). Databases after a full page are asked for one row only, to
    tell whether another page follows.
    """
    import pandas as pd
    frames, more, remaining = [], False, int(limit)
    if not 1 <= remaining <= MAX_LIMIT:  # checked here: 0 would only probe, never reach the query's check
        raise ValueError("limit must be 1-%d (use find_pages() for more)" % MAX_LIMIT)
    started = after is None
    for scope in scopes:
        after_id = None
        if not started:  # database None: the source's first database
            if scope.source != after[0] or (after[1] is not None and str(scope.database).lower() != str(after[1]).lower()):
                continue
            started, after_id = True, after[2]
        if remaining == 0:  # page full: does anything follow?
            probe, _ = fetch(scope, 1, after_id)
            if len(probe):
                more = True
                break
            continue
        frame, scope_more = fetch(scope, remaining, after_id)
        if len(frame):
            frames.append(frame)
            remaining -= len(frame)
        if scope_more:
            more = True
            break
    if not started:
        raise ValueError("after: its source and database are not in this search's scope")
    frames = [f for f in frames if len(f)]
    return (pd.concat(frames, ignore_index=True) if frames else empty_catalog()), more


def count(cursor, **filters):
    sql, params, notes = build_query("count", **filters)
    cursor.execute(sql, params)
    n = cursor.fetchall()[0][0]
    _warn_unconverted(cursor, notes)
    return n


# ---------------------------------------------------------------------------
# Variables of a page, and the wide display table
# ---------------------------------------------------------------------------

VARIABLE_COLUMNS = ["source", "database", "result_id", "kind", "variable", "value", "number", "unit", "scouting"]


def _result_ids(catalog):
    ids = catalog["result_id"] if hasattr(catalog, "columns") else catalog
    return [int(i) for i in ids]


def fetch_variables(cursor, catalog, names=None, source=None, database=None, columns=None, label=None):
    """
    Long table of search-criteria rows for the results in `catalog` (a catalog page or
    ResultIDs, all in the database the cursor is in): `VARIABLE_COLUMNS`, one row per
    stored row, in stored order, as `run.variables()` gives them. `names` limits it to
    these variables; `source` and `database` fill those columns. `columns` (the
    database's SchemaProfile): without a unit column, units are empty; without the name
    or value columns, the table is empty (with a warning naming `label`).
    """
    import pandas as pd
    ids = _result_ids(catalog)
    if len(ids) > MAX_LIMIT:
        raise ValueError("variables: at most %d results at once (one page; got %d)" % (MAX_LIMIT, len(ids)))
    if not _has(columns, _CRITERIA):
        _warn_missing(label, "variables", columns, _CRITERIA, "cannot be read; none are returned from there")
        ids = []
    # without them: no unit, and rows in stored order within a result are not guaranteed
    unit = "sc.ExtraDisplayInformation" if _has(columns, _UNIT_COLUMN) else "NULL"
    order = ", sc.ResultSearchCriteriaID" if _has(columns, (("ResultSearchCriteria", "ResultSearchCriteriaID"),)) else ""
    rows = []
    for start in range(0, len(ids), _ID_CHUNK):
        params = _Params()
        sql = ("SELECT sc.ResultID, sc.SearchCriteriaName, sc.KeyWord1, sc.KeyWord2, %s "
               "FROM dbo.ResultSearchCriteria sc WHERE sc.ResultID IN (%s)"
               % (unit, ", ".join(params.add(i) for i in ids[start:start + _ID_CHUNK])))
        if names is not None:
            sql += " AND sc.KeyWord1 IN (%s)" % ", ".join(params.add(n) for n in _as_list(names))
        cursor.execute(sql + " ORDER BY sc.ResultID" + order, params)
        rows += cursor.fetchall()
    frame = pd.DataFrame({
        "source": pd.Series([source] * len(rows), dtype="category"),
        "database": pd.Series([database] * len(rows), dtype="category"),
        "result_id": pd.Series([r[0] for r in rows], dtype="int64"),
        "kind": pd.Series([r[1] for r in rows], dtype="category"),
        "variable": pd.Series([r[2] for r in rows], dtype="category"),
        "value": pd.Series([r[3] for r in rows], dtype=object),
        "number": pd.Series([parse_number(r[3]) for r in rows], dtype=float),
        "unit": pd.Series([r[4] for r in rows], dtype="category"),
        "scouting": pd.Series([r[1] == SCOUTING_KIND for r in rows], dtype=bool),
    }, columns=VARIABLE_COLUMNS)
    return frame


def wide(catalog, variables, kinds=VARIABLE_KINDS):
    """
    The catalog with one text and one number column per method variable present in
    `variables` (from `fetch_variables`, or `pycorn.file_catalog.catalog_from_files(...,
    variables=True)`; file rows match on path and member), for display or export; built
    here, never stored.

    Columns are '<name>' (stored text) and '<name> (number)'. When every value of a
    variable has the same unit, it is in both headers ('Flow rate [ml/min]'); when units
    differ, a third column '<name> (unit)' holds them. Should a result hold a name twice,
    the scouting value wins (it overrides the method value).
    """
    import pandas as pd
    v = variables[variables["kind"].isin(kinds)] if kinds else variables
    out = catalog.copy()
    if v.empty:
        return out
    if {"path", "member"} <= set(v.columns) and {"path", "member"} <= set(out.columns):
        keys = ["path", "member"]  # file rows (pycorn.file_catalog)
    else:
        # rows match on (source, database, result_id) when both tables have them
        keys = [k for k in RESULT_KEY if k in v.columns and k in out.columns
                and (k == "result_id" or (out[k].notna().all() and v[k].notna().all()))]
    v = v.assign(_s=v["scouting"].astype(int), **{k: v[k].astype(object) for k in keys if k != "result_id"})
    v = v.sort_values(keys + ["_s"], kind="stable").drop_duplicates(keys + ["variable"], keep="last")
    order = list(dict.fromkeys(variables.loc[v.index.sort_values(), "variable"]))
    if keys == ["result_id"]:
        index = pd.Index(out["result_id"])
    else:
        index = pd.MultiIndex.from_frame(out[keys].astype({k: object for k in keys if k != "result_id"}))
    columns = {}
    for name in order:
        rows = v[v["variable"] == name].set_index(keys)
        units = {u for u in rows["unit"].astype(object) if u not in (None, "") and not (isinstance(u, float) and math.isnan(u))}
        label = "%s [%s]" % (name, next(iter(units))) if len(units) == 1 else str(name)
        columns[label] = rows["value"].astype(object).reindex(index).values
        columns[label + " (number)"] = rows["number"].astype(float).reindex(index).values
        if len(units) > 1:
            columns[label + " (unit)"] = rows["unit"].astype(object).reindex(index).values
    return pd.concat([out.reset_index(drop=True), pd.DataFrame(columns)], axis=1).set_index(out.index)


# ---------------------------------------------------------------------------
# Pick lists
# ---------------------------------------------------------------------------

def pick_list(cursor, field, limit=1000, variable=None, curve_kinds=None, columns=None, label=None):
    """
    The values filter `field` can take (see `PICK_LISTS`), with `n_results`, most common
    first, at most `limit` rows (a warning says when there are more). Grouped on the
    server: only the list itself comes back.

    'folder' gives paths; 'variable' gives name, unit and counts; 'variable_value' the
    stored values of one `variable`; 'curve_role' the roles some original curve has (no
    counts: that would need every curve row per result).
    """
    import pandas as pd
    if field not in PICK_LISTS:
        raise ValueError("unknown pick list %r; choose from %s" % (field, ", ".join(PICK_LISTS)))
    if field == "curve_role":
        return pd.DataFrame({"value": roles_present(curve_kinds or [])})
    empty = {"folder": ["folder_id", "value", "n_results"], "variable": ["value", "unit", "n_results", "n_numeric"],
             "variable_value": ["value", "unit", "n_results"]}.get(field, ["value", "n_results"])
    needs = {"folder": _r("FolderID"), "column": _RUN_INFO, "variable": _CRITERIA,
             "variable_value": _CRITERIA}.get(field) or _r(_RESULT_TEXT[field][2:])
    if not _has(columns, needs):
        _warn_missing(label, "pick_list(%r)" % field, columns, needs, "has no values there")
        return pd.DataFrame(columns=empty)
    # without a unit column: unit NULL, grouped by name/value alone (T-SQL can't GROUP BY a constant)
    has_unit = _has(columns, _UNIT_COLUMN)
    unit = "sc.ExtraDisplayInformation" if has_unit else "CAST(NULL AS nvarchar(50))"
    by_unit = ", sc.ExtraDisplayInformation" if has_unit else ""
    params = _Params()
    top = params.add(int(limit) + 1)
    kinds = ", ".join(params.add(k) for k in VARIABLE_KINDS)
    if field in _RESULT_TEXT:
        sql = ("SELECT TOP (%s) %s AS value, COUNT(*) AS n_results FROM dbo.[Result] r GROUP BY %s "
               "ORDER BY COUNT(*) DESC, %s" % (top, _RESULT_TEXT[field], _RESULT_TEXT[field], _RESULT_TEXT[field]))
    elif field == "folder":
        sql = ("SELECT TOP (%s) r.FolderID AS value, COUNT(*) AS n_results FROM dbo.[Result] r GROUP BY r.FolderID "
               "ORDER BY COUNT(*) DESC, r.FolderID" % top)
    elif field == "column":
        name = _COLUMN_NAME_SQL.replace("colinfo.stored", "ri.RunInformation")
        sql = ("SELECT TOP (%s) %s AS value, COUNT(*) AS n_results FROM dbo.ResultRunInformation ri "
               "WHERE ri.RunInformationTypeName = 'ColumnInformation' GROUP BY %s ORDER BY COUNT(*) DESC, %s"
               % (top, name, name, name))
    elif field == "variable":
        # both counts are results: one result can store a variable twice (method and scouting value)
        sql = ("SELECT TOP (%s) sc.KeyWord1 AS value, %s AS unit, COUNT(DISTINCT sc.ResultID) AS n_results, "
               "COUNT(DISTINCT CASE WHEN %s IS NOT NULL THEN sc.ResultID END) AS n_numeric "
               "FROM dbo.ResultSearchCriteria sc WHERE sc.SearchCriteriaName IN (%s) "
               "GROUP BY sc.KeyWord1%s ORDER BY COUNT(DISTINCT sc.ResultID) DESC, sc.KeyWord1"
               % (top, unit, NUMBER_SQL, kinds, by_unit))
    else:  # variable_value
        if variable is None:
            raise ValueError("pick_list('variable_value') needs variable=<name>")
        sql = ("SELECT TOP (%s) sc.KeyWord2 AS value, %s AS unit, COUNT(DISTINCT sc.ResultID) AS n_results "
               "FROM dbo.ResultSearchCriteria sc WHERE sc.SearchCriteriaName IN (%s) AND sc.KeyWord1 = %s "
               "GROUP BY sc.KeyWord2%s ORDER BY COUNT(DISTINCT sc.ResultID) DESC, sc.KeyWord2"
               % (top, unit, kinds, params.add(variable), by_unit))
    cursor.execute(sql, params)
    names = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    if len(rows) > int(limit):
        warnings.warn("pick_list(%r): more than %d values; showing the %d most common" % (field, limit, limit),
                      stacklevel=3)
        rows = rows[:int(limit)]
    frame = pd.DataFrame([list(r) for r in rows], columns=names)
    if field == "folder":
        paths = folder_paths(cursor, frame["value"], columns)
        frame.insert(0, "folder_id", frame["value"])
        frame["value"] = [paths.get(f) for f in frame["folder_id"]]
    elif field == "column":
        frame["value"] = [_column_name(v) for v in frame["value"]]
    return frame


def merge_pick_lists(frames, field, limit=1000, by_database=False):
    """
    One pick list from several databases: `frames` is [(source, database, frame from
    pick_list)]. Merged (default, owner 2026-11-08): counts summed per value (folders by
    path, as folder IDs differ between databases), most common first, at most `limit`
    rows. `by_database=True`: the frames one under the other, with 'source' and
    'database' columns first. Each database contributes at most `limit` values (it warns
    when it has more), so merged counts cover those values.
    """
    import pandas as pd
    if by_database:
        parts = [f.assign(source=s, database=d) for s, d, f in frames]
        if not parts:
            return pd.DataFrame(columns=["source", "database", "value"])
        out = pd.concat(parts, ignore_index=True)
        return out[["source", "database"] + [c for c in out.columns if c not in ("source", "database")]]
    if field == "curve_role":
        present = {v for _, _, f in frames for v in f["value"]}
        return pd.DataFrame({"value": [r for r in ROLES if r in present]})
    parts = [f.drop(columns=["folder_id"], errors="ignore") for _, _, f in frames if len(f)]
    if not parts:
        return frames[0][2].drop(columns=["folder_id"], errors="ignore") if frames else pd.DataFrame(columns=["value"])
    data = pd.concat(parts, ignore_index=True)
    keys = [c for c in ("value", "unit") if c in data.columns]
    counts = [c for c in data.columns if c not in keys]
    out = data.groupby(keys, dropna=False, sort=False)[counts].sum().reset_index()
    out = out.assign(_v=out["value"].astype(str)).sort_values(["n_results", "_v"], ascending=[False, True], kind="stable")
    out = out.drop(columns="_v").reset_index(drop=True)
    if len(out) > int(limit):
        warnings.warn("pick_list(%r): more than %d values; showing the %d most common" % (field, limit, limit),
                      stacklevel=3)
        out = out.iloc[:int(limit)]
    return out
