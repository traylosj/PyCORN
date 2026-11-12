# -*- coding: utf-8 -*-
"""
Read one UNICORN result from the SQL Server database into a `pycorn.model.Run`.

Two steps:

1. `collect_result_rows()` gathers every row tied to the result by walking the foreign
   keys declared in the database itself. No table list is hard-coded, so database
   versions with other tables work as long as their foreign keys are declared.
   - 'owned': rows below `dbo.Result` (chromatograms, curves and their point blobs,
     events, peak and pool tables, `*Data` blobs, run information, search criteria, ...).
     Rows are followed only through ownership keys: those in a row's primary key, else its
     mandatory ones. Nullable cross-references don't decide ownership: a chromatogram copied
     from another result (`OriginalResultID` pointing there) still belongs to this one, and
     one copied out of this result into another is left out.
   - 'linked': rows behind a link table that only this result uses, and what hangs below
     them (`ResultDoc_AuditTrail` -> `AuditTrail` -> `AuditTrailEntry`).
   - 'referenced': rows the others point at, and their parents in turn (folder chain,
     lookup tables, report formats, column records, ...). Other results can share them.
     This includes the instrument (`UCSystem`, by the result's `SystemName`, which is not a
     declared foreign key; see IMPLIED_KEYS) and its `InstrumentConfiguration`. The walk
     stops there: the firmware, VID configuration and libraries above it are installed
     software, not result data (TERMINAL_TABLES).
2. `build_run()` turns the rows into the same `Run` that `open_run()` builds from a ZIP
   export. The rows are laid out like the bundle's XML (UNICORN's element names, values
   as text) and go through the ZIP reader's builders. The curve and `*Data` blobs are
   byte-identical to the bundle entries (docs/PHASE0_FINDINGS.md) and go through the
   same decoders. Every row is also kept in `run.tables`.

Only SELECT statements are run.
"""
import datetime
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field

import numpy as np

from ..axes import add_axes
from ..model import DbTable, Run
from .zip_source import (_build_chromatogram, _build_metadata, _evaluation_log_from_trails,
                         _read_entry, _result_from_root, _run_information, _unique_key)

ROOT_TABLE = "dbo.Result"
_ROWVERSION_TYPES = ("timestamp", "rowversion")
_BINARY_TYPES = ("binary", "varbinary", "image") + _ROWVERSION_TYPES
_MAX_PARAMS = 1000  # per statement; SQL Server allows 2100


@dataclass(frozen=True)
class ForeignKey:
    name: str
    child: str  # 'schema.table'
    child_columns: tuple
    parent: str
    parent_columns: tuple


# Links UNICORN uses but does not declare as foreign keys; treated as if declared.
# dbo.Result names its instrument, whose row (unique: SystemName is the primary key) holds the
# computer, system type and InstrumentConfiguration. These are the system's settings now, not
# at run time; the result's own snapshot is in ResultAuditTrail (SystemData, ...). A result
# imported from another database usually names a system this database doesn't have; then
# there is no row to find and nothing is added (normal, so no warning).
IMPLIED_KEYS = (
    ForeignKey("implied:Result.SystemName", "dbo.Result", ("SystemName",), "dbo.UCSystem", ("SystemName",)),
)
# Referenced rows of these tables are kept, but their own parents are not fetched. Above
# InstrumentConfiguration are the instrument's firmware, VID configuration and libraries:
# installed software, not result data (tens of MB when tested against a lab environment).
TERMINAL_TABLES = frozenset(["dbo.InstrumentConfiguration"])


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

@dataclass
class SchemaGraph:
    """Tables, column types, primary keys and foreign keys of one database."""
    columns: dict  # table -> OrderedDict(column -> data type)
    primary_keys: dict  # table -> tuple of columns
    foreign_keys: list
    nullable: dict = field(default_factory=dict)  # table -> set of columns that allow NULL
    _children: dict = field(default_factory=dict, repr=False)
    _parents: dict = field(default_factory=dict, repr=False)

    def __post_init__(self):
        children, parents = defaultdict(list), defaultdict(list)
        for fk in self.foreign_keys:
            children[fk.parent].append(fk)
            parents[fk.child].append(fk)
        self._children, self._parents = dict(children), dict(parents)

    def children(self, table):
        """Foreign keys that point at `table`."""
        return self._children.get(table, [])

    def parents(self, table):
        """Foreign keys of `table`."""
        return self._parents.get(table, [])

    def ownership_keys(self, table, tables):
        """
        The foreign keys of `table` into `tables` that say which rows own its rows: those in its
        primary key (a curve's ChromatogramID), else the mandatory ones (a chromatogram's
        ResultID), else all. Nullable links such as Chromatogram.OriginalResultID or a curve's
        injection reference are cross-references, not ownership.
        """
        fks = [fk for fk in self.parents(table) if fk.parent in tables and fk.parent != table]
        pk = set(self.primary_keys.get(table, ()))
        nullable = self.nullable.get(table, set())
        for rule in (lambda fk: set(fk.child_columns) <= pk,
                     lambda fk: not set(fk.child_columns) & nullable):
            chosen = [fk for fk in fks if rule(fk)]
            if chosen:
                return chosen
        return fks

    def is_link_table(self, table):
        """True if every column (apart from a rowversion) is part of a foreign key, and it has two or more."""
        fks = self.parents(table)
        fk_columns = {c for fk in fks for c in fk.child_columns}
        data_columns = [c for c, t in self.columns.get(table, {}).items() if t not in _ROWVERSION_TYPES]
        return len(fks) >= 2 and bool(data_columns) and set(data_columns) <= fk_columns


def read_schema(cursor):
    """Read tables, columns, primary keys and foreign keys from the system catalog."""
    cursor.execute(
        "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS "
        "ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION")
    columns, nullable = defaultdict(OrderedDict), defaultdict(set)
    for schema, table, column, data_type, is_nullable in cursor.fetchall():
        columns["%s.%s" % (schema, table)][column] = data_type.lower()
        if is_nullable == "YES":
            nullable["%s.%s" % (schema, table)].add(column)

    cursor.execute(
        "SELECT s.name, t.name, c.name FROM sys.indexes i "
        "JOIN sys.tables t ON t.object_id = i.object_id "
        "JOIN sys.schemas s ON s.schema_id = t.schema_id "
        "JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
        "JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "WHERE i.is_primary_key = 1 ORDER BY s.name, t.name, ic.key_ordinal")
    primary_keys = defaultdict(tuple)
    for schema, table, column in cursor.fetchall():
        primary_keys["%s.%s" % (schema, table)] += (column,)

    cursor.execute(
        "SELECT fk.name, cs.name, ct.name, cc.name, ps.name, pt.name, pc.name "
        "FROM sys.foreign_keys fk "
        "JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id "
        "JOIN sys.tables ct ON ct.object_id = fkc.parent_object_id "
        "JOIN sys.schemas cs ON cs.schema_id = ct.schema_id "
        "JOIN sys.columns cc ON cc.object_id = fkc.parent_object_id AND cc.column_id = fkc.parent_column_id "
        "JOIN sys.tables pt ON pt.object_id = fkc.referenced_object_id "
        "JOIN sys.schemas ps ON ps.schema_id = pt.schema_id "
        "JOIN sys.columns pc ON pc.object_id = fkc.referenced_object_id AND pc.column_id = fkc.referenced_column_id "
        "ORDER BY cs.name, ct.name, fk.name, fkc.constraint_column_id")
    parts = OrderedDict()
    for name, cschema, ctable, ccol, pschema, ptable, pcol in cursor.fetchall():
        child, parent = "%s.%s" % (cschema, ctable), "%s.%s" % (pschema, ptable)
        fk = parts.setdefault((child, name), [child, parent, [], []])
        fk[2].append(ccol)
        fk[3].append(pcol)
    foreign_keys = [ForeignKey(name, child, tuple(cc), parent, tuple(pc))
                    for (_, name), (child, parent, cc, pc) in parts.items()]
    return SchemaGraph(dict(columns), dict(primary_keys), _with_implied_keys(columns, primary_keys, foreign_keys),
                       dict(nullable))


def _with_implied_keys(columns, primary_keys, foreign_keys):
    """
    `foreign_keys` plus the IMPLIED_KEYS this database has: both tables and columns exist,
    the parent columns are its primary key (so they match at most one row), and no declared
    foreign key already links them.
    """
    out = list(foreign_keys)
    declared = {(fk.child, fk.child_columns, fk.parent) for fk in foreign_keys}
    for fk in IMPLIED_KEYS:
        if (fk.child, fk.child_columns, fk.parent) in declared:
            continue
        if not (set(fk.child_columns) <= set(columns.get(fk.child, ()))
                and tuple(primary_keys.get(fk.parent, ())) == fk.parent_columns):
            continue
        out.append(fk)
    return out


# ---------------------------------------------------------------------------
# Fetching rows
# ---------------------------------------------------------------------------

def _quote(name):
    return "[%s]" % name.replace("]", "]]")


def _qualified(table):
    schema, name = table.split(".", 1)
    return "%s.%s" % (_quote(schema), _quote(name))


class SqlFetcher:
    """Runs the parameterized SELECTs the walker needs on a DB-API cursor (pymssql)."""

    def __init__(self, cursor, schema):
        self.cursor = cursor
        self.schema = schema

    def rows(self, table, columns, keys):
        """Rows of `table` whose `columns` equal one of the `keys` (tuples), in primary-key order."""
        keys = list(keys)
        if not keys:
            return []
        names = list(self.schema.columns[table])
        select = "SELECT %s FROM %s WHERE " % (", ".join(_quote(c) for c in names), _qualified(table))
        order = self.schema.primary_keys.get(table)
        order_by = " ORDER BY " + ", ".join(_quote(c) for c in order) if order else ""
        batch = max(1, _MAX_PARAMS // len(columns))
        out = []
        for start in range(0, len(keys), batch):
            chunk = keys[start:start + batch]
            if len(columns) == 1:
                where = "%s IN (%s)" % (_quote(columns[0]), ", ".join(["%s"] * len(chunk)))
            else:
                one = "(" + " AND ".join("%s = %%s" % _quote(c) for c in columns) + ")"
                where = " OR ".join([one] * len(chunk))
            self.cursor.execute(select + where + order_by, tuple(v for key in chunk for v in key))
            out.extend(OrderedDict(zip(names, row)) for row in self.cursor.fetchall())
        return out

    def count(self, table, columns, key):
        """Number of rows of `table` whose `columns` equal `key`."""
        where = " AND ".join("%s = %%s" % _quote(c) for c in columns)
        self.cursor.execute("SELECT COUNT(*) FROM %s WHERE %s" % (_qualified(table), where), tuple(key))
        return self.cursor.fetchone()[0]


def _norm(value):
    # SQL Server's default collations compare text case-insensitively, ignoring trailing spaces
    return value.rstrip().casefold() if isinstance(value, str) else value


def _key(row, columns):
    return tuple(_norm(row[c]) for c in columns)


def _keys(rows, columns):
    """Distinct keys (as stored, first spelling kept) of `rows` with no NULL part."""
    seen = OrderedDict()
    for row in rows:
        if all(row[c] is not None for c in columns):
            seen.setdefault(_key(row, columns), tuple(row[c] for c in columns))
    return list(seen.values())


def _identity(schema, table, row):
    pk = schema.primary_keys.get(table)
    if pk:
        return _key(row, pk)
    return tuple(_norm(v) for v in row.values())


def _closure_down(schema, root, skip):
    """Tables reachable from `root` through foreign keys pointing at it, in walking order."""
    order, seen = [root], {root} | set(skip)
    for table in order:
        for fk in schema.children(table):
            if fk.child not in seen:
                seen.add(fk.child)
                order.append(fk.child)
    return order


def _topological(schema, tables):
    """`tables` ordered parents first (self-references ignored; cycles keep walking order)."""
    members = set(tables)
    pending = {t: {fk.parent for fk in schema.parents(t) if fk.parent in members and fk.parent != t}
               for t in tables}
    order = []
    while pending:
        ready = [t for t in tables if t in pending and not pending[t]]
        if not ready:  # a cycle: take the next table in walking order
            ready = [next(t for t in tables if t in pending)]
        for t in ready:
            order.append(t)
            del pending[t]
        for deps in pending.values():
            deps.difference_update(ready)
    return order


def _walk_down(fetcher, schema, root, root_rows, skip=()):
    """
    Rows below `root_rows`: for each table under `root`, the rows reached through its
    ownership keys (`SchemaGraph.ownership_keys`) from rows already kept, and kept only if
    every ownership key points at a kept row (or is NULL). A chromatogram of this result that
    came from another one (OriginalResultID) is kept; one copied out of it into another result
    is not.
    """
    tables = _closure_down(schema, root, skip)
    kept = OrderedDict([(root, list(root_rows))])
    for table in _topological(schema, tables)[1:]:
        fks = [fk for fk in schema.ownership_keys(table, tables) if fk.parent in kept]
        candidates = OrderedDict()
        for fk in fks:
            for row in fetcher.rows(table, fk.child_columns, _keys(kept[fk.parent], fk.parent_columns)):
                candidates.setdefault(_identity(schema, table, row), row)
        targets = {fk: {_key(r, fk.parent_columns) for r in kept[fk.parent]} for fk in fks}
        rows = [row for row in candidates.values()
                if all(any(row[c] is None for c in fk.child_columns) or _key(row, fk.child_columns) in targets[fk]
                       for fk in fks)]
        kept[table] = rows
    return OrderedDict((t, r) for t, r in kept.items() if r), set(tables)


def collect_result_rows(fetcher, schema, result_id, root=ROOT_TABLE, terminal=TERMINAL_TABLES):
    """
    Every row tied to one result: OrderedDict of table name -> `DbTable`.

    `fetcher` runs the queries (`SqlFetcher` on a database cursor). See the module
    docstring for what 'owned', 'linked' and 'referenced' mean. Referenced rows of
    `terminal` tables are kept without fetching their parents.
    """
    pk = schema.primary_keys.get(root)
    if not pk or len(pk) != 1:
        raise ValueError("%s has no single-column primary key; cannot select a result" % root)
    result_rows = fetcher.rows(root, pk, [(result_id,)])
    if not result_rows:
        raise LookupError("no row in %s with %s = %r" % (root, pk[0], result_id))

    owned, walked = _walk_down(fetcher, schema, root, result_rows)

    # rows owned through a link table that only this result uses
    linked = OrderedDict()
    for table, rows in owned.items():
        if not schema.is_link_table(table):
            continue
        for fk in schema.parents(table):
            if fk.parent in walked:
                continue
            exclusive = []
            for key in _keys(rows, fk.child_columns):
                mine = sum(1 for r in rows if _key(r, fk.child_columns) == tuple(_norm(v) for v in key))
                if fetcher.count(table, fk.child_columns, key) == mine:
                    exclusive.append(key)
            parent_rows = fetcher.rows(fk.parent, fk.parent_columns, exclusive)
            if not parent_rows:
                continue
            below, tables = _walk_down(fetcher, schema, fk.parent, parent_rows, skip=walked)
            walked |= tables
            for t, r in below.items():
                _merge(schema, linked, t, r)

    # rows pointed at from the rows above, and their parents in turn
    referenced = OrderedDict()
    queue = list(owned.items()) + list(linked.items())
    while queue:
        table, rows = queue.pop(0)
        if table in terminal:
            continue
        for fk in schema.parents(table):
            if fk.parent in walked:
                continue
            have = {_key(r, fk.parent_columns) for r in referenced.get(fk.parent, [])}
            keys = [k for k in _keys(rows, fk.child_columns) if tuple(_norm(v) for v in k) not in have]
            new = _merge(schema, referenced, fk.parent, fetcher.rows(fk.parent, fk.parent_columns, keys))
            if new:
                queue.append((fk.parent, new))

    out = OrderedDict()
    for relation, group in (("owned", owned), ("linked", linked), ("referenced", referenced)):
        for table, rows in group.items():
            name = table if table not in out else "%s [%s]" % (table, relation)
            out[name] = DbTable(table, relation, OrderedDict(schema.columns.get(table, {})), rows)
    return out


def _merge(schema, group, table, rows):
    """Add `rows` to `group[table]`, skipping ones already there; return the new rows."""
    existing = group.setdefault(table, [])
    seen = {_identity(schema, table, r) for r in existing}
    new = []
    for row in rows:
        ident = _identity(schema, table, row)
        if ident not in seen:
            seen.add(ident)
            new.append(row)
    existing.extend(new)
    if not existing:
        del group[table]
    return new


# ---------------------------------------------------------------------------
# Rows -> bundle-shaped XML dicts
# ---------------------------------------------------------------------------

def _float32_text(value):
    """Shortest text that reads back as the same float32, as UNICORN writes these values."""
    text = str(np.float32(value))
    return text[:-2] if text.endswith(".0") else text


def _text(value, data_type):
    """A column value as UNICORN's XML writes it ('' becomes None, as an empty element does)."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        # real columns hold float32; some float columns (ColumnVolume) hold widened float32 values
        if data_type == "real" or (np.isfinite(value) and float(np.float32(value)) == value):
            return _float32_text(value)
        return repr(value)
    if isinstance(value, datetime.datetime):
        text = value.replace(tzinfo=None).isoformat()
        return text.rstrip("0") if "." in text else text
    if isinstance(value, str):
        # as the ZIP reader gets element text: line ends normalized, outer whitespace stripped
        # (by xmltodict); `run.tables` keeps the stored text
        value = value.replace("\r\n", "\n").replace("\r", "\n").strip()
        return value if value != "" else None
    return str(value)


def _element(tag, items):
    """A collection element as xmltodict reads it: None if empty, a dict for one child, else a list."""
    if not items:
        return None
    return OrderedDict([(tag, items[0] if len(items) == 1 else items)])


def _xml_fields(row, columns, rename=None, skip=()):
    """
    One row as an XML element's children: NULL columns are left out (as UNICORN's XML
    leaves out null fields), datetimeoffset values add `<name>UtcOffsetMinutes`, and
    blobs and rowversions are left to `run.tables`.
    """
    rename = rename or {}
    out = OrderedDict()
    for column, value in row.items():
        data_type = columns.get(column, "")
        if column in skip or value is None or data_type in _BINARY_TYPES:
            continue
        name = rename.get(column, column)
        out[name] = _text(value, data_type)
        if isinstance(value, datetime.datetime) and value.utcoffset() is not None:
            out[name + "UtcOffsetMinutes"] = str(int(value.utcoffset().total_seconds() // 60))
    return out


class _Rows:
    """Read access to collected tables by name, grouped by key columns."""

    def __init__(self, tables):
        self.tables = tables

    def all(self, table):
        t = self.tables.get(table)
        return t.rows if t is not None else []

    def columns(self, table):
        t = self.tables.get(table)
        return t.columns if t is not None else {}

    def where(self, table, sort=None, **match):
        rows = [r for r in self.all(table) if all(r.get(k) == v for k, v in match.items())]
        return sorted(rows, key=lambda r: r.get(sort)) if sort else rows


def _number_ref(pos):
    return OrderedDict([("CurveNumber", str(pos))]) if pos is not None else None


def _curve_xml(row, chrom_name, chrom_names, rows, entries, warnings):
    chrom_id, pos = row["ChromatogramID"], row["ChromatogramPos"]
    xml = OrderedDict()
    xml["@CurveDataType"] = row.get("CurveDataTypeName")
    xml.update(_xml_fields(row, rows.columns("dbo.Curve"),
                           rename={"Description": "Name", "IsoCroneTypeName": "IsoChroneType",
                                   "ChromatogramPos": "CurveNumber"},
                           skip=("ChromatogramID", "CurveDataTypeName", "InjectionEventCurveChromID",
                                 "InjectionEventCurveChromPos")))
    injection = _number_ref(row.get("InjectionEventCurveChromPos"))
    if injection is not None:
        xml["InjectionEventCurve"] = injection

    points = []
    for cp in rows.where("dbo.CurvePoint", ChromatogramID=chrom_id, ChromatogramPos=pos):
        file_name = "%s_%s_%s" % (chrom_name, pos, bool(cp.get("IsFullResolution")))
        while file_name in entries:
            file_name += "+"
        blob = cp.get("BinaryCurvePoints")
        if blob is None:
            warnings.append("%s/%s: point set has no BinaryCurvePoints" % (chrom_name, row.get("Description")))
        else:
            entries[file_name] = _read_entry(file_name, bytes(blob), warnings)
        point = _xml_fields(cp, rows.columns("dbo.CurvePoint"), skip=("ChromatogramID", "ChromatogramPos"))
        point["BinaryCurvePointsFileName"] = file_name
        points.append(point)
    xml["CurvePoints"] = _element("CurvePoint", points)

    keys = ("ChromatogramID", "ChromatogramPos")
    xml["GuidePoints"] = _element("GuidePoint", [
        _xml_fields(r, rows.columns("dbo.GuidePoint"), skip=keys + ("CurvePos",))
        for r in rows.where("dbo.GuidePoint", sort="CurvePos", ChromatogramID=chrom_id, ChromatogramPos=pos)])
    for r in rows.where("dbo.CurveUVInfo", ChromatogramID=chrom_id, ChromatogramPos=pos):
        xml["CurveUVInfo"] = _xml_fields(r, rows.columns("dbo.CurveUVInfo"), skip=keys)
    for r in rows.where("dbo.DerivedCurveOrigin", ChromatogramID=chrom_id, ChromatogramPos=pos):
        origin = OrderedDict()
        for prefix in ("First", "Second"):
            ref_id, ref_pos = r.get(prefix + "CurveChromID"), r.get(prefix + "CurveChromPos")
            if ref_pos is not None:
                origin[prefix + "Curve"] = OrderedDict([("ChromatogramName", chrom_names.get(ref_id, str(ref_id))),
                                                        ("CurveNumber", str(ref_pos))])
        origin.update(_xml_fields(r, rows.columns("dbo.DerivedCurveOrigin"),
                                  rename={"CurveOperationName": "CurveOperation"},
                                  skip=keys + ("FirstCurveChromID", "FirstCurveChromPos",
                                               "SecondCurveChromID", "SecondCurveChromPos")))
        xml["DerivedCurveOrigin"] = origin
    return xml


def _event_curve_xml(row, rows):
    chrom_id, pos = row["ChromatogramID"], row["ChromatogramPos"]
    xml = OrderedDict()
    xml["@EventCurveType"] = row.get("EventCurveTypeName")
    xml.update(_xml_fields(row, rows.columns("dbo.EventCurve"),
                           rename={"Description": "Name", "ChromatogramPos": "CurveNumber"},
                           skip=("ChromatogramID", "EventCurveTypeName", "InjectionEventCurveChromID",
                                 "InjectionEventCurveChromPos")))
    injection = _number_ref(row.get("InjectionEventCurveChromPos"))
    if injection is not None:
        xml["InjectionEventCurve"] = injection
    events = []
    for e in rows.where("dbo.Event", sort="EventCurvePos", ChromatogramID=chrom_id, ChromatogramPos=pos):
        event = OrderedDict([("@EventType", e.get("EventTypeName")), ("@EventSubType", e.get("EventSubTypeName"))])
        event.update(_xml_fields(e, rows.columns("dbo.Event"),
                                 skip=("ChromatogramID", "ChromatogramPos", "EventTypeName", "EventSubTypeName")))
        meta = rows.where("dbo.EventMetaData", ChromatogramID=chrom_id, ChromatogramPos=pos,
                          EventCurvePos=e["EventCurvePos"])
        if meta:
            event["EventMetaData"] = _element("MetaData", [
                _xml_fields(m, rows.columns("dbo.EventMetaData"),
                            skip=("ChromatogramID", "ChromatogramPos", "EventCurvePos")) for m in meta])
        events.append(event)
    xml["Events"] = _element("Event", events)
    return xml


def _lookup(rows, table, key_column, value, name_columns):
    """The name of a lookup row (e.g. ResolutionAlgorithm 3 -> 'Algorithm3'), else None."""
    for r in rows.where(table, **{key_column: value}):
        for column in name_columns:
            if r.get(column) not in (None, ""):
                return r[column]
    return None


def _peak_table_xml(row, rows):
    chrom_id, pos = row["ChromatogramID"], row["ChromatogramPos"]
    keys = ("ChromatogramID", "ChromatogramPos")
    xml = OrderedDict()
    for column, element in (("DataCurveChromPos", "DataCurve"), ("BaseLineCurveChromPos", "BaseLine")):
        ref = _number_ref(row.get(column))
        if ref is not None:
            xml[element] = ref
    xml.update(_xml_fields(row, rows.columns("dbo.PeakTable"),
                           rename={"Description": "Name", "CalculationRetentionTypeName": "CalculationRetention",
                                   "ChromatogramPos": "CurveNumber"},
                           skip=("ChromatogramID", "DataCurveChromID", "DataCurveChromPos",
                                 "BaseLineCurveChromID", "BaseLineCurveChromPos")))
    algorithm = row.get("ResolutionAlgorithmID")
    if algorithm is not None:
        name = _lookup(rows, "dbo.ResolutionAlgorithm", "ResolutionAlgorithmID", algorithm,
                       ("ResolutionAlgorithmName", "Name", "Description"))
        xml["ResolutionAlgorithm"] = name if name is not None else str(algorithm)
    units = rows.where("dbo.PeakUnit", ChromatogramID=chrom_id, ChromatogramPos=pos)
    if units:
        xml["PeakUnit"] = _xml_fields(units[0], rows.columns("dbo.PeakUnit"), skip=keys)
    peaks = [_xml_fields(p, rows.columns("dbo.Peak"),
                         rename={"Description": "Name", "StartPeakLimitTypeName": "StartPeakLimitType",
                                 "EndPeakLimitTypeName": "EndPeakLimitType",
                                 "StartPeakEndPointHeight": "StartPeakEndpointHeight",
                                 "EndPeakEndPointHeight": "EndPeakEndpointHeight"},
                         skip=keys)
             for p in rows.where("dbo.Peak", sort="PeakTablePos", ChromatogramID=chrom_id, ChromatogramPos=pos)]
    xml["Peaks"] = _element("Peak", peaks)
    return xml


def _pool_tables_xml(chrom_id, rows):
    tables = []
    keys = ("ChromatogramID", "ChromatogramPos")
    for row in rows.where("dbo.PoolTable", sort="ChromatogramPos", ChromatogramID=chrom_id):
        pos = row["ChromatogramPos"]
        xml = _xml_fields(row, rows.columns("dbo.PoolTable"),
                          rename={"Description": "Name", "ChromatogramPos": "CurveNumber"}, skip=("ChromatogramID",))
        units = rows.where("dbo.PoolUnit", ChromatogramID=chrom_id, ChromatogramPos=pos)
        if units:
            xml["PoolUnit"] = _xml_fields(units[0], rows.columns("dbo.PoolUnit"), skip=keys)
        pools = []
        for p in rows.where("dbo.Pool", sort="PoolTablePos", ChromatogramID=chrom_id, ChromatogramPos=pos):
            pool = _xml_fields(p, rows.columns("dbo.Pool"), skip=keys)
            events = [_xml_fields(e, rows.columns("dbo.Pool_Event"), skip=("ChromatogramID",))
                      for e in rows.where("dbo.Pool_Event", ChromatogramID=chrom_id, PoolChromatogramPos=pos,
                                          PoolTablePos=p.get("PoolTablePos"))]
            if events:
                pool["Events"] = _element("Event", events)
            pools.append(pool)
        if pools:
            xml["Pools"] = _element("Pool", pools)
        tables.append(xml)
    return _element("PoolTable", tables)


def _chromatogram_xml(row, chrom_names, key, rows, entries, warnings):
    """
    A `Chrom.N.Xml` root element from a dbo.Chromatogram row and the rows below it. `key`
    (the name, unless another chromatogram of the result has it) prefixes the point entries.
    """
    chrom_id = row["ChromatogramID"]
    name = chrom_names[chrom_id]
    root = OrderedDict([("ChromatogramName", name)])
    root.update(_xml_fields(row, rows.columns("dbo.Chromatogram"),
                            rename={"IsReadOnly": "IsReadonly"}, skip=("ResultID", "Description")))
    # UNICORN writes this element even when it is empty
    root.setdefault("ChromatogramLayoutXml", None)
    reports = [_xml_fields(r, rows.columns("dbo.InstallationTestReport"), skip=("ChromatogramID",))
               for r in rows.where("dbo.InstallationTestReport", sort="ChromatogramPos", ChromatogramID=chrom_id)]
    if reports:
        root["InstallationTestReports"] = _element("InstallationTestReport", reports)
    root["Curves"] = _element("Curve", [
        _curve_xml(r, key, chrom_names, rows, entries, warnings)
        for r in rows.where("dbo.Curve", sort="ChromatogramPos", ChromatogramID=chrom_id)])
    root["EventCurves"] = _element("EventCurve", [
        _event_curve_xml(r, rows)
        for r in rows.where("dbo.EventCurve", sort="ChromatogramPos", ChromatogramID=chrom_id)])
    root["PeakTables"] = _element("PeakTable", [
        _peak_table_xml(r, rows)
        for r in rows.where("dbo.PeakTable", sort="ChromatogramPos", ChromatogramID=chrom_id)])
    root["PoolTables"] = _pool_tables_xml(chrom_id, rows)
    return root


def _folder_path(rows, folder_id):
    """'/<root>/<sub>/...' from the folder chain, as UNICORN writes FolderPath."""
    names, seen = [], set()
    while folder_id is not None and folder_id not in seen:
        seen.add(folder_id)
        folder = rows.where("dbo.Folder", FolderID=folder_id)
        if not folder:
            return None
        names.append(folder[0].get("Description") or "")
        folder_id = folder[0].get("ParentFolderID")
    return "/" + "/".join(reversed(names))


def _result_root(row, rows, chrom_names):
    result_id = row["ResultID"]
    one = ("ResultID",)
    root = _xml_fields(row, rows.columns("dbo.Result"),
                       rename={"ResultStateName": "ResultState", "Description": "Name",
                               "RunTypeName": "RunType", "FormatVersion": "ResultFormatVersion"})
    path = _folder_path(rows, row.get("FolderID"))
    if path is not None:
        root["FolderPath"] = path
    for table, element in (("dbo.ResultDocumentation", "ResultDocumentation"),
                           ("dbo.ResultAuxiliary", "ResultAuxiliary")):
        found = rows.where(table, ResultID=result_id)
        if found:
            root[element] = _xml_fields(found[0], rows.columns(table), skip=one) or None
    run_information = [
        _xml_fields(r, rows.columns("dbo.ResultRunInformation"),
                    rename={"RunInformationTypeName": "@RunInformationType"}, skip=one)
        for r in rows.where("dbo.ResultRunInformation", ResultID=result_id)]
    root["ResultRunInformation"] = run_information[0] if len(run_information) == 1 else run_information or None
    criteria = [_xml_fields(r, rows.columns("dbo.ResultSearchCriteria"),
                            rename={"SearchCriteriaName": "Name", "KeyWord1": "Keyword1", "KeyWord2": "Keyword2"},
                            skip=one + ("ResultSearchCriteriaID",))
                for r in rows.where("dbo.ResultSearchCriteria", sort="ResultSearchCriteriaID", ResultID=result_id)]
    root["ResultSearchCriterias"] = _element("ResultSearchCriteria", criteria)
    root["Chromatograms"] = _element("Chromatogram", [
        OrderedDict([("ChromatogramFileName", key), ("ChromatogramName", name)]) for key, name in chrom_names])
    return root, run_information


def _evaluation_log_xml(result_id, rows):
    """Audit trails of the result (dbo.ResultDoc_AuditTrail -> AuditTrail -> AuditTrailEntry)."""
    trails = []
    ids = [r["AuditTrailID"] for r in rows.where("dbo.ResultDoc_AuditTrail", sort="AuditTrailID", ResultID=result_id)]
    for trail_id in ids:
        found = rows.where("dbo.AuditTrail", AuditTrailID=trail_id)
        if not found:
            continue
        trail = _xml_fields(found[0], rows.columns("dbo.AuditTrail"))
        trail["AuditTrailEntries"] = _element("AuditTrailEntry", [
            _xml_fields(e, rows.columns("dbo.AuditTrailEntry"))
            for e in rows.where("dbo.AuditTrailEntry", sort="AuditTrailEntryID", AuditTrailID=trail_id)])
        trails.append(trail)
    return trails


def build_run(tables, adjust_axes=True, origin=None, axes_options=None):
    """
    Build a `Run` from the rows `collect_result_rows()` returns, as `open_run()` builds one
    from a ZIP export. `origin` (server, port, database) goes to `run.origin` with the result id.
    `axes_options` are passed to `pycorn.axes.add_axes` (CV and UTC).
    """
    rows = _Rows(tables)
    results = rows.all(ROOT_TABLE)
    if len(results) != 1:
        raise ValueError("expected one %s row, got %d" % (ROOT_TABLE, len(results)))
    result_row = results[0]
    result_id = result_row["ResultID"]
    warnings = []
    entries = OrderedDict()

    chrom_rows = rows.where("dbo.Chromatogram", sort="ChromatogramID", ResultID=result_id)
    # UNICORN exports a chromatogram exactly as named in Description; 'Chrom.N' only if it is empty.
    # Keys of `run.chromatograms` (and the bundle-style entry names) must be unique: a repeated
    # name keeps its exact `Chromatogram.name`, and its key gets the ChromatogramID, with a warning.
    chrom_names, chrom_keys = OrderedDict(), OrderedDict()
    for number, row in enumerate(chrom_rows, 1):
        chrom_id = row["ChromatogramID"]
        chrom_names[chrom_id] = name = row.get("Description") or "Chrom.%d" % number
        chrom_keys[chrom_id] = _unique_key(set(chrom_keys.values()), name, chrom_id, "result", warnings)
    chromatograms = OrderedDict()
    for row in chrom_rows:
        key = chrom_keys[row["ChromatogramID"]]
        root = _chromatogram_xml(row, chrom_names, key, rows, entries, warnings)
        chromatograms[key] = _build_chromatogram(key + ".Xml", root, entries, warnings, adjust_axes)
    used = set(entries)

    # the *Data blobs: byte-identical to the bundle entries of the same name (in bundle order)
    blob_columns = rows.columns("dbo.ResultAuditTrail")
    for row in rows.where("dbo.ResultAuditTrail", ResultID=result_id):
        for column in sorted(row):
            if row[column] is not None and blob_columns.get(column) in ("varbinary", "binary", "image"):
                entries[column] = _read_entry(column, bytes(row[column]), warnings)

    root, items = _result_root(result_row, rows, [(chrom_keys[i], chrom_names[i]) for i in chrom_names])
    # stored decoded in the database (a bundle has it base64-encoded); 'raw' is that text
    run_information = [_run_information(item.get("@RunInformationType"), item.get("RunInformation"),
                                         item.get("RunInformation"), warnings, "ResultRunInformation")
                       for item in items]
    run = Run(
        source="sql",
        locator="sql:ResultID=%s" % result_id,
        result=_result_from_root(root, run_information, warnings, ROOT_TABLE),
        chromatograms=chromatograms,
        metadata=_build_metadata(entries, used, warnings),
        evaluation_log=_evaluation_log_from_trails(_evaluation_log_xml(result_id, rows)),
        manifest=[],
        entries=entries,
        origin=OrderedDict([("server", (origin or {}).get("server")), ("port", (origin or {}).get("port")),
                            ("database", (origin or {}).get("database")), ("result_id", result_id)]),
        tables=tables,
    )
    run.warnings.extend(warnings)
    add_axes(run, **(axes_options or {}))
    return run


def read_result(cursor, result_id, adjust_axes=True, schema=None, server=None, axes_options=None, port=None):
    """
    Load one result (by `dbo.Result.ResultID`) through a DB-API cursor into a `Run`.

    `schema` (from `read_schema(cursor)`) can be passed to skip re-reading the catalog
    when loading several results from the same database. `run.origin` gets `server` (the
    name used to connect; the server's own name, @@SERVERNAME, if not given), `port` (the
    port used to connect, None for the default; several services can share one host),
    the database the cursor is in, and the result id.
    """
    schema = schema or read_schema(cursor)
    cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
    server_name, database = cursor.fetchone()
    tables = collect_result_rows(SqlFetcher(cursor, schema), schema, result_id)
    return build_run(tables, adjust_axes=adjust_axes,
                     origin={"server": server or server_name, "port": port, "database": database},
                     axes_options=axes_options)
