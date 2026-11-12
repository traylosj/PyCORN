# -*- coding: utf-8 -*-
"""
Database sources: which servers and databases `PyCORNDatabase` reads, from `.env`.

A **source** is one server + login, named by a short alias. `.env` is the source of truth
(credentials never come from code); arguments to the search functions can narrow the
scope, or widen it to every UNICORN database a listed login can see (`sources="discover"`).

    # Today's single connection: the source named "default"
    UNICORN_SERVER=
    UNICORN_PORT=
    UNICORN_DATABASE=          one database, or
    UNICORN_DATABASES=         a comma list, or * (every UNICORN database the login can see)
    UNICORN_USER=
    UNICORN_PASSWORD=

    # Optional: more sources, searched in this order ('default' = the variables above)
    UNICORN_SOURCES=default,lab2
    UNICORN_LAB2_SERVER=
    UNICORN_LAB2_PORT=
    UNICORN_LAB2_DATABASES=
    UNICORN_LAB2_USER=
    UNICORN_LAB2_PASSWORD=

Without `UNICORN_SOURCES` there is one source, "default", exactly as before. A source with
no database connects to the login's default database.

Output from this module and the connector names a source by its alias, and a database by
its position in that source's list (`'lab2#2'`), never by server or database name (owner,
2026-11-08). Database names appear only in returned data (catalog `database` column).
"""
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

DEFAULT_SOURCE = "default"
DISCOVER = "discover"  # sources="discover": every UNICORN database on each listed source
ALL_DATABASES = "*"
SOURCES_VAR = "UNICORN_SOURCES"
_ALIAS = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

# A database counts as UNICORN when it has these tables (plan section 3)
UNICORN_TABLES = ("Result", "Chromatogram", "Curve", "CurvePoint")


def env_prefix(alias: str) -> str:
    """'UNICORN_' for the default source, 'UNICORN_<ALIAS>_' for the others."""
    return "UNICORN_" if alias.lower() == DEFAULT_SOURCE else "UNICORN_%s_" % alias.upper()


@dataclass
class Source:
    """One server + login, and the databases to search there (names, '*' or None = login default)."""
    alias: str
    params: object  # db_connector.ConnectionParams (database = the one to connect to first)
    databases: Union[List[str], str, None] = None

    def __str__(self) -> str:
        if self.databases == ALL_DATABASES:
            dbs = "all UNICORN databases"
        elif self.databases:
            dbs = "%d listed" % len(self.databases)
        else:
            dbs = "login default"
        return "Source(%r: %s; %s)" % (self.alias, dbs, str(self.params).replace("ConnectionParams", "").strip("()"))

    __repr__ = __str__


def _database_list(plural: Optional[str], singular: Optional[str]):
    """Names from <prefix>DATABASE then <prefix>DATABASES (deduplicated), '*', or None."""
    names = []
    for text in (singular, plural):
        for name in (text or "").split(","):
            name = name.strip()
            if name == ALL_DATABASES:
                return ALL_DATABASES
            if name and name.lower() not in {n.lower() for n in names}:
                names.append(name)
    return names or None


def sources_from_env(env: Dict[str, str], params_class, port_parser) -> "OrderedDict[str, Source]":
    """
    Sources, in search order, from `.env` variables (`env`: upper-case keys). Raises
    ValueError naming the variable (never its value) when a source has no server.
    """
    listed = [a.strip() for a in (env.get(SOURCES_VAR) or "").split(",") if a.strip()]
    sources = OrderedDict()
    for alias in listed or [DEFAULT_SOURCE]:
        if not _ALIAS.match(alias) or alias.lower() == DISCOVER:
            raise ValueError("%s: %r is not a valid source alias (letters, digits and _, starting with a letter; "
                             "not %r)" % (SOURCES_VAR, alias, DISCOVER))
        if alias.lower() in {a.lower() for a in sources}:
            raise ValueError("%s lists %r twice" % (SOURCES_VAR, alias))
        if alias.lower() == DEFAULT_SOURCE:
            alias = DEFAULT_SOURCE
        prefix = env_prefix(alias)
        server = env.get(prefix + "SERVER", "")
        databases = _database_list(env.get(prefix + "DATABASES"), env.get(prefix + "DATABASE"))
        if not server and (listed or databases):
            raise ValueError("Missing %sSERVER (source %r)" % (prefix, alias))
        params = params_class(
            server=server,
            database=databases[0] if isinstance(databases, list) else "",
            user=env.get(prefix + "USER"),
            password=env.get(prefix + "PASSWORD"),
            port=port_parser(env.get(prefix + "PORT"), prefix + "PORT"),
        )
        sources[alias] = Source(alias, params, databases)
    return sources


def plan_scope(sources: "OrderedDict[str, Source]", wanted=None) -> List[tuple]:
    """
    [(alias, databases)] to search, in order, from a `sources=` argument:
    None = everything `.env` lists; 'discover' = every UNICORN database on each listed
    source; an alias or list of aliases = those sources as listed; a dict
    {alias: names, '*' or 'discover'} = those databases (names the login can see, even
    ones `.env` doesn't list). The command line's text forms work too: 'alias:db1,db2' and
    'alias:*', alone or in a list (`--sources`).
    """
    texts = [wanted] if isinstance(wanted, str) else wanted
    if isinstance(texts, (list, tuple)) and any(isinstance(t, str) and ":" in t for t in texts):
        plan_dict = OrderedDict()
        for text in texts:
            alias, sep, names = str(text).partition(":")
            names = names.strip()
            plan_dict[alias.strip()] = (names if names == "*" else [n.strip() for n in names.split(",") if n.strip()]) \
                if sep else None
        wanted = plan_dict

    def known(alias):
        for a in sources:
            if a.lower() == str(alias).lower():
                return a
        raise KeyError("unknown source %r; sources in .env: %s" % (alias, ", ".join(sources)))

    if wanted is None:
        return [(a, s.databases) for a, s in sources.items()]
    if isinstance(wanted, str) and wanted.lower() == DISCOVER:
        return [(a, ALL_DATABASES) for a in sources]
    if isinstance(wanted, str):
        wanted = [wanted]
    if isinstance(wanted, dict):
        plan = []
        for alias, names in wanted.items():
            alias = known(alias)
            if isinstance(names, str) and names.lower() in (DISCOVER, ALL_DATABASES):
                names = ALL_DATABASES
            elif names is None:
                names = sources[alias].databases
            else:
                names = _database_list(",".join([names] if isinstance(names, str) else list(names)), None)
                if not names:
                    raise ValueError("sources: no database given for %r" % alias)
            plan.append((alias, names))
        return plan
    plan = [(known(a), sources[known(a)].databases) for a in wanted]
    if len({a for a, _ in plan}) != len(plan):
        raise ValueError("sources: a source is listed twice")
    return plan


# ---------------------------------------------------------------------------
# On the server
# ---------------------------------------------------------------------------

def list_databases(cursor) -> List[tuple]:
    """
    [(name, is_unicorn)] for every online user database the login can open, by name.
    One query; `is_unicorn` = it has all of `UNICORN_TABLES`.
    """
    has = " AND ".join("OBJECT_ID(QUOTENAME(d.name) + '.dbo.%s', 'U') IS NOT NULL" % t for t in UNICORN_TABLES)
    cursor.execute(
        "SELECT d.name, CASE WHEN %s THEN 1 ELSE 0 END FROM sys.databases d "
        "WHERE d.database_id > 4 AND d.state_desc = 'ONLINE' AND HAS_DBACCESS(d.name) = 1 "
        "ORDER BY d.name" % has)
    return [(name, bool(flag)) for name, flag in cursor.fetchall()]


def resolve_databases(available: List[tuple], wanted) -> List[tuple]:
    """
    [(number, name)]: the server's spelling of each wanted database ('*' = every UNICORN
    one, by name), checked against `available` (from `list_databases`). `number` is the
    position in the wanted list (1-based); `name` is None when the database is not there
    (or the login cannot open it). `wanted` None = [(1, None)]: stay in the login's default.
    """
    if wanted == ALL_DATABASES:
        return list(enumerate([name for name, unicorn in available if unicorn], 1))
    by_lower = {name.lower(): name for name, _ in available}
    return [(number, by_lower.get(name.lower())) for number, name in enumerate(wanted, 1)]


def use_database(cursor, name: str) -> None:
    """Switch the connection to `name` (already checked by `resolve_databases`)."""
    cursor.execute("USE [%s]" % name.replace("]", "]]"))


@dataclass
class SchemaProfile:
    """What one database offers: dbo table -> column names, and its compatibility level."""
    database: str
    compatibility_level: Optional[int]
    columns: Dict[str, set] = field(default_factory=dict)

    def missing(self, needed: Dict[str, tuple]) -> List[str]:
        """'Table.Column' for each needed column the database lacks."""
        out = []
        for table, columns in needed.items():
            have = {c.lower() for c in self.columns.get(table, ())}
            out += ["%s.%s" % (table, c) for c in columns if c.lower() not in have]
        return out

    def has(self, table: str, *columns: str) -> bool:
        """True when dbo.`table` has every one of `columns` (names compare without case)."""
        if not hasattr(self, "_lower"):
            self._lower = {t.lower(): {c.lower() for c in cs} for t, cs in self.columns.items()}
        have = self._lower.get(table.lower())
        return have is not None and all(c.lower() in have for c in columns)


def read_profile(cursor) -> SchemaProfile:
    """The `SchemaProfile` of the database the cursor is in (two small catalog queries)."""
    cursor.execute("SELECT DB_NAME(), (SELECT compatibility_level FROM sys.databases WHERE name = DB_NAME())")
    database, level = cursor.fetchall()[0]
    cursor.execute("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'dbo'")
    columns = {}
    for table, column in cursor.fetchall():
        columns.setdefault(table, set()).add(column)
    return SchemaProfile(database, level, columns)
