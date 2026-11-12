# -*- coding: utf-8 -*-
"""
PyCORN Database Connector (SQL Server - No ODBC)
Connects to UNICORN 7.11 SQL Server database using pure Python TCP/IP (pymssql).

(c)2026 - traylosj
Based on unicorn_cte.sql in the package's sql folder

This version reads CTE queries from SQL files instead of embedding them,
uses environment variables for credentials, and provides a Pythonic object-oriented interface.
"""

import argparse
import numbers
import os
import re
import warnings
from contextlib import closing
from pathlib import Path
from typing import Optional, Tuple, Any, Dict, List
from dataclasses import dataclass


# Environment variable names (security-first approach)
UNICORN_SERVER_VAR = "UNICORN_SERVER"
UNICORN_DATABASE_VAR = "UNICORN_DATABASE"
UNICORN_USER_VAR = "UNICORN_USER"
UNICORN_PASSWORD_VAR = "UNICORN_PASSWORD"
# Optional: several SQL Server services can share one host, each on its own port
UNICORN_PORT_VAR = "UNICORN_PORT"
PROJECT_ROOT = Path(__file__).parent.parent
# sql/.env of a source checkout (editable install); see find_env_file() for the full lookup
DEFAULT_ENV_FILE = PROJECT_ROOT / "sql" / ".env"
# Optional: path of the .env to read, checked before any other place
ENV_FILE_VAR = "PYCORN_ENV_FILE"
# Used, with a warning, only when nothing names a server and a SQL Server answers here
LOCAL_SERVER = "localhost"
SQL_SERVER_DEFAULT_PORT = 1433
DEFAULT_CTE_FILE = Path(__file__).parent / "sql" / "unicorn_cte.sql"

# Prepended to the CTE so its filter variables are always declared.
# Values are substituted by pymssql (pyformat), which handles quoting.
CTE_PARAM_DECLARATIONS = (
    "DECLARE @FolderFilter VARCHAR(1000) = %(folder_filter)s;\n"
    "DECLARE @DescriptionPattern VARCHAR(200) = %(description_pattern)s;\n"
    "DECLARE @HideHidden BIT = %(hide_hidden)s;\n"
)


def _import_pymssql():
    """pymssql, or ImportError saying how to install it (never installs anything itself)."""
    try:
        import pymssql
        return pymssql
    except ImportError:
        raise ImportError('The database connector needs pymssql: pip install "pycorn[database]" '
                          '(or pip install pymssql)') from None


def _redact(message: Any, *values: Optional[str]) -> str:
    """Replace connection details (server, user, password, ...) in a message."""
    text = str(message)
    for value in values:
        if not value:
            continue
        value = str(value)
        # Also cover the bare host when the server includes a port or instance
        for part in {value, re.split(r"[,:\\]", value)[0]}:
            if part:
                # Whole tokens only, so short values don't mangle ordinary words
                text = re.sub(rf"(?<![\w.-]){re.escape(part)}(?![\w.-])", "[redacted]", text)
    return text


def _parse_port(value: Any, name: str = UNICORN_PORT_VAR) -> Optional[int]:
    """A TCP port (1-65535) from text or a number; None when empty. The value is never echoed."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    try:
        port = int(str(value).strip())
    except ValueError:
        port = 0
    if not 1 <= port <= 65535:
        raise ValueError(f"{name} is not a valid TCP port (1-65535)")
    return port


# (pattern in the driver's message, plain sentence); the first match wins
_CONNECT_ERRORS = (
    (r"\b18456\b|login failed", "the server refused the login: check the user name and password"),
    (r"\b4060\b|cannot open database", "the login cannot open the database: check its name, or ask the "
                                        "database administrator for access"),
    (r"\b20003\b|timed out|timeout", "no answer from the SQL Server in time: check the server name and port, "
                                     "and that this PC is on the server's network (VPN, firewall)"),
    (r"\b20009\b|\b20002\b|unable to connect|connection refused|10061|unavailable or does not exist",
     "cannot reach the SQL Server: check the server name and port, that this PC is on the server's network (VPN, firewall), "
     "and that the server is running"),
)


def _plain_connect_error(text: str) -> Optional[str]:
    """A sentence for a pymssql connect error's (already redacted) text, None when unknown."""
    for pattern, sentence in _CONNECT_ERRORS:
        if re.search(pattern, text, re.IGNORECASE):
            return sentence
    return None


def _connect_safely(pymssql, **kwargs):
    """
    pymssql.connect() whose errors never expose the connection details: ConnectionError with
    one plain sentence; the driver's own (redacted) text is kept in its `detail`.
    """
    try:
        return pymssql.connect(**kwargs)
    except Exception as e:
        details = [kwargs.get(k) for k in ('server', 'host_name', 'user', 'password', 'database', 'port')]
        detail = _redact(e, *details)
        error = ConnectionError(_plain_connect_error(detail) or detail)
        error.detail = detail
        # 'from None' keeps the unredacted original out of tracebacks
        raise error from None


def read_env_file(path: Path) -> Dict[str, str]:
    """
    KEY=VALUE lines of a .env file (keys upper-cased, '-' -> '_'; '#' comments skipped).
    UTF-8, with or without a BOM. A value in matching quotes keeps everything inside them
    (including '#'); unquoted, ' #' starts a comment. A bare UNICORN_ key means empty.
    """
    env_vars = {}
    for line in Path(path).read_text(encoding='utf-8-sig').splitlines():
        line = line.strip()
        # Skip comments and empty lines
        if not line or line.startswith('#'):
            continue
        # Parse KEY=VALUE format (handles spaces around =)
        if '=' in line:
            key, value = line.split('=', 1)
            key = key.strip().upper().replace('-', '_')
            value = value.strip()
            end = value.find(value[0], 1) if value[:1] in ('"', "'") else -1
            if end > 0:
                value = value[1:end]
            elif ' #' in value:
                value = value.split(' #')[0].strip()
            env_vars[key] = value

        # A key without '=' is an empty setting (e.g. no port)
        elif line.upper().startswith('UNICORN_'):
            key = line.strip().upper().replace('-', '_')
            env_vars[key] = ''
    return env_vars


def user_env_file() -> Path:
    """
    The per-user .env `pycorn-setup` writes: %APPDATA%\\pycorn\\.env on Windows, else
    $XDG_CONFIG_HOME/pycorn/.env (default ~/.config/pycorn/.env). Found from any working folder.
    """
    if os.name == "nt" and os.environ.get("APPDATA"):
        return Path(os.environ["APPDATA"]) / "pycorn" / ".env"
    base = os.environ.get("XDG_CONFIG_HOME") or str(Path.home() / ".config")
    return Path(base) / "pycorn" / ".env"


def env_file_candidates() -> List[Path]:
    """
    Where PyCORNDatabase() looks for a .env when no env_file= is given, in order: the file
    PYCORN_ENV_FILE names, sql/.env and .env in the working folder, the source checkout's
    sql/.env, the per-user file (user_env_file()). The first one that exists is read
    (find_env_file()).
    """
    named = os.environ.get(ENV_FILE_VAR, '').strip()
    cwd = Path.cwd()
    return ([Path(named)] if named else []) + [cwd / "sql" / ".env", cwd / ".env", DEFAULT_ENV_FILE,
                                               user_env_file()]


def find_env_file(env_file: Optional[str] = None) -> Optional[Path]:
    """
    The .env PyCORNDatabase(env_file) reads: env_file when given (used only if it exists),
    else the first of env_file_candidates() that exists; None when there is none.
    """
    if env_file:
        return Path(env_file)
    named = os.environ.get(ENV_FILE_VAR, '').strip()
    if named and not Path(named).is_file():
        warnings.warn(f"{ENV_FILE_VAR} names a file that does not exist; looking in the default places")
    return next((path for path in env_file_candidates() if path.is_file()), None)


def _local_server_answers(port: int) -> bool:
    """True when something accepts TCP connections on localhost:port (1 s timeout)."""
    import socket
    try:
        with socket.create_connection((LOCAL_SERVER, port), timeout=1):
            return True
    except OSError:
        return False


def load_env(env_file: Optional[str] = None) -> Dict[str, str]:
    """
    UNICORN_* settings: the .env from find_env_file(env_file), then the process environment,
    which overrides the file key by key. When neither names a server (UNICORN_SERVER or
    UNICORN_SOURCES), localhost is used with a warning if a SQL Server answers there on
    UNICORN_PORT (default 1433); otherwise ValueError, naming the places looked (never values).
    """
    from .db_sources import SOURCES_VAR
    path = find_env_file(env_file)
    env = read_env_file(path) if path is not None and path.is_file() else {}
    env.update({k.upper(): v for k, v in os.environ.items() if k.upper().startswith('UNICORN_')})
    if env.get(UNICORN_SERVER_VAR, '').strip() or env.get(SOURCES_VAR, '').strip():
        return env
    port = _parse_port(env.get(UNICORN_PORT_VAR)) or SQL_SERVER_DEFAULT_PORT
    if not _local_server_answers(port):
        looked = [str(p) for p in ([Path(env_file)] if env_file else env_file_candidates())]
        from .messages import SettingsError
        raise SettingsError(f"no database server is set up ({UNICORN_SERVER_VAR}) and no SQL Server answers on "
                            f"this PC: run pycorn-setup to enter it. A .env was looked for in: {'; '.join(looked)}",
                            "no_settings")
    warnings.warn(f"No {UNICORN_SERVER_VAR} set; using the SQL Server that answers on {LOCAL_SERVER}")
    env[UNICORN_SERVER_VAR] = LOCAL_SERVER
    return env


class _Scope:
    """One database being searched: its source alias, position in the source, name and an open cursor."""
    __slots__ = ("source", "number", "database", "cursor", "profile")

    def __init__(self, source, number, database, cursor, profile=None):
        self.source, self.number, self.database, self.cursor = source, number, database, cursor
        self.profile = profile  # SchemaProfile when the caller asked for one (searches)

    @property
    def label(self) -> str:
        """'alias#n': safe to print (no server or database name)."""
        return "%s#%d" % (self.source, self.number)


@dataclass
class ConnectionParams:
    """Encapsulates database connection parameters with validation."""
    server: str
    database: str
    user: Optional[str] = None
    password: Optional[str] = None
    login_timeout: int = 30
    use_windows_auth: bool = False
    # None = the driver's default (1433); set it to reach a service on another port
    port: Optional[int] = None

    def __post_init__(self):
        self.port = _parse_port(self.port)

    def __str__(self) -> str:
        # Show only which fields are set; values never appear in output
        fields = ('server', 'port', 'database', 'user', 'password')
        status = ', '.join(f"{f}={'set' if getattr(self, f) else 'empty'}" for f in fields)
        auth = "Windows Auth" if self.use_windows_auth else "SQL Auth"
        return f"ConnectionParams({status}; {auth})"

    __repr__ = __str__


@dataclass
class TableReference:
    """Represents a database table in the CTE result set."""
    name: str
    columns: List[str] = None

    def __post_init__(self):
        if self.columns is None:
            self.columns = []

    @property
    def column_dict(self) -> Dict[str, str]:
        """Return columns as a dictionary for easier access."""
        return {col.lower().replace(' ', '_'): col for col in self.columns}


class PyCORNDatabase:
    """
    Pythonic database connector class for UNICORN SQL Server.

    Provides object-oriented access to CTE query results and database tables.
    Uses environment variables for credentials (security-first approach).
    """

    # Column groups of query()'s folder summary (DataFrameResult.tables); see folder_summary_group()
    AVAILABLE_TABLES = ["folder", "results", "methods"]

    def __init__(self, env_file: Optional[str] = None, cte_file: Optional[str] = None):
        """
        Initialize the database connector.

        Parameters
        ----------
        env_file : str, optional
            Path to a .env file for credentials (default: find_env_file(), i.e.
            PYCORN_ENV_FILE, ./sql/.env, ./.env, the source checkout's sql/.env).
            UNICORN_* environment variables override the file key by key.
        cte_file : str, optional
            Custom CTE SQL file path

        Examples
        --------
        >>> from pycorn.db_connector import PyCORNDatabase
        >>> db = PyCORNDatabase()  # Uses the first .env found (see find_env_file())
        >>> df = db.query()  # Execute default CTE query
        """
        self.cte_file = cte_file or None
        self._read_env_credentials(env_file)
        self._pymssql = _import_pymssql()

    def _read_env_credentials(self, env_file: Optional[str] = None) -> None:
        """
        Read credentials: the .env file, overridden by UNICORN_* environment variables
        (load_env()).

        Parameters
        ----------
        env_file : str, optional
            Path to .env file. If None, the first file find_env_file() finds.

        Examples
        --------
        >>> db._read_env_credentials()  # Uses the first .env found
        >>> db._read_env_credentials(".venv/.env.local")  # Custom path
        """
        from .db_sources import DEFAULT_SOURCE, sources_from_env

        try:
            env_vars = load_env(env_file)
            # Every source (server + login) in search order; see pycorn.db_sources
            self.sources = sources_from_env(env_vars, ConnectionParams, _parse_port)
        except Exception as e:
            from .messages import SettingsError
            raise SettingsError(f"database settings: {e}", getattr(e, "error_key", "bad_settings")) from None
        # load_run(), query() and test_connection() use this source unless told otherwise
        self.default_source = DEFAULT_SOURCE if DEFAULT_SOURCE in self.sources else next(iter(self.sources))
        self.conn_params = self.sources[self.default_source].params
        self._discovered = {}   # alias -> [(database, is_unicorn)]
        self._profiles = {}     # (alias, database) -> SchemaProfile
        self._curve_kinds_cache = {}
        self._schemas = {}      # (alias, database) -> sql_source schema
        # alias -> why: sources the last connection attempt could not reach (searches leave them out)
        self.unreachable = {}

    def _connect(self, source: Optional[str] = None):
        """Open a new pymssql connection for one source (default: the default source)."""
        # For TCP/IP connections, use SQL Server authentication.
        # Windows Authentication requires named pipes (LOCALSERTYPE=SNAPSHOT), not TCP/IP.
        params = self.sources[source or self.default_source].params
        kwargs = dict(
            server=params.server,
            database=params.database,
            login_timeout=params.login_timeout,
            user=params.user if params.user and params.password else None,
            password=params.password,
        )
        if params.port is not None:
            kwargs['port'] = params.port
        return _connect_safely(self._pymssql, **kwargs)

    def get_connection_params(self) -> ConnectionParams:
        """Get the current connection parameters."""
        return self.conn_params

    def test_connection(self, quiet: bool = False, source: Optional[str] = None) -> bool:
        """
        Test database connection using stored credentials.

        Parameters
        ----------
        quiet : bool
            If True, suppress output
        source : str, optional
            Alias of the source to test (default: the default source)

        Returns
        -------
        bool
            True if connection successful

        Examples
        --------
        >>> db.test_connection()  # Uses credentials from .env
        >>> if db.test_connection(): print("Connected!")
        >>> all(db.test_connection(source=s) for s in db.sources)
        """
        source = self._alias(source)
        conn_params = self.sources[source].params

        try:
            connection = self._connect(source)
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            connection.close()

            if not quiet:
                print(f"[OK] Connection successful to source {source!r}: {conn_params}")
            return True

        except Exception as e:
            error_msg = str(e).lower()
            if "not allowed" in error_msg or "unauthorized access" in error_msg:
                if conn_params.password and (conn_params.user == "sa"):
                    print("[FAIL] Connection refused. Check credentials.")
                else:
                    print(f"[FAIL] Connection refused: credentials may need adjustment")
            elif "login timeout" in error_msg or "timeout" in error_msg:
                print(f"[FAIL] Login timed out. Check server connectivity.")
            else:
                print(f"[FAIL] Connection failed: {e}")

            return False

    def _create_cursor(self, **filters) -> Tuple[Any, List[str]]:
        """Create and execute cursor for CTE query (filters go to execute_cte_query)."""
        cursor = self._connect().cursor()
        return execute_cte_query(cursor, self.cte_file, **filters)

    def query(self, quiet: bool = False, **filters) -> 'DataFrameResult':
        """
        Execute the CTE query and return results.

        Parameters
        ----------
        quiet : bool
            If True, suppress output
        **filters
            Optional server-side filters passed to execute_cte_query():
            folder_ids, description_pattern, hide_hidden_folders

        Returns
        -------
        DataFrameResult
            Object containing DataFrame, columns, and table accessors

        Examples
        --------
        >>> result = db.query()
        >>> df = result.df  # Full DataFrame
        >>> rows = result.rows  # Raw rows list
        >>> print(result.column_names)  # Column names
        >>> result = db.query(folder_ids=[123, 456], hide_hidden_folders=True)
        """
        cursor, columns = self._create_cursor(**filters)
        df = fetch_results(cursor, columns)

        if not quiet:
            print(f"\nQuery executed successfully!")
            print(f"Columns ({len(columns)}): {', '.join(columns[:10])}...")
            print(f"\nResults shape: {df.shape}")

        return DataFrameResult(df, columns, groups=folder_summary_group)

    def query_raw(self, quiet: bool = False, **filters) -> Tuple[Any, List[str]]:
        """
        Execute query and return cursor + columns without DataFrame conversion.

        Accepts the same filters as query(). Useful for streaming large
        results with cursor.fetchmany().

        Returns
        -------
        tuple
            (cursor, columns) ready for further processing
        """
        return self._create_cursor(**filters)

    def export_to_csv(self, output_path: str = "unicorn_data.csv") -> None:
        """
        Export current query results to CSV.

        Parameters
        ----------
        output_path : str
            Path to output CSV file

        Examples
        --------
        >>> db.export_to_csv("/path/to/output.csv")
        """
        result = self.query()
        export_to_csv(result.df, output_path)

    def load_run(self, result_id: Any, adjust_axes: bool = True, column_volume: float = None,
                 column_volume_unit: str = None, use_name_hint: bool = False,
                 source: Optional[str] = None, database: Optional[str] = None):
        """
        Load one result, with every database row tied to it, into a `pycorn.model.Run`.

        The run has the same structure as one `pycorn.open_run()` reads from a ZIP export
        of the same result (curves, events, peaks, metadata, evaluation log), plus
        `run.tables`: every row found through the database's foreign keys, by table.
        Only SELECT statements are run.

        Parameters
        ----------
        result_id : int or catalog row
            dbo.Result.ResultID, or one row of a `find()` page (it names its source and
            database).
        adjust_axes : bool
            As for open_run(): False keeps every adjusted axis equal to the stored one.
        column_volume, column_volume_unit, use_name_hint
            As for open_run(): override the column volume used for CV, or accept one from the
            run or folder name.
        source, database : str, optional
            Where the result is (default: the default source and its database). Needed
            when the default source lists several databases.

        Examples
        --------
        >>> run = db.load_run(1234)
        >>> run.chromatogram().curves_with_role("uv")
        >>> run.tables["dbo.Peak"].to_frame()
        >>> runs = [db.load_run(row) for _, row in db.find(sources="discover").iterrows()]
        """
        from .db_sources import ALL_DATABASES
        if hasattr(result_id, "keys") and "result_id" in result_id.keys():  # a catalog row
            source = source or result_id.get("source")
            database = database or result_id.get("database")
            result_id = result_id["result_id"]
        source = self._alias(source)
        wanted = [database] if database else self.sources[source].databases
        if wanted == ALL_DATABASES or (wanted and len(wanted) > 1):
            raise ValueError("source %r lists several databases: pass database= (or a catalog row)" % source)
        scopes = self._scopes([(source, wanted)], strict=True)
        options = dict(adjust_axes=adjust_axes, axes_options={
            "column_volume": column_volume, "column_volume_unit": column_volume_unit, "use_name_hint": use_name_hint})
        with closing(scopes):
            for scope in scopes:
                try:
                    return self._read_in_scope(scope, int(result_id), options)
                except LookupError as e:
                    raise LookupError("%s in %s (searched only there; for another database give source=, "
                                      "database= or a find() row)" % (e, scope.label)) from None

    def _read_in_scope(self, scope, result_id, options):
        """One result of the database `scope` is in, as a Run (`load_run`)."""
        from .sources.sql_source import read_result, read_schema
        params = self.sources[scope.source].params
        # the catalog is read once per database, then reused
        key = (scope.source, scope.database.lower())
        if key not in self._schemas:
            self._schemas[key] = read_schema(scope.cursor)
        run = read_result(scope.cursor, result_id, schema=self._schemas[key], server=params.server or None,
                          port=params.port, **options)
        run.origin["source"] = scope.source
        run.origin.move_to_end("source", last=False)
        return run

    def load_many(self, catalog, strict: bool = False, check_tstamp: bool = True, adjust_axes: bool = True,
                  column_volume: float = None, column_volume_unit: str = None, use_name_hint: bool = False,
                  on_skip=None):
        """
        Load every result of a catalog (`find()` pages, filtered further or deduplicated
        as you like), one `Run` at a time, in catalog order: a generator, so only one run
        is in memory at once. Rows of the same database share one connection.

        A result that cannot be read (deleted since the search, a connection or read
        error) is skipped with a warning naming the source alias, database number and
        ResultID; `strict=True` raises instead (owner, 2026-11-08). When the catalog has
        `tstamp`, a result changed in UNICORN since the search is loaded as it is now,
        with a warning (`check_tstamp=False` skips the check).
        Other arguments as for `load_run()`. `on_skip(key, message)`: called for each row
        skipped, `key` (source, database, ResultID), or ('file', path, member) for a file row;
        `message` the warning's reason.

        File rows (`catalog_from_files()`: a `path`, no `database`) may be mixed in; each run
        of consecutive file rows goes to `pycorn.file_catalog.load_files` (owner, 2026-11-08),
        its `match_key` checked when `check_tstamp` is on.

        Examples
        --------
        >>> for page in db.find_pages(folder="/DefaultHome/Projects/**", dedupe=True):
        ...     for run in db.load_many(page):
        ...         export(run)
        """
        from .file_catalog import is_file_row, load_files
        if not {"source", "database"} <= set(catalog.columns):
            raise ValueError("load_many: the catalog needs source and database columns (a find() page or "
                             "catalog_from_files rows)")
        paths = catalog["path"] if "path" in catalog.columns else [None] * len(catalog)
        files = [is_file_row(p, d) for p, d in zip(paths, catalog["database"])]
        if not all(files):
            db_rows = catalog[[not f for f in files]]
            if "result_id" not in catalog.columns or db_rows["database"].isna().any() or db_rows["result_id"].isna().any():
                raise ValueError("load_many: every row needs a database and result_id (a find() page) or a path "
                                 "(catalog_from_files)")
        options = dict(strict=strict, adjust_axes=adjust_axes, column_volume=column_volume,
                       column_volume_unit=column_volume_unit, use_name_hint=use_name_hint)
        start = 0
        while start < len(catalog):  # consecutive rows of one kind
            end = start
            while end < len(catalog) and files[end] == files[start]:
                end += 1
            part = catalog.iloc[start:end]
            if files[start]:
                yield from load_files(part, check_key=check_tstamp, on_skip=on_skip, **options)
            else:
                yield from self._load_db_rows(part, check_tstamp=check_tstamp, on_skip=on_skip, **options)
            start = end

    def _load_db_rows(self, catalog, strict, check_tstamp, adjust_axes, column_volume, column_volume_unit,
                      use_name_hint, on_skip=None):
        """`load_many` for database rows."""
        from .search import REQUIRED_COLUMNS
        options = dict(adjust_axes=adjust_axes, axes_options={
            "column_volume": column_volume, "column_volume_unit": column_volume_unit, "use_name_hint": use_name_hint})
        has_tstamp = check_tstamp and "tstamp" in catalog.columns
        # consecutive rows of one database form a group: one connection, one tstamp query
        groups = []
        for row in catalog.itertuples(index=False):
            where = (row.source, str(row.database))
            if not groups or groups[-1][0] != where:
                groups.append((where, []))
            groups[-1][1].append((int(row.result_id), getattr(row, "tstamp", None) if has_tstamp else None))
        for (source, database), rows in groups:
            scopes = self._scopes([(self._alias(source), [database])], strict, need=REQUIRED_COLUMNS)
            with closing(scopes):
                for scope in scopes:
                    current = self._current_tstamps(scope, [i for i, _ in rows]) if has_tstamp else None
                    for result_id, tstamp in rows:
                        where = "ResultID %d in %s" % (result_id, scope.label)
                        if current is not None:
                            if result_id not in current:
                                self._skip_result(strict, "load_many: %s is no longer in the database" % where,
                                                  on_skip, (source, database, result_id))
                                continue
                            if isinstance(tstamp, str) and current[result_id] is not None and tstamp != current[result_id]:
                                warnings.warn("load_many: %s changed since the search; loading it as it is now" % where,
                                              stacklevel=4)
                        try:
                            run = self._read_in_scope(scope, result_id, options)
                        except Exception as e:
                            if strict:
                                raise
                            params = self.sources[scope.source].params
                            self._skip_result(False, "load_many: %s not loaded (%s: %s)" % (
                                where, type(e).__name__,
                                _redact(e, params.server, params.user, params.password, scope.database)),
                                on_skip, (source, database, result_id))
                            continue
                        yield run

    def match_files(self, files, sources: Any = None, strict: bool = False):
        """
        Which database results the result files are (plan section 5, `match_zip_to_db`):
        `files` is paths (files or folders, as for `pycorn.catalog_from_files`) or a file
        catalog. Every database in scope (default: everything `.env` lists) is asked for
        the files' match keys (first chromatogram second in UTC, system, run index), hidden
        folders included; one indexed query per database per 400 keys.

        The key leaves the name out, so the same run saved under other names (evaluated
        copies) is found too; the name is the tie-breaker. Returns one row per (file
        result, database result) with the same key, same-name rows first: `path`,
        `member`, `name`, `match_key`, `status`, `source`, `database`, `result_id`,
        `db_name`, `n_matches` (database results with the key and the file's name, over
        every database), `n_copies` (with the key, any name), `file_modified_utc`,
        `db_modified_utc` and `changed_in_db` (the database result was changed after the
        export, compared in whole seconds). `status`:

        - 'exact': same run and name, the only one with that name in its database;
        - 'ambiguous': same run and name, several in the same database;
        - 'copy': the same run under another name (usually another evaluation of it);
        - 'missing': no database result has the key (database columns empty);
        - 'no_key': the file has no chromatogram time to match on.
        """
        import pandas as pd
        from .file_catalog import catalog_from_files
        from .search import COPY_LOOKUP_COLUMNS, REQUIRED_COLUMNS, copy_keys, fetch_copies
        cat = files if hasattr(files, "columns") else catalog_from_files(files)
        keys = copy_keys(cat) if len(cat) else []
        found = []
        if keys:
            scopes = self._scopes(self._plan(sources), strict, need=REQUIRED_COLUMNS)
            with closing(scopes):
                for scope in scopes:
                    found.append(fetch_copies(scope.cursor, keys, source=scope.source, database=scope.database,
                                              columns=scope.profile, label=scope.label, include_hidden=True))
        found = [f for f in found if len(f)]
        db = pd.concat(found, ignore_index=True) if found else pd.DataFrame(columns=COPY_LOOKUP_COLUMNS)
        rows = []
        for _, f in cat.iterrows():
            key = None if pd.isna(f["match_key"]) else f["match_key"]
            base = {"path": f["path"], "member": f["member"], "name": f["name"], "match_key": key}
            hits = db[db["match_key"] == key] if key is not None else db.iloc[:0]
            if not len(hits):
                rows.append(dict(base, status="no_key" if key is None else "missing", source=None, database=None,
                                 result_id=None, db_name=None, n_matches=0, n_copies=0,
                                 file_modified_utc=f["modified_utc"], db_modified_utc=None, changed_in_db=None))
                continue
            same = hits["name"] == f["name"]
            hits = pd.concat([hits[same], hits[~same]])  # same-name rows first
            same = hits["name"] == f["name"]
            per_db = hits[same].groupby(["source", "database"])["result_id"].size()
            for (_, h), named in zip(hits.iterrows(), same):
                file_mod, db_mod = f["modified_utc"], h["modified_utc"]
                changed = None if pd.isna(file_mod) or pd.isna(db_mod) else bool(db_mod.floor("s") > file_mod.floor("s"))
                status = ("exact" if per_db[(h["source"], h["database"])] == 1 else "ambiguous") if named else "copy"
                rows.append(dict(base, status=status, source=h["source"], database=h["database"],
                                 result_id=int(h["result_id"]), db_name=h["name"], n_matches=int(same.sum()),
                                 n_copies=len(hits), file_modified_utc=file_mod, db_modified_utc=db_mod,
                                 changed_in_db=changed))
        columns = ["path", "member", "name", "match_key", "status", "source", "database", "result_id", "db_name",
                   "n_matches", "n_copies", "file_modified_utc", "db_modified_utc", "changed_in_db"]
        out = pd.DataFrame(rows, columns=columns)
        for c in ("file_modified_utc", "db_modified_utc"):
            out[c] = pd.to_datetime(out[c], utc=True)
        out["result_id"] = out["result_id"].astype("Int64")
        return out

    @staticmethod
    def _skip_result(strict: bool, message: str, on_skip=None, key=None) -> None:
        if strict:
            raise LookupError(message)
        warnings.warn(message + "; skipped", stacklevel=5)  # load_many's caller
        if on_skip is not None:
            on_skip(key, message)

    @staticmethod
    def _current_tstamps(scope, result_ids) -> Dict[int, Any]:
        """{ResultID: tstamp hex (None when the database has no tstamp)} of the ids still there."""
        from .search import _ID_CHUNK, _tstamp
        has = scope.profile is None or scope.profile.has("Result", "tstamp")
        out = {}
        for start in range(0, len(result_ids), _ID_CHUNK):
            chunk = result_ids[start:start + _ID_CHUNK]
            scope.cursor.execute("SELECT ResultID, %s FROM dbo.[Result] WHERE ResultID IN (%s)"
                                 % ("tstamp" if has else "NULL", ", ".join(["%d"] * len(chunk))), tuple(chunk))
            out.update({int(i): _tstamp(t) for i, t in scope.cursor.fetchall()})
        return out

    # -----------------------------------------------------------------------
    # Several sources and databases (pycorn.db_sources)
    # -----------------------------------------------------------------------

    def _alias(self, source: Optional[str]) -> str:
        """The alias as written in .env (default: the default source)."""
        if source is None:
            return self.default_source
        for alias in self.sources:
            if alias.lower() == str(source).lower():
                return alias
        raise KeyError("unknown source %r; sources in .env: %s" % (source, ", ".join(self.sources)))

    def discover_databases(self, source: Optional[str] = None) -> List[str]:
        """
        Names of the UNICORN databases (they have dbo.Result, Chromatogram, Curve and
        CurvePoint) that one source's login can open, by name. Returned, never printed.
        """
        from .db_sources import list_databases
        source = self._alias(source)
        conn = self._connect(source)
        try:
            self._discovered[source] = list_databases(conn.cursor())
        finally:
            conn.close()
        return [name for name, unicorn in self._discovered[source] if unicorn]

    def schema_profile(self, source: Optional[str] = None, database: Optional[str] = None):
        """`pycorn.db_sources.SchemaProfile` of one database (default: the default source's first)."""
        source = self._alias(source)
        wanted = [database] if database else self.sources[source].databases
        if wanted == "*":
            raise ValueError("schema_profile: pass database= (source %r lists all databases)" % source)
        scopes = self._scopes([(source, wanted[:1] if wanted else None)], strict=True, need={})
        with closing(scopes):
            for scope in scopes:
                return self._profiles[(source, scope.database.lower())]

    def _plan(self, sources) -> List[tuple]:
        from .db_sources import plan_scope
        return plan_scope(self.sources, sources)

    def _skip(self, strict: bool, message: str) -> None:
        """A source or database that can't be searched: raise when strict, else warn and go on."""
        if strict:
            raise LookupError(message)
        warnings.warn(message + "; its results are left out", stacklevel=4)

    def _scopes(self, plan, strict: bool = False, start_source: Optional[str] = None, need=None):
        """
        Yield a `_Scope` (alias, number, database, open cursor) for each database in
        `plan`, in order: one connection per source, switched with USE only after the name
        is checked against what the login can open. A source that cannot connect, a missing
        database, or (with `need`, a {table: columns} dict) a database lacking a needed
        column is warned about and skipped, or raises when `strict`. Sources before
        `start_source` are not connected to. Close the generator when stopping early.
        """
        from .db_sources import read_profile, resolve_databases, use_database, list_databases
        started = start_source is None
        for alias, wanted in plan:
            if not started:
                if alias != start_source:
                    continue
                started = True
            try:
                conn = self._connect(alias)
            except ConnectionError as e:
                self.unreachable[alias] = str(e)
                if strict:
                    raise
                self._skip(False, "source %r: cannot connect (%s)" % (alias, e))
                continue
            self.unreachable.pop(alias, None)
            try:
                cursor = conn.cursor()
                if wanted is None:
                    databases = [(1, None)]
                else:
                    if alias not in self._discovered:
                        self._discovered[alias] = list_databases(cursor)
                    databases = resolve_databases(self._discovered[alias], wanted)
                for number, name in databases:
                    label = "%s#%d" % (alias, number)
                    if name is None and wanted is not None:
                        # numbered as given: by the caller (sources=), or in .env's list
                        given = ("database no. %d of the ones given in sources=" % number
                                 if wanted != self.sources[alias].databases else "database %s" % label)
                        self._skip(strict, "source %r: %s is not on the server, is offline, or the login "
                                           "cannot open it" % (alias, given))
                        continue
                    if name is not None:
                        use_database(cursor, name)
                    profile = None
                    if need is not None:
                        key = (alias, (name or "").lower())
                        if key not in self._profiles:
                            self._profiles[key] = read_profile(cursor)
                            self._note_empty_columns(label, self._profiles[key])
                        profile = self._profiles[key]
                        self._profiles[(alias, profile.database.lower())] = profile
                        missing = profile.missing(need)
                        if missing:
                            self._skip(strict, "database %s lacks %s" % (label, ", ".join(missing)))
                            continue
                        name = profile.database
                    elif name is None:
                        cursor.execute("SELECT DB_NAME()")
                        name = cursor.fetchall()[0][0]
                    yield _Scope(alias, number, name, cursor, profile)
            finally:
                conn.close()

    @staticmethod
    def _note_empty_columns(label, profile):
        """Once per database: which catalog columns stay empty there, and why (another UNICORN version)."""
        from .search import empty_catalog_columns
        empty = empty_catalog_columns(profile)
        if empty:
            lacking = sorted({c for cols in empty.values() for c in cols})
            warnings.warn("database %s lacks %s: catalog columns %s are empty there; filters on them match "
                          "nothing there" % (label, ", ".join(lacking), ", ".join(empty)), stacklevel=5)

    def _curve_kinds(self, scope):
        """Distinct original-curve (type, unit, name), read once per database (for curve-role filters)."""
        key = (scope.source, scope.database.lower())
        if key not in self._curve_kinds_cache:
            from .search import read_curve_kinds
            self._curve_kinds_cache[key] = read_curve_kinds(scope.cursor, scope.profile)
        return self._curve_kinds_cache[key]

    def _filters_for(self, scope, filters):
        filters = dict(filters, columns=scope.profile, label=scope.label)
        if filters.get("has_curve_roles"):
            filters["curve_kinds"] = self._curve_kinds(scope)
        return filters

    def _after(self, after, plan):
        """(source, database or None, result_id) to continue after, from find()'s `after=`."""
        from .search import after_key
        if isinstance(after, numbers.Integral) and not isinstance(after, bool):
            alias, wanted = plan[0]
            if len(plan) == 1 and (wanted is None or (isinstance(wanted, list) and len(wanted) == 1)):
                return alias, None, int(after)
            raise ValueError("after=<ResultID> works only when one database is searched; "
                             "pass the last page (after=page) instead")
        return after_key(after)

    def _find_page(self, limit, after, sources, strict, filters, dedupe=False):
        from .search import REQUIRED_COLUMNS, after_key, check_filters, fetch_page, page_across
        check_filters(filters)
        plan = self._plan(sources)
        key = self._after(after, plan)

        def fetch(scope, n, after_id):
            return fetch_page(scope.cursor, limit=n, after=after_id, source=scope.source,
                              **self._filters_for(scope, filters))

        scopes = self._scopes(plan, strict, start_source=key[0] if key else None, need=REQUIRED_COLUMNS)
        with closing(scopes):
            frame, more = page_across(scopes, fetch, limit, key)
        # where the next page starts: the last row read, even when dedupe drops it
        last = after_key(frame)
        return self._mark_copies(frame, plan, strict, filters, dedupe), more, last

    def _mark_copies(self, frame, plan, strict, filters, dedupe):
        """
        n_copies / duplicate_group of a page: one lookup per database searched, with the same
        filters, for the page's match keys (owner, 2026-11-08); `dedupe` keeps the most
        recently changed copy of each run (`pycorn.search.mark_copies`).
        """
        import pandas as pd
        from .search import COPY_LOOKUP_COLUMNS, REQUIRED_COLUMNS, copy_keys, fetch_copies, mark_copies
        keys = copy_keys(frame) if len(frame) else []
        if not keys:
            return frame
        found = []
        scopes = self._scopes(plan, strict, need=REQUIRED_COLUMNS)
        with closing(scopes):
            for scope in scopes:
                found.append(fetch_copies(scope.cursor, keys, source=scope.source, database=scope.database,
                                          **self._filters_for(scope, filters)))
        found = [f for f in found if len(f)]
        copies = pd.concat(found, ignore_index=True) if found else pd.DataFrame(columns=COPY_LOOKUP_COLUMNS)
        return mark_copies(frame, copies, dedupe)

    def find(self, limit: int = None, after: Any = None, sources: Any = None, strict: bool = False,
             dedupe: bool = False, **filters):
        """
        One page of the run catalog: a DataFrame, one row per result (`pycorn.search.CATALOG_COLUMNS`),
        at most `limit` rows (default 500, at most 5000).

        The same run can be in several databases (imported, copied): such rows are kept and
        marked with `n_copies` (how many results of this search are that run, over every
        database searched) and `duplicate_group` (`match_key`, plan section 5).
        `dedupe=True` keeps only the most recently changed copy (`modified_utc`), so every
        run appears once over all pages; a page can then be shorter than `limit`.

        Every database `.env` lists is searched (`sources=` narrows or widens it, see
        `pycorn.db_sources.plan_scope`; 'discover' = every UNICORN database each listed login
        can see). A page fills from one database at a time, in source and database order,
        each in ResultID order; a result is identified by (source, database, result_id).
        A source or database that cannot be searched is left out with a warning
        (`strict=True` raises instead).

        A warning says when more results match; continue with `after=` the page itself,
        or use `find_pages()`. Filters are described in `pycorn.search`; `pick_list()`
        shows the values they can take.

        Examples
        --------
        >>> cat = db.find(folder="/DefaultHome/Projects/**", run_start=("2019-01-01", "2019-06-30"),
        ...               variables={"Flow rate": (">=", 1.0, "ml/min")}, has_curve_roles=["uv"])
        >>> more = db.find(after=cat, folder="/DefaultHome/Projects/**")
        >>> runs = [db.load_run(row) for _, row in cat.iterrows()]
        """
        from .search import DEFAULT_LIMIT
        limit = DEFAULT_LIMIT if limit is None else limit
        frame, more, last = self._find_page(limit, after, sources, strict, filters, dedupe)
        # With dedupe, rows after the page's last row may have been read and dropped: after=page
        # reads them again and drops them again. Only a page left empty cannot say where to go on.
        if more and dedupe and not len(frame):
            warnings.warn("find(): more results match, but every row of this page is a copy kept on another page; "
                          "use find_pages(dedupe=True) (stopped at source %r, ResultID %d)" % (last[0], last[2]),
                          stacklevel=2)
        elif more:
            warnings.warn("find(): more than %d results match; next page: find(after=<this page>, ...) or use "
                          "find_pages() (this page ends at source %r, ResultID %d)"
                          % (len(frame), last[0], last[2]), stacklevel=2)
        return frame

    def find_pages(self, page_size: int = None, sources: Any = None, strict: bool = False, dedupe: bool = False,
                   **filters):
        """Every result matching `filters`, one catalog page (DataFrame) at a time, as `find()` orders them."""
        from .search import DEFAULT_LIMIT
        page_size = DEFAULT_LIMIT if page_size is None else page_size
        after = None
        while True:
            frame, more, last = self._find_page(page_size, after, sources, strict, filters, dedupe)
            if len(frame):
                yield frame
            if not more or last is None:
                return
            after = last

    def database_labels(self, sources: Any = None, strict: bool = False) -> Dict[tuple, str]:
        """
        {(source, database name in lower case): 'alias#n'} for every database in scope
        (`sources=` as for `find()`): the label warnings use, safe to print or put in a
        path. `n` is the database's position in its source's `.env` list, or among the
        UNICORN databases on the server by name for '*' and 'discover'.
        """
        out = {}
        scopes = self._scopes(self._plan(sources), strict)
        with closing(scopes):
            for scope in scopes:
                out[(scope.source, str(scope.database).lower())] = scope.label
        return out

    def count(self, sources: Any = None, strict: bool = False, by_database: bool = False, **filters):
        """
        How many results match `filters` (as for `find()`), counted on the server: one
        total, or with `by_database=True` a DataFrame (source, database, n).
        """
        import pandas as pd
        from .search import REQUIRED_COLUMNS, check_filters, count
        check_filters(filters, "count")
        rows = []
        scopes = self._scopes(self._plan(sources), strict, need=REQUIRED_COLUMNS)
        with closing(scopes):
            for scope in scopes:
                rows.append((scope.source, scope.database, count(scope.cursor, **self._filters_for(scope, filters))))
        if by_database:
            return pd.DataFrame(rows, columns=["source", "database", "n"])
        return sum(n for _, _, n in rows)

    def variables(self, catalog, names: Optional[List[str]] = None, strict: bool = True):
        """
        Method variables of the results in `catalog` (one `find()` page, or ResultIDs when
        one database is searched) as one long table (`pycorn.search.VARIABLE_COLUMNS`):
        source, database, result_id, kind, variable, value, number, unit, scouting. `names`
        limits it to these variables. `pycorn.search.wide(catalog, table)` turns it into columns.
        """
        import pandas as pd
        from .search import REQUIRED_COLUMNS, VARIABLE_COLUMNS, fetch_variables
        if hasattr(catalog, "columns") and {"source", "database"} <= set(catalog.columns):
            groups = list(dict.fromkeys(zip(catalog["source"], catalog["database"])))
            plan = []
            for alias, database in groups:
                if plan and plan[-1][0] == alias:
                    plan[-1][1].append(database)
                else:
                    plan.append((self._alias(alias), [database]))
            pick = lambda scope: catalog[(catalog["source"] == scope.source)
                                         & (catalog["database"].str.lower() == scope.database.lower())]
        else:
            plan = self._plan(None)
            if len(plan) != 1 or plan[0][1] == "*" or (plan[0][1] and len(plan[0][1]) > 1):
                raise ValueError("variables(): ResultIDs alone are ambiguous over several databases; pass a find() page")
            pick = lambda scope: catalog
        frames = []
        scopes = self._scopes(plan, strict, need=REQUIRED_COLUMNS)
        with closing(scopes):
            for scope in scopes:
                frames.append(fetch_variables(scope.cursor, pick(scope), names, scope.source, scope.database,
                                              columns=scope.profile, label=scope.label))
        frames = [f for f in frames if len(f)] or frames[:1]
        if not frames:
            return pd.DataFrame({c: pd.Series(dtype=object) for c in VARIABLE_COLUMNS})
        out = pd.concat(frames, ignore_index=True)
        for c in ("source", "database", "kind", "variable", "unit"):
            out[c] = out[c].astype(object).astype("category")
        return out

    def pick_list(self, field: str, limit: int = 1000, variable: Optional[str] = None, sources: Any = None,
                  strict: bool = False, by_database: bool = False):
        """
        Values a `find()` filter can take, with result counts, most common first (see
        `pycorn.search.PICK_LISTS`): 'folder', 'system', 'system_type', 'run_type', 'state',
        'created_by', 'batch_id', 'column', 'variable', 'variable_value' (with `variable=`),
        'curve_role'. Over several databases the counts are summed per value (folders by
        path); `by_database=True` lists each database's values, with source and database.
        """
        from .search import REQUIRED_COLUMNS, merge_pick_lists, pick_list
        frames = []
        scopes = self._scopes(self._plan(sources), strict, need=REQUIRED_COLUMNS)
        with closing(scopes):
            for scope in scopes:
                kinds = self._curve_kinds(scope) if field == "curve_role" else None
                frames.append((scope.source, scope.database,
                               pick_list(scope.cursor, field, limit=limit, variable=variable, curve_kinds=kinds,
                                         columns=scope.profile, label=scope.label)))
        if len(frames) == 1 and not by_database:
            return frames[0][2]
        return merge_pick_lists(frames, field, limit=limit, by_database=by_database)

    def execute_custom_query(self, sql: str) -> 'DataFrameResult':
        """
        Execute a custom SQL query.

        Parameters
        ----------
        sql : str
            Raw SQL query string

        Returns
        -------
        DataFrameResult
            Object containing results

        Examples
        --------
        >>> custom_sql = "SELECT TOP 10 * FROM dbo.Folder WHERE IsHidden = 0"
        >>> result = db.execute_custom_query(custom_sql)
        """
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            return DataFrameResult(fetch_results(cursor, columns), columns)

        except Exception as e:
            raise RuntimeError(f"Custom query execution failed: {e}")
        finally:
            conn.close()


def folder_summary_group(column: str) -> str:
    """
    Group of one folder-summary column (pycorn/sql/unicorn_cte.sql, folderSummary):
    'results' (ResultCount, LatestResultCreated, TotalResultCount, Rollup...), 'methods'
    (the same for methods), else 'folder' (the dbo.Folder columns, Level, FolderPath).
    """
    name = column.lower()
    if 'result' in name:
        return 'results'
    if 'method' in name:
        return 'methods'
    return 'folder'


class DataFrameResult:
    """
    Wrapper class for query results with table-like access.

    Examples
    --------
    >>> result = db.query()
    >>> df = result.df               # Full DataFrame
    >>> rows = result.rows           # List of row dictionaries
    >>> print(result.column_names)   # All column names
    >>> result.get_table_columns('results')  # ['ResultCount', 'LatestResultCreated', ...]

    Note
    ----
    `tables` groups the columns of query()'s folder summary: 'folder', 'results' and
    'methods' (PyCORNDatabase.AVAILABLE_TABLES), each column in exactly one group. Other
    queries (execute_custom_query) have no groups. Not the same as `run.tables` from
    load_run(), which holds the database rows of one result.
    """

    def __init__(self, df: Any, columns: Optional[List[Any]] = None, groups=None):
        """
        Initialize with pandas DataFrame and column names.

        Parameters
        ----------
        df : pandas.DataFrame
            Query results as DataFrame
        columns : list, optional
            Column names, or a cursor.description sequence. Defaults to df.columns.
        groups : callable, optional
            column name -> group name, filling `tables`; None = no groups
        """
        import pandas as pd
        self.df = df if hasattr(df, 'to_string') else pd.DataFrame(df)

        if columns is None:
            columns = list(self.df.columns)
        # Accept either plain names or cursor.description tuples
        self._columns = [c[0] if isinstance(c, (tuple, list)) else c for c in columns]

        self.tables: Dict[str, List[str]] = {}
        if groups is not None:
            for column in self._columns:
                self.tables.setdefault(groups(column), []).append(column)

    @property
    def column_names(self) -> List[str]:
        """Get all column names as a list."""
        return self._columns.copy()

    @property
    def rows(self) -> List[Dict[str, Any]]:
        """Get results as list of dictionaries (one per row)."""
        return self.df.to_dict(orient='records') if hasattr(self.df, 'to_dict') else []

    def get_table_columns(self, table_name: str) -> List[str]:
        """
        Columns of one group of `tables` (KeyError naming the groups there are).

        Parameters
        ----------
        table_name : str
            Group name: 'folder', 'results' or 'methods' for query()

        Returns
        -------
        list
            Column names in that group, in query order

        Examples
        --------
        >>> result = db.query()
        >>> result.get_table_columns('folder')
        ['FolderID', 'ParentFolderID', 'Description', ...]
        """
        if table_name not in self.tables:
            raise KeyError(f"no column group {table_name!r}; groups here: {', '.join(self.tables) or 'none'}")
        return list(self.tables[table_name])

    def to_dict(self, orient: str = 'records') -> Dict[str, Any]:
        """Convert DataFrame to dictionary."""
        import pandas as pd
        if hasattr(self.df, 'to_dict'):
            return self.df.to_dict(orient=orient)
        return {}

    def head(self, n: int = 5) -> Any:
        """Get first n rows (like DataFrame.head())."""
        if hasattr(self.df, 'head'):
            return self.df.head(n)
        return None

    def shape(self) -> Tuple[int, int]:
        """Get DataFrame shape (rows, columns)."""
        if hasattr(self.df, 'shape'):
            return self.df.shape
        return (len(self.rows), len(self.column_names))

    def __str__(self) -> str:
        """String representation of results."""
        rows, cols = self.shape()
        columns_preview = ', '.join(self.column_names[:10])
        if len(self.column_names) > 10:
            columns_preview += '...'
        return (f"PyCORN Query Results\n"
                f"=========================\n"
                f"Shape: {rows} rows x {cols} columns\n"
                f"Columns: {columns_preview}\n")


def get_cte_query(filepath: Optional[str] = None) -> str:
    """
    Read the CTE query from a SQL file.

    Parameters
    ----------
    filepath : str, optional
        Path to unicorn_cte.sql file (defaults to pycorn/sql/unicorn_cte.sql
        in the package, independent of the working directory)

    Returns
    -------
    str
        The CTE SQL query string

    Raises
    ------
    FileNotFoundError
        If the specified file doesn't exist
    """
    path = Path(filepath) if filepath else DEFAULT_CTE_FILE
    if not path.exists():
        raise FileNotFoundError(f"CTE SQL file not found: {path}")
    return path.read_text(encoding='utf-8').strip()


def get_connection_params(server: str, database: str,
                          username: Optional[str] = None,
                          password: Optional[str] = None,
                          login_timeout: int = 30,
                          port: Optional[Any] = None) -> dict:
    """
    Generate connection parameters for SQL Server using pymssql.

    Parameters
    ----------
    server : str
        Database server name or IP address (e.g., 'SERVERNAME' or '192.168.1.10')
    database : str
        Database name (e.g., 'UNICORN', 'UNICO711')
    username : str, optional
        Username for SQL Server authentication (None for Windows auth)
    password : str, optional
        Password for SQL Server authentication (omit for Windows auth)
    login_timeout : int, optional
        Timeout in seconds for connection attempts
    port : int or str, optional
        TCP port of the SQL Server service (UNICORN_PORT). None uses the driver's
        default (1433). Needed when several services share one host.

    Returns
    -------
    dict
        Connection parameters as keyword arguments for pymssql.connect()
    """

    conn_params = {
        'server': server,
        'database': database,
        'login_timeout': login_timeout,
    }
    port = _parse_port(port)
    if port is not None:
        conn_params['port'] = port

    if username and password:
        conn_params['user'] = username
        conn_params['password'] = password
    else:
        # Windows Authentication (Integrated Security)
        conn_params['host_name'] = server

    return conn_params


def test_connection(conn_params: dict) -> bool:
    """
    Test database connection.

    Parameters
    ----------
    conn_params : dict
        Connection parameters from get_connection_params()

    Returns
    -------
    bool
        True if connection successful, False otherwise
    """
    pymssql = _import_pymssql()
    try:
        connection = _connect_safely(pymssql, **conn_params)
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"[FAIL] Connection test failed: {e}")
        return False


def execute_cte_query(
    cursor,
    filepath: Optional[str] = None,
    folder_ids: Optional[List[int]] = None,           # Filter by specific folder IDs (comma-separated)
    description_pattern: Optional[str] = None,        # Fuzzy match on Description column
    hide_hidden_folders: bool = False                  # If True, exclude hidden folders
) -> Tuple[Any, list]:
    """
    Execute the CTE query from a SQL file with optional filtering.

    DECLARE statements for @FolderFilter, @DescriptionPattern and @HideHidden
    are prepended to the CTE, with values passed through pymssql parameter
    substitution. With no filters, the variables are NULL/0 and the query
    returns the full folder hierarchy starting at root folders.

    Parameters
    ----------
    cursor : pymssql.Cursor
        Database cursor
    filepath : str, optional
        Path to custom CTE SQL file. It may reference the three variables
        above but must not DECLARE them itself.
    folder_ids : list of int, optional
        Start the hierarchy at these folder IDs
    description_pattern : str, optional
        Substring to match in folder Description (LIKE '%pattern%')
    hide_hidden_folders : bool, default False
        If True, exclude hidden folders and their subtrees

    Returns
    -------
    tuple
        (cursor, columns_list) where cursor has results

    Examples
    --------
    >>> # Default query (backward compatible)
    >>> cursor, columns = execute_cte_query(cursor)

    >>> # Filter by specific folder IDs and fuzzy description match
    >>> cursor, columns = execute_cte_query(
    ...     cursor,
    ...     folder_ids=[123, 456],
    ...     description_pattern='Sample'
    ... )

    See pycorn/sql/unicorn_cte.sql for the filter semantics.
    """
    cte_sql = get_cte_query(filepath)

    params = {
        # int() rejects anything that is not a folder ID
        'folder_filter': ','.join(str(int(i)) for i in folder_ids) if folder_ids else None,
        'description_pattern': description_pattern.strip() if description_pattern and description_pattern.strip() else None,
        'hide_hidden': 1 if hide_hidden_folders else 0,
    }

    # pymssql only substitutes %(name)s placeholders with dict params, so
    # literal '%' in the SQL (e.g. LIKE 'Phase%') needs no escaping
    sql = CTE_PARAM_DECLARATIONS + cte_sql

    try:
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description]
        return cursor, columns
    except Exception as e:
        raise RuntimeError(f"Database query execution failed: {e}") from e


def fetch_results(cursor, columns: list) -> Any:
    """
    Fetch results from cursor and convert to DataFrame.

    Parameters
    ----------
    cursor : pymssql.Cursor
        Cursor with executed query
    columns : list
        Column names from cursor description

    Returns
    -------
    pandas.DataFrame
        Query results as DataFrame
    """
    import pandas as pd

    # Fetch all data and convert to dictionary
    rows = cursor.fetchall()

    # Create dictionary of columns to values
    data_dict = {col: [] for col in columns}

    for row in rows:
        for i, col in enumerate(columns):
            data_dict[col].append(row[i])

    return pd.DataFrame(data_dict)


def export_to_csv(df: Any, output_path: str) -> None:
    """
    Export query results to CSV file.

    Parameters
    ----------
    df : pandas.DataFrame
        Query results
    output_path : str
        Path to output CSV file
    """
    df.to_csv(output_path, index=False)
    print(f"[INFO] Results exported to: {output_path}")


def create_database_user(sql_file: str, server: str, database: str,
                         username: str, password: str) -> None:
    """
    Create a read-only user for the UNICORN database using SQL Server authentication.

    Parameters
    ----------
    sql_file : str
        Path to create-user.sql template
    server : str
        Database server name
    database : str
        Database name
    username : str
        New username to create
    password : str
        Password for new user
    """
    import pymssql

    conn = _connect_safely(pymssql, server=server, database=database,
                           user=username, password=password)
    cursor = conn.cursor()

    with open(sql_file, 'r') as f:
        create_sql = f.read()

    # Replace placeholders if needed
    try:
        cursor.execute(create_sql.replace('{USERNAME}', username)
                      .replace('{PASSWORD}', password))
        print(f"User '{username}' created successfully")
    except Exception as e:
        print(f"Error creating user: {e}")

    cursor.close()
    conn.close()


def main():
    """
    Main function with command-line interface.

    Usage examples:

    # SQL Server Authentication (TCP/IP)
    python db_connector.py -s "SERVERNAME" -d "UNICORN" \
        -u "username" -p "password" -o "output.csv"

    # Custom CTE file
    python db_connector.py -s "SERVERNAME" -d "UNICORN" \
        -u "username" -p "password" -c "custom_query.sql" -o "output.csv"
    """
    parser = argparse.ArgumentParser(
        description='Connect to UNICORN 7.11 database using pymssql (no ODBC)'
    )

    parser.add_argument('-s', '--server', required=True,
                        help='Database server (IP address or hostname)')
    parser.add_argument('-d', '--database', required=True,
                        help='Database name (e.g., UNICORN, UNICO711)')
    parser.add_argument('--port', default=None,
                        help='TCP port of the SQL Server service (default: the driver default, 1433)')
    parser.add_argument('-u', '--user', default=None,
                        help='Username for SQL Server authentication (required for TCP/IP)')
    parser.add_argument('-p', '--password', default=None,
                        help='Password for SQL Server authentication (required for TCP/IP)')
    parser.add_argument('-c', '--cte-file', dest='cte_file', default=None,
                        help='Custom CTE SQL file path (optional, uses pycorn/sql/unicorn_cte.sql by default)')
    parser.add_argument('-o', '--output', default='unicorn_data.csv',
                        help='Output CSV file path (default: unicorn_data.csv)')
    parser.add_argument('--quiet', action='store_true',
                        help='Suppress connection/test output')

    args = parser.parse_args()

    # Build connection parameters (TCP/IP - SQL Server authentication)
    conn_params = get_connection_params(
        server=args.server,
        database=args.database,
        username=args.user,
        password=args.password,
        login_timeout=30,
        port=args.port,
    )

    # Test connection if credentials provided or no password
    if not args.quiet:
        print(f"Connecting to {args.server}:{args.database}...")

    try:
        if test_connection(conn_params):
            if not args.quiet:
                print("Connection successful!")
        else:
            return

        # Create cursor and execute query
        conn = _connect_safely(_import_pymssql(), **conn_params)
        cursor = conn.cursor()

        try:
            if not args.quiet:
                print("Executing CTE query...")

            cursor, columns = execute_cte_query(cursor, filepath=args.cte_file)

            if not args.quiet:
                print(f"Query executed successfully!")
                print(f"Columns returned: {len(columns)}")

            # Fetch and display results
            df = fetch_results(cursor, columns)

            if not args.quiet:
                print(f"\nResults shape: {df.shape}")
                print("\nFirst 5 rows:")
                print(df.head())

            # Export to CSV if output path specified
            if args.output:
                export_to_csv(df, args.output)

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        print(f"Error: {e}")


if __name__ == '__main__':
    main()
