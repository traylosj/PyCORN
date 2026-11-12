# PyCORN Database Connector Guide

`pycorn/db_connector.py` queries a UNICORN 7.x SQL Server database over TCP/IP
using `pymssql` (no ODBC). It runs the folder summary query in
`pycorn/sql/unicorn_cte.sql` and returns pandas DataFrames.

## Setup

```bash
pip install -r sql/requirements.txt
copy sql\.env.example sql\.env      # then fill in your values
python sql/test_connection.py       # connection smoke test
python sql/test_validation.py       # offline checks + connection attempt
```

`sql/.env` is gitignored. The connector reads these variables:

| Variable | Purpose |
|----------|---------|
| `UNICORN_SERVER` | SQL Server hostname or IP |
| `UNICORN_DATABASE` | UNICORN database name |
| `UNICORN_USER` | SQL Server login |
| `UNICORN_PASSWORD` | Password for that login |
| `UNICORN_PORT` | Optional TCP port of the SQL Server service; empty = 1433. Needed when several services share one host |
| `UNICORN_DATABASES` | Optional: more databases on this server, a comma list, or `*` for every UNICORN database the login can open |
| `UNICORN_SOURCES` | Optional: more servers, as aliases (`default,lab2`); each alias reads `UNICORN_<ALIAS>_SERVER`, `_PORT`, `_DATABASES`, `_USER`, `_PASSWORD` |

**Where settings come from.** `PyCORNDatabase()` reads the first `.env` it finds
(`pycorn.db_connector.find_env_file()`):

1. `env_file=` passed to `PyCORNDatabase`
2. the file named by the `PYCORN_ENV_FILE` environment variable
3. `sql/.env` in the current folder
4. `.env` in the current folder
5. `sql/.env` in the PyCORN source folder (a git checkout or `pip install -e .`)
6. the per-user file `pycorn-setup` writes: `%APPDATA%\pycorn\.env` on Windows,
   `~/.config/pycorn/.env` elsewhere (`$XDG_CONFIG_HOME/pycorn/.env` when set)

**Without editing files.** `pycorn-setup` asks for the server, port, database, login and
password (typed hidden), saves them to the per-user file and tests the connection. It never
prints a value, only whether each setting is set. Run it again to change a setting (Enter
keeps the current one), `pycorn-setup --test-only` to test the settings `pycorn find` reads
(every source alias, [OK] / [FAIL] each; with `--file PATH`, that file), `--file PATH` to write another
file. `pycorn sources [--test] [--json]` shows the same per alias (set / empty, never a value). A `.env` higher in the list above still wins; the tool says so when one exists.

**From a deployment script.** `pycorn-setup --from-env` asks nothing: it writes the file
(per-user, or `--file PATH`) from the `UNICORN_*` environment variables the script set, so
the password is never on a command line. It writes `UNICORN_SERVER`, `UNICORN_USER`,
`UNICORN_PASSWORD` and, when set, `UNICORN_PORT`, `UNICORN_DATABASE`, `UNICORN_DATABASES`,
plus `UNICORN_SOURCES` and each listed source's `UNICORN_<ALIAS>_*` keys when
`UNICORN_SOURCES` is set; other variables are not written. Server, user and password are
needed for every source; when one is empty, or a port or alias cannot be read, it writes
nothing and exits with 2. A file already there is left alone (exit 2) unless `--overwrite`
is given; then it is updated in place like an interactive run (other lines stay; a key the
file has but the environment does not is cleared). Before an existing file is changed, by
either mode, a dated copy `<name>.<YYYY-MM-DD_HHMMSS>.bak` is kept next to it (never removed;
past 5 copies a warning asks you to remove old ones, as each holds the password). Then the
connection is tested ([OK] / [FAIL]; exit 1 on a failure, `--no-test` skips it). It prints
setting names with set / empty only, never a value. There is still no password flag or
stdin mode.

`UNICORN_*` variables set in the process environment override the same keys from the file,
one key at a time, so a shell or CI job can change one setting without editing the file.
With no file, only the environment is used. If nothing names a server, PyCORN uses
`localhost` with a warning when a SQL Server answers there (on `UNICORN_PORT`, default
1433), and otherwise raises `ValueError` listing the places it looked.
Values may be quoted; inside quotes `#` is kept, unquoted ` #` starts a comment.

`db.find()`, `count()`, `pick_list()` search every database listed; `sources="discover"`
adds every UNICORN database each listed login can open (`db.discover_databases()` lists
them). Output names a source by its alias only. See `pycorn/db_sources.py`.
From a shell, `pycorn find ... --csv runs.csv` and `pycorn export --from runs.csv --out DIR` use
the same `.env` lookup (`pycorn --env-file FILE export ...` picks another file). See the README, "From the command line".
A read-only login can be created with `sql/scripts/create-user.sql`. Replace
`YOUR_PASSWORD_HERE` locally before running it, and do not commit the real value.

**Authentication:** pymssql over TCP/IP needs SQL Server authentication.
Windows/integrated authentication is not supported by this connector.

## Basic Usage

```python
from pycorn import PyCORNDatabase

db = PyCORNDatabase()              # reads the first .env found (see Setup)
if db.test_connection():
    result = db.query()            # DataFrameResult
    df = result.df                 # pandas DataFrame
    print(result.shape())          # (rows, cols)
    for row in result.rows[:5]:    # list of dicts
        print(row["FolderID"], row["FolderPath"], row["TotalResultCount"])
```

### `PyCORNDatabase`

| Method | Returns |
|--------|---------|
| `PyCORNDatabase(env_file=None, cte_file=None)` | Loads credentials; optional custom `.env` and SQL file |
| `test_connection(quiet=False, source=None)` | `bool`; `source` is an alias from `.env` (default: the default source) |
| `query(quiet=False, **filters)` | `DataFrameResult` |
| `query_raw(quiet=False, **filters)` | `(cursor, columns)` for streaming |
| `execute_custom_query(sql)` | `DataFrameResult` for any SQL string |
| `export_to_csv(output_path)` | Runs `query()` and writes a CSV |
| `get_connection_params()` | `ConnectionParams` (prints without the password) |
| `load_run(result_id, adjust_axes=True, source=None, database=None)` | `pycorn.model.Run` for one result (below). `result_id` is a ResultID or a `find()` row; `database=` is needed when the source lists several |
| `load_many(catalog, strict=False, check_tstamp=True)` | Generator of `Run`, one per catalog row (database rows and `catalog_from_files()` rows); unreadable results are skipped with a warning |
| `find(limit=None, after=None, sources=None, dedupe=False, **filters)` | One page of the run catalog (DataFrame, default 500 rows); filters in `pycorn/search.py` |
| `find_pages(page_size=None, sources=None, dedupe=False, **filters)` | Generator of catalog pages, every result that matches |
| `count(sources=None, by_database=False, **filters)` | Number of matching results, or a DataFrame per database |
| `variables(catalog, names=None)` | Method variables of the catalog's results, one long table |
| `pick_list(field, limit=1000, variable=None, sources=None)` | Values a `find()` filter can take, with counts |
| `match_files(files, sources=None)` | Which database results the `.zip` / `.UFol` files are (`status`: exact, ambiguous, copy, missing, no_key) |
| `discover_databases(source=None)` | Names of the UNICORN databases one source's login can open |

### Loading one result

`load_run()` reads everything the database holds for one `dbo.Result.ResultID` into the
same `Run` that `pycorn.open_run()` reads from a ZIP export: curves (decoded from
`BinaryCurvePoints`), events, peaks, retention zero, method/system `*Data` blobs, run
information, search criteria and the evaluation log. `run.tables` keeps every row, found
by following the database's foreign keys from `dbo.Result`:

```python
run = db.load_run(1234)
uv = run.chromatogram().curves_with_role("uv")[0]
uv.time, uv.volume, uv.values
peaks = run.tables.get("dbo.Peak")       # None when the result has no peaks
if peaks is not None:
    peaks.to_frame()                     # rows as stored
[(name, t.relation, len(t.rows)) for name, t in run.tables.items()]
```

`relation` is `owned` (below the result), `linked` (its audit trail) or `referenced`
(rows other results share: folders, lookups, and the instrument's `UCSystem` and
`InstrumentConfiguration`, matched by `SystemName`). The instrument rows hold its current
settings; a result imported from another database has none when its system isn't in this
one. The instrument's firmware, VID configuration and libraries are left out: they are
installed software, not result data. Only SELECT statements are run.

`run.origin` says where the run came from: `source` (the `.env` alias), `server`, `port`
(None for the default), `database` and `result_id` (for `open_run()`: `file_name` and
`path`). It contains connection details, so don't print or share it.

**Exporting to CADET.** A database run exports like a file run:
`pycorn.export_cadet(db.load_run(1234), "cadet_out/", core_version="6")` writes the
CADET-Process files and a CADET-Core 5 or 6 input built from what the run executed. Model
values the run doesn't hold (porosities, transport, binding) are placeholders unless you set them
with `core_overrides=` in CADET-Core's own names. The exported files hold no connection
details. See the README, "To CADET".

### `DataFrameResult`

`.df`, `.rows`, `.column_names`, `.shape()`, `.head(n)`, `.to_dict(orient)`,
`.tables` / `.get_table_columns(name)`. For `query()`, `.tables` puts each folder summary
column in one of three groups: `folder` (the folder's own columns, `Level`, `FolderPath`),
`results` (result counts and latest result) and `methods` (the same for methods).
`execute_custom_query()` results have no groups. Not the same as `run.tables` (above).

## Result Columns

The default query returns one row per folder:

- **Folder:** `FolderID`, `ParentFolderID`, `Description`, `IsGlobal`, `IsHidden`,
  `Created`, `CreatedBy`, `LastModified`, `LastModifiedBy`, `tstamp`, `Level`, `FolderPath`
- **Direct counts:** `ResultCount`, `MethodCount`, `LatestResultCreated(By)`,
  `LatestMethodCreated(By)`
- **Rolled up over subfolders:** `TotalResultCount`, `TotalMethodCount`,
  `RollupLatestResultCreated`, `RollupLatestMethodCreated`

## Server-Side Filtering

The filters restrict the folder hierarchy in SQL before any rows are sent:

```python
db.query(folder_ids=[123, 456])            # subtrees starting at these folders
db.query(description_pattern="Sample")     # subtrees of folders whose Description contains "Sample"
db.query(hide_hidden_folders=True)         # drop hidden folders and their subfolders
db.query(folder_ids=[123], hide_hidden_folders=True)   # filters combine
```

With no filters, the hierarchy starts at the root folders. When a filter is given,
`FolderPath` and `Level` are relative to the matched starting folder.

How it works: `execute_cte_query()` puts `DECLARE` statements for `@FolderFilter`,
`@DescriptionPattern` and `@HideHidden` before the SQL file. The values go in through
pymssql parameter substitution, which quotes them safely. Folder IDs that are not
integers raise `ValueError`. The query runs at any database compatibility level from
SQL Server 2012 on: UNICORN databases can sit at an older level than the server, so the
SQL files avoid `STRING_SPLIT` (level 130+).

The functional form works with any pymssql cursor:

```python
from pycorn import execute_cte_query, fetch_results
cursor, columns = execute_cte_query(cursor, folder_ids=[123])
df = fetch_results(cursor, columns)
```

**Running the SQL file in SSMS:** paste these lines above the query first:

```sql
DECLARE @FolderFilter VARCHAR(1000) = NULL;
DECLARE @DescriptionPattern VARCHAR(200) = NULL;
DECLARE @HideHidden BIT = 0;
```

**Custom SQL files** (`cte_file=` or `-c`) may use these variables but must not
`DECLARE` them, since the connector already does.

## Large Result Sets

`query()` loads everything into memory. For big databases:

1. **Filter on the server** (above). This is the most effective option.
2. **Stream with the raw cursor:**

   ```python
   import pandas as pd
   cursor, columns = db.query_raw(hide_hidden_folders=True)
   while True:
       rows = cursor.fetchmany(1000)
       if not rows:
           break
       process(pd.DataFrame(rows, columns=columns))
   cursor.connection.close()
   ```

3. **Work in batches of folders:** loop over chunks of folder IDs with `db.query(folder_ids=chunk)`.
4. **Write narrower SQL** with `execute_custom_query()`. For example:

   ```python
   db.execute_custom_query(
       "SELECT TOP 20 FolderID, Description, Created FROM dbo.Folder "
       "WHERE IsHidden = 0 ORDER BY Created DESC"
   )
   ```

   `execute_custom_query` does not take parameters. Never build its SQL from untrusted input.

## Command Line

```bash
python -m pycorn.db_connector -s SERVER -d DATABASE -u USER -p PASSWORD -o out.csv [-c custom.sql] [--quiet]
```

Passing `-p` on the command line can leave the password in your shell history.
Prefer `PyCORNDatabase`, which reads a `.env` file (see Setup).

## Troubleshooting

`python sql/test_connection.py` reports which settings file it used and whether the login
works; `python sql/test_validation.py` lists which settings are set (never their values).

| Symptom | Check |
|---------|-------|
| "cannot reach the SQL Server" / "no answer ... in time" (unknown server name, refused, timed out) | Server name/IP; the firewall must allow the SQL Server port (`UNICORN_PORT`, default 1433); `Test-NetConnection SERVER -Port 1433` (PowerShell) |
| "the server refused the login" | User and password; SQL Server authentication enabled (Mixed Mode, in SQL Server Configuration Manager / SSMS); the login is not disabled or locked |
| "the login cannot open the database" | Database name as listed in SSMS (`SELECT name FROM sys.databases`); the login has a user in that database (`sql/scripts/create-user.sql`) |
| Slow server | The functional API takes `get_connection_params(..., login_timeout=N)` (seconds, default 30) |
| `ValueError: database settings: no database server is set up ...` | No `.env` was found; the message lists where PyCORN looked (see Setup); `pycorn-setup` writes one |

Connection errors (`ConnectionError`) carry one plain sentence; the driver's own text, with
server, login and password redacted, is in the exception's `detail` (the CLI prints it under
`--debug`).

Use a dedicated read-only login, not `sa`.

## Related Files

- `pycorn/sql/unicorn_cte.sql`: the query, with notes on its filter behavior
- `sql/example_usage.py`: functional API walkthrough
- `sql/example_usage_pycorn.py`: `PyCORNDatabase` walkthrough
- [`docs/PROJECT_SUMMARY.md`](../docs/PROJECT_SUMMARY.md): overview of the whole repository
