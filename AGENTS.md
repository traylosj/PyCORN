# PyCORN Agent Collaboration Notes

## Credentials - CRITICAL

Chat history, examples and commits can be exported, shared or leaked. Even text that only
looks like example credentials is dangerous. These rules apply everywhere: agent chat, code,
examples, tests, sample data, docs, commit messages and pull requests.

- **NEVER write real passwords, usernames, server IPs, database names or other secrets** -
  not in chat, files, logs, debug output or git
- **NEVER print connection values** (server, database, user, password): report only set/empty
- In code, read settings by name only: `os.getenv('UNICORN_SERVER', '')`,
  `os.getenv('UNICORN_DATABASE', 'YOUR_DATABASE')`, `os.getenv('UNICORN_PASSWORD', '')`
- Placeholders are empty strings or `YOUR_PASSWORD_HERE`-style values; `sql/.env.example`
  is the tracked template, `.env` files are gitignored and never committed (the `.gitignore`
  keeps `!.env.example`; it has no inline comments, so put `#` comments on their own line)
- Connection errors are raised as `ConnectionError` with server, user, password and database
  redacted; keep it that way

## Database Connection

- Pure Python: `pymssql` over TCP/IP (`pip install pymssql pandas`), no ODBC. Keep only
  pymssql code; no pyodbc in code, examples or tests
- SQL Server authentication only: `UNICORN_SERVER`, `UNICORN_DATABASE`, `UNICORN_USER`,
  `UNICORN_PASSWORD`, optional `UNICORN_PORT` (empty = 1433; always passed to the driver
  when set, since several SQL Server services can share one host). Windows authentication
  is not supported (pymssql does no Kerberos/NTLM over TCP/IP)
- Where settings come from (`find_env_file()` / `load_env()` in `pycorn/db_connector.py`):
  the first `.env` of `env_file=`, `PYCORN_ENV_FILE`, `./sql/.env`, `./.env`, the
  checkout's `sql/.env`, the per-user file `pycorn-setup` writes (`user_env_file()`, last);
  `UNICORN_*` environment variables override it key by key
- No server set: `localhost` with a warning only if a SQL Server answers there, else
  `ValueError` (owner, 2026-11-08). Never add a silent default server

Quick start: `copy sql\.env.example sql\.env`, fill it in (or run `pycorn-setup`), then
`python sql/test_connection.py`, then `from pycorn import PyCORNDatabase`.

### SQL query files

- Queries live in files, never embedded strings. The code reads them from `pycorn/sql/`
  (resolved from the package, shipped as package data); the default is
  `pycorn/sql/unicorn_cte.sql`, a custom one goes in with `-c/--cte-file`
- The connector prepends `DECLARE` statements for `@FolderFilter`, `@DescriptionPattern`,
  `@HideHidden`; SQL files must not declare them
- `db_connector.py` flow: `get_cte_query()` -> `get_connection_params()` ->
  `test_connection()` -> `execute_cte_query()` (server-side folder, description and hidden
  filters) -> `fetch_results()` (pandas DataFrame)

## Testing

- `python -m pytest` from the repository root (`pytest.ini`), or
  `cd tests && PYTHONPATH=.. python -m pytest`. `tests/conftest.py` runs every test from
  `tests/` (so `../samples` paths work) and clears `UNICORN_*`. The database parity test runs
  only when `sql/.env` exists and the server answers
- `python sql/test_connection.py` tests the connection; `python sql/test_validation.py`
  checks imports, the CTE file and `.env` setup
- Test scripts read credentials through the connector (`find_env_file()` / `load_env()`),
  never with their own `.env` parser
- Tests must not depend on the developer's shell: clear `UNICORN_*` and `PYCORN_ENV_FILE`
  (see the fixture in `tests/test_db_connector.py`)

## Coding Rules

### Curve and event names vary by instrument
Never hardcode curve names (`UV 1_280`, `Cond`, `Run Log`, ...) in library code: process-scale
and other systems use different names (e.g. `UV_280`), and local samples do not cover them all.
Curve numbers also change between runs. Look up by role (`Chromatogram.curves_with_role()`,
`pycorn/curve_roles.py`) or stored type (`Chromatogram.curves_of_type()`,
`event_curves_of_type()`), take names from the caller, and raise with the available names
(`Chromatogram.curve(name)`) rather than guessing aliases. Do not validate types against a
closed list. See `samples/SAMPLE_ZIP_CONTENTS.md`, "Identifying Curves".

### ASCII console output
The Windows console may use cp1252. In scripts and examples run from a terminal, print
`[OK]` / `[FAIL]` / `warning:` instead of check marks, crosses or emoji.

### No heredocs, ever
Never use shell heredocs (`<<EOF`, `<<'EOF'`, PowerShell here-strings piped to a program)
to write files, edit files or feed scripts to an interpreter. Use Read to look, Edit for
changes, Write for new files (put one-off scripts in a temp folder, then run them with
`python -I <path>`). Heredocs break on quoting, silently change line endings (some files
here, such as this one, are stored with CRLF), and hide what was written from review.

### Local vs universal
Tracked files (docs, code, comments, tests, commit messages) hold only what is true for any
UNICORN install: rules, formats, decisions and why. Facts about the owner's lab are local:
survey counts, database names and result IDs, run and project names, instrument settings,
machine paths, what the local test files hold. They go in the gitignored `local/` folder
at the repository root (start at `local/README.md`), never in tracked files. Where evidence
matters, write "tested against a lab environment" (or "tuned against a lab environment" for
a threshold) and nothing more about what is local or where it is.

## Key Files

| File | Purpose |
|------|---------|
| `sql/.env.example` | Settings template (copy to the gitignored `sql/.env`) |
| `sql/scripts/create-user.sql` | Admin script: read-only login/user (`drop-user.sql` removes it) |
| `pycorn/db_connector.py` | Database connector: settings, connection, CTE query |
| `pycorn/sql/unicorn_cte.sql` | CTE query the connector reads by default |
| `pycorn/setup_env.py` | `pycorn-setup`: writes the per-user `.env` by questions (password hidden, no value printed), tests the connection; no password flag or stdin. `--from-env [--overwrite]`: the same file from the `UNICORN_*` environment variables, no questions (deployments; exit 2 and nothing written when server/user/password is empty or the file exists); both modes keep a dated `<name>.<YYYY-MM-DD_HHMMSS>.bak` copy before changing a file (never removed; a warning past 5). `source_status()`: the settings `pycorn find` reads, per source alias set/empty and connected (`--test-only`, `pycorn sources [--test] [--json]`) |
| `pycorn/messages.py` | `plain_error(e)`: one sentence for OS/zip/tar/memory errors (CLI, warnings, any app); `EXIT_CODES` (0/1/2/3/130, returned by `cli.main()`; `--debug` there) (ROADMAP item 5); `ERROR_KEYS` / `error_key(e)`: the `key` of the `--json` / `--jsonl` error object (only ever added); `SettingsError` (`no_settings`, `bad_settings`) |
| `pycorn/json_output.py` | Machine-readable CLI output (Track 9, A4): `SCHEMA_VERSION` and `document()` (`{"schema": 1, "version", "command", "masked", ...}`) for every command's `--json`, `JsonLines` for `--jsonl` (header, rows, summary), `error_document()`, `home_tilde()`, `PeopleMasker` / `mask_people()` (catalog people columns `system`, `created_by`, and `match_key`, `export_error`; `run_export.mask_index` applies it to index.csv with `--mask-identifying` and the page's mask; run/folder names shown), `catalog_rows()` (never the `database` column: label only). Contracts per command: `docs/AGENT_GUIDE.md` |
| `pycorn/__init__.py` | `__all__`: the supported public API (Track 9, A1); `__version__`; heavy names load lazily (`_LAZY`). Add a name here only as a promise; renaming one needs a CHANGELOG entry |
| `pycorn/schema.py` | `build_schema()`: tables and columns (`export_tables.py`), groups, curve roles, units, exit codes, error keys as one dict; `pycorn schema [--json]` (`"schema": 1`); `browser.names()` is built from it (`page_names()`) |
| `pycorn/summary.py` | `run_summary(run, mask=True)`: what a run holds as one JSON-ready dict (chromatograms, curves by role, event positions, variables, Vc, warnings; never curve values, IP or ports); masked by default: people etc. via `Masker`, run/folder/file/source-result/method/user-set chromatogram and curve names as Run1/Folder1/File1.zip/Result1/Method1/Chrom1/Curve1 and text variable values via `SummaryNames` (one per document); `pycorn inspect [--json | --jsonl] [--no-mask]` (`"schema": 1`, `document()` on `json_output.document`) (Track 9, A3). Not `inspect.py`: `pycorn.run_summary` is the function, so the module has another name |
| `docs/AGENT_GUIDE.md` | Using PyCORN from scripts and agents: settings without prompts, `schema --json`, exit codes, `__all__`, masking before output leaves the PC |
| `pycorn/browser.py` | Python side of the in-browser page (`Session`: add files, view a run, downloads; JSON out; options through `ExportOptions.from_dict`; Download all writes `pycorn export`'s `index.csv` columns (`run_export.index_frame`) and `options.json`); `web/index.html` is the page template, `tools/build_web_page.py` builds the one-file page (wheels embedded), `.github/workflows/pages.yml` deploys it (ROADMAP item 6) |
| `pycorn/excel_export.py` | The `.xlsx` of `pycorn export` for people: summary sheet, chart by curve role, formatted tables |
| `pycorn/export_tables.py` | One name per table for page, Excel sheet and CSV (`title`, `file_stem`); origin per table and column (UNICORN / PyCORN); `column_guide`, `README.txt` text. Add every new table or column here |
| `pycorn/version.py` | `__version__` (set only here; `setup.py` reads it), git `commit()`, `version_text()`; bump it and add a `CHANGELOG.md` entry with every release, tag `v<version>` |
| `CHANGELOG.md` | Release notes, newest first: what changed version to version (renamed outputs first) |
| `pycorn/run_details.py` | Run-level tables for every export path, laid out as UNICORN's report: result and method, method steps (call order, UNICORN's method text), variable list, scouting runs, column, calibration, instrument (never IP/ports), system settings, audit log; `Masker` (`--mask-identifying`) |
| `pycorn/sources/zip_source.py` | `open_run()` reader: everything in a UNICORN 6+ `.zip` into a `Run` (`pycorn/model.py`) |
| `pycorn/sources/sql_source.py` | `PyCORNDatabase.load_run(result_id)` reader: every row tied to a result (found through the database's foreign keys) into the same `Run`, plus `run.tables` |
| `pycorn/sources/ufol_source.py` | `open_folder()` reader: every result in a UNICORN `.UFol` folder export (a tar of `.Result` zips) |
| `pycorn/curve_roles.py` | Find curves by role (UV, conductivity, pH, Conc B, ...) instead of instrument-specific names |
| `pycorn/system_configuration.py` | Installed inlet valves A/B, sample inlet valve, sample pump, quaternary valve from the result's stored configuration (names or hardware codes; unknown -> not known); `run.system_configuration()`; drives the inlet labels (`run_log_sections.inlet_state`: known / assumed / not known) |
| `pycorn/search.py` | `db.find()` / `find_pages()` / `count()` / `variables()` / `pick_list()`: paged run catalog, every filter in SQL (`pycorn/sql/results/find_results.sql`); never a whole-database query; `match_key` / `n_copies` / `dedupe=True`; `db.load_many(catalog)` yields runs |
| `pycorn/file_catalog.py` | `catalog_from_files()`: the same catalog from `.zip`/`.UFol` files (light read, no curves); `load_files(catalog)` loads its rows (`db.load_many` takes mixed catalogs); `match_zip_to_db()` / `db.match_files()` |
| `pycorn/db_sources.py` | Several servers and databases from `.env` (`UNICORN_SOURCES`, `<ALIAS>_DATABASES` list or `*`), `sources=` scope, `discover_databases()`, `SchemaProfile`; output names the alias only |
| `pycorn/variables.py` | `run.variables()` / `run.variable(name)`: method variables as the run used them (search criteria; scouting flag; text + number) |
| `pycorn/axes.py` | `add_axes(run)`: Vc, CV and UTC for every curve and event; every reader runs it at load |
| `pycorn/units.py` | Unit table, conversions, UV path length, `run.normalized()`; display decimals (`DISPLAY_DECIMALS`, `convert_decimals()`: every conversion carries them) |
| `pycorn/display_precision.py` | Decimals per table column for display only (page, Excel cell formats); values never rounded in code, csv or JSON |
| `pycorn/column_volume.py` | Vc from all sources by priority; column valve by-pass; CV (in-column volume / Vc) |
| `pycorn/column_geometry.py` | Bed height, diameter, particle radius (SI) from the run's `ColumnTypeData`; checked against Vc, never used as Vc |
| `pycorn/timeline.py` | UTC rebuilt from the method start and the run log's pauses |
| `pycorn/event_tables.py` | Phase, injection and fraction tables with start/end time, volume, CV, UTC |
| `pycorn/concentration.py` | mg/ml curve from a UV curve and a caller-given extinction coefficient; `response_factor`; `ACCURACY_NOTE` on every result (issue #1) |
| `pycorn/uv_calibration.py` | Extinction coefficient from sample sent to the column by-pass: plateau max or pulse area / caller loop volume; `pycorn bypass-uv`, `export --bypass-uv` (issue #1) |
| `pycorn/window_stats.py` | Curve statistics over windows relative to a phase (M22): `phase_windows()`, `window_stats()`, `pycorn export --window` / `--window-curves`, `pycorn windows` (the same rows without an export; `--json` / `--jsonl` / `--csv -` masked as `bypass-uv`); no default window or curve; interim phase rule (as logged, one row per occurrence) |
| `pycorn/run_log_sections.py` | Process sections from what ran (executed run-log entries): flow, %B/Q ramps, valves, holds, pauses; checked against the curves |
| `pycorn/method_hints.py` | The method as hints only (D17/D20/D23): planned injections and block lengths (run's variable values) vs what ran, sample-application dialog, BufferPro, variables, documentation |
| `pycorn/exporters/cadet/` | `build_experiment(run)` -> `CadetExperiment` (all SI) for CADET / CADET-Process; `calibration.py` (UV AU/cm, Beer-Lambert, salt); `buffer_readings.py` (conductivity and pH at column by-pass); `process_export.py` (CADET-Process 0.12 references and `Process`, CSV + `experiment.json`; `pycorn[cadet]`); `core_input.py` (`to_core_input(exp, core_version='5'|'6')`: CADET-Core 5.1.2 or 6.0.0a5 layout, JSON or HDF5 via `pycorn[cadet-core]`; `overrides=` in CADET-Core's own names, D28); `api.py` (`pycorn.export_cadet()`: the CLI's CADET files from Python) |
| `tools/anonymize_zip.py` | Sanitize a result `.zip` before it leaves the lab (samples in this repo too); its docstring lists the fields that identify people, hosts, instruments and projects |
| `sql/test_connection.py`, `sql/test_validation.py` | Connection test; import, CTE file and `.env` checks |
| `sql/example_usage.py`, `sql/example_usage_pycorn.py` | Functional API and `PyCORNDatabase` examples |
| `sql/CONNECTOR_GUIDE.md` | Connector documentation |
| `docs/PROJECT_SUMMARY.md` | Repository overview |
| `docs/ROADMAP.md` | The one roadmap: goals table, status by track, every owner decision (D1-D30, Q1-Q8, ...), open items by goal, "Modeler input" (M1-M13). Start at "Goals" / "Start here". Facts and gotchas (F, X) and the 2026-11-08 review are in `docs/FACTS.md`; what each work package built and checked is in `docs/HANDOFFS.md` |
| `docs/GETTING_STARTED.md` | New-user guide by task (browser, zip to Excel, find runs, database setup); no programming assumed |
| `docs/CADET_GUIDE.md` | User reference for the CADET / CADET-Process / CADET-Core export |
| `docs/RUN_STRUCTURE_GUIDE.md`, `docs/RUN_STRUCTURE_REVIEW.md` | How phases, sections, pauses and holds are read from what ran; the expert review with open choices |
| `docs/WEB_PRIVACY.md` | The in-browser page: how it works, why files are not uploaded, running it from a shared drive |
| `pycorn/curve_sampling.py`, `pycorn/resample.py` | Which curves and how many points go in the curves table (`--curves`, `--max-points`; no defaults, D28) |
| `web/i18n/*.json` | The page's texts per language (en, de, es, fr, it, pt, sv); keep keys in step with `web/index.html` |
| `docs/USER_GUIDE.md` | User guide: the `Run` model, axes, roles, units, tables (for users, not a plan) |
| `docs/EXPORT_GUIDE.md` | User reference for `pycorn find` / `pycorn export`: flags, layout, every file and column, the options JSON keys (`--options`); keep in step with `run_export.py` (`export_selection`: the batch loop of `pycorn export`; `ExportOptions.to_dict` / `from_dict`) |
| `docs/PHASE0_FINDINGS.md` | Database spike: schema, mappings, units, vocabularies, pause texts |
| `samples/SAMPLE_ZIP_CONTENTS.md` | What a UNICORN 6+ bundle contains and what `PcUni6` extracts |
