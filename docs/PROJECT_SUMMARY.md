# PyCORN Project Summary

PyCORN reads ÄKTA chromatography runs into one Python model (`Run`, `pycorn/model.py`),
from either source:

1. **Files**: UNICORN 6+ `.zip` / `.Result` exports (`open_run()`), `.UFol` folder exports
   (`open_folder()`), and legacy UNICORN 3-5 `.res` files (`PcRes3`). The original parsers
   (`PcRes3`, `PcUni6`) are from the upstream project (forked from `ronald-jaepel/PyCORN`,
   GPLv2, version 0.20) and keep their output.
2. **Database**: a UNICORN 7.x SQL Server over TCP/IP with `pymssql` (no ODBC).
   `PyCORNDatabase.find()` searches results page by page, and `load_run()` loads one into the
   same `Run` as a ZIP gives, across several servers and databases from `.env`.

Every reader adds the same axes at load: elapsed time, volume, column volumes (CV) and
wall-clock time (UTC). On top of the model: curve roles, units, phase/injection/fraction
tables, method variables, process sections from the executed run log, a `pycorn find` /
`pycorn export` command line, and a CADET / CADET-Process exporter.

User documentation: [`README.md`](../README.md) (short), [`docs/GETTING_STARTED.md`](GETTING_STARTED.md) (by task,
no programming), [`docs/USER_GUIDE.md`](USER_GUIDE.md) (the run model),
[`docs/EXPORT_GUIDE.md`](EXPORT_GUIDE.md) (`find` / `export`), [`docs/CADET_GUIDE.md`](CADET_GUIDE.md) (CADET export),
[`docs/RUN_STRUCTURE_GUIDE.md`](RUN_STRUCTURE_GUIDE.md) (how phases, sections and pauses are read),
[`docs/WEB_PRIVACY.md`](WEB_PRIVACY.md) (the browser page and your data),
[`sql/CONNECTOR_GUIDE.md`](../sql/CONNECTOR_GUIDE.md) (database settings) and
[`CHANGELOG.md`](../CHANGELOG.md) (what changed in each release).

**Status** (start at "Start here" in [`docs/ROADMAP.md`](ROADMAP.md)): every track is complete: loading,
axes, units, run search, the CLI, UV concentration (issue #1) and the CADET export (phases
C0-C5, owner decisions D1-D30). What's left is in the roadmap: the "Modeler input" questions,
open questions and possible items by goal ("Open, by goal"); ask the owner before starting any of it.

---

## Directory Layout

```
PyCORN/
|-- setup.py                  Package metadata (name=pycorn, GPLv2), extras, the `pycorn` and
|                             `pycorn-setup` commands; the version is read from pycorn/version.py
|-- README.md                 Features, install, quick start
|-- CHANGELOG.md              Release notes, newest first
|-- AGENTS.md                 Collaboration and security rules for AI agents; key files
|-- docs/
|   |-- PROJECT_SUMMARY.md    This file
|   |-- GETTING_STARTED.md    By task: browser, Excel, find runs, database setup (no programming)
|   |-- USER_GUIDE.md         User guide: Run model, roles, axes, units, tables
|   |-- EXPORT_GUIDE.md       User reference: pycorn find / export flags, files, columns
|   |-- CADET_GUIDE.md        User guide: CADET / CADET-Process export
|   |-- RUN_STRUCTURE_GUIDE.md  How phases, sections, pauses and holds are read from what ran
|   |-- RUN_STRUCTURE_REVIEW.md Run structure for experts: views, open choices
|   |-- WEB_PRIVACY.md        The browser page: how it works, why your data stays private
|   |-- ROADMAP.md            Goals, status, owner decisions, open items by goal, modeler questions
|   |-- HANDOFFS.md           What each work package built and how it was checked
|   |-- FACTS.md              Bundle, database, run log, method and CADET facts; the 2026-11-08 review
|   |-- PHASE0_FINDINGS.md    Database spike: schema, units, Vc coverage, pause texts
|   |-- images/               Figures and screenshots used by the docs
|-- pycorn/                   Python package
|   |-- __init__.py           Exports parsers, model, open_run/open_folder, connector,
|   |                         load_files, export_cadet
|   |-- __main__.py, cli.py   `pycorn find` / `export` / `bypass-uv` (also python -m pycorn)
|   |-- setup_env.py          `pycorn-setup`: writes the per-user .env by questions, tests the connection
|   |-- messages.py           plain_error(): one plain sentence for file and memory errors
|   |-- version.py            __version__ (set only here), git commit, version_text()
|   |-- run_export.py         Export writers: layout, tables, stamps, CADET outputs
|   |-- export_tables.py      One name per table for page, Excel and CSV; origin per column
|   |-- excel_export.py       The .xlsx of `pycorn export`: summary sheet, chart, formatted tables
|   |-- run_details.py        Run-level tables (method, calibration, instrument, settings, audit log)
|   |-- curve_sampling.py, resample.py   Choose curves and thin them for the curves table
|   |-- browser.py            Python side of the in-browser page (Session)
|   |-- pycorn.py             Legacy file parsers: PcRes3, PcUni6
|   |-- utils.py              import_xml_as_df(): UNICORN 6 zip -> DataFrame
|   |-- bundle_codecs.py      MS-NRBF and nested-zip decoders for UNICORN 6+ bundles
|   |-- model.py              Run, Chromatogram, Curve, EventCurve, PeakTable, Entry, DbTable
|   |-- sources/              zip_source (open_run), ufol_source (open_folder),
|   |                         sql_source (PyCORNDatabase.load_run)
|   |-- db_connector.py       PyCORNDatabase, DataFrameResult, functional API, CLI
|   |-- db_sources.py         Several servers/databases from .env, discover_databases()
|   |-- search.py             find() / find_pages() / count() / variables() / pick_list()
|   |-- file_catalog.py       catalog_from_files(), load_files(), match_zip_to_db()
|   |-- variables.py          run.variables(): method variables as the run used them
|   |-- curve_roles.py        Find curves by role (uv, conductivity, ph, conc_b, flow, ...)
|   |-- axes.py               add_axes(run): Vc, CV and UTC for every curve and event
|   |-- units.py              Unit table, conversions, SI, UV path length
|   |-- column_volume.py      Vc by source priority, column by-pass, CV
|   |-- column_geometry.py    Bed height, diameter, particle radius from ColumnTypeData
|   |-- timeline.py           UTC from the method start and the run log's pauses
|   |-- event_tables.py       Phase, injection, fraction tables
|   |-- concentration.py      mg/ml from UV and a caller-given extinction coefficient
|   |-- uv_calibration.py     extinction coefficient from sample sent to the column by-pass (issue #1)
|   |-- run_log_sections.py   Process sections from the executed run log
|   |-- method_hints.py       The method as hints only: planned vs executed
|   |-- exporters/cadet/      build_experiment() -> CadetExperiment (SI); calibration,
|   |                         buffer readings; CADET-Process and CADET-Core 5/6 writers;
|   |                         api.py: export_cadet()
|   |-- sql/                  Query files shipped with the package:
|   |                         unicorn_cte.sql (folder summary, query()/CLI),
|   |                         results/find_results.sql (run catalog behind find())
|   |-- docs/                 Upstream docs: usage, changelog, license, .res layout
|-- examplescripts/
|   |-- pycorn-bin.py         CLI: plot/export .res/.zip files (matplotlib, xlsxwriter)
|   |-- cadet_walkthrough.py  UNICORN run -> CADET-Core simulation next to what was measured
|-- web/
|   |-- index.html            Template of the in-browser page (Pyodide + Plotly)
|   |-- i18n/                 Page texts: en, de, fr, it, sv
|-- .github/workflows/pages.yml  Builds and deploys the page to GitHub Pages
|-- tools/
|   |-- anonymize_zip.py      Sanitize a result .zip before it leaves the lab
|   |-- build_web_page.py     Build the one-file page (wheels embedded)
|-- samples/
|   |-- sample.zip, u711_column_test.zip, u711_manual_run.zip, u711_proa_day12.zip,
|   |   u711_cadet_reference.zip, u73_blank.zip
|   |                         Anonymized UNICORN 6+ result bundles used by tests
|   |-- SAMPLE_ZIP_CONTENTS.md  Entry-by-entry contents; identifying curves
|-- tests/                    pytest: parsers, ZIP/UFol/SQL readers, axes, search,
|                             sources, variables, roles/geometry, run log, CADET
|-- unicorn_test_files/       Lab exports (gitignored); tests use them when present
|-- sql/                      Connector support files
    |-- .env.example          Credential template (real .env is gitignored)
    |-- requirements.txt      pymssql, pandas, xmltodict
    |-- scripts/              Admin scripts (not used by the code)
    |   |-- create-user.sql   Creates a read-only login/user on UNICORN DBs
    |   |-- drop-user.sql     Removes that login/user
    |-- test_connection.py    Connection smoke test using .env
    |-- test_validation.py    Offline checks plus a connection attempt
    |-- example_usage.py      Functional API walkthrough
    |-- example_usage_pycorn.py  PyCORNDatabase walkthrough
    |-- CONNECTOR_GUIDE.md    Setup, sources, API, filtering, large results, troubleshooting
```

---

## Reading Runs

| Entry point | Returns |
|-------------|---------|
| `open_run(path)` | `Run` with everything in a UNICORN 6+ bundle: curves, events, peak tables, result and run info, method/system metadata, audit log, every raw entry. `run.to_res3_dict()` gives the `PcUni6` dict. |
| `open_folder(path)` | One `Run` per result in a `.UFol` folder export. |
| `PyCORNDatabase().load_run(row_or_id)` | The same `Run` from the database, plus `run.tables` (every row tied to the result). Tested against a lab environment: equal to the same results exported to `.zip` and `.UFol` files (`tests/test_sql_source.py`, run only when `sql/.env` exists and the server answers). |
| `PcRes3`, `PcUni6`, `utils.import_xml_as_df()` | Legacy dict / DataFrame output, unchanged. |

Curve and event names differ between instruments: look curves up with
`chrom.curves_with_role()` or `curves_of_type()`, never by a hardcoded name.

## Database

Connects over TCP/IP with SQL Server authentication. Credentials come from a `.env` file
(`UNICORN_SERVER`, `UNICORN_DATABASE`, `UNICORN_USER`, `UNICORN_PASSWORD`, optional
`UNICORN_PORT`; more databases and servers through `UNICORN_DATABASES` and
`UNICORN_SOURCES`). The file is the first of `PYCORN_ENV_FILE`, `./sql/.env`, `./.env` and
the checkout's `sql/.env` (`find_env_file()`); `UNICORN_*` environment variables override it
key by key (`load_env()`). With no server set, `localhost` is used only if a SQL Server
answers there (warned), else `ValueError`. Output names a source by its alias only.

- **Search** (`pycorn/search.py`): `db.find(...)` returns one page of the run catalog, every
  filter applied in SQL; `find_pages()`, `count()`, `pick_list(field)` and
  `variables(page)` go with it. Never a whole-database query. Missing columns in older
  schemas fall back instead of dropping the database.
- **Folder summary** (`query()`, CLI): `pycorn/sql/unicorn_cte.sql`, with server-side
  `@FolderFilter`, `@DescriptionPattern` and `@HideHidden` (the connector declares them).
- Errors are raised as `ConnectionError` with server, user, password and database redacted.

Details: [`sql/CONNECTOR_GUIDE.md`](../sql/CONNECTOR_GUIDE.md).

## CADET Export

`pycorn.exporters.cadet.build_experiment(run, ...)` returns a `CadetExperiment` in SI units,
built from what the run executed (`run_log_sections()`), not from the method. UV is
exported as AU/cm; mg/ml and salt need caller-given calibration. Buffer conductivity and pH
are read where the column is by-passed. The method is read only as hints
(`pycorn.method_hints`): planned injections and block lengths, with the run's variable values,
against what ran, plus sample-application, BufferPro, variables and documentation; nothing
from the method changes a section. `pycorn.export_cadet(run, folder, core_version=...)` (or
`pycorn export --cadet` / `--cadet-core 5|6`) writes CADET-Process CSVs with
`experiment.json`, and a CADET-Core 5.1.2 or 6.0 input file. Model values go in through
`core_overrides`, in CADET's own names; missing ones are filled with listed placeholders.
Details: [`docs/CADET_GUIDE.md`](CADET_GUIDE.md).

## Export to Files

`pycorn find` saves a search as CSV; `pycorn export` writes each run (from that CSV or from
`.zip` / `.UFol` files) to `<out>/<alias#n>/<folder tree>/<run>/`. It writes a file per
chromatogram and tables for phases, sections, Vc and warnings, with opt-in mg/ml, units,
geometry and CADET outputs. Runs already there are skipped; `--update` exports the changed
ones again. Details: [`docs/EXPORT_GUIDE.md`](EXPORT_GUIDE.md).

---

## Open Issues

- `pycorn-bin` (seen while fixing its axes, 0 nm channels and `-r`/`-i` for zips): `-r 0` or a
  negative value breaks; `--xmin`/`--xmax` outside the data can make the y scaling fail; one
  real export scales to about -17 mAU; a `.res` file (none in this repository) had its pH plot flat near 146. Curve
  styles and the `--par1` default still use instrument curve names.
