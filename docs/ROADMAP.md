# PyCORN Roadmap: Status, Decisions and Open Work

This file replaces the three planning documents `PLAN_unified_sources.md` (loading, axes,
units, search, `pycorn export`), `PLAN_cadet_export.md` (CADET export) and
`PROPOSAL_run_search.md` (run search design) as of 2026-11-08. The public git history was
rebuilt on 2026-11-12 and no longer holds their earlier text.
User references stay in the guides: [`docs/USER_GUIDE.md`](USER_GUIDE.md),
[`docs/EXPORT_GUIDE.md`](EXPORT_GUIDE.md), [`docs/CADET_GUIDE.md`](CADET_GUIDE.md),
[`sql/CONNECTOR_GUIDE.md`](../sql/CONNECTOR_GUIDE.md); database findings in
[`docs/PHASE0_FINDINGS.md`](PHASE0_FINDINGS.md); bundle layout in
[`samples/SAMPLE_ZIP_CONTENTS.md`](../samples/SAMPLE_ZIP_CONTENTS.md).

## Goals

One row per goal: where it stands and what comes next. Details live in the sections named.

| Goal | Status | Next step (owner decides) | Where |
|------|--------|---------------------------|-------|
| Read runs from zip, UFol and SQL into one `Run`; axes, units, search | Built | Nothing queued | Status by track |
| Export for people and scripts (`pycorn export`, Excel, web page) | Built, 0.24.0 | Run-structure decisions change column names; see below | Status by track, [`CHANGELOG.md`](../CHANGELOG.md) |
| Non-programmers: setup, Excel, plain messages, web page | Built | Web page review 2026-11-10 (W1-W17): 0.32.0-0.37.0 released (downloads, tables, report round 2, W16, W11 overlays, W17 chromatogram selection, run list ticks and M22's page table in 0.37.0); W12 after the phase decisions; then PyPI | Status by track: "Web page review (owner, 2026-11-10)", Open, by goal |
| Modelers: CADET export and UV / salt calibration | Built (C0-C5) | A modeling SME settles M1-M13 and D30; per-phase curve statistics built (M22, 0.35.0, interim phase rule; page table and `pycorn windows` 0.37.0); UV baseline / breakthrough: proposed, under owner review, test data by CADET simulation (nothing built) | Modeler input (M1-M9), Status by track: UV concentration and signal calibration (issue #1, M10-M13, M22) |
| Run structure: how phases, steps and sections reach each audience | Active; eight decisions made; the page's chart tracks and x axes built; inlets from the system configuration built (0.35.0); held-over folding decided (Decide 7-10, 2026-11-12, not built) | Track 0 (owner decisions): phases, repeats, lead-in and feed volumes (the other Decide items of 1-26), the enabler for CADET and M22's final phase rule; a new sample with a ramp | Run structure, "Web page review" release plan, [`RUN_STRUCTURE_REVIEW.md`](RUN_STRUCTURE_REVIEW.md) |
| Agents and scripts: everything the page and CLI do, usable from code | A1, A2, A6, A9 built (0.36.0); A3, A4, A5, A7, A11 built (0.37.0) | A8 (the page's analysis logic in a public module); the MCP server (A10) paused (owner, 2026-11-12) | Track 9 in "Web page review" tracks; `local/AGENTIC_AUDIT.md` |
| Upstream merge into ronald-jaepel/PyCORN | Issue #4 open, no reply; stage 1 branch pushed, PR 1 not opened | Decide how to proceed, then freeze `upstream-base` | Upstream merge |
| Search and export extras (file-catalog filters, local index, tidy tables) | Ideas only | Ask first, with options | Open, by goal |

Where the rest is: "Start here" (current state and rules), [`HANDOFFS.md`](HANDOFFS.md)
(what was built in each work package, how it was checked, what is not checked),
"Decisions" (every owner decision), [`FACTS.md`](FACTS.md) (bundle, database, run log,
method and CADET facts, and the 2026-11-08 review).

## Start here

**Built.** Every track is complete: reading runs from `.zip`, `.UFol` and the UNICORN
database into one `Run`; time, volume, CV and UTC axes; units, Vc and event tables; run
search over several databases and over files; `pycorn find` / `pycorn export`; UV
concentration with accuracy notes and by-pass epsilon (issue #1); CADET export for
CADET-Process 0.12 and CADET-Core 5 / 6 (phases C0-C5, decisions D1-D30). The full review of
2026-11-08 found 26 items (a-z); a-g, n and o-z are fixed. For
non-programmers (2026-11-08): `pycorn-setup`, the Excel workbook for people,
plain-language messages (item 5) and the in-browser page (item 6) are built (Decisions,
"Usability for non-programmers"); getting-started docs and PyPI are roadmap item 7 (open:
`docs/GETTING_STARTED.md` is written, nothing is on PyPI).

Since 2026-11-09 (releases 0.21.0-0.24.0, tags `v0.21.0`-`v0.24.0`; what changed is in
[`CHANGELOG.md`](../CHANGELOG.md)): run-level tables on every export path and
`--mask-identifying` (Decisions, "`pycorn export`"); one name per table for page,
Excel sheet and CSV, an origin per table and column (UNICORN / User input / PyCORN calculated /
PyCORN analysis), `column_guide.csv`, `README.txt` and `provenance.json` per export;
"Planned vs run" tables (renamed from method hints); Scouting runs says where it comes from;
a column picker on the page's wide tables; phase, fraction and pause tracks and an x-axis
picker on the page's chart (0.24.0). Output names changed in 0.21.0 and 0.22.0, so
scripts reading the old CSV names need the CHANGELOG tables. Every release bumps
`pycorn/version.py` and adds a CHANGELOG entry (AGENTS.md).

**Active.** Run structure: how the instrument records a run, how models should receive it
and how general users want to see it (status track "Run structure";
[`docs/RUN_STRUCTURE_REVIEW.md`](RUN_STRUCTURE_REVIEW.md) for experts,
[`docs/RUN_STRUCTURE_GUIDE.md`](RUN_STRUCTURE_GUIDE.md) for new users).
Under discussion with the owner; seven decisions made (listed under "Run structure" below). Built:
the web page's phase, fraction and pause tracks and its x-axis picker (Decide 4, 5). The table
decisions are not built yet (checked: no `original_time_unit` column exists in the code).

**Open.** Build nothing below without the owner.

- "Modeler input" (end of this file): M1 and M3-M9, the CADET physics items i-m of the review,
  the flow path by system, the real-value check, and how parameters come in (M2 is built).
- Issue #1, UV concentration and signal calibration (M10-M13: nonlinear UV, polynomial fits,
  UV baseline, UV-to-conductivity volume; the CADET forum comparison): "Status by track", "UV
  concentration and signal calibration (issue #1)".
  Both wait for a modeling SME and the owner.
- Owner questions: D30 (assays as export options). D28 (6) is settled (2026-11-11, W17): the
  original chromatogram by default; every selected chromatogram exported, with a warning.
- "Open, by goal": every open question and possible item, grouped by goal (modelers, run
  structure, non-programmers, search and export): file-catalog filters, a local index, export
  ideas, getting-started docs, PyPI.
- "Upstream merge (ronald-jaepel/PyCORN)": planned in five PRs. Remote `upstream` is added.
  The `pr1-parser-packaging` branch was removed with the history rebuild (owner,
  2026-11-12); stage 1 starts again from the rebuilt history. Issue #4 to Ronald is open
  (opened 2026-11-09, no replies when last viewed). PR 1 has not been opened.
  `upstream/master` is not merged into the fork yet.

**Handoffs.** What each work package built, how it was checked and what is not checked
(non-programmer usability, plain-language messages, in-browser page)
is in [`HANDOFFS.md`](HANDOFFS.md). Read it before changing `excel_export.py`, `messages.py`,
`cli.main()` exit codes, `browser.py` or `web/`; the page's offline and privacy promise is
stated there and in the README. Item 7 (getting-started docs, PyPI) is open.

**Rules for agents** (beyond [`AGENTS.md`](../AGENTS.md)):

- Build from what ran: the executed run (event) log is the ground truth (D13). The method is
  read only for hints and never sets a section, a time, a length or a hold (D17). Measured
  curves are a check: warn, never correct.
- Never hardcode curve or event names, and don't assume the known names, types or units are
  complete. Look up by role (`curve_roles.py`) or stored type; take names from the caller.
- D28: pycorn sits on top of CADET, never copies it. Model values go in only in CADET's own
  names, as one generic block per target that pycorn passes on unread (`core_overrides` /
  `--core-overrides`, `process_overrides` / `--process-overrides`). No pycorn names for CADET
  parameters, no comparison or fitting code.
- Ask the owner when a design choice is open (which log entry counts, an edge case, what a
  stored field means), with numbers from the data and a recommended option. Don't build on an
  assumption.
- Commit only your own hunks, with a fresh message file (`git commit -F`), and never amend.
  When a file also holds someone else's edits, stage your hunks from a patch (HEAD blob plus
  your parts, `git hash-object -w --no-filters`, `git update-index --cacheinfo`).
- No heredocs; temp scripts run with `python -I`. ASCII console output; wrap UNICORN text in
  `ascii()`. Never print connection values, `run.origin`, run-log texts or `.asc` rows (they
  hold paths, user and computer names).
- Database access is read-only (`SELECT` only). Don't commit matched ZIPs, data extracted from
  the database or fixtures built from them; the anonymized zips in `samples/` are the only
  committed run data. Don't modify, move or delete anything in `unicorn_test_files/`.
- Local vs universal (AGENTS.md): no facts about the lab's databases, instruments, files or
  machine (counts, result IDs, run names, paths) in tracked files; say "tested against a lab
  environment".
- SQL files: no `STRING_SPLIT`, `OPENJSON`, `STRING_AGG`, `TRY_CONVERT` or anything else that
  needs compatibility level 130+ (UNICORN databases can be at 100). New SQL the code reads
  goes under `pycorn/sql/`.
- After any change to `timeline.py` or `column_volume.py`, rerun the live parity test and the
  axes survey: a change can be right on the sample runs and wrong on database results.

**Testing notes.**

- Full suite: `python -m pytest` from the repository root (counts change: run it, don't quote old
  ones; the CADET-Core 5/6, CADET-Process, openpyxl and live-database tests skip when their
  dependency or server is missing). The live database tests run only when
  `sql/.env` exists and the server answers; they never print connection values.
- CADET-Process / HDF5 tests need a venv with `CADET-Process==0.12.0`, h5py, pytest, pandas,
  xmltodict (a venv outside the repo). Opt-in simulations: set `PYCORN_CADET_CLI_5` /
  `PYCORN_CADET_CLI_6` to a `cadet-cli` from venvs with `CADET-Core==5.1.2` /
  `CADET-Core==6.0.0a5`, run `tests/test_cadet_core_run.py` with that venv's Python.
- Expected Core files `tests/data/core_input_reference_v5.json` / `_v6.json` (hand-checked
  on `samples/u711_cadet_reference.zip`): regenerate with `PYCORN_REGENERATE_EXPECTED=1` only after an intended change,
  and check the diff.
- xlsx tests: xlsxwriter installs with PyCORN; the workbook checks also need openpyxl
  (`pip install openpyxl`), else they skip.
- Run `tests/test_cli.py` and `tests/test_export_cadet.py` before changing anything the CLI
  or `export_cadet()` calls (`cadet_outputs`, `_file_stems`, `_geometry_vc`,
  `build_experiment`, `to_csv`, `to_core_input`, `write_json` / `write_hdf5`, `uv_wavelengths`).
- Survey scripts are throwaway, outside the repo, and print counts only.
- The code was tested against a lab environment; tests that need it skip elsewhere.
- CADET sources for reading (never run): CADET-Core `v6.0.0a5` and `v5.1.2`, CADET-Process
  `v0.12.0` (`github.com/fau-advanced-separations/CADET-Process`; the `github.com/cadet/` copy
  has only an old `main`), CADET-Python `v1.3.0`.

## Status by track

### Sources and model (ZIP, UFol, SQL)

Goal: load every relevant piece of a run from a UNICORN 6+ `.zip`, a `.UFol` folder export or
any UNICORN SQL Server database through one API, with identical results.

- `pycorn.open_run(path)`: a `.zip` or a `.Result` taken out of a folder export
  (`sources/zip_source.py`, `bundle_codecs.py`, `model.py`). Nothing is dropped: raw bytes of
  every entry are kept, undecodable ones go to `run.warnings`. `open_run(path,
  adjust_axes=False)` keeps every axis as stored.
- `pycorn.open_folder(path)`: every result in a `.UFol` (an uncompressed tar of `.Result`
  zips), read in memory; `run.container` has folder, name, `original_id`, `original_path`
  (`sources/ufol_source.py`; `iter_ufol()` reads one run at a time).
- `PyCORNDatabase.load_run(result_id | catalog row, source=, database=)`
  (`sources/sql_source.py`, `read_result(cursor, id)`): the same `Run`, `source = "sql"`, plus
  `run.tables` (every row tied to the result). Rows are found by walking the declared foreign
  keys (`collect_result_rows`): owned rows below `dbo.Result` (37 tables can hold them),
  linked rows behind a link table only this result uses (`ResultDoc_AuditTrail` ->
  `AuditTrail` -> `AuditTrailEntry`, the evaluation log) and referenced rows plus parents.
  Ownership follows only ownership keys (`SchemaGraph.ownership_keys`: foreign keys in the
  primary key, else mandatory ones); nullable links are cross-references. `build_run` lays the
  rows out as the bundle XML and reuses the ZIP builders. The instrument
  (`Result.SystemName` -> `UCSystem` -> `InstrumentConfiguration`, `IMPLIED_KEYS`) is added as
  referenced rows.
- `run.to_res3_dict()` equals `PcUni6.load_all_xml()`. `PcUni6` decodes arrays exactly (it used
  to drop the first 5 and last 12 points); `import_xml_as_df` zeroes at the injection UNICORN
  uses, returns NaN outside the data and has `axis="time"`.
- Checked against a lab environment: exported results equal their database rows (curve
  values and stored volumes bit-identical, the rest within the XML's 7 digits; database
  timestamps keep microseconds); every database result loads, with chromatogram, curve and
  event counts equal to direct `COUNT`s.
- Accepted limits: `run.unicorn_version` is None for database runs (the bundle attribute is
  the exporting software's). Instrument rows are the system's settings now; the run-time
  snapshots are `SystemData` / `InstrumentConfigurationData`. Imported results often name a
  system the database lacks: then no instrument rows. Peak values database vs
  ZIP are not compared yet. The legacy `ResSource` (`.res` files) is not built.

### Axes, units, Vc, UTC

- `pycorn.axes.add_axes(run)` runs at load in every reader: `cv`, `utc`, Vc and the timeline,
  the same for ZIP, `.UFol` and database runs. Call it again, or pass `column_volume=` to
  `open_run` / `open_folder` / `load_run`, to override Vc; `geometry_volume=` gives Vc from
  caller geometry as a last resort (D26). Warnings are tagged `<chromatogram>: Vc|CV|UTC: ...`
  (`axes.is_axes_warning`).
- `units.py`: unit table (`UNITS`, `CANONICAL`), `convert`, `uv_path_length_cm`, `to_si()` /
  `si_unit()` / `SI_UNITS` (C0), `check_units()`, mass units (mg, g, ug) and ug/ml (issue #1);
  `run.normalized(units=, uv_path_length_cm=)` returns converted copies with
  `curve.conversion` (axis conversions in `conversion["axes"]`).
- `column_volume.py`: Vc candidates by priority (`chrom.column_volume_info`,
  `run.column_volume*`), the column valve path (`chrom.column_path`), `curve.cv` / `event.cv`.
- `column_geometry.py`: bed height, diameter, particle radius (SI) from the run's
  `ColumnTypeData`; checked against Vc; caller values as fallback or override (D26).
- `timeline.py`: `chrom.timeline` (anchors, pauses, residual, quality; `CLOCK_STOPS`,
  `GRID_STARTS`), `curve.utc` (numpy datetime64[us], UTC), `event.utc` (numpy datetime64[us],
  naive UTC; breaking). `start_utc` / `end_utc` stay aware.
- `event_tables.py`: `chrom.phase_table()`, `injection_table()`, `fraction_table()` (start/end
  time, volume, CV, UTC; leading pseudo-rows) and `chrom.zero_offsets(kind, number)`.
- `concentration.py`: `concentration(curve, epsilon=, fallback_path_cm=, fallback_basis=,
  response_factor=)` -> mg/ml curve; `ACCURACY_NOTE`.
- `curve_roles.py`: roles (UV, conductivity, pH, Conc B, `conc_q`, `flow`, `sample_flow`, ...).
- Accepted limits: Vc from column records (`ColumnTypeData` / `ColumnIndividualData`) is not
  a Vc source (owner); no Vc mapping file (callers pass `column_volume=`); stored extinction
  coefficients are not used (always empty); UTC/CV are missing on curves with one stored axis
  (F18, F23; noted since review w). UNICORN's zero-at-phase UI setting is read but unverified.
  Residual over 2 s after the UTC rebuild stays on a few runs (2-6 s, warned, not placed).
  The UV path-length fallback label is still plain 'fallback' (the owner asked only about
  geometry labels).

### Run search and catalogs

- `run.variables()` / `run.variable(name)` (`variables.py`): `kind`, `variable`, `value`
  (stored text), `number`, `unit`, `scouting`; `variable()` raises with the available names.
- `db.find(limit=500, after=, dedupe=, sources=, **filters)`, `find_pages()`, `count()`,
  `variables(page)`, `pick_list(field)`, `pycorn.search.wide(page, table)` (`search.py`,
  `pycorn/sql/results/find_results.sql`). Every filter in SQL; pages of 1-5000 rows by
  ResultID; `search.FILTERS` / `check_filters` name the closest filter on a typo.
- Several servers and databases (`db_sources.py`): `UNICORN_SOURCES`, `<ALIAS>_DATABASES`
  (list or `*`), `<ALIAS>_PORT`, `sources=` (also the CLI text forms such as `'default:*'`),
  `sources="discover"`, `discover_databases()`, `SchemaProfile` (columns, compatibility level).
- `db.load_many(catalog)` (generator; database, file and mixed rows), `pycorn.load_files()`,
  `catalog_from_files(paths)` (light read), `db.match_files()` / `match_zip_to_db()`
  (`file_catalog.py`). Catalog: `source`, `database`, `result_id`, `match_key`, `n_copies`,
  `duplicate_group`, `modified_utc`, `tstamp`, ...
- Timing (tested against a lab environment): a 500-row page under 0.5 s,
  `count()` and variables of a page well under 0.2 s; a discover walk over two databases
  under 3 s; loads under 1 s per run.
- Accepted limits: `column=` searches only the run-time `ColumnInformation` (it often says
  only `Any`). The curve-role filter reads every curve row once per connector.
  `find(after=<ResultID>)` works for one database only (pass the page otherwise). Copies
  between databases are tested on synthetic catalogs. The copy lookup seeks `IX_Result_SystemName`
  (`Chromatogram.Created` has no index). `duplicate_group` is NaN for single copies;
  `n_copies` is `<NA>` without a key. A file catalog has no filters (filter in pandas).

### `pycorn export`

- `pycorn` console script and `python -m pycorn` (`cli.py`, `__main__.py`): `pycorn find`,
  `pycorn export`, `pycorn bypass-uv`. `run_export.py`: layout, writers, `ExportOptions`,
  `cadet_outputs()`. User reference: [`docs/EXPORT_GUIDE.md`](EXPORT_GUIDE.md).
- Built in steps: default outputs, Vc from caller geometry, D27 flags, file rows in
  `load_many`, review fixes and `--update`, library follow-ups, user guides.
- Writes per chromatogram: curves (`.csv` every curve side by side with time, volume, adjusted
  time/volume, CV, UTC; `.xlsx` sheets; `.pkl` res3 dict), `_events`, `_peaks`, `_phases`,
  `_injections`, `_fractions`, `_sections`, `_pauses`; per run `variables.csv`,
  `run_info.csv` (with `export_stamp`, `match_key`), `warnings.csv`; `index.csv` in `--out`.
  CSVs are UTF-8 with BOM (`run_export.CSV_ENCODING`). Opt-in flags: see "Flag decisions".
- Checked against a lab environment (database and file runs) with every flag; re-export,
  `--update` and a faked `tstamp` change.
- Accepted limits (owner): a database run is compared by the search CSV's `tstamp` (run `find`
  again for fresh changes); a database or CSV without `tstamp` is re-exported by every
  `--update`; a file touched without a content change is hashed again on each export; stamps
  written before the hash was added match on time only. Collisions are settled within one export's
  selection, so a different selection can name a run differently. Curve CSVs are large (tens of
  MB per chromatogram, NaN-padded, over 100 columns). A sheet over 1,048,576 rows fails that
  run (checked before writing; use csv). The README stays short on purpose (owner): details go
  in the guides.
- Excel for people (2026-11-08, `pycorn/excel_export.py`): `<run name>_<chrom>.xlsx` with a
  `summary` sheet first, a native chart sheet, formatted tables, empty tables left out; see
  "Usability for non-programmers" under Decisions. Checked on the three samples, rendered with
  LibreOffice (Excel itself not checked).
- `pycorn-setup` (`pycorn/setup_env.py`): writes the per-user `.env` by questions and tests
  the connection (same section).

### UV concentration and signal calibration (issue #1)

GitHub issue #1: a theoretical extinction coefficient can be 0-15 % off. One topic: what is
built, the owner's decisions, how it compares with the CADET forum advice on signal
calibration, and what is open (M10-M13, a follow-up idea). Everything else about UV
concentration and calibration is here; "Modeler input" and "Open, by goal" point back.

#### Built

- `concentration.ACCURACY_NOTE` is in the warnings of every mg/ml and CADET concentration
  (coefficient vs detector response, saturation, non-linear absorbance, baseline drift, path
  length), kept for measured or corrected coefficients too.
- `pycorn/uv_calibration.py`: `bypass_uv_readings()` reads the UV of sample sent to the column
  by-pass (the D18 windows from the run log) per window and channel; plateau: epsilon = (max -
  before) / (path x c); pulse: area / (path x loop volume x c). Flags flat tops, a caller
  absorbance limit, cut pulses, drift. `epsilon_from_injection()` for a caller window and mass
  (warns below `NO_SIGNAL_AU`).
- CLI: `pycorn bypass-uv`; `export --bypass-uv --sample-conc --loop-volume --absorbance-limit
  --uv-response`. Python: `response_factor=` on `concentration()` and `build_experiment()`,
  `uv_response` / `absorbance_limit` in `export_cadet()`. No defaults for any of them.
- Tested against a lab environment: some runs send sample to the by-pass, as plateaus (can
  sit near the linear limit) or short pulses (much narrower than their valve periods).

#### Owner decisions (2026-11-08)

- The accuracy note is on every concentration, generic in wording (saturation, Beer-Lambert
  non-linearity; "approach all with care, even the normalization approach"), even for a
  calibrated or corrected coefficient.
- Existing runs' by-pass injections feed the epsilon estimate automatically (the D18 windows).
- Plateau: the maximum (no integration). Pulses: the area too ("max, plus area for pulses").
- A pulse needs the caller's loop volume: the valve-period volume is only an upper bound.
- Absorbance limit: caller only, always noted. Response factor: caller only, no default (no
  built-in 8 %).
- The README acknowledges alters9595 (owner request).

#### CADET forum advice vs PyCORN (2026-11-08)

> **For the modeling SME: M10-M13 below need your view.** Each changes what the CADET files
> say about the measured signals; nothing is built until the owner settles it (D2, D28:
> caller inputs only, no built-in defaults).

Source: alters' reply (23-24 Apr 2023) in "Unicorn files imported to CADET-Process",
https://forum.cadet-web.de/t/unicorn-files-imported-to-cadet-process/673. How each piece of
advice compares with what PyCORN does today:

| Advice (alters) | PyCORN today | Status |
|---|---|---|
| UV -> concentration, one component: by-pass dilution series, peak heights, 2nd-order polynomial through zero | Linear Beer-Lambert with a caller epsilon (`concentration.py`, `cadet/calibration.py`) and an optional scalar `response_factor`; `uv_calibration.py` measures epsilon from by-pass sample in a run (plateau max, or pulse area / loop volume) | **Gap: M10** (no multi-point or nonlinear UV calibration) |
| Simpler: an apparent extinction coefficient, only if linear | The main path; `ACCURACY_NOTE` on every result | Matches |
| Saturation depends on wavelength and path (about 3500 mAU at 280 nm; 2 mm cell about 15 mg/ml) | No limit assumed; caller `absorbance_limit`, flat-top warning | Matches (flagged, not corrected) |
| Check for and correct baseline shifts | `uv_calibration.py` takes a baseline under pulses and warns on drift; exported UV curves are not corrected | **Partial gap: M12** |
| Several components: fractions and offline assays (NanoDrop, SEC, cIEF) | D30: `build_experiment(components=, fraction_masses=)`, CADET_GUIDE "Your offline assays", walkthrough | Matches; D30 open (assays as export options) |
| Conductivity -> salt: %B steps in by-pass, 2nd-order polynomial | D22 `buffer_readings.py` reads by-pass buffer windows routine runs often already have, checked against the column outlet; caller `buffer_salt` -> `salt_calibration_points()` -> `salt_from_conductivity()` (linear interpolation, clipped, or a caller function) | Mostly matches; a fit only through a caller function: **M11** |
| pH: calibration less needed; conductivity helps explain pH shifts | pH read in the by-pass and compared with the outlet (`PH_MATCH` 0.1); no calibration | Matches |
| Dead volume: correct for it in post-processing if the model has no tubing | D27 mixer and tubing CSTRs in the model (tubing volume from the caller); stored "Delay volume" settings reported as hints | Different on purpose: modeled, not shifted |
| Tubing between the detectors (UV cell to conductivity cell) | Not handled: UV and conductivity are taken at the same point | **Gap: M13** (small at lab scale) |

Where PyCORN differs on purpose: the peak height of a short pulse is below the sample's own
absorbance (dispersion), so
`uv_calibration.py` uses height only for plateaus and the area for pulses. Calibration data
comes from what routine runs already hold; dedicated calibration runs still work through
caller inputs.

#### Open

Build nothing below without the owner.

- **Follow-up idea:** use a run's own by-pass epsilon for its mg/ml curves automatically (not
  built; ask first).
- **M10 Nonlinear UV calibration.** Let `uv_concentration()` (and `concentration()`) take a
  caller calibration as a table of (absorbance per cm, mg/ml) points or a function, as
  `salt_from_conductivity()` does; a helper could gather `bypass_uv_readings()` from several
  runs or dilutions into those points. Fits issue #1. Questions: table, function or both; how
  the conversion is recorded in the export; behavior outside the calibrated range (clip, warn,
  or no value).
- **M11 Polynomial fit through zero** for the UV and conductivity tables, beside today's
  linear interpolation (alters' 2nd-order fit as one option, not the only one). Questions:
  which order(s); through zero always or optional; whether the fit and its residuals go into
  the export.
- **M12 UV baseline correction on export**, off by default, recorded in the conversion info
  and warnings. Questions: method (a line between caller-named points or windows, the
  equilibration section's level, a run-log-based reference); per curve or per run; whether
  CADET references should ever be corrected or only overlays. A nominal and a measured
  (window) baseline are proposed: "UV baseline and breakthrough (proposal)" below; the drifting-baseline line and CADET references stay open.
- **M13 Volume between the UV and conductivity cells**, caller-given, to shift conductivity
  (and pH) against UV, or a second outlet CSTR in the model. Questions: shift the data or
  model it (D27 models the tubing); whether any system stores it (the stored "Delay volume"
  settings seen so far are monitor to fraction collector).
- **M21 Dynamic binding capacity (DBC / QB at x % breakthrough)**, in mg sample per ml column:
  titer x load volume at x % of feed / Vc. Planned after the breakthrough curve (proposal below). Open:
  the load-volume basis and the hold-up (system volume) normalization, x from the caller,
  one value per load. The web page gets breakthrough once this is defined.
- **M22 Per-phase curve statistics: built in 0.35.0** (owner, 2026-11-10 / 11-11;
  `pycorn/window_stats.py`, handoff in [`HANDOFFS.md`](HANDOFFS.md)). For each phase and each
  curve the caller names or picks by role, statistics over windows relative to a phase:
  phase name plus offsets in time, volume or CV from the phase start or from its end
  (`--window "NAME=PHASE:FROM:TO UNIT"`, repeatable, with `--window-curves`; `end-2:end` for
  "the last 2 CV"; Python `phase_windows()` / `window_stats()` / `window_stats_rows()`).
  No default window or curve. One long table (`phase_window_statistics`): a row per run x
  phase occurrence x window x curve, with the window as given and the absolute start / end
  it resolved to. Owner decisions (2026-11-11):
  - statistics for now: n, mean, median, SD (sample, n-1), min, max on the curve's own
    points, bounds inclusive; a `STATISTICS` registry takes slope, start and end value later
  - interim phase rule until Decide 9 / 23 (lead-in, repeats): phase start and end as
    logged, no lead-in folding; a repeated phase gives a row per occurrence, from 1; a window
    reaching outside its phase is clipped with a warning; the table says "interim"
  - origins: the statistics "PyCORN calculated", the phase and resolved-bounds columns
    "PyCORN analysis" (built on phase ends)
  - built in 0.37.0 (owner, 2026-11-11): the page table (a folded form: a phase picker that
    writes the CLI's window text, curve roles ticked, none by default; one table over the
    ticked runs; downloads and `options.json` carry the windows) and `pycorn windows` (A7:
    the same rows without an export, exit 1 when a run, curve or window gave nothing)
  - not built: moving `run_log_sections._curve_median` and the
    `uv_calibration.py` window means onto these functions (only where tests show the same
    results); the UV baseline below builds on them

#### UV baseline and breakthrough (proposal, 2026-11-10; version set when the owner confirms)

**Proposed, under owner review; nothing is built.** Two requests together: a nominal UV
baseline the user subtracts before any normalization, and the Protein A breakthrough of
harvested cell culture fluid (HCCF). There the feed (product plus UV-absorbing impurities) goes
through the column by-pass first (the feed signal), then onto the column; the impurities flow
through at once, so the outlet level shortly after the load starts is the impurity baseline,
before the product breaks through. Today no curve is baseline-corrected and nothing computes
C/C0, a titer concentration, breakthrough or DBC; baselines exist only inside
`uv_calibration.py` (the level before the sample, the pulse edge line).

Owner decisions (2026-11-10), to confirm at the next review:

- **Formula:** `C/C0 = (Outlet - Base) / (Bypass - Base)`, 100 % of feed at full breakthrough;
  `c = C/C0 x titer` with the caller's inlet titer.
- **Measured baseline is general:** the level over a window; its use depends on the feed.
  Harvest: a window into the load, default 1-2 CV after load start, overridable (an explicit
  owner exception to "no built-in defaults"). Pure feed: a window before the load (the outlet
  buffer level when the UV was not auto-zeroed), or a nominal 0. Windows in CV from load start;
  negative = before the load.
- **Nominal baseline:** a raw stored value (what the user reads in UNICORN, at the stored
  path), subtracted in stored units before any path or unit conversion (`run.normalized`).
- **By-pass plateau level = plateau mean**, for C/C0 and for the by-pass epsilon (changes the
  2026-11-08 "plateau maximum" rule; a CHANGELOG line when built).
- **Own buffer levels:** by-pass net = plateau mean - the by-pass level before the sample;
  outlet and baseline relative to the outlet buffer level before the load. Warn on an Auto
  zero between the by-pass and the load, and when the by-pass starts from a non-zero UV.
- **Each load** gets its own C/C0, paired with the last sample-to-by-pass window before it; the
  caller can name the load or window, or give the by-pass level (runs with no by-pass).
- **Generalized epsilon** (Base = the non-target absorbance in the feed; Base = 0 gives
  today's formulas): plateau `eps = (plateau mean - before - Base) / (l x c)`; pulse
  `eps = (area - Base x V_loop) / (l x V_loop x c)`. Assumes the impurities flow through at
  their feed level (no binding, no dilution); noted on every result.
- **Units say "of what":** unit strings stay machine-readable for the units table; labels,
  column guide and docs name the basis. C/C0 is "% of feed"; concentrations are mg sample per
  ml fluid; DBC (M21) is mg sample per ml column. Curves `<UV> (% of feed)` and
  `<UV> (mg/ml, titer)`, so a chart never puts breakthrough on the %B axis and the two mg/ml
  curves are not mistaken for one. No "normalized" wording (it already means unit and path
  conversion).
- **Scope when built:** nominal and measured baseline, % of feed, titer mg/ml, caller by-pass
  level, baseline on the mg/ml path, generalized epsilon. Later: M21 DBC; the web page after
  M21; CADET files unchanged (M12 references stay open).

Review 2026-11-10 (owner): not ready to build. The owner keeps it on the roadmap to sit with
it further; the decisions above stay "to confirm". Recorded at the review:

- **Plateau mean** (owner): the mean over the detected stretch, the longest stretch within
  `PLATEAU_SPAN` of the maximum held at least `PLATEAU_MIN_S` (today's detection in
  `uv_calibration.py`); the stretch's start, end and SD go in the row. No new constant.
- **Mean or median for levels** (owner asked which is best; recommendation, to confirm):
  the median for window levels (measured baseline, buffer levels before the by-pass and
  before the load), the mean for the plateau stretch, both reported by `window_stats` with a
  warning when they differ by more than the noise. On a clean window they agree within noise,
  and on a linear drift both give the mid-window value; they differ only with spikes (air,
  valve switches) or part of a rise at the window edge, where the median gives the level and
  the mean is pulled toward the artefact. The median matches D22 (`buffer_readings`) and the
  section checks; the buffer level and the baseline are subtracted from each other, so they
  use the same statistic. The plateau stretch is already cut to a 2 % band, so the mean is
  the better estimate there. This replaces "measured baseline = mean" above if confirmed.
- **Shared functions:** builds on M22's `window_stats` / `phase_windows` (built first), so
  this adds only the baseline, breakthrough and the generalized epsilon.
- **Test data** (owner): simulate the breakthrough in CADET with tubing dispersion (D27):
  a binding product (Langmuir) and a non-binding UV-absorbing impurity; the by-pass plateau
  through the tubing, then the load; UV = sum of eps_i x c_i plus noise and a spike. Vary
  load and flow until breakthrough reaches the 1-2 CV window, to set the rising-slope
  warning. The simulated curves go in small test fixtures turned into synthetic runs, so the
  tests need no CADET install; the generator script needs `pycorn[cadet-core]`. Docs say
  "tuned against simulated breakthrough (CADET, with tubing dispersion)".
- **Still open before a build:** the owner confirms the decisions above; mean or median; the
  slope threshold (from the simulation); M12 (drifting baseline); M21 stays after this.

Conflicts between the cases and how they are resolved:

- Measured baseline on mg/ml: a general window the caller picks per use; a load window is not
  silently applied to the whole run.
- HCCF by-pass epsilon (the plateau holds impurities too): the generalized formula.
- Two mg/ml curves (Beer-Lambert and titer) can disagree: distinct names and basis labels; a
  cross-check, not a conflict.
- Zero of a nominal value: raw stored. Path rescaling: subtract before conversion; C/C0 has no
  path in it.
- Epsilon and response factor cancel in C/C0: say so in the guides.
- CADET references stay uncorrected while exports may show corrected curves (M12 open).
- A default 1-2 CV window catches early product breakthrough on an overloaded column or at
  high flow: warn on a rising slope in the window.
- Auto-applying a run's own by-pass epsilon (follow-up idea) waits on the generalized epsilon.

Concentration handling cases (first version):

| # | Case | Formula / input | Status |
|---|------|-----------------|--------|
| 1 | Raw UV | stored mAU / AU | Built |
| 2 | Path normalization | `run.normalized(uv_path_length_cm=)`; `*/cm` never rescaled | Built |
| 3 | Beer-Lambert, caller epsilon (mass, or molar + MW) | A / (eps x l), mg sample / ml fluid | Built |
| 4 | Response factor | eps x factor, caller only | Built |
| 5 | Absorbance limit | caller limit -> NaN; flat-top warning | Built |
| 6 | Epsilon from by-pass plateau | (plateau - before - Base) / (l x c) | Built with max, Base = 0; mean + Base proposed |
| 7 | Epsilon from by-pass pulse | (area - Base x V_loop) / (l x V_loop x c) | Built with Base = 0; Base proposed |
| 8 | Epsilon from a caller window + mass | `epsilon_from_injection` (Python only) | Built |
| 9 | A run's own by-pass epsilon auto-applied to its mg/ml | - | Open (after 6, 7) |
| 10 | Nominal baseline | raw stored value, subtracted before conversion | Proposed |
| 11 | Measured baseline, general window | window mean vs outlet buffer level; CV from load start, harvest default 1-2 CV | Proposed |
| 12 | Drifting baseline (line between windows) | - | Open (M12) |
| 13 | Breakthrough, % of feed | (Out - Base) / (Bypass - Base) | Proposed |
| 14 | Titer concentration, mg sample / ml fluid | % of feed x titer | Proposed |
| 15 | Caller by-pass level (no by-pass in the run) | caller value | Proposed |
| 16 | DBC / QB at x %, mg sample / ml column | titer x V_x / Vc after hold-up normalization | Planned (M21) |
| 17 | Nonlinear / multi-point UV calibration | table or function | Open (M10) |
| 18 | Polynomial fit through zero | - | Open (M11) |
| 19 | Several components (fractions, offline assays) | `build_experiment(components=, fraction_masses=)` | Built (CADET); D30 open |
| 20 | CADET concentration | AU/cm; mol/m^3 with MW | Built; baseline on references open (M12) |
| 21 | Salt from conductivity | caller table or function | Built (M11 fit open) |
| 22 | Stored UNICORN ExtCoeff (Peak, Pool, ProteinInfo) | - | Not read (empty in tested data) |
| 23 | Volume between the UV and conductivity cells | - | Open (M13) |
| 24 | Second wavelength when the primary saturates | - | Open (M17) |

Design sketch for the next review (not built):

- `pycorn/uv_baseline.py`: load windows (feed sections on the column, as
  `bypass_sample_windows`) through M22's `phase_windows` with a load-start anchor;
  `nominal_baseline()`, `measured_baseline()` as `window_stats` over the window in CV from
  load start (load start CV read from the curve's time -> CV axis, by-passed volume already
  excluded by `column_volume.apply_cv`).
- `concentration(baseline=)`: subtract in stored units; the conversion records level, source
  and window. The absorbance limit still compares unsubtracted values.
- `pycorn/breakthrough.py`: `breakthrough(run, curve, ...)` -> per load the % of feed and titer
  curves and a summary row (by-pass window and level, buffer levels, baseline and its window,
  titer, warnings, `ACCURACY_NOTE`).
- `uv_calibration.py`: plateau mean; `feed_baseline=` for the generalized epsilon.
- Export: `--uv-baseline NM=VALUE|measured`, `--baseline-window A:B`, `--breakthrough`,
  `--bypass-level NM=VALUE`, `--titer`; a new table in `export_tables.py` with origins; also
  pass `--path-length` to `bypass_uv_readings` (it is not passed today).
- Tests: synthetic runs from the `tests/test_run_log_sections.py` helpers, as
  `tests/test_uv_calibration.py` builds them (by-pass feed plateau, impurity step, later product
  rise; Auto zero; two loads; no by-pass with a caller level), and the CADET-simulated
  fixtures (review 2026-11-10).

#### CADET forum: other threads (2026-11-09)

Other UNICORN threads on https://forum.cadet-web.de (635 Extracting Data from UNICORN Files,
723 UV profiles to concentrations, 552 / 1172 / 1253 AKTA tubing, 1332 reference data). Most of
what they ask is built (pause-corrected time, `.UFol`, UNICORN 7 zips, flow rates). Checked in
`tests/test_forum_checks.py` on every sample bundle: pH keeps its decimals (the original PyCORN
read pH 8.0 as 80, thread 635 post 21; ours reads 0-14 with decimals), time axes never repeat a
point (a repeated time point broke a CADET-Match fit, thread 552; the volume axis does repeat
during pauses, as it should), and runs without a column build with a warning.

- **M14 AKTA Pure / UNICORN 7.3 users without `.res` files** (635 posts 12-16) export curves
  as ZIP, CSV or ASC. `.asc` is rejected on purpose today (`file_catalog.py`, F20/F24: curves
  only, no method, run log or axes setting). Question: stay with "export as .zip", or read a
  curves-only `.asc` into a limited `Run` (no sections, so no CADET export)?
- **M15 No-column runs as hold-up calibration** (552: blue dextran or tracer without a column
  gives the system volume for D27's tubing). Today such a run builds with column warnings only;
  nothing marks it as a hold-up run or reports a first moment / volume. Questions: detect it
  (no column named, one injection) and report the delay volume; a caller flag instead?
- **M16 Tubing dispersion** (1172, 1253): CADET stalls at zero axial dispersion (about 1e-4 for
  1 mm ID by the Taylor expression), and mixed inner diameters skew volumes. Applies to D27's
  mixer and tubing blocks; D28 says never mirror CADET parameters, so this is a note in
  CADET_GUIDE and a check that we never write 0, not a default.
- **M17 UV wavelength choice** (723): a lower-absorbance wavelength (295 nm) keeps peaks in the
  linear range (about 1500 mAU). D8 takes 280 nm primary and the caller can name another; the
  open part is a warning when the primary channel passes the caller's limit and another channel
  is live. Same data as M10.
- **Reference data (1332):** a student asked for raw runs. The sanitized samples are the answer
  (`tools/anonymize_zip.py` first, see the sanitize-shared-samples rule).

#### Interpolation and down-sampling (2026-11-09)

Curves are stored on different grids (UV on a time or volume grid of its own, conductivity,
pH and pressure on others; F20), and modeling tools want one grid and a manageable size. Today:

- `utils.py` (the `pycorn-bin` path) interpolates linearly onto a unified index, inside the
  recorded points only (no extrapolation, as UNICORN draws nothing outside the data).
- `resample.hybrid_index` (page plot) keeps derivative- and evenly-spaced points, min/max and steps (4000 per
  curve) so peaks survive, for the page's plot only.
- `salt_from_conductivity()` interpolates a caller table linearly and clips at its ends.
- Exports and CADET files keep every stored point per curve; no common grid (the `--grid` idea
  in "Search and export", item 3, is not built).

Decided (owner, 2026-11-09): derivative-based sampling (alone or blended with interval sampling
at a ratio) is for **display**, where each curve keeps its own grid. **CADET files stay as the
app requires** (every stored point per curve; `USER_SOLUTION_TIMES` from the primary UV grid):
no thinning and no common grid there. A uniform common grid (`resample.common_grid`) is
built for tables and CSV only if the owner asks. Error-bounded thinning (a tolerance instead of
a count) is not built; revisit if people tune point counts by hand. Built: `pycorn/resample.py`
(`select_index`, `select_points`, `common_grid`, `compare`, `hybrid_index`). The page plot uses
`hybrid_index`: 50% derivative + 50% interval along method time, plus both sides of every step
transition (a change over 5% of the range and 20 times the average change); no other output uses it.

Open (build nothing without the owner):

- **M18 Common grid** for export and CADET references (CADET-Match and CADET-Process compare
  simulated and measured curves point by point). Questions: one grid per run or per signal;
  step from the caller, from the finest curve, or from the flow; time or volume axis (volume
  repeats during pauses, so time); linear only or also a caller choice (nearest, previous
  value for step signals such as flow and valve positions); gaps and pauses (no values inside
  them, as `limit_area="inside"`, or hold); where the record is recorded in the export (method,
  step, source points).
- **M19 Peak-preserving down-sampling** for the files (not only the plot). Questions: is the
  page's min/max bucket rule right for a model reference (it keeps extremes but not the area or
  the first moment); area-preserving averaging per bucket as the alternative; a point limit or
  a step from the caller; never a built-in default (D28). A check should compare peak height,
  area and retention of the thinned curve with the stored one and put the largest difference
  in the warnings.
- **M20 Noise and sensor lag.** Smoothing (moving average, Savitzky-Golay) before thinning, and
  whether UNICORN's own filter (stored per curve?) is already in the data. Probably not
  PyCORN's job: record what is stored, leave smoothing to the model side. Needs the SME.
- **Signal conversions after resampling:** concentration, salt and CV axes are computed on the
  stored points; resampling afterwards keeps them exact, resampling first does not (the
  conversion is nonlinear). Order to be fixed: convert first, then resample.

### CADET export

Pipeline: `Run` -> `build_experiment(run, chrom, ...)` -> `CadetExperiment` (all SI, no CADET
import) -> `to_csv` / `to_cadet_process` (CADET-Process 0.12, runs on Core 5) /
`to_core_input(exp, core_version='5'|'6', overrides=)` + `write_json` / `write_hdf5`, in
`pycorn/exporters/cadet/` (`experiment.py`, `calibration.py`, `buffer_readings.py`,
`process_export.py`, `core_input.py`, `api.py`). Shared modules: `run_log_sections.py`
(sections from the executed run log), `method_hints.py` (D17). User reference:
[`docs/CADET_GUIDE.md`](CADET_GUIDE.md); example: `examplescripts/cadet_walkthrough.py` (on `sample.zip`).

Entry points:

- `pycorn.export_cadet(run, folder, core_version=None, cadet_process=True, chromatogram=None,
  core_format='json', core_overrides=None, overwrite=False, **options)`: options are exactly
  the `pycorn export` flags (`ExportOptions`); it goes through `run_export.cadet_outputs()`, so
  its files are byte-identical to the CLI's. Returns `{'chromatograms', 'written', 'warnings'}`.
  Bad arguments raise before writing; a Core file that can't be written raises and removes the
  `_cadet/` it wrote.
- `pycorn export --cadet` / `--cadet-core 5|6` (`--core-format json|hdf5`).
- Extras: `pycorn[cadet]` = `CADET-Process>=0.12,<0.13`; `pycorn[cadet-core]` = h5py. No new
  required dependency.

| Phase | Built |
|---|---|
| C0 | `units.to_si()`, `flow` / `sample_flow` roles, `column_geometry.py` |
| C1 | `run_log_sections.py`, `CadetExperiment` / `build_experiment()`, `calibration.py`, role `conc_q` |
| C1b | `method_hints.py` (D17, D20, D23), `CadetExperiment.method_hints` |
| C2 | `process_export.py`: `process_inputs`, `to_references`, `to_fractionation_reference`, `to_process`, `to_cadet_process`, `to_csv` (D24) |
| C3 | `core_input.py`: `to_core_input`, `write_json`, `write_hdf5`; expected files in `tests/data` (D25) |
| C4 | Simulated on CADET-Core 5.1.2 and 6.0.0a5; fixes X20-X22; `tests/test_cadet_core_run.py` |
| C5 | `pycorn.export_cadet()`, `to_core_input(overrides=)`, `pycorn export --cadet-core` |

Also built: UV path fallback, D26 geometry, CSTRs (`to_csv(system_volumes=)`), D27,
`uv_wavelengths`.

**CadetExperiment and flow sheet.** `CadetExperiment` holds `source` (run, locator kind,
chromatogram, PyCORN version; never `run.origin`), `time_zero` and offset, `column` (length,
diameter, area, volume, particle radius, each with its source; usually None, D10), `sections`
(start/end s, name, block, flow `(Q0, Q1)`, %B, inlet, paused, `sample_use`, system and sample
flow, `feed_line`, `feed_source`), `events`, `signals` (as stored plus SI; UV in AU/cm;
wavelength and source; `primary`; `not_column_outlet`), `flow`, `fractions`, `components`,
`hints`, `method_hints`, `bypass`, `buffer_readings`, `system` (mixer and tubing volumes and
sources, delay-volume hints), `warnings`. One flow sheet for both outputs (`system_units` /
`system_flows` in `process_export.py`): 'buffer' inlet -> 'mixer' CSTR -> column -> 'tubing'
CSTR -> outlet; 'feed' inlet -> column for valve feed while the mixer holds (Q = 0). Core
file units: `unit_000` buffer inlet, `unit_001` column, `unit_002` outlet, then feed, mixer,
tubing as present (`/meta/UNIT_NAMES`).

Results (tested against a lab environment): both Cores keep sample and salt mass at 99.99 %,
the sample lags the feed by about the accessible volume (placeholder porosities), v5 FV = v6
FV exactly, v6 DG within 0.7 %; the C2 `Process` on 5.1.2 matches the Core-file outlet to
1.6e-6. Runs simulated on Core 5 JSON, Core 6 JSON and HDF5: sample balance 1.0000, section
flow and %B match the curves. Walkthrough: protein / impurity balance
1.000, salt 0.92 (0.79 before M2; column and mixer start empty, M1), fraction mass within 1.2 % of the assays,
v5 FV vs v6 DG within 1 % of the peak; the simulated peak is earlier and narrower than
measured (placeholders, nothing fitted). The README's linear-binding example simulates on
both Cores (first moment 641 s tracer -> 675 s linear, v5 and v6 within 0.2 %).

Known limitations (documented in the guide): sample pumped through an A/B line with no
injection mark is treated as buffer; the instrument stores no column -> UV tubing volume;
the column, mixer and tubing start empty (M1); pycorn doesn't compare or fit.

### Run structure: phases, steps and sections (active, 2026-11-09)

Goal: decide, topic by topic, how each audience gets a run's data and how it is shown. The
instrument (what UNICORN does and records) is the evidence. The model (CADET, scripts) and
the general user (Excel, web page) are the audiences. Two documents:

- [`docs/RUN_STRUCTURE_REVIEW.md`](RUN_STRUCTURE_REVIEW.md) (experts; renamed from `PHASE_STEPS_REVIEW.md`). It covers
  twelve topics: levels, time axes, units, phase-start lag, pauses, holds, flow and valves,
  feed, gradients, inlets, names, unknowns. Each has the four views (instrument, PyCORN today,
  model, user) and numbered **Decide** items (1-26). It also has the ISA-88 reading, the
  views per audience and the u73 evidence.
- [`docs/RUN_STRUCTURE_GUIDE.md`](RUN_STRUCTURE_GUIDE.md) (new users): today's behaviour in plain terms, with the logic
  figures (`docs/images/logic_*.svg`, synthetic run logs put through the real code).

Raised on a database run; evidence from `samples/u73_blank.zip`.

- **Decided, not built** (owner, 2026-11-09):
  - time and volume columns with fixed units in their names, values converted, stored units
    kept as `original_time_unit` / `original_volume_unit`
  - inlets: only the lines pumping
  - gradient length in min, ml and CV
  - one carry-over configuration block and one helper used by every output
- **Built in 0.35.0: inlets from the system configuration** (owner, 2026-11-11; Decide 22,
  D27 (4); `pycorn/system_configuration.py`, `run.system_configuration()`; tested against a
  lab environment, two system families and results with no sample pump and with a feed
  through line A). Each result stores its system's components (`SystemData`
  `EnabledComponents`, `StrategyData` `ComponentStructure`, the same from zip, `.UFol` and
  database), which say per line whether it has a valve, and whether there is a sample inlet
  valve, a sample pump and a quaternary valve. Names differ by system family, so a small
  documented map from component names / hardware codes to roles (installed / not installed /
  not known); a system not in the map gives "not known", never a guess. Labels in the
  `inlets` column with `inlets_source` (known / assumed / not known):
  - a logged position and a fixed line (no valve: nothing to log) are both known values
  - a line with a valve and no position logged yet: the valve's first position, labelled
    assumed (`B1 (assumed)`); the sample inlet valve (Buffer plus S1-S7 where installed):
    `Buffer`, its default, in method and manual runs (owner, 2026-11-11; the system help: a
    direct inject with a sample pump uses the buffer inlet)
  - by installed hardware, never by system model: any valve can be missing on one system and
    installed on many others, so each rule keys on the component in the result's configuration;
    other systems' valves and positions are learned from their stored configuration as results
    with them arrive
  - no B valve: show `B` (today's `B1 (assumed)` is wrong there: B1 does not exist)
  - no sample pump: sample reaches the column through an A/B line, inferred and labelled so
  - quaternary valve: for now its lines feed the A and B pumps (mixing by switching valves);
    Q1-Q4 shares listed as logged. Revisit with the instrument manual (the UNICORN manuals
    here describe BufferPro, not the plumbing)
  - configuration not known: `A (not known)`, `B (not known)`, `Sample inlet (not known)`,
    with a warning
  - CADET: sections and hints carry the inlet states; `buffers=` keyed by a line warns where
    the position is assumed or not known; existing keys work unchanged
  - an assumed `Buffer` sample inlet (a direct inject with no sample inlet logged) still
    counts as feed in `sample_use` and CADET; its `inlets:` warning says so (owner,
    2026-11-11; the warning in 0.36.0)
  - as built: "inferred" (no sample pump) shows only in CADET warnings, not in
    `inlets_source`; a role the configuration doesn't list at all reads "not known", not
    "not installed"
- **Decided and built** (owner, 2026-11-09; web page only, Excel chart unchanged for now):
  - tracks above the page's chart, as the review's figures: phases and fractions as bars
    from start to end, pauses as marks (a span on the clock axis); the cursor label names the
    phase, fraction and pause at every x (`browser.plot_data`, `trackTraces` in `web/index.html`)
  - an inlets track (owner, 2026-11-09, chose one combined lane, always shown when inlets are
    set): one bar per stretch with the same inlet positions, text as the run log set them
    ('A2 B3 S3': line A, line B, sample inlet), from the executed run log's sections (D17);
    nothing before the first inlet is set; shows what was set, not which inlet is flowing at
    a given %B (not built: that would need PyCORN's own rule for mixing) (`browser._inlet_bars`)
  - the chart's x axis picked by the user: clock time (the instrument's local time, pauses
    included), method time, volume or CV; default volume as the Excel chart (Decide 4, 5)
  - track labels without the `Phase ` prefix (Decide 23, for the chart; tables keep the logged
    name for now)
- **Decided, not built: held-over values** (owner, 2026-11-12; review 3.4, Decide 7-10).
  Display outputs (summary, steps, page) fold a flow, %B or %Q change that lasts less than
  10 s **and** differs by less than 1 % into the step around it, with a `held_over` note;
  valves, column position and feed never fold; phase starts and ends never move. M22 windows
  measured from a phase start skip the held-over seconds (this replaces the lead-in part of
  M22's interim rule). CADET never folds: its sections stay exact.
- **Main proposals:**
  - three levels: a user summary per phase, steps for modelers (keyed by `phase_number`,
    `step_number`, with `state`), and sections 1:1 for CADET
  - CADET keeps the exact sections; summary and steps fold flow and %B only, with a
    `held_over` note
  - UNICORN names in outputs, the ISA-88 mapping in the docs only (UNICORN Pause = ISA-88
    Held, UNICORN Hold = ISA-88 Paused)
- **Gap found:** `pycorn export` passes no loop or sample path volume to the CADET feed (its
  `--loop-volume` is for the by-pass UV only). Review 3.8, Decide 19.
- **Open:** Decide 1-21 and 23-26 in the review, and a new shareable sample with a ramp
  (review section 6). This topic replaces open owner question 6 and roadmap item
  8. Owner, 2026-11-10: how phases are shown and exported is a critical enabler for CADET
  and not decided yet (Track 0); it also settles M22's interim phase rule. Results with
  repeated phases and loop cycles exist for that evidence (same survey).

### Web page review (owner, 2026-11-10)

The owner's review of the page after 0.31: slow downloads, too many places to download,
the drop zone keeps its space, no split between key information and extras, an "Excel"
download that holds CSVs, status texts partly in English, two more languages, multi-curve
views, a taller injection mark, downloading chosen runs only, table features, a note that the CADET export is experimental. Items W1-W15.
Built in 0.32.0 (2026-11-10): W1, W2, W3, W4, W5, W7, W8, W9, W15; built in 0.33.0 (2026-11-10): W6, W10, W13; built in 0.34.0 (2026-11-11): W16; built in 0.35.0 (2026-11-11): W11, the Injections table
folded at load; built in 0.36.0: W17; built in 0.37.0: W17's follow-up, run list ticks, M22's page table; open: W12; W14 on hold. Each item lists what is there today, the proposal and its open choices; ask
the open lines before building (owner rule).

**Downloads**

- **W1 Download follows the shown curves** (owner). Today every download writes every curve
  at every stored point (`run_export.curves_frame`, `curve_sampling.chosen_curves` with no
  names), written by xlsxwriter in Pyodide: the main reason downloads are slow. The curve
  chips change only the plot. Proposal: the curves ticked on the plot are the curves
  downloaded, at full resolution, with one line saying so; the size modes (hybrid, every Nth,
  grid) stay as options. The command line keeps D28 (no defaults): on the page the user's
  chip choice decides, not a default. Decided (owner, 2026-11-10): the "UV, conductivity, pH,
  pressure and %B/%Q only" checkbox is dropped; the chips are the only curve choice.
- **W2 One download place, as a popover** (owner). Today seven buttons sit in three places
  (Runs section, the run's download box, plot bar, tables), and the download box takes half
  the summary row. Proposal: one "Download" button opens a panel with the format (Excel /
  CSV / CADET-Core), what goes in (shown run / ticked runs / all) and the mask and size
  settings; nothing of it stays on the page. PNG / SVG become icons on the plot bar, Copy /
  CSV icons on each table bar (W13). Decided (owner, 2026-11-10): the panel keeps the last
  settings across visits (`localStorage`, settings only, never file data).
- **W3 Pick runs to download.** Today "Download all" writes every run in the list
  (`Session.export_start`); the filter only hides rows. Proposal: a tick box per run, "tick
  all shown" (so the filter helps), `export_start(ids, ...)`.
- **W4 One Excel layout from every route** (owner: "all Excel exports should look the same
  from any route"; chose workbook + README). Every route already writes the same workbook
  (one `write_workbook` call; run-level tables are sheets in it). Only what surrounds it
  differs: the page's single-run download gives the bare `.xlsx`; "Download all" and
  `pycorn export --format xlsx` give a run folder with the `.xlsx`, the run-level CSVs
  (`_write_into` writes them for every format), `README.txt` and the provenance JSON.
  Decision: every route gives the `.xlsx` plus `README.txt`, no CSVs, no provenance JSON
  (provenance is in the workbook): a single run on the page as a small zip, all runs as one
  folder per run plus `index.csv`, the command line the same folder; run-level CSVs only
  with `csv` among the formats. A renamed output: CHANGELOG first. Build detail: the export's
  "run unchanged?" stamp is read from `column_and_timeline.csv`; for xlsx-only folders read
  it from the workbook (an `.xlsx` is a zip: a light XML read, no openpyxl), and keep
  reading older folders. A test compares the file set and the sheets of the three routes.
- **W16 Download panel says what comes with the run** (owner, 2026-11-10). Today the
  panel says only how many curves are ticked on the plot (`dl.curves`); every table goes
  in, unnamed. Proposal: the panel lists what the download holds (the curves by name,
  following the chips; the tables by title, grouped as on the page) and lets the user
  choose (tables: all, key tables only, or picked), kept with the other panel settings in
  `localStorage`. Decided (owner, 2026-11-10): tables by page group, one tick box per
  group (Key: phases, injections, fractions; Peaks; Method; Run details; Audit log), all
  ticked at first, each group's tables named under its box; curves by a selector "Plot
  chips" (the default; the ticked curves named) or "All curves"; Summary, Warnings, Column
  guide and `README.txt` always go in; the command line gets `pycorn export --tables
  <groups>` with the same group names, every table when left out (as today), curves as D28.
  Owner, 2026-11-11: a sixth group, Run log (events, run log sections, pauses: in downloads,
  not on the page); Column and timeline always goes in. Built in 0.34.0 (group names `key`,
  `peaks`, `method`, `details`, `audit`, `runlog`; a group keeps the curves table).

**Layout**

- **W5 Drop zone shrinks after the first drop** (owner). Large when the page opens; once
  files are dropped or chosen and runs are listed, a one-line "Add files" bar. Dropping
  anywhere keeps working; Clear brings the large zone back.
- **W6 Key information vs extras.** Today the view is summary | download box, the plot, then
  17 tables. Proposal: key, always shown: run name, summary, plot, phases; the rest in one
  "More details" group with a divider and headings (injections, fractions, peaks, method,
  run details, audit); Runs, View and Download as separate cards. Decided (owner,
  2026-11-10): key = run name, summary, plot, phases, injections, fractions; everything else
  in "More details".
- **W13 Table features on every table** (owner). Today Copy / CSV are text buttons under
  each table, with no select, sort or filter. Proposal, all in the page: Copy and CSV as
  small icons (title and label translated) at the top right of the table bar, beside
  Columns; select cells or rows (click, shift-click, drag) and Copy gives tab-separated text
  that pastes into Excel (nothing selected: the whole table, as today); sort by a header
  click (numbers as numbers, on the full value, not the shown decimals); a filter per table;
  frozen first column(s) and a sticky header. Decided (owner, 2026-11-10): Copy / CSV give
  what you see (sort and filter applied, full values); columns keep their built order and
  each table freezes every column up to and including its `freeze_through` column
  (`export_tables.py`; e.g. phases: name, method steps: block, system settings: parameter;
  nothing frozen when the block would exceed ~40 % of the table's width); sort and filter
  run on every row, then the first 500 are shown with "Show all" as today. W6's "More
  details" title shows the run's warning count (owner, 2026-11-10).

**Text**

- **W7 Status bar fully translated.** In English today: Python's error texts (sizes, Excel
  row limit, CADET-Core version, files not read), the raw format name (`xlsx` / `csv`) in
  "Writing the ... files", Pyodide's own load progress, and the page's engine errors.
  Proposal: Python returns a message key with values (as the `step.*` keys do) plus the
  English text as fallback; the page translates. Table titles and plot labels stay as the
  translation note says.
- **W8 Spanish and Portuguese.** `web/i18n/es.json`, `pt.json`, machine-translated and noted
  as such; the build reads every file there and the tests check keys and placeholders.
  Decided (owner, 2026-11-10): pt-PT.

**Plot**

- **W9 Taller injection mark.** Today a small triangle at the bottom of the plot. Proposal:
  a full-height thin line as for pauses, or a tall tick, label kept. Decided (owner,
  2026-11-10): a full-height thin line, page only; the Excel chart unchanged.
- **W10 Stacked panels (one run).** One panel per unit group on a shared x axis, instead of
  three right axes plus the "% of range" axis. Decided (owner, 2026-11-10): a "Stacked /
  Overlay" toggle on the plot bar, overlay the default, the choice kept across visits
  (`localStorage`).
- **W11 Overlaid runs.** One curve role (`curves_with_role`) from several runs in one plot.
  The page keeps one run in memory today: needs a small per-run cache of thinned curves.
  Decided (owner, 2026-11-11): at most 10 runs; a toggle, kept like W10's
  (`localStorage`): curves as panels (a panel per curve role, runs overlaid, colour by run;
  the default) or runs as panels (a panel per run, its curves overlaid), on W10's stacked
  panels. Built in 0.35.0: runs picked with W3's tick boxes and a "This run / Ticked runs"
  toggle on the plot bar; `Session.overlay_run()` / `overlay()` cache each run's role curves
  thinned to about 1000 points (cache of 20 runs); each role in its first run's unit; runs
  without the picked x axis are left out with a note; on the clock axis runs of different
  days don't overlap; only each run's first chromatogram with role curves (W17 changes
  this). Checked in headless Edge; not in Firefox, Safari, at phone width or over HTTP.
- **W12 Phase-aligned overlay.** W11 with x = 0 at the start of a picked phase (time,
  volume or CV). Open: a run without the phase, or with it several times (cycles). Later
  option: W10-W12 in Excel too.
- **W17 Chromatograms selectable everywhere** (owner, 2026-11-11; Track 8). A result can hold
  several chromatograms: the run's own and others imported or derived in Evaluation (imported
  ones store `OriginalResultID` / `OriginalResultName`, every curve non-original, an
  `ImportedLogbook`; tested against a lab environment). Integration and other analysis may sit
  on them, so every chromatogram is selectable in Python, the CLI, Excel / CSV and the page,
  each labelled original / imported (with its source result) / derived. Default for export and
  visuals: the original chromatogram only. Page: a folded chromatogram picker under each
  result; the plot, tables, downloads and the W11 overlay follow the ticked chromatograms
  (two of one run can be overlaid). Built in 0.36.0 (`Chromatogram.kind` / `source_result`,
  `run.describe_chromatograms()` / `select_chromatograms()`; `--chromatogram
  NAME|all|original|imported|derived`; `ExportOptions(chromatograms=)`; tested on a synthetic
  fixture). Settles D28 (6) (owner, 2026-11-11): every selected chromatogram is
  exported in its own files, with a warning naming them. Follow-up (owner, 2026-11-11):
  phase, injection and fraction tables, sections, M22 statistics and CADET read an imported
  chromatogram's `ImportedLogbook`, labelled as from its source result; built in 0.37.0.
  Owner answers (2026-11-11): provenance as warnings only, no new columns; this result's
  run-level data (Vc, run start, system configuration, method hints, column type, mixer and
  delay volumes) used with a warning each; curves of another method run inside an imported
  chromatogram get no role (kept in the chromatogram and the curves table, one warning).
  Known limit: a renamed pooled curve in a zip (no `EventText2`) counts as fractions. Not
  checked: a derived chromatogram from a real result.
- **Run list ticks** (owner, 2026-11-11; built in 0.37.0): a plain click ticks only that
  run and shows it, Ctrl / Cmd-click toggles one, Shift-click ticks a range (rows the filter
  hides skipped), keyboard too; the plot follows the ticks (2+ overlaid), "This run" shows
  the shown run alone until the ticks change; the tables follow the last run clicked; the
  download panel defaults to "Ticked runs" at 2+ when no scope was picked.

**CADET in the page: on hold** (owner, 2026-11-10: roadmap only, unbuilt until further
input; no track)

- **W14 CADET export form.** Today the page writes CADET-Core JSON with version, bed height,
  diameter, particle size and a parameter JSON file in CADET-Core's own layout. Idea: a
  panel to fill in column and geometry (prefilled from the run where known, source shown),
  components and fraction masses (D30), binding and transport parameters in CADET-Core's own
  names (D28 overrides) as fields instead of a JSON upload (save and load of the form as JSON
  kept), a preview of what will be written, then the download. No PyCORN defaults: empty
  stays empty. Open: fields from a schema of the CADET-Core 5 / 6 parameters, or a short
  hand-picked set plus "advanced JSON"; CADET-Process files (CSV + `experiment.json`) on the
  page too.
- **W15 CADET export marked experimental** (owner, 2026-11-10; in 0.31.2). One sentence:
  the CADET export's assumptions are incomplete and not yet tested, and will change
  significantly as development continues. Shown in four places: the page's CADET-Core
  download box (every language, a link to the CADET guide), the top of
  [`CADET_GUIDE.md`](CADET_GUIDE.md), the README's CADET line, and a line in
  `warnings.csv` (and the CLI's output) whenever CADET files are written: `--cadet`, the
  CADET-Core files, `pycorn.export_cadet()` and the page's CADET-Core download. One constant
  in the code, used the way `concentration.ACCURACY_NOTE` is.

**UNICORN report comparison, remaining sections.** 0.31.0 laid the method information out
as UNICORN's report prints it, checked value by value against a lab environment for the
method text, variables, peaks, column, calibration and evaluation log. Round 2
(2026-11-10, against a lab environment, every result read from the database) also checked
scouting; method, start, run and evaluation notes and start-protocol questions; evaluation
procedures; Watch and condition parameters beyond `Hold until`; gradients with length > 0;
peak tables integrated in time; pressure-sensor calibrations; results with no column
(nothing printed, no column table written); a second lab-scale system type; and the
database path against the zip (identical method tables). Its findings wait for the owner's
pick. Not checked yet (no result had them): a block called from two places; BufferPro;
several chromatograms; a peak table without zero at injection; a custom column;
calibrations of other sensors (UV, sample pump); signatures; process-scale and other system
families. Method:
each result with its UNICORN report
(every section ticked), compared value by value; findings kept local; fixes in the shared
builders, so CSV, Excel and the page change together, after the owner picks them. Every fix
follows "Outputs stay atomic" (Decisions, General). Kept as they are (owner, 2026-11-10):
`method_steps` with its own run values, scouting runs wide (the long variables table serves
programs), Main's variables with no phase. Round-2 picks (owner, 2026-11-10): every item
below is accepted except the phase name (held: the owner reviews more results first).
Pooled fractions: their own table plus a `pool` column on Fractions. The manual run's
report header names a method that comes from example values stored in UNICORN's own report
templates, not from the result: a manual run has no method in any output (owner: skip it).
- **Data fixes:** the phase is the user's name, with the library type as its own
  `phase_type` column (waits on the owner's review: in the results checked, the method's
  phase name differs from its library type only where a user renamed a phase; the run log
  already prints the user's name, so the phases table is unchanged and the method-derived
  tables change); the breakpoint variable on the instruction that stores it; Watch
  action blocks in call order with a `called_from` step.
- **Text only:** `Vc=` inside the parentheses in `line`.
- **New data as fields or small long tables, not UNICORN's sentences:**
  - evaluation procedures as run-details rows (name, selected)
  - start-protocol questions as a table (number, question, answer type, answer)
  - result naming's variable name as its own field
  - scouting `run` numbered from 1, beside the stored position
  - the peak baseline type and resolution algorithm id, each with a readable name
  - the notes renamed
  - a flag on the peak summary when the peak table is normalized to 1 cm
  - pooled fractions as their own table, and a `pool` column on Fractions

**Release plan** (point versions; minor = something users notice, renamed outputs first in
the CHANGELOG; patch = fixes, small page changes, translations). Per release: merge in the
order given, bump `version.py`, CHANGELOG entry and its "Status" block (in progress,
planned; finished work goes in the release entry), `python -m pytest`, build the page and
check it in a headless browser (load, sample, plot, every download, every language), tag
`v<version>`; push only when the owner says it is done.

Order (owner, 2026-11-10): whatever is unblocked goes into the next release; items that
wait on a decision get a version only once the decision is taken.

| Version | Theme | Items | Tracks (merge order) | Waits on |
|---|---|---|---|---|
| 0.32.0 | Quick page fixes and downloads (0.31.2 folded in, owner 2026-11-10: one release) | W9, W8, W5, W15, W4 (renamed outputs), W1, W3, W2, W7 | 1 -> 3 -> 2 -> 5 | released 2026-11-10 |
| 0.33.0 | Tables and layout | W13, W6, W10 | 3, then 2 -> 5 | released 2026-11-10 |
| 0.34.0 | Method information as UNICORN's report, round 2 | the owner's picks (all but the phase name), their page tables, W16 | 7, then 2 -> 5 | released 2026-11-11 |
| 0.35.0 | Inlets from the system configuration, per-phase statistics, overlaid runs | inlets (Decide 22, D27 (4)), M22 (interim phase rule, no page table), W11, Injections folded at load | 4 -> 3 -> 6 | released 2026-11-11 |
| 0.36.0 | Agents and scripts, first items; chromatograms selectable | Track 9 A1 (`--version`, `__all__`), A2 (`pycorn schema`), A6 (`--test-only` fix, `pycorn sources`, settings path as `~`), A9 (agent guide); W17; the assumed-Buffer feed warning | 9, 8 | released 2026-11-11 |
| 0.37.0 | Imported chromatograms' run log; agents and scripts, next items | W17 follow-up (tables, sections, M22, CADET from `ImportedLogbook`); Track 9 A3 (`pycorn inspect`), A4 (`--json` / `--jsonl` / `--csv -`, structured errors), A5 (`export_selection`, `--options`, the page's `index.csv` = the CLI's: renamed output), A7 (`pycorn windows`), A11 (`pycorn-setup --from-env`, dated copies); run list ticks; M22's page table | 6, 4, 9, 3 | released 2026-11-12 |
| after decisions | Run structure build | what the decisions settle: phases, steps, feed in sections, tables, exports, CADET sections, page tracks | 6 -> 3 -> 5 | Decide 1-21, 23-26 (Track 0, one topic at a time) |
| after decisions | Phase-aligned overlay; M22 off its interim rule | W12; M22 `phase_windows` per the decisions | 4, 3 | phase decisions (lead-in, repeats) |
| after decisions | Phase name as the user's name | R1 of the report comparison | 7 | owner reviews more results |
| after decisions | UV baseline and breakthrough | the proposal under issue #1 | 4, 4b | owner confirms the decisions |
| later | Agentic, rest | A8 the page's analysis logic in a public module | 9 | nothing |
| paused | MCP server | A10 (`pycorn[mcp]`; answers kept in Decisions, "Agents and scripts") | 9 | owner (paused 2026-11-12) |
| on hold | CADET form | W14 | none | owner input; inlet and feed decisions |

**Tracks** (each in its own git worktree and branch; can run at once because each owns its
files or regions):

| Track | Items | Owns | Model |
|---|---|---|---|
| 0 Run structure decisions (owner, 2026-11-10: critical for CADET, not decided yet) | Decide 1-26 in [`RUN_STRUCTURE_REVIEW.md`](RUN_STRUCTURE_REVIEW.md), CADET's needs first: phases and repeats (Decide 1-3, 6-14, 23), inlet lines (Decide 22, "only the lines pumping" in exports, inlets per section and step, `buffers=` keys, feed through an A/B line D27 (4)), sample and feed volumes (Decide 17-19, the `--loop-volume` gap), and what the instrument's configuration says about the flow when no valve or inlet switch is logged (owner, 2026-11-10: systems with inlet valves on both system pumps and on the sample pump, such as an AKTA avant, versus systems with no sample inlet valve; inferred from what the result stores about the system: instrument configuration, strategy, enabled components, already read for the `instrument` table; every inference labelled as such, a logged value always wins). One owner session per topic with the review's evidence; decisions go in Decisions and the review | decisions only, no code | Opus |
| 1 Export core (Python) | W1, W3, W4, W7 (Python side), W15's note in `warnings.csv` | `browser.py`; in `run_export.py` `_write_into`, the stamp reader and the curves path; `curve_sampling.py`; `docs/EXPORT_GUIDE.md`; their tests | Sonnet |
| 2 Page shell and downloads | W2, W5, W6, W15, W1 / W3 / W7 (page side) | `web/index.html`: header, drop zone, Runs, view layout, download panel; JS for download, status, progress, run rows | Opus |
| 3 Tables and plot | W9, W13, W10, then W11, W12; M22's page table | `web/index.html`: table tools and table CSS, plot code; for W11 a new curve-cache method in `browser.py` | Sonnet (W9, W13, W10), Opus (W11, W12) |
| 4 Analysis | M22, later the UV baseline | new window / statistics module, `uv_calibration.py`, `run_log_sections._curve_median`, its table in `export_tables.py`, `cli.py` | Opus |
| 4b Simulated breakthrough | CADET-Core test data for the UV baseline | a new generator script under `tools/`, fixtures under `tests/` | Opus |
| 6 Run structure build | what Track 0 decides: phases, steps, inlets and feed in sections, tables, exports and CADET sections | `run_log_sections.py`, `event_tables.py`, the section code in `exporters/cadet/`, their tests (Track 4 reads these, never edits them) | Opus |
| 5 Languages | W8, then every new key in all languages | `web/i18n/*.json` except the new English blocks | Sonnet |
| 7 Report comparison | the remaining sections above | `run_details.py`, `method_hints.py` (its block reader feeds both planned-vs-run and the run details), `run_export.peaks_frame` and the peak summary, the readers where a value is not read, their tests | Opus |
| 8 Chromatograms (built 0.36.0, follow-up 0.37.0) | W17 | the chromatogram kind on the model (`model.py`), chromatogram selection in `run_export.py` / `cli.py` / `exporters/cadet/api.py`, the chromatogram picker and overlay keys in `browser.py` and `web/index.html` (run list), their tests and fixture; `chrom=` arguments elsewhere only | Opus |
| 9 Agentic (owner, 2026-11-11) | everything the page does is a public Python call behind the scenes, and everything the CLI does is usable by an agent or script with no human (non-interactive, machine-readable output, stable exit codes, discoverable commands, tables and roles). Audit done 2026-11-11 (`local/AGENTIC_AUDIT.md`): batch export only in `cli.py`, no JSON output or `--version`, no inspect without export, some analysis only in `browser.py`, `--test-only` checks the wrong settings, no `__all__`. Items A1-A11 in the release plan; A1, A2, A6, A9 built in 0.36.0; A3, A4, A5, A7, A11 in 0.37.0; A10 paused; A8 open. Later decisions: Decisions, "Agents and scripts (Track 9)". Owner decisions (2026-11-11): `inspect` / `--json` output masked by default (`--no-mask` to opt out; export files keep today's rule); the API is `__all__` in `pycorn/__init__.py`; JSON both as one document (`--json`, `"schema": 1`, keys only added, a rename raises the number) and as JSON lines (`--jsonl`, streaming); the page's `index.csv` takes the CLI's columns; `pycorn-setup` gets a non-interactive `--from-env` (writes the per-user file from `UNICORN_*` variables, prints set/empty only, never a password argument); an MCP server soon, with A3 / A4; no plot images from Python for now; the settings path under the home folder shown as `~`; `--version` without a prefix; a source tested with one connection. Method hints defaults kept, recorded in the options JSON | decided per item from the audit; never the page's offline and privacy promise or the connection-value rules | Opus |
| Integrator | merges, version, CHANGELOG, page build and check, tag | `CHANGELOG.md`, `version.py` | Sonnet |

Rules between tracks: only the integrator edits `CHANGELOG.md` and `version.py`; new
English keys go in a block per track prefix (`dl.*`, `tbl.*`, `plot.*`, `msg.*`), so merges
only add; `export_tables.py` changes only each track's own entries. Contracts fixed before
Tracks 1 and 2 start: `export_start(ids, curves=[names])`, and messages as
`{key, values, text}` with `text` the English fallback. Tracks 0, 1, 2, 3, 4, 4b, 5 and 7
can start at once; Track 6 starts per topic as Track 0 decides it; M22's phase windows wait
for the phase decisions (lead-in, repeats), W12 for repeats, any CADET change and W14 for
the inlet and feed decisions; W11 / W12 wait for W10, M22's page table for W13. Work done
early waits on its branch for its release.

## Decisions

Every owner decision, grouped by track. IDs where they exist; dates are 2026-11-07 or
2026-11-08 as given. A later decision replaces an earlier one; the replaced rule is named.

### General

- Ask, don't assume: open design choices go to the owner with data and a recommended option
  (owner, 2026-11-08).
- Every roadmap item is built only when the owner asks (owner, 2026-11-08).
- The README stays short; details go in the guides (owner).
- Shared samples are anonymized (`tools/anonymize_zip.py`).
- **Outputs stay atomic** (owner, 2026-11-10): match UNICORN's outputs where it makes
  sense, but the outputs are mainly for programmers and programs, so they must not get
  harder to use. One kind of row per table, one value per column, its unit in its own
  column. Information about another thing stays in its own table, joined by a key
  (variables by name, phases by `phase`, steps by `step`): values next to the row they
  belong to are fine (`method_steps` with its own run values), but the whole variable list
  is never summarized into the method table. UNICORN's layout may set order, names and
  grouping, and a readable text column (such as `line`) may sit beside the atomic columns,
  never instead of them. A table shaped for people (scouting runs, wide) is fine where an
  atomic table holds the same facts for programs (the long variables table). Combine
  information into one view only where that helps.

### Sources and model

| Topic | Decision (owner, 2026-11-07 unless dated) |
|---|---|
| First output | The existing res3-like dict (`PcUni6` shape). Data-product exports come later, each a separate exporter |
| Scope | Full: curves, events, peaks, run/method/system metadata, audit, from both ZIP and database |
| Run identity | ZIP exports and database rows are matched exactly; run selection narrows the database |
| Axes | Elapsed time, volume, CV and UTC for every curve and event |
| Column volume | `ColumnVolume` isn't always stored: Vc also from the run log and other sources |
| Units | Check every axis. Only the Phase 0 units were tested against a lab environment; build table-driven so units and instruments can be added, tested synthetically |
| UTC | Stored time excludes pauses; rebuild UTC from log anchors and pauses |
| Database versions | Any database with valid UNICORN entries; detect the schema at runtime, never hardcode 7.11 / 7.3 |
| Legacy output | Don't change `PcUni6` / `PcRes3`, their dict shape, `pycorn-bin`, `utils.import_xml_as_df`, `query()` / `query_raw()` / `execute_custom_query()`, `execute_cte_query()`'s signature, `unicorn_cte.sql` output or the exports in `pycorn/__init__.py`. New data goes into the new model only |
| Loading vs output | Keep them separate: exporters sit on the model, readers don't change for them |
| Exact decoding | `PcUni6` decodes arrays exactly; the owner approved the resulting change in legacy values |
| Retention zero | Curves are zeroed at injection 1 by default, as UNICORN's `.asc` exports do (F21); `adjust_axes=False` keeps axes as stored |
| Process scale (rev 8a) | `L` replaces `ml` on process skids for every volume-based quantity; nothing may assume `ml`. The process-scale data request was dropped |
| Rounding (Still Open #5; owner, 2026-11-10) | The model, tables, csv and JSON keep raw, unrounded values; only display rounds (page, Excel cell formats; `pycorn/display_precision.py`). Decimals come from UNICORN's specification, never the value's digits: curve `AmplitudePrecision`; for time, volume, CV, flow and %B the run-log decimals (min 2, ml 3, CV 2, ml/min 3, % 1; `units.DISPLAY_DECIMALS`). Every unit conversion carries them (`units.convert_decimals`: ceil(d - log10 factor), so ml -> L adds 3 and min -> s gives 1) |
| Chromatogram names (2026-11-08) | `Chromatogram.Description` exactly, as UNICORN exports them (`Chrom.1 (1)` suffixes are UNICORN's). Two equal names keep the name; only the `run.chromatograms` key gets the ChromatogramID, with a warning (ZIP reader too) |
| Instrument rows (2026-11-08) | Leave off what is above `InstrumentConfiguration` (`TERMINAL_TABLES`): firmware, VID configuration and libraries are installed software, not result data (megabytes per result) |
| `run.origin` (2026-11-08) | All sources: `server`, `port`, `database`, `source`, `result_id`; or `file_name`, `path` (plus `member` for `.UFol`). Compare in tests, never print |
| Local test files | Don't modify, move or delete anything in `unicorn_test_files/`; tests glob it and skip when empty |
| Imported results (2026-11-08) | Results imported from a database that is not a configured source can't be matched to their originals; add that source to `.env` to mark copies |
| Settings | `.env` resolution and the `localhost` rule are in [`AGENTS.md`](../AGENTS.md) (owner, 2026-11-08): never a silent default server |

### Axes, units, Vc, UTC

#### Finding the column volume (Vc)

Every candidate is collected; the first usable one by priority is used; the run records the
source and all candidates (`column_volume`, `_unit`, `_source`, `_candidates`).

| Priority | Source | Notes |
|---|---|---|
| 1 | Caller override (`column_volume=`) | For runs where nothing works, or to correct a value |
| 2 | Curve / event-curve `ColumnVolume` (> 0) | F3 |
| 3 | Run-time used-columns record (`ResultRunInformation` `ColumnInformation`) | F6; several columns: record all, flag |
| 4 | Run-log `Base CV, Vc=<x> {<unit>}` | F4; every occurrence collected |
| 5 | Flow ratio `X flow` / `X flow (CV/h)` | F5; median of non-trivial flow, small spread; a consistency check |
| 6 | Peak table `ColumnVolume` | F8; only after evaluation |
| 7 | Column records (`ColumnTypeData`, `ColumnIndividualData`) | Not a Vc source (owner); geometry only |
| 8 | Method (`mColumnVolume`, `Base CV` instruction) | Low confidence (F7): recorded, never used for CV, even when nothing else exists (owner) |
| 9 | Name hint (`0.7mL` in a name) | Opt-in heuristic |
| 10 | Caller geometry (pi*d^2/4*L; `add_axes(geometry_volume=)`, `--bed-height` / `--diameter`) | Last resort with a warning, source `geometry` (D26); `column_geometry` then skips its Vc check |

Rules (owner, 2026-11-08 unless noted):

- CV = cumulative in-column volume / main-method Vc (Still Open #3). It doesn't reset at
  `Base Volume` / `Base SameAsMain`. While the column valve is at by-pass CV stays constant (as
  during a pause). Up and down flow both count. Plain `volume / Vc` isn't stored.
- By-pass from run-log texts `Column position By-pass <Down|Up> flow` vs `Column position <n>`
  / `Position <n>`, at the `(Completed)` entry. Patterns live in a table; an unknown `Column
  position` text warns. Manual entries count too.
- More than one non-by-pass position: a warning naming them; CV is not split per column.
- Before the first `Column position` text the column is taken as in line; warn when that
  volume is above 0.1 ml and the run goes to by-pass (the start-up volume is usually far
  smaller; tuned against a lab environment).
- CV counts from method start only; no injection-adjusted CV. Zero offsets come from the event
  tables (`chrom.zero_offsets`).
- Several distinct `Base CV, Vc=` values: record all with time/volume, use the first, warn.
  Piecewise CV per block is a possible later option.
- Candidates more than 1 % apart: keep the higher priority, record the disagreement.
- No candidate: `cv = None` with a warning; never divide by zero.
- Geometry vs Vc (D11, D26): stored record geometry is never a Vc source; caller geometry is
  the last resort.

#### UTC

- The curve grid counts from `ChromatogramStartTime`, else `MethodStartTime` (owner,
  2026-11-08: one method run can hold several chromatograms; `timeline.GRID_STARTS`).
- `utc(t) = start_utc + t + sum(pauses before t)`. Convert each anchor to UTC at once with its
  own offset; never do date arithmetic in local time (DST and mixed zones need nothing more).
- A pause is a jump at one elapsed time: points at it and the pausing event get the pre-pause
  instant, the resume event the post-pause one. Points within 10 ms of a pause's time are at
  the pause (the XML keeps 7 significant digits).
- Pause stops elapsed time and volume (owner confirmed volume); Hold does not, so a Hold is
  never a pause. A Hold can resume a pause.
- Each resume re-anchors the clock (pause duration = resume - predicted pause start).
  A Pause text without an offset uses the offset of the Continue / End that resumes it. The
  `(Issued)` entry of each instruction is the anchor.
- Cases: both ends timestamped: `exact`. One untimed pause: duration = residual, `inferred`.
  Several untimed pauses: exact up to the first unresolved one, then UTC empty (NaT / None), no
  estimate (owner), `ambiguous`. Continue with no Pause (wash, pump sync, alarm, error): a
  pause at the Continue, `cause` from `CLOCK_STOPS`, `inferred`. Gap at End / Hold after an
  alarm, error or wash with no Continue: `clock_stop`. No end anchor: `unresolved`. Residual
  with no pause events: a warning, the gap isn't placed.
- Residual check: `end_utc - start_utc - t_end - pauses` within about 2 s.
- The log-date parser picks day-first vs month-first by matching the exact `MethodStartTime`.
- `Curve.utc`: numpy datetime64[us] UTC (owner). `Event.utc`: numpy datetime64[us], naive UTC,
  or None (owner, 2026-11-08; was an aware datetime) so curve and event UTC subtract directly.
  Quality is per chromatogram (`chrom.timeline["quality"]`); `Run.timeline` is the first
  chromatogram's.

#### Units and concentration

- Never assume a unit from a curve name. Store values and the unit string as UNICORN wrote
  them. One data table maps spellings (`ml`/`mL`, `l`/`L`, `ul`/`µl`, `mAU`/`AU`, `cm/h`,
  `CV/h`, ...) to a quantity and factor; adding a unit is one line. Unknown units pass through
  with a warning: never an error, never a guessed conversion; loading and `to_res3_dict()`
  never depend on the table. Calculations convert first (Vc flow ratio, `Vc=` parser, CV).
  Curves in one chromatogram can have different units: check before combining.
- Normalization is opt-in and recorded (`raw_unit`, `unit`, `factor`, UV path from / to).
  `normalized(uv_path_length_cm=)` is a target path to rescale to; it sets the rescaled
  curve's `uv_info` to the new nominal path (review a).
- `mAU/cm` and `AU/cm` curves are already per cm of path; plain `mAU`/`AU` are not (owner,
  2026-11-07). Never rescale a `*/cm` curve; rescale plain curves with their own
  `CurveUVInfo` (nominal path if normalized, else the actual path; stored in mm). `AU/cm` <->
  `mAU/cm` converts by 1000 without a path.
- Concentration (owner, 2026-11-07): a derived view, `c [mg/ml] = A [AU] / (epsilon [ml mg^-1
  cm^-1] x l [cm])`; the stored curve never changes and `to_res3_dict()` never includes it.
  epsilon belongs to one curve, picked by the caller (never parsed from a name). Sources:
  caller (mass, or molar plus MW: epsilon_mass = epsilon_molar / MW); then stored
  (`Peak.ExtCoeff`, `Pool.ExtCoeff`, `ProteinInfo.ExtinctionCoefficient`, only inside that
  peak or pool; empty in the lab environment, built synthetically); else no curve and a
  warning, never a default. A plain curve with no `CurveUVInfo` gets no curve (no guessed
  path) unless the caller gives a fallback path. Optional caller absorbance limit masks
  points; never a built-in limit. Results record epsilon, unit, source, path and basis.
- Phase / injection / fraction tables (owner request, 2026-11-08): as `unicorn_cte.sql`, with
  start/end time, volume, CV and UTC. A phase ends at the next `Phase` BlockStart; the last at
  its `End Phase`, else the last log entry. Injections and fractions end at the next event of
  their curve, the last at the last log entry; waste marks stay. A leading pseudo-row (number
  0, `pseudo=True`) covers method start to the first real row. Zero at a phase reads the 7.11
  UI settings (`AdjustRetentionToZeroAtPhaseX` as the occurrence, 0 = first): unverified,
  warned.
- What ran in each phase / injection / fraction row (owner, 2026-11-08: "start with what's
  decided for CADET export already. Then we enhance"; "all app branches and all export
  branches get the same features and formats"): `chrom.phase_table()` / `injection_table()` /
  `fraction_table()`, the CSV and Excel export and the page give the same columns
  (`event_tables.COLUMNS`, `with_details`): `length_time`, `length_volume`, `length_cv`,
  `blocks`, system and column flow (D14), `percent_b` / `percent_q` programs (D13, D15),
  `inlets`, `column_position`, `injection_valve`, `sample_use` (D16, D18, D21) and
  `set_marks` (`run_log_sections.span_details`). Values in time order, repeats dropped, pauses
  give none (D7); nothing new is inferred. The CADET export is left as it was (owner: modelers
  may not need these). Checked against a lab environment on a run with a %B ramp: the
  elution phase reads `0 -> ramp 0 to 100 -> 100`. Known: the run log writes a phase's start about a
  second before its own settings, so most phases begin with the previous value (`1 -> 5`);
  see "Run structure (active)" under "Open, by goal".
- `column_geometry` default labels are 'fallback (bed_height=)' etc. (owner, 2026-11-08).

### Run search

Answers to the proposal's questions (owner, 2026-11-08):

- **Q1 Filters:** folder, name, run date, method variables, column, run type / state,
  equipment name (`system`, `Result.SystemName`) and type (`system_type`), batch / run index,
  result IDs, user (`created_by`), curve roles present (`has_curve_roles`), peak tables,
  chromatogram count. No notes search (few results have notes).
- **Q2 Run date:** `run_start` = `RunStartDate`, else `Chromatogram.Created`. `Result.Created`
  is reset on import, so it is only `added_utc`.
- **Q3 Variables in the catalog** (settled by the owner's three factors): search touches only
  structured or indexed tables (no blobs, so no programmed values from `MethodData`); storage
  is one long table for the returned set (categorical columns, no stored pivots); display is
  one row per result with a text and a number column for each variable present, the pivot
  built at display or export time. The catalog stays one row per result so `load_many` takes it
  directly (variable filters as `EXISTS`). Option (A) of (A) long table, (B) every variable a
  column, (hybrid) one cell.
- **Q4 Values:** text (`value`) and number (`number`, NaN when it doesn't parse) side by side
  everywhere, including exports, because the text keeps significant figures (`"0.070"`) and
  shows false numbers (`"7e3"`, `"007"`). Comparisons use `number`, text filters `value`.
- **Q5 Units:** both. With a unit, stored values convert through `units.py` before comparing
  (unknown units excluded with a warning); without, raw stored numbers compare.
- **Q6 Databases:** `.env` is the source of truth; default = everything it lists;
  `sources="discover"` widens; duplicates are kept and marked.
- **Q7 Method name:** not a filter (it exists only inside `MethodData`); a future enhancement on
  top of a local index.
- **Q8 Scouting:** a scouting value overrides the method variable; with none, the method value
  runs. UNICORN stores one row per variable, already the value used (a scouted variable only as
  `ScoutingVariableValue`), so `run.variables()` has a `scouting` flag, not the proposal's two
  rows with `effective`. The programmed value (only in `MethodData`) is left out (owner).

Several sources (owner answers a-d, 2026-11-08):

- a. The alias + `UNICORN_SOURCES` layout. With no `UNICORN_SOURCES`, behaviour is one
  server, one database, as before; the single connection is the source `default`.
- b. Default scope = everything `.env` lists; `<ALIAS>_DATABASES` takes a list or `*`; the
  default source takes `UNICORN_DATABASES` as well as `UNICORN_DATABASE`.
- c. `sources="discover"` on every listed source, including a local SQL Server (an ordinary
  source with its own login). Servers not in `.env` can't be searched by argument.
- d. Duplicates kept and marked (`duplicate_group`, `n_copies`); `dedupe=True` is an option.
- Port: each source has `<ALIAS>_PORT`; `UNICORN_PORT` is passed to the driver.

Search rules (owner, 2026-11-08, while building):

- Never load the whole database or run a huge query: pages (`limit` 1-5000, `after=`),
  `find_pages()`, server-side `count()`; every filter in SQL so pages are never short.
  Paging continues after the last ResultID (no OFFSET).
- Keep filters simple: each searches the most logical fast place; no filter = everything.
  `column=` is the run-time `ColumnInformation` only; method variables go through
  `variables=`.
- Pick lists for every filter: `db.pick_list(field)` (`folder`, `system`, `system_type`,
  `run_type`, `state`, `created_by`, `batch_id`, `column`, `variable`, `variable_value`,
  `curve_role`).
- Run date clock: plain dates are the run's local time; with a zone they compare in UTC; a
  plain end date includes the whole day.
- Variables per page through `db.variables(page)` and `search.wide()`; the owner had no
  preference on how; `DataFrame.attrs` was ruled out (pandas 3 deep-copies it on every filter
  and drops it on merge / concat).
- Paging over several databases fills one database at a time (source order, then database
  order, each by ResultID); results from different databases don't mix by date.
- `count()` and `pick_list()` merge across databases (folders by path); `by_database=True`
  gives a breakdown.
- Output names the alias only (`'lab2#2'`: alias and the database's position), never server or
  database names; database names appear only in returned data.
- Missing columns fall back ("fall back without them, rather than dropping entirely"): only
  `Result.ResultID` is required; a missing catalog column is NULL; a filter that needs a
  missing column matches nothing there, with a warning; hidden folders that can't be told
  apart are included, with a warning; `load_run()` reads whatever exists. SQL marks
  column-dependent parts with `IF Table.Column` / `ELSE` / `END` block comments
  (`search.resolve_blocks`).
- A source that can't connect, or a missing listed database, is warned (alias only) and left
  out; `strict=True` raises. `variables()` and `load_run()` are strict.

Identity and loading (owner, 2026-11-08, step 4):

- `load_many(catalog)` is a generator: one `Run` at a time, one connection per block of rows
  from one database. An unreadable result is skipped with a warning (alias, database number,
  ResultID); `strict=True` raises. A result changed since the search (`tstamp`) loads as it is
  now, with a warning (`check_tstamp=False` skips the query).
- `match_key` = first chromatogram time in UTC, cut to whole seconds | system | run index.
  Not `Result.Created` (reset on import). **The name is not in the key (rev 10i):** UNICORN
  saves an evaluation as a new result with a new name and the same chromatogram, and the
  owner counts those as the same run. File matching uses the name as a tie-breaker.
- Copies are counted per page, over every database searched, with the same filters (counting
  regardless of filters was rejected: dedupe could drop a row whose kept copy lies outside
  the filters). `duplicate_group` is a 10-character id, set only when `n_copies` > 1.
- `dedupe=True` keeps the most recently changed copy (`Result.LastModified`, `modified_utc`),
  the first in search order on a tie, unknown times last.
- `catalog_from_files(paths)` reads light (`Result.xml` and chromatogram XML only), same
  columns as `find()` plus `path`, `member`, `original_id`; copies among files are marked.
- `match_files` statuses: `exact`, `ambiguous`, `copy`, `missing`, `no_key`; plus `n_matches`,
  `n_copies`, `changed_in_db`. Hidden folders are searched.
- File rows load (owner: both APIs, `export --from` takes file rows, and a match-key check):
  `pycorn.load_files(catalog)` and `db.load_many` with mixed catalogs; a run whose key changed
  since the catalog loads as it is now, with a warning.

### `pycorn export`

#### Command line

Owner decisions (2026-11-08, before step 5):

- Entry point: both, a `pycorn` console script with subcommands and `python -m pycorn`,
  calling one `main()`.
- `find` flags: every `find()` filter 1:1 (`--folder`, `--name`, `--run-start`, `--system`,
  `--system-type`, `--run-type`, `--state`, `--batch-id`, `--run-index`, `--result-id`,
  `--created-by`, `--column`, `--var`, `--has-role`, `--has-peaks`, `--chromatograms`,
  `--include-hidden`, `--sources`, `--dedupe`, `--limit` / `--after`). Ranges `a..b`, `3,5` =
  any of.
- `--var` inline and repeatable: `"Flow rate>=1 ml/min"`, `"UV1==280"`, `"Column
  position=Position 2"` (text, wildcards), `"Sample_ID"` (has it); operators `=`, `==`, `!=`,
  `<`, `<=`, `>`, `>=`; unit optional.
- `find` prints the server-side count, then the first 20 rows (`--show N`; `--show 0` prints
  the count only): label `alias#n`, ResultID, name, run start, system, state, copies. ASCII,
  alias only. The whole catalog goes to `--csv` (plus `db_label`), which creates its folder.
- `export` formats: CSV, Excel, pickle (res3 dict); CADET added later. One file per
  chromatogram in every format, every curve with all axes, raw values.
- `export` takes a find CSV or `.zip` / `.UFol` files.
- Layout: `<out>/<alias>#<db number>/<UNICORN folder tree>/<run name>/<chromatogram>.<ext>`
  (files: `zip` / `ufol` in place of the alias). Names cleaned for file systems; colliding run
  names in one folder all get `_<ResultID>` (same run in two `.UFol`: `_<original id>`, then a
  counter).
- Existing output is skipped and listed (`index.csv`); `--overwrite` replaces it. Each run is
  written into `<folder>.partial` and renamed when done.
- `--cadet-core 5|6` belongs to `pycorn export`, next to `--cadet`, not to `pycorn-bin`.

#### Default outputs and flag decisions

Default outputs (owner, 2026-11-08, after a review of the CADET-track outputs): phase,
injection and fraction tables; run-log sections with the curve checks; per run `run_info`
(Vc and candidates, geometry, timeline quality, start/end, paused time, version) with a pauses
table; one `warnings.csv` per run. Opt-in: `--method-hints`, `--units`, `--cadet`,
`--epsilon`, `--cadet-core`, `--path-length`.

Flag decisions (owner, 2026-11-08):

- `--path-length CM`: the UV path only where `uv_path_length_cm(curve)` gives none for a plain
  mAU/AU curve; never on `*/cm` curves or over a stored path; basis 'fallback
  (--path-length)', one warning per filled curve. Feeds `--epsilon` and the CADET outputs.
  Unused: one `export` note.
- `--epsilon 280=1.4` per wavelength (repeatable); `--epsilon-molar 280=43824 --mw "66000
  g/mol"`. Only UV curves at a listed wavelength get mg/ml, as extra columns '<curve> (mg/ml):
  ...' with their own axes, plus a `_concentration` table.
- A UV curve's wavelength follows D8 (run-log `Wavelength` instruction; name suffix only as a
  warned fallback), through `pycorn.exporters.cadet.uv_wavelengths`.
- `--cadet` with `--epsilon` gets the coefficient of the primary wavelength only (280 nm, else
  the lowest channel); none given for it: no CADET concentration, with a note.
- `--cadet-core`: JSON by default; `--core-format hdf5` needs h5py (exit 2 without it). Without
  geometry (D10) a `cadet_core` line asks for `--bed-height` / `--diameter`; the rest of the
  run is written.
- Geometry flags `--bed-height`, `--diameter`, `--particle-diameter` (value and unit) are a
  fallback per value; `--override-geometry` makes them win (D26). Vc from them as the last
  resort (source `geometry`).
- D27 flags (owner: all four): `--mixer-volume`, `--tubing-volume`, `--sample-inlets`,
  `--no-system-volumes`, only with `--cadet` / `--cadet-core` (exit 2 otherwise).
- Every flag is checked before a run is loaded (exit 2 on bad input).
- A table or flag output that raises is left out with an `export` line in `warnings.csv`; the
  run is still written.

Run-level tables and masking (owner, 2026-11-09; `pycorn/run_details.py`):

- Every export path (csv, xlsx, the in-browser page and its downloads) gets seven run-level
  tables, each left out when empty: `run_details` (result and method), `method_steps` (one
  row per block instruction, with the values the run used), `scouting_runs`, `calibration`,
  `instrument` (configuration, mixer, UV cell, module serials and run hours, enabled
  components), `system_settings`, `audit_log`. The summary sheet adds method and instrument.
- 0.31.0 (owner, 2026-11-10): method information laid out as UNICORN's own report prints it.
  `method_steps` in call order, indented, with UNICORN's method text (`line`), the calling
  phase, and hidden parameters left out. The variable list by phase and block with `{base}`
  resolved. Calibrations as references then constants. Dates with their UTC offset, plus the
  result naming. Two new tables: `column` (the column type in the report's three groups;
  column volume and linear flows calculated) and the per-chromatogram `peak_summary` (each
  peak table's header). Peaks get units, the injection the retentions count from, and the
  maximum on the run's axis. Also fixed: Hold until conditions lost their parameters
  (`ConditionParameterName`). Still not checked against UNICORN's report, for lack
  of a result that has them: scouting, notes, BufferPro, questions, evaluation procedures,
  signatures.
- Shown as stored by default: people, computer and system names and serials. IP addresses and
  ports never. `--mask-identifying` (`ExportOptions.mask_identifying`; the page's "Mask names
  and serials" download box) writes User1, Computer1, System1, Serial1 in every table and
  pickle. Run, folder and method names are not masked. Notes (method, start, run,
  evaluation) and free-text start-protocol answers are masked whole, vendor text in
  Method notes included (owner, 2026-11-10: keep masked; names in free text cannot be
  found reliably).
- `--method-hints` stays opt-in on the command line; the page shows the hints and its
  downloads include them.
- Found on the way: the pushed samples held real module serials (base64 `OperationalStatistics`
  in SystemData) and the instrument's IP. `tools/anonymize_zip.py` now decodes base64 XML
  blocks; the samples were re-sanitized. The public history was rebuilt on 2026-11-12
  without the earlier versions.

#### Fix plan (export review)

The 2026-11-08 review of `pycorn export` found six gaps; the owner chose and the fixes are
built:

1. `warnings.csv`, `run_info.csv`, `variables.csv` are written for every format.
2. Unused flags with pickle alone (`--epsilon`, `--method-hints`) are noted and skipped
   (owner's choice), not errors.
3. A failed run leaves no `.partial` folder; the Excel row limit is checked before writing.
4. Changed runs: a per-run `export_stamp` (`run_info.csv`, `index.csv` `export_changed`), and
   `--update` re-exports changed and unchecked runs (`--update` with `--overwrite`: exit 2).
   Database rows use `tstamp`; files use `'<size>:<mtime ns>:<sha256>'`, hashed only when size
   or time differ (owner: "size+time, then hash"). The stored stamp is the loaded run's
   (`loaded_stamp`, review c).
5. Repeated rows in a selection are exported once; `cadet_outputs` reuses the sections
   `write_run` built (unless `--units` changed the run).

### CADET export (D1-D30)

| # | Decision | Rule and why |
|---|---|---|
| D1 | Target | Both outputs on one neutral layer: `CadetExperiment` (C1), CADET-Process objects (C2), Core input (C3; widened to Core 5 and 6 by D9) |
| D2 | UV signal | Always absorbance per path in AU/cm (mAU / 1000 / path cm; `*/cm` curves as they are). No path: AU, no concentration, warning. Concentration when the caller gives epsilon (and MW). **Rev 11b ("if it can use it, we leave it in"): the CADET files carry mol/m^3 only**, because CADET components share one unit and binding models count in mol/m^3; mg/ml stays in the regular export and the conversion in `experiment.json` |
| D3 | Buffer concentrations | The caller supplies them (mol/m^3 per component and line). Hints only, never used silently: BufferPro prep (D17 case 2), the inlet line and %B per section (e.g. `A1, 0 %B`), Set Mark texts and phase / block names |
| D4 | Fraction assays | Optional CSV keyed by tube id (`tube, component, mass_mg` or `conc_g_per_l`); without it, times and volumes only, no `FractionationReference`. **Rev 11b:** tube names repeat only for waste, which isn't assayed, so matching by tube name is enough |
| D5 | Time zero | Method start (raw elapsed time), so equilibration is in the process; the injection offset goes in the metadata |
| D6 | Process source | Run-log sections (executed entries, D13), checked against flow and %B curves; warn beyond 2 % flow / 1 %B. Never built from curves |
| D7 | Pauses and hidden stops | A zero-flow section (Q = 0) of the pause length, with a warning, so diffusion with no flow is modelled; includes unlogged stops (`continue_without_pause`). The time axis becomes wall-clock time from method start; signals shift the same way. Use `chrom.timeline["pauses"]` |
| D8 | UV curves | Every channel above 0 nm (0 nm skipped with a note). 280 nm is primary (its grid is `USER_SOLUTION_TIMES`; epsilon applies to it unless the caller names another); no 280: the lowest live channel, warned. Wavelength from the run-log `Wavelength 280 {nm} 295 {nm} Off {nm}` (channel n = n-th value, last one before the section); the name suffix only as a warned fallback; warn on a change during the run |
| D9 | Core version | Both Core 5 (5.1.2 layout) and 6 (6.0.0a5); `core_version` required, no default; anything else raises naming both. One file per target (X17). CADET-Process runs on Core 5 only (X3) |
| D10 | Geometry source | The run's own `ColumnTypeData` copy, only when the run links a column type and its record is there (rev 5c: users aren't expected to reference column definitions; few runs link one). Never from the column library (linked by name only, today's values), the column name or Vc. Otherwise CV only; the Core input then needs caller geometry. Caller values: D26 |
| D11 | Geometry and Vc | Geometry is never a Vc source; the geometric volume is checked against Vc (warning above 1 %). **Amended by D26** for caller geometry |
| D12 | SI conversion | A separate `units.to_si()`; existing conversions unchanged |
| D13 | What ran | Only what ran, from the run (event) log, `Method` and `Manual` entries alike (manual commands can override anything). Each executed instruction holds until the next of its kind (flow, %B, inlets, valves, wavelength, column position). `Next breakpoint` and `End` need nothing: the log shows what ran next; skipped blocks leave no section. Curves are only the D6 check. Refined by D17 |
| D14 | Column flow | By injection valve position: system flow; 'Direct inject' = sample flow; 'System pump waste' = 0 (warning). Both set-points and the valve kept per section. Column by-pass: no column flow |
| D15 | Quaternary | Q1-Q4 per section from `Quaternary gradient a b c d <length>`; ramp and hold like %B, checked against the `conc_q` curves; caller buffers per Q line |
| D16 | Feed | Sections with the valve at 'Inject' / 'Direct inject' are feed; a caller `loop_volume` ends loop feed after that volume. **Replaced by D18-D20** (rev 7b); the rev 7 method-volume cap (D17 case 1) is gone too |
| D17 | Method data | "The method can be read if helpful, but ground truth is as executed." Read for what the log doesn't record: sample application, BufferPro prep, variables / scouting / documentation, a planned-vs-executed report. Precedence caller > run log > method; every method value `source='method'`; a disagreement is a warning; sections, times, lengths and holds never come from the method |
| D18 | Feed through by-pass | Valve at Inject / Direct inject while the column is by-passed (priming, purging bubbles, checking UV / cond / pH) is `to_bypass`, not feed, but uses up loop volume. UV, conductivity (and pH) there stay in the references, flagged not column outlet, with a warning |
| D19 | Fixed loops | Loop volume from the caller only (stored nowhere); without it the whole Inject period is feed, with a warning. Volume is used up by flow through the valve (system flow at Inject, sample flow at Direct inject), by-pass included. A valve left at Inject after the loop is empty is tubing: buffer follows |
| D20 | Superloop | The logged valve period is the feed (a Superloop can't run on once empty). The planned volume (the Inject block's `End_Block` `BreakPointValue`, not the stale dialog `mSampleVolumeForSuperloop`) is only a check |
| D21 | Sample inlet `Buffer` | Direct inject with the sample inlet at `Buffer` the whole period: sample-pump buffer, not feed, warned (column flow counts). A switch to `Buffer` partway is a chase: with a caller `sample_path_volume` the feed runs that volume past the switch (and starts that much later after a buffer wash); without it the feed ends at the switch, warned. From the AKTA avant help. Being revisited as M4 |
| D22 | By-pass windows | Every by-pass window (buffer or sample) flags UV and conductivity as not column outlet. Buffer-only windows give buffer conductivity and pH: median over the last third of the last steady piece (same inlets, constant %B/%Q, >= 30 s, `READ_MIN_S`); settled when the spread is <= 2 % (conductivity) and <= 0.05 (pH); compared with the outlet in the nearest matching section, warned on a difference, never corrected. **Rev 7g: pH outlet-match tolerance 0.1** (`PH_MATCH`; shifts of 0.01-0.09 are plausibly the column); settled stays 0.05 (`PH_SETTLED`), conductivity 2 % both. `buffer_salt=` + `salt_calibration_points()` give (mS/cm, mol/m^3) points; quaternary windows get no salt |
| D23 | Method hints as built | (1) Planned-vs-executed: one row per executed block with a fixed length (planned with the run's variable values), report only, no warnings except injections; blocks never run listed by name. (2) Injection check: allowance = the measured pre-switch gap (the owner first chose previous flow x 1.5 s; tested against a lab environment that warned on most injections, so changed); short beyond gap + 2 % (min 0.005 ml) or over by more than 2 % warns. (3) A planned 0 is checked like the rest. (4) BufferPro parsed to the documented fields, tested synthetically (off in the lab environment), raw kept, layout warned. (5) Every variable value, scouting flags, documentation, phase texts, start-protocol items as labels. Direct-inject rows reported, not checked |
| D24 | CADET-Process output | (1) One inlet (**replaced by D27**). (2) No caller concentrations: placeholder components, 'salt' following %B (0-100 mol/m^3) and 'sample' = 1 in feed, so it simulates as a tracer; caller values are all or nothing. (3) By-pass windows stay in the references as measured, listed, warned. (4) The Process needs `time_zero='method_start'` |
| D25 | Core input | (1) `placeholders=True` fills transport from CADET-Core's GRM benchmark (bed porosity 0.37, particle porosity 0.75, axial dispersion 5.75e-8 m^2/s, film 6.9e-6 m/s, pore diffusion 6.07e-11 m^2/s, particle radius 45 um only without a linked column; LRM total porosity 0.8425), each in `/meta/placeholders` with a warning; binding `NONE`; geometry never gets a placeholder. **Rev 11b: caller values only through `overrides=` (D28); `parameters=` / `binding=` removed (TypeError pointing to `overrides`).** (2) Core 6 DG by default (16 elements, degree 4; particle 1 element, degree 4); FV on request; Core 5 FV (`NCOL` 100, `NPAR` 5, WENO 3). (3) Hand-checked expected files for `samples/u711_cadet_reference.zip` are committed (names and version left out; solution times as count, first, last) |
| D26 | Caller geometry | Asked via the CLI session. (1) Caller bed height, diameter, particle diameter are a fallback per value; a stored value wins. (2) An unused caller value is always noted, equal or not. (3) `override=True` / `--override-geometry` makes the caller win (an AKTA set up wrong). (4) **Amends D11:** caller geometry gives Vc as a last resort (some database results have no Vc); stored record geometry never does. Particle radius order: override > stored > caller fallback > D25 placeholder |
| D27 | System volumes | (1) A CSTR before (mixer) and after (column -> UV tubing) the column by default. (2) Mixer volume from `SystemData` `MixerInformation`; the caller wins. The mixer by-pass valve (V9-M) is taken as always in line (never switched in the lab environment). (3) The mixer is before the injection valve: valve injections skip it; two inlets, the mixer holds (Q = 0) during valve feed. (4) Feed through an A/B pump line goes through the mixer: found from an injection mark with no valve switch (rare), only when an A or B inlet switched within 60 s of the mark (owner, after the data); feed runs while that position stays selected, its share by %B; else a warning. `sample_inlets=` wins. (5) Tubing volume from the caller only (the stored delay volumes are after the UV: hints only); without it no tubing CSTR and a warning. `system_volumes=False` leaves the CSTRs out |
| D28 | On top of CADET, never a copy | CADET keeps adding models; mirroring them means maintaining both. (1) pycorn builds what the run gives and nothing of the model. (2) Model values in CADET's own terms: `core_overrides` is a nested dict in CADET-Core's input layout (`{'input': ...}`), merged unchanged just before writing (no translation, no check, no name list); for CADET-Process, its own API on the returned `Process`. (3) Don't mirror CADET's model and parameter permutations; one marked example block in the docs is fine. (4) Placeholders fill what isn't given; one the overrides replace leaves `/meta/placeholders`; `/meta/OVERRIDES` lists the paths set. (5) `parameters=` / `binding=` removed (rev 11b). (6) A run with one chromatogram is exported. **Amended rev 11b: "warn on multiple chromatograms until we have a clear choice from the user community"**: without `chromatogram=` both paths export them all with a warning (the rev 11 rule, an error asking for one, is replaced). (7) **Amended 2026-11-08 (owner: "I like your params block argument; maybe we implement that everywhere for now and don't make any assumptions about what it should contain, to keep it very generic")**: one block per target on every route. Core: `core_overrides=` (`export_cadet`, `to_core_input(overrides=)`) and `pycorn export --core-overrides FILE.json`. CADET-Process: `process_overrides=` in its own parameter layout (what `process.parameters` shows); `to_process` / `to_cadet_process` set it last with `process.parameters = ...` (a copy: CADET-Process empties the dict it gets; it rejects unknown names itself), `export_cadet` / `to_csv` / `pycorn export --cadet --process-overrides FILE.json` store it unchanged in `experiment.json`. pycorn reads nothing inside either block |
| D29 | `--mw` with a mass epsilon | "We might want a molecular weight flag." (1) `--mw` / `molecular_weight=` also applies with a mass coefficient, so the CADET files get the `<UV> mol/m^3` reference; no second flag. (2) A `cadet` warning when there is a coefficient but no MW. (3) **Rev 11b:** an unused `--mw` is a note in `warnings.csv`, not an error |
| D30 | Offline assays, walkthrough | (1) Document only: no new options; [`docs/CADET_GUIDE.md`](CADET_GUIDE.md) "Your offline assays" shows `build_experiment(components=, fraction_masses=)`, the CSV template and the g/L -> mol/m^3 arithmetic. (2) A walkthrough on `samples/sample.zip` (`examplescripts/cadet_walkthrough.py`, opt-in tests on both Cores). (3) Overlay, no fitting; comparing and fitting go through CADET-Process (`Comparator`, `OptimizationProblem`). **Open:** whether `export_cadet()` / `pycorn export` should take the assays (D28 doesn't decide it: measured items aren't model inputs) |

Other CADET decisions (owner, 2026-11-08):

- **CADET `buffers=` keys are inlet positions as the run log names them** (`A1`, `A2`, `B1`, or
  a process system's own names), with the pump line (`A`) as fallback (owner, 2026-11-08).
  Runs do switch inlet positions on both lines.
- Every `Gradient` instruction carries its ramp length; 0 is a step (rev 5d). A ramp runs from
  the current %B to the target over that length (converted with the flow in force and Vc for
  CV), then holds; the next %B instruction takes over, ending an interrupted ramp where it was.
- Flow set-points come from the run log (`System flow 5.000 {ml/min} ...`, `Sample flow ...`);
  manual commands override anything (rev 4).
- Phases start in the old line (rev 9c): a `Phase` / `Block` entry comes before its own inlet,
  gradient and flow instructions, so never read a phase's inlet, %B or flow from its first
  section (the A/B feed rule uses the inlet switch time, buffer readings the last steady piece,
  the D3 hints are per section).
- UV path fallback (via the CLI session): `build_experiment(uv_path_length_fallback_cm=,
  uv_path_length_fallback_basis=)` and `calibration.uv_au_per_cm(curve, path_length_cm,
  fallback_path_cm, fallback_basis)` only where the run gives no path; `uv_path_length_cm=`
  still overrides every plain curve.
- Not modelled: the pump-to-detector delay (reported; dead volumes are the user's choice in
  CADET). Measured pH / temperature as `EXT_` external sources (X8) only on request. Several
  absorbing components (a multi-wavelength matrix) are out of scope. Conductivity to salt only
  with a caller calibration; a saturation threshold only from the caller. No smoothing (X14).

#### Method hints (D17, D20, D23)

`method_hints(run, chrom=None, sections=None)` -> `MethodHints` (`method`, `variables`,
`scouting`, `documentation`, `buffer_prep`, `sample_application`, `injections`, `blocks`,
`blocks_not_run`, `warnings`). `build_experiment()` stores it (times shifted with `time_zero`),
adds its warnings as 'method: ...', and changes no section or feed. Sources:
`run.metadata['MethodData']`, `['MethodDocumentationData']`; field names matched as stored,
raw kept; an unknown layout warns.

- The method is a template: every breakpoint variable takes the run's value
  (`pycorn.variables`), never `BreakPointValue` (about half of the variable `End_Block` breakpoints
  tested against a lab environment ran with another value).
- A breakpoint variable belongs to its position: instructions at one breakpoint move together.
- Planned injection = from the breakpoint that turns the valve to Inject / Direct inject to the
  next valve instruction in the block (most), else to `End_Block`; compared per
  executed run of the block with the valve period (set-points, pauses out, by-pass in).
- Block rows: base through 'SameAsMain'; CV against the logged Vc; 'as_planned' within 5 %;
  cause 'next_breakpoint', 'watch_in_block', 'hold_or_pause'. Phases get no row.

### Usability for non-programmers

Owner, 2026-11-08 (asked "how can we make this as user friendly as possible for
non-programmers?"):

- **`pycorn-setup`** (console script, `pycorn/setup_env.py`): asks for server, port,
  database, login and password (typed hidden), writes the `.env`, tests the connection with
  [OK] / [FAIL]. No value is ever printed, only set/empty. Saves to the per-user file
  (`db_connector.user_env_file()`: `%APPDATA%\pycorn\.env`, else
  `$XDG_CONFIG_HOME/pycorn/.env` or `~/.config/pycorn/.env`), which is the **last** place
  `find_env_file()` looks, so a `.env` in the working folder or checkout still wins (the tool
  warns when one does, and names overriding `UNICORN_*` variables). `--file` writes elsewhere;
  `--test-only`, `--no-test`. An existing file is updated in place (Enter keeps a value; other
  lines kept). Password stays in the `.env` as before (file mode 600 off Windows); Windows
  Credential Manager / keyring was offered and not chosen. Tests never read the developer's
  per-user file (`tests/conftest.py`).
- **Excel output** (`pycorn/excel_export.py`; gaps found on the samples: no Excel library
  installed by default, no frozen header / filter / widths so UTC showed `#####`, empty
  `peaks` sheet, 140-column curves sheet with no chart, every workbook named `Chrom.1.xlsx`,
  no overview). Built: xlsxwriter is a core dependency; `summary` sheet first (run, file name
  only, never its folders, dates, Vc, column, units, warning count, validation note, one line
  per sheet); `chart` sheet: UV curves (left) and conductivity, else %B (right), found by role,
  flat curves left out, against volume (time when a curve has none), injection and fraction
  starts as labelled markers (no labels over 60 marks; data in a hidden `chart_marks` sheet);
  every table: frozen header, filter, fitted widths, dates as dates; empty tables left out
  (curves and events always kept), as in csv; time/volume columns without a unit get
  `(<unit>)` in the Excel header only (csv names unchanged, so scripts keep working); workbook
  named `<run name>_<chrom>.xlsx`. With only openpyxl: plain tables, summary first, no chart.
- **Plain-language messages** (item 5; owner chose, 2026-11-08: wording at the source as well
  as in the CLI, the "looks like success" cases fixed, unexpected errors in one line).
  Failure cases were run first; none showed a traceback, but raw text (`[WinError 183]`,
  `[Errno 2]`, DB-Lib byte strings) and four silent successes did. Built:
  `pycorn/messages.py` `plain_error(e)` (OS, zip, tar and memory errors in one sentence;
  PyCORN's own messages unchanged), used by file warnings, the export's "not written" and the
  CLI. `_connect_safely` raises a sentence per case (login refused 18456, database not opened
  4060, timed out 20003, not reached 20009/10061), the redacted driver text in `detail`.
  No server set: "run pycorn-setup". A zip with neither `Result.xml` nor a chromatogram is
  "not a UNICORN result" (`read_zip`, `read_headers`). Exit codes: 0 done, 1 something left out
  (a file not read: `catalog.attrs['not_read']`; an `--epsilon` wavelength no UV curve has:
  `missing_outputs`; a source not reached: `db.unreachable`, find names it once at the end),
  2 worded error, 3 unexpected (one line, report link), 130 interrupted. `--debug` before or
  after the command adds the traceback (and `detail`). Library warnings dropped their
  `catalog_from_files:` / `load_files:` prefixes. Not changed: other library exceptions
  (already sentences), `bypass-uv` with no by-pass in the run (exit 0, nothing to read), mg/ml
  that `concentration()` refused for a wavelength the run has (still only in `warnings.csv`).
- **App without hosting:** option B chosen for the roadmap (an in-browser page, item 6, built; see the next entry).
  Not chosen: a drop-on-icon / Send-to shortcut (A), a packaged desktop app (C), a shared lab
  instance (D).
- **In-browser page** (item 6; owner chose, 2026-11-08): hosted on GitHub Pages **and** built
  as one self-contained file for shared drives (`tools/build_web_page.py`); views: chromatogram
  plot by role (the Excel chart's rule: live UV left, conductivity else %B right, injection
  starts marked; curves thinned to about 4000 points by min/max per bucket; phase, fraction
  and pause tracks above it and an x-axis picker, clock / method time / volume / CV, added
  2026-11-09, see "Run structure"),
  phase / injection / fraction tables, the Excel summary rows, method variables, warnings;
  downloads: the `pycorn export` workbook, its csv files zipped, and CADET-Core JSON (version
  5 or 6, optional bed height / diameter / particle diameter and a parameter JSON file in
  CADET-Core's own layout, D28). No CADET-Process (does not run in Pyodide) and no HDF5. No
  offline copy yet: the first load needs the Pyodide CDN. Files only; no database.
- **Web page usability audit** (owner asked, 2026-11-09; code read plus checks in headless
  Edge; limits: one file, Pyodide with numpy and pandas only, Plotly basic bundle, no server,
  files never leave the computer, short texts in five languages). Built: drop anywhere on the
  page; run list sort, filter and keyboard rows; tables drawn when opened, first 500 rows
  until "Show all", per-table Copy and CSV; plot curve selector (also the legend click) and
  PNG / SVG buttons (the camera button is removed; images are always light and keep the
  zoom); dark mode with the system, plot included; column and table hints as text on tap,
  click or Enter (hover has no touch); live status line, disabled buttons while busy, file
  input and table names for screen readers, larger touch targets; load progress, reload
  button, WebAssembly check; "Try a sample" where a `sample.zip` sits beside the page (the
  Pages workflow adds it); SRI hashes on the Pyodide and Plotly scripts (update them with
  the versions); a Content-Security-Policy (2026-11-09, tried in Edge over http and file://:
  `default-src 'none'`, scripts only the page's own hash and the two CDNs, `connect-src` only
  those CDNs and the page's address, so a request to any other host is refused; Pyodide needs
  `'unsafe-eval'` and `'wasm-unsafe-eval'`, Plotly needs inline styles; the build script puts
  the script hash in; when a CDN file or the page's script changes, rebuild and re-run the
  load, sample, plot and every download). Built 2026-11-09 (owner's choices): Python in a web
  worker, so the page stays live while it reads or exports; progress by file, run and step;
  Cancel stops before the next step, "Stop now" restarts Python and reads the list again;
  where no worker starts, Python on the page as before (see `HANDOFFS.md`). Not built, open: a skip link and labelled sections, a keyboard and screen-reader way to
  read plot values, the plot's legend and tracks at phone width (not checked), hints in
  the Columns picker on touch. Not tested: Firefox, Safari (downloads after long work), a
  real screen reader, the Copy button, contrast ratios, the Pages sample on the live site.
  Left out on purpose: keeping runs across reloads (IndexedDB; "closing the tab clears
  everything" is a promise in `WEB_PRIVACY.md`). Translations of the new texts are by
  machine. Released: 0.26.0.

### Agents and scripts (Track 9)

The 2026-11-11 decisions are in the Track 9 row of "Web page review", "Tracks". Later
answers (owner, 2026-11-11 / 11-12; built in 0.37.0 unless marked):

- **A11 `pycorn-setup --from-env`:** an existing file is updated in place only with
  `--overwrite` (else exit 2, nothing written). Before any change to an existing settings
  file, in both modes, a dated copy `<name>.<YYYY-MM-DD_HHMMSS>.bak` is kept next to it;
  copies are never removed, one warning past 5 (each holds the password). A new file omits
  empty optional keys (both modes).
- **A3 `pycorn inspect`:** module `pycorn/summary.py` (not `inspect.py`, not
  `run_summary.py`: a submodule of that name would shadow `pycorn.run_summary`). Masked by
  default: people, run / folder / file / method names, names a user set on chromatograms
  and curves, text variable values; result IDs, column, phase and fraction names, the system
  type and the database label stay.
- **A4 masking per command:** `find`, `export`, `bypass-uv` and `windows` mask people only
  (run names shown), on stdout and stderr alike in the machine modes; a database is never
  named, only its label.
- **A5:** the page's `index.csv` takes the CLI's columns (a renamed output, CHANGELOG
  0.37.0); `export_error` in both; `--mask-identifying` masks `index.csv`. Kept as built:
  the page's index stamps empty, `options.json` without formats, `path_length_cm` a plain
  number; the page CSV with LF line ends, the CLI's with CRLF.
- **A7 `pycorn windows`:** strict exit 1 (a run not read, a curve matching nothing, a
  window with no rows or n = 0); `source` and `result_id` columns first. M22's page:
  unnamed windows `w1..`, every role listed, none ticked, unit default min, ticked runs not
  capped.
- **A10 MCP server: paused** (owner, 2026-11-12). Answers kept for later: tools `schema`,
  `sources`, `inspect`, `find`, `export`, `bypass-uv` (and `windows`); masked unless the
  person starting it passes `--allow-unmasked`; export only under an `--export-root` set at
  start.

## Facts and gotchas

Moved to [`FACTS.md`](FACTS.md) (bundle facts F1-F24, database, run log and timeline,
method and injection, CADET facts X1-X24, and the closed review of 2026-11-08, a-z).
References such as F21 or X3 in this file mean that file.

## Open, by goal

Owner, 2026-11-08: ask before starting any of these, with options and data. The first column
of the Goals table at the top of this file points here.

### Modelers (CADET)

1. D30: should `export_cadet()` / `pycorn export` take the offline assays themselves (e.g.
   `--fractions assays.csv`)? Today: `build_experiment(components=, fraction_masses=)`.
2. D28 (6): settled 2026-11-11 (W17, 0.36.0): the original chromatogram by default, every
   selected one exported with a warning; imported chromatograms read their own run log
   (0.37.0).
3. The CADET physics items in "Modeler input" (M1-M9). The review
   items i-m (see [`FACTS.md`](FACTS.md), "Review 2026-11-08") are M1 and M3-M6.
4. Issue #1 (UV concentration and signal calibration): M10-M13 and the follow-up idea (a run's
   own by-pass epsilon for its mg/ml curves, ask first), all under "Status by track", "UV
   concentration and signal calibration (issue #1)", "Open".
5. Small asks of the owner: a run whose layout explicitly turns retention adjustment off (to
   confirm the injection-1 default, F21); a 7.11 run with the x axis zeroed at a phase (to
   confirm `AdjustRetentionToZeroAtPhase` / `...X`); compare peak values database vs ZIP;
   what `mFillLoopWith` / `mEmptyLoopWith` mean; the `xUV cell path length` name in
   pycorn-bin output (review x, not done).
6. **CADET export form in the web page** (W14, owner 2026-11-10): on hold, unbuilt until the
   owner gives further input; see "Web page review (owner, 2026-11-10)". Per-phase curve
   statistics (M22) are built (0.35.0, interim phase rule), inlets from the system
   configuration too; the remaining run structure decisions (Track 0: phases, repeats,
   feed volumes) come before any further CADET change.
7. **Inlet follow-ups:** the quaternary valve's plumbing from the instrument manual (today
   provisional: its lines feed the A and B pumps). An assumed `Buffer` sample inlet keeps
   counting as feed, with a warning (owner, 2026-11-11; 0.36.0). See "Run structure",
   "inlets from the system configuration".

### Run structure (active)

See "Status by track", "Run structure", and the Decide items in
[`docs/RUN_STRUCTURE_REVIEW.md`](RUN_STRUCTURE_REVIEW.md). This replaces the old open
question 6 and roadmap item 8: **phase / fraction table enhancements** (after the first cut
built from the CADET sections, Decisions "What ran in each phase"). Open, with the samples
behind them: (a) the lead-in: leave out a previous phase's value in a phase's first seconds
(u73_blank: 0.4-1.8 s at every phase start), or keep every value (decided 2026-11-12,
Decide 7-10: fold for display with a `held_over` note, see "Run structure"; not built); (b) changes as a sequence
(today), the longest-running value plus min/max, or both; (c) %B as text (today),
start/end numbers, or both; (d) more columns from the sections: sample flow, outlet valve,
wavelengths, holds, pauses in the row. Every branch (Python tables, export, page) keeps the
same columns and formats. Decided and built: inlets (Decide 22, 0.35.0). (a) and the repeats
(Decide 9, 23) also replace M22's interim phase rule.

### Non-programmers

1. **Getting-started docs** (owner, 2026-11-08; roadmap item 7): `docs/GETTING_STARTED.md`
   is written (2026-11-09): one page by task (browser, zip to Excel, find runs, database
   setup, errors), the git-URL install, modeling left to the existing guides; linked from
   the README. Screenshots of the page, the workbook Summary and Chart (sample run; hidden
   data sheets and fit-to-width only in a temp copy for LibreOffice) and a `pycorn-setup`
   transcript with placeholders are in. Open: the install line once PyPI exists (item 2),
   and re-making the workbook images when a release changes the Summary.
2. **PyPI**: publishing so installing is `pip install pycorn` rather than a git URL. Waits
   on the upstream answer about numbering (see "Upstream merge").
3. Built, later only with the owner: plain-language messages (word more errors as users
   report them: `plain_error`, `_CONNECT_ERRORS`); the in-browser page (an offline bundle,
   more views; SRI hashes, the Content-Security-Policy and the web worker are in, see the
   usability audit entry). Details: Decisions, "Usability for
   non-programmers"; handoffs in [`HANDOFFS.md`](HANDOFFS.md).
4. **Web page review** (owner, 2026-11-10): W1-W13 and W15, with a release plan (0.31.2-0.37.0) and
   tracks that can run at once, in "Status by track", "Web page review (owner,
   2026-11-10)"; the UNICORN report comparison of the remaining sections is there too. Ask
   each item's open lines before building it.

### Search and export

1. **Filters on file catalogs.** The `db.find()` filters on `catalog_from_files()` rows, plus
   `pycorn find` flags for files. Open: `filter_catalog(cat, **filters)` shared with the
   database rules, or `catalog_from_files(paths, **filters)`; whether `find` takes paths;
   reuse `search.py`'s parsing so both behave alike.
2. **Local index.** One row per result with what SQL can't give (phases, duration, volume, CV,
   Vc source, pauses and UTC quality, injection and fraction counts), stored outside the repo
   (default the user's cache directory, never committed), keyed by database, ResultID and
   `tstamp` (files: path, member, `match_key`), rebuilt only for changed results; `find()`
   would join it (`min_cv=`). A method-name filter (Q7) would sit on top. Open: format,
   location, fields, cost of a full build.
3. **Export ideas:** an export-wide `summary.csv` (a row per run); `export --dry-run`; a
   Python `pycorn.export_runs(catalog, out, formats, options)`; stable folder names across
   exports (always `_<ResultID>`, or reuse an existing `index.csv`); Parquet / Feather; a long
   ("tidy") curves table; curve selection by role (`--roles`) or caller names; resampling to a
   common grid (`--grid`); parallel loading; reporting runs that left the selection on
   `--update`.
4. Smaller: a Vc mapping file; piecewise CV per block for runs with several `Base CV` values;
   reading `.res` files into the model (`ResSource`).

### Upstream merge (ronald-jaepel/PyCORN)

Planned 2026-11-08. Progress at 2026-11-12: `upstream` is a remote; the merge of
`upstream/master` into master is not done. The public history was rebuilt on 2026-11-12, so
the old `upstream-base` tag and the `pr1-parser-packaging` branch were removed (owner); pick
and tag a new frozen base on the rebuilt history before stage 1. The issue (step 0 item 3) is
open as ronald-jaepel/PyCORN#4 (no replies yet); PR 1 is not opened. Do the rest in order and
ask the owner before each step.

**Where things stand.** The fork was taken from upstream at `85cf59a` (2024-02-01).
- Upstream since then: `3447ca8` + merge `da03b43` (PR #3), a one-line `setup.py` fix
  (`README.rst` -> `README.md`). The fork already fixes this its own way.
- Fork since then: about 100 files, about +32.6k / -0.6k lines (2026-11-09).

One PR of that size cannot be reviewed, so the work goes up in five stages.

**Owner decisions (2026-11-08):**
- Direction: fork -> upstream PRs.
- Each stage is one snapshot commit of a set of files from a frozen fork commit; the full
  history stays in the fork.
- Fork-only files stay here: `AGENTS.md`, `docs/ROADMAP.md`, `docs/PHASE0_FINDINGS.md`,
  `sql/test_connection.py`, `sql/test_validation.py`.

**Step 0.**
1. Push the fork and tag the frozen commit `upstream-base`.
2. Add the remote `upstream` and merge `upstream/master` into master. The only conflict is the
   `long_description` line in `setup.py`; keep the fork's version.
3. Open an issue upstream before any code: draft below. Ronald may not want PR 4 or PR 5; those
   stages then stay in the fork.

**Mechanics.** For each stage:
1. Create a branch from `upstream/master`, or stack it on the previous stage while that one is
   still under review.
2. Run `git checkout upstream-base -- <paths>`.
3. Trim `pycorn/__init__.py` and `setup.py` (`packages=`, extras, entry points) so they name
   only what has landed.
4. Make one commit and open a PR against `ronald-jaepel/PyCORN:master`.

The cut lines follow the imports:
- `pymssql` is imported lazily (`db_connector.py`).
- `run_export.py` and `uv_calibration.py` import `exporters.cadet` inside functions.
- `file_catalog.py` takes only helpers from `search`.
- `utils.py` and `examplescripts/pycorn-bin.py` need `curve_roles` and `sources`, so they
  cannot go before PR 2.

**Stages.**
1. **Parser fixes + packaging.**
   - Files: `pycorn/pycorn.py`, `bundle_codecs.py`, `pycorn/docs/USAGE_pycorn_module.md`,
     `setup.py`, `pytest.ini`, `tests/conftest.py`, `.gitignore`, `.gitattributes`, the
     anonymized `samples/sample.zip`, `tools/anonymize_zip.py`, and the `PcUni6` tests of
     `tests/test_pycorn.py`.
   - `setup.py`: keep upstream's author and `url`, and restore the GPLv2 classifier. Keep
     numpy/pandas/xlsxwriter optional until Ronald answers.
   - The PR text must say that the output changes for existing users: `PcUni6` returns full
     arrays (22,597 points on `sample.zip`), takes `keep_all=True`, has a `date`, and allows a
     second `clean_up()`. List every change from `git diff 85cf59a <frozen base> -- pycorn/pycorn.py`.
   - Open check: the old `import_xml_as_df` test may fail on the anonymized zip without the new
     `utils.py`. If so, move the zip swap to PR 2.
2. **Run model + file readers.**
   - Files: `model`, `units`, `curve_roles`, `variables`, `event_tables`, `axes`, `timeline`,
     `column_volume`, `column_geometry`, `concentration`, `sources/` (zip, ufol), `utils.py`,
     `pycorn-bin.py`, the new samples, `SAMPLE_ZIP_CONTENTS.md`, `docs/USER_GUIDE.md`.
   - Tests: `test_axes`, `test_zip_source`, `test_ufol_source`, `test_variables`,
     `test_si_roles_geometry`, `test_pycorn_bin`, and the rest of `test_pycorn`.
3. **Process analysis + CADET export.**
   - Files: `run_log_sections`, `method_hints`, `uv_calibration`, and `exporters/cadet/`
     without `api.py`; `docs/CADET_GUIDE.md`, `cadet_walkthrough.py`,
     `tests/data/core_input_*.json`, `samples/u711_cadet_reference.zip`; the extras `cadet` and `cadet-core`.
   - Tests: `test_run_log_sections`, `test_method_hints`, `test_uv_calibration`, `test_cadet_*`.
   - Ask Ronald to review the CADET mapping.
4. **Database reader + search.**
   - Files: `db_connector`, `db_sources`, `search`, `sources/sql_source`, `setup_env`,
     `pycorn/sql/**`, `sql/.env.example`, `sql/CONNECTOR_GUIDE.md`, `sql/example_usage*.py`,
     `sql/requirements.txt`, `sql/scripts/*`; the extra `database`; `pycorn-setup`.
   - Tests: `test_db_connector`, `test_db_sources`, `test_search`, `test_sql_source`,
     `test_setup_env`.
5. **Catalog, export, CLI.**
   - Files: `file_catalog`, `run_export`, `excel_export`, `messages`, `cli`, `__main__`,
     `exporters/cadet/api.py`, the final `__init__.py`, `docs/EXPORT_GUIDE.md`,
     `docs/PROJECT_SUMMARY.md`, `README.md` (keep upstream's url and credits); the `pycorn`
     entry point and the agreed `install_requires`.
   - Tests: `test_cli`, `test_load_many`, `test_excel_export`, `test_export_cadet`,
     `test_messages`.

**Rewrite on each stage branch only.**
- Find text that points to fork-only things with
  `git grep -nE "ROADMAP|AGENTS\.md|PHASE0|\bD[0-9]{1,2}\b|\bQ[0-9]\b|issue #1"`.
- Code comments citing D-numbers become plain explanations. The fork keeps its references.

**Checks for each stage.**
- In a clean venv with `.[testing]` plus that stage's extras, `python -m pytest` passes.
- `import pycorn` works without pymssql, CADET-Process, h5py, and (before PR 5) xlsxwriter.
- `git diff upstream-base` is empty except `__init__.py`, `setup.py`, the README and the
  rewritten docs.
- Run `anonymize_zip.py` again on every zip, including the nested members.
- `sql/.env.example` holds only empty placeholders.

**After each merge.**
- Merge `upstream/master` back into the fork; on conflicts, keep the fork's `__init__.py`,
  `setup.py`, README and docs.
- Move `upstream-base` forward before the next stage.

**Draft issue.** Title: "Offering upstream: UNICORN 6+ Run model, database reader and CADET
export (staged)". The body:
- Introduce the fork: the `Run` model with Vc/CV/UTC axes, the `.zip`/`.UFol` readers, the
  UNICORN 7 SQL Server reader, CSV/Excel export, and the CADET-Process/CADET-Core exporter.
- List the five stages.
- Ask five questions:
  - Should all five stages go upstream, or only some?
  - Should numpy/pandas/xlsxwriter become required?
  - Is replacing `samples/sample.zip` with an anonymized copy OK?
  - Is the change to `PcUni6` returning full arrays welcome?
  - Can the merged package be numbered as the alpha of 1.0 (`1.0.0a1`, PEP 440)? The `PcUni6`
    change breaks existing output, which a major version signals; "alpha" matches "validated
    only on the files and databases it was tested with". PyPI `pycorn` is at 0.19 (upstream's
    name), the fork at 0.20. pip skips pre-releases without `--pre`, so the first release on
    PyPI for `pip install pycorn` (item 7) would be an `rc` or 1.0 itself.
    The fork stays on its own numbering (0.24.0 now) until Ronald answers (owner,
    2026-11-08; written when it was 0.20).

## Modeler input

Questions for a modeling SME (owner, 2026-11-08). PyCORN builds only what the run gives
(D28); each item below changes what the CADET files say about the system, so nothing is built
until the owner settles it, with the SME's view.

- **M1 Starting state** (review i). The column, mixer and tubing start empty: `INIT_C` 0 (and
  `INIT_CP` 0 in the particles), salt too, although runs start equilibrated. Most of the
  walkthrough's salt balance below 1 (0.92 since M2) comes from this. Today: set `INIT_C` / `INIT_CP` with
  `core_overrides` (CADET_GUIDE, Known limitations). Options: (a) keep empty, documented;
  (b) start each unit at the first section's buffer when the caller gives `buffers=` (what the
  run had, no new parameter); (c) start at a caller-named section's buffer.
- **M2 Mixer during a column by-pass** (review h). **Decided and built** (owner, 2026-11-08:
  "If there is flow during a by-pass, then yes the mixer stays on and inline"). In a by-pass
  section with flow (not paused, not feed) the buffer inlet and the mixer carry the system
  flow, which goes around the column and joins its outlet before the UV ('tubing', else
  'outlet'); the column gets none. `process_inputs` rows have `bypass_flow_m3_s`,
  `inputs['bypass_flow']`; `system_flows(units, row, bypass)`; CADET-Process splits the
  mixer with `output_states` events. So the simulated outlet in a by-pass is what the UV and
  conductivity saw. Checked: Core 5, 6 DG and 6 FV and CADET-Process give outlet salt 99 % of
  the new buffer 85 s after a %B step inside a 2 ml/min by-pass; walkthrough salt balance
  0.79 -> 0.92. Tested against a lab environment: by-pass windows are common, usually
  followed by the column again, often with a %B or inlet change inside the window. Sample sent to the by-pass stays buffer in the model (D18).
- **M8 Flow path differs by system** (owner, 2026-11-08: "other types of process skids might
  have all inputs including load going through a bubble trap mixer, so there can be
  permutations of this at scale"). Today the path is fixed (D27): buffer inlet -> mixer ->
  column; valve feed enters after the mixer (the mixer holds); feed through an A/B pump line
  passes the mixer; by-pass from the mixer (M2). On such skids the load passes the bubble
  trap / mixer too, so its hold-up spreads the load front. Nothing in a run says which layout
  a system has, and curve or instrument names must not be guessed from (AGENTS.md). Options:
  (a) a caller option for where valve feed enters (`feed_through_mixer` / `--feed-via mixer`);
  (b) a small caller-given flow-path description (named hold-up volumes per path: buffer,
  load, by-pass), built into the same flow sheet, with today's layout as the default;
  (c) a per-system profile file the lab keeps (system type -> path), picked by the run's
  stored system type, never by name guessing. Needs the SME's view of which permutations
  matter (bubble trap volume, filters, pre-column hold-ups).
- **M3 Loop volume on the export path** (review j). `build_experiment(loop_volume=)` ends loop
  feed after the loop volume (D19), but `export_cadet()` and `pycorn export --cadet` don't take
  it (`--loop-volume` reaches the by-pass UV only), so a valve held at Inject counts as feed
  the whole time (over an hour on a run tested against a lab environment). Question: one loop volume per export command, per run
  (CSV column), or both; and Superloop runs.
- **M4 Sample pump inlet named 'Buffer'** (review k). Owner: running sample through the sample
  pump's Buffer inlet is possible, so it must be handled. Today a direct injection drawing only
  from that inlet is 'buffer only, not feed' (D21). Tested against a lab environment, such
  injections can show a clear UV signal in each window. Options: (a) a caller option naming it as sample (as
  `sample_inlets` does for A/B lines); (b) infer from a UV rise in the window; (c) per run in a
  CSV column.
- **M5 Paused sections inside an Inject period** (review l). They are marked feed with Q = 0
  (no mass enters). Tested against a lab environment: they occur, from under a second to
  hours, valve at Inject or Direct inject. The owner is reviewing the data before deciding;
  also whether `exp.fractions` should drop the pseudo 'Waste' rows.
- **M6 Core 5 vs Core 6** (review m). One run tested against a lab environment differs by 8.2 % at the peak (step
  feed, made-up 2.5 x 0.5 cm column at 10 ml/min); others 0.1-0.7 %. Likely discretization; a
  modeler should say what discretization defaults are acceptable before it is called a bug.
- **M7 A check with real model values** (CADET open item 3): needs a run with known
  porosities, transport and binding from the owner.
- **M9 How modelers bring their parameters in** (owner, 2026-11-08: "an important roadmap item
  for the modelers is how to wire CADET params into this tool or how to insert them after").
  Built for now: the generic blocks (D28 (7)): `--core-overrides` / `--process-overrides`
  JSON files and `core_overrides=` / `process_overrides=` in Python, passed on unread; or
  set them after the export on the Core file or the `Process`. Open for the SME: where
  parameter sets live and how they are picked (one file per column / resin / molecule,
  reused across runs; a per-run column in the find CSV; merged from several files, e.g.
  column + binding); whether results of a CADET-Process fit should flow back into a block
  for the next export; and which of the two targets modelers use.

- **M10-M13 (signal calibration: nonlinear UV, polynomial fits, UV baseline, UV-to-conductivity
  volume)** are part of issue #1 and live in "Status by track", "UV concentration and signal
  calibration (issue #1)", with the CADET forum comparison. They need the SME's view too.

