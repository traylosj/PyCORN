# Finding and Exporting Runs from the Command Line

`pycorn find` searches the UNICORN databases your `.env` lists. `pycorn export` writes runs
to CSV, Excel or pickle files, one file per chromatogram. They work from a search or from
`.zip` / `.UFol` files. Both commands come with the package (`pycorn ...`); `python -m pycorn ...` is the same.

For the Python API behind them, see [`docs/USER_GUIDE.md`](USER_GUIDE.md). For the database settings
(`.env`, several servers and databases), see [`sql/CONNECTOR_GUIDE.md`](../sql/CONNECTOR_GUIDE.md).

> Review the outputs before relying on them. PyCORN has only been checked against the
> instruments and UNICORN versions we had access to. Every run folder has a `warnings.csv`:
> read it.

## Typical workflow

```
pycorn find --folder "/DefaultHome/Projects/**" --run-start 2024-01-01..2024-06-30 --csv runs.csv
pycorn export --from runs.csv --out exports/
```

1. `find` prints how many results match and the first 20. The search runs in SQL and is
   paged, so it never reads a whole database. `--csv` saves the selection.
2. You can edit the CSV (delete rows) before exporting. It is the record of what you chose.
   `database`, `result_id` and `tstamp` make it repeatable.
3. `export` loads each run and writes its folder. Runs already exported are skipped.

To use another settings file, give it before the command: `pycorn --env-file FILE find ...`.

Every command says what went wrong in one line and exits with:

| Exit | Meaning |
|------|---------|
| 0 | Everything asked for was done |
| 1 | It ran, but left something out; the lines above say what (a database not reached, a file not read, a run not loaded or `incomplete`) |
| 2 | `error: ...`: a flag, a file or folder name, the settings or the connection; nothing was done |
| 3 | `unexpected error (...)`: a fault in PyCORN; run the same command again with `--debug` and report the output |
| 130 | Interrupted (Ctrl+C) |

`--debug` (before or after the command) adds the full traceback, and for a connection error
the driver's own text with server, login and password redacted. `pycorn --version` prints the
version and commit. For scripts and agents: [AGENT_GUIDE.md](AGENT_GUIDE.md).

## pycorn schema and pycorn sources

`pycorn schema` lists every table this version writes (title, CSV file, `--tables` group,
origin), the download groups, curve roles, units and exit codes; `--json` prints one JSON
document (`"schema": 1`) with every column's origin, meaning and check as well. It reads no
run and no settings.

`pycorn sources` lists the database sources `find` would search, from the same settings
(`--env-file`, `PYCORN_ENV_FILE`, the first `.env` found, `UNICORN_*` variables): for each
alias whether each setting is set or empty, never a value. `--test` connects to each once
(exit 1 when one fails); `--json` prints one JSON document. `pycorn-setup --test-only` runs the
same test.

## pycorn inspect

`pycorn inspect <files>` or `pycorn inspect --from <CSV>` shows what each run holds without
writing anything: name, source, folder, run start, system, column, every chromatogram (kind,
the one export picks by default, Vc and its source, curves with role, unit, points and axes,
phase, injection and fraction positions), method variables and warnings. Never curve values,
IP addresses or ports. Logins, computer and instrument names, serial numbers, run, folder,
file and method names, user-set chromatogram and curve names and text variable values are
masked (`User1`, `Computer1`, `System1`, `Serial1`, `Run1`, `Folder1`, `File1.zip`,
`Method1`, `Chrom1`, `Curve1`, `(text, masked)`) unless `--no-mask`; column and phase names
are not changed. `--json` prints one JSON document (`"schema": 1`, a list of runs), `--jsonl`
one line per run (keys: [AGENT_GUIDE.md](AGENT_GUIDE.md)). Exit 1 when a file was not read or
a run not loaded.

## Output for programs: --json, --jsonl, --csv -

`find`, `export`, `bypass-uv`, `windows` and `inspect` take `--json` (one JSON document on stdout) or
`--jsonl` (one JSON line per row as it is produced, a header first and a summary last); `find`,
`bypass-uv` and `windows` also `--csv -` (the CSV on stdout instead of a file). Then every human line goes
to stderr, an error is one JSON object on stderr (`--json` / `--jsonl`), and the output is
masked: logins and instrument names (`created_by`, `system`, ...) become `User1`, `System1`
unless `--no-mask`; run and folder names are shown. Databases appear by label only (the
`database` column is left out). Files written (`--csv FILE`, `index.csv`, the export files) are
not changed by these flags. Every key and line type: [AGENT_GUIDE.md](AGENT_GUIDE.md).

## pycorn find

Text filters take UNICORN wildcards (`*` and `?`). Repeating a text flag means "any of".
Ranges are `a..b`, and either end may be empty.

| Flag | Meaning | Example |
|------|---------|---------|
| `--folder` | Folder path from the root; `**` includes subfolders | `"/DefaultHome/Projects/**"` |
| `--name` | Result name | `"*Test*"` |
| `--run-start` | Run date range, on the run's own clock (one date = that day) | `2024-01-01..2024-06-30`, `2024-03-01..` |
| `--system`, `--system-type` | Equipment name, equipment type | |
| `--run-type`, `--state` | e.g. `Method`, `Evaluated` | |
| `--batch-id`, `--created-by`, `--column` | Batch id, user, column name recorded at run time | |
| `--run-index`, `--result-id`, `--chromatograms` | A number, a list (`3,5`) or a range (`3..8`) | `--result-id 4700..4800` |
| `--var` | Method variable as the run used it; repeatable, all must hold | see below |
| `--has-role` | Curve roles the run must have | `uv,conductivity` |
| `--has-peaks` / `--no-peaks` | With or without peak tables | |
| `--include-hidden` | Include results in hidden folders | |
| `--sources` | Where to search (default: everything `.env` lists) | `discover`, `ALIAS`, `ALIAS:db1,db2`, `ALIAS:*` |
| `--dedupe` | Keep one copy of a run found in several places (the most recently changed) | |
| `--limit`, `--after`, `--all` | Page size (default 500), continue after a row, or every page | `--after ALIAS#1:4711` |
| `--variables` | Add a text and a number column per method variable to `--csv` | |
| `--show` | Rows to print (default 20) | |
| `--csv` | Save the selection (input for `export --from`); `-` writes it to stdout (no `database` column: `export --from` finds it again by `db_label`; people masked unless `--no-mask`) | `--csv runs.csv`, `--csv -` |
| `--strict` | Stop at a database that cannot be searched (default: report it and go on) | |
| `--json`, `--jsonl` | The rows on stdout as one JSON document or JSON lines (above) | |
| `--no-mask` | `--json`, `--jsonl`, `--csv -`: show logins and instrument names as stored | |

A source that cannot be reached is named at the end (`not searched: source 'ALIAS': cannot
reach the SQL Server: ...`); the count then says "in the databases reached" and the exit code
is 1.

`--var` forms:

- `"Flow rate>=1 ml/min"`: a number comparison. Operators are `==`, `!=`, `<`, `<=`, `>`, `>=`.
  With a unit, stored values are converted to it first. Without one, the stored numbers are
  compared as they are.
- `"Column position=Position 2"`: a single `=` compares text (wildcards allowed).
- `"Sample_ID"`: the run has this variable.

Databases are named by their `.env` alias and a number (`ALIAS#1`), never by server or
database name. When more results follow, `find` prints the `--after` value to continue.

The search CSV holds one row per result. The main columns are `db_label`, `source`, `database`,
`result_id`, `name`, `folder_path`, `run_start_local` / `run_start_utc`, `system`, `state`,
`column`, `tstamp`, `match_key` and `n_copies` (the same run found in other places).

## pycorn export

```
pycorn export --from runs.csv --out exports/ [--format csv,xlsx,pickle] [options]
pycorn export run1.zip project.UFol some_folder/ --out exports/
```

Give either `--from` or files and folders, not both. `--from` accepts a `find` CSV, a CSV from
`catalog_from_files(...).to_csv()`, or a mix of the two.

| Flag | Meaning |
|------|---------|
| `--out` | Output folder (required) |
| `--format` | `csv` (default), `xlsx`, `pickle`; a comma list or repeated |
| `--overwrite` | Replace every run already exported |
| `--update` | Export again only the runs that changed since they were exported, or can't be checked (not with `--overwrite`) |
| `--strict` | Stop at the first run that cannot be loaded or written |
| `--options FILE.json` | The export options from a JSON file (see "Options as a file" below); flags given on the command line replace the file's value of their option |
| `--json`, `--jsonl` | The `index.csv` rows and the totals on stdout (one document, or one line per run as it is done); progress on stderr |
| `--no-mask` | `--json` / `--jsonl`: logins and instrument names as stored (the files are not changed either way) |
| `--chromatogram NAME\|all\|original\|imported\|derived` | Which chromatograms of each run are written; repeatable (`--chromatogram imported --chromatogram Chrom.1`). Default: the run's original chromatogram only (see below). Not `find`'s `--chromatograms`, which filters by their number |

### Which chromatograms

A result can hold more than one chromatogram: the run's own (`original`) and chromatograms
added in UNICORN's Evaluation, `imported` from another result (sometimes a copy of the run
itself) or `derived` there (the user guide, "Several chromatograms", says how they are told
apart). By default only the original chromatogram is written; the others are named in
`warnings.csv` (`source` `chromatograms`: "N other chromatogram(s) not written: ..."), with
their kind and source result. `--chromatogram` picks others by name (as the run stores it) or
by kind; `all` writes every one. A picked chromatogram keeps the file stem it has in an export of
`all`, so the original's files are named the same whatever else is picked. A name a run lacks
is a `chromatograms` line in that run's `warnings.csv`, not an error (one export can cover runs
with different chromatograms). `column_and_timeline.csv` has one row per chromatogram written,
with `chromatogram_kind`, `source_result_id`, `source_result_name` and `copy_of_run`. Python:
`ExportOptions(chromatograms=["imported", "Chrom.1"])`; `pycorn.export_cadet(chromatogram=...)`
takes the same names and words.

An imported chromatogram's phases, injections, fractions, run log sections, phase window
statistics and CADET files come from the run log it carries (`ImportedLogbook`): what ran in
its source result. Its `warnings.csv` lines say so ("imported chromatogram: run log ... is the
source result's ..."). Where this result's own method, system configuration, column type,
mixer, run information or run start is used for it, one line each says so ("... taken from
this result (ResultID ...), which may differ from the source result's run"). Curves of another
method run that its source result had overlaid are written with its curves and named in one
`curves` line; they have no curve role (the user guide, "Several chromatograms"). The columns
are the same as for the original chromatogram.

Until W17 every chromatogram of a run was written; a run with imported or derived
chromatograms now writes only its original one unless `--chromatogram` asks for more.

### Folder layout

```
<out>/
  index.csv  options.json
  <alias>#<n>/<UNICORN folder tree>/<run name>/
    README.txt  provenance.json  <chromatogram>.csv  <chromatogram>_events.csv  ...
    method_variables.csv  column_and_timeline.csv  warnings.csv  column_guide.csv
```

With `--format xlsx` and no `csv` (since 0.32.0, see [CHANGELOG.md](../CHANGELOG.md)) the run
folder holds only the workbook(s) and `README.txt`:

```
  <alias>#<n>/<UNICORN folder tree>/<run name>/
    README.txt  <run name>_<chromatogram>.xlsx
```

No run-level CSVs, no `column_guide.csv` and no `provenance.json`: they are sheets of the
workbook (variables, column and timeline, warnings, column guide, ... and "How it was made" on
the Summary sheet). Every Excel route gives the same: `pycorn export --format xlsx`, the
page's single-run download (a small zip of that folder, `<run name>_xlsx.zip`) and its
"download all" (one such folder per run in `PyCORN_export_xlsx.zip`, plus `index.csv` and
`options.json`). The run
CSVs and `provenance.json` are written when `csv` (or `pickle`, or only `--cadet` /
`--cadet-core`) is among the formats. `--update` and the "changed since export" check read a
workbook-only folder's stamp from the workbook's Column and timeline sheet, and still read the
`column_and_timeline.csv` of older folders.

Names (since 0.21.0, see [CHANGELOG.md](../CHANGELOG.md)): every table has one name, the same
in the in-browser page, the Excel sheets and the CSV files. The CSV file is the title in lower
case with `_` (Planned vs run - blocks: `planned_vs_run_blocks.csv`). The list is in
`pycorn/export_tables.py`. CADET files keep CADET's own names.

Where the data comes from (since 0.22.0): every column is one of

- `UNICORN`: stored data, only reshaped.
- `User input`: a value you gave, as given (epsilon, response factor, loop volume, sample
  concentration). Column geometry can be stored or yours run by run, so it is analysis
  (`geometry_sources` says which).
- `PyCORN calculated`: a fixed formula on stored values and your inputs (adjusted time and volume, `end -
  start` of stored values, stored date - stored UTC offset, ...). Its `check` in the column
  guide says how to redo it or where UNICORN shows the same number.
- `PyCORN analysis`: PyCORN's rules and choices (Vc, pauses, where a phase or section ends,
  planned-vs-run status, ...). There is no UNICORN number to compare with. A formula on an
  analysis value is analysis too: CV (needs Vc), UTC (needs the rebuilt pauses), a row's end
  and length.
- `PyCORN`: about the export itself (version, options, the column guide).

A table is one of these, or `UNICORN + PyCORN` when it holds stored and PyCORN columns.
`column_guide.csv` lists every column written, with its origin, meaning and check.

How it was made: `README.txt` (for people) and `provenance.json` (for scripts) record the app,
version and git commit, the repository, the route (`CLI: pycorn export`, the web page, or a
Python call), the command line, the formats and every option that differs from that version's
defaults. The Excel Summary sheet and `column_and_timeline.csv` (`pycorn_version`,
`export_route`, `export_options`) hold the same. To reproduce an export, install that commit
(or release `v<version>`) and run the same command on the same input.

- File runs use `zip` or `ufol` in place of `<alias>#<n>`, then the folder tree stored in
  the file.
- Names are cleaned to characters every file system accepts.
- When two runs in one export would get the same folder, both get `_<ResultID>` (file runs:
  the original id of a `.UFol` member, else the file name). If that still leaves two the
  same (e.g. the same `.UFol` member in two folder exports), the second gets `_2`, the third
  `_3`, ... This is decided per export, so a different selection can name a run differently.
- Each run is written into `<run>.partial` and renamed when done, so an interrupted export
  never leaves a half-written run under the real name. A run that fails removes its
  `.partial` folder.
- A run folder that already exists is skipped before anything is loaded. Its
  `column_and_timeline.csv` (`run_info.csv` before 0.21.0) holds the stamp it was exported with (`export_stamp`), and the export compares it with the
  stamp now. For a database run the stamp is the result's `tstamp` (the one loaded, which is
  newer than the search CSV's if the result changed in between); it changes whenever the
  result is changed in UNICORN. For a file it is the file's size, its
  modified time and a SHA-256 of its content. The content is only read again when the size
  or time differ, so a copied or touched file with the same content still counts as unchanged. `--update` exports the changed runs again; `--overwrite` replaces all of them.

`index.csv` lists every selected run with these columns, then the selection's own columns:

- `export_status`: one of
  - `written`
  - `incomplete`: written, but a CADET file you asked for (`--cadet`, `--cadet-core`) was not,
    for at least one chromatogram, e.g. no column geometry or no run log; the console and
    `warnings.csv` say which and why
  - `skipped (exists)`: unchanged
  - `skipped (changed since export)`
  - `skipped (exists, not checked)`: no stamp then or now, such as a folder written by an
    older PyCORN or a CSV without `tstamp`
  - `failed`
  - `not loaded`
- `export_folder`, `export_files`
- `export_error`: one sentence why a run is `not loaded` (the reason in the console warning),
  `failed` (the error) or `incomplete` (`not written: ...`, what was asked for and not
  written); empty otherwise. The home folder is written `~`
- `export_changed`: `yes`, `unknown` or empty
- `export_stamp`

With `--mask-identifying` (`mask_identifying=True`) `index.csv` is masked as the run files
are: the selection's `system` and `created_by` become `System1`, `User1`, ... (built-in
accounts such as `Default` stay; a login's `DOMAIN\name` parts are masked too), and those
values are replaced inside `match_key` and `export_error`. Without it, as the selection holds
them. (`export_error` and this masking: see [CHANGELOG.md](../CHANGELOG.md).)

The page's "Download all" zip has an `index.csv` with the same columns (see [CHANGELOG.md](../CHANGELOG.md); before, it had
`run`, `file`, `member`, `export_folder`, `export_status`, `export_files` (a count) and `error`).
There, `export_changed` and `export_stamp` are empty (the page does not skip runs), and "mask"
masks it as `--mask-identifying` does.

`options.json` (next to `index.csv`, rewritten by every export like it) holds the options of
that export in the form `--options` reads, so `pycorn export ... --options <out>/options.json`
writes the same files again. The formats are not in it (give `--format`).

The exit code is 0 when everything was written or skipped, 1 when a run failed, could not be
loaded or is `incomplete`, or a file given could not be read (not a zip, damaged, or not a
UNICORN result), and 2 for a bad flag or setting (nothing loaded). `incomplete` also covers an
`--epsilon` wavelength that no UV curve of the run has. A search CSV that lists a run twice
exports it once, with a warning.

### Files per run (csv)

Per chromatogram (`<c>` is its cleaned name). Tables that would be empty are left out,
except the curves and events files.

| File | Content |
|------|---------|
| `<c>.csv` | Every curve side by side, NaN-padded. Per curve: `<curve>: time (<unit>)`, `volume`, `CV`, `UTC`, `value (<unit>)`. Raw values as stored; adjusted time and volume are time and volume minus `retention_zero_time` / `retention_zero_volume` in the column and timeline table |
| `<c>_events.csv` | Every event: `event_curve`, `event_curve_type`, the same axes, `text`, `type`, `subtype`, `instruction_feedback` |
| `<c>_peaks.csv` | Every peak table row, UNICORN's peak table first, each unit after its value: `peak_table`, `number` (UNICORN's "No"), `MaxPeakRetention`, `retention_unit`, `Area`, `area_unit`, `PercentOfTotalPeakArea`, `Height`, `height_unit`, `StartPeakVial`, `MaxPeakVial`, `EndPeakVial`, `max_peak_on_run_axis` (the maximum on the run's stored volume or time axis, where the curves put it), `retention_zero_injection` (the injection the retentions count from, as UNICORN shows them), then every other stored column |
| `<c>_peak_table_summary.csv` | Each peak table's header as UNICORN's report prints it under the peaks: `peak_table`, `field` (Total area, Total peak width, Calculated from, Baseline name, Baseline type, Column height, Resolution algorithm, Resolution algorithm id, Resolution algorithm name, ...), `value` (curve numbers as curve names), `unit`, `stored_as` (the stored field); every other stored field under its stored name. `Baseline name` is the baseline curve's name, or for a zero baseline `Zero Baseline`, as the report prints it; `Resolution algorithm name` is the report's formula (`1.177 * ((Ret2-Ret1) / (WidthHalfHeight2 + WidthHalfHeight1))` for algorithm 3). Readable names are given only where a UNICORN report showed them; the database stores the baseline type and algorithm id, a .zip only the algorithm (`Algorithm3`, its id read from it). For a UV peak table, `UV normalized to 1 cm path length` (true / false) says whether heights and areas are of the UV signal normalized to 1 cm (UNICORN's evaluation setting; then they are on another scale than the stored curve): from a stored field when there is one (none seen so far), else from the evaluation log's `Peak Integration` entry that made the table (same chromatogram, logged at the table's `Created`; true when it says `UV signal is normalized to 1 cm path length`); `stored_as` names the entry. When neither tells, the row is left out and `warnings.csv` says so |
| `<c>_phases.csv`, `_injections.csv`, `_fractions.csv` (PyCORN, from the event log) | One row per phase / injection / fraction: `number`, `name`, start and end on every axis (`start_time` ... `end_utc`), `length_time` / `length_volume` / `length_cv` (end - start), then what ran in it, from the sections below: `blocks`, `system_flow_ml_min`, `column_flow_ml_min`, `percent_b`, `percent_q`, `inlets` and `inlets_source` (see below), `column_position`, `injection_valve`, `sample_use`, `set_marks` (values in time order, `5 -> 1`; a %B ramp reads `ramp 0 to 100`), and `end_source`. Row 0 (`pseudo` true) is the stretch before the first one. An injection has no text in UNICORN, so its `name` is `Injection 1`, `Injection 2`, ... (`text` stays empty). Fractions have a `pool` column after `name`: the pooled fraction the fraction is in (its `pool` in `_pooled_fractions.csv`), empty when none |
| `<c>_pooled_fractions.csv` (PyCORN, from the event log) | Fractions pooled in UNICORN's evaluation (Pool Fractions), one row per pool: `number`, `pool` (its first and last fraction, `3.A.2-3.A.3`), `fractions` (JSON list of the Fractions table's fractions that start inside it), `n_fractions`, `first_fraction` (stored), `last_fraction` (stored in the database; from a .zip, the last fraction inside it), start and end on every axis and `length_*` as in Fractions, `end_source`, `event_curve` (the pooled curve), `time_unit`, `volume_unit`. A pooled curve is found by its stored type, never its name: an event curve of type `Fraction` made in evaluation (not original data) with UNICORN's default name ending `,POOL` or an event that stores a second text (`EventText2`, the pool's last fraction). Each of its marks ends where the next starts; a mark holding two or more fractions (or storing `EventText2`) is a pool, one holding one fraction or none (waste) is not. A pool whose fractions do not run from its stored first to last fraction gives a warning |
| `<c>_run_log_sections.csv` | Process sections from the executed run log: times (with and without pauses), `phase`, `block`, flows, `percent_b` and its slope, `percent_q`, valve positions, `feed` / `sample_use`, `inlets` and `inlets_source` (see below), `wavelengths_nm`, then the curve checks (`system_flow_measured`, `sample_flow_measured`, `percent_b_max_deviation`, `percent_q_max_deviation`) |

| `<c>_pauses.csv` | Pauses the UNICORN clock stopped for: elapsed time, start/end UTC, duration, source, quality |

`inlets` is the inlets the run draws from: line A below 100 %B, line B above 0 %B (a
constant %B between 0 and 100 reads `A1+B1`, a %B ramp `ramp A1 to B1`), the quaternary shares as
logged (`Q1 50%+Q2 50%`), `Loop` while the injection valve is at Inject, the sample inlet at
Direct inject. A logged position is shown as logged (`B2`). Where the run log set none, the
system configuration stored with the result decides (`run.system_configuration()`): a line
with no inlet valve is one fixed line, shown alone (`B`); a line with a valve takes the
valve's first position, written with `(assumed)` (`B1 (assumed)`; the sample inlet valve
`Buffer (assumed)`); a configuration that doesn't say gives `B (not known)` and a warning.
`inlets_source` (phases, injections, fractions and sections) says how sure, in time order:
`known` (logged, or a line with no valve), `assumed` or `not known`; the least sure line
used decides. The quaternary valve's lines are taken, for now, as feeding the A and B pumps
(provisional, a warning says so).

Per run:

| File | Content |
|------|---------|
| `README.txt` | What every file of the folder holds, its origin, and how the export was made (version, commit, command, options) |
| `provenance.json` | The same record for scripts: `run`, `read_from`, `app`, `version`, `commit`, `repository`, `route`, `command`, `formats`, `options`, `exported_utc` |
| `column_guide.csv` | Every column of the tables written: `table`, `csv_file`, `table_origin`, `column`, `origin` (`UNICORN`, `User input`, `PyCORN calculated`, `PyCORN analysis` or `PyCORN`), `meaning`, `check` (calculated columns: how to check it). Curve columns are listed once per kind (`<curve>: time (<unit>)`) |
| `method_variables.csv` | Method variables as the run used them, listed as UNICORN's variable list: `phase` and `block` (where the method first uses it), `variable`, `value` (text as stored), `unit` (as UNICORN shows it: `base` replaced by the block's base, CV, ml or min), `number`, `kind`, `scouting`, `stored_unit` (the unit as stored). In method order; stored variables the method does not use come last |
| `column_and_timeline.csv` | One row per chromatogram written: `chromatogram`, `chromatogram_kind` (`original`, `imported`, `derived`), `source_result_id` / `source_result_name` (imported: the result it came from), `copy_of_run` (imported: true when it is a copy of this run), run name and UNICORN version, Vc (`vc`, `vc_unit`, `vc_source`, `vc_candidates`, `vc_disagreements`), column positions, column geometry in SI (`column_length_m`, `column_diameter_m`, `particle_radius_m`, `geometry_sources`, `geometry_vs_vc`), UTC timeline (`timeline_quality`, `start_utc`, `end_utc`, `total_paused_s`, `n_pauses`, ...), then `export_stamp`, `match_key`, `pycorn_version`, `export_route`, `export_options` |
| `warnings.csv` | `chromatogram`, `source` (`run`, `chromatograms`, `sections`, `geometry`, `concentration`, `phase_windows`, `method_hints`, `cadet`, `cadet_core`, `curves`, `export`), `category` (`Vc`, `CV`, `UTC` for axis warnings), `message`. Whenever CADET files are written, one `cadet` line says the CADET export is experimental: its assumptions are incomplete and not yet tested, and will change significantly as development continues |
| `run_and_method.csv` | `section`, `field`, `value`: the result (name, system, folder, created / modified with their UTC offset and by whom, run type, and the notes as UNICORN's report names them: `Method notes`, `Start notes`, `Run notes`, `Evaluation notes`) and the method (name, dates with their UTC offset and by whom, technique, units, CIP / BufferPro flags, scouting name, start-protocol items, result folder and naming, `result name variable` when the result is named by a variable, documentation), then one `evaluation procedure` row per evaluation procedure of the method (`field` its name, `value` `selected` or `not selected`). A result's `created` is when it was saved, which for a result re-saved in a later UNICORN is not when it ran (that is `start_utc` in `column_and_timeline.csv`) |
| `method_steps.csv` | The method as programmed, as UNICORN's report prints it: from the main block, each called block's instructions right after its call (a `Block` instruction, or a `Watch` whose action calls a block). `step`, `phase` (of the phase block that calls it; a `Phase:` line belongs to the phase it opens), `block`, `line` (UNICORN's method text, indented 4 spaces per level: `0.00 System flow: (5.000)#Flow rate {ml/min}, (Pre column pressure)#Pressure control`; a breakpoint variable on the instruction that stores it, `(22.00)#SampleVol Injection valve: Manual load`, `(5.00)#Equilibration volume End_Block`; a column volume set by a variable `(Vc=0.170)#ColVolume {ml}`), `depth` (0 = main block), `path` (`Main > EQUILIBRATION > Equilibrate`), `called_from` (the `step` of the instruction that calls the block; empty in the main block), `base`, `breakpoint`, `breakpoint_variable` (on every instruction at that breakpoint: it moves them all), `breakpoint_used` (as the run's variables set it), `instruction`, `parameters` (`name: value unit [variable]`), `run_values` (the values the run used for those variables), `main_block`, `unused`. The page shows `phase`, `block` and `line` by default. Parameters UNICORN stores as hidden are left out, as UNICORN does. The method is a template: what ran is in the sections |
| `scouting_runs.csv` | The scouting plan from the method: one row per planned run (`run`, numbered from 1 as UNICORN's report prints it; `position`, as stored, from 0; `included`), then one column per scouted variable in the method's variable-list order, with the values typed into the method's scouting table. Every run in the series has the same table, and it has no units. For the values this run used, with units, see the rows marked `scouting` in `method_variables.csv` |
| `start_questions.csv` | The method's start-protocol questions, as UNICORN's report lists them: `number` (from 1), `question`, `answer_type` (as the report prints it, `Textual input`; the stored name where the report's text is not known), `answer` (the answer the run was started with; empty when none was given), `stored_answer_type`, `mandatory`, `requires_signature`, `visible_in_chromatogram`, `method_position` |
| `column.csv` | The column type stored with the run, as UNICORN's report lists it: `column`, `group` (Run parameters, Details, Ordering information, then `Other (as stored)` for every other stored field), `parameter`, `value`, `unit`, `source` (the stored field). Column volume and the linear flow rates are calculated from diameter and bed height (`source` gives the formula); the packing instructions are left out |
| `calibration.csv` | As UNICORN's report lists a calibration: per sensor its reference values, then its constants. `calibration` (pH, conductivity, pressure sensors, ...), `date` (as stored), `utc_offset_min`, `date_utc`, `operator`, `locally_calibrated`, `kind` (`reference` or `constant`), `position`, `item` (`Reference value 1`, or the constant's description, e.g. `Calibrated electrode slope`), `value`, `unit` (a constant's unit is the end of its stored description, `...; %`) |
| `instrument.csv` | `group`, `item`, `value`, `unit`: instrument configuration (model, firmware, library versions), strategy, system (name, computer, serial), mixer volume, UV cell (nominal and real path length), every module's serial number, versions, run hours and pump strokes, and the enabled components. IP addresses and ports are never written |
| `system_settings.csv` | `group`, `instruction`, `parameter`, `value`: the system settings the run started with (alarm limits, wavelengths, averaging, ...) |
| `audit_log.csv` | One row per audit-trail entry: `trail`, `time` (as stored), `utc_offset_min`, `time_utc`, `type`, `group`, `operator`, `entry`, `has_details` |

The last nine are left out when empty (e.g. a result without its method). They show names,
logins, computer and system names and serial numbers as stored; `--mask-identifying` masks
them (below).

Conventions:

- CSV files (and `index.csv`, `pycorn find --csv`) are UTF-8 with a byte order mark, so Excel
  shows units such as `°C` right; pandas skips the mark. The CADET-Process signal CSVs
  (`<c>_cadet/`) hold numbers only and have no mark.
- UTC columns are UTC without a time zone (Excel cannot store zones).
- Cells holding lists or dicts (Vc candidates, section inlets, ...) are JSON text.
- If an extra table can't be built, it is left out with an `export` line in `warnings.csv`.
  The curves are still written.
- Vc comes from the run (see [`docs/USER_GUIDE.md`](USER_GUIDE.md)). The column geometry is only a cross-check,
  unless you give it yourself (below).

### xlsx and pickle

- `xlsx`: one workbook per chromatogram, `<run name>_<c>.xlsx`, made to be read in Excel:
  - `Summary` first: run, chromatogram, chromatogram kind (`original`, `imported from <result>`,
    `derived`), file name, dates, method, instrument, Vc, column,
    units, how many warnings to check, how it was made (app, version, commit, repository,
    route, command, options), the origins, and what each sheet holds with its origin.
  - `Chart`: the chromatogram against volume (time when a curve has no volume): UV curves on
    the left axis, conductivity (else %B) on the right, injection and fraction starts as
    labelled markers. Curves are found by role, so other instruments' names work; flat
    curves (an unused UV channel) are left out.
  - then one sheet per table above, with the header row frozen, filter buttons, fitted
    column widths and dates shown as dates. Numbers are shown with the decimals UNICORN
    specifies: a curve's `AmplitudePrecision`; min 2, ml 3, CV 2, ml/min 3, % 1 (carried to s,
    L, ... by the conversion). The cells keep the full values, as the csv files do. Empty
    tables are left out, as in csv. Time and
    volume columns whose name has no unit get it in the Excel header (`start_time (min)`);
    the csv columns keep their names. Sheets take the tables' titles (Method variables, Run
    and method, Method steps, Scouting runs, Start questions, Column, Calibration, Instrument, System settings,
    Audit log, Column and timeline, Warnings, Column guide: the run-level ones, repeated in
    each workbook).

  Uses `xlsxwriter` (installed with PyCORN); with only `openpyxl` you get the plain tables
  without formatting or chart. A sheet holds at most 1,048,576 rows, and a run over that
  fails: use csv.
- `pickle`: `<c>.pkl` holds `{chromatogram: Run.to_res3_dict()[chromatogram]}`, the legacy
  res3 layout. Only open pickles you trust.
- `README.txt` is written in every format. The other per-run files (`provenance.json`,
  `method_variables.csv`, `column_and_timeline.csv`, `warnings.csv`, `column_guide.csv` and the
  method, calibration and instrument tables) are written in every format except `xlsx` without
  `csv` (0.32.0): there they are sheets of the workbook, and the folder is the workbook and
  `README.txt` (see "Folder layout"). With `pickle` alone the tables are not written, so mg/ml curves (`--epsilon`) and
  `--method-hints` are skipped, with a line in `warnings.csv`. `--epsilon` still reaches the
  CADET outputs.

## Opt-in outputs

Every flag is checked before any run is loaded. A bad value or unit stops with exit code 2.

| Flag | Effect |
|------|--------|
| `--units Q=UNIT,...` | Convert before writing, e.g. `volume=L,flow=L/h,time=h`. Curves, events, tables and Vc follow. CADET outputs convert to SI themselves |
| `--epsilon NM=VALUE [unit]` | Mass extinction coefficient per wavelength (default ml/(mg*cm)), e.g. `280=1.4`; repeatable. Each UV curve at that wavelength gets mg/ml columns `<curve> (mg/ml): ...` and a row in `<c>_concentration.csv` |
| `--epsilon-molar NM=VALUE [unit]` + `--mw "66000 g/mol"` | The same from a molar coefficient (default 1/(M*cm)) |
| `--uv-response FACTOR` | Detector response factor on the coefficient, e.g. `0.92` for a detector that reads 8 % low (mg/ml and CADET). No default; recorded in the `response_factor` column of `<c>_concentration.csv` |
| `--absorbance-limit MAU` | Your detector's linear limit, in stored units (mAU unless a unit follows). mg/ml points above it are empty, and the by-pass table gives no coefficient above it. No default |
| `--bypass-uv` | `<c>_by_pass_uv.csv`: the UV of sample sent to the column by-pass, one row per window and UV channel (kind plateau / pulse / no_signal, level before, max, area, absorbance per cm). Implied by the two flags below |
| `--sample-conc "1.2 mg/ml"` | Sample concentration from your offline assay: adds the extinction coefficient this detector gives (`epsilon_ml_per_mg_cm`) |
| `--loop-volume "20 ul"` | Fixed loop volume, needed for a coefficient from a pulse (the valve period only gives an upper bound) |
| `--window "NAME=PHASE:FROM:TO UNIT"` + `--window-curves NAMES` | `<c>_phase_window_statistics.csv`: curve statistics over windows relative to a phase (below). Repeatable; both flags are needed, there is no default window or curve |
| `--path-length CM` | UV path length used only where the run stores none (mg/ml and CADET); each use is warned |
| `--bed-height`, `--diameter`, `--particle-diameter` | Column geometry with a unit (`"10 cm"`, `"34 um"`). By default each is a fallback: a stored value wins, and every unused value is noted. A run with no Vc at all gets Vc from bed height and diameter (source `geometry`, warned) |
| `--override-geometry` | The geometry you give wins over the stored values (for a system set up wrong) |
| `--method-hints` | `<c>_planned_vs_run_blocks.csv` and `<c>_planned_vs_run_injections.csv`: planned vs executed blocks and injections. These are hints only; the export itself uses what ran |
| `--mask-identifying` | For files you share: logins (also without their domain), computer and system names and serial numbers become `User1`, `Computer1`, `System1`, `Serial1`, ... in every table and pickle, event texts included. Built-in accounts (`Default`, `System`) stay. Free text people type, which may name anyone (method, start, run and evaluation notes; start-protocol answers that are not numbers), becomes `(free text, masked)` as a whole. Run, folder and method names are not changed. The CADET files hold none of these |
| `--cadet`, `--cadet-core 5\|6`, `--core-format`, `--mixer-volume`, `--tubing-volume`, `--sample-inlets`, `--no-system-volumes` | CADET outputs (`<c>_cadet/`, `<c>_core_v<n>.json` / `.h5`); see [`docs/CADET_GUIDE.md`](CADET_GUIDE.md). Experimental: the CADET export's assumptions are incomplete and not yet tested, and will change significantly as development continues; the command prints that as a `note:` and `warnings.csv` holds it whenever CADET files are written |
| `--core-overrides FILE.json`, `--process-overrides FILE.json` | Your model values, one JSON object each, in CADET-Core's / CADET-Process's own layout: merged into every Core file / stored in each `experiment.json`, unchanged (see [`docs/CADET_GUIDE.md`](CADET_GUIDE.md), "Your model values") |

### Options as a file (`--options`)

Every option above and under "Smaller files" can come from one JSON object instead of flags:
the `options.json` an export writes next to `index.csv`, the one in the page's "Download all"
zip, the `options` of a run's `provenance.json`, or your own. Keys are the Python names of
`ExportOptions` (`ExportOptions.to_dict()` writes every key, in this order, defaults included):

| Key | Flag | Value |
|-----|------|-------|
| `units` | `--units` | `{"volume": "L", "flow": "L/h"}` |
| `method_hints` | `--method-hints` | `true` / `false` (the page always writes `true`) |
| `mask_identifying` | `--mask-identifying` | `true` / `false` |
| `path_length_cm` | `--path-length` | a number, in cm |
| `epsilon`, `epsilon_molar` | `--epsilon`, `--epsilon-molar` | `{"280": [1.4, "ml/(mg*cm)"]}` (wavelength in nm as text) |
| `molecular_weight` | `--mw` | `[66000, "g/mol"]` |
| `uv_response` | `--uv-response` | a number |
| `bypass_uv` | `--bypass-uv` | `true` / `false` |
| `sample_concentration`, `loop_volume`, `absorbance_limit` | `--sample-conc`, `--loop-volume`, `--absorbance-limit` | `[1.2, "mg/ml"]`, `[20, "ul"]`, `[2500, "mAU"]` |
| `windows`, `window_curves` | `--window`, `--window-curves` | lists of texts, `["load=Sample Application:1:3 CV"]`, `["uv"]` (the page writes both when its window form has a window and a curve) |
| `bed_height`, `diameter`, `particle_diameter` | `--bed-height`, `--diameter`, `--particle-diameter` | `[10, "cm"]` |
| `override_geometry` | `--override-geometry` | `true` / `false` |
| `cadet`, `cadet_core`, `core_format` | `--cadet`, `--cadet-core`, `--core-format` | `true` / `false`, `"5"` / `"6"`, `"json"` / `"hdf5"` |
| `mixer_volume`, `tubing_volume` | `--mixer-volume`, `--tubing-volume` | `[0.6, "ml"]` |
| `sample_inlets` | `--sample-inlets` | `["A5"]` |
| `system_volumes` | `--no-system-volumes` | `true` (default) / `false` |
| `core_overrides`, `process_overrides` | `--core-overrides`, `--process-overrides` | the JSON object itself (not a file name) |
| `tables`, `curves`, `curve_roles` | `--tables`, `--curves`, `--curve-roles` | lists of texts |
| `curves_missing_ok` | (the page) | `true`: a curve name a run lacks is a warning, not an error |
| `max_points`, `method`, `every_n`, `derivative_share`, `grid_step`, `grid_axis` | `--max-points`, `--method`, `--every`, `--derivative-share`, `--grid-step`, `--grid-axis` | numbers; `method` and `grid_axis` text |
| `chromatograms` | `--chromatogram` | `["all"]`, `["imported", "Chrom.1"]` |

Quantities are `[value, "unit"]`. A missing key or `null` keeps the default. An unknown key, a
value of the wrong kind or a unit of the wrong quantity stops with exit code 2 and names the key.
Flags given on the command line replace the file's value of their option, so
`--options options.json --tables key` keeps everything in the file except `tables`. A flag can
set or change an option, not clear it: to turn off an option the file sets, edit the file.
`route` and `command` (how an export was asked for) are not options; they are recorded in the
provenance only.

### Phase window statistics (`--window`)

`--window` names a window relative to a phase: `NAME=PHASE:FROM:TO UNIT`.

- `PHASE`: the phase as the run log names it; case, repeated spaces and a leading `Phase ` are
  ignored (`Sample Application` matches `Phase Sample Application`). It may hold spaces and `:`.
- `FROM`, `TO`: a number counts from the phase start (`1`, `-0.5`; `start` is 0); `end`,
  `end-N`, `end+N` count from the phase end.
- `UNIT` sets the axis: a time unit (`s`, `min`, `h`), a volume unit (`ml`, `L`, `ul`) or `CV`.

```
--window "load=Sample Application:1:3 CV"   1 to 3 CV after the phase start
--window "tail=Elution:end-2:end CV"        the last 2 CV of the phase
--window "whole=Column Wash:0:end min"      the whole phase
```

`--window-curves` picks the curves: names as the run stores them and/or roles (`uv`,
`conductivity`, `ph`, `conc_b`, ...; a role takes every original curve that has it). A name or
role a run does not have, and a phase a run does not have, give a `phase_windows` line in
`warnings.csv` and no rows.

The table has one row per phase occurrence x window x curve: `run`, `chromatogram`, `window`,
`phase` (as logged), `occurrence` (1, 2, ... for a phase that runs several times),
`phase_number` (its row in the phases table), `axis`, `window_unit`, `start_anchor` /
`end_anchor` (`phase start` or `phase end`), `start_offset` / `end_offset` (as given),
`start_time` ... `end_cv` (the window on every axis, clipped to the phase), `time_unit`,
`volume_unit`, `curve`, `curve_unit`, `curve_precision`, then `n`, `mean`, `median`, `sd`
(sample SD, n - 1; empty for n < 2), `min`, `max` in the curve's unit, and `warnings` (JSON
list).

- **Interim phase rule** (until the lead-in and repeats decisions): phase start and end as
  logged (the phases table), no lead-in folding; a repeated phase gives one row per occurrence.
  `warnings.csv` repeats this whenever the table is written.
- A window reaching before the phase start or past its end (or the run end, for the last
  phase) is clipped to it, with a row warning. A window empty after clipping gives `n` = 0 and
  no statistics.
- Each curve is read on its own points and axes (curves are sampled on different grids); the
  bounds are inclusive and empty points are left out. A curve without the window's axis is read
  on another axis the window was resolved on, with a row warning.
- A CV window in a run without Vc gives a warning and no rows.
- Values are not rounded; Excel shows the statistics with the curve's precision.

The web page has the same table in its "Phase window statistics" section: a form that writes
these window texts (phase, from, to, unit, optional name; a window without a name gets `w1`,
`w2`, ...) and curve-role tick boxes. With a window and a curve set, its downloads (the shown
run, ticked runs, all runs) hold `<c>_phase_window_statistics` and its `options.json` records
`windows` and `window_curves`, so `pycorn export --options` writes the same table.

### Smaller files

All off unless you ask (no default size). The CADET files and the stored run are never changed.
Together they cut a full export (every curve, every point, every table) to a fraction of its size.

| Flag | Effect |
|------|--------|
| `--tables LIST` | Write only these groups of tables, as the web page groups them, e.g. `key,peaks` (comma list or repeated): `key` (phases, injections, fractions, pools), `peaks` (peaks, peak_summary), `method` (hint_blocks, hint_injections, variables, method_steps, scouting_runs, start_questions), `details` (run_details, column, calibration, instrument, system_settings), `audit` (audit_log), `runlog` (events, sections, pauses). A group keeps the curves table (which curves: `--curves`, `--curve-roles`) and the `--epsilon` / `--bypass-uv` / `--window` tables. Single tables work too, by the table keys of the column guide (`curves`, `events`, `peak_summary`, `phases`, `concentration`, `bypass_uv`, `phase_windows`, ...; `peaks` is the group); then only those. An unknown name stops with the lists. `run_info` (column and timeline), `warnings`, the column guide and `README.txt` are always written (all but `README.txt`, and the provenance, as sheets of the workbook for `xlsx` without `csv`) |
| `--curves NAMES`, `--curve-roles ROLES` | Curves table (and the Excel chart): only these curves, by name as the run stores them and/or by role (`uv`, `conductivity`, ...; the role works whatever the instrument names the curve). With both, the union. An unknown name or role stops with what exists |
| `--max-points N --method M` | Thin each curve on its own axis to about N points. `M`: `interval` (evenly spaced), `derivative` (more points where the curve changes), `minmax` (smallest and largest value per bucket) or `hybrid` (half derivative-spaced, half evenly spaced, plus min/max and step transitions; the web page's rule, up to about 1.35 N points). Both sides of every step transition stay. Curves with fewer points stay whole. There is no default method |
| `--every N` | Instead of `--max-points`: keep every Nth stored point of each curve, the last point and both sides of every step transition |
| `--derivative-share 0-1` | With `interval` or `derivative`: share of points placed where the curve changes (`1` = all, `0.5` = half, half evenly spaced) |
| `--grid-step STEP [--grid-axis time\|volume\|cv]` | Instead of thinning: every curve on one evenly spaced grid (default axis time; step in that axis' unit). Each curve's other axes (volume, CV, UTC, adjusted) are interpolated onto it. Linear between stored points, empty outside a curve's own range, pauses bridged |

`--max-points`, `--every` and `--grid-step` exclude each other. The web page writes the curves
ticked on its plot (since 0.32.0: the chips decide, at full resolution; no role box; a curve a
run does not have is left out of that run's curves table with a `curves` line in `warnings.csv`),
and offers the same size choices in one "Size" menu (all data, one grid, every Nth point,
hybrid) with a number field (filled in when a size is chosen: grid step 0.05 in the run's time unit, every 10th point, 2000 points per curve). Thinned values are stored values (nothing
is smoothed), and points with an empty value are dropped. Every thinned or gridded export adds a
`sampling` line to `warnings.csv`: the largest difference to the stored curves in peak height,
area and retention (x shift), and which curve it is. The mg/ml curves of `--epsilon` are not
affected by `--curves` / `--curve-roles`.

The UV wavelength of each curve comes from the run log's Wavelength instruction. The curve
name's suffix is used only as a fallback, with a warning.

Concentration from UV is an estimate (GitHub issue #1). Every mg/ml curve and every
`_by_pass_uv` table adds a note to `warnings.csv`: the coefficient against this detector's
response, saturation and non-linear absorbance at high signal, baseline drift and the path
length all add error, also with a measured coefficient or a response factor.

`pycorn bypass-uv <files> | --from <CSV> [--sample-conc ...] [--loop-volume ...]
[--absorbance-limit ...] [--csv out.csv]` prints the same by-pass readings without exporting;
`--csv -`, `--json` or `--jsonl` put them on stdout for a program (masked unless `--no-mask`;
see "Output for programs" above).

## pycorn windows

`pycorn windows <files> | --from <CSV> --window "NAME=PHASE:FROM:TO UNIT" --window-curves NAMES
[--chromatogram ...] [--csv out.csv | --csv - | --json | --jsonl] [--no-mask] [--strict]` prints
the curve statistics of `export --window` without exporting: the same rows and columns as
`<chromatogram>_phase_window_statistics` (window syntax and rules there), with `source` and
`result_id` first. `--window` and `--window-curves` are both required (no default window or
curve); either missing, or a window that does not parse, stops with exit 2. `--chromatogram`
as for `export` (default: the run's original chromatogram). Without `--csv` / `--json` a table
on screen, each window's notes (clipping) under it, warnings (a phase not in the run, a curve
not found) and the interim phase rule as a note. `--csv FILE` writes the rows as stored
(`warnings` as JSON text, as the export writes them); `--csv -`, `--json`, `--jsonl` mask
people only (logins, computers, instruments, serial numbers; run, chromatogram, window, phase
and curve names shown) unless `--no-mask`.

Exit 1 when a file was not read, a run not loaded, a chromatogram not read, a `--window-curves`
name matched no curve, a window gave no row (phase not in the run, CV without Vc), a row has
no point inside its window (`n` = 0), or `--chromatogram` picked nothing in a run. A clipped
window is a warning only.

```
pycorn windows --from runs.csv --window "eq=Equilibration:end-2:end CV" --window-curves conductivity,ph --csv eq.csv
```

## Examples

Export everything from one project in the first half of 2024 to Excel, with volumes in litres:

```
pycorn find --folder "/DefaultHome/Projects/ProjectX/**" --run-start 2024-01-01..2024-06-30 --all --csv runs.csv
pycorn export --from runs.csv --out exports/ --format xlsx --units volume=L
```

Protein concentration from UV 280, for files received from another lab:

```
pycorn export incoming/ --out exports/ --epsilon 280=1.0
```

Conductivity and pH over the last 2 CV of every equilibration, and UV during the load:

```
pycorn export --from runs.csv --out stats/ --window "eq=Equilibration:end-2:end CV" --window "load=Sample Application:0:end CV" --window-curves conductivity,ph,uv
```

A CADET-Core 6 input for each run, with the column geometry given:

```
pycorn export --from runs.csv --out cadet/ --cadet-core 6 --bed-height "10 cm" --diameter "1 cm" --tubing-volume "0.2 ml"
```

## Troubleshooting

- **Exit 2, "error: ..."**: a flag value, the `.env` file or the connection. Nothing was
  loaded. `pycorn-setup --test-only` (or `pycorn sources --test`) checks the connection of
  every source; "no database server is set up"
  means no settings were found: run `pycorn-setup`.
- **"cannot reach the SQL Server" / "the server refused the login" / "the login cannot open
  the database"**: server name or port, network (VPN); user name or password; database name
  or access. [`sql/CONNECTOR_GUIDE.md`](../sql/CONNECTOR_GUIDE.md), Troubleshooting, has the checks.
- **"... not read: not a zip file, or the file is damaged" / "not a UNICORN result"**: export
  the result from UNICORN again as `.zip` (a copy cut short, or another kind of zip).
- **Exit 3, "unexpected error"**: run the same command with `--debug` and report the output
  (and the file, if you can share it) at https://github.com/traylosj/PyCORN/issues.
- **`not loaded` in index.csv**: the run could not be read. The console warning says why.
- **`incomplete` in index.csv**: an output asked for was not written. A CADET file: usually
  the column geometry: give `--bed-height` and `--diameter`. A chromatogram with no run log (one derived in Evaluation without one) has no sections to
  build from, so it gets no CADET files. "mg/ml at
  N nm": the run has no UV curve at that wavelength; `warnings.csv` lists the UV channels it has.
- **A folder export skips `.asc` / `.res` files**: they are not result files; a warning says
  how many. Export the result as `.zip` (a `.res` reads with `pycorn.PcRes3`).
- **`skipped (changed since export)`**: run the same command with `--update`. A database
  run is compared using the search CSV's `tstamp`, so run `find` again first to pick up
  changes made since that search.
- **Mixed-type warning reading `<c>.csv` in pandas**: the UTC columns are empty where a curve
  is shorter than the longest one. Use `pd.read_csv(..., low_memory=False)` or
  `parse_dates` on the `: UTC` columns.
