# What changed in PyCORN

Every release, newest first. The version is in `pycorn/version.py`; each release is tagged
`v<version>` in git. Every export records the version and git commit that wrote it
(`README.txt`, `provenance.json`, the Excel Summary sheet), so you can find here what
changed since your files were made.

## Status (2026-11-12)

- **In progress:** nothing.
- **Planned:** for scripts and agents: the page's analysis logic as a public module. After
  decisions: how phases, steps and feed are read from a run (lead-in, repeats, held-over
  values); phase-aligned overlay; UV baseline and breakthrough. On hold: an MCP server, a
  CADET form on the page. Details in [`docs/ROADMAP.md`](docs/ROADMAP.md).

## 0.37.0 (2026-11-12)

### New: imported chromatograms' run log; PyCORN from scripts and agents, next items

Changed outputs first:

- **The page's `index.csv`** (Download all) now has the columns of `pycorn export`'s
  `index.csv`: `export_status`, `export_folder`, `export_files` (the file names, was a
  count), `export_error`, `export_changed`, `export_stamp`, then the file catalog's columns.
  Renamed: `run` -> `name`, `file` -> `path`, `error` -> `export_error`; `member` now
  comes with the catalog's columns. Status `failed` for a run that was not read is now `not loaded`
  (`written` / `incomplete` / `failed` / `not loaded`, as the CLI). With "mask" ticked,
  the catalog's people columns are masked as the run files are.
- **New column `export_error`** in `pycorn export`'s `index.csv`, after `export_files`: why
  a run was not loaded, failed or is incomplete (`not written: ...`); empty when fine.
- **`--mask-identifying` now masks `index.csv` too:** `system` and `created_by` become
  `System1` / `User1`, and those names inside `match_key` and `export_error`.
- **`pycorn export`** writes a new `options.json` next to `index.csv` (the options it used;
  feed it back with `--options`). The page's Download all writes one as well.
- **Imported chromatograms** (selected with `--chromatogram` or on the page): phase,
  injection and fraction tables, run log sections, phase window statistics, CV, UTC and
  CADET now come from the run log the chromatogram carries (the source result's), where
  they were empty or not written. This result's run-level data (Vc, run start, system
  configuration, method hints, column type, mixer and delay volumes) are used with a
  warning each. Curves of another method run inside an imported chromatogram are left out
  of role lookups (plots by role, window statistics, CADET signals) with one warning; they
  stay in the curves table. The original chromatogram's outputs are unchanged.
- **`pycorn-setup`:** before it changes an existing settings file it keeps a dated copy,
  `<name>.<YYYY-MM-DD_HHMMSS>.bak`, next to it (never removed; a warning past 5, since
  each holds the password). A new file no longer gets empty lines for optional settings.

New:

- **`pycorn inspect <files | --from csv> [--json | --jsonl] [--no-mask]`:** what each run
  holds without exporting: source, run start, system, column, method variables, every
  chromatogram (kind, Vc and its source, curves with role, unit and points, phase /
  injection / fraction positions) and warnings; never curve values, IP addresses or
  ports. Masked by default: people, run / folder / file / method names, names a user set
  on chromatograms and curves, and text variable values. Python: `pycorn.run_summary()`.
- **`--json` / `--jsonl` on `find`, `export`, `bypass-uv`, `inspect` and `windows`**, and
  `--csv -` on `find`, `bypass-uv` and `windows`: one JSON document (`"schema": 1`) or
  JSON lines (header, rows, summary) on stdout, human lines on stderr. People are masked
  by default in these modes (stdout and stderr; `--no-mask` to opt out); a database is
  named by its label only. Errors come as `{"error": {"text", "key", "exit"}}`; the keys
  are listed in `pycorn schema`. Contracts per command:
  [`docs/AGENT_GUIDE.md`](docs/AGENT_GUIDE.md).
- **Batch export from Python:** `pycorn.export_selection(catalog, out, formats, options,
  ...)`, the loop of `pycorn export`. `ExportOptions.to_dict()` / `from_dict()` and
  `pycorn export --options FILE.json` (flags on the command line win over the file).
- **`pycorn windows`:** the per-phase curve statistics of `export --window` without an
  export (`--window`, `--window-curves`, `--chromatogram`, `--csv`, `--json`, `--jsonl`).
  Exits 1 when a run was not read, a curve matched nothing or a window gave no rows.
- **`pycorn-setup --from-env [--overwrite]`:** writes the settings file from the
  `UNICORN_*` environment variables with no questions (for deployments); exit 2 and
  nothing written when server, user or password is empty or the file exists without
  `--overwrite`.
- **Web page, phase window statistics:** a folded form (pick a phase, from / to, unit;
  curves by role or name, none at first) and one table over the ticked runs; downloads and
  `options.json` carry the windows.
- **Web page, run list:** click ticks one run and shows it, Ctrl-click (Cmd on a Mac) adds
  or removes one, Shift-click ticks a range; keyboard too. The plot follows the ticks (2 or
  more: overlaid); the tables show the last run clicked. The download panel starts at
  "Ticked runs" when 2 or more are ticked.

## 0.36.0 (2026-11-11)

### New: every chromatogram of a result selectable; PyCORN from scripts and agents

Changed outputs first:

- **Only the run's own chromatogram by default.** A result can hold other chromatograms,
  imported from other results or made in Evaluation. Exports (`pycorn export`, Excel, CSV,
  `export_cadet()`) and the web page now write and show only the original one; before, every
  chromatogram was written. `warnings.csv` names the ones left out. Pick others with
  `--chromatogram NAME|all|original|imported|derived` (repeatable; also on `bypass-uv`),
  `ExportOptions(chromatograms=)`, `export_cadet(chromatogram=)` or the page's picker.
- **`column_and_timeline.csv`:** a row per chromatogram written and four new columns after
  `chromatogram`: `chromatogram_kind`, `source_result_id`, `source_result_name`,
  `copy_of_run`. The Excel and page summaries get a "Chromatogram kind" row.
- **CADET:** the warning reads "N chromatograms selected (...)" instead of "the run has N
  chromatograms".
- **`from pycorn import *`** gives only the supported names (`pycorn.__all__`); standard
  library and helper names that leaked in before (`os`, `struct`, `ZipFile`, ...) are gone,
  also as `pycorn.os`-style attributes.
- **`pycorn-setup --test-only`** (with no `--file`) tests the settings `pycorn find` really
  reads (every `.env` place, `UNICORN_*` variables, every source alias), one `[OK]` /
  `[FAIL]` line per alias; a settings file under your home folder is shown as `~\...`.
- **Inlets:** the warning for an assumed `Buffer` sample inlet adds that the stretch still
  counts as feed.

New:

- **Chromatogram kinds:** `Chromatogram.kind` (original / imported / derived),
  `Chromatogram.source_result`, `run.describe_chromatograms()`,
  `run.select_chromatograms()`. Curve role lookups use every curve on an imported or
  derived chromatogram.
- **Web page:** a folded chromatogram picker in the run list for results with several
  chromatograms; the view, downloads and the overlay follow it, so two chromatograms of one
  run can be overlaid ("run - chromatogram" in the legend).
- **`pycorn --version`**; `pycorn.__version__`.
- **`pycorn schema [--json]`:** every table and column (origin, meaning), the download
  groups, curve roles, units and exit codes; JSON is one document with `"schema": 1`.
- **`pycorn sources [--test] [--json]`:** the source aliases, which settings are set or
  empty (never a value), and with `--test` whether each connects.
- **[`docs/AGENT_GUIDE.md`](docs/AGENT_GUIDE.md):** using PyCORN from scripts and agents.

## 0.35.0 (2026-11-11)

### New: inlets from the system's configuration, per-phase curve statistics, overlaid runs

Changed outputs first:

- **Inlets** (phases, injections, fractions, run log sections, the page's inlets track):
  a line or valve the run log never set is now read from the system configuration the
  result stores, not from fixed defaults.
  - A line with no valve shows the line alone: `B`, was `B1 (assumed)`.
  - A valve with no position logged yet shows its first position as assumed (`B1 (assumed)`;
    the sample inlet valve `Buffer (assumed)`), as before.
  - When the configuration does not say: `A (not known)`, `B (not known)`,
    `Sample inlet (not known)`, with a warning (were `A1 (assumed)`, `B1 (assumed)`,
    `Buffer (assumed)`).
  - A direct inject on a system with no sample inlet valve: `Sample pump inlet`.
  - A logged quaternary gradient adds its shares (`Q1 50%+Q2 50%`); that they feed the A
    and B pumps is provisional, with a warning.
  - New column `inlets_source` (known / assumed / not known) and new `inlets:` warnings.
- **CADET:** `experiment.json` hints gain `inlet_labels` and `inlet_sources`, `system`
  gains `inlet_configuration`. `buffers=` keyed by a line warns where the position is
  assumed or not known; existing keys work unchanged.

New:

- **Per-phase curve statistics** (`pycorn export --window "NAME=PHASE:FROM:TO UNIT"`,
  repeatable, with `--window-curves`): n, mean, median, SD, min and max of the curves you
  name (or pick by role) over windows relative to a phase start or end, in time, volume or
  CV (`load=Sample Application:1:3 CV`, `tail=Elution:end-2:end CV`). One table,
  `<chromatogram>_phase_window_statistics.csv` and an Excel sheet, a row per run, phase
  occurrence, window and curve. No default window or curve. For now the phase starts and
  ends as logged and a repeated phase gives a row per occurrence (interim, stated in the
  table). Python: `pycorn.window_stats`.
- **`run.system_configuration()`:** which inlet valves, sample inlet valve, sample pump and
  quaternary valve the system has, read from the result (installed / not installed / not
  known), by installed hardware, never by system model.
- **Web page, overlaid runs:** tick runs and pick "Ticked runs" on the plot bar to overlay
  up to 10 runs; "Curves as panels" (one panel per curve role, a colour per run; the
  default) or "Runs as panels"; the x axis picker as before. The page remembers the layout.
- **Web page:** the Injections table starts folded.

## 0.34.0 (2026-11-11)


### Changed: method information closer to UNICORN's report; choose what a download holds

Changed outputs first:

- **Method steps:** the breakpoint variable is in `line` on the instruction that stores it,
  not on `End_Block`; a Watch's action block follows its Watch line, one level deeper,
  instead of at the end (so `step`, depth, path and phase change for those rows);
  `Vc=(x)#var` reads `(Vc=x)#var`. New column `called_from`.
- **Method variables:** in method order (a breakpoint variable before its instruction's
  parameters).
- **Run and method:** the notes are `Method notes`, `Start notes`, `Run notes`,
  `Evaluation notes` (were `MethodNote`, ...); the raw `Question/...` rows moved to the
  new Start questions table. New rows: `result name variable`, `evaluation procedure`.
- **Peak table summary:** `Baseline type` and `Resolution algorithm id` (were
  `BaselineType`, `ResolutionAlgorithmID`); new rows `Resolution algorithm name`, `UV
  normalized to 1 cm path length` (from the evaluation log; a warning when it cannot be
  told), and `Baseline name` for a zero baseline.
- **Scouting runs:** new first column `run` (from 1); variable columns in method order.
- **`--tables`:** takes the web page's groups (`key`, `peaks`, `method`, `details`,
  `audit`, `runlog`); a group keeps the curves table. `peaks` is now the group (peaks and
  peak table summary). Column and timeline is always written, as warnings and the column
  guide.

New:

- **Start questions** table (`start_questions.csv`): number, question, answer type, answer.
- **Pooled fractions** table (`<chromatogram>_pooled_fractions.csv`) and a `pool` column
  on Fractions.
- `--mask-identifying` masks the notes and free-text answers as a whole.
- **Web page:** Pooled fractions under Fractions (only when the run has pools); Start
  questions after Scouting runs.
- **Web page Download panel:** names the curves that go in, with "Plot chips" (the ticked
  curves) or "All curves"; a tick box per table group, its tables named under it, all
  ticked at first. The choices are kept in the browser.

Fixed:

- Web page: downloading ticked or all runs no longer failed when no curve list was sent.

## 0.33.0 (2026-11-10)

### New: tables you can sort and filter, stacked plot, key information first

The web page only; exported files are unchanged.

- **Tables:** sort by a header click (numbers as numbers, on the full value), a filter box
  per table, select cells or rows by click, shift-click or drag and copy them (Ctrl+C or
  the Copy icon) as tab-separated text that pastes into Excel. Copy and CSV give what you
  see: sort and filter applied, full values. Sort and filter cover every row; the first
  500 are shown, with "Show all" as before.
- **Frozen columns:** the header stays on top and each table keeps its first columns at
  the left while you scroll sideways (Phases up to `name`, Method steps up to `block`,
  ...). In a narrow window, where they would take more than 40 % of the width, nothing is
  frozen. Copy and CSV are icons on the table bar; a column's description is behind the
  small "?" in its header.
- **Stacked plot:** an Overlay / Stacked switch on the plot bar. Stacked draws one panel
  per unit on a shared x axis. The page remembers the choice.
- **Key information first:** run name, summary, plot, Phases, Injections and Fractions are
  always shown; the other tables are in "More details" (peaks, method, run details, audit
  log and warnings), closed by default, with the run's warning count in its title. Runs
  and the run view are separate cards.

## 0.32.0 (2026-11-10)

### Changed: one Excel layout, one download place, downloads follow the plot

Renamed or removed outputs first:

- **Excel exports are the workbook plus `README.txt`.** `pycorn export --format xlsx` no
  longer writes the run-level CSVs (`method_variables.csv`, `column_and_timeline.csv`,
  `warnings.csv`, `column_guide.csv`, the method and calibration tables) or
  `provenance.json`; those tables and the provenance are sheets of the workbook. Add `csv`
  to `--format` to get them back. Exports that write only pickle or CADET files keep the
  CSVs as before.
- **Web page, Excel:** a single run's download is now a small zip, `<run name>_xlsx.zip`
  (workbook and `README.txt`), not a bare `.xlsx`. Excel downloads of several runs give
  the same folder per run, plus `index.csv`, in `PyCORN_export_xlsx.zip`.
- **Web page, curves:** a download holds the curves ticked on the plot, at full resolution.
  The "UV, conductivity, pH, pressure and %B/%Q only" box is gone. The command line is
  unchanged: no default curves (D28).
- `--update` and the "changed since export" check read the stamp from the workbook in
  workbook-only folders, and still read older folders.

New:

- **One Download button** on the web page opens a panel with the format (Excel, CSV,
  CADET-Core), which runs (the shown run, ticked runs, all) and the mask and size settings.
  The panel remembers these settings in the browser (`localStorage`; settings only, never
  file data). PNG and SVG are icons on the plot bar.
- **Tick boxes per run** in the run list, with "tick all shown", which follows the filter.
- **Drop zone:** large when the page opens, a one-line "Add files" bar once runs are listed;
  Clear brings it back.
- **Injection marks** on the page's plot are full-height thin lines, like pauses.
- **Spanish and Portuguese (Portugal)** for the page, machine-translated.
- **Status and error messages translated** on the page (sizes, Excel row limit, CADET-Core
  version, files not read, format names, loading).
- **CADET export marked experimental:** a note in the CADET guide, the README and the page's
  CADET-Core panel, and a line in `warnings.csv` and the command line output whenever CADET
  files are written.

## 0.31.1 (2026-11-10)

### Changed: Method steps and default columns read in order

- **Method steps** on the web page shows `phase`, `block` and `line` by default. It used to
  show `line` and `run_values`, which repeated the values already in the line and made the
  table twice as wide. The indent is now padding, so it shows clearly and a long line that
  wraps keeps its indent. Phase and block stay on one line. A `Phase:` line now carries the
  phase it opens.
- **Column order** in `method_steps.csv` and the sheet: `step`, `phase`, `block`, `line`, then
  `depth`, `path`, `base`, the breakpoint columns, `instruction`, `parameters`, `run_values`,
  `main_block`, `unused`.
- **Peaks** columns follow UNICORN's peak table: No, retention, area, % of total peak area,
  height, fraction tubes, each unit right after its value, then the other stored columns.
- **Default columns on the web page** now appear in the order they are listed. Before, they
  followed the table's column order, which split time from length in Phases, for example.
  A column you tick in the Columns menu goes next to the shown column before it in the table.

## 0.31.0 (2026-11-10)

### Changed: method information laid out as UNICORN shows it

The method, its variables, the column and the calibrations now read like the Method and
Calibration sections of UNICORN's own report. This applies to the csv files, the Excel sheets
and the web page.

Renamed or reordered outputs first:

- **Calibration** (`calibration.csv`): one row per item, the reference values first, then the
  constants, as UNICORN lists them. The columns `reference_value`, `reference_unit`,
  `constant_value` and `constant_description` are replaced by `kind` (`reference` or
  `constant`), `item` (`Reference value 1`, or the constant's description, e.g. `Calibrated
  electrode slope`), `value` and `unit`. Before, a constant shared a row with the reference
  value at the same position, so the electrode slope looked as if it belonged to the pH 7
  reference.
- **Method variables** (`method_variables.csv`): listed as UNICORN's variable list, by `phase`
  and `block` in method order (it was alphabetical). `unit` is now the unit UNICORN shows:
  `{base}` becomes the block's base, `CV`, `ml` or `min`. The stored text is in the new
  `stored_unit`. Column order: `phase`, `block`, `variable`, `value`, `unit`, `number`, `kind`,
  `scouting`, `stored_unit`.
- **Method steps** (`method_steps.csv`): rows in the order UNICORN prints the method, from the
  main block with each called block's lines right after its call. It used to list blocks as
  stored, with sub-blocks before their phase and the main block last. New columns: `step`,
  `depth`, `line` (UNICORN's method text, indented: `0.00 System flow: (5.000)#Flow rate
  {ml/min}, (Pre column pressure)#Pressure control`) and `path`. `phase` is now filled for
  sub-blocks too, from the phase that calls them. Parameters UNICORN stores as hidden and
  never shows (`System flow linear flow selected: 0`) are left out. On the page, the table
  shows the indented method text.
- **Dates** in Run and method and on the Summary sheet carry their UTC offset, as UNICORN
  prints them: `2024-03-05T14:02:33.000 -05:00`.

New:

- **Column** (`column.csv`, a sheet, a page section): the column type in the report's three
  groups (Run parameters, Details, Ordering information), then every other stored field.
  The groups hold pressure limits, default and max flow, pH ranges, typical loading, typical
  peak width, particle size, and the resin and hardware code numbers. Column volume and the
  linear flow rates are calculated from diameter and bed height, and the `source` column
  gives the formula.
- **Peak table summary** (`<chromatogram>_peak_table_summary.csv`, a sheet, a page section):
  each peak table's header, as the report prints it under the peaks. It lists the totals,
  peak width, "Calculated from", the baseline curve by name, the column values, the
  resolution algorithm and the injection the retentions count from.
- **Peaks** get `number` (UNICORN's "No"), `retention_unit`, `area_unit`, `height_unit`,
  `retention_zero_injection` and `max_peak_on_run_axis`. UNICORN counts retentions from the
  injection, so a peak's retention is not where the curves put it on the run's volume
  axis when the injection came after method start. The new column gives that position. The page now shows the peaks too.
- **Run and method** adds the result folder and the naming rule (`Method name, without
  unique identifier`).

### Fixed

- A `Hold until` instruction lost its condition in Method steps: `None: 5.00 base` instead
  of `UV 1, Stable signal, 0.03 min, 5.00 base`. UNICORN names condition parameters
  differently from other parameters, and they are now read.

## 0.30.1 (2026-11-10)

### Changed: the web page on a phone

On a small screen (600 px wide or less) the chart gets most of the space:

- At most two y axes. With a UV curve on the left, one unit on the right keeps its own axis;
  two or more share the "% of each curve's range" axis. Without a left curve, two units keep
  their own axes. The cursor label still shows every curve's own value.
- No legend and no title inside the chart: the curve buttons above it are the legend, and the
  run's name is the heading above.
- Smaller buttons, curve buttons, drop-downs and text.

Turning the phone or widening the window lays the chart out again.

### Fixed

- The cursor label could cover the whole chart on a touch screen with no way to close it. A
  tap outside the chart, a second tap on the chart, or Esc now closes it.
- The track labels (phases, inlets, fractions) were cut to one letter in bars that had room
  for the whole name. They are now measured, and cut only when the name does not fit.
- The x axis names (Clock time, Method time, Volume, Column volumes) and the "Choose files"
  buttons were always in English or the browser's language. They now follow the page's
  language.

## 0.30.0 (2026-11-10)

### Changed: numbers shown with the decimals UNICORN specifies

The web page and the Excel workbooks used to show long tails of decimals on time, volume and
CV, such as `5.010654465884225` or `3.0149999999999997`. They now show the decimals UNICORN
specifies:

- Curve values use the curve's own precision (`AmplitudePrecision`): UV 3, conductivity 2,
  %B 1, ...
- Time, volume, CV, flow and %B use the decimals UNICORN prints in its run log: min 2, ml 3,
  CV 2, ml/min 3, % 1. UNICORN stores no precision for these.

Unit conversions carry the decimals. Volumes in L get 6 (the same significant figures as
3 in ml), seconds get 1, AU gets 3 more than mAU, and mg/ml curves get theirs from the UV
curve.

Only the display rounds. Excel cells and the exported csv files keep the full values, so
nothing is lost for calculations. The page's Copy button copies the text as shown. The
chart's hover label uses the same decimals.

## 0.29.1 (2026-11-10)

### Changed: the `inlets` column shows the inlets in use; `inlets_in_use` removed

The `inlets` column of the phase, injection, fraction and process-sections tables now holds
the inlets the run draws from: line A below 100 %B, line B above 0 %B, `A1+B1` at a constant
mix, `ramp A1 to B1` in a %B ramp, `Loop` while the injection valve is at Inject, the sample
inlet at Direct inject. This is the same text as the page's inlets track. A position the run
log never set is taken as UNICORN's default on standard lab systems (A1, B1; sample inlet
Buffer) and marked `(assumed)`; not yet checked on process skids.

The separate `inlets_in_use` column added in 0.27.0 is removed. The positions exactly as the
run log set them are no longer in the tables or exports; the CADET export still reads them
internally.

### Fixed: track labels on the web page's chart

With many curves on, the extra right-hand axes narrowed the plot and the phase, inlet and
fraction labels disappeared. Each box now shows its name when it fits, the name cut short when
it nearly fits, and at least its first letter however narrow it is; zooming refits them.

### Changed: the page's loading text

While the page loads, the status line says "Downloading Python and its packages" (with
Pyodide's own package lines), then "Packages loaded. Starting Python and PyCORN...", then
"Ready". Five languages.

### Docs

- The first visit downloads about 15 MB (measured with an empty cache; the docs said 20 MB).
  The browser normally keeps these files, so later visits download almost nothing, also after
  restarting the browser; a private window or cleared browser data downloads them again.
- New front page screenshot; getting-started guide, README, run-structure guide and privacy
  page describe the chart's tracks, x axis, curve chips, languages and Cancel / Stop now.
- Project summary and `AGENTS.md` key-file lists brought up to date (modules, docs, `web/`).

## 0.29.0 (2026-11-09)

### Changed: the web page stays usable while Python works

Python now runs in a web worker (a background part of the same tab): while files are read or
downloads are written, the chart, tables and pickers keep working. The status line says how
far it is ("Reading file 3 of 12", "Writing run 3 of 35... (reading the run)"). **Cancel**
stops before the next file or run; if one step takes too long, **Stop now** restarts Python
and reads the files in the list again. In a browser that can't start a worker, Python runs on
the page as before (the footer says so).

### Fixed: chart curves and the cursor label

Curves switched on no longer share one right axis (a 0.007 MPa pressure next to a 700 CV/h
flow drew as a flat line): the first three units on the right get their own axis, a fourth
or more share one axis in % of each curve's range (the cursor label keeps the values). The
cursor label stays at the mouse's height inside the curves (it jumped over the tracks when no
UV curve was on). Redrawing the chart is about ten times faster.

### Changed: tables and panels

Tables have a rounded frame, a shaded header, lines between rows only and lightly shaded
alternate rows; wide tables scroll inside their section instead of running to its edge.
Every panel has the same rounded corners.

## 0.28.0 (2026-11-09)

### Changed: web page pickers, fields and sections

- Curves: one chip per curve in its chart color above the chart, filled when drawn; click
  to switch it (the legend click still does too). All / None next to them.
- Columns: a "Columns (n of m)" button above a table opens a menu of its columns, with a
  filter box for long lists and Defaults / All. Esc or a click outside closes it.
- Drop-downs, number and search fields, checkboxes and the file button restyled: rounded,
  the page's colors, light and dark mode.
- Expandable sections (tables, CADET-Core) are rounded cards with a turning chevron.

## 0.27.0 (2026-11-09)

### Changed: every curve in the chart's curve selector

"Curves (n of m)" listed only the curves the Excel chart draws (UV, then conductivity or
%B). It now lists every curve that changes during the run (pH, pressure, %B, temperature,
flow, ...). The Excel chart's curves are on at first, the others off; a curve you switch on
goes on the right axis, whose title lists the units shown. The page's run data grows with
the number of curves (a few MB for a run with about 15).

### Changed: web page buttons

Rounded, outlined buttons with a hover color; the main download (Excel) and "Try a sample"
filled. Light and dark mode both.

### Changed: the inlets track shows the inlets in use, with assumed positions marked

The track showed only the positions the run log set, so a method that sets only line A drew
one bar ("A1") across injection and a 100 %B elution. Each bar now names the inlets the run
draws from: line A below 100 %B, line B above 0 %B (`A1 -> B1` in a ramp), `Loop` while the
injection valve is at Inject, the sample inlet at Direct inject. A line drawn across each bar
shows %B (bottom 0, top 100): a ramp slopes, a constant 50 %B sits halfway.

**Assumption:** a position the run log never set is taken as UNICORN's default on standard
lab systems (A1, B1; sample inlet Buffer at Direct inject) and labelled `(assumed)`, e.g.
`B1 (assumed)`. Not yet checked on process skids.

### Added: `inlets_in_use` column

In the phase, injection and fraction tables and the process-sections table, next to
`inlets` (unchanged: the positions as the run log set them). Same rule and `(assumed)` mark as
the track; a ramp reads `ramp A1 to B1` (as `percent_b` reads `ramp 0 to 100`; on the track
`A1 -> B1`, from to to), a constant %B between 0 and 100 `A1+B1`, 0 or 100 %B only the line
in use (`A1`, `B1`).

### Changed: web page size box filled in

Choosing a size fills the number in (grid step 0.05 in the run's time unit, every 10th
point, 2000 points per curve for hybrid), so a download no longer stops with "Give a number".

## 0.26.0 (2026-11-09)

### Added: an inlets track on the web page's chart

One lane under the phases: a bar for each stretch of the run where no inlet position changed,
labelled as the run log set them (`A2 B3 S3`: line A, line B, sample inlet), on every x axis.
It shows what was set, not which inlet is flowing at a given %B; nothing is drawn before the
first inlet is set. Built from the executed run log's sections, as the process-sections table.

### Added: web page usability

Chromatogram: a "Curves (n of m)" selector (also the legend click) switches curves on and
off without losing the zoom, curves keep their colors, a single-curve y axis takes its
curve's color, and "Image (PNG)" / "Image (SVG)" save the plot under the run's name.
Run list: sortable columns, a filter box for long lists, rows reachable and openable by
keyboard. Tables: drawn when their section opens, first 500 rows until "Show all", per-table
Copy and CSV. Files dropped anywhere on the page are added (a miss no longer opens the file in
the browser). "Try a sample" on GitHub Pages. Status line stays in view and is announced by
screen readers; dark mode follows the system (the plot too; saved images are always light and keep
your zoom); tapping or pressing Enter on a column or table name shows its hint as text (hover
tooltips do not exist on touch); a Content-Security-Policy limits the page to its own script,
the two CDNs and its own address, so the browser itself refuses any other request;
buttons are disabled while the page works; load progress and a reload button
on failure; larger touch targets; integrity hashes on the Pyodide and Plotly scripts.

## 0.25.0 (2026-11-09)

### Added: smaller exports (`pycorn export`, `ExportOptions`)

Choose what is written and how much: `--tables` (only these tables), `--curves` /
`--curve-roles` (only these curves), `--max-points` with `--method interval|derivative|minmax`
and `--derivative-share` (each curve thinned on its own axis, step transitions kept), or
`--grid-step` with `--grid-axis` (every curve and its axes on one evenly spaced grid, pauses
bridged), or `--every N` (every Nth point); `--method hybrid` is the page's rule. The web page
has a "Size" menu for Excel and CSV downloads (all data, one grid, every Nth point, hybrid) and a box for UV, conductivity, pH, pressure and %B/%Q curves only. All off by default; the CADET files are unchanged. A `sampling` line in `warnings.csv`
gives the largest difference in peak height, area and retention. The Excel chart follows the
thinned or reduced curves. See `docs/EXPORT_GUIDE.md`, "Smaller files".

### Changed (renamed outputs): no adjusted time and volume columns in the curves table

`<curve>: time adjusted` and `<curve>: volume adjusted` are gone from `<chromatogram>.csv` and
the Excel `curves` sheet (two of seven columns per curve). They were stored time and volume
minus the retention zero; the zero is now in the column and timeline table
(`retention_zero_adjusted`, `_injection`, `_time`, `_volume`, `_source`), so
`adjusted = time - retention_zero_time` (volume likewise). The events and phase tables keep
their adjusted columns.

### Changed: the web page's plot sampling

The plot keeps about 4000 points per curve: half placed where the curve changes, half evenly
spaced, plus the smallest and largest value per bucket and both sides of every step transition
(was: min/max per bucket only).

### Added: the web page in German, French, Italian and Swedish

The page's fixed texts are in `web/i18n/<language>.json` (`en.json` is the reference); the page
starts in the language you picked last, else your browser's, else English, and a picker in the
header switches it. The translations are by machine. Table and column names, warnings, program
error messages and the downloaded files stay in English; the page says so in the other languages.

## 0.24.0 (2026-11-09)

### Added: phase, fraction and pause tracks and an x-axis picker on the web page

Above the page's chart, phases and fractions are drawn as bars from start to end, and pauses
on a row of their own, as the figures in `docs/RUN_STRUCTURE_REVIEW.md`. The cursor label
names the phase, fraction and pause at every point. Phase bars drop UNICORN's `Phase ` prefix
(`Elution`); the tables keep the logged name. The x axis can be clock time (the instrument's
local time, pauses included), method time, volume or CV; volume is the default and the
browser remembers the pick. On clock time a pause has its real length; on the other axes it
is a mark. Fraction starts are now a track instead of diamond marks. The Excel chart is
unchanged.

### Added: two samples for the run structure review

`samples/u711_manual_run.zip` (a manual run: `Inlet B` logged, sample pump feed, flow changes,
pauses, no Vc) and `samples/u711_proa_day12.zip` (the run behind `u73_blank.zip`, Vc stored,
steps only). Sanitized with `tools/anonymize_zip.py`. `samples/SAMPLE_ZIP_CONTENTS.md` lists
what each sample covers and what is not covered yet (a ramp with a stored Vc, a hold, a
second slope, a direct inject).

### Changed: local-file tests skip deliberate edge runs

`test_local_runs_build` skips runs with no stored Vc and `test_local_superloop_injection_as_planned`
skips runs with no method (manual runs), instead of failing on them.

### Changed: CADET tubing warning wording

The CADET export's warning when the tubing volume is unknown now starts "column-to-UV tubing
volume: the instrument doesn't store it, ..." (was "no column -> UV tubing volume: ...").
Scripts that match the old text need the new one. `docs/CADET_GUIDE.md` adds a note: the
mixer and tubing are stirred tanks with no dispersion, and a plug-flow tube put in their place
should not get zero axial dispersion (CADET forum advice).

### Added: checks from the CADET forum review

`tests/test_forum_checks.py`: pH keeps its decimals, time axes never repeat a point, CADET
signal times strictly increase, and a run with no column builds with a warning.

## 0.23.0 (2026-11-09)

### Added: pick the columns the in-browser page shows

The wide tables on the page (phases, injections, fractions, Planned vs run - injections,
method steps, calibration, audit log) show a default set of columns; "Columns" above each
picks more, and the browser remembers the pick. Downloads still hold every column.

### Changed: Scouting runs says where it comes from

The Scouting runs table is the scouting plan from the method: every planned run, values as
typed, no units, the same in every run of the series. Its description now says so on the
page, in the Excel sheets, `README.txt` and `column_guide.csv`, and points to Method
variables for the values this run used, with units. File and column names are unchanged.

## 0.22.0 (2026-11-09)

### Changed: method hint tables are now "Planned vs run"

The two tables that compare the method's plan with what ran have plainer names. They stay
two tables: a block row is one run of a block, an injection row one valve period inside it.
Scripts that read the old CSV names need updating:

| Before | Now: CSV file | Now: page section and Excel sheet |
|--------|---------------|-----------------------------------|
| `<c>_method_hints_blocks.csv` | `<c>_planned_vs_run_blocks.csv` | Planned vs run - blocks |
| `<c>_method_hints_injections.csv` | `<c>_planned_vs_run_injections.csv` | Planned vs run - injections |

The `--method-hints` option and `pycorn.method_hints` keep their names.

### Changed: PyCORN columns are "calculated" or "analysis"; your inputs are "User input"

The `PyCORN` origin is split, in the app, the Excel sheets, `column_guide.csv` and
`README.txt`:

- `User input`: a value you gave, as given (epsilon, response factor, loop volume, sample
  concentration).
- `PyCORN calculated`: a fixed formula on stored values and your inputs (adjusted time and volume, UTC from a
  stored offset, a value read as a number, ...). The column guide's new `check` column says
  how to redo it or where UNICORN shows the same number.
- `PyCORN analysis`: PyCORN's rules and choices (Vc, pauses, phase and section ends, planned
  vs run status, warnings). A formula on an analysis value is analysis too, so CV (needs
  Vc), UTC (needs the rebuilt pauses) and a row's end and length are analysis.
- `PyCORN` now marks only what is about the export itself (version, options, the guide).

Tables that were `PyCORN` (Phases, Injections, Run log sections, ...) are now `PyCORN
analysis`. Scripts that filter `column_guide.csv` on `origin == "PyCORN"` need updating.
In the app, calculated columns have a solid underline, analysis columns a dotted one, your
inputs a dashed one.

### Fixed: the plot cursor names its x value

In the app, the label at the plot's vertical cursor line now says what x is and its unit
(`volume: 12 ml`), not a bare number. The page now loads Plotly 3.7.0.

## 0.21.0 (2026-11-09)

The first release with notes. Earlier versions (0.20 and before) have no notes; the public git
history was rebuilt on 2026-11-12 and starts at 0.37.0.

### Changed: export file and sheet names

The app, the Excel sheets and the CSV files now use the same human-friendly names. Scripts
that read the old CSV names need updating:

| Before | Now: CSV file | Now: page section and Excel sheet |
|--------|---------------|-----------------------------------|
| `variables.csv` | `method_variables.csv` | Method variables |
| `run_details.csv` | `run_and_method.csv` | Run and method |
| `run_info.csv` | `column_and_timeline.csv` | Column and timeline |
| `<c>_sections.csv` | `<c>_run_log_sections.csv` | Run log sections |
| `<c>_hint_blocks.csv` | `<c>_method_hints_blocks.csv` | Method hints - blocks |
| `<c>_hint_injections.csv` | `<c>_method_hints_injections.csv` | Method hints - injections |
| `<c>_bypass_uv.csv` | `<c>_by_pass_uv.csv` | By-pass UV |

Other CSV names are unchanged; Excel sheets now start with a capital (`Summary`, `Chart`,
`Curves`, ...). `pycorn export --update` still reads the stamp from folders written by
earlier versions (`run_info.csv`). CADET-Core and CADET-Process files keep CADET's own names.

### New: what is UNICORN's data and what is PyCORN's analysis

- Every table has an origin: `UNICORN` (stored data, only reshaped), `PyCORN` (PyCORN's
  analysis) or `UNICORN + PyCORN` (both). It is shown in the app beside each section, in
  the Excel Summary's sheet list, and in `README.txt`.
- `column_guide.csv` (and a "Column guide" sheet): one row per column of every table
  written, with its origin and what it means.
- In the app, column headers PyCORN computed are underlined; hover a header for its meaning.

### New: every export can be traced and reproduced

- Each run folder gets `README.txt` (what every file holds, its origin, how it was made)
  and `provenance.json` (app, version, git commit, repository, route, command line, formats
  and every option that differs from the defaults).
- The Excel Summary sheet has a "How it was made" block with the same.
- `column_and_timeline.csv` has `pycorn_version`, `export_route` and `export_options`.
- CADET-Core files hold it in `meta/EXPORT`, CADET-Process files in `experiment.json`
  (`source.export`); `PYCORN_VERSION` now includes the commit.
- The version is set in one place (`pycorn/version.py`); installs and the web page record
  the git commit they were built from.

### New in the app (web page)

- A vertical line follows the cursor on the chromatogram, with the x value and every
  curve's value at that point.
- Download all: every run in the list as Excel or CSV in one zip, one folder per run as
  `pycorn export` lays them out, with `index.csv`.
- The page says plainly that files stay on the computer, links to these release notes, and
  shows the version and commit it was built from.
