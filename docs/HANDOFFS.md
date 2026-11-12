# Handoffs: what each work package built, how it was checked

Moved out of `docs/ROADMAP.md` ("Start here") on 2026-11-09 so the roadmap stays short.
Each handoff says what was built, what was checked, what was **not** checked, behaviour
changes and gotchas. Add a new one at the top when a work package ends; the roadmap keeps
only the one-line state. Decisions are in [`ROADMAP.md`](ROADMAP.md); what changed per
release is in [`CHANGELOG.md`](../CHANGELOG.md).

**Handoff: 0.37.0 (2026-11-12): imported chromatograms' run log, Track 9 A3, A4, A5, A7,
A11, run list ticks, M22's page table.** Decisions in [`ROADMAP.md`](ROADMAP.md),
"Agents and scripts (Track 9)" and W17.

- Built, W17 follow-up: `Chromatogram.run_log_curves()` / `run_log()` by stored type,
  `run_log_source()`, `own_event_curves()`, `from_other_run()` / `other_run_curves()`,
  `Run.holds_run_of()`, `imported_note()`; tables, sections, window statistics, Vc
  run-log candidate, CV, UTC and CADET read the imported chromatogram's `ImportedLogbook`;
  this result's run-level data used with one warning each; curves of another method run get
  no role.
- Built, A11: `pycorn-setup --from-env [--overwrite]`; `keep_dated_copy()` (created
  exclusively, 0600 off Windows; if it can't be written nothing is written, exit 2); empty
  optional keys left out of a new file in both modes.
- Built, A3 / A4: `pycorn/summary.py` (`run_summary`, `SummaryNames`, `document()`),
  `pycorn inspect`; `pycorn/json_output.py` (envelope, `JsonLines`, `error_document()`,
  `PeopleMasker`); `--json` / `--jsonl` / `--no-mask` on find / export / bypass-uv /
  inspect / windows, `--csv -`; `messages.ERROR_KEYS` / `error_key()` / `SettingsError`,
  listed by `pycorn schema`.
- Built, A5 / A7: `run_export.export_selection` (the CLI's batch loop moved out of
  `cli.py`: `index_frame`, `INDEX_COLUMNS`, `write_selected`, ...), `ExportOptions.to_dict()`
  / `from_dict()`, `export --options`, `options.json`, `export_error`, `mask_index`;
  `load_files` / `load_many` `on_skip=`; `pycorn windows`.
- Built, page: run list ticks (click / Ctrl / Shift, keyboard; plot follows the ticks);
  M22 form and table (`Session.window_info` / `parse_window` / `window_stats`, results
  cached per run, windows and curves); new keys in all seven languages.
- Checked: full suite with a live database (808 passed, 23 skipped); new tests
  `tests/test_inspect.py`, `test_json_output.py`, `test_export_selection.py`,
  `test_windows_cli.py`, more in `test_setup_env.py`, `test_chromatograms.py`,
  `test_browser.py`; every sample's default CSV and CADET export byte-identical for the
  original chromatogram; CLI human output and existing files byte-identical with A5; the
  built page in headless Edge from `file://` (load, multi-chromatogram fixture, ticks and
  overlay, Download all's `index.csv` and `options.json`, the M22 form and table, every
  language); imported chromatograms' tables and sections match their source results',
  tested against a lab environment.
- **Not checked:** the run list ticks in Firefox, Safari, at phone width and in dark mode;
  the overlay note past 10 ticked runs; a derived chromatogram from a real result;
  `pycorn-setup --from-env` against a live server from a deployment script.
- Behaviour changes: the page's `index.csv` columns (CHANGELOG 0.37.0); `export_error` and
  `options.json` in every export; `--mask-identifying` masks `index.csv`; imported
  chromatograms now have tables, sections and CADET files; `pycorn-setup` keeps `.bak`
  copies.
- Gotchas: in machine modes warnings are held to the end of the command so every run's
  names are known to the masker; a renamed pooled curve in a zip (no `EventText2`) counts
  as fractions; `pycorn.run_summary` is a function, so no submodule may take that name.

**Handoff: scripts and agents, Track 9 A1, A2, A6, A9 (0.36.0, 2026-11-11).**

- Built: `pycorn --version`, `pycorn.__version__`, an explicit `__all__` (heavy names load
  lazily, `_LAZY`); `pycorn/schema.py` `build_schema()` and `pycorn schema [--json]`
  (`browser.names()` now `schema.page_names()`); `messages.EXIT_CODES`;
  `setup_env.source_status()` behind `pycorn-setup --test-only` and `pycorn sources [--test]
  [--json]` (set/empty only; a settings file under the home folder as `~`);
  `docs/AGENT_GUIDE.md`.
- Checked: `tests/test_agentic.py` (public names import, schema JSON and text, page names
  from the schema, `--test-only` and `sources` with a fake driver and a cleared
  environment, the `~` path); the full suite together with W17.
- **Not checked:** `pycorn sources --test` against a live server from the CLI (the fake
  driver covers the logic).
- Gotchas: `test_connection` and `window_stats` (the function) are importable but not in
  `__all__` (pytest would collect the first; the second clashes with its module name).

**Handoff: W17 chromatograms selectable everywhere (0.36.0, 2026-11-11, Track 8).** Owner
chose: every chromatogram selectable in Python, CLI, Excel / CSV and the page; the default for
export and visuals the run's original chromatogram only.

- Built: `model.chromatogram_kind` / `describe_chromatograms` / `select_chromatograms` (one
  rule for every path), `Chromatogram.kind` / `.source_result` / `.only_original()`,
  `Run.describe_chromatograms()` / `select_chromatograms()` / `chromatogram_selection()` /
  `result_id`; `run.chromatogram()` is the first original one. Role and type lookups default to
  `original_only=None`: original curves on the original chromatogram, every curve on an imported
  or derived one. `ExportOptions.chromatograms`, `run_export.chromatogram_stems` (stems from
  every chromatogram, so they don't move), `pycorn export` / `bypass-uv --chromatogram`,
  `export_cadet(chromatogram=` name, word or list`)`, four new `run_info` columns, a
  "Chromatogram kind" summary row; `file_catalog.read_headers` gives each chromatogram's `kind`,
  `file_catalog.file_chromatograms()` the list from the light read. Page: `rows()` 'chromatograms',
  `view(index, chromatograms)`, `options['chromatograms']` (download; Download all a dict per run),
  overlay ids `[index, chromatogram]`; a folded picker in the run list's Chromatograms cell, keys
  `chrom.*` in every language. Tests: `tests/test_chromatograms.py` on a synthetic fixture
  (`tests/multichrom_fixture.py`: sample.zip plus an imported, a copied-in and a derived chromatogram).
- Checked: full suite; the built page in headless Edge from `file://` with the fixture (picker,
  view, csv and Download all, overlay of two chromatograms of one run, every language, offline
  after Ready, no console errors); checked against a lab environment.
- **Not checked:** a derived chromatogram from a real result (the rule is
  "every curve and event non-original, no original result"); a zip export of a result with
  imported chromatograms (the copy of the run is then found by name, a zip stores no ResultID).
- Behaviour changes: exports, `export_cadet()` and the page write and show the original
  chromatogram only (were: every chromatogram); `column_and_timeline.csv` has
  `chromatogram_kind`, `source_result_id`, `source_result_name`, `copy_of_run` after
  `chromatogram`, one row per chromatogram written; the workbook summary has a "Chromatogram
  kind" row; a `chromatograms` line in `warnings.csv` names the chromatograms left out; the
  CADET warning reads "N chromatograms selected (...)".
- Gotchas: an imported chromatogram's run log is `ImportedLogbook`, so its phase / injection /
  fraction tables, sections, window statistics and CADET files are empty or not written (open
  owner question). Don't use `sed -i` from Git Bash on this checkout's files: it rewrites CRLF as LF.

**Handoff: inlets from the system configuration (0.35.0, 2026-11-11).** Decide 22, D27 (4).

- Built: `pycorn/system_configuration.py` maps the result's stored components
  (`EnabledComponents`, `ComponentStructure`) to five roles (inlet valve A / B, sample inlet
  valve, sample pump, quaternary valve) by component name (two system families) or the
  hardware code in its display name; installed / not installed (configured "none", or listed
  but disabled) / not known (not listed, conflicting, no configuration). `run.system_configuration()`,
  also set on each chromatogram at load. `run_log_sections.py` lost `DEFAULT_INLETS`,
  `DEFAULT_SAMPLE_INLET`; labels and `inlets_source` as in the ROADMAP entry; CADET sections
  and hints carry the states.
- Checked: 18 tests in `tests/test_system_configuration.py` (every `samples/*.zip`); results
  with no sample pump and one with a feed through line A, tested against a lab environment: no `B1` label, line B `B` in every section, one line-feed section with the
  no-sample-pump wording. CADET reference JSON unchanged.
- **Not checked:** other system families, BufferPro, a quaternary valve in use, a shared
  A/B valve (no result has them).
- Gotchas: an assumed `Buffer` sample inlet still counts as feed (`sample_use`), as before;
  a role the configuration doesn't list reads "not known".

**Handoff: per-phase curve statistics, M22 (0.35.0, 2026-11-11).**

- Built: `pycorn/window_stats.py`: `phase_windows()` (phases from `event_tables.phase_rows`,
  one window per phase occurrence, resolved on time, volume and CV), `window_stats()`
  (n, mean, median, SD n-1, min, max; inclusive bounds; NaN left out; `STATISTICS`
  registry), `window_stats_rows()` / `to_frame()`. `ExportOptions(windows=, window_curves=)`,
  CLI `--window` / `--window-curves`, table `phase_window_statistics` (CSV and Excel).
- Checked: 27 tests in `tests/test_window_stats.py` (synthetic runs and samples): missing
  phase or curve, CV without Vc, clipping, empty windows, curves on other grids.
- **Not checked:** the page (no table there yet); runs with repeated phases from a database.
- Gotchas: the interim phase rule (logged starts, no lead-in) changes when Decide 9 / 23 land.

**Handoff: overlaid runs on the page, W11 (0.35.0, 2026-11-11).**

- Built: `Session.overlay_run(index)` (one run read, its role curves thinned to about 1000
  points and cached, cache of 20, the run dropped) and `Session.overlay(ids, roles, axis)`;
  a worker job reads one run per step (Cancel between runs); plot bar toggles "This run /
  Ticked runs" and "Curves as panels / Runs as panels" (`pycorn.overlaylayout`); at most 10
  runs, a note names those left out or without the axis. Pressed toggle buttons no longer
  turn unreadable on hover.
- Checked: 3 tests in `tests/test_browser.py`; headless Edge from `file://`: 6 samples, 3
  overlaid in both layouts, layout kept, four x axes, the 10-run cap, German, PNG, Clear, no
  network request after "Ready".
- **Not checked:** HTTP serving, Firefox, Safari, phone width, languages other than German
  in a browser.

**Handoff: web page, Python in a web worker (2026-11-09).** Owner chose: Cancel stops at the
next step and a second click ("Stop now") restarts Python; progress by step within a run;
the page stays live for viewing, one Python job at a time; where no worker starts, Python on
the page as before.

- Built: `web/index.html` holds the engine as text (`<script type="text/plain" id="engine">`,
  `makeEngine(post)`: jobs start, add, view, download, all, clear). The page runs it in a
  module worker made from a blob (`newWorker`); from `file://` (origin null) the module worker
  fails and a classic one is used, with `importScripts` hidden (Pyodide checks it only to
  refuse classic workers and loads with `import()` either way); with no worker at all, the
  same engine runs on the page (`newLocal`, eval) and the footer says so. `pyodide.js` is
  fetched with `integrity` and run with indirect eval (`PYODIDE`, `PYODIDE_SRI`), so the SRI
  check stays; the page's `<script>` for Pyodide is gone. Files go to the worker as
  transferred ArrayBuffers, one `Session.add` per file (rows appear as each is read);
  downloads come back as bytes. `Session(progress=)` reports `browser.STEPS` (texts
  `step.*` in `web/i18n/`). Stop now: `worker.terminate()`, a new worker, the files in `kept`
  read again in order (same rows; the list is rebuilt if the count differs). Tests:
  `test_progress_reports_the_steps_of_a_view_and_a_download`; the CSP and i18n tests know the
  engine's URL and the step keys.
- Also: the tracks' hover traces are invisible lines, not invisible markers (about 52 000 SVG
  nodes on the sample): a chart draw went from 550 ms (1.1 s with every curve) to 50-75 ms.
- Checked in headless Edge, `file://` and HTTP: worker starts (module over HTTP, classic from
  a file); 35 runs listed with "Reading file n of m" and view steps; Download all with "Writing
  run n of 35 (step)"; longest page freeze about 0.1 s while reading and during Download all
  with a curve switched (5.4 s before); Cancel between runs; Stop now in Download all restarts and keeps the 35
  rows, a run shows and downloads after; Clear; fallback (`window.Worker = undefined`):
  list, download, Download all. Offline after "Ready": files, Stop now (Pyodide again from the
  browser cache: requests to the CDN, served from cache) and a download after it.
- **Not checked:** Firefox and Safari (module workers from blob, `import()` in a classic
  worker from `file://`); a Stop now with a cold or cleared browser cache offline (the restart
  then fails and the page offers "Reload"); memory with large drops (files are in the page as
  `File` objects and in the worker's file system).
- Gotchas: the worker inherits the page's Content-Security-Policy (`worker-src blob:`,
  `'unsafe-eval'` for the eval of `pyodide.js`). `postMessage` from the worker reaches the page
  while Python works, but a cancel message reaches the worker only between steps (the engine's
  `pause()`); one step can't be interrupted without SharedArrayBuffer, which needs COOP/COEP
  headers that GitHub Pages and `file://` can't send. A view or download cancelled after its
  one step has started finishes in the worker and its result is dropped.

**Handoff: non-programmer usability (2026-11-08).** The owner asked how to make
PyCORN friendly for non-programmers; options were offered and chosen (Decisions, "Usability
for non-programmers").

- Built: `pycorn-setup` (`pycorn/setup_env.py`, console script in `setup.py`) and the Excel
  workbook for people (`pycorn/excel_export.py`, called from the xlsx branch of
  `run_export._write_into()`); xlsxwriter is a core dependency. Tests:
  `tests/test_setup_env.py`, `tests/test_excel_export.py`, `test_write_run_xlsx` in
  `tests/test_cli.py`.
- Checked: full suite green; the three samples exported to xlsx; charts rendered
  through LibreOffice (`soffice --convert-to pdf` on a workbook cut down to the plotted
  columns; a full one is 1000+ pages); `pycorn-setup --test-only --file sql/.env` against a
  lab server: [OK].
- **Not checked:** the workbooks in real Excel (label rotation and data labels may look
  different; LibreOffice ignores the -45 degree rotation, so labels overlap there), and
  `pycorn-setup`'s question flow by a person at a real console (tests drive it with fake
  input; `getpass` in some IDE consoles echoes or warns).
- Behaviour changes for existing users: workbooks are `<run name>_<chrom>.xlsx` (were
  `<chrom>.xlsx`); empty sheets are no longer written; Excel headers of time/volume columns
  carry units (csv unchanged); `summary`, a `chart` sheet and a hidden `chart_marks` sheet are
  added (`pd.read_excel(sheet_name=None)` skips the chart sheet but returns `chart_marks`).
- Gotchas: the per-user `.env` is the **last** lookup place, so an old `./sql/.env` silently
  wins (the tool warns); `setup_env` calls `db_connector.user_env_file()` through the module so
  the `tests/conftest.py` patch reaches it; keep it that way. Chart curves come from roles
  (`uv`; `conductivity`, else `conc_b`) and flat curves are dropped, never names. "Read from"
  in the summary is the file name only: folders can name the user.
- Item 5 (plain-language messages) built after this handoff: see the next handoff.

**Handoff: plain-language messages, item 5 (2026-11-08, pushed).** Owner chose:
wording at the source and in the CLI, "looks like success" cases fixed, unexpected errors in
one line (Decisions, "Usability for non-programmers", "Plain-language messages", has the
details).

- Built: `pycorn/messages.py` (`plain_error`, `REPORT`, `SETUP_HINT`); `_CONNECT_ERRORS` /
  `_plain_connect_error` in `db_connector.py` (sentence in `str(e)`, redacted driver text in
  `e.detail`); `db.unreachable` (alias -> reason, set in `_scopes`); `NOT_A_RESULT` in
  `zip_source.py` (raised by `read_zip` and `file_catalog.read_headers`);
  `catalog.attrs['not_read']`; mg/ml wavelength check in `run_export.missing_outputs`;
  `cli.main()` exit codes 0/1/2/3/130 and `--debug` (a parent parser gives it to every
  command; `default=SUPPRESS` there keeps one given before the command). Tests:
  `tests/test_messages.py`, the "plain-language messages" block in `tests/test_cli.py`.
- Checked: full suite green (507 passed, 21 skipped); failure cases run through the real
  CLI before and after (the script was a temp file, not kept); `pycorn find` against a lab
  server: exit 0, unchanged.
- **Not checked:** connection sentences against a real wrong password / wrong database
  (patterns from pymssql's error codes 18456, 4060, 20003, 20009 and tested with fake driver
  text; only "refused" was seen live, on a closed local port). One full-suite run had a single
  failure that did not come back in three reruns; which test it was is not known.
- Behaviour changes: some runs exit 1 that exited 0 (file not read, non-UNICORN zip, missing
  `--epsilon` wavelength, source not reached; `bypass-uv` with runs not loaded); `open_run()`
  rejects a zip with neither `Result.xml` nor a chromatogram; library warnings lost their
  `catalog_from_files:` / `load_files:` prefixes; `ValueError("database settings: ...")`
  replaces "Failed to load credentials".
- Gotchas: `run_find` mutes the per-query "cannot connect" warnings inside its own
  `catch_warnings` and names unreachable sources once at the end; `main()` wraps each command
  in `catch_warnings`, so filters never leak between calls (tests call `main()` many times).
  The connection-error patterns are checked in order: login before "unable to connect",
  because pymssql's text for a refused login can also carry 20002 ("connection failed").
- Left as is: mg/ml that `concentration()` refused for a wavelength the run has (only in
  `warnings.csv`); `bypass-uv` with no by-pass in the run (exit 0). Word more errors as users
  report them.
- Item 6 (in-browser page) built after this handoff: see the next handoff.

**Handoff: in-browser page, item 6 (2026-11-08, pushed, live at
https://traylosj.github.io/PyCORN/).** Owner chose: GitHub Pages and the same single file for
a shared drive; views: plot by role, phase/injection/fraction tables, run summary and
variables, xlsx and csv downloads; CADET-Core JSON as the only CADET output; no offline copy
for now (Decisions, "Usability for non-programmers", "In-browser page"). Pages is on (Source:
GitHub Actions); every push to master that touches `pycorn/`, `web/`, the build script or
`setup.py` redeploys (about 1 minute; check the `pages` run under Actions).

- Built: `pycorn/browser.py` (`Session`: `add`, `rows`, `view`, `download`; JSON text out,
  errors as `{'error': plain_error(e)}`), `web/index.html` (template: Pyodide 314.0.7 and
  Plotly basic 2.35.2 from CDNs, plain JS, no framework), `tools/build_web_page.py` (builds
  the PyCORN wheel from a temp copy of the sources, fetches pinned xmltodict / xlsxwriter
  wheels, embeds all three base64 into one `build/web/index.html`, about 610 KB),
  `.github/workflows/pages.yml` (builds and deploys on pushes to master that touch
  `pycorn/`, `web/`, the build script or `setup.py`). Tests: `tests/test_browser.py`.
  README: "No install: in the browser" and "How it works, and why your data stays private"
  (linked from the feature list and from the page's header "(how)"); the page header links
  to the repository and issues.
- Checked: full suite green (517 passed, 21 skipped); the built page in headless Edge (Playwright from a temp venv,
  script not kept), from `file://` and over HTTP: Python ready in about 5 s (CDN cached),
  runs from the samples and lab `.UFol` files listed in 0.5 s, plot drawn, other
  runs shown, xlsx (same sheets as `pycorn export`), csv zip and CADET-Core v5 JSON
  downloaded, a missing-geometry Core export worded, no console errors. The live page
  (traylosj.github.io/PyCORN) passes the same test; with the network cut after "Ready",
  dropping a file and the xlsx download still work and the page makes no request (the
  README's privacy section says so).
- **Not checked:** Firefox and Safari; a first load on a slow network or behind a lab proxy
  that blocks jsDelivr / cdn.plot.ly; large `.UFol` folders against browser memory (the
  largest tried: 10 MB); runs with several chromatograms (the selector is untested: no
  sample has two); a person using the page.
- To test after a change: `python -m pytest tests/test_browser.py` (CPython, same code), then
  `python tools/build_web_page.py` and open `build/web/index.html` (double-click works). For a
  scripted check: Playwright in a venv outside the repo (`pip install playwright`,
  `chromium.launch(channel="msedge")` uses the installed Edge, no browser download); wait for
  `#status` to start with "Ready", `set_input_files("#files", [...])`, click
  `button[data-kind=xlsx|csv|core]` inside `expect_download()`, fail on any `pageerror` or
  console error. For the privacy claim: `context.set_offline(True)` after "Ready" and record
  `page.on("request")`; it must stay empty.
- The README promises that no file leaves the computer and that the page works offline after
  "Ready". Keep it true: everything Python needs is loaded in `start()` (no `micropip` from
  PyPI, no `loadPackage` later), and nothing may `fetch` at run time. A new dependency goes in
  `DEPENDENCIES` (embedded) or in the `loadPackage` list at start; rerun the offline check.
- Upgrading Pyodide: change `PYODIDE` (the version in the URL) and `PYODIDE_SRI` (sha384 of the
  new `pyodide.js`) in the engine in `web/index.html`; check its
  `pyodide-lock.json` for the Python, numpy and pandas versions and run the suite on them
  locally first (314.0.7 = Python 3.14.2, pandas 3.0.2, numpy 2.4).
- Gotchas: Pyodide now numbers releases after Python (314.x = Python 3.14, pandas 3.0);
  xlsxwriter and xmltodict are not in Pyodide, so the page carries them (pins in
  `build_web_page.DEPENDENCIES`). `pip wheel` on the checkout builds in-tree and reuses a
  stale `build/lib`, hence the temp copy. Python ran on the page's main thread then (a web
  worker since 2026-11-09, see that handoff). Each download reloads the run from its file,
  because caller geometry changes a run's axes (D26). The page shows file names only, never
  folders. Python values reach the page as JSON text; NaN must become null
  (`browser._cell`), or `JSON.parse` fails (`test_view` checks `allow_nan=False`). The page
  version line reads `importlib.metadata`, so PyCORN must be installed from the wheel (not
  copied in). The README anchor `#how-it-works-and-why-your-data-stays-private` is linked
  from the page: renaming that heading breaks the link.
- Later, only with the owner: SRI hashes on the CDN scripts, a web worker, an offline
  bundle, more views (sections, pauses, mg/ml with a caller epsilon, by-pass UV), a release
  note or announcement to users, item 7 (getting-started guide can start from the page).
