# Using PyCORN from Scripts and Agents

Everything `pycorn` does runs without a person: no prompts, one line per error on stderr,
fixed exit codes. This page lists what a script or an AI agent needs. Flags, files and
columns: [EXPORT_GUIDE.md](EXPORT_GUIDE.md); the Python model: [USER_GUIDE.md](USER_GUIDE.md).

## Install

```
pip install "pycorn[database] @ git+https://github.com/traylosj/PyCORN.git@master"
pycorn --version
```

`pycorn --version` prints the version and the git commit (`<version> (commit <id>)`).
`pycorn.__version__` holds the version in Python.

## Database settings without prompts

A person writes the settings once with `pycorn-setup` (the password is typed hidden). It
stays human-only: there is no password flag and no stdin mode, so the password never passes
through a script or an agent. An admin or deployment script can write the same file without
prompts from the `UNICORN_*` environment variables: `pycorn-setup --from-env [--file PATH]
[--overwrite] [--no-test]` (server, user and password needed; prints names with set / empty
only; exit 2 and nothing written when one is empty or the file is there without
`--overwrite`, 1 when the connection test fails; an existing file is first copied to a dated
`.bak` next to it, never removed, with a warning past 5). The script then points at that file:

- `pycorn --env-file FILE find ...`, or the `PYCORN_ENV_FILE` environment variable;
- else the usual lookup (`./sql/.env`, `./.env`, the per-user file `pycorn-setup` wrote);
- `UNICORN_*` environment variables override the file key by key (they put the password in
  the process environment: prefer the file).

Check them, as `pycorn find` reads them, with no value shown:

```
pycorn sources --test --json      # every source alias: settings set / empty, connected or the error
pycorn-setup --test-only          # the same test, [OK] / [FAIL] per source
```

## Discover tables, roles and units

```
pycorn schema --json
```

One JSON document with `"schema": 1`: every exported table (title, CSV file, download group,
origin) and column (origin, meaning, how to check it), the `--tables` groups, curve roles,
units and exit codes. It reads no run and no settings. A key is only ever added; a renamed or
removed key raises `schema`. In Python: `pycorn.build_schema()`.

## Look inside runs without exporting

```
pycorn inspect results/*.zip folder.UFol --json
pycorn inspect --from runs.csv --json            # a find CSV or file catalog, as export --from
```

One JSON document: `"schema": 1`, `version`, `command` (`"inspect"`), `masked`, `runs` (one
object per run), `not_read` (file name and reason per file that could not be read) and
`problems` (what was left out; exit 1 then). Each run holds `name`, `source` (`kind`, `label`:
a database by its label `alias#n` and `result_id`, a file by its file name, never a server or
database name), `folder`, `run_start` (`local`, `utc`, `utc_offset_minutes`, `source`),
`system` (`name`, `type`), `unicorn_version`, `column` (the columns UNICORN named), `variables`
(`name`, `value`, `number`, `unit`, `scouting`), `chromatograms` and `warnings`. Each
chromatogram: `name`, `kind` (original / imported / derived), `label`, `source_result_id`,
`source_result_name`, `copy_of_run`, `default` (what export picks without `--chromatogram`),
`column_volume` (`value`, `unit`, `source`), `curves` (`name`, `role`, `data_type`, `unit`,
`points`, `axes`, `original`, ...) and `phases` / `injections` / `fractions` (positions only:
start and end time, volume, CV, UTC). Never curve values, IP addresses or ports.

`"masked": true` means people, computers, instruments and serial numbers (`User1`, `Computer1`,
`System1`, `Serial1`) and run, folder and file names (`Run1`, `Folder1`, `File1.zip`; the
source result of an imported chromatogram as that run's label, else `Result1`), the method
name (`Method1`), names a user set on imported or derived chromatograms and evaluated curves
(`Chrom1`, `Curve1`; the original chromatogram and curves as UNICORN recorded them keep theirs)
and method variable values that are text (`(text, masked)`; numbers stay) are replaced, in
every field and text of the document; refer to runs by their label or position, and the
person can re-run with `--no-mask` locally to see the names. Masked chromatogram names can't
be passed to `export --chromatogram`: use `imported`, `derived` or `all`, or `--no-mask`
locally. Labels hold within one document; result IDs, column, phase and fraction names, the
system type and the database label are not changed. Without `--json` the same as text. In
Python: `pycorn.run_summary(run, mask=True)` (one run: `Run1`); for several runs pass one
`pycorn.summary.SummaryNames()` as `names=` to every call. `--jsonl` streams the same runs
(below).

## Machine-readable output: --json, --jsonl, --csv -

`find`, `export`, `bypass-uv`, `windows` and `inspect` take `--json` or `--jsonl` (not both). Then stdout
holds only the result and every human line (progress, notes, warnings, `incomplete: ...`)
goes to stderr; without them the output is the same as for a person. `--csv -` (`find`,
`bypass-uv`, `windows`) writes the CSV to stdout instead of a file (UTF-8, no BOM), human lines to stderr;
it can't be combined with `--json` / `--jsonl`.

`--json`: one document, `{"schema": 1, "version", "command", "masked", ...}`, then per command:

| Command | Keys after the envelope |
|---------|-------------------------|
| `find` | `total` (matches), `more`, `next_after` (`'alias#n:ResultID'` for `--after`, or null), `rows` (the catalog rows), `unreachable` (`source`, `reason` per source not searched), `problems` |
| `export` | `out`, `index` (the output folder and its `index.csv`), `totals` (`selected`, `written`, `incomplete`, `skipped`, `failed`, `not_loaded`, `not_read`), `rows` (the `index.csv` rows, `export_error` included, plus `position`, the row's place in `index.csv`), `not_read` (`file`, `reason`), `problems` |
| `bypass-uv` | `readings` (the `--csv` columns; `warnings` a list), `runs` (loaded), `not_read`, `problems` |
| `windows` | `rows` (the `--csv` columns: `source`, `result_id`, then the export's `phase_window_statistics` columns; `warnings` a list), `runs` (loaded), `not_read`, `problems`, `warnings` (`run`, `chromatogram`, `text`: a phase or curve not found, each window's clipping), `notes` (the interim phase rule when there are rows) |
| `inspect` | `runs`, `not_read`, `problems` (above) |

`pycorn windows <files> | --from CSV --window "NAME=PHASE:FROM:TO UNIT" --window-curves NAMES`
(M22 statistics without an export; both flags required, a missing one is `bad_flag`, exit 2):
exit 1 when a file or run was not read, a chromatogram failed, a curve name matched nothing, a
window gave no row, a row has `n` = 0 or `--chromatogram` picked nothing; each such case is a
line in `problems`. A clipped window is only a warning.

`--jsonl`: one JSON object per line as rows are produced. First
`{"schema": 1, "version", "command", "type": "header", "masked"}`, then one line per row,
`{"type": "row", ...}` (find), `"run"` (export, as each run's status is known, so not in
`index.csv` order: use `position`; inspect), `"reading"` (bypass-uv), `"row"` (windows), last
`{"type": "summary", ...}` with what `--json` holds besides the rows (`total`, `rows` as a count,
`more`, `next_after`, `unreachable`, `problems` for find; `totals`, `out`, `index`, `not_read`,
`problems` for export; `readings`, `runs`, `not_read`, `problems` for bypass-uv; `rows` as a
count, `runs`, `not_read`, `problems`, `warnings`, `notes` for windows; `runs`,
`not_read`, `problems` for inspect). No summary line means the command stopped (see the error).

Every document and line follows `"schema": 1`: a key is only ever added; a renamed or removed
key raises the number. A database is named by its label only (`db_label`, `alias#n`): the
`database` column of the catalog and the index is left out, never a server or database name.
Paths under the home folder are written `~` (`path`, `out`, `index`, error texts). A CSV that
`find --csv -` wrote can go back to `export`, `bypass-uv` and `inspect --from`: the database is
found again by its label in the settings.

Masking (`"masked": true` by default, `--no-mask` shows the values as stored):

| Output | Masked |
|--------|--------|
| `find --json` / `--jsonl` / `--csv -` | people only: `system` (System1, ...), `created_by` (User1, ...; Default and System stay), and the system inside `match_key`. Run and folder names, method variables, column, batch id: shown (pick runs by them) |
| `export --json` / `--jsonl` | the index rows, as `find` (and those values inside `export_error`). The files written are not changed: as stored unless `--mask-identifying` |
| `bypass-uv --json` / `--jsonl` / `--csv -` | people, computers, instruments and serial numbers the run stores, wherever they appear in a reading's texts (`run_details.Masker`'s rule); run, chromatogram and curve names shown |
| `windows --json` / `--jsonl` / `--csv -` | as `bypass-uv`, in a row's texts and the `warnings` and `problems` texts; run, chromatogram, window, phase and curve names shown |
| `inspect --json` / `--jsonl` | as above (people, run, folder, file, method and user-set names) |
| `--csv FILE`, `index.csv`, the export files | not masked (today's rule; `export --mask-identifying` masks the files and `index.csv`) |

Labels count once per document (the same name, any case, the same label). In these modes
stderr is masked the same way, with the same labels (warnings, notes, the error object's
`text`; warnings come at the end of the command, when every run's names are known);
`--no-mask` shows both as stored.

### Errors

With `--json` or `--jsonl` an error is one JSON object on stderr, stdout stays empty (or ends
without its summary line):

```
{"error": {"text": "not found: ~\\results\\a.zip", "key": "file_not_found", "exit": 2}}
```

`exit` is the process exit code; `hint` is added for a connection error (check the settings)
and an unexpected one (how to report it). `key` (also in `pycorn schema --json`, `error_keys`):
`bad_flag` (usage or a flag's value; argparse errors too), `bad_input`, `no_settings`,
`bad_settings`, `connection_failed`, `file_not_found`, `permission_denied`, `os_error`,
`out_of_memory`, `missing_package`, `interrupted` (130), `unexpected` (3). A key is only ever
added. `--debug` still prints the traceback, before the object. Without `--json` / `--jsonl`
errors are the plain lines below.

## Exit codes

| Exit | Meaning |
|------|---------|
| 0 | Everything asked for was done |
| 1 | It ran, but left something out (stderr, `problems` and `index.csv` say what) |
| 2 | `error: ...` on stderr (or the error object): a flag, a file name, the settings or the connection; nothing was done |
| 3 | `unexpected error (...)`: a fault in PyCORN; `--debug` adds the traceback |
| 130 | Interrupted |

`pycorn.cli.main([...])` runs a command in-process and returns the same code.

## Find and export

```
pycorn find --folder "/DefaultHome/Projects/**" --run-start 2024-01-01.. --all --csv runs.csv
pycorn export --from runs.csv --out exports/ --format csv --tables key,peaks
pycorn export results/*.zip --out exports/
pycorn find --name "Sample*" --json                  # the rows on stdout, people masked
pycorn export --from runs.csv --out exports/ --jsonl # one line per run as it is written
```

Read `exports/index.csv` (one row per run, with its status) and each run's `warnings.csv`,
`column_guide.csv` and `provenance.json`. Runs already exported are skipped (`--update`,
`--overwrite`). `exports/options.json` holds the options of the export;
`pycorn export ... --options exports/options.json` repeats them (flags given as well replace
the file's value of their option). The page's "Download all" zip holds the same `index.csv`
columns and an `options.json` too, so `--options` on it gives the files the page gave (the page
always writes the method hints: its `options.json` says `"method_hints": true`).

### Options as data

`ExportOptions.to_dict()` gives every option as one JSON object, keys in field order, defaults
included; `ExportOptions.from_dict(d)` reads it back (a partial dict too: a missing key or
`null` keeps the default). Quantities are `[value, "unit"]`, extinction coefficients
`{"<nm>": [value, "unit"]}`:

```json
{"method_hints": true, "mask_identifying": true, "tables": ["key", "curves"],
 "bed_height": [10, "cm"], "epsilon": {"280": [1.4, "ml/(mg*cm)"]}, "max_points": 500, "method": "hybrid"}
```

An unknown key, a value of the wrong kind or a unit of the wrong quantity raises `ValueError`
naming the key. Every key and its flag: [EXPORT_GUIDE.md](EXPORT_GUIDE.md), "Options as a file".

## Python API

`pycorn.__all__` names the supported API; everything else is reached by module path and may
change between releases.

```python
import pycorn
run = pycorn.open_run("result.zip")                 # or open_folder(".UFol"), PyCORNDatabase().load_run(id)
catalog = pycorn.catalog_from_files(["results/"])   # or PyCORNDatabase(env_file=...).find(...)
chrom = next(iter(run.chromatograms.values()))
uv = pycorn.curves_with_role(chrom, "uv")           # by role, never by curve name
pycorn.write_run(run, "out/run1", ("csv",), options=pycorn.ExportOptions(mask_identifying=True))
```

Batch export, as `pycorn export` does it (skips, `update=`, `overwrite=`, `index.csv` and
`options.json` in `out`); returns the index rows as a DataFrame:

```python
import json
import pycorn

catalog = pycorn.catalog_from_files(["results/"])   # or db.find(...); then db=db
options = pycorn.ExportOptions.from_dict(json.load(open("options.json")))
index = pycorn.export_selection(catalog, "exports/", ["csv"], options=options, update=True,
                                on_run=lambda e: e["event"] == "row" and print(e["row"]["export_status"]))
print(index["export_status"].value_counts())
```

`on_run(event)` gets a dict per step: `selected` (once, with the counts), `loaded` (a run read,
before it is written), `failed` (while its exception is handled) and `row` (once per run:
`position`, `row` with the `export_*` columns, `written`, `missing`). A run that cannot be
loaded or written is a row (`not loaded`, `failed`) and a warning, unless `strict=True`. With
database rows in the catalog give `db=` (a `PyCORNDatabase`, or a function returning one).
The files and `index.csv` follow today's masking rule: as stored unless
`mask_identifying=True` (then the run files and `index.csv`'s `system`, `created_by`,
`match_key` and `export_error` are masked; `run_export.mask_index`). `export_error` says why a
run is `not loaded`, `failed` or `incomplete`.

Also: `load_files`, `export_cadet`, `bypass_uv_readings`, `parse_windows` / `phase_windows` /
`window_stats_rows`, `source_status`, `run_summary`, `plain_error`.

## Before output leaves the PC

- Give `--mask-identifying` (or `ExportOptions(mask_identifying=True)`) to any export whose
  files go to a remote model or service: logins, computer and instrument names and serial
  numbers become `User1`, `Computer1`, `System1`, `Serial1`, and typed notes become
  `(free text, masked)`. Run, folder and method names are not changed: check them.
- `--json`, `--jsonl` and `--csv -` are masked by default (table above); `find`, `export` and
  `bypass-uv` mask people only, so run and folder names still go out: check them too.
- Never print or send connection values (server, database, login, password) or the `.env`
  file; `pycorn` itself shows only whether each is set, and names databases by label (`alias#n`).
- Do not send whole result files; [`tools/anonymize_zip.py`](../tools/anonymize_zip.py)
  sanitizes one before it is shared.
