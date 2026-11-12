# Facts, gotchas and the 2026-11-08 review

Moved out of `docs/ROADMAP.md` on 2026-11-09: reference material (facts about the bundle,
the database, the run log, the method and CADET, numbered F and X) and the closed review.
Owner decisions stay in [`ROADMAP.md`](ROADMAP.md) ("Decisions"). Open review items i-m are
tracked as M1-M6 in the roadmap's "Modeler input". Tested against a lab environment.

## Facts and gotchas

### Bundle facts (F1-F24)

On `samples/sample.zip` (F1-F15) and UNICORN 7.3 / 7.11 results (F16-F24).

| # | Fact |
|---|---|
| F1 | Curves have `IsoChroneType = Time`: `time[i] = DistanceToStartPoint + i * DistanceBetweenPoints` (min) |
| F2 | Volume is stored per point (`CoordinateData.Volumes`), monotonic; inner zips hold only volumes and amplitudes |
| F3 | `ColumnVolume` on every curve and event curve (0.7 ml in the sample) |
| F4 | Run log `Base CV, Vc=0.700 {ml}, Any` at t=0; later `Base Volume` / `Base SameAsMain` are block bases, not column changes |
| F5 | `System flow` x 60 / `System flow (CV/h)` = Vc at every point with flow |
| F6 | `ResultRunInformation` blobs (base64 UTF-16 XML): used columns (name, volume, unit), UNICORN version, run start |
| F7 | `MethodData` `mColumnVolume` can contradict the Vc used (0.353 vs 0.700): the method is not a Vc source |
| F8 | Peak table `ColumnVolume` = Vc; `ColumnTypeData` empty in the sample (not elsewhere) |
| F9 | `CurveUVInfo`: `IsUVValuesNormalizedToNominalUVPathLength`, `UVPathLength` 2.08, `NominalUVPathLength` 2 (mm, no unit); the `UV cell path length` curve stores 0.2 cm |
| F10 | `MethodStartTime` / `ChromatogramStartTime` with `...UtcOffsetMinutes`, ms precision |
| F11 | Run-log `Method Run <date> <offset>` at 0 and `End ... (Issued)` / `(Completed)` give wall-clock anchors |
| F12 | `Result.Created`, `Chromatogram.Created` are fallbacks; `EvaluationLog.xml` holds post-run evaluation actions |
| F13 | The sample has no Pause / Hold / Continue |
| F14 | UNICORN zeroes the x axis at an injection (`AdjustRetentionZeroToInjectionNumber`); peak positions reproduce from raw curves. `Chromatogram.retention_zero`, `*_adjusted` axes |
| F15 | `AmplitudePrecision` = displayed decimals (3 UV / pressure / flow, 2 cond / pH, 1 %, temperature); values are not rounded to it. `Curve.precision` |
| F16 | Raw data is identical between 7.3 and 7.11 for the same run; 7.11 adds evaluation / layout fields |
| F17 | `NextFracData` can be a PNG null-padded to 128 KiB (kind `png`) |
| F18 | Derived 7.11 baselines: `IsoChroneType = Volume`, amplitudes only, no time axis |
| F19 | Curve numbers differ between runs; `CurveDataType` + unit identify UV, conductivity, %, pH, pressure, temperature; only the name separates `Conc B` from `Conc Q1-Q4` or system from sample flow. Hence `curve_roles.py` |
| F20 | `.asc`: a time-based export is the stored data (`t0`, then `t0 + (i+1)*step`, 7 digits); a volume-based export is resampled, not raw; events on the method axis |
| F21 | The 7.3 time and 7.11 volume `.asc` exports zero at injection 1 with no stored setting; `open_run()` does the same (`source = "default"`, warned); 7.11 also stores it in `UISettings` |
| F22 | `.UFol` = uncompressed POSIX tar of `<folder>/<name>.<original id>.Result` zips plus `systemnames.xml`; `<file>.UFol.export.txt` gives each original path and id |
| F23 | Time-grid evaluation curves (e.g. `mAU/cm` ratio curves) can store amplitudes only with no volume axis; not in `to_res3_dict()` |
| F24 | An `.asc` holds only the curves picked in UNICORN; they match the F20 rule, zeroed by the layout's setting |

### Database facts

- UNICORN databases can sit at compatibility level 100: write SQL for it (`TRY_CAST`,
  `CONCAT`, `IIF`, `SWITCHOFFSET`, `OFFSET/FETCH` work; `TRY_CONVERT`, `STRING_SPLIT` do not).
  The default CTE's folder filter is a `LIKE` on the comma list. Check each level in
  `SchemaProfile`.
- `BinaryCurvePoints` and the `ResultAuditTrail` `*Data` blobs are byte-identical to the bundle
  entries, so the ZIP decoders read database rows. `ChromatogramPos` = `CurveNumber`. A
  `.UFol` `original_id` is the `ResultID`. Run information is `dbo.ResultRunInformation`.
- Stored `BinaryCurvePoints` differ even between copies in one database (re-written on save):
  blob hashes can't identify copies; compare decoded values.
- `Result.Created` is reset on import; `RunStartDate` survives (always `MM/DD/YYYY hh:mm:ss
  +HH:MM`, month first), present on almost every result. `Result.LastModified` is never NULL.
- `ResultSearchCriteria`: one row per variable (`SearchCriteriaName`, `KeyWord1` name,
  `KeyWord2` text value, `ExtraDisplayInformation` unit); index
  `IX_ResultSearchCriteria_ResultID (ResultID, KeyWord1)`. Kinds include `VariableValue`,
  `ScoutingVariableValue` and `QuestionAnswer`; most values, not all, parse as numbers.
  Repeated variables get `_1` suffixes. `number`
  matches SQL Server's `TRY_CAST(... AS float)` except empty text (NaN here, 0 there): SQL
  uses `TRY_CAST(NULLIF(LTRIM(RTRIM(KeyWord2)), '') AS float)`. Empty stored text is `''`.
- The method name exists only inside `MethodData` (about 10 KB per result).
- Copies: evaluated copies under new names exist within a database (`dedupe=True` keeps one);
  results imported from elsewhere get a new `Result.Created`. Some exports store
  `Chromatogram.Created` without fractions (up to about 1 s early), hence whole seconds in the
  key.
- Vc: some results have no usable Vc source (caller `column_volume=`). `ColumnTypeData` holds
  geometry (units cm, cm, um) when the run names a column; pi*d^2/4*L agrees with Vc within
  0.5 % where checked. `TotalLiquidVolume` is kept as a hint; no porosity is derived from it.
  `ColumnInformation` often says only `Any`. Not every result has a sample flow curve. The
  column library is linked by name only and not read; `ColumnIndividual*` can be empty.
- Folders hold results at several depths; hidden folders exist. A result can have several
  chromatograms, some copied in from other results.
- Stored extinction coefficients (`Peak.ExtCoeff`, `Pool.ExtCoeff`,
  `ProteinInfo.ExtinctionCoefficient`) are usually empty: take epsilon from the caller.
- Units seen: `min`, `ml`, `mAU`, `mAU/cm`, `mS/cm`, `%`, `MPa`, `°C`
  (stored correctly; garbled only when cp1252 is read as UTF-8), `cm/h`, `ml/min`, `CV/h`, `cm`,
  `Ratio`, pH with no unit. UV paths are mm in metadata, cm in curve data.

### Run log and timeline facts

- UNICORN stops elapsed time without logging a `Pause` during washes (`System`, `Fraction
  collector`, `Accumulator`, `Pump wash`: minutes), pump synchronization (seconds) and alarms /
  errors until dealt with; it logs only the `Continue` (or the run ends). Without this, runs
  end minutes off their logged End; with it, within about 0.5 s.
- Log texts are cut to whole seconds; Pause texts sit within +-1.5 s of the prediction.
  A few runs keep unexplained gaps (warned). An alarm refusing a Continue was a case to fix
  (the next Continue had been dropped).
- A manual dated `Hold <date>` (not `Hold until ...`) keeps flow running (flow instructions act
  at once) but stops %-ramps; %B / quaternary instructions issued during it act at the
  `Continue`. A `Pause` during a Hold resumes into the Hold; a `Hold` during a `Pause` takes
  over, and the next `Continue` ends it. Instructions logged at a pause's time act after the
  pause section.
- A setting with no instruction yet (flow, sample flow, %B) is the median of its curve over
  that interval (`source='curve'`, warned above 30 s); no curve and no instruction: 0.
- Manual commands are common: `Next breakpoint`, `System flow`, `Gradient`, `Pause` /
  `Continue` / `Hold` / `End`, `Injection valve`, `Column position`.
- Run-log sections rebuilt from what ran match the %B and Q1-Q4 curves; flow warnings that
  remain are real (pressure control in `PathSet` blocks, pump ramps). %B before the first
  `Gradient` comes from the curve; some runs have no Conc B curve and no early `Gradient`.
- Conc Q curves are sampled staggered every 2 s: checked 6 s from section edges.
- The UTC start of U7.11 exports and imported database runs sits on a whole second (up to 1 s
  early); noted (review w), the parity test ignores it.

### Method and injection facts

- Injection gap (settled with the owner, don't reopen): the ~0.08 ml planned-vs-executed
  shortfall is the pre-switch gap, not a dead volume or an instrument setting. An Inject block
  counts its length from block start, but the valve turns 1-3 s later while the previous flow
  runs (measured median 2.7 s on an AKTA avant). Gap about 0.004 ml at 0.35 ml/min, 0.06-0.08 ml at
  5 ml/min, 0.16 ml at 15 ml/min. Independent of the delay-volume settings (those are after
  the UV monitor). The feed starts at the logged `Injection valve Inject`
  (Completed), so no correction; use set-points, not flow-curve integrals (pump ramp-down).
- The phase dialog `mSampleVolumeForSuperloop` can be stale: it differs from the Inject
  block's `End_Block` breakpoint, and those runs follow the breakpoint. A stopped run can end
  with the valve at Inject.
- Sample application settings (Superloop size, loading by the sample pump) are not stored for
  many executed loop injections. BufferPro is parsed; scouting runs are common.
- Planned vs executed: injections and blocks can be cut by `Next breakpoint`, shortened by a
  watch, lengthened by a hold or pause, or differ unexplained; planned-0 injections can be
  purge pulses.
- Feed: `to_bypass` windows and direct injects of `buffer` occur (system tests); many
  direct-inject periods have no air-sensor or finalization entries.
- By-pass buffer readings: most runs have a buffer-only by-pass window, mostly a `Purge`
  before the first feed. Conductivity and pH at the by-pass often, not always, settle and
  match the outlet (outlet pH usually slightly lower).
- The AKTA avant help: "Fill loop with" = sample volume the pump puts into the loop
  (`mFillLoopWith`); "Empty loop with" = buffer volume to empty a capillary loop
  (`mEmptyLoopWith`); Superloop "Sample volume" = volume injected; "Finalize sample injection"
  pushes the flow-path sample onto the column with buffer. Still unclear: `mEmptyLoopWith`
  stays 1 while the "Empty loop with" breakpoint is 6.2.
- Delay volumes stored by the AKTA are after the UV monitor (monitor -> outlet valve,
  monitor -> frac, pH restrictor and cell); "Max flow during valve turn" is a flow limit, not a
  volume.

### CADET facts (X1-X24)

| # | Fact |
|---|---|
| X1 | Core 6 reads `GEOMETRY` on every column unit with no default (`AXIAL_FLOW_CYLINDER`, ...); the radial docs call it `COLUMN_GEOMETRY`, the code `GEOMETRY` |
| X2 | Core 6 has `COLUMN_MODEL_1D`; the old unit names are legacy; particle and binding fields live in `particle_type_ZZZ/` (`NPARTYPE`, `PAR_TYPE_VOLFRAC`) |
| X3 | CADET-Process 0.12 and its `dev` branch write the v5 layout: a `Process` won't run on Core 6 |
| X4 | `cadet-cli` reads JSON input as well as HDF5 |
| X5 | Inlet `PIECEWISE_CUBIC_POLY` with `CONST/LIN/QUAD/CUBE_COEFF` per section; flow is on `connections/switch_XXX/CONNECTIONS` rows (`Q0..Q3`, m^3/s) |
| X6 | `solver/sections`: `NSEC`, `SECTION_TIMES` (NSEC+1), `SECTION_CONTINUITY` (NSEC-1); `USER_SOLUTION_TIMES` in s |
| X7 | Outlet output is split per component unless `SPLIT_COMPONENTS_DATA=0`, `SPLIT_PORTS_DATA=0` |
| X8 | External functions (`LINEAR_INTERP_DATA`) can feed measured pH / temperature into `EXT_` binding models |
| X9 | `ReferenceIO(name, time, solution, ...)`: `solution` must be an ndarray `(n_t, n_comp)` |
| X10 | `Fraction(mass, volume, start, end)` -> `FractionationReference`; only `FractionationSSE` uses it |
| X11 | `Process.add_concentration_profile` / `add_flow_rate_profile` raise if time > `cycle_time` |
| X12 | No detector, UV, extinction or calibration model in Core or Process |
| X13 | CADET-Process has no file format for references or processes; examples read two-column CSV with no header |
| X14 | `SolutionIO.smooth_data` fails and `Comparator.add_reference(smooth=True)` drops the smoothed copy: don't rely on them |
| X15 | Everything in CADET is SI |
| X16 | Core 5 layout: `GENERAL_RATE_MODEL` etc., particle fields and `adsorption` directly on `unit_XXX`, no `GEOMETRY` / `particle_type_*` |
| X17 | Core 6 doesn't read a v5 file, and a file mixing both layouts is rejected: one file per target |
| X18 | Shared between v5 and v6: inlet, outlet, connections, sections, `USER_SOLUTION_TIMES`, return, `CROSS_SECTION_AREA`, `COL_POROSITY`, `COL_DISPERSION`. Not the bed length: v5 `COL_LENGTH`, v6 `BED_LENGTH` |
| X19 | Particle names v5 / v6: `PAR_DIFFUSION` / `PORE_DIFFUSION`; `PAR_SURFDIFFUSION` / `SURFACE_DIFFUSION` + `HAS_SURFACE_DIFFUSION`; `NPAR` / `NCELLS` (FV) or `PAR_NELEM`, `PAR_POLYDEG` (DG). v6 needs `particle_type_000/HAS_FILM_DIFFUSION`; the `HAS_*` flags pick the particle model; DG reads `NELEM` / `POLYDEG` |
| X20 | Switches: at a discontinuous section change both Cores take `section % NSWITCHES` and step at most one switch; continuous boundaries aren't checked. With fewer switches than sections later switches never fire, silently (the feed never reached the column). Write one switch per section and `SECTION_CONTINUITY` all 0, as CADET-Process does |
| X21 | Core 6.0.0a5 requires `FORWARD_FLOW` on the column (1 = forward); Core 5 ignores it |
| X22 | A CSTR with `NBOUND` all 0 makes both Cores throw ("Particle type 0 does not have a bound state"); leave `NBOUND` out |
| X23 | `cadet-cli` 5.1.2 also reads JSON; output must be `.h5` on both. Q = 0 rows, two inlets into one unit and `ADSORPTION_MODEL = NONE` without an `adsorption` group run on both; each Core rejects the other layout |
| X24 | CADET-Process metrics slice reference and simulation by component name: re-label a one-component reference as the simulated species. Its `Shape` metric warns about NaN / Inf from inside the metric on the walkthrough run |

Other gotchas:

- Mass balances: integrate on a fine grid with the section flows; the solver output grid has
  almost no points in pauses. The outlet reports 0 while Q = 0.
- Versions (checked 2026-11-08, nothing newer since): CADET-Core 6.0.0a5 (pip `CADET-Core`,
  import `cadet_core`, ships `cadet-cli` only), 5.1.2 (2026-10-30, patch over 5.1.1),
  CADET-Process 0.12.0 (needs `cadet-python>=1.0.4`; `dev` still without Core 6), CADET-Python
  1.3.0. Core `master` has about 50 commits after a5 with no change to fields pycorn writes.
  Re-check releases before Core-input work. Pin target versions in `/meta`.
- `Cadet(install_path=)` is the environment root; `pip install pycorn[cadet]` installs no
  simulator (`pip install CADET-Core==5.1.2`). `PAR_SURFDIFFUSION` isn't needed on 5.1.2.
- Core 6 is an alpha: field names may change; the v6 column writer is one function. The v5
  writer is kept small so it can be dropped when CADET-Process supports Core 6.
- Run-log wording may vary by UNICORN version and language: match event types and `{unit}`
  tokens, keep raw text, warn on unknown instructions.

## Review 2026-11-08 (a-z)

Every route was run end to end: zips, `.UFol`s, databases (incl. a multi-chromatogram run),
`pycorn export`, a fresh non-editable install, CADET-Core 5.1.2 / 6.0.0a5 and CADET-Process
0.12. Checked and fine: full axes on every member; zip / UFol / database parity; search paging
and filters; `match_files`; every EXPORT_GUIDE file and column; the walkthrough and simulated
runs;
hand-checked UV, conductivity, flow, area, mixer, fraction mass and salt conversions.

| Item | Finding | Status |
|---|---|---|
| a | `normalized(uv_path_length_cm=)` left `uv_info`, so concentration / CADET divided by the path again | Fixed |
| b | CADET export crashed on imported chromatograms (no run-log sections) | Fixed: clear `ValueError`; `pycorn export` writes "CADET export not written" |
| c | `export --update` stored the search CSV's old `tstamp` | Fixed (`loaded_stamp`) |
| d | `find(limit=0)` raised `TypeError` | Fixed: 1-5000 checked first |
| e | `pick_list("variable")` `n_numeric` counted rows | Fixed: distinct results |
| f | `time_zero='injection'` didn't shift `exp.hints` | Fixed |
| g | `read_fraction_masses` took a `Path` as rows | Fixed |
| h | The mixer stops during a column by-pass | Fixed: M2 (owner decision, same day) |
| i | Column and CSTRs start empty (`INIT_C` 0) | Open: M1 (documented in CADET_GUIDE, Known limitations) |
| j | No loop volume on the export path | Open: M3 |
| k | Sample pump inlet named 'Buffer' gives no feed | Open: M4 |
| l | Paused sections in an Inject period are feed with Q = 0; pseudo 'Waste' fraction rows | Open: M5 |
| m | Core 5 vs 6 differ by 8.2 % on one run | Open: M6 |
| n | pandas a hidden core dependency; setup and requirements faults | Fixed: pandas in `install_requires`, `python_requires`, no `distutils`, `processing` extra kept empty, `sql/requirements.txt` fixed |
| o | `--cadet-core` said "written" when the Core file wasn't | Fixed: run `incomplete`, exit 1 (`missing_outputs`) |
| p | `epsilon_from_injection` gave an epsilon from no signal | Fixed: warns below `NO_SIGNAL_AU` |
| q | Two `epsilon` shapes; API messages named CLI flags | Fixed: shapes checked with an example; messages name flag and keyword |
| r | Raw errors on bad input; silent unknown `--var`; undocumented filters | Fixed: clear errors, `search.FILTERS` / `check_filters`, close names |
| s | `sources=` forms differed; `load_run` didn't name the database searched | Fixed |
| t | Duplicate warnings | Fixed: each printed once; missing databases named by position |
| u | Placeholder names weren't CADET-Core fields; misspelt overrides silent | Fixed: `/meta/PLACEHOLDER_PATHS`; overrides adding new fields listed (names still unchecked, D28) |
| v | CSVs without BOM (`°C` garbled in Excel) | Fixed: UTF-8 with BOM (signal CSVs unchanged) |
| w | Whole-second starts and a curve with no time axis, unwarned | Fixed: noted (UTC category) |
| x | `pycorn-bin` silent without optional packages; small CLI issues | Fixed (except the `xUV cell path length` name) |
| y | CADET_GUIDE gaps | Fixed |
| z | README, USAGE_pycorn_module, EXPORT_GUIDE, CONNECTOR_GUIDE, PROJECT_SUMMARY gaps | Fixed |

Also fixed then: CADET `buffers=` keyed by inlet position (see "Other CADET decisions").
