# `samples/sample.zip`: Contents and PyCORN Coverage

Reference for what a UNICORN 6+ result bundle contains and what `PcUni6`
(`pycorn/pycorn.py`) does with each part. Built by listing the zip and tracing
`PcUni6.load()` / `load_all_xml()` on this sample (UNICORN 7.3, AKTA pure 25).
Generated 2026-11-07. Updated 2026-11-07 after the decoder fix (point counts below are the
full arrays) and the addition of `open_run()`, which keeps every entry (see the end).

## Legend

| Mark | Meaning |
|------|---------|
| **USED** | Decoded and present in the final `load_all_xml()` output |
| **DECODED, DISCARDED** | `load()` decodes it, but `load_all_xml()` → `clean_up()` deletes it |
| **RAW, DISCARDED** | Kept as bytes by `load()`, then deleted by `clean_up()` |
| **SKIPPED** | Present in the file but never read |

## How the bundle is built

- The outer zip has 39 entries: 4 plain XML files, 34 nested zips and 1 empty file.
- Each nested zip holds pairs of `<name>DataType` (a .NET type name such as `System.String`
  or `System.Single[]`) and `<name>` (a .NET BinaryFormatter blob).
- The blobs are MS-NRBF: a 17-byte header record, one value record, and a 1-byte
  MessageEnd (`0x0B`). Decoded by `pycorn/bundle_codecs.py` (`decode_nrbf`):
  - **Strings:** record `0x06`, a 7-bit varint length, then UTF-8. Empty strings
    (24-byte blobs) become `None` in `PcUni6`.
  - **Float arrays:** record `0x0F` with an element count and primitive type `0x0B`
    (Single), so the data is `[27:-1]`. Until 2026-11-07, `PcUni6._unpacker` sliced
    `inp[47:-49]`, which **dropped the first 5 and last 12 points of every curve**.
  - **Padded zips:** inner zips have trailing null bytes (Python issue 24621), trimmed
    at the end-of-central-directory record.
- **Which entries get deleted:** `clean_up()` removes every file named in `Manifest.xml`
  (37 entries, i.e. everything except itself) and then `Manifest.xml` too. After
  `load_all_xml()`, only two keys remain:
  - `Chrom.1`: the curves and events, in a res3-like layout
  - `Chrom.1.Xml_dict`: the whole chromatogram XML as a dict

## Entry-by-entry

### Chromatogram

| Entry | Size | Format | What it is | PyCORN |
|-------|------|--------|------------|--------|
| `Chrom.1.Xml` | 118,596 | plain XML | Chromatogram definition: curve metadata, events, peak tables | **USED** (curves and events go into `Chrom.1`; the full XML is kept as `Chrom.1.Xml_dict`). The raw bytes are discarded. |
| `Chrom.1_<N>_True` ×20 | 11–176 KB | zip: `CoordinateData.Volumes` + `CoordinateData.Amplitudes` (`System.Single[]`) | One curve's x (volume) / y values | **USED**: decoded to float lists, zipped into `(volume, value)` pairs under `Chrom.1[<curve name>]['data']`. The standalone `Chrom.1_<N>_True` keys are then discarded. |

The 20 curves and the files they come from. All 20 binaries are referenced; none are orphaned.

| Curve | Type | Unit | Points | File |
|-------|------|------|--------|------|
| UV 1_280 | UV | mAU | 22,597 | `Chrom.1_1_True` |
| UV 2_295 | UV | mAU | 4,520 | `Chrom.1_2_True` |
| UV 3_0 | UV | mAU | 4,520 | `Chrom.1_3_True` |
| Cond | Conduction | mS/cm | 11,298 | `Chrom.1_4_True` |
| % Cond | Conduction | % | 2,260 | `Chrom.1_5_True` |
| Conc B | Other | % | 2,260 | `Chrom.1_6_True` |
| System flow | Other | ml/min | 2,260 | `Chrom.1_7_True` |
| System linear flow | Other | cm/h | 2,260 | `Chrom.1_8_True` |
| PreC pressure | Pressure | MPa | 2,260 | `Chrom.1_9_True` |
| DeltaC pressure | Pressure | MPa | 2,260 | `Chrom.1_10_True` |
| Sample flow | Other | ml/min | 2,260 | `Chrom.1_13_True` |
| Sample linear flow | Other | cm/h | 2,260 | `Chrom.1_14_True` |
| Sample pressure | Pressure | MPa | 2,259 | `Chrom.1_15_True` |
| pH | pH | (none) | 2,260 | `Chrom.1_16_True` |
| System pressure | Pressure | MPa | 22,597 | `Chrom.1_20_True` |
| PostC pressure | Pressure | MPa | 2,260 | `Chrom.1_21_True` |
| Cond temp | Temperature | °C | 1,129 | `Chrom.1_23_True` |
| UV cell path length | Other | cm | 2,260 | `Chrom.1_38_True` (renamed `xUV cell path length` so pycorn-bin skips it) |
| Sample flow (CV/h) | Other | CV/h | 2,260 | `Chrom.1_89_True` |
| System flow (CV/h) | Other | CV/h | 2,260 | `Chrom.1_90_True` |

The `°C` unit is stored as UTF-8 and decodes correctly. It only shows as `�C` when
printed to a cp1252 Windows console.

### Inside `Chrom.1.Xml`

| Element | PyCORN |
|---------|--------|
| `Curves` (20) → `Name`, `CurveDataType`, `AmplitudeUnit`, first `CurvePoints` file name | **USED** |
| `Curves` → `TimeUnit`, `VolumeUnit`, `ScanInterval`, `DistanceBetweenPoints`, `MethodStartTime`, `ColumnVolume`, `IsOriginalData`, `CompressionDesc`, `CurveUVInfo`, `GuidePoints`, … | **SKIPPED** in `Chrom.1` (only in `Chrom.1.Xml_dict`) |
| `Curves` → `CurvePoints` beyond the first set | **SKIPPED** (only `[0]` is read; this sample has one set per curve) |
| `EventCurves` → Fraction (2), Injection (1), Run Log (85): `EventVolume`, `EventText` | **USED** as `(volume, text)` pairs. `Fraction` is renamed `Fractions`. |
| `EventCurves` → `EventTime`, `InstructionFeedback` | **SKIPPED** |
| `EventCurves` with `IsOriginalData=false` | **SKIPPED** (prints "not added"; none in this sample) |
| `PeakTables` (1 table, 20 peaks: area, height, retention, width, asymmetry, conductivity, …) | **SKIPPED** in `Chrom.1` (only in `Chrom.1.Xml_dict`) |
| `PoolTables` (empty), `ChromatogramName`, `Created`, `TimeUnit`, `VolumeUnit`, `ChromatogramLayoutXml`, `VisualAppearance`, `ChromatogramID` | **SKIPPED** in `Chrom.1` (only in `Chrom.1.Xml_dict`) |

**Note:** curve data is volume-based only. Time axes are not built, even though
`TimeUnit`/`EventTime` are present.

### Run metadata (plain XML)

| Entry | Size | What it is | PyCORN |
|-------|------|------------|--------|
| `Result.xml` | 8,428 | Result record: state, system name/type, run name, batch, folder path, run info, `Created` | **RAW, DISCARDED**. `PcUni6.date` reads it; it now keeps a copy, so `date` also works after `load_all_xml()`. |
| `EvaluationLog.xml` | 7,265 | Audit trail of evaluation steps | **RAW, DISCARDED** |
| `Manifest.xml` | 4,579 | Export manifest listing the other 37 entries | Used by `clean_up()` to decide what to delete, then **RAW, DISCARDED** |

### Run metadata (nested zip, `XmlDataType` + `Xml`)

| Entry | Inner XML size | What it is | PyCORN |
|-------|---------------:|------------|--------|
| `MethodData` | 147,545 | Full method (blocks, instructions, settings) | **DECODED, DISCARDED** |
| `SystemData` | 21,381 | System/instrument components | **DECODED, DISCARDED** |
| `SystemSettingData` | 11,960 | System settings | **DECODED, DISCARDED** |
| `StrategyData` | 11,708 | Instrument strategy name/version | **DECODED, DISCARDED** |
| `CalibrationSettingData` | 2,006 | Calibration settings | **DECODED, DISCARDED** |
| `InstrumentConfigurationData` | 1,541 | Instrument configuration description | **DECODED, DISCARDED** |
| `MethodDocumentationData` | 1,096 | Method notes/documentation | **DECODED, DISCARDED** |
| `NextBufferPrepData` | 142 | Buffer prep settings for the next run | **DECODED, DISCARDED** |
| `EvaluationProcedureData` | 120 | Evaluation procedures | **DECODED, DISCARDED** |
| `ColumnTypeData` | 93 | Column type definitions (empty list here) | **DECODED, DISCARDED** |
| `ColumnIndividualData` | 24 | Individual column data (empty) | `None`, then discarded |
| `ReportFormatData` | 24 | Report format (empty) | `None`, then discarded |
| `VersionInformationData` | 24 | Version info (empty) | `None`, then discarded |

### Other

| Entry | Size | PyCORN |
|-------|------|--------|
| `NextFracData` | 0 (empty) | Not a zip, so it stays as `b''`. **RAW, DISCARDED**. With `load(print_log=True)` it is listed under "Not supported". |

## Summary

**Extracted:** all 20 sensor curves as `(volume, value)` pairs with name, type and unit,
plus the Fraction, Injection and Run Log events as `(volume, text)` pairs.

**Not extracted into the res3-like output:**
- the peak table (20 integrated peaks)
- all time-based data
- curve acquisition details (scan interval, column volume, UV info)
- run metadata (`Result.xml`)
- method, system, strategy, calibration and instrument configuration
- the evaluation audit log

To get the metadata, call `load()` without `load_all_xml()`. The decoded dicts are
then available under each entry name (e.g. `u["MethodData"]["Xml"]`).
`Chrom.1.Xml_dict` keeps the peak table and curve details either way.

## Additional Findings (2026-11-07; facts F1-F24 in [`docs/ROADMAP.md`](../docs/ROADMAP.md))

These were checked while planning time/CV/UTC support. The plan lists them as facts F1-F13.

- **Time grid:** every curve has `IsoChroneType = Time`. Point i is at
  `DistanceToStartPoint + i * DistanceBetweenPoints` (min). The inner zips store only
  volumes and amplitudes, so time has to be built from these fields. With the full arrays,
  every curve ends at 37.63-37.66 min, i.e. at the run-log `End` (37.6617), and every
  volume axis ends at 15.770 ml, the final `Waste` fraction event.
- **Column volume:** stored in several places that agree on 0.7 ml: `ColumnVolume`
  on each curve/event curve, the run-log event `Base CV, Vc=0.700 {ml}, Any`, the ratio
  `System flow` / `System flow (CV/h)`, and the peak table. `MethodData` has
  `mColumnVolume = 0.353`, which disagrees, so it isn't reliable.
- **`ResultRunInformation`** (in `Result.xml`, base64): 1) UTF-16 XML `usedColumns`
  (`Any`, 0.7 ml), 2) UNICORN version `7.3.0.473`, 3) run start
  `03/10/2023 16:30:07 -05:00`.
- **UV path length:** `CurveUVInfo` has `IsUVValuesNormalizedToNominalUVPathLength = true`,
  `UVPathLength = 2.079998`, `NominalUVPathLength = 2` (no unit; presumably mm). The
  `UV cell path length` curve stores 0.2 **cm**.
- **Wall clock:** `MethodStartTime` 2023-03-10T16:30:07.838 with
  `MethodStartTimeUtcOffsetMinutes = -300`. Run log: `Method Run 3/10/2023 4:30:07 PM -05:00`
  at t=0 and `End 3/10/2023 5:07:47 PM -05:00` at t=37.6617 min. Start + elapsed
  matches End, so the run has no pauses. There are no Pause/Hold events in this sample.
- `EvaluationLog.xml` holds post-run evaluation actions, not run events.

## Issues Found While Tracing (all resolved 2026-11-07)

1. `PcUni6._unpacker` dropped the first 5 and last 12 points of every curve (fixed-offset
   slicing). Fixed: arrays are decoded from their MS-NRBF header.
2. `load_all_xml()` deleted the metadata, which broke `PcUni6.date`. Fixed: `date` keeps
   a copy of `Result.xml`; `load_all_xml(keep_all=True)` deletes nothing; `clean_up()` can
   be called twice (`pycorn-bin` does).
3. The `Cond temp` unit: not a bug (console display only, see above).
4. `utils.import_xml_as_df()` returned `None` for this sample. It only accepted chromatogram
   keys containing `"Cond"` and an experiment keyword. Fixed: if none match, it uses every
   chromatogram with UV curves. `np.NaN` (removed in numpy 2) is now `np.nan`.
5. `import_xml_as_df()` zeroed the axis at the *last* injection and filled index points
   outside the data with the first/last value. Fixed: it uses the injection UNICORN uses
   (see below), leaves points outside the data as NaN, and has `axis="time"`.

## Curve precision

Each curve has `AmplitudePrecision` (decimals UNICORN displays), exposed as
`Curve.precision`: 3 for UV, pressures, system flow and CV/h; 2 for Cond, pH and sample
linear flow; 1 for % Cond, Conc B, system linear flow, Cond temp and UV cell path length.
The stored float32 values are **not** rounded (UV values carry digits beyond 3 decimals),
and UNICORN's peak table is computed from the unrounded values (heights such as
2.146412). There is no precision for the time/volume axes in the chromatogram;
`MethodData` has `NormalFlowUnitNumberOfDecimals` / `NormalPressureUnitNumberOfDecimals`.

## Matching UNICORN's x axis (injection-adjusted)

UNICORN plots volume (or time) relative to an injection, so data before it is negative.
The setting is in the chromatogram layout (`ChromatogramLayoutXml` -> `Axis/AxisScales/
XAxisScale`): `AdjustRetentionZeroToInjectionNumber = true`,
`InjectionNumberForRetentionAdjustment = 1`. Injection 1 is at 13.57932 ml / 6.365 min,
so method start is at -13.57932 ml. Each curve links to its injection event curve
(`InjectionEventCurve/CurveNumber` = 12).

- `Chromatogram.retention_zero` records the zero and its source (`layout`, else the peak
  table's `ZeroAdjustedToInjectionNumber`, else `none`). Curves and events get
  `volume_adjusted` / `time_adjusted`; the raw axes are unchanged.
- **Check against UNICORN's peak table** (same frame, `ZeroAdjustedToInjectionNumber = 1`):
  every `MaxPeakRetention` is within one point spacing of a stored point, and the raw curve
  interpolated at that position equals `Height` within 0.01% for all 20 peaks. UNICORN puts
  the maximum between stored points, so its position is not always the raw argmax.
- **Pre-flow setup points:** for the first ~0.04 min the flow is 0 and the volume stays 0
  (28 UV 1_280 points, 3 on the 1 s curves). They all sit at -13.57932 ml, where UNICORN
  draws a vertical segment. `open_run()` and `PcUni6` keep them; on a volume index
  `import_xml_as_df` can keep only the first, while `axis="time"` keeps them all.

## Full Contents: `open_run()`

`pycorn.open_run("sample.zip")` returns a `Run` (`pycorn/model.py`,
`pycorn/sources/zip_source.py`) that keeps everything above:

| Field | Contents |
|-------|----------|
| `run.entries` | Every outer entry and nested-zip member: raw bytes, kind, .NET type, decoded value |
| `run.result` | `Result.xml`, plus decoded `run_information` (base64) and `search_criteria` |
| `run.chromatograms["Chrom.1"]` | `curves` (time, volume and their injection-adjusted versions, values, all point sets, units, Vc, UV info, full XML), `event_curves` (same axes, text, type, subtype), `peak_tables`, `pool_tables`, `retention_zero`, `info` (layout and appearance XML parsed) |
| `run.metadata` | Every `*Data` entry (method, system, strategy, calibration, ...), parsed |
| `run.evaluation_log`, `run.manifest` | The audit trail and the export manifest |
| `run.warnings` | Anything that could not be decoded or placed (empty for this sample) |

`run.to_res3_dict()` returns the same dict as `PcUni6.load_all_xml()`.

## Other Bundles: UNICORN 7.3 vs 7.11 (2026-11-07)

Tested against a lab environment: results from 7.3.0.473 and 7.11.0.1097, some runs exported
from both versions (the 7.11 copies are the same results re-saved and evaluated in 7.11).

- **Raw data is unchanged between versions.** For the paired runs, every original
  curve (volume, values, time), unit, precision and event is identical.
- **`NextFracData` can be a PNG** (820x350), null-padded to 128 KiB; empty in `sample.zip`.
  `open_run()` stores it as kind `png` with the padding removed from `value`.
- **Volume-grid curves:** evaluation in 7.11 adds a derived baseline curve
  (`DerivedCurveOrigin/CurveOperation = Baseline`, `IsOriginalData = false`,
  `IsoChroneType = Volume`). Its point file holds only `CoordinateData.Amplitudes`; point i
  is at `DistanceToStartPoint + i * DistanceBetweenPoints` ml on the method (unadjusted)
  axis, spanning the integration window. `open_run()` builds `volume` from that grid and
  leaves `time` as `None`.
- **New in 7.11** (kept in `xml`/`info`, no typed attributes yet):
  - Chromatogram: `AnalysisSettings` (integration window, baseline algorithm, column
    `V0`/`Vt`/bed height, `ZeroAtInjection`, `DistanceToZeroPointInTime/Volume`),
    `UISettings` (curve layouts, axis scales, peak table columns), `OriginalResultName`.
  - `UISettings/.../ScaleSettings/XAxis`: `IsAdjustToZeroAtInjectionSelected` ('True'/'False'
    text) and `AdjustRetentionToZeroAtInjection`. `open_run()` uses these when the layout
    has no setting (`retention_zero["source"] = "ui_settings"`); they agree with the layout
    in every sample. Zero-at-phase (`IsAdjustToZeroAtPhaseSelected`) is not supported and
    gives a warning.
  - Curves: `DerivedCurveOrigin`, `OriginalResultName`. Events: `MarkValue`, type
    `Manual/BlockEnd`.
  - Peak tables: `BaseLine/CurveNumber`, `ColumnHeight`, `ColumnVt`; peaks add
    `CapacityFactor`, `PlateHeight`, `PlatesPerMeter`, `SmallH`, start/end/max baseline heights.
  - Metadata: `MethodData` `ScoutingRuns/ScoutingRun` fields, `StrategyData` `HasFracF9T`.
- **No retention-zero setting:** the 7.3 results and the native 7.11 run have no layout
  setting, UI settings or peak table. `open_run()` then zeroes the adjusted axes at
  injection 1, as UNICORN's text export does (`source = "default"`, with a warning; see below).

## Check Against UNICORN's Text Export (.asc, 2026-11-07)

Tested against a lab environment on three `.asc` exports and their bundles (7.3 in min;
7.11 in ml; 7.11 in min). Tab-separated: row 1 chromatogram, row 2 curve names, row 3 x/y units,
then one x/y column pair per curve, each with its own length. Values are printed with
7 significant digits, x with 3 decimals. Tests: `test_asc_*` in `tests/test_zip_source.py`.

- **An export holds the curves picked in UNICORN**, not necessarily all: the 7.11 time
  export has 7 original curves and no event curves. Derived curves were never exported.
- **Events** (Fraction, Injection, Run Log) match `open_run()` exactly and are on the
  method axis (not retention-adjusted) in both exports that include them.
- **Time-based export = the stored data.** Every exported curve (20 in 7.3, 7 in 7.11) is
  reproduced to print precision by: a leading point `(t0, y0)`, then sample i at
  `t0 + (i+1)*step`, with the x axis zeroed at the first injection. The last one or two
  samples may be missing. In 7.11 the zero comes from the stored layout setting
  (`source = "layout"`), in 7.3 from the injection-1 default.
  The extra step is how the export writes points: within the bundle, `t0 + i*step` is
  right. With it, stored volumes match the run-log (time, volume) pairs to ~1e-6 ml in
  every sample; with `+1 step` they are off by up to 0.05 ml.
- **Volume-based export is resampled, not raw.** Each curve keeps its point count but is
  put on an evenly spaced volume grid (`spacing = step * V_end / T_end`, starting one spacing
  after 0, zeroed at injection), as if the flow had been constant. Values are close to the
  stored curve at that volume but not reproducible exactly. Use a time-based export to
  check raw data.
- **Retention zero:** the first two exports zero the curves at injection 1, although neither
  bundle stores a retention setting. `open_run()` does the same (`source = "default"`); the 7.3
  test checks our `time_adjusted` against the export directly.
- **As stored:** `open_run(path, adjust_axes=False)` (also `import_xml_as_df(...,
  adjust_axes=False)`) skips every adjustment: the `*_adjusted` axes equal the stored ones
  and `source = "disabled"`. The stored `time`/`volume` axes never depend on the flag.

## Folder Exports (.UFol, 2026-11-07)

UNICORN's folder export (7.11.0.1097, tested against a lab environment) is an **uncompressed POSIX tar**, not a zip. It holds the exported folder tree:

```
<Folder>/                                  directory entry (subfolders the same way)
<Folder>/<Result name>.<original id>.Result
systemnames.xml                            .NET ArrayOfString of system names (empty here)
```

- **Each `.Result` is an ordinary result bundle**, byte-for-byte the same kind of zip as a
  `.zip` export (same entries; only the curve set differs by instrument and method).
- **`<original id>`** is the result's id in the source database.
- UNICORN writes `<file>.UFol.export.txt` next to it: export date and host, then one
  tab-separated line per folder/result with `Original path` (e.g.
  `/DefaultHome/<Folder>/<Result>`), `Exported path` and `Original id`.

`pycorn.open_folder(path)` reads every `.Result` in memory (nothing is extracted to disk)
with the same code as `open_run()`, and returns a list of `Run`s with `source = "ufol"` and
`run.container` = `member`, `folder`, `name`, `original_id` and `original_path` (from the
summary, `None` if it is missing). A `.Result` taken out of the tar opens with `open_run()`.
Tests: `tests/test_ufol_source.py`.

**Time-grid curves with amplitudes only.** One result has `UV 3_280@0339,DIV`, a UV ratio
curve made in evaluation: `CurveDataType = UV`, unit `mAU/cm`, `IsOriginalData = false`,
`IsoChroneType = Time`, empty `VolumeUnit`, and a point file holding only
`CoordinateData.Amplitudes`. Point i is at `DistanceToStartPoint + i * DistanceBetweenPoints`
min. `open_run()` builds `time` from that grid and leaves `volume` as `None` (the mirror of
the volume-grid baselines above); `to_res3_dict()` leaves it out, as it does every curve
without a volume axis. Being derived, it is not returned by the `uv` role unless
`original_only=False`; its `mAU/cm` unit (UV divided by `@0339`, presumably the path
length curve) is the way to tell it from absorbance.

## Samples for the run structure review (2026-11-09)

Added for `docs/RUN_STRUCTURE_REVIEW.md` section 6, picked from a lab environment
(UNICORN 7.11). Sanitized with `tools/anonymize_zip.py` (person names
as `--token`), then searched again in every nested member for names, logins and result names.
`u711_column_test.zip` is the ColumnTest result that scan also picked (same 27 curves); it
was already here.

| File | Run | What it covers |
|---|---|---|
| `u711_manual_run.zip` | A manual run: no phases, no method | `Inlet B` logged (B2, B3, B4); sample pump (`Sample flow 10`, `Sample inlet S3`, valve at Inject); system flow 10 -> 5 -> 10 ml/min at 516 s and 570 s; seven Pauses; %B steps (100, 50, 0). No Vc stored. |
| `u711_proa_day12.zip` | The real run behind `u73_blank.zip` (same phases and blocks) | Vc 5.027 ml stored (`Base CV, Vc=...`); loop inject; every `Gradient` has length 0 (steps), so **no ramp**; `Inlet B` never logged. |
| `u711_column_test.zip` | Column packing test, method run | Vc stored; flow changes at 117 s and 1025 s inside the one phase; loop inject. |

Not covered yet: a linear %B ramp together with a stored Vc, a ramp with a hold or second
slope, a direct inject. `RUN_STRUCTURE_REVIEW.md` section 6 still lists them.

## CADET reference run (2026-11-09)

`u711_cadet_reference.zip` (UNICORN 7.11, sanitized as above) is the run behind the
hand-checked CADET-Core files `tests/data/core_input_reference_v5.json` / `_v6.json`
(`tests/test_cadet_core_input.py`, `tests/test_cadet_core_run.py`). It links its column type,
so the Core export needs no caller geometry; 28 sections with pauses, the feed starting at
section 13.

## Identifying Curves (names are not a fixed set)

Names depend on the instrument and its configuration, and the samples here (lab-scale AKTA)
do not cover them all; process-scale systems use others (e.g. `UV_280` instead of `UV 1_280`).
Tested against a lab environment:

- **Curve numbers change between runs** (`System flow` is #7 or #8, `pH` #16 or #7). Use them
  only within one run (e.g. `InjectionEventCurve/CurveNumber`, peak table `DataCurve`).
- **`CurveDataType` is coarse but stable**: `UV`, `Conduction` (`Cond` and `% Cond`), `pH`,
  `Pressure`, `Temperature`, `Other` (flows, concentrations, counted volume, UV ratio and path
  length). Derived curves (e.g. baselines) keep their source's type with `IsOriginalData = false`;
  so does the evaluation curve `UV 3_280@0339,DIV` (`UV`, `mAU/cm`).
- **`EventCurveType`** is `Fraction`, `Injection` or `Logbook` in all samples.
- **UV wavelength is not stored per curve**; it appears only in the name (`UV 1_280`,
  `UV 2_0` = off) and in method instructions, which can change it during a run.

- **No stored field separates** `Conc B` from `Conc Q1`-`Q4` (all `Other`, `%`), system from
  sample flow (`Other`, `ml/min`), or one pressure sensor from another (`Pressure`, `MPa`).

In code, use roles (`pycorn/curve_roles.py`, `Chromatogram.curves_with_role()`), which match
on type and unit: `uv`, `conductivity` (`Conduction`, unit not `%`), `conductivity_percent`,
`ph`, `pressure`, `temperature`. Only `conc_b` needs a name pattern (case, spaces and
punctuation ignored: `Conc B`, `Conc_B`, `ConcB`, `Concentration B`, `%B`); extend
`curve_roles.ROLES` for names not seen yet. A role can match several curves (e.g. two
conductivity cells) and returns all of them. There is no flow role: system and sample flow
cannot be told apart without names. `import_xml_as_df` keys can be exact names, roles, or the
legacy `Cond`/`UV`/`Conc B`/`pH`, which stand for roles; anything unmatched raises a
`KeyError` listing the run's curves and the roles.
