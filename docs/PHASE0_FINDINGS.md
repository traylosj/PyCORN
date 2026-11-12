# Phase 0 Findings: UNICORN Database vs ZIP Exports

Read-only spike, 2026-11-07, tested against a lab environment (UNICORN databases with
`DB_Version` 1.7.4.0 and 1.8.6.0). Decisions and facts that came from it are in
[`docs/ROADMAP.md`](ROADMAP.md). This file keeps schema names, mappings, vocabularies and
rules. Every query was a `SELECT`.

## Summary

- **Reuse the ZIP decoders for the database.** `dbo.CurvePoint.BinaryCurvePoints` is
  **byte-identical** to the bundle's `Chrom.1_<N>_True` entry. `read_nested_zip` +
  `decode_nrbf` decode it unchanged.
- **ResultAuditTrail `*Data` columns are byte-identical** to the bundle's `MethodData`,
  `SystemData`, ... entries.
- **`ChromatogramPos` = `CurveNumber`**, for curves and for event curves.
- **Don't use `Result.Created` in the match key.** Results imported from another database
  get a new `Result.Created` (the import time) and a new `ChromatogramID`.
  `Chromatogram.Created` keeps the original value.
- **`.UFol` `original_id` is the database `ResultID`**, and `original_path` is the
  database folder path plus `/<result name>`.
- **Elapsed time stops during Pause but keeps running during Hold.** Pause, Hold,
  Continue, End and the run start are all timestamped in the run log. Continue and End
  also carry the UTC offset.
- Vc: `Curve.ColumnVolume` can be NULL (never 0 where seen); some results have no Vc on any
  curve.

## Schema

Discovery: `sys.databases` (`database_id > 4`) plus `HAS_DBACCESS`; a UNICORN database has
`Result`, `Chromatogram`, `Curve` and `CurvePoint`.

**Schema differences** (1.8.6.0 relative to 1.7.4.0):

- Tables only in 1.8.6.0: `AnalysisMetadata`, `AnalysisMetadataMap`, `AnalysisTemplate`,
  `DoEResult_FeatureData`, `EventMetaData`, `FracPlate`, `NextFractionCollector`.
- Extra columns in 1.8.6.0: `CurrentSessions`, `DB_Setting`,
  `InstrumentConfiguration_NextFracCassetteType`, `Integrate`,
  `MethodDocumentation_Reference_Curve`, `NextFracCassetteType`, `PasswordPolicy`,
  `ResultSetting`, `UserGroup`.
- **None of the tables the reader needs differ.** `Result`, `Chromatogram`, `Curve`,
  `CurvePoint`, `CurveUVInfo`, `EventCurve`, `Event`, `PeakTable`, `Peak`,
  `ResultSearchCriteria`, `ResultRunInformation`, `ResultAuditTrail` and
  `ResultDocumentation` have the same columns in both. `SchemaProfile` is for other versions.

## Run matching

Each exported result has exactly one row where `Result.Description` = `Result.xml` `Name`,
and on that row `SystemName` and `RunIndex` match.

Two kinds of result:

| | Native (exported from the database it was run on) | Imported (run on another database, imported) |
|---|---|---|
| `ChromatogramID` | same as `Chrom.1.Xml` | new |
| `Result.Created` | same (within 1 ms) | **import time** (months to years later) |
| `Chromatogram.Created` | same | same |
| Curve `MethodStartTime` / `ChromatogramStartTime` | same; DB has 100 ns precision, `.UFol` XML truncates to ms | **DB truncated to whole seconds** (XML has the fraction) |
| Folder path | same | usually the same; can be imported into a different folder |

`Result.Created` more than 2 days after the first curve's `MethodStartTime` points to an
import.

**Consequence for `match_key`**: use (`run_name`, `Chromatogram.Created` in UTC,
`system_name`, `run_index`), or curve `MethodStartTime` truncated to whole seconds. Not
`Result.Created`. For a `.UFol`, `original_id` is a direct key.

## Curves

Compared per curve, with ZIP `CurveNumber` = DB `ChromatogramPos`:

| Field | Result |
|---|---|
| Curve count, `Description`=`Name`, `CurveDataTypeName`, `AmplitudePrecision`, `IsoCroneTypeName`, `ScanInterval`, `IsOriginalData`, `CompressionDesc`, `VolumeUnit`, `ColumnVolumeUnitName` | equal |
| `AmplitudeUnit` | equal, except that pH curves have `''` in the DB where the XML element is empty (`None` in the reader) |
| `TimeUnit` | equal, except volume-grid curves: `''` in the DB, `None` in the reader |
| `BinaryCurvePoints` | **byte-identical**. Volumes and amplitudes decode to identical float32 arrays |
| `DistanceBetweenPoints`, `DistanceToStartPoint` | DB `real` is the exact float32. The `.UFol` XML (and some U7.11 zips) prints 7 significant digits, off by 1-3e-7 relative. Compare with that tolerance |
| `ColumnVolume` | DB `float` is the float32 value widened to double: equal when both are rounded to float32 |
| `CurveUVInfo` | path length, nominal path length and normalization flag equal |

- **Point sets:** one `CurvePoint` row per curve with `IsFullResolution = 1` where seen;
  several point sets did not occur.
- **`FilestreamGUID`** is set on every `CurvePoint` row (the blob is a FILESTREAM column),
  and `SELECT BinaryCurvePoints` returns it through pymssql without extra setup.
- **Grid origin:** `MethodStartTime` = `ChromatogramStartTime` wherever seen; both can be
  NULL. The owner decided (2026-11-08): the grid counts from `ChromatogramStartTime`, because
  one method run can hold several chromatograms and a later one doesn't start at the method
  start. `MethodStartTime` only when there is no chromatogram start (`pycorn/timeline.py`,
  `GRID_STARTS`).
- **Curve numbers have gaps:** the largest `ChromatogramPos` can exceed the curve count.
  Never index curves by position.

## Events and peaks

- Event curves: count, `Description`, `EventCurveTypeName` equal; `ChromatogramPos` =
  `CurveNumber`.
- Events: count, text, type, subtype and `InstructionFeedback` equal, in `EventCurvePos`
  order. `EventTime`/`EventVolume` equal for the zips; for the `.UFol` they differ by the
  7-significant-digit rounding of the XML (up to 4e-7 relative).
- `EventCurve.ColumnVolume` can differ, probably the same float32 widening as on curves.
- Peak tables and peaks: counts equal. Values not compared yet.

## Metadata entries mapped

| Bundle | Database | Result |
|---|---|---|
| `CalibrationSettingData`, `ColumnIndividualData`, `ColumnTypeData`, `EvaluationProcedureData`, `InstrumentConfigurationData`, `MethodData`, `MethodDocumentationData`, `NextBufferPrepData`, `NextFracData`, `ReportFormatData`, `StrategyData`, `SystemData`, `SystemSettingData`, `VersionInformationData` | `dbo.ResultAuditTrail`, column of the same name | **byte-identical** |
| (not in bundles) | `ResultAuditTrail.InstrumentInformationData` | NULL where seen |
| `Result.xml` `ResultRunInformation` (base64) | `dbo.ResultRunInformation.RunInformation` | equals the **decoded text**. Types: `ColumnInformation` (XML), `InstrumentServerUNICORNVersion`, `RunStartDate`, sometimes `RecoveryInformation` |
| `Result.xml` `ResultSearchCriterias` (`Name`, `Keyword1`, `Keyword2`, `ExtraDisplayInformation`) | `dbo.ResultSearchCriteria` (`SearchCriteriaName`, `KeyWord1`, `KeyWord2`, `ExtraDisplayInformation`) | same rows. Equal as text, but `open_run()` types the values (numbers) and turns `''` into `None`, so parity tests should compare text |
| `Result.xml` `ResultDocumentation` | `dbo.ResultDocumentation` | `RunNote`, `EvaluationNote` equal. `MethodNote`, `StartNote` are only in the DB |
| `Chrom.1.Xml` `ChromatogramLayoutXml`, `UISettings`, `AnalysisSettings`, `VisualAppearance`, `OriginalResultName` | `dbo.Chromatogram`, same names | same text (XML-escaped in the file). Empty in both where empty |
| `Result.xml` `FolderPath` | `dbo.Folder` hierarchy | equal (see Run matching) |
| `EvaluationLog.xml` | `dbo.ResultDoc_AuditTrail` -> `AuditTrail` -> `AuditTrailEntry` | same trails and entries, same `LogEntry` text. For imported results, the DB trail's `Created`/`LogTime` are whole seconds and `CreatedBy` differs |

The **retention setting** (F21) is in `Chromatogram.ChromatogramLayoutXml` /
`Chromatogram.UISettings` in the database, the same text the ZIP reader parses; filled or
empty alike in the DB and the bundles.

There is no method link on `dbo.Result` (no `MethodID`). The method used in a run is in
`ResultAuditTrail.MethodData`, the same blob as the bundle's `MethodData`.

Checked later with the database reader:

- Text in the database keeps CRLF line ends and trailing newlines. The bundle's XML has
  LF line ends, and xmltodict strips leading and trailing whitespace.
- `Curve.GuidePoints`, `DerivedCurveOrigin` and `InstallationTestReports` (UFol service
  results) are the `GuidePoint`, `DerivedCurveOrigin` and `InstallationTestReport` tables.
- Imported results also have `Result.LastModified` and the curve start times truncated to
  whole seconds in the database.

## Column volume (Vc)

- `ColumnVolumeUnitName` is `ml` (or NULL); curves of one result never disagreed on Vc.
- Every result has `ColumnInformation` run information (XML).
- Fallbacks (`column_volume.py`: run information, run log, flow ratio, peak table) agreed
  with the curves within 1 % where both exist, but never rescued a result with no Vc on its
  curves: those need `column_volume=` (by-pass service runs among them). The method's
  `mColumnVolume` alone is low confidence and not used.

## Units inventory

Curve `CurveDataTypeName` x `AmplitudeUnit` seen:

| Type | Units |
|---|---|
| `UV` | `mAU`, `mAU/cm` |
| `Conduction` | `mS/cm`, `%` |
| `pH` | `''` |
| `Pressure` | `MPa` |
| `Temperature` | `°C` |
| `Other` | `cm/h`, `ml/min`, `CV/h`, `%`, `cm`, `Ratio`, `ml` |

- `TimeUnit` is only `min` (or `''` on volume-grid curves). `VolumeUnit` is only `ml`
  (or `''` on time-grid ratio curves). `Chromatogram.TimeUnit`/`VolumeUnit` are
  `min`/`ml`.
- No `AU`, `L/h` or `L` was seen; process-scale systems may use them. Unit handling is
  table-driven so it can be extended (see the plan's Units section).
- `mAU/cm` UV curves are already divided by the path length in cm; plain `mAU` curves are
  not (checked on a derived curve with no `CurveUVInfo`: a `mAU` curve's values divided by
  its 0.2 cm nominal path).
- `°C` is stored correctly (UTF-16 `B0 00 43 00`). pymssql returns it unchanged.
  Printing it to a cp1252 console works, but piping it through tools that expect UTF-8
  shows a replacement character.

UV path length (`CurveUVInfo`): `UVPathLength` 2.08 / 2.01 with `NominalUVPathLength` 2.0,
and 0.51 with 0.5 (mm, the standard 2 mm and 0.5 mm flow cells). `IsSystemProvided` is true;
most curves are normalized to the nominal path, a few are not.

**Extinction coefficient:** the schema has `Peak.ExtCoeff`/`ExtCoeffConc`/`ExtCoeffAmount`
(units in `PeakUnit.ExtCoeffUnit`, ...), `Pool.ExtCoeff` and
`ProteinInfo.ExtinctionCoefficient`. They were empty where surveyed, so for now it comes
from the user (plan: "Concentration output").

Event vocabulary: `EventCurveTypeName` = `Logbook`, `Injection`, `Fraction`, and
`ImportedLogbook`. `EventTypeName` = `Method`, `Manual`, `ManualAtInstrument`, `System`,
`SystemTimeout`, `Fraction`, `Injection`, `Alert`; `EventSubTypeName` = `Undefined`,
`BlockStart`, `BlockEnd`, `SetMark`, `Acknowledge`, `Error`, `Alarm`, `Warning`,
`UserControl`.

## UTC and pauses

**Timestamped run-log texts** (`EventCurveTypeName = 'Logbook'`; digits masked here):

| Text | Event type | Timestamp |
|---|---|---|
| `Method Run M/D/YYYY h:mm:ss AM` | `Method`/`BlockStart` | local time, no offset |
| `Pause Infinite min M/D/YYYY h:mm:ss ...` | `Manual`, `ManualAtInstrument`, `Method` | local time |
| `Hold M/D/YYYY h:mm:ss PM -HH:MM` | `Manual` | local time + UTC offset |
| `Continue M/D/YYYY h:mm:ss PM -HH:MM` | `Manual`, `ManualAtInstrument`, `System` | local time + UTC offset |
| `End M/D/YYYY h:mm:ss PM -HH:MM` | `Method`, `Manual`, `System` (`Undefined` or `BlockEnd`) | local time + UTC offset |

- `Hold until <condition>` texts (`Hold until UV ...`, `Hold until Condition Occurred
  ...`, `Hold until Timeout`, `Hold until Air sensor ...`) are method waits, not
  operator holds, and carry no timestamp.
- **Date format:** month-first in every case seen (the first field never exceeds 12, the
  second often does), 12-hour `AM`/`PM` clock. Other locales may differ: the parser checks
  against `MethodStartTime`.
- **Start anchor:** the first dated text agrees with the curve `MethodStartTime` (local
  wall clock) within 2 s on almost every chromatogram.
- **Pause:** in a Pause -> Continue/End interval, elapsed `EventTime` does **not** advance,
  however long the wall-clock gap.
- **Hold:** in a Hold -> Continue/End interval, elapsed time **advances by the wall-clock
  gap** (rarely only partly). Hold does not stop the clock.
- The residual (wall clock - start - elapsed) grows during almost every run, at a Continue
  or an End. The UTC rebuild has to handle this; it is the common case.
- `dbo.Event` has no date/time column. The only `datetimeoffset` columns relevant to a
  run are `Result.Created`, `Chromatogram.Created`, `Curve`/`EventCurve`
  `MethodStartTime`/`ChromatogramStartTime` and `PeakTable.Created`. Wall-clock times
  inside a run must come from the run-log texts.

Consequences for UTC: paired Pause/Continue texts give exact pause durations (`exact`
case). Hold must **not** be added as a pause. The UTC offset on Continue/End lets each anchor
be converted without guessing the zone.

## Residual after the UTC rebuild

- **Fixed:** an alarm that refuses a Continue. The log reads `Continue (Issued)`, the
  alarm, the system's `Pause ... (Ignored)`, `Continue (Completed)`, then the operator's
  next `Continue (Issued)` minutes later at the same elapsed time. The anchor dedupe took
  that second Continue for the first one's follow-up entry and dropped it. Now only
  (Processing)/(Completed) entries are follow-ups, and a repeat of the previous anchor's
  kind (a Pause while paused) is still merged.
- **Left (warned, not placed):** a few runs end a few seconds late after hours with no
  pauses (drift of the elapsed clock, or a stop UNICORN didn't log).
- Alarm texts carry `Occurred at time <date> <offset>`. It agrees with the rebuilt clock to
  the second in the runs inspected. Not used as an anchor (a possible cross-check).
- Volume stops during a pause, as elapsed time does (owner confirmed).
