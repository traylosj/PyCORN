# Run structure: read a run once, describe it in ISA-88 terms (proposal)

Draft, 2026-11-12, for the owner's review. Nothing here is built.

**In short.** Today PyCORN reads a run's structure (phases, steps, flow, %B, pauses, feed)
separately for each output: the phase tables, the CADET sections, the page's tracks, the
window statistics. They can drift apart. The proposal: read the run **once** into a single
description, and have every output take what it needs from it. The description uses the
words of ISA-88, the batch-control standard that plant systems already use (section 1, "Why
ISA-88"), next to UNICORN's own words. It keeps both what the method asked for and what the
instrument actually did, so a modeler, a person reading Excel, and a script each get the
version they need without re-deriving it.

This document reworks [`RUN_STRUCTURE_REVIEW.md`](RUN_STRUCTURE_REVIEW.md) (topics 3.1-3.12,
Decide 1-26) around that idea. The ISA-88 notes follow the standard's concepts (procedural,
physical and process models; states; batch records, Part 4); they were not checked against
its text, and the BatchML names should be checked against the published schemas before
anything is written to them. Section 6 (2026-11-14) compares it with CADET-Process 0.13 and
CADET-GUI.

## 1. Who it is for, and what must not change

**Who reads a run's structure.** The instrument is the evidence, not a reader.

| Audience | Gets | Wants |
|---|---|---|
| Model (CADET, CADET-Process, modelers' scripts) | sections, steps, `experiment.json`, CADET files | what the equipment actually did, on one wall clock, SI, every value with its source, nothing folded |
| Display (Excel, web page, phase tables, M22 statistics) | phase rows, tracks, notes | the recipe as the user wrote it: one value per setting per phase, plain notes for what differed |
| Scripts and agents (Track 9) | JSON (`inspect`, `--json`) | the same record as the model, stable keys, masked by default |

**Decisions already made** that the design must keep (ROADMAP "Decisions", the review):

- Sections from what ran in the run log (D13); the method only as hints (D17); each
  instruction holds until the next.
- Column flow by valve position (D14); feed by valve, caller loop and path volumes, never
  assumed (D16-D21, D27); no defaults that mirror CADET parameters (D28).
- Wall clock for the model, pauses as zero-flow stretches (D5, D7); Hold freezes %B.
- Units in column names, SI for the model (3.3); x-axis picker and tracks on the page (4, 5).
- Inlets from the stored system configuration: known / assumed / not known (22).
- Phase start lag (2026-11-12): display folds flow, %B and %Q within 10 s and 1 % of the
  phase with a `held_over` note; valves and feed never fold; phase start and end never
  move; M22 windows from a phase start skip the held-over seconds; CADET never folds.
- Unknowns said in plain words, never guessed; masking rules (Track 9); full values in code,
  rounding at display only.

**ISA-88** already explains most of these: the method (procedure) issues commands, the
instrument (equipment) carries them out, and the batch record keeps both, without smoothing
either one.

**Why ISA-88.** Plant batch systems and historians already organize run data this way.
Emerson DeltaV Batch is built on ISA-88 (procedures, unit procedures, operations, phases)
and keeps batch history from its Batch Executive; AVEVA's PI System batch interfaces,
including the one for DeltaV Batch, build event frames level by level along the same
hierarchy. ISA-88 Part 4 (ANSI/ISA-88.00.04-2006) defines the batch production record, and
MESA's BatchML is its free XML form. A run record tagged with ISA-88 levels can therefore sit
beside plant batch data without a new vocabulary. Neither product is known to exchange
BatchML, so this is about structure, not a file format. Sources: "References" at the end.

## 2. The idea: read once, then three views

Today each output builds its own picture from the run log (phase tables, sections, CADET,
page tracks, M22 windows). The proposal is one **run record**: a single description of the
run, built once per result (from all its original chromatograms; section 3,
"Chromatograms"). It holds:

- **what ran, in order**: phases, steps and pauses as the run log shows them;
- **asked for vs. delivered**, side by side: the setting the method gave (e.g. flow
  5 ml/min) and what the instrument did (from the curves and the volume axis);
- **where each value came from**, and how sure it is (logged, measured, inferred, assumed).

Each reader then gets a **view** of the record: the same facts, chosen and worded for them.
The rules for each view are written in one place (the `step_rules.py` block the review
already decided on):

| View | What it shows |
|---|---|
| Model (CADET) | what the instrument did where it was measured, else what was asked for; every change kept, nothing smoothed; each section says where its values came from |
| Display (Excel, page) | the method's settings per phase, as the user wrote them; a short delay at a phase start is folded into the phase with a note (3.4 rule); UNICORN's words |
| Scripts and agents | the whole record as JSON (versioned, masked by default) |

A view never computes anything the record doesn't hold, so the three can't disagree; where
they show different numbers, the record says why (`held_over`, `source`, `quality`).

## 3. ISA-88 mapping

The review's table (3.1), extended to the other ISA-88 models. The CADET column uses
CADET-Process 0.13 names (`instruments`, unreleased; section 6).

**Procedural model** (what ran)

| ISA-88 | UNICORN | CADET-Process 0.13 | In the record |
|---|---|---|---|
| Procedure | the method run (one result) | `PhasedProcess(name, flow_sheet, steps)` | `procedure`: ResultID, method (recipe) name |
| Unit procedure | the method on one system and column | the same `PhasedProcess`, on one `LCFlowSheet` | `unit_procedure`: one per run |
| Operation | a UNICORN **phase** | none (CADET-GUI: a "Phase ..." run-log marker) | `operation`: number, instance (repeats), logged text, display name |
| Phase | a UNICORN **block**, and the activities running at once inside a phase (flow/gradient, sample feed, fractionation, watches) | none (activities are flow-sheet units) | `phase`: one per equipment activity, with its equipment module |
| Steps of phase logic | run-log instructions; PyCORN sections | **`Phase`** (one flow, linear composition) and `ValveEvent` | `step` (constant state: one flow, one %B piece, one feed state, one state) and `section` (the model's piecewise inputs) |

The names collide twice: a UNICORN phase is an ISA-88 operation, and a CADET-Process `Phase`
is an ISA-88 step. The record carries the ISA-88 and UNICORN words; display keeps UNICORN's;
`Phase` appears only in CADET objects.

**Physical model** (where it ran): unit = the system (CADET: `LCFlowSheet`); equipment
modules = system pump (CADET: buffer inlets A-D and mixer), sample pump, injection valve
(sample loop, `ValveEvent`), column valve (`bypass_units`, static for the run), inlet valves,
outlet valve / fraction collector; control modules = UV, conductivity, pH, pressure monitors
(CADET: observation points such as `column.outlet`, `tubing_detectors.outlet`). Source: the
stored system configuration (already read for inlets, 22).

**Process model** (what it did to the material): equilibrate, load, wash, elute, strip,
clean (CADET: the templates `PulseInjection`, `Step`, `StepElution`, `LWE`, `Breakthrough`;
CADET-GUI: experiment types). UNICORN doesn't store this; the method's phase names usually
say it. The record keeps a `process_action` field that is empty unless the caller gives a
mapping (never guessed), because a phase named "Wash 2" may elute.

**States.** ISA-88 procedural states, with UNICORN's words kept. The words cross over:

| UNICORN | ISA-88 state | Pumps | Clock | %B ramp | CADET-Process 0.13 |
|---|---|---|---|---|---|
| running | Running | on | runs | runs | `Phase` with flow > 0 |
| **Pause** | **Held** | stop | stops | stops | `Phase(d, 0.0, {})` |
| **Hold** | **Paused** | run | runs | frozen | `Phase` with `composition_start = composition_end` |

**Parameters.** Method variables are formula parameters (the recipe); measured lengths,
volumes and statistics are report parameters (the record).

**Chromatograms.** A method can start a new chromatogram mid-run (UNICORN's New Chromatogram
instruction); some users do this to set one piece of work (say a by-pass measurement) apart
from another (say the column run), others don't. The result then holds two or more original
chromatograms. Owner's statement from memory (2026-11-14), not checked on any example: the new chromatogram's time starts at 0 at the
instruction; each chromatogram's `Logbook` holds only its own part of the run log; collection
stops in the first and starts in the second, so the two don't overlap. Open test case (no
sample yet): how the run log records the instruction itself, and whether set-points (flow,
%B, valves, phase, Pause / Hold state) carry across the cut and how the second `Logbook`
shows them (repeated at its start, or not at all).

Storage points the same way: in the database a run log (`EventCurve` with
`EventCurveTypeName = 'Logbook'`, its lines in `Event`) is keyed by `ChromatogramID`, and only
the chromatogram carries `ResultID`; in a `.zip` the event curves sit in each `Chrom.N.Xml`.
The method, system settings and other `ResultAuditTrail` data are per result. So a record per
result likely joins each chromatogram's `Logbook` in turn and shares the result-level data;
to be verified with an example.

In ISA-88 a chromatogram is not a procedural level; it is a stretch of the batch record's
data. So:

| Thing | In the record |
|---|---|
| The result | one `procedure` / `unit_procedure` on one clock |
| Each original chromatogram | a `chromatogram` segment: name, start and end on the procedure clock, its curves; every segment, value and curve carries a `chromatogram` tag |
| The procedure clock | chromatogram 1's own; chromatogram n starts where n-1 ends (no overlap), checked against its `ChromatogramStartTime` in UTC |
| Set-points across the cut | **open test case.** If they carry over (each instruction holds until the next, D13) and the second `Logbook` doesn't repeat them, the record has to take them from chromatogram 1 (`source = run_log` with the chromatogram it came from): hard to read, and not built until a sample shows it. Until then, chromatogram 2's set-points before its first instruction are `not_known` |
| An operation across the cut | open, with the set-points: one operation in both segments if the second `Logbook` starts inside it with no phase line |
| The instruction | a `step` where it ran, if the run log records it |
| What the user meant by the split | not inferred; the caller may map a chromatogram to an operation or `process_action`, as for phases |
| Imported / derived chromatograms (W17) | not part of the procedure: an imported one links to its source result (`OriginalResultID`), a derived one is analysis; linked, never merged |

Views: display keeps one table set per chromatogram, as today, with operations cut at
the boundary noted; the model picks a span by chromatogram or by operation on the one clock
(e.g. a by-pass measurement and a column run as separate CADET inputs); agents get the record
with its tags. Today each chromatogram is read from its own `Logbook` alone (inferred from
the code, not run): if set-points do carry across the cut unlogged, chromatogram 2's phase
table and sections would miss them until its first instruction. Untested: no sample with two
original chromatograms.

## 4. The record: fields and tags

Every segment (operation, phase, step, section) carries the same core tags:

| Tag | Values | Why |
|---|---|---|
| `s88_level` | `procedure`, `unit_procedure`, `operation`, `phase`, `step`, `section` | the level, in ISA-88 words |
| `path` | `OP3/PH1/ST2` (numbers from `phase_number`, `step_number`, Decide 2) | stable keys across outputs |
| `unicorn_level`, `unicorn_text` | `phase` / `block` / `instruction`; the logged text | UNICORN's own words, never lost |
| `instance` | 1, 2, ... | repeated operations (Equilibration twice: Decide 23) |
| `state` | `running`, `held`, `paused` (ISA-88), with `unicorn_state` | pauses and holds as states (Decide 11, 14) |
| `start`, `end` | wall s, elapsed s, volume ml, CV, UTC | every axis (3.2) |
| `equipment_module` | `system_pump`, `injection_valve`, ... | phase level only |

Every value in a segment is a small object, not a bare number:

| Field | Values |
|---|---|
| `value`, `unit` | full precision; the unit as a UCUM code (`mL/min`, `%`, `mL`, `s`; CV as the annotation `{CV}`) |
| `kind` | `setpoint` (commanded), `actual` (measured), `derived` (calculated) |
| `source` | `run_log`, `volume_axis`, `curve`, `system_configuration`, `method_hint`, `caller`, `default`, `not_known` |
| `quality` | `exact`, `inferred` (e.g. an inferred pause), `assumed` (e.g. `B1 (assumed)`), `folded` (display only, with `held_over`) |

Example, u73 Sample Application, column flow (numbers from the public sample):

```json
{"s88_level": "operation", "path": "OP2", "unicorn_text": "Phase Sample Application",
 "display_name": "Sample Application", "instance": 1, "state": "running",
 "column_flow": [
   {"value": 1.0, "unit": "mL/min", "kind": "setpoint", "source": "run_log", "quality": "exact"},
   {"value": 5.0, "unit": "mL/min", "kind": "setpoint", "source": "run_log", "quality": "exact",
    "held_over": {"duration_s": 2.4}},
   {"value": 2.5, "unit": "mL/min", "kind": "actual", "source": "volume_axis", "quality": "exact",
    "span_s": [315.7, 316.3]}],
 "feed_volume": [
   {"value": 0.417, "unit": "mL", "kind": "derived", "source": "run_log"},
   {"value": 0.389, "unit": "mL", "kind": "actual", "source": "volume_axis"}]}
```

Display shows `1 ml/min (first 2.4 s at 5)` and `0.42 ml` (Decide 18 still open: which
volume); the model takes the actual; an agent sees all three and why they differ.

## 5. Standard formats

Proposal, in order of value:

1. **The record as JSON with ISA-88 names** (`pycorn record --json`, `schema: 1`, masked by
   default like every `--json`). Element names follow ISA-88 Part 4 / **BatchML** (the
   MESA / ISA XML schemas for batch records: procedure, unit procedure, operation, phase
   elements; events; parameters with values and units), so a later BatchML XML writer is a
   mapping, not a redesign. JSON first because the rest of PyCORN's machine output is JSON
   (A4) and agents read it directly.
2. **Units as UCUM codes** and times as ISO 8601 (UTC instants, durations), in the record and
   its JSON; display keeps today's column names (`_min`, `_ml`, 3.3).
3. **BatchML XML** (optional, later): only if a user's MES / batch-record system wants it.
4. **Allotrope Simple Model (ASM), liquid chromatography** (optional, later): for the
   chromatogram and peak results going to an ELN / LIMS. It covers the measurement, not the
   procedure, so it sits beside the record, not instead of it.
5. CADET keeps its own formats (unchanged).

Masking, local-vs-universal and "never print connection values" apply to every format.

## 6. CADET-Process 0.13 and CADET-GUI

Review of 2026-11-14, read in the code of CADET-Process `dev` (`e11f82e`, 2026-11-13) and
CADET-GUI `main` (`bb5539e`, 2026-10-28). CADET-Process's release notes announce
`instruments`, `calibration` and `characterization` for 0.13.0 (no date); `pycorn[cadet]` is
on 0.12 today. Their API may still change.

**"Phase" has three meanings.** A UNICORN phase is an ISA-88 operation (section 3); a
CADET-Process `Phase(duration, flow_rate, composition_start, composition_end)` is one stretch
of constant flow and linear composition, i.e. an ISA-88 step (a PyCORN section). The record
keeps its ISA-88 words; the model view says `Phase` only in CADET objects. CADET-Process
asks in its forum whether its vocabulary (phase, valve event) fits how users see their
instrument; the record's mapping is a ready answer.

| Record | CADET-Process 0.13 (`instruments`) | CADET-GUI |
|---|---|---|
| procedure / unit procedure | `PhasedProcess(name, flow_sheet, steps)` | a study / run |
| operation (UNICORN phase) | none (a `Phase` has no name) | markers: "Phase ..." run-log texts, on a volume axis only |
| step / section, `state = running` | `Phase` (flow, %B as `{"A": 1-b, "B": b}`, ramp via `composition_end`) | "Phase <i>" spans from the template |
| `state = held` (UNICORN Pause) | `Phase(d, 0.0, {})` (allowed: composition checked only when flow > 0) | not represented (one constant flow) |
| `state = paused` (UNICORN Hold) | `Phase` with `composition_start = composition_end` | not represented |
| valve setpoint (injection valve) | `ValveEvent("load" / "inject" / "direct_inject" / "run")` | none |
| equipment modules (physical model) | `LCFlowSheet`: sample loop, mixer, four tubings, column, `bypass_units` | instrument templates |
| `process_action` (process model) | templates `PulseInjection`, `Step`, `StepElution`, `LWE`, `Breakthrough` | experiment types (system pulse, column pulse, LWE, breakthrough, ...) |
| `actual` curves | `ReferenceIO(time, solution, flow_rate)`, SI by convention, no unit field | `ExperimentalRun` / `Channel`, units dropped |
| feed start (a setpoint with time) | - | `Comparison.injection_marker` (a text prefix) |

**What the model view would lose today** (gaps to raise with CADET, none to work
around in PyCORN by guessing):

- One flow per `Phase`: UNICORN flow ramps are not representable (a flow at phase end would be).
- Duration must be > 0 and two `ValveEvent`s in a row are rejected: zero-length steps are
  dropped, with a warning naming their `path`.
- No name on `Phase`: the `path` tag and UNICORN's text can't travel with the steps, so the
  GUI can't align on UNICORN's phases. Ask for an optional `name`.
- Buffers A-D are mixer inlets (a quaternary pump), not AKTA's A/B pumps with inlets
  A1-A4/B1-B4; `bypass_units` is static, so a mid-run column by-pass (a column-valve state in
  the record) has no counterpart.
- `ReferenceIO` has no unit or metadata, and every `calibration` function returns a new one:
  the record's `unit`, `source`, `quality` stay in `experiment.json` beside it.
- CADET-GUI reads `.csv` only, forces a volume axis in mL even for a `min` export, cannot save
  a time-axis run with markers, and converts with one typed flow. A record-based import would
  carry time in s, markers from operations (`path` + `unicorn_text`), units, and the
  injection time as a number from the feed setpoint.

**What the record supplies that CADET doesn't read from a run:** column geometry and Vc, mixer
volume from the stored system configuration, inlet positions (known / assumed / not known),
pauses and holds as states, the executed sections with their source. Loop volume stays the
caller's (D19); `process_action` and the template stay the caller's (UNICORN names are free
text), even though CADET's templates are exactly the process model ISA-88 describes.

## 7. The open Decide items, re-sorted

**Settled by the frame** (proposed answers; confirm as a set):

| # | Item | Proposed answer from the frame |
|---|---|---|
| 1 | steps level | yes: the ISA-88 step of the flow/gradient phase (constant state); `chrom.step_table()`, `<c>_steps.csv` |
| 2 | keys | yes: `phase_number`, `step_number`, and the `path` tag |
| 3 | ISA-88 terms in columns | the record's JSON uses ISA-88 words (`s88_level`, `operation`, ...) beside UNICORN's; display tables and CSV keep UNICORN's words only |
| 9, 10 | held-over note; "at the start" | decided 2026-11-12 (display) |
| 11 | pauses in steps | `state = held` rows (UNICORN Pause = ISA-88 Held), `unicorn_state = pause` |
| 13 | inferred pauses | same rows, `quality = inferred`; display marks them ("about 40 s, not logged") |
| 14 | holds | `state = paused` (UNICORN Hold), a note per phase in display |
| 16 | by-pass | an equipment state of the column valve module; display note |
| 20 | ramp length | both: `setpoint` (logged length) and `actual` (measured on each axis); display shows the actual |
| 23 | repeats | `instance` numbers; display `Equilibration (2)`; the logged text kept |
| 24 | warnings per phase | warnings carry the `path` they belong to; display puts them on the phase row |

**Display choices still the owner's** (the frame doesn't settle wording): 12 phase length
with or without pauses (proposal: both), 15 which flow users see, 17 feed wording, 18 feed
volume shown, 21 the gradient wording rule, 25 section-lane chart, 26 summary beside or
instead of today's sequences.

**Evidence in, owner to confirm:** 6, the model's "actual" flow and feed. The survey
(review 3.4) shows the volume axis is the flow the pump
delivered (valve-turn stops, pressure-control ramps, sample pump alone), not a measurement
and not column volume. The frame then says: the run log gives structure, timing, valves and
feed (procedure); inside a section the model takes the volume axis's flow where it differs
by more than 0.01 ml and 1 % (equipment actuals), with a warning, then D14's zeroing. Display
note: with pressure control a flow increase still ramps about 10 s after the instruction,
longer than the fold cutoff, so the `held_over` note should also say "ramped n s".

**Tooling gap:** 19, `pycorn export --loop-volume` / `--sample-path-volume` for the CADET
feed (one flag or two).

## 8. Plan

Each step additive: today's outputs stay byte-identical until a step says otherwise.

1. **Record** (`pycorn/run_record.py`): built from `run_log_sections`, the timeline, the
   system configuration and the curves; ISA-88 levels, states, tags, setpoint / actual /
   derived values with source and quality. Tests: today's phase table, sections and CADET
   inputs re-derived from the record equal today's outputs.
2. **Rules** (`pycorn/step_rules.py`): the 3.4 block (decided) and the other display rules
   as they're decided; one function per view.
3. **Display view**: the decided 3.4 fold and 23 names in the phase table, Excel and the
   page, plus the M22 lead-in skip. A renamed or changed output: CHANGELOG entry.
4. **Steps table** (Decide 1, 2) for modelers, from the record.
5. **`pycorn record --json`** (and `inspect` gains the record's operation list): the
   standard-format JSON of section 5, masked by default.
6. **Model view** reads `actual` values once Decide 6 is settled by the survey.
7. When CADET-Process 0.13.0 ships: the model view as `PhasedProcess` steps on an
   `LCFlowSheet` (section 6; needed for `characterization`, which takes an `LCProcess`),
   replacing PyCORN's own flow sheet and event compiler; the gaps raised with CADET first.
8. Later, on request: BatchML XML, Allotrope ASM.

## 9. Limits

- `process_action` stays empty without a caller mapping: UNICORN names are free text.
- The volume axis is "actual" only if the survey shows it is what the pump delivered; else
  it is a derived value and the run log stays the model's source.
- One unit procedure per run: UNICORN results hold one system's run, however many original
  chromatograms it was recorded in.
- Two original chromatograms in one result: built on the owner's statement from memory of
  how UNICORN splits them (section 3), not on any example. Open test case: how the run log records the
  New Chromatogram instruction, and whether and how set-points carry across the cut (in
  `samples/SAMPLE_ZIP_CONTENTS.md`, "Coverage and wanted samples").
- CADET-Process's `instruments` and CADET-GUI are unreleased; section 6 maps to their code
  as reviewed and is rechecked when 0.13.0 ships.
- ISA-88 conformance is not claimed: the record uses its concepts and names as a frame.

## References

Read 2026-11-14. The AVEVA pages were seen only as search excerpts (they need a browser);
open them before quoting them outside this draft.

ISA-88 and BatchML:

- ANSI/ISA-88.00.04-2006, *Batch Control Part 4: Batch Production Records* (ISA, preview):
  https://www.isa.org/getmedia/4b46d97c-6954-4936-97f5-db672f3fd154/S_880004_preview.pdf
  ("a reference model for batch production records")
- MESA International, B2MML-BatchML (BatchML: "an XML implementation of the ANSI/ISA-88,
  Batch Control, family of standards", IEC 61512):
  https://github.com/MESAInternational/B2MML-BatchML

Emerson DeltaV:

- *DeltaV Batch Control and Operations* ("based on ISA88 Batch Standard"):
  https://www.emerson.com/en-us/automation/control-and-safety-systems/distributed-control-systems-dcs/deltav-distributed-control-system/deltav-batch-control-and-operations
- *Recipe Exchange*, product data sheet ("procedures, unit procedures, operations, and
  formulas"; Emerson's own XML, not BatchML):
  https://www.emerson.com/is/content/emerson/pt/systems-and-software/deltav-distributed-control-system-dcs/product-data-sheets/documents/deltav-recipe-exchange.pdf
- *Batch Executive*, product data sheet ("detailed batch historical records"):
  https://www.emerson.com/is/content/emerson/en/corporate/industries/life-sciences/documents/batch-executive.pdf

AVEVA PI System (search excerpts only):

- PI Interface for Emerson DeltaV Batch (event frames from the DeltaV Batch Historian):
  https://docs.aveva.com/bundle/pi-interface-for-emerson-deltav-batch/page/1269356.html
- PI Interface for Siemens SIMATIC Batch ("one template for each level in the standard s88
  batch hierarchy"): https://docs.aveva.com/bundle/pi-interface-for-siemens-simatic-batch/page/1010973.html
- PI Event Frames Generator ("batches that contain unit procedures, operations, and phases"):
  https://docs.aveva.com/bundle/pi-event-frames-generator/page/1011413.html
