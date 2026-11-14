# Run structure: read a run once, describe it in ISA-88 terms (proposal)

Draft for the owner's review, started 2026-11-12, reorganized 2026-11-14. Nothing here is
built.

## 1. In short

**The problem.** PyCORN reads a run's structure (phases, steps, flow, %B, pauses, feed)
separately for every output: the phase tables, the CADET sections, the page's tracks, the
window statistics. Each output re-derives the same facts its own way, so they can drift
apart, and a question such as "what was the flow in the load phase?" has more than one
answer depending on where you look.

**The proposal.** Read each run **once** into a single description, the **run record**, and
let every output take what it needs from it. The record holds three things:

- **what ran, in order**: the phases, steps and pauses the run log shows;
- **what was asked for next to what was delivered**: the method's setting (say 5 ml/min) and
  what the instrument actually did (from the curves and the volume axis);
- **where each value came from**, and how sure it is (logged, measured, inferred, assumed).

**Three views of one record.** Each reader gets the same facts, chosen and worded for them:

| Reader | Gets |
|---|---|
| A modeler (CADET) | what the instrument did where it was measured, else what was asked for; every change kept, nothing smoothed; each value says where it came from |
| A person (Excel, the web page) | the method's settings per phase, as the user wrote them, in UNICORN's words; a short delay at a phase start is folded into the phase with a note |
| A script or agent | the whole record as JSON (versioned, masked by default) |

A view never computes anything the record doesn't hold, so the views can't disagree; where
they show different numbers, the record says why.

**The vocabulary.** The record names its levels with ISA-88, the batch-control standard
that plant batch systems and historians already use (section 2), next to UNICORN's own
words. ISA-88's core idea fits a chromatography run: the method (procedure) issues
commands, the instrument (equipment) carries them out, and the batch record keeps both,
without smoothing either.

**What stays open.** Display wording (Decide items in section 11), how UNICORN splits a run
over several chromatograms, and how cycles are marked (section 9 lists every open question
and the samples that would settle it).

## 2. Why ISA-88

Plant batch systems and historians already organize run data this way. Emerson DeltaV Batch
is built on ISA-88 (procedures, unit procedures, operations, phases) and keeps batch history
from its Batch Executive; AVEVA's PI System batch interfaces, including the one for DeltaV
Batch, build event frames level by level along the same hierarchy. ISA-88 Part 4
(ANSI/ISA-88.00.04-2006) defines the batch production record, and MESA's BatchML is its free
XML form. A run record tagged with ISA-88 levels can therefore sit beside plant batch data
without a new vocabulary. Neither product is known to exchange BatchML, so this is about
structure, not a file format. Sources: "References" at the end.

ISA-88 conformance is not claimed: the record uses the standard's concepts and names as a
frame. Definitions quoted here are from ISA-88.01-1995 (Part 1), the edition whose text was
checked; the 2010 edition (ANSI/ISA-88.00.01-2010, IEC 61512-1) may word them differently.

## 3. From a production process down to one value

This section walks from the largest scale to the smallest. At each level: what UNICORN
stores, what ISA-88 calls it, and what the record keeps.

### 3.1 Across results: a production process

UNICORN stores one method run per result. Whatever spans several results is a **link between
records**, never one merged record.

| Setup | ISA-88 reading | In the record |
|---|---|---|
| Several chromatography steps of one product (e.g. capture, then polishing), on different days, with different methods, on the same skid | one procedure per result, all on the same unit; the steps of one product batch are linked by the material, not by the procedure | one record per result; a caller-given lot or batch label links them; the unit (masked system name) and column say where each ran |
| The same, on different skids | as above, on different units (ISA-88's train: the units "used to make a batch of material") | as above; the unit differs |
| A scouting run: one method run N times with stored variations | N procedures from one recipe | one record per result; a series link (scouting name, run number). Whether the series repeats one process (cycles, 3.3) or varies it (method development) is the caller's to say |
| Methods run back to back from a queue | separate procedures | separate records, ordered by UTC; linked like production steps |
| Connected processing across skids (one skid's outlet feeds the next, at the same time) | units running concurrently; ISA-88 "does not specifically address" continuous processes, and ISA-106 (Procedure Automation for Continuous Process Operations) covers them | one record per skid, aligned on the UTC axis every record has; which outlet feeds which inlet comes from the caller (UNICORN doesn't store it) |
| Multi-column continuous chromatography on one skid (columns load, wash and elute in a staggered rotation) | one procedure with parallel work on several columns | **not supported**: the record has one sequence of sections per run; it would need a column tag on every section and parallel timelines. Waits for a sample. CADET-Process models such processes with its own builders (`CarouselBuilder`, `SMBBuilder`) |

The link labels have ready places in a batch record: BatchML's batch production record has
`BatchID`, `LotID` and `CampaignID` fields. ISA-88 defines a lot ("a unique amount of
material having a set of common traits") but not a campaign, and it says nothing about
splitting or combining batches; ISA-95 (B2MML) tracks material as `MaterialLot` and
`MaterialSubLot`. So a caller's lot label maps to `LotID`, and a series (scouting, a
production campaign) to `CampaignID`, as the record's own terms.

### 3.2 One result: the procedure

A result is one method run: one recipe executed once. In ISA-88 that is one **procedure**,
on one clock, on one unit (the system). The record is built once per result, from all its
original chromatograms (3.4); the method, the system settings and the other result-level
data are shared by all of them.

### 3.3 Inside the procedure: unit procedures and cycles

A **unit procedure** is a complete piece of work on one unit (ISA-88: "a strategy for
carrying out a contiguous process within a unit", made of contiguous operations). A run has
**one** by default.
It has several only when the run repeats a whole process, which chromatographers call
**cycles**: to process enough material, one recipe runs the same sequence (equilibrate, load,
wash, elute, clean) N times on one column. "Cycle" is not an ISA-88 term; the record keeps
the chromatographers' word and maps each cycle to one unit procedure.

The same N cycles can be recorded three ways:

| How the cycles were recorded | ISA-88 reading | In the record |
|---|---|---|
| A scouting run: N results | N procedures from one recipe | one record per result with a series link (3.1); `cycle` = run number when the caller says the series is cycles |
| One result, a new chromatogram at each cycle boundary | one procedure, N unit procedures | each chromatogram a unit procedure with `cycle = n`, when the caller says so; the cuts are checked to fall between operations |
| One result, no new chromatograms (or cuts elsewhere) | one procedure, N unit procedures | `cycle` from the caller (e.g. "a cycle starts at each Equilibration"); a repeat found in the logged phase order is offered as a hint, never applied on its own |

Every segment carries `cycle` (empty when the run doesn't cycle), so the views work the same
whatever the recording: a summary row and window statistics per cycle, the overlay aligned at
each cycle's start (W12), one CADET input per cycle (real cycles differ as the column loads
and ages, so each keeps its own measured reference).

One rule holds the hierarchy together: a unit procedure is made of whole operations, and an
operation "is carried to completion in a single unit", so a cycle or unit-procedure boundary
must fall **between** UNICORN phases. ISA-88 neither states nor rules out several unit
procedures one after another on the same unit (it does allow several running at once on
different units); reading each cycle as one is this proposal's choice.

### 3.4 Chromatograms: how the data was recorded

A method can start a new chromatogram partway through a run (UNICORN's New Chromatogram
instruction). Some users do this to set one piece of work apart (a by-pass measurement, then
the column run; or one cycle from the next), others don't, so the split is not a reliable
sign of intent. **A chromatogram is not an ISA-88 level**: it is a stretch of the batch
record's data. The record tags every segment, value and curve with the chromatogram it came
from, and joins the chromatograms on one clock.

What is known, and what isn't:

- The owner's statement from memory (2026-11-14), not checked on any example: the new
  chromatogram's time starts at 0 at the instruction; each chromatogram's run log
  (`Logbook`) holds only its own part; collection stops in one and starts in the next, so
  they don't overlap.
- Storage points the same way: in the database a run log (`EventCurve` with
  `EventCurveTypeName = 'Logbook'`, its lines in `Event`) is keyed by `ChromatogramID`, and
  only the chromatogram carries `ResultID`; in a `.zip` the event curves sit in each
  `Chrom.N.Xml`. Likely, but to be verified with an example.
- Not known: how the run log records the instruction itself, and whether settings (flow,
  %B, valves, phase, Pause / Hold state) carry across the cut and are repeated at the start
  of the next run log.

| Thing | In the record |
|---|---|
| Each original chromatogram | a `chromatogram` segment: name, start and end on the procedure clock, its curves |
| The clock | chromatogram 1's own; chromatogram n starts where n-1 ends, checked against its `ChromatogramStartTime` in UTC |
| Settings across the cut | **open test case.** If they carry over unrepeated, the record takes them from the previous chromatogram, which is hard to read and not built until a sample shows it; until then they are `not_known` before the first instruction |
| A UNICORN phase across the cut | open, with the settings: one operation in both segments if the next run log starts inside it |
| The instruction | a step where it ran, if the run log records it |
| What the user meant by the split | never inferred; the caller may make chromatograms unit procedures or cycles (3.3), or map them to an operation |
| Imported and derived chromatograms (W17) | not part of the procedure: an imported one links to its source result, a derived one is analysis; linked, never merged |

Display keeps one table set per chromatogram, as today, noting a phase cut at the boundary;
the model picks a span by chromatogram, cycle or operation on the one clock. Today each
chromatogram is read from its own run log alone (from the code, not run on an example), so
if settings carry across the cut unlogged, the second chromatogram's tables would miss them
until its first instruction.

### 3.5 Inside a unit procedure: operations, phases, steps, sections

| ISA-88 | UNICORN | CADET-Process 0.13 | In the record |
|---|---|---|---|
| Operation | a UNICORN **phase** (Equilibration, Sample Application, ...) | none (CADET-GUI: a "Phase ..." run-log marker) | `operation`: number, instance (repeats), logged text, display name |
| Phase | a UNICORN **block**, and the activities running at once inside a phase (flow and gradient, sample feed, fractionation, watches) | none (activities are flow-sheet units) | `phase`: one per equipment activity, with its equipment module |
| Step | a stretch of constant state: one flow, one %B piece, one feed state, one state; from the executed run-log instructions | **`Phase`** (one flow, linear composition) and `ValveEvent` | `step`, and `section`: the model's piecewise inputs |

The word "phase" means three things: a UNICORN phase is an ISA-88 operation, an ISA-88 phase
is closer to a UNICORN block, and a CADET-Process `Phase` is an ISA-88 step. The record uses
ISA-88's words with UNICORN's beside them; display keeps UNICORN's; `Phase` appears only in
CADET objects.

### 3.6 Where it ran and what it did to the material

**Physical model** (where it ran): the unit is the system (CADET: `LCFlowSheet`). Equipment
modules: system pump (CADET: buffer inlets A-D and mixer), sample pump, injection valve
(sample loop, `ValveEvent`), column valve (`bypass_units`, fixed for the run), inlet valves,
outlet valve and fraction collector. Control modules: the UV, conductivity, pH and pressure
monitors (CADET: observation points such as `column.outlet`). Source: the system
configuration stored in the result.

**Process model** (what it did to the material): equilibrate, load, wash, elute, strip,
clean (CADET: the templates `PulseInjection`, `Step`, `StepElution`, `LWE`, `Breakthrough`;
CADET-GUI: experiment types). UNICORN doesn't store this; phase names usually say it, but a
phase named "Wash 2" may elute. The record keeps a `process_action` field that is empty
unless the caller gives a mapping, never guessed.

### 3.7 States: running, paused, held

ISA-88 lists example states for procedural elements (running, holding, paused, stopped,
aborted, complete) and doesn't require them. By analogy: **Paused** is a short stop at the
next safe point, resumed where it stopped; **Held** puts the process "into a known state"
for a long stop, left only by a restart. UNICORN's words cross over:

| UNICORN | ISA-88 state | Pumps | Clock | %B ramp | CADET-Process 0.13 |
|---|---|---|---|---|---|
| running | Running | on | runs | runs | `Phase` with flow > 0 |
| **Pause** | **Held** | stop | stops | stops | `Phase(d, 0.0, {})` |
| **Hold** | **Paused** | run | runs | frozen | `Phase` with `composition_start = composition_end` |

### 3.8 One value: asked for, delivered, and why they differ

Every value is a small object, not a bare number: the setting the method gave (`setpoint`),
what was measured (`actual`) or what was calculated (`derived`), each with its source and
how sure it is. Method variables are the recipe's parameters; measured lengths, volumes and
statistics are the record's.

Example, the Sample Application phase of `samples/u73_blank.zip`, column flow and feed:

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

Display shows `1 ml/min (first 2.4 s at 5)` and `0.42 ml` (which feed volume to show is
still open, Decide 18); the model takes the actual; an agent sees all three and why they
differ.

## 4. Edge cases and how they are resolved

| Edge case | Resolution |
|---|---|
| A new chromatogram cuts inside a phase | chromatograms stay data segments; making them unit procedures or cycles fails with an error naming the phase that crosses the cut |
| A cycle boundary isn't a phase start (e.g. the next cycle's equilibration is part of the previous regeneration) | the caller's rule names the phase or block that starts a cycle; nothing guessed |
| Work before the first cycle or after the last (system prime, final clean) | `cycle` empty for those segments, shown as "before cycles" / "after cycles" |
| Unequal cycles (a short last load, an aborted or extra cycle) | numbered as they ran; a warning lists the phases that differ from the other cycles; never padded or dropped |
| A pause or hold across a cycle boundary | belongs to the cycle in which it started; the next cycle starts when its first phase starts |
| The logged phase order repeats ambiguously (e.g. two washes in one cycle) | the detected repeat is a hint only; the caller decides |
| A scouting run that varies settings (method development, DoE) rather than repeating cycles | linked as a series either way; only the caller calls it cycles |
| Scouting runs that each hold several cycles | both levels: the series link between results, `cycle` within each |
| Eluates pooled across cycles | material, outside the record; fraction and pool rows keep their `cycle` tag |
| A by-pass measurement split from the column run by a new chromatogram | two operations in one unit procedure (same unit), unless the caller says otherwise |

## 5. The record's fields

Every segment (unit procedure, operation, phase, step, section) carries the same tags:

| Tag | Values | Why |
|---|---|---|
| `s88_level` | `procedure`, `unit_procedure`, `operation`, `phase`, `step`, `section` | the level, in ISA-88 words |
| `path` | `OP3/PH1/ST2` (from `phase_number`, `step_number`); `UP2/OP3/PH1/ST2` only when the run has more than one unit procedure | stable keys across outputs |
| `unicorn_level`, `unicorn_text` | `phase` / `block` / `instruction`; the logged text | UNICORN's own words, never lost |
| `instance` | 1, 2, ... | a phase run more than once (Equilibration twice) |
| `cycle`, `cycle_source` | 1, 2, ... or empty; `caller`, `chromatogram`, `scouting` | cycles, however they were recorded (3.3) |
| `chromatogram` | the original chromatogram the segment was recorded in | results with several chromatograms (3.4) |
| `state` | `running`, `held`, `paused` (ISA-88), with `unicorn_state` | pauses and holds (3.7) |
| `start`, `end` | wall s, elapsed s, volume ml, CV, UTC | every axis |
| `equipment_module` | `system_pump`, `injection_valve`, ... | phase level only |

Each value inside a segment:

| Field | Values |
|---|---|
| `value`, `unit` | full precision; the unit as a UCUM code (`mL/min`, `%`, `mL`, `s`; CV as the annotation `{CV}`) |
| `kind` | `setpoint` (asked for), `actual` (measured), `derived` (calculated) |
| `source` | `run_log`, `volume_axis`, `curve`, `system_configuration`, `method_hint`, `caller`, `default`, `not_known` |
| `quality` | `exact`, `inferred` (e.g. an unlogged pause), `assumed` (e.g. `B1 (assumed)`), `folded` (display only, with `held_over`) |

## 6. How it fits CADET-Process 0.13 and CADET-GUI

Read in the code of CADET-Process `dev` (`e11f82e`, 2026-11-13) and CADET-GUI `main`
(`bb5539e`, 2026-10-28). CADET-Process's release notes announce `instruments`,
`calibration` and `characterization` for 0.13.0 (no date); `pycorn[cadet]` is on 0.12 today.
Their API may still change.

CADET-Process asks in its forum whether its vocabulary (phase, valve event) fits how users
see their instrument; the mapping in 3.5 is a ready answer.

| Record | CADET-Process 0.13 (`instruments`) | CADET-GUI |
|---|---|---|
| procedure / unit procedure | `PhasedProcess(name, flow_sheet, steps)` | a study / run |
| operation (UNICORN phase) | none (a `Phase` has no name) | markers: "Phase ..." run-log texts, on a volume axis only |
| step / section, running | `Phase` (flow, %B as `{"A": 1-b, "B": b}`, ramp via `composition_end`) | "Phase <i>" spans from the template |
| held (UNICORN Pause) | `Phase(d, 0.0, {})` (allowed: composition is checked only when flow > 0) | not represented (one constant flow) |
| paused (UNICORN Hold) | `Phase` with `composition_start = composition_end` | not represented |
| injection valve setting | `ValveEvent("load" / "inject" / "direct_inject" / "run")` | none |
| equipment modules | `LCFlowSheet`: sample loop, mixer, four tubings, column, `bypass_units` | instrument templates |
| `process_action` | templates `PulseInjection`, `Step`, `StepElution`, `LWE`, `Breakthrough` | experiment types (system pulse, column pulse, LWE, breakthrough, ...) |
| measured curves | `ReferenceIO(time, solution, flow_rate)`, SI by convention, no unit field | `ExperimentalRun` / `Channel`, units dropped |
| feed start | - | `Comparison.injection_marker` (a text prefix) |

**What the model view would lose today** (gaps to raise with CADET, none to work around in
PyCORN by guessing):

- One flow per `Phase`: UNICORN flow ramps can't be expressed (a flow at phase end would do).
- Duration must be > 0 and two `ValveEvent`s in a row are rejected: zero-length steps are
  dropped, with a warning naming their `path`.
- No name on `Phase`: the `path` and UNICORN's text can't travel with the steps, so the GUI
  can't align on UNICORN's phases. Ask for an optional `name`.
- Buffers A-D are mixer inlets (a quaternary pump), not AKTA's A/B pumps with inlets
  A1-A4/B1-B4; `bypass_units` is fixed, so a column by-pass partway through a run has no
  counterpart.
- `ReferenceIO` has no unit or metadata, and every `calibration` function returns a new one:
  the record's `unit`, `source`, `quality` stay in `experiment.json` beside it.
- CADET-GUI reads `.csv` only, forces a volume axis in mL even for a `min` export, can't save
  a time-axis run with markers, and converts with one typed flow. A record-based import would
  carry time in s, markers from operations, units, and the injection time as a number.

**What the record supplies that CADET doesn't read from a run:** column geometry and Vc,
mixer volume from the stored system configuration, inlet positions (known / assumed / not
known), pauses and holds as states, the executed sections with their source. Loop volume
stays the caller's (D19); `process_action` and the template stay the caller's, even though
CADET's templates are exactly the process model ISA-88 describes.

## 7. Output formats

In order of value:

1. **The record as JSON with ISA-88 names** (`pycorn record --json`, `schema: 1`, masked by
   default like every `--json`). Element names follow ISA-88 Part 4 / BatchML (procedure,
   unit procedure, operation, phase elements; events; parameters with values and units), so
   a later BatchML XML writer is a mapping, not a redesign. JSON first because the rest of
   PyCORN's machine output is JSON and agents read it directly.
2. **Units as UCUM codes** and times as ISO 8601 (UTC instants, durations) in the record and
   its JSON; display keeps today's column names (`_min`, `_ml`).
3. **BatchML XML** (later, optional): only if a user's MES or batch-record system wants it.
4. **Allotrope Simple Model (ASM), liquid chromatography** (later, optional): for chromatogram
   and peak results going to an ELN or LIMS. It covers the measurement, not the procedure, so
   it sits beside the record, not instead of it.
5. CADET keeps its own formats.

Masking and "never print connection values" apply to every format.

## 8. Plan

Each step adds to what exists: today's outputs stay byte-identical until a step says
otherwise.

1. **Record** (`pycorn/run_record.py`): built from `run_log_sections`, the timeline, the
   system configuration and the curves; ISA-88 levels, states, tags, setpoint / actual /
   derived values with source and quality. Tests: today's phase table, sections and CADET
   inputs re-derived from the record equal today's outputs.
2. **Rules** (`pycorn/step_rules.py`): the decided phase-start rule and the other display
   rules as they are decided; one function per view.
3. **Display view**: the phase-start fold and repeat names in the phase table, Excel and the
   page, plus the M22 lead-in skip. A changed output gets a CHANGELOG entry.
4. **Steps table** for modelers, from the record.
5. **`pycorn record --json`** (and `inspect` gains the record's operation list), masked by
   default.
6. **Model view** reads `actual` values (the decided CADET flow source).
7. When CADET-Process 0.13.0 ships: the model view as `PhasedProcess` steps on an
   `LCFlowSheet` (needed for `characterization`, which takes an `LCProcess`), replacing
   PyCORN's own flow sheet and event compiler, after raising the gaps in section 6.
8. Later, on request: cycles and series links (3.1, 3.3) once a sample shows them; BatchML
   XML; Allotrope ASM.

## 9. Open questions and test cases

- **Several chromatograms** (3.4): how the run log records New Chromatogram; whether
  settings carry across the cut; whether storage really splits the run log per
  chromatogram. Needs a short test method with the instruction partway through a phase.
- **Cycles** (3.3): how the caller marks them; real multi-cycle runs with and without a new
  chromatogram per cycle, and a scouting series.
- **Production links** (3.1): the label the caller gives for a lot; connected skids;
  multi-column continuous chromatography.
- **Terms** (checked 2026-11-14 against ISA-88.01-1995 and the BatchML schemas): batch,
  lot, train, unit procedure, operation and the states are quoted from the standard;
  "cycle", "campaign" and "series" are this proposal's own terms. Still to check: the 2010
  edition's wording, and IEC 62264's own definitions behind B2MML's material lots.
- The samples that would settle these are listed in `samples/SAMPLE_ZIP_CONTENTS.md`,
  "Coverage and wanted samples".

## 10. Limits

- One procedure per result, on one system; one unit procedure by default, several only when
  the caller marks cycles or chromatograms. Links between results are links, never merged
  records.
- Multi-column continuous chromatography on one skid is not supported.
- `process_action` stays empty without a caller mapping: UNICORN's names are free text.
- The volume axis counts as "actual" only because the survey (review 3.4) shows it is the
  flow the pump delivered; otherwise it would be a derived value and the run log the
  model's source.
- Several chromatograms in one result: built on the owner's statement from memory, not on
  an example.
- CADET-Process's `instruments` and CADET-GUI are unreleased; section 6 is rechecked when
  0.13.0 ships.

## 11. Decisions this builds on, and the open Decide items

This document reworks [`RUN_STRUCTURE_REVIEW.md`](RUN_STRUCTURE_REVIEW.md) (topics
3.1-3.12, Decide 1-26).

**Decisions already made** that the design keeps (ROADMAP "Decisions", the review):

- Sections from what ran in the run log (D13); the method only as hints (D17); each
  instruction holds until the next.
- Column flow by valve position (D14); feed by valve, caller loop and path volumes, never
  assumed (D16-D21, D27); no defaults that mirror CADET parameters (D28).
- Wall clock for the model, pauses as zero-flow stretches (D5, D7); Hold freezes %B.
- Units in column names, SI for the model (review 3.3); x-axis picker and tracks on the page.
- Inlets from the stored system configuration: known / assumed / not known (Decide 22).
- Phase start lag (2026-11-12): display folds flow, %B and %Q within 10 s and 1 % of the
  phase with a `held_over` note; valves and feed never fold; phase start and end never
  move; M22 windows from a phase start skip the held-over seconds; CADET never folds.
- CADET flow source (2026-11-14, Decide 6, review 3.4): run log for structure and timing,
  the method-flow entry for which pump, the volume axis for the amount where it differs by
  more than 0.01 ml and 1 %, a fitted ramp inside such a section, then D14's zeroing.
- Unknowns said in plain words, never guessed; masking rules (Track 9); full values in code,
  rounding at display only.

**Open Decide items the frame settles** (proposed answers; confirm as a set):

| # | Item | Proposed answer |
|---|---|---|
| 1 | steps level | yes: the ISA-88 step (constant state); `chrom.step_table()`, `<c>_steps.csv` |
| 2 | keys | yes: `phase_number`, `step_number`, and the `path` tag |
| 3 | ISA-88 terms in columns | the record's JSON uses ISA-88 words beside UNICORN's; display tables and CSV keep UNICORN's words only |
| 9, 10 | held-over note; "at the start" | decided 2026-11-12 (display) |
| 11 | pauses in steps | `state = held` rows (UNICORN Pause = ISA-88 Held), `unicorn_state = pause` |
| 13 | inferred pauses | same rows, `quality = inferred`; display marks them ("about 40 s, not logged") |
| 14 | holds | `state = paused` (UNICORN Hold), a note per phase in display |
| 16 | by-pass | an equipment state of the column valve module; display note |
| 20 | ramp length | both: `setpoint` (logged length) and `actual` (measured on each axis); display shows the actual |
| 23 | repeats | `instance` numbers; display `Equilibration (2)`; the logged text kept |
| 24 | warnings per phase | warnings carry the `path` they belong to; display puts them on the phase row |

**Display choices still the owner's**: 12 phase length with or without pauses (proposal:
both), 15 which flow users see, 17 feed wording, 18 feed volume shown, 21 the gradient
wording rule, 25 section-lane chart, 26 summary beside or instead of today's sequences.
With pressure control a flow increase still ramps about 10 s after the instruction, longer
than the fold cutoff, so the `held_over` note should also say "ramped n s".

**Tooling gap:** Decide 19, `pycorn export --loop-volume` / `--sample-path-volume` for the
CADET feed (one flag or two).

## References

Read 2026-11-14. The AVEVA pages were seen only as search excerpts (they need a browser);
open them before quoting them outside this draft.

ISA-88, ISA-106 and BatchML:

- ISA-88.01-1995, *Batch Control Part 1: Models and Terminology*: definitions 3.5 (batch),
  3.28 (lot), 3.59 (train), 3.61 (unit procedure); 4.1 (continuous processes not
  specifically addressed); 5.1.2.2 (operations in a single unit); 5.7.2 (example states).
  The 2010 edition (ANSI/ISA-88.00.01-2010) was not checked.
- ISA-TR106.00.01-2013 and ANSI/ISA-106.00.01-2023, *Procedure Automation for Continuous
  Process Operations*: https://www.isa.org/products/isa-tr106-00-01-procedure-automation-for-continuou
- BatchML batch production record schema (`BatchID`, `LotID`, `CampaignID`):
  https://raw.githubusercontent.com/MESAInternational/B2MML-BatchML/master/Schema/BatchML-BatchProductionRecord.xsd
- ANSI/ISA-88.00.04-2006, *Batch Control Part 4: Batch Production Records* (ISA, preview):
  https://www.isa.org/getmedia/4b46d97c-6954-4936-97f5-db672f3fd154/S_880004_preview.pdf
  ("a reference model for batch production records")
- MESA International, B2MML-BatchML (BatchML: "an XML implementation of the ANSI/ISA-88,
  Batch Control, family of standards", IEC 61512):
  https://github.com/MESAInternational/B2MML-BatchML

CADET-Process multi-column builders (`CarouselBuilder`, `SMBBuilder`):
https://github.com/fau-advanced-separations/CADET-Process/blob/dev/CADETProcess/modelBuilder/carouselBuilder.py

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
