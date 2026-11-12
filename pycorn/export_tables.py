# -*- coding: utf-8 -*-
"""
Names and origins of every exported table (owner, 2026-11-09): one human-friendly name per
table, the same in the in-browser page, the Excel sheets and the CSV files, and for every
table and column whether it is UNICORN's data or PyCORN's analysis.

- `title(key)`: the name people see (page section, Excel sheet; at most 31 characters, no
  ':' as Excel requires). `file_stem(key)`: the same name for CSV files, lower case with
  '_' ('Planned vs run - blocks' -> `planned_vs_run_blocks`).
- Origins (owner, 2026-11-09): `UNICORN` (stored data, only reshaped), `USER` (a value the
  user gave, as given), `CALCULATED` (a fixed formula on stored values and user inputs:
  anyone can redo it, and its `check` says how), `ANALYSIS`
  (PyCORN's rules and choices: Vc, pauses, phase and section ends, ...), `MIXED` (a table
  with stored and PyCORN columns), `PYCORN` (about the export itself: version, options, the
  guide). A column is `CALCULATED` only if every input is stored or calculated; one
  `ANALYSIS` input makes it `ANALYSIS` (CV from the chosen Vc, UTC from the rebuilt pauses,
  a row's end and length).
- `TABLES[key].origin`: the table's origin. `column_origin(key, column)`: (origin, meaning)
  of one column; `column_check(key, column)`: how to check a calculated column;
  `column_guide(frames)`: the 'Column guide' table, one row per column of the tables written.
- `TABLES[key].shown` (owner, 2026-11-09): the columns the in-browser page shows by default
  for a wide table (empty: every column); people pick more there. Downloads always hold every
  column.
- `TABLES[key].freeze_through`: the last frozen column (the page keeps every column up to it at
  the left of a wide table; empty: only the first).

Internal keys (`hint_blocks`, `run_info`, ...) stay as they are in the code; only what is
written or shown takes the title. CADET files keep CADET's own names (not listed here).
"""
import re
from collections import OrderedDict
from typing import NamedTuple

UNICORN = "UNICORN"
USER = "User input"
CALCULATED = "PyCORN calculated"
ANALYSIS = "PyCORN analysis"
MIXED = "UNICORN + PyCORN"
PYCORN = "PyCORN"
ORIGIN_NOTES = OrderedDict([
    (UNICORN, "data as UNICORN stored it, only reshaped into a table"),
    (USER, "a value you gave (option or field), as given; not from UNICORN"),
    (CALCULATED, "a fixed formula on stored values and your inputs: redo it or compare it with UNICORN (the Column "
                 "guide's 'check' says how)"),
    (ANALYSIS, "PyCORN's rules and choices (Vc, pauses, phases, sections, ...): no UNICORN number to "
               "compare with; check it before relying on it"),
    (MIXED, "stored columns next to PyCORN's (see the Column guide)"),
    (PYCORN, "about the export itself: the PyCORN that wrote it, how, and what each column means"),
])


class Table(NamedTuple):
    title: str
    origin: str
    note: str
    columns: dict  # column -> (origin, meaning, check); columns not listed take `default`
    default: tuple  # (origin, meaning, check) of a column not listed
    shown: tuple = ()  # columns the page shows by default (empty: all); downloads hold all
    freeze_through: str = ""  # the page freezes the columns up to and including this one (empty: the first only)


def _u(meaning):
    return UNICORN, meaning, ""


def _y(meaning):
    return USER, meaning, ""


def _c(meaning, check):
    """A calculated column: `check` says how to redo it or where UNICORN shows the same."""
    return CALCULATED, meaning, check


def _a(meaning):
    return ANALYSIS, meaning, ""


def _x(meaning):
    return PYCORN, meaning, ""


_AXES = OrderedDict([
    ("time", _u("elapsed method time, as stored (stops during a pause)")),
    ("volume", _u("volume pumped, as stored")),
    ("time_adjusted", _c("time with zero at the injection UNICORN uses for its plots",
                         "UNICORN's plot axis after the injection: stored time - injection time")),
    ("volume_adjusted", _c("volume with zero at the injection UNICORN uses for its plots",
                           "UNICORN's plot axis after the injection: stored volume - injection volume")),
    ("cv", _a("column volumes through the column since method start (volume through the column / Vc)")),
    ("utc", _a("wall-clock time in UTC, rebuilt from the method start and the run log's pauses")),
])
_STEP_TIMES = OrderedDict([
    ("start_s", _a("start, wall-clock seconds from method start (pauses included)")),
    ("end_s", _a("end, wall-clock seconds from method start (pauses included)")),
])

_EVENT_TABLE = OrderedDict([
    ("number", _c("row number; 0 is the stretch before the first one", "rows counted in time order")),
    ("name", _u("the event text UNICORN logged")),
    ("text", _u("the full event text, as stored")),
    ("event_curve", _u("the UNICORN event curve the row comes from")),
    ("kind", _a("phase, injection or fraction")),
    ("pseudo", _c("true for row 0, the stretch before the first one", "row number 0")),
    ("start_time", _c("start on the stored time axis", "the event's stored time")),
    ("end_time", _a("end on the stored time axis")),
    ("start_volume", _c("start on the stored volume axis", "the event's stored volume")),
    ("end_volume", _a("end on the stored volume axis")),
    ("start_cv", _a("start in column volumes")), ("end_cv", _a("end in column volumes")),
    ("start_utc", _a("start, UTC")), ("end_utc", _a("end, UTC")),
    ("length_time", _a("end - start, time")), ("length_volume", _a("end - start, volume")),
    ("length_cv", _a("end - start, column volumes")),
    ("time_unit", _u("unit of the time columns")), ("volume_unit", _u("unit of the volume columns")),
    ("end_source", _a("what ends the row: the next row, the end of the run, ...")),
    ("blocks", _a("method blocks that ran in it (run log)")),
    ("system_flow_ml_min", _a("system flow set-points in time order (run log)")),
    ("column_flow_ml_min", _a("flow through the column in time order (by-pass and valves applied)")),
    ("percent_b", _a("%B set-points in time order; a ramp reads 'ramp 0 to 100'")),
    ("percent_q", _a("Q1-Q4 set-points in time order (quaternary systems)")),
    ("inlets", _a("inlets drawn from in time order: line A below 100 %B, line B above 0 %B, a constant "
                   "mix 'A1+B1', a %B ramp 'ramp A1 to B1', quaternary shares as logged ('Q1 50%+Q2 50%'); "
                   "'Loop' while the injection valve is at Inject, the sample inlet at Direct inject. A logged "
                   "position as logged ('B2'); a line with no inlet valve in the run's stored system "
                   "configuration as the line alone ('B'); a valve with no position logged yet at its first "
                   "position, '(assumed)' ('B1 (assumed)', sample inlet 'Buffer (assumed)'); '(not known)' "
                   "when the configuration doesn't say ('B (not known)'). Quaternary lines are taken as "
                   "feeding the A and B pumps (provisional)")),
    ("inlets_source", _a("how sure the inlets are, in time order: known (logged, or a line with no valve), "
                          "assumed (a valve's first position, none logged yet) or not known (the system "
                          "configuration doesn't say); the least sure line used decides")),
    ("column_position", _a("column valve positions in time order")),
    ("injection_valve", _a("injection valve positions in time order")),
    ("sample_use", _a("feed, to_bypass or buffer while the injection valve sends sample")),
    ("set_marks", _a("Set mark texts in time order")),
])
_POOLS = OrderedDict([
    ("number", _c("pool number, in time order", "pools counted in time order")),
    ("pool", _c("the pool's name: its first and last fraction ('3.A.2-3.A.3')", "first_fraction-last_fraction")),
    ("fractions", _a("the fractions of the Fractions table that start inside the pool (JSON list)")),
    ("n_fractions", _a("how many fractions it holds")),
    ("first_fraction", _u("the pool's first fraction, as stored (EventText)")),
    ("last_fraction", _u("the pool's last fraction, as stored (EventText2)")),
    *((k, v) for k, v in _EVENT_TABLE.items() if k.startswith(("start_", "end_", "length_"))),
    ("end_time", _a("end on the stored time axis: the pooled curve's next mark")),
    ("end_source", _a("what ends the pool: 'next' (the pooled curve's next mark) or 'run_end'")),
    ("event_curve", _u("the pooled fraction curve (stored type Fraction, made in evaluation)")),
    ("time_unit", _EVENT_TABLE["time_unit"]), ("volume_unit", _EVENT_TABLE["volume_unit"]),
])
_INJECTIONS = OrderedDict(_EVENT_TABLE, name=_c("'Injection 1', 'Injection 2', ...: UNICORN logs no text "
                                                "for an injection", "injections counted in time order"))

_SECTIONS = OrderedDict([
    ("start_s", _STEP_TIMES["start_s"][1]), ("end_s", _STEP_TIMES["end_s"][1]),
    ("elapsed_start_s", "start on the stored method time (s; stops during a pause)"),
    ("elapsed_end_s", "end on the stored method time (s)"),
    ("paused", "true for a pause"), ("phase", "the phase it is in"), ("block", "the method block it is in"),
    ("system_flow_ml_min", "system flow set-point"), ("sample_flow_ml_min", "sample pump flow set-point"),
    ("column_flow_ml_min", "flow through the column (by-pass and valves applied)"),
    ("percent_b", "%B at the start"), ("percent_b_slope_per_s", "%B change per wall-clock second (ramps)"),
    ("percent_q", "Q1-Q4 at the start and their slopes"),
    ("column_position", "column valve position"), ("bypass", "true while the column is by-passed"),
    ("injection_valve", "injection valve position"), ("outlet_valve", "outlet valve position"),
    ("feed", "true while sample goes to the column"), ("sample_use", "feed, to_bypass or buffer"),
    ("inlets", "inlets drawn from (as in the phase table): logged positions, a line with no valve as "
               "the line alone ('B'), a valve's first position '(assumed)', '(not known)' when the "
               "system configuration doesn't say"),
    ("inlets_source", "known, assumed or not known (as in the phase table)"),
    ("wavelengths_nm", "UV wavelengths set; channel n = n-th value, 0 = off"),
    ("set_marks", "Set mark texts at its start"), ("changes", "run-log instructions at its start"),
    ("system_flow_measured", "check: system flow from the curve, against the set-point"),
    ("sample_flow_measured", "check: sample flow from the curve, against the set-point"),
    ("percent_b_max_deviation", "check: largest difference of the Conc B curve from the %B built here"),
    ("percent_q_max_deviation", "check: largest difference of the Q curves from the Q built here"),
])

_PAUSES = OrderedDict([
    ("elapsed_s", "stored method time when it paused (s)"), ("start_utc", "pause start, UTC"),
    ("end_utc", "pause end, UTC"), ("duration_s", "length (s)"), ("source", "where the pause was found"),
    ("quality", "how sure the timing is"), ("pause_index", "run-log row of the pause"),
    ("resume_index", "run-log row of the continue"), ("cause", "why it paused, when logged"),
])

_CONCENTRATION = OrderedDict([
    ("curve", _u("the UV curve")), ("mg_ml_curve", _a("the mg/ml curve made from it")),
    ("wavelength_nm", _a("its wavelength, from the run log's Wavelength instruction")),
    ("wavelength_source", _a("where the wavelength came from")),
    ("epsilon_ml_per_mg_cm", _c("extinction coefficient used, ml/(mg*cm), after the response factor",
                                "epsilon in ml/(mg*cm) x response factor")),
    ("epsilon", _y("the coefficient you gave, with its unit")),
    ("response_factor", _y("the factor you gave")),
    ("path_length_cm", _a("UV path length used")), ("path_length_basis", _a("where the path length came from")),
])

_BYPASS_UV = OrderedDict([
    ("window", "number of the by-pass window"), ("start_s", _STEP_TIMES["start_s"][1]),
    ("end_s", _STEP_TIMES["end_s"][1]), ("elapsed_start_s", "start on the stored method time (s)"),
    ("elapsed_end_s", "end on the stored method time (s)"), ("blocks", "method blocks that ran in it"),
    ("valve", "injection valve position"), ("valve_ml", "volume through the valve in it"),
    ("curve", "the UV curve"), ("wavelength_nm", "its wavelength"), ("wavelength_source", "where the wavelength came from"),
    ("raw_unit", "the curve's stored unit"), ("kind", "plateau, pulse or no_signal"),
    ("before", "UV level before the window"), ("max", "highest UV in it"), ("plateau_s", "length of the plateau (s)"),
    ("area_au_ml", "peak area (AU*ml)"), ("loop_volume_ml", "the loop volume you gave"),
    ("path_length_cm", "UV path length used"), ("path_length_basis", "where the path length came from"),
    ("absorbance_per_cm", "absorbance per cm of path"), ("sample_concentration_mg_ml", "the concentration you gave"),
    ("epsilon_ml_per_mg_cm", "the extinction coefficient this gives"), ("epsilon_from", "plateau or pulse"),
    ("warnings", "what to check for this row"),
])

_PHASE_WINDOW_CHECK = "the curve's points with start <= axis <= end (this row's start_/end_ on its axis), NaN left out"
_PHASE_WINDOWS = OrderedDict([
    ("run", _u("result name")), ("chromatogram", _u("chromatogram name")),
    ("window", _y("the window's name, as you gave it (--window NAME=...)")),
    ("phase", _a("the phase, as the run log names it. INTERIM phase rule (until the lead-in and repeats "
                 "decisions): phase start and end as logged, no lead-in folding")),
    ("occurrence", _a("which run of the phase: 1, 2, ... in time order (INTERIM: one row per occurrence)")),
    ("phase_number", _a("the phase's row number in the Phases table")),
    ("axis", _y("the window's axis (time, volume or cv), from the unit you gave")),
    ("window_unit", _y("the unit you gave the offsets in")),
    ("start_anchor", _y("what start_offset counts from: phase start or phase end")),
    ("start_offset", _y("the window's start as you gave it, from start_anchor")),
    ("end_anchor", _y("what end_offset counts from: phase start or phase end")),
    ("end_offset", _y("the window's end as you gave it, from end_anchor")),
    ("start_time", _a("window start on the stored time axis, clipped to the phase; on other axes than the "
                      "window's, mapped through a curve's stored axes")),
    ("end_time", _a("window end on the stored time axis, clipped to the phase")),
    ("start_volume", _a("window start on the stored volume axis, clipped to the phase")),
    ("end_volume", _a("window end on the stored volume axis, clipped to the phase")),
    ("start_cv", _a("window start in column volumes (empty without Vc)")),
    ("end_cv", _a("window end in column volumes")),
    ("time_unit", _u("unit of the time columns")), ("volume_unit", _u("unit of the volume columns")),
    ("curve", _u("the curve, by its stored name (picked by name or role)")),
    ("curve_unit", _u("the curve's unit; the statistics are in it")),
    ("curve_precision", _u("decimals UNICORN shows the curve with (AmplitudePrecision); values are not rounded")),
    ("n", _c("number of curve points in the window (NaN left out)", _PHASE_WINDOW_CHECK)),
    ("mean", _c("mean of those points", _PHASE_WINDOW_CHECK)),
    ("median", _c("median of those points", _PHASE_WINDOW_CHECK)),
    ("sd", _c("sample standard deviation (n - 1) of those points; empty for n < 2", _PHASE_WINDOW_CHECK)),
    ("min", _c("smallest of those points", _PHASE_WINDOW_CHECK)),
    ("max", _c("largest of those points", _PHASE_WINDOW_CHECK)),
    ("warnings", _a("what to check for this row: clipped to the phase or run end, empty window, curve read on "
                    "another axis or not covering the window (JSON list)")),
])

_BASE = _c("the block's base unit (volume, CV or time)",
           "the block's Base instruction; 'SameAsMain' takes the main block's")
_HINT_BLOCKS = OrderedDict([
    ("block", _u("method block name")), ("start_s", _STEP_TIMES["start_s"]),
    ("end_s", _STEP_TIMES["end_s"]), ("base", _BASE),
    ("planned", _u("block length the method set (End_Block), with the run's variable value")),
    ("variable", _u("the method variable that sets the length")),
    ("executed", _a("length that ran, in the base unit (set-point flows, pauses excluded)")),
    ("executed_ml", _a("volume that ran (ml)")), ("status", _a("as_planned, shorter or longer")),
    ("cause", _a("next_breakpoint, watch_in_block or hold_or_pause, where the log shows one")),
])

_HINT_INJECTIONS = OrderedDict([
    ("block", _u("method block name")), ("position", _u("injection valve position the method sets")),
    ("base", _BASE),
    ("planned", _c("planned valve period (planned_end - planned_start)", "planned_end - planned_start")),
    ("planned_start", _u("breakpoint that turns the valve")), ("planned_end", _u("breakpoint that ends it")),
    ("planned_end_at", _a("what ends it: the next valve instruction or End_Block")),
    ("variable", _u("the method variable that sets the end")),
    ("start_s", _a("valve turned, wall-clock s from method start")), ("end_s", _a("valve turned back (s)")),
    ("executed", _a("valve period that ran, in the base unit")), ("executed_ml", _a("volume through the valve (ml)")),
    ("bypass_ml", _a("of it, volume while the column was by-passed (ml)")),
    ("pre_switch_s", _a("seconds from the breakpoint to the logged valve switch")),
    ("pre_switch", _a("amount pumped in that gap, in the base unit")),
    ("gap_allowance", _a("shortfall allowed: the gap plus tolerance")),
    ("status", _a("as_planned, short, over or not_executed")),
    ("next_breakpoint", _c("true when a Next breakpoint was pressed in the block",
                           "a 'Next breakpoint' entry in the block's run log")),
    ("source", _x("'method': read from the method, a hint only")),
])

_RUN_INFO = OrderedDict([
    ("chromatogram", _u("chromatogram name")),
    ("chromatogram_kind", _a("original (the run's own), imported (from another result in Evaluation) or derived "
                             "(made in Evaluation)")),
    ("source_result_id", _u("imported chromatogram: the ResultID it came from (OriginalResultID)")),
    ("source_result_name", _u("imported chromatogram: the result name it came from (OriginalResultName)")),
    ("copy_of_run", _a("imported chromatogram: true when it is a copy of this same run")),
    ("run_name", _u("result name")),
    ("unicorn_version", _u("UNICORN version that wrote the result")),
    ("vc", _a("column volume (Vc) PyCORN uses")), ("vc_unit", _a("its unit")),
    ("vc_source", _a("where Vc came from (run log, column type, geometry, ...)")),
    ("vc_location", _a("where in that source")), ("vc_candidates", _a("every Vc found, with its source")),
    ("vc_disagreements", _a("candidates that differ from the one used")),
    ("column_positions", _a("column valve positions used in the run")),
    ("column_name", _u("column type name stored with the run")),
    ("column_length_m", _a("bed height (m) from the column type, or yours")),
    ("column_diameter_m", _a("column diameter (m)")), ("particle_radius_m", _a("particle radius (m)")),
    ("geometry_volume_m3", _a("bed volume from height and diameter (m3)")),
    ("geometry_sources", _a("where each geometry value came from")),
    ("geometry_vs_vc", _a("relative difference of the geometry volume from Vc (a check)")),
    ("column_hints", _a("other column fields stored, as hints")),
    ("timeline_quality", _a("how well the UTC timeline fits the run log")),
    ("start_utc", _a("method start, UTC")), ("start_source", _a("where the start time came from")),
    ("local_utc_offset_min", _u("the instrument computer's UTC offset (min)")),
    ("end_utc", _a("method end, UTC")), ("end_elapsed_s", _u("stored method time at the end (s)")),
    ("total_paused_s", _a("seconds paused")), ("n_pauses", _a("number of pauses")),
    ("residual_s", _a("timeline check: seconds not explained by the pauses")),
    ("unplaced_s", _a("pause seconds not placed in time")), ("valid_until_s", _a("UTC trusted up to this time (s)")),
    ("retention_zero_adjusted", _u("whether UNICORN plots with zero at an injection")),
    ("retention_zero_injection", _u("the injection number UNICORN zeroes its plot axis at")),
    ("retention_zero_time", _u("stored time at that injection; adjusted time = time - this")),
    ("retention_zero_volume", _u("stored volume at that injection; adjusted volume = volume - this")),
    ("retention_zero_source", _a("where the zero was found: the stored setting, the peak table or a default")),
    ("export_stamp", _x("what tells a later export the run changed")),
    ("match_key", _a("key that matches copies of a run")),
    ("pycorn_version", _x("PyCORN version and git commit that wrote this export")),
    ("export_route", _x("how the export was asked for: CLI, web page or Python")),
    ("export_options", _x("every export option that differs from this version's defaults (JSON); "
                          "provenance.json and README.txt hold the full record")),
])

# the page's default columns of the wide tables (owner, 2026-11-09); 'valves' = both valve columns
_RAN = ("blocks", "inlets", "column_position", "injection_valve", "set_marks", "time_unit", "volume_unit")
_SHOWN_PHASES = ("number", "name", "start_time", "length_time", "start_volume", "length_volume", "length_cv",
                 "column_flow_ml_min", "percent_b") + _RAN
_SHOWN_INJECTIONS = ("number", "name", "start_time", "length_time", "start_volume", "length_volume", "length_cv",
                     "column_flow_ml_min", "sample_use") + _RAN
_SHOWN_FRACTIONS = ("number", "name", "pool", "start_volume", "end_volume", "length_volume", "start_cv", "start_time") + _RAN

_TABLES = [
    ("summary", Table("Summary", MIXED, "what the run is, how many warnings to check, and what each sheet holds",
                      {}, _a(""))),
    ("chart", Table("Chart", MIXED, "the stored curves as an Excel chart, with PyCORN's injection and fraction marks",
                    {}, _a(""))),
    ("curves", Table("Curves", MIXED, "every curve: stored time, volume and value, with PyCORN's column volumes "
                     "(CV) and UTC; the adjusted axes are time and volume minus the retention zero "
                     "(Column and timeline)", {}, _u(""))),
    ("events", Table("Events", MIXED, "every stored event (logbook, injections, fractions, ...) on the same axes",
                     OrderedDict([("event_curve", _u("the UNICORN event curve")),
                                  ("event_curve_type", _u("its stored type")), *_AXES.items(),
                                  ("text", _u("event text")), ("type", _u("event type")),
                                  ("subtype", _u("event subtype")),
                                  ("instruction_feedback", _u("instruction feedback, as stored"))]), _u("as stored"))),
    ("peaks", Table("Peaks", MIXED, "peak tables stored with the run, one row per peak; retentions count from "
                    "the injection in retention_zero_injection, as UNICORN shows them",
                    OrderedDict([("peak_table", _u("the peak table's name")),
                                 ("number", _c("the peak's number in its table (UNICORN's 'No')",
                                               "peaks counted in stored order")),
                                 ("retention_unit", _u("unit of the retention, width and volume columns")),
                                 ("area_unit", _u("unit of Area")), ("height_unit", _u("unit of the height columns")),
                                 ("retention_zero_injection", _u("the injection the retentions count from (empty: "
                                                                 "from method start)")),
                                 ("max_peak_on_run_axis", _c("the peak maximum on the run's stored volume (or "
                                                             "time) axis, where the curves put it",
                                                             "MaxPeakRetention + retention_zero_volume (Column and "
                                                             "timeline)"))]),
                    _u("peak table column, as stored"),
                    ("number", "MaxPeakRetention", "retention_unit", "Area", "area_unit", "PercentOfTotalPeakArea",
                     "Height", "height_unit", "StartPeakVial", "MaxPeakVial", "EndPeakVial", "max_peak_on_run_axis"))),
    ("peak_summary", Table("Peak table summary", UNICORN, "each peak table's header, as UNICORN's report prints it "
                           "under the peaks: totals, baseline, column, settings",
                           {"peak_table": _u("the peak table's name"),
                            "field": _u("the field, named as UNICORN's report names it (else as stored)"),
                            "value": _u("its value as stored; curve numbers given as the curve's name; 'Baseline "
                                        "name' and 'Resolution algorithm name' as UNICORN's report prints them "
                                        "(only where a report showed the name); 'UV normalized to 1 cm path "
                                        "length' read from the evaluation log entry that made the table when not "
                                        "stored (warnings say when neither tells)"),
                            "unit": _u("its unit"), "stored_as": _u("the stored field it comes from (or the "
                                                                    "evaluation log entry)")},
                           _u("as stored"))),
    ("phases", Table("Phases", ANALYSIS, "one row per method phase: start, end and length on every axis, then flow, "
                     "%B, inlets, valves and set marks in time order; row 0 is the stretch before the first",
                     _EVENT_TABLE, _a(""), _SHOWN_PHASES)),
    ("injections", Table("Injections", ANALYSIS, "one row per injection: start, end and length on every axis, then "
                         "what ran; row 0 is before the first injection", _INJECTIONS, _a(""), _SHOWN_INJECTIONS)),
    ("fractions", Table("Fractions", ANALYSIS, "one row per fraction: start, end and length on every axis, then what "
                        "ran; row 0 is before the first fraction",
                        OrderedDict(_EVENT_TABLE, pool=_c("the pooled fraction it is in (Pooled fractions), empty when "
                                                          "none", "the pool's fractions in Pooled fractions")),
                        _a(""), _SHOWN_FRACTIONS)),
    ("pools", Table("Pooled fractions", ANALYSIS, "fractions pooled in UNICORN's evaluation, one row per pool: its "
                    "fractions, start, end and length on every axis; a pool ends where the pooled curve's next "
                    "mark starts", _POOLS, _a(""))),
    ("sections", Table("Run log sections", ANALYSIS, "process sections from the executed run log: flows, %B, valves, "
                       "holds (times in seconds), checked against the curves",
                       OrderedDict((k, _a(v)) for k, v in _SECTIONS.items()), _a(""))),
    ("pauses", Table("Pauses", ANALYSIS, "pauses the UNICORN clock stopped for (seconds)",
                     OrderedDict((k, _a(v)) for k, v in _PAUSES.items()), _a(""))),
    ("concentration", Table("Concentration", ANALYSIS, "mg/ml curves: extinction coefficient used and where it came "
                            "from", _CONCENTRATION, _a(""))),
    ("bypass_uv", Table("By-pass UV", ANALYSIS, "UV of sample sent to the column by-pass, and the extinction "
                        "coefficient it gives",
                        OrderedDict((k, _y(v) if k in ("loop_volume_ml", "sample_concentration_mg_ml")
                                     else _a(v)) for k, v in _BYPASS_UV.items()), _a(""))),
    ("phase_windows", Table("Phase window statistics", CALCULATED, "curve statistics (n, mean, median, SD, min, "
                            "max) over the windows you gave relative to a phase (--window), one row per phase "
                            "occurrence x window x curve. INTERIM phase rule: phase start and end as logged, no "
                            "lead-in folding, one row per occurrence of a repeated phase",
                            _PHASE_WINDOWS, _a(""),
                            ("run", "chromatogram", "window", "phase", "occurrence", "curve", "curve_unit", "n",
                             "mean", "median", "sd", "min", "max", "warnings"))),
    ("hint_blocks", Table("Planned vs run - blocks", ANALYSIS, "method blocks as planned vs as executed (hints only: "
                          "PyCORN reads the method, the run log decides)", _HINT_BLOCKS, _a(""))),
    ("hint_injections", Table("Planned vs run - injections", ANALYSIS, "injections as planned vs as executed (hints "
                              "only)", _HINT_INJECTIONS, _a(""),
                              ("block", "position", "base", "planned", "executed", "executed_ml", "status",
                               "pre_switch_s", "next_breakpoint"))),
    ("variables", Table("Method variables", MIXED, "method variables as the run used them, listed as UNICORN's "
                        "variable list: by phase and block, in method order",
                        OrderedDict([("phase", _u("the phase whose block first uses the variable (from the method)")),
                                     ("block", _u("the block that first uses it")),
                                     ("variable", _u("variable name")), ("value", _u("value, text as stored")),
                                     ("unit", _c("unit as UNICORN's variable list shows it: 'base' is the block's "
                                                 "base (CV, ml, min)", "stored_unit; 'base' from the block's Base "
                                                 "instruction (Method steps)")),
                                     ("number", _c("the value read as a number (empty when not a number)",
                                                   "the value text as a number")),
                                     ("kind", _u("stored kind (VariableValue, ScoutingVariableValue, ...)")),
                                     ("scouting", _c("true for a scouted variable (from its kind)",
                                                     "kind is a scouting kind")),
                                     ("stored_unit", _u("unit as stored with the value"))]), _u(""),
                        ("phase", "block", "variable", "value", "unit"))),
    ("run_details", Table("Run and method", UNICORN, "the result and the method: names, dates (with their UTC "
                          "offset) and by whom, technique, units, result naming, notes (method, start, run, "
                          "evaluation), evaluation procedures (selected or not), documentation", {},
                          _u("as stored"))),
    ("method_steps", Table("Method steps", MIXED, "the method as programmed, as UNICORN's report prints it: in call "
                           "order and indented, one row per instruction, with the variable values the run used",
                           {"step": _c("the row's place in the method text", "rows counted in printed order"),
                            "depth": _c("how deep the block is called from the main block (0 = main)",
                                        "the Block instructions that lead to it"),
                            "line": _c("the instruction as UNICORN's method text prints it: breakpoint, "
                                       "instruction, (value)#variable {unit}; indented by depth",
                                       "UNICORN's report, Method section"),
                            "phase": _c("the phase of the phase block that calls it", "the calling Block instructions"),
                            "path": _c("the blocks from the main block to this one", "the calling Block instructions"),
                            "called_from": _c("the step of the instruction that calls this block: a Block call, "
                                              "or a Watch whose action runs it (empty in the main block)",
                                              "the calling instruction's step"),
                            "breakpoint_variable": _u("the variable that sets the breakpoint; UNICORN stores it on "
                                                      "one instruction ('line' shows it there), and it moves "
                                                      "every instruction at that breakpoint"),
                            "base": _BASE,
                            "breakpoint_used": _c("the breakpoint as the run's variable value set it",
                                                  "the breakpoint variable's value in Method variables"),
                            "run_values": _c("the values the run used for the instruction's variables",
                                             "the variables' values in Method variables"),
                            "parameters": _u("parameters as stored: name: value unit [variable]; hidden ones left "
                                             "out, as UNICORN does")},
                           _u("as stored in the method"), ("phase", "block", "line"))),
    ("scouting_runs", Table("Scouting runs", UNICORN, "the scouting plan from the method: one row per planned "
                            "run, with the values typed into the method's scouting table. Every run in the series "
                            "has the same table, and it has no units. For the values this run used, with units, "
                            "see the rows marked scouting in Method variables",
                            {"run": _c("the run's number as UNICORN's report prints it ('Run 1'), from 1",
                                       "rows counted in stored order"),
                             "position": _u("the run's stored position in the scouting plan (from 0)"),
                             "included": _u("true if the run was ticked to run")},
                            _u("value typed into the scouting table, without its unit. Method variables has the "
                               "value this run used, with its unit"))),
    ("start_questions", Table("Start questions", UNICORN, "the method's start-protocol questions, as UNICORN's "
                              "report lists them, with the answer the run was started with",
                              {"number": _c("the question's number as UNICORN's report prints it ('No. 1')",
                                            "questions counted in stored order"),
                               "question": _u("the question, as stored"),
                               "answer_type": _u("the answer type as UNICORN's report prints it ('Textual input'); "
                                                 "the stored name where the report's text is not known"),
                               "answer": _u("the answer the run was started with (the result's QuestionAnswer); "
                                            "empty when none was given"),
                               "stored_answer_type": _u("the answer type as stored (AnswerType)"),
                               "method_position": _u("the stored position (MethodPos)")},
                              _u("as stored"))),
    ("column", Table("Column", MIXED, "the column type stored with the run, as UNICORN's report lists it: run "
                     "parameters, details, ordering information, then every other stored field",
                     {"column": _u("column type name"), "group": _u("the report's group; 'Other (as stored)' for "
                                                                    "the rest"),
                      "parameter": _u("the parameter as UNICORN's report names it (else the stored path)"),
                      "value": _c("the stored value; column volume and linear flows calculated", "source"),
                      "unit": _u("its unit"),
                      "source": _u("the stored field, or 'calculated: ...' with the formula")},
                     _u("as stored"), ("group", "parameter", "value", "unit"))),
    ("calibration", Table("Calibration", MIXED, "instrument calibrations (pH, conductivity, pressure), as UNICORN's "
                          "report lists them: per sensor, its reference values, then its constants",
                          {"date_utc": _c("date converted to UTC with the stored offset",
                                          "stored date - stored UTC offset"),
                           "kind": _u("reference (a calibration point) or constant (what the calibration gave)"),
                           "item": _c("'Reference value n', or the constant's stored description without its "
                                      "'; <unit>' ending", "the stored description"),
                           "unit": _c("the reference's stored unit, or the end of the constant's description",
                                      "the stored description after its last ';'")},
                          _u("as stored"), ("calibration", "date", "utc_offset_min", "operator", "kind", "item",
                                            "value", "unit"))),
    ("instrument", Table("Instrument", UNICORN, "the instrument: configuration, firmware, mixer, UV cell, every "
                         "module's serial, versions and run hours, enabled components (never IP addresses or ports)",
                         {}, _u("as stored"))),
    ("system_settings", Table("System settings", UNICORN, "system settings the run started with: alarms, "
                              "wavelengths, ...", {}, _u("as stored"))),
    ("audit_log", Table("Audit log", MIXED, "the result's audit trail: time, user and entry",
                        {"time_utc": _c("time converted to UTC with the stored offset",
                                        "stored time - stored UTC offset")}, _u("as stored"),
                        ("time_utc", "type", "operator", "entry"))),
    ("run_info", Table("Column and timeline", ANALYSIS, "column volume (Vc) and its sources, column geometry, UTC "
                       "timeline; one row per chromatogram", _RUN_INFO, _a(""))),
    ("warnings", Table("Warnings", ANALYSIS, "PyCORN's checks: things to look at before relying on the numbers "
                       "(not UNICORN's own messages)",
                       {"chromatogram": _u("chromatogram name"),
                        "source": _a("the PyCORN step that warned"), "category": _a("Vc, CV or UTC for axis warnings"),
                        "message": _a("what to check")}, _a(""))),
    ("column_guide", Table("Column guide", PYCORN, "every column of these tables: UNICORN data, PyCORN calculated "
                           "or PyCORN analysis, what it means and how to check it", {}, _x(""))),
]
# Where the page's frozen block ends in each table (owner, 2026-11-10): every column from the left
# up to and including this one stays in view while a wide table scrolls; columns keep their order.
# Tables not listed (the page does not show them): empty, so only the first column is frozen.
_FREEZE_THROUGH = {
    "phases": "name", "injections": "name", "fractions": "name", "peaks": "number", "peak_summary": "field",
    "hint_blocks": "block", "hint_injections": "position", "variables": "variable", "method_steps": "block",
    "scouting_runs": "position", "run_details": "field", "column": "parameter", "calibration": "calibration",
    "instrument": "item", "system_settings": "parameter", "audit_log": "time", "warnings": "source",
    "phase_windows": "run",
}
TABLES = OrderedDict((k, t._replace(freeze_through=_FREEZE_THROUGH.get(k, ""))) for k, t in _TABLES)

# curve columns: '<curve>: <what> (<unit>)'; mg/ml curves: '<curve> (mg/ml): <what> ...'
_CURVE_COLUMN = re.compile(r"^(?P<curve>.*?)(?P<mgml> \(mg/ml\))?: (?P<what>time|volume|CV|UTC|value)"
                           r"(?: \((?P<unit>.*)\))?$")
_CURVE_AXES = {"time": "time", "volume": "volume", "CV": "cv", "UTC": "utc"}


def title(key):
    """The name people see for table `key` (page, Excel sheet)."""
    return TABLES[key].title if key in TABLES else key


def file_stem(key):
    """The CSV name of table `key`: its title in lower case, '_' between words."""
    return re.sub(r"[^a-z0-9]+", "_", title(key).lower()).strip("_")


def origin(key):
    return TABLES[key].origin if key in TABLES else ANALYSIS


def note(key):
    return TABLES[key].note if key in TABLES else ""


def generic_column(key, column):
    """The column name as the guide lists it: curve names and units replaced by placeholders."""
    if key == "curves":
        m = _CURVE_COLUMN.match(str(column))
        if m:
            unit = " (<unit>)" if m.group("unit") else ""
            return "<curve>%s: %s%s" % (m.group("mgml") or "", m.group("what"), unit)
    return str(column)


def _column(key, column):
    """(origin, meaning, check) of `column` of table `key`."""
    table = TABLES.get(key)
    if table is None:
        return _a("")
    if key == "curves":
        m = _CURVE_COLUMN.match(str(column))
        if not m:
            return table.default
        if m.group("mgml"):  # the path length and wavelength behind it are PyCORN's choices
            return _a("mg/ml curve PyCORN made from the UV curve (%s)" % (
                "value" if m.group("what") == "value" else _AXES[_CURVE_AXES[m.group("what")]][1]))
        if m.group("what") == "value":
            return _u("curve value, as stored")
        return _AXES[_CURVE_AXES[m.group("what")]]
    return table.columns.get(column, table.default)


def column_origin(key, column):
    """(origin, meaning) of `column` of table `key`."""
    return _column(key, column)[:2]


def column_check(key, column):
    """How to check a calculated column (redo the formula, or where UNICORN shows it); else ''."""
    return _column(key, column)[2]


GUIDE_COLUMNS = ["table", "csv_file", "table_origin", "column", "origin", "meaning", "check"]


def csv_file(key, chrom_part=True):
    """How the CSV file of table `key` is named ('<chromatogram>_...' for per-chromatogram tables)."""
    if key == "curves":
        return "<chromatogram>.csv"
    return ("<chromatogram>_%s.csv" if chrom_part else "%s.csv") % file_stem(key)


FORMAT_NOTES = {
    "xlsx": ("Excel workbook", MIXED, "every table of the chromatogram and of the whole run as a sheet, with "
             "Summary (and how the export was made), Chart and Column guide; Excel adds the unit to time and "
             "volume headers"),
    "pickle": ("Python pickle", MIXED, "the chromatogram in PyCORN's legacy res3 layout; only open pickles you trust"),
    "cadet": ("CADET input", ANALYSIS, "built by PyCORN for CADET; names and layout are CADET's own "
              "(see docs/CADET_GUIDE.md); PyCORN's metadata records the provenance"),
    "provenance": ("Provenance", PYCORN, "app, version, commit, route, command and options of this export, as "
                   "JSON (the same as above)"),
}
REPOSITORY = "https://github.com/traylosj/PyCORN"


def docs_url(name, version, commit):
    """Link to `docs/<name>` as of the code that wrote the export: its commit, else its release tag."""
    ref = commit.split(".")[0] if commit else "v" + version
    return "%s/blob/%s/docs/%s" % (REPOSITORY, ref, name)


def provenance_rows(prov):
    """[(label, text)] of an export's provenance (`run_export.provenance`), for people."""
    import json
    options = prov.get("options") or {}
    return [
        ("App", "%s %s (commit %s)" % (prov.get("app"), prov.get("version"), prov.get("commit") or "unknown")),
        ("Repository", prov.get("repository") or ""),
        ("Reference", docs_url("EXPORT_GUIDE.md", prov.get("version"), prov.get("commit"))),
        ("Route", prov.get("route") or ""),
        ("Command", prov.get("command") or "(none: not run from the command line)"),
        ("Formats", ", ".join(prov.get("formats") or [])),
        ("Options", json.dumps(options, ensure_ascii=False) if options
         else "all at this version's defaults"),
        ("Exported (UTC)", prov.get("exported_utc") or ""),
    ]


def readme_text(run_name, read_from, described, prov):
    """
    README.txt of an exported run folder: what wrote it and how (`provenance_rows`), where the
    data comes from, and every file with its table, origin and content. `described`: [(file,
    table key or 'xlsx' / 'pickle' / 'cadet' / 'provenance', chromatogram or None)].
    """
    rows = [("Run", run_name or ""), ("Read from", read_from or "")] + provenance_rows(prov)
    width = max(len(label) for label, _ in rows) + 1
    lines = ["PyCORN export of one UNICORN run", "=" * 32, ""]
    lines += ["%-*s %s" % (width, label + ":", text) for label, text in rows]
    lines += ["", "To reproduce: install PyCORN at the commit above (or release v%s), then run the command "
              "(or the route) with the same options on the same file or database result." % prov.get("version"),
              "", "Where the data comes from", "-" * 25]
    width = max(len(o) for o in ORIGIN_NOTES)
    lines += ["  %-*s  %s" % (width, o, n) for o, n in ORIGIN_NOTES.items()]
    csv = any(str(file).lower().endswith(".csv") for file, _, _ in described)  # an xlsx-only folder has none
    where_guide = "%s.csv (and the '%s' sheet of each workbook)" % (file_stem("column_guide"), title("column_guide")) \
        if csv else "the '%s' sheet of each workbook" % title("column_guide")
    lines += ["", "Every column, its origin, and how to check a calculated one: %s." % where_guide,
              "", "Files", "-----"]
    for file, key, chrom in described:
        if key in FORMAT_NOTES:
            name, o, text = FORMAT_NOTES[key]
        else:
            name, o, text = title(key), origin(key), note(key)
        where = "chromatogram %s" % chrom if chrom else "whole run"
        lines += ["%s" % file, "    %s [%s], %s: %s" % (name, o, where, text)]
    lines += ["", "Conventions", "-----------"]
    if csv:
        lines += ["- CSV files are UTF-8 with a byte order mark (Excel shows units such as degC right)."]
    lines += ["- UTC columns are UTC without a time zone. Cells holding lists or dicts are JSON text.",
              "- Tables that would be empty are left out (curves and events always written).",
              "- PyCORN is validated only on the files and databases it was tested with; review the",
              "  numbers before relying on them, starting with %s." % (
                  file_stem("warnings") + ".csv" if csv else "the '%s' sheet" % title("warnings")), ""]
    return "\n".join(lines)


def column_guide(chrom_frames, run_frames):
    """
    The 'Column guide' frame: one row per column of the tables in `chrom_frames` and
    `run_frames` ([(key, frame)]; per-chromatogram frames of all chromatograms may repeat a
    key), in table order. Curve columns are listed once per kind ('<curve>: time (<unit>)').
    """
    import pandas as pd
    rows, seen = [], set()
    for frames, per_chrom in ((chrom_frames, True), (run_frames, False)):
        for key, frame in frames:
            if key == "column_guide" or frame is None:
                continue
            for column in frame.columns:
                name = generic_column(key, column)
                if (key, name) in seen:
                    continue
                seen.add((key, name))
                o, meaning, check = _column(key, column)
                rows.append((title(key), csv_file(key, per_chrom), origin(key), name, o, meaning, check))
    order = {title(k): i for i, k in enumerate(TABLES)}
    rows.sort(key=lambda r: order.get(r[0], len(order)))  # stable: columns keep their order
    return pd.DataFrame(rows, columns=GUIDE_COLUMNS)
