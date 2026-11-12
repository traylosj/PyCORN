# -*- coding: utf-8 -*-
"""
Phase, injection and fraction tables with start and end on every axis (time, volume, CV, UTC).

Rows (owner, 2026-11-08):

- Phases: a run-log 'Phase ...' BlockStart ends where the next one starts (as
  pycorn/sql/unicorn_cte.sql does). The last phase ends at its 'End Phase' entry if logged,
  else at the run log's last entry.
- Injections and fractions: each event ends where the next event of the same event curve
  starts; the last ends at the run log's last entry. Waste marks are rows like any other.
- Each table starts with a pseudo-row (`pseudo=True`) from the run log's first entry (method
  start) to the first real row, when that row starts after 0. Labels: the first run-log text
  for phases (as the CTE's first row), 'Before injection', and 'Waste' for fractions (the
  fractionator is at waste until the first fraction).

Each row has its length on the time, volume and CV axes (`length_*`, end - start). The tables
(`chrom.phase_table()` etc., the export, the page) also give what ran in each row
(`SPAN_COLUMNS`: flow, %B, inlets, set marks, ...) from the run-log sections the CADET export
reads (`with_details`; `pycorn.run_log_sections.span_details`).

An imported chromatogram (copied in from another result in Evaluation) has the source run's log
as its run log ('ImportedLogbook', `Chromatogram.run_log()`) and only non-original event
curves: its tables are the source run's phases, injections and fractions
(`Chromatogram.run_log_source()` says which result), on its own curves' axes.

`zero_offsets(row)` gives the values to subtract to put zero at a row's start, as UNICORN
does when it adjusts the x axis to an injection or a phase.
"""
import re
from collections import OrderedDict

STATUS_SUFFIX = re.compile(r"(\s*\((?:Issued|Processing|Completed|Manual|System)\))+\s*$")
PHASE_START = re.compile(r"^Phase\b")
PHASE_END = re.compile(r"^End Phase\b")
PSEUDO_NAMES = {"injection": "Before injection", "fraction": "Waste"}  # phases use the first run-log text

# what ran in a row, from the run-log sections (`pycorn.run_log_sections.span_details`)
SPAN_COLUMNS = ["blocks", "system_flow_ml_min", "column_flow_ml_min", "percent_b", "percent_q", "inlets",
                "inlets_source", "column_position", "injection_valve", "sample_use", "set_marks"]
COLUMNS = ["kind", "number", "name", "pseudo", "start_time", "end_time", "start_volume", "end_volume",
           "start_cv", "end_cv", "start_utc", "end_utc", "length_time", "length_volume", "length_cv"] + SPAN_COLUMNS + [
           "end_source", "event_curve", "time_unit", "volume_unit", "text"]


def strip_status(text):
    """'Phase Elution (Issued) (Processing) (Completed)' -> 'Phase Elution'."""
    return STATUS_SUFFIX.sub("", text or "") or None


def _row(kind, number, name, pseudo, start, end, end_source, ec, text):
    row = OrderedDict([("kind", kind), ("number", number), ("name", name), ("pseudo", pseudo)])
    for axis in ("time", "volume", "cv", "utc"):
        row["start_" + axis] = getattr(start, axis, None) if start is not None else None
        row["end_" + axis] = getattr(end, axis, None) if end is not None else None
    for axis in ("time", "volume", "cv"):  # stored time stops during a pause; UTC end - start includes it
        a, b = row["start_" + axis], row["end_" + axis]
        row["length_" + axis] = b - a if a is not None and b is not None else None
    row.update([("end_source", end_source), ("event_curve", ec.name if ec is not None else None),
                ("time_unit", ec.time_unit if ec is not None else None),
                ("volume_unit", ec.volume_unit if ec is not None else None), ("text", text)])
    return row


def _log(chrom):
    log = chrom.run_log()  # an imported chromatogram's: the source run's log ('ImportedLogbook')
    return log if log is not None and log.events else None


def _starts_after_zero(event):
    return (event.time or 0) > 0 or (event.volume or 0) > 0


def phase_rows(chrom):
    log = _log(chrom)
    if log is None:
        return []
    events = log.events
    starts = [i for i, e in enumerate(events) if e.subtype == "BlockStart" and PHASE_START.match(e.text or "")]
    rows = []
    if not starts:
        return rows
    first = events[starts[0]]
    if starts[0] > 0 and _starts_after_zero(first):
        rows.append(_row("phase", 0, strip_status(events[0].text), True, events[0], first, "next", log,
                         events[0].text))
    for k, i in enumerate(starts):
        if k + 1 < len(starts):
            end, source = events[starts[k + 1]], "next"
        else:
            closing = [j for j in range(i + 1, len(events))
                       if events[j].subtype == "BlockEnd" and PHASE_END.match(events[j].text or "")]
            end, source = (events[closing[-1]], "end_phase") if closing else (events[-1], "last_log_event")
        rows.append(_row("phase", k + 1, strip_status(events[i].text), False, events[i], end, source, log,
                         events[i].text))
    return rows


def _point_rows(chrom, kind, event_type):
    log = _log(chrom)
    run_end = log.events[-1] if log is not None else None
    rows = []
    for ec in chrom.own_event_curves(event_type):
        events = sorted(ec.events, key=lambda e: (e.time if e.time is not None else float("inf")))
        if log is not None and _starts_after_zero(events[0]):
            rows.append(_row(kind, 0, PSEUDO_NAMES[kind], True, log.events[0], events[0], "next", ec, None))
        for k, e in enumerate(events):
            end, source = (events[k + 1], "next") if k + 1 < len(events) else (run_end, "run_end")
            # an Injection event has no text: name it by its number ('Injection 1'); `text` stays as stored
            name = e.text if e.text else "%s %d" % (event_type, k + 1)
            rows.append(_row(kind, k + 1, name, False, e, end, source, ec, e.text))
    return rows


def injection_rows(chrom):
    return _point_rows(chrom, "injection", "Injection")


def fraction_rows(chrom):
    return _point_rows(chrom, "fraction", "Fraction")


TABLES = {"phase": phase_rows, "injection": injection_rows, "fraction": fraction_rows}


def rows(chrom, kind):
    if kind not in TABLES:
        raise KeyError("unknown table %r; known: %s" % (kind, ", ".join(TABLES)))
    return TABLES[kind](chrom)


def with_details(chrom, table_rows, sections=None):
    """
    `table_rows` with what ran in each row (`SPAN_COLUMNS`), from `sections`, a
    `run_log_sections()` result of `chrom` (None: built here; False: the columns stay empty,
    e.g. when building the sections failed).
    """
    from .run_log_sections import run_log_sections, span_details
    from .units import conversion_factor
    if sections is None:
        sections = run_log_sections(None, chrom)
    for k, row in enumerate(table_rows):
        row.update((column, None) for column in SPAN_COLUMNS)
        if not sections or row["start_time"] is None or row["end_time"] is None or not row["time_unit"]:
            continue
        to_s = conversion_factor(row["time_unit"], "s")
        row.update(span_details(sections, row["start_time"] * to_s, row["end_time"] * to_s,
                                closed=k == len(table_rows) - 1))
    return table_rows


def to_frame(table_rows):
    import pandas as pd
    return pd.DataFrame(table_rows, columns=COLUMNS)


def zero_offsets(row):
    """Values to subtract from each axis to put zero at the start of `row`."""
    return OrderedDict([("time", row["start_time"]), ("volume", row["start_volume"]), ("cv", row["start_cv"]),
                        ("utc", row["start_utc"])])
