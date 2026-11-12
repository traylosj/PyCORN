# -*- coding: utf-8 -*-
"""
UTC (wall-clock) time for curves and events.

Stored time is elapsed method time, and it stops during a Pause (not during a Hold; Phase 0).
So UTC is rebuilt from the chromatogram start plus the pauses found in the run log:

    utc(t) = start_utc + t + sum(duration of pauses at elapsed < t)

Every timestamped run-log text (`LOG_KINDS`) is an anchor: it gives the wall clock at its
elapsed time. A Pause and the Continue/End that resumes it give an exact duration. A
Continue whose wall clock is ahead of the prediction (a pause UNICORN didn't log as 'Pause')
adds a pause of that size at its elapsed time (elapsed stops during a pause, so that is where
it happened). Any other gap over `TOLERANCE_S` is reported and not placed. A pause whose
duration can't be found (no timestamped anchor after it, or a second untimestamped pause
before one) stops the timeline there: UTC after it is left empty (NaT / None).

Each anchor is converted to UTC with its own offset. Pause texts without an offset use the
offset of the Continue/End that resumes them; the run start uses the grid start's offset.
The grid start is `ChromatogramStartTime` (a later chromatogram of a method run doesn't start
at the method start), else `MethodStartTime`.
Log dates are month-first in all data seen; day-first is used only if a date proves it (a first
field over 12), or if it matches the exact grid start better.

Curves get `utc` as numpy datetime64[us] in UTC, and events a numpy datetime64[us] scalar
(naive UTC as well; None when unknown), so the two compare and subtract directly.
A data point at a pause's elapsed time gets the pre-pause instant; run-log events from the
resuming entry on get the post-pause instant.

An imported chromatogram (W17) keeps the source run's start times and its run log
('ImportedLogbook', `Chromatogram.run_log()`), on the same elapsed axis as its curves (tested
against a lab environment: the copies match the source results' log, curves and start time
exactly), so its UTC is the source run's wall clock.
"""
import datetime as dt
import re
from collections import OrderedDict

import numpy as np

from .units import UnitError, conversion_factor

TOLERANCE_S = 2.0  # log texts have 1 s resolution
# a point this close to a pause's elapsed time is at the pause (the XML keeps 7 significant digits)
AT_PAUSE_S = 0.01
# Run-log entries after which UNICORN stops elapsed time without logging a 'Pause' (seen in
# UNICORN runs): washes, pump synchronization, and alarms and errors until they
# are dealt with. Callers can add more. A wall-clock gap found at the next anchor is placed at the
# latest such entry.
WASH = re.compile(r"^(?:System|Fraction collector|Accumulator|Pump) wash\b")
PUMP_SYNC = re.compile(r"^System pump synchronization\b.*\bstarted\b")
CLOCK_STOPS = [
    ("alarm", lambda e: e.subtype == "Alarm"),
    ("error", lambda e: e.subtype == "Error"),
    ("wash", lambda e: WASH.match(e.text or "") is not None and "(Issued)" in (e.text or "")),
    ("pump_synchronization", lambda e: PUMP_SYNC.match(e.text or "") is not None),
]
# an anchor this close (elapsed) to a clock stop is where that stop happened: the clock never restarted
STOPPED_THROUGH_S = 1.0
UTC = dt.timezone.utc

DATE = re.compile(r"(?P<a>\d{1,2})/(?P<b>\d{1,2})/(?P<y>\d{4})\s+(?P<H>\d{1,2}):(?P<M>\d{2}):(?P<S>\d{2})"
                  r"(?:\s*(?P<ampm>[AaPp][Mm]))?(?:\s+(?P<off>[+-]\d{1,2}:\d{2}))?")
ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?")
# run-log texts that carry the wall clock; a Hold doesn't stop elapsed time, so it's only a check.
# ('Hold until ...' and 'End_Block' / 'End Phase' don't match.)
LOG_KINDS = [
    ("start", re.compile(r"^Method Run\b")),
    ("pause", re.compile(r"^Pause\b")),
    ("hold", re.compile(r"^Hold\s+\d")),
    ("continue", re.compile(r"^Continue\b")),
    ("end", re.compile(r"^End\s+\d")),
]


def _kind(text):
    return next((kind for kind, pattern in LOG_KINDS if pattern.match(text or "")), None)


def _offset_minutes(text):
    sign = -1 if text.startswith("-") else 1
    hours, minutes = text.lstrip("+-").split(":")
    return sign * (int(hours) * 60 + int(minutes))


def parse_log_datetime(match, order="mdy"):
    """(naive local datetime, UTC offset in minutes or None) for a `DATE` match."""
    a, b = int(match.group("a")), int(match.group("b"))
    month, day = (a, b) if order == "mdy" else (b, a)
    hour = int(match.group("H"))
    ampm = (match.group("ampm") or "").upper()
    if ampm == "PM" and hour != 12:
        hour += 12
    elif ampm == "AM" and hour == 12:
        hour = 0
    local = dt.datetime(int(match.group("y")), month, day, hour, int(match.group("M")), int(match.group("S")))
    off = match.group("off")
    return local, (_offset_minutes(off) if off else None)


def parse_iso(text):
    """Naive datetime from 'YYYY-MM-DDTHH:MM:SS[.ffffff]' (as stored in the XML and the database)."""
    m = ISO.match(text or "")
    if not m:
        return None
    frac = (m.group(7) or "0")[:6].ljust(6, "0")
    return dt.datetime(*(int(g) for g in m.groups()[:6]), int(frac))


def _int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


# Where the curve grid of a chromatogram counts from (owner, 2026-11-08): the chromatogram's own
# start, since one method run can hold several chromatograms and a later one doesn't start at the
# method start; the method start only when the chromatogram has none.
GRID_STARTS = ("ChromatogramStartTime", "MethodStartTime")


def _grid_start(chrom):
    """(local, offset minutes, utc, field) of the chromatogram's grid origin (`GRID_STARTS`), else None."""
    items = list(chrom.curves.values()) + list(chrom.event_curves.values())
    for field in GRID_STARTS:
        for item in items:
            local = parse_iso(item.xml.get(field))
            offset = _int(item.xml.get(field + "UtcOffsetMinutes"))
            if local is not None and offset is not None:
                return local, offset, (local - dt.timedelta(minutes=offset)).replace(tzinfo=UTC), field


def _date_order(matches, start_local):
    firsts = any(int(m.group("a")) > 12 for m in matches)
    seconds = any(int(m.group("b")) > 12 for m in matches)
    if firsts != seconds:
        return "dmy" if firsts else "mdy"
    if start_local is not None and matches:
        best = None
        for order in ("mdy", "dmy"):
            try:
                diff = abs((parse_log_datetime(matches[0], order)[0] - start_local).total_seconds())
            except ValueError:
                continue
            if best is None or diff < best[0]:
                best = (diff, order)
        if best:
            return best[1]
    return "mdy"


def _anchors(log, to_s, start_local, start_offset, warnings):
    found = []
    for index, e in enumerate(log.events):
        kind = _kind(e.text)
        if kind is None or e.time is None:
            continue
        found.append((index, e, kind, DATE.search(e.text)))
    order = _date_order([m for _, _, _, m in found if m], start_local)
    anchors = []
    for index, e, kind, m in found:
        elapsed = e.time * to_s
        # An instruction's (Processing)/(Completed) entries follow its (Issued) one, maybe after other
        # entries (an alarm's ignored Pause between a Continue's Issued and Completed). A repeat of the
        # previous anchor's kind at the same elapsed time adds nothing (a Pause while paused). Any other
        # (Issued) entry is a new instruction (the Continue that clears the alarm).
        same = [a for a in anchors if a["kind"] == kind and abs(a["elapsed_s"] - elapsed) < 1e-6]
        if same and ("(Issued)" not in (e.text or "") or same[-1] is anchors[-1]):
            continue
        local = offset = None
        if m:
            try:
                local, offset = parse_log_datetime(m, order)
            except ValueError as err:
                warnings.append("run-log date not read in %r (%s)" % (e.text, err))
        anchors.append(OrderedDict([("kind", kind), ("index", index), ("elapsed_s", elapsed), ("local", local),
                                    ("offset_minutes", offset), ("utc", None), ("offset_source", "text"),
                                    ("text", e.text)]))
    for k, a in enumerate(anchors):
        if a["local"] is None or a["offset_minutes"] is not None:
            continue
        if a["kind"] == "pause":
            resume = next((b for b in anchors[k + 1:] if b["kind"] in ("continue", "end")), None)
            if resume is not None and resume["offset_minutes"] is not None:
                a["offset_minutes"], a["offset_source"] = resume["offset_minutes"], "resume"
        if a["offset_minutes"] is None and start_offset is not None:
            a["offset_minutes"], a["offset_source"] = start_offset, "method_start"
            if a["kind"] != "start":
                warnings.append("no UTC offset in %r; used the run start's" % a["text"])
    for a in anchors:
        if a["local"] is not None and a["offset_minutes"] is not None:
            a["utc"] = (a["local"] - dt.timedelta(minutes=a["offset_minutes"])).replace(tzinfo=UTC)
    return anchors


def _run_start_date(run, start_order="mdy"):
    """UTC from ResultRunInformation 'RunStartDate' ('03/10/2023 16:30:07 -05:00'), else None."""
    for ri in run.result.get("run_information") or []:
        if ri.get("type") == "RunStartDate" and isinstance(ri.get("value"), str):
            m = DATE.search(ri["value"])
            if m and m.group("off"):
                local, offset = parse_log_datetime(m, start_order)
                return local, offset, (local - dt.timedelta(minutes=offset)).replace(tzinfo=UTC)
    return None


def build_timeline(run, chrom):
    """
    The UTC timeline of `chrom`: start, end, anchors, pauses, residual and quality
    ('exact', 'inferred', 'ambiguous', 'unresolved', or None without a start).
    Elapsed values are in seconds; `time_unit` is the run log's stored time unit.
    """
    warnings = []
    tl = OrderedDict([("start_utc", None), ("start_source", None), ("local_utc_offset", None),
                      ("end_utc", None), ("end_elapsed_s", None), ("total_paused_s", 0.0), ("residual_s", None),
                      ("unplaced_s", 0.0), ("gaps", []), ("quality", None), ("valid_until_s", None), ("time_unit", None),
                      ("anchors", []), ("pauses", []), ("warnings", warnings)])
    log = chrom.run_log()  # an imported chromatogram's: the source run's log, on its curves' axes
    start = _grid_start(chrom)
    if start is not None:
        tl["start_source"] = start[3]
    anchors = []
    if log is not None:
        tl["time_unit"] = log.time_unit
        try:
            to_s = conversion_factor(log.time_unit, "s")
        except UnitError as e:
            warnings.append("run log not used (%s)" % e)
        else:
            anchors = _anchors(log, to_s, start[0] if start else None, start[1] if start else None, warnings)
    tl["anchors"] = anchors
    first = next((a for a in anchors if a["kind"] == "start"), None)
    if start is None and first is not None and first["utc"] is not None:
        start = (first["local"], first["offset_minutes"], first["utc"])
        tl["start_source"] = "run_log"
    if start is None:
        start = _run_start_date(run)
        if start is not None:
            tl["start_source"] = "RunStartDate"
            if not run.holds_run_of(chrom):  # W17: this result's run start, used with a warning
                from .model import this_result_note
                warnings.append(this_result_note(run, "the UTC start (RunStartDate)"))
    if start is None:
        warnings.append("no chromatogram or method start time found; no UTC")
        return tl
    start_utc = start[2]
    tl["start_utc"], tl["local_utc_offset"] = start_utc, start[1]
    if tl["start_source"] in GRID_STARTS and start_utc.microsecond == 0:
        # seen on imported results (in a database, and in UNICORN 7.11 exports of them); a run's own
        # result keeps milliseconds
        warnings.append("%s is stored to the whole second (an imported result?): UTC may be up to 1 s early "
                        "(the first inferred pause that much longer)" % tl["start_source"])
    if first is not None and first["utc"] is not None and tl["start_source"] in GRID_STARTS:
        diff = (first["utc"] - start_utc).total_seconds()
        if abs(diff) > TOLERANCE_S + 1:
            warnings.append("run-log start %r is %.0f s from %s" % (first["text"], diff, tl["start_source"]))

    pauses, pending, gaps = [], [], []
    paused = unplaced = 0.0
    stopped_at = None

    def predicted(elapsed):
        return start_utc + dt.timedelta(seconds=elapsed + paused)

    def add_pause(elapsed, duration, source, quality, pause_index, resume_index, cause=None):
        nonlocal paused
        begin = predicted(elapsed)
        pauses.append(OrderedDict([("elapsed_s", elapsed), ("start_utc", begin),
                                   ("end_utc", begin + dt.timedelta(seconds=duration)), ("duration_s", duration),
                                   ("source", source), ("quality", quality), ("pause_index", pause_index),
                                   ("resume_index", resume_index), ("cause", cause)]))
        paused += duration

    stops = []  # (log index, elapsed s, source) of entries in CLOCK_STOPS
    if log is not None and anchors:
        for index, e in enumerate(log.events):
            source = next((name for name, test in CLOCK_STOPS if test(e)), None)
            if source and e.time is not None:
                stops.append((index, e.time * to_s, source))

    def last_stop(after_index, upto_index):
        found = [st for st in stops if after_index < st[0] <= upto_index]
        return found[-1] if found else None

    previous_index = -1

    for a in anchors:
        if a["kind"] == "start":
            continue
        if a["kind"] == "pause":
            if pending:
                stopped_at = pending[0]["elapsed_s"]
                warnings.append("several pauses without a timestamped resume (first at %.4g s elapsed); "
                                "no UTC after it" % stopped_at)
                break
            a["previous_index"] = previous_index
            pending.append(a)
            previous_index = a["index"]
            continue
        if a["utc"] is None:
            continue
        stop = last_stop(previous_index, a["index"])
        previous_index = a["index"]
        gap = (a["utc"] - predicted(a["elapsed_s"])).total_seconds() - unplaced
        if pending:
            p = pending.pop()
            # an operator Hold can end a pause too (elapsed runs again during a Hold)
            resumes = a["kind"] in ("continue", "end", "hold") and abs(a["elapsed_s"] - p["elapsed_s"]) < 1e-6
            if resumes and p["utc"] is not None:
                # The resume re-anchors the clock: duration = resume - predicted pause start, so the
                # whole-second truncation of each text doesn't add up over many pauses. Time the
                # Pause text shows beyond the prediction (e.g. a System wash just before it) is
                # part of the stop and lands at the pause; then the pause counts as inferred.
                before = (p["utc"] - predicted(p["elapsed_s"])).total_seconds() - unplaced
                quality = "exact" if abs(before) <= TOLERANCE_S else "inferred"
                cause = None
                if before > TOLERANCE_S:
                    st = last_stop(p["previous_index"], p["index"])
                    cause = st[2] if st else "unknown"
                add_pause(p["elapsed_s"], gap, "pause_text", quality, p["index"], a["index"], cause)
                pauses[-1]["before_pause_s"] = before
            else:
                add_pause(p["elapsed_s"], gap, "pause_text", "inferred", p["index"],
                          a["index"] if resumes else None)
        elif a["kind"] == "continue" and gap > TOLERANCE_S:
            # elapsed stood still until the Continue, so its elapsed time is where the clock stopped
            add_pause(a["elapsed_s"], gap, "continue_without_pause", "inferred", stop[0] if stop else None,
                      a["index"], stop[2] if stop else None)
        elif gap > TOLERANCE_S and stop is not None:
            # an alarm, error or System wash stopped the clock and the run went on (or ended)
            # without a Continue: the gap belongs at the stop
            at = a["elapsed_s"] if a["elapsed_s"] - stop[1] <= STOPPED_THROUGH_S else stop[1]
            add_pause(at, gap, "clock_stop", "inferred", stop[0], a["index"], stop[2])
        elif abs(gap) > TOLERANCE_S:
            warnings.append("wall clock %.0f s off at %r with no pause to explain it; gap not placed"
                            % (gap, a["text"]))
            gaps.append(OrderedDict([("elapsed_s", a["elapsed_s"]), ("gap_s", gap), ("kind", a["kind"]),
                                     ("index", a["index"])]))
            unplaced += gap
        if a["kind"] == "end" and tl["end_utc"] is None:
            tl["end_utc"], tl["end_elapsed_s"] = a["utc"], a["elapsed_s"]
            tl["residual_s"] = (a["utc"] - predicted(a["elapsed_s"])).total_seconds()

    quality = "inferred" if any(p["quality"] == "inferred" for p in pauses) else "exact"
    if stopped_at is not None:
        quality, tl["valid_until_s"] = "ambiguous", stopped_at
    elif pending:
        quality, tl["valid_until_s"] = "unresolved", pending[0]["elapsed_s"]
        warnings.append("pause at %.4g s elapsed has no timestamped resume; no UTC after it" % pending[0]["elapsed_s"])
    if tl["end_utc"] is None and log is not None:
        warnings.append("no timestamped End in the run log; UTC not checked at the end")
    tl["pauses"], tl["total_paused_s"], tl["unplaced_s"], tl["quality"] = pauses, paused, unplaced, quality
    tl["gaps"] = gaps  # wall-clock gaps no pause explains (not placed)
    return tl


def _shift(tl, elapsed_s, log_index=None):
    """Paused seconds before `elapsed_s` (array or number)."""
    t = np.asarray(elapsed_s, dtype=np.float64)
    shift = np.zeros_like(t)
    for p in tl["pauses"]:
        shift = shift + np.where(t > p["elapsed_s"] + AT_PAUSE_S, p["duration_s"], 0.0)
        if log_index is not None and p["resume_index"] is not None and log_index >= p["resume_index"]:
            shift = shift + np.where(np.abs(t - p["elapsed_s"]) <= AT_PAUSE_S, p["duration_s"], 0.0)
    return shift


def apply_utc(chrom, tl):
    """Set `utc` on the curves and events of `chrom` from the timeline `tl`."""
    warnings = []
    start = tl["start_utc"]
    start64 = np.datetime64(start.replace(tzinfo=None), "us") if start is not None else None
    until = tl["valid_until_s"]
    log = chrom.run_log()
    for curve in chrom.curves.values():
        curve.utc = None
        if start64 is None or curve.time is None:
            continue
        try:
            t = curve.time.astype(np.float64) * conversion_factor(curve.time_unit, "s")
        except UnitError as e:
            warnings.append("%s: no UTC (%s)" % (curve.name, e))
            continue
        utc = start64 + np.round((t + _shift(tl, t)) * 1e6).astype("timedelta64[us]")
        if until is not None:
            utc[t > until + AT_PAUSE_S] = np.datetime64("NaT", "us")
        curve.utc = utc
    for ec in chrom.event_curves.values():
        to_s = None
        if start is not None:
            try:
                to_s = conversion_factor(ec.time_unit, "s")
            except UnitError as e:
                warnings.append("%s: no UTC (%s)" % (ec.name, e))
        for index, e in enumerate(ec.events):
            e.utc = None
            if to_s is None or e.time is None:
                continue
            t = e.time * to_s
            if until is not None and t > until + AT_PAUSE_S:
                continue
            shift = float(_shift(tl, t, index if ec is log else None))
            # numpy datetime64[us], naive UTC, as the curves (owner, 2026-11-08)
            e.utc = start64 + np.timedelta64(int(round((t + shift) * 1e6)), "us")
    return warnings
