# -*- coding: utf-8 -*-
"""
Round 2 of the UNICORN report comparison (docs/ROADMAP.md, "UNICORN report comparison,
remaining sections"): each fix checked on a shipped sample, or on a sample with the stored
field put in where no shipped sample has it.
"""
import dataclasses
import json
import os

import pandas as pd
import pytest

from pycorn import browser, open_run
from pycorn import pools
from pycorn.model import EventCurve
from pycorn.run_details import (FREE_TEXT_MASK, Masker, method_steps_frame, method_variables_frame,
                                run_details_frame, scouting_runs_frame, start_questions_frame)
from pycorn.run_export import ExportOptions, event_table_frame, peak_summary_frame, write_run

SAMPLES = os.path.abspath("../samples")


def load(name):
    return open_run(os.path.join(SAMPLES, name))


@pytest.fixture(scope="module")
def column_test():
    return load("u711_column_test.zip")


def documentation(run):
    return run.metadata["MethodDocumentationData"]["MethodDocumentation"]


# --- R2, R3, R4: the method text ----------------------------------------------------------

def test_breakpoint_variable_on_the_instruction_that_stores_it(column_test):
    lines = [line.strip() for line in method_steps_frame(column_test)["line"]]
    # R2: UNICORN prints '(15.00)#PurgeVol System flow: ...', not only on End_Block
    assert "(15.00)#PurgeVol System flow: (0.350)#EquiFlow {ml/min}, Off" in lines
    assert "(22.00)#SampleVol Injection valve: Manual load" in lines
    assert "22.00 End_Block" in lines and "(22.00)#SampleVol End_Block" not in lines
    assert "(30.00)#Wash Volume Set mark: Wash_end" in lines and "30.00 Stop fractionation" in lines
    assert "(20.00)#Equilibration volume End_Block" in lines  # stored on End_Block there


def test_variable_order_puts_the_breakpoint_variable_first(column_test):
    names = list(method_variables_frame(column_test)["variable"])
    assert names.index("PurgeVol") == names.index("EquiFlow") - 1  # as UNICORN's variable list


def test_watch_action_block_follows_its_watch(column_test):
    steps = method_steps_frame(column_test)
    watch = steps.index[steps["line"].str.strip().str.startswith("0.00 Watch: System pressure")][0]
    after = steps.loc[watch + 1:watch + 3]
    assert [line.strip() for line in after["line"]] == ["0.00 Base: SameAsMain", "0.00 Injection valve: Manual load",
                                                        "0.00 End_Block"]
    assert set(after["depth"]) == {steps.loc[watch, "depth"] + 1}
    assert set(after["called_from"]) == {steps.loc[watch, "step"]}
    assert set(after["block"]) == {"turnload"} and after["path"].iloc[0].endswith("SampleLoad > turnload")
    assert set(after["phase"]) == {steps.loc[watch, "phase"]}
    assert steps["block"].iloc[-1] != "turnload"  # no longer printed at the end
    # a Block call: its rows name the call's step; the main block's rows none
    call = steps.index[steps["line"].str.strip() == "0.00 Block: Purge"][0]
    assert steps.loc[call + 1, "called_from"] == steps.loc[call, "step"]
    assert pd.isna(steps["called_from"].iloc[0])


def test_vc_inside_the_parentheses_when_a_variable_sets_it(column_test):
    assert method_steps_frame(column_test)["line"].iloc[0] == "0.00 Base: CV, (Vc=0.170)#ColVolume {ml}, Any"
    plain = method_steps_frame(load("u711_cadet_reference.zip"))["line"].iloc[0]
    assert plain.startswith("0.00 Base: CV, Vc=5.027 {ml}, (")


# --- G1, G3, G6: run and method rows -------------------------------------------------------

def test_evaluation_procedures_result_name_variable_and_notes():
    run = load("u711_column_test.zip")
    documentation(run)["EvaluationProcedures"] = {"EvaluationProcedure": [
        {"EvaluationProcedureName": "Report_A", "DisplayName": "Report_A", "IsSelected": "false"},
        {"EvaluationProcedureName": "Report_B", "DisplayName": "Report_B", "IsSelected": "true"}]}
    run.result["ResultDocumentation"] = {"EvaluationNote": None, "RunNote": "run note", "StartNote": "start note",
                                         "MethodNote": "method note"}
    rows = [tuple(r) for r in run_details_frame(run).values]
    assert ("evaluation procedure", "Report_A", "not selected") in rows
    assert ("evaluation procedure", "Report_B", "selected") in rows
    assert ("method", "result name variable", "RunName") in rows  # G3
    notes = [r for r in rows if r[1].endswith(" notes")]
    assert notes == [("result", "Method notes", "method note"), ("result", "Start notes", "start note"),
                     ("result", "Run notes", "run note")]  # UNICORN's order; empty ones left out


# --- G2: start-protocol questions ---------------------------------------------------------

def _with_questions():
    run = load("u711_column_test.zip")
    documentation(run)["Questions"] = {"Question": [
        {"Name": "Column lot?", "MethodPos": "0", "IsMandatory": "true", "IsRequiresSignature": "false",
         "IsVisibleInChromatogram": "false", "Answer": {"@AnswerType": "InputField"}},
        {"Name": "Second question", "MethodPos": "1", "Answer": {"@AnswerType": "SomeOtherType"}}]}
    run.result["search_criteria"] = list(run.result["search_criteria"]) + [
        {"Name": "QuestionAnswer", "Keyword1": "Column lot?", "Keyword2": "Lot made-up 17"},
        {"Name": "QuestionAnswer", "Keyword1": "Second question", "Keyword2": ""}]
    return run


def test_start_questions_table():
    q = start_questions_frame(_with_questions())
    assert list(q["number"]) == [1, 2]
    assert list(q["question"]) == ["Column lot?", "Second question"]
    assert list(q["answer_type"]) == ["Textual input", "SomeOtherType"]  # the report's text only where known
    assert q["answer"].iloc[0] == "Lot made-up 17" and pd.isna(q["answer"].iloc[1])
    assert bool(q["mandatory"].iloc[0]) is True
    assert start_questions_frame(load("u711_column_test.zip")).empty
    rows = run_details_frame(_with_questions())
    assert not rows["field"].str.startswith("Question").any()  # no raw question rows any more


def test_masker_masks_answers_and_notes_as_a_whole(tmp_path):
    run = _with_questions()
    run.result["ResultDocumentation"] = {"RunNote": "Ran by Jane Roe on bay 7", "StartNote": None}
    masker = Masker(run)
    assert masker.text("Lot made-up 17") == FREE_TEXT_MASK
    assert masker.text("Ran by Jane Roe on bay 7") == FREE_TEXT_MASK
    assert masker.text("Column lot?") == "Column lot?"  # the method's question stays
    write_run(run, tmp_path / "m", formats=("csv",), options=ExportOptions(mask_identifying=True))
    text = "".join(open(os.path.join(tmp_path / "m", f), encoding="utf-8-sig").read()
                   for f in os.listdir(tmp_path / "m") if f.endswith(".csv"))
    assert "Jane Roe" not in text and "made-up 17" not in text and FREE_TEXT_MASK in text


# --- G4: scouting runs --------------------------------------------------------------------

def test_scouting_run_numbered_from_one_in_method_variable_order(column_test):
    s = scouting_runs_frame(column_test)
    assert list(s.columns[:4]) == ["run", "position", "included", "ColVolume"]
    assert s["run"].iloc[0] == 1 and s["position"].iloc[0] == 0
    names = list(s.columns[3:])
    order = [v for v in method_variables_frame(column_test)["variable"] if v in names]
    assert names == order


# --- G5, O1: peak table summary -----------------------------------------------------------

def test_peak_summary_readable_names_and_normalization():
    run = load("u711_cadet_reference.zip")
    chrom = run.chromatograms["Chrom.1"]
    problems = []
    s = peak_summary_frame(chrom, run, problems).set_index("field")
    assert s.loc["Resolution algorithm", "value"] == "Algorithm3"
    assert s.loc["Resolution algorithm id", "value"] == 3
    assert s.loc["Resolution algorithm name", "value"] == \
        "1.177 * ((Ret2-Ret1) / (WidthHalfHeight2 + WidthHalfHeight1))"
    # the evaluation log's Peak Integration entry that made the table does not say it is normalized
    assert s.loc["UV normalized to 1 cm path length", "value"] == False  # noqa: E712
    assert s.loc["UV normalized to 1 cm path length", "stored_as"].startswith("evaluation log: Peak Integration")
    assert not problems
    # a zero baseline (no baseline curve): UNICORN prints 'Zero Baseline'; an unknown algorithm gets no name
    table = chrom.peak_tables[0]
    table.info.pop("BaseLine")
    table.info.update(BaselineType="ZeroBaseline", ResolutionAlgorithmID=7, ResolutionAlgorithm="Algorithm7")
    s = peak_summary_frame(chrom, run).set_index("field")
    assert tuple(s.loc["Baseline name", ["value", "stored_as"]]) == ("Zero Baseline", "BaselineType")
    assert s.loc["Baseline type", "value"] == "ZeroBaseline"
    assert s.loc["Resolution algorithm id", "value"] == 7 and "Resolution algorithm name" not in s.index


def test_normalized_from_the_evaluation_log_or_a_warning():
    run = load("u711_cadet_reference.zip")
    chrom = run.chromatograms["Chrom.1"]
    for trail in run.evaluation_log:
        for e in trail["AuditTrailEntries"]:
            if str(e.get("LogEntry")).startswith("Peak Integration"):
                e["LogEntry"] = e["LogEntry"].replace("Source curve: UV 1_280",
                                                      "Source curve: UV 1_280\n    UV signal is normalized to 1 cm "
                                                      "path length.")
    s = peak_summary_frame(chrom, run).set_index("field")
    assert s.loc["UV normalized to 1 cm path length", "value"] == True  # noqa: E712
    # no matching entry: no row, a warning
    sample = load("sample.zip")
    problems = []
    s = peak_summary_frame(sample.chromatogram(), sample, problems)
    assert "UV normalized to 1 cm path length" not in set(s["field"])
    assert len(problems) == 1 and "1 cm path length" in problems[0][2]


# --- O2: pooled fractions -----------------------------------------------------------------

def _pooled(name="Fraction@11,POOL", second_text=True):
    """u711_cadet_reference with 3.A.1 and 3.A.2 pooled, as UNICORN's Pool Fractions stores it."""
    run = load("u711_cadet_reference.zip")
    chrom = run.chromatograms["Chrom.1"]
    frac = chrom.event_curves_of_type("Fraction")[0]
    first, waste = frac.events[0], frac.events[2]
    xml = dict(first.xml, EventText2="3.A.2") if second_text else dict(first.xml)
    events = [dataclasses.replace(first, xml=xml), dataclasses.replace(waste)]
    chrom.event_curves[name] = EventCurve(name, "Fraction", 18, frac.time_unit, frac.volume_unit, False,
                                          frac.column_volume, frac.column_volume_unit, events, {})
    return run, chrom


@pytest.mark.parametrize("name, second_text", [("Fraction@11,POOL", True),  # database: EventText2 stored
                                               ("Fraction@11,POOL", False),  # .zip: no EventText2
                                               ("Pooled by me", True)])  # renamed, EventText2 stored
def test_pool_table_and_fraction_pool_column(name, second_text):
    run, chrom = _pooled(name, second_text)
    problems = []
    p = event_table_frame(chrom, "pool", False, problems)
    assert len(p) == 1 and not problems
    row = p.iloc[0]
    assert row["pool"] == "3.A.1-3.A.2" and json.loads(row["fractions"]) == ["3.A.1", "3.A.2"]
    assert (row["first_fraction"], row["last_fraction"], row["n_fractions"]) == ("3.A.1", "3.A.2", 2)
    frac = chrom.event_curves_of_type("Fraction")[0]
    assert row["start_volume"] == pytest.approx(frac.events[0].volume)
    assert row["end_volume"] == pytest.approx(frac.events[2].volume)  # the pooled curve's next mark
    assert row["length_volume"] == pytest.approx(row["end_volume"] - row["start_volume"])
    assert row["event_curve"] == name and "start_utc" in p.columns and "start_cv" in p.columns
    f = event_table_frame(chrom, "fraction", False)
    assert [None if pd.isna(x) else x for x in f["pool"]] == [None, "3.A.1-3.A.2", "3.A.1-3.A.2", None]


def test_no_pools_without_a_pooled_curve():
    run = load("u711_cadet_reference.zip")
    chrom = run.chromatograms["Chrom.1"]
    assert event_table_frame(chrom, "pool", False).empty
    assert event_table_frame(chrom, "fraction", False)["pool"].isna().all()
    # a copy of the fraction curve made in evaluation, without the pool ending: not pools
    frac = chrom.event_curves_of_type("Fraction")[0]
    chrom.event_curves["Copy"] = dataclasses.replace(frac, name="Copy", is_original=False)
    assert pools.pooled_curves(chrom) == []


def test_pools_in_every_export_path(tmp_path):
    run, _ = _pooled()
    written = write_run(run, tmp_path / "r", formats=("csv", "xlsx"))
    assert "Chrom.1_pooled_fractions.csv" in written
    book = next(w for w in written if w.endswith(".xlsx"))
    assert "Pooled fractions" in pd.ExcelFile(tmp_path / "r" / book).sheet_names
    guide = pd.read_csv(tmp_path / "r" / "column_guide.csv")
    assert {"pool", "fractions"} <= set(guide[guide["table"] == "Pooled fractions"]["column"])
    assert "pool" in set(guide[guide["table"] == "Fractions"]["column"])


def test_browser_view_has_the_new_tables():
    s = browser.Session()
    s.add([os.path.join(SAMPLES, "u711_column_test.zip")])
    v = json.loads(s.view(0))
    assert "start_questions" in v["details"]
    chrom = v["chromatograms"][0]
    assert "pools" in chrom and "pool" in chrom["fractions"]["columns"]
    steps = v["details"]["method_steps"]
    assert "called_from" in steps["columns"]
