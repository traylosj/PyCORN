"""
Database reader (`pycorn.sources.sql_source`).

Offline tests run the foreign-key walker on an in-memory database, and build a `Run`
from rows made out of samples/sample.zip. The parity test against the real database
only runs when sql/.env exists, the server answers and the matched exports are in
unicorn_test_files/; it reports only pass/fail (no names, no connection values).
"""
import datetime
import glob
import os
import re
import warnings
from collections import OrderedDict

import numpy as np
import pytest

from pycorn import open_folder, open_run
from pycorn.sources.sql_source import (ForeignKey, SchemaGraph, _element, _text, _with_implied_keys,
                                       build_run, collect_result_rows)

SAMPLE = os.path.join("..", "samples", "sample.zip")
TEST_FILES = os.path.join("..", "unicorn_test_files")
# UNICORN's XML prints 7 significant digits; the database keeps the exact float32
REL = 6e-7


# ---------------------------------------------------------------------------
# In-memory database
# ---------------------------------------------------------------------------

_T = "nvarchar"
COLUMNS = {
    "dbo.Folder": {"FolderID": "int", "ParentFolderID": "int", "Description": _T},
    "dbo.ResultState": {"ResultStateName": _T},
    "dbo.Result": {"ResultID": "int", "FolderID": "int", "Description": _T, "SystemName": _T, "RunIndex": "int",
                   "ResultStateName": _T, "Created": "datetimeoffset", "tstamp": "timestamp"},
    "dbo.Chromatogram": {"ChromatogramID": "int", "ResultID": "int", "OriginalResultID": "int", "Description": _T,
                         "IsReadOnly": "bit", "ChromatogramLayoutXml": _T, "TimeUnit": _T, "VolumeUnit": _T,
                         "Created": "datetimeoffset"},
    "dbo.Curve": {"ChromatogramID": "int", "ChromatogramPos": "int", "Description": _T, "IsoCroneTypeName": _T,
                  "InjectionEventCurveChromID": "int", "InjectionEventCurveChromPos": "int",
                  "CurveDataTypeName": _T, "DistanceBetweenPoints": "real", "DistanceToStartPoint": "real",
                  "MethodStartTime": "datetimeoffset", "TimeUnit": _T, "VolumeUnit": _T,
                  "AmplitudePrecision": "int", "AmplitudeUnit": _T, "IsOriginalData": "bit", "CompressionDesc": _T,
                  "ColumnVolume": "float", "ColumnVolumeUnitName": _T},
    "dbo.CurvePoint": {"ChromatogramID": "int", "ChromatogramPos": "int", "IsFullResolution": "bit",
                       "BinaryCurvePoints": "varbinary"},
    "dbo.CurveUVInfo": {"ChromatogramID": "int", "ChromatogramPos": "int",
                        "IsUVValuesNormalizedToNominalUVPathLength": "bit", "UVPathLength": "real",
                        "NominalUVPathLength": "real", "IsSystemProvided": "bit"},
    "dbo.EventCurve": {"ChromatogramID": "int", "ChromatogramPos": "int", "EventCurveTypeName": _T,
                       "Description": _T, "TimeUnit": _T, "VolumeUnit": _T, "IsOriginalData": "bit"},
    "dbo.Event": {"ChromatogramID": "int", "ChromatogramPos": "int", "EventCurvePos": "int", "EventTypeName": _T,
                  "EventSubTypeName": _T, "EventTime": "real", "EventVolume": "real", "EventText": _T,
                  "InstructionFeedback": "int"},
    "dbo.ResultAuditTrail": {"ResultID": "int"},  # *Data columns added from the sample
    "dbo.ResultRunInformation": {"ResultID": "int", "RunInformationTypeName": _T, "RunInformation": _T},
    "dbo.ResultSearchCriteria": {"ResultSearchCriteriaID": "int", "ResultID": "int", "SearchCriteriaName": _T,
                                 "KeyWord1": _T, "KeyWord2": _T, "ExtraDisplayInformation": _T},
    "dbo.ResultDoc_AuditTrail": {"ResultID": "int", "AuditTrailID": "int", "tstamp": "timestamp"},
    "dbo.AuditTrail": {"AuditTrailID": "int", "AuditTrailType": _T, "CreatedBy": _T},
    "dbo.AuditTrailEntry": {"AuditTrailEntryID": "int", "AuditTrailID": "int", "LogEntry": _T},
    "dbo.DoEResult": {"DoEResultID": "int", "Description": _T},
    "dbo.DoEResult_Result": {"DoEResultID": "int", "ResultID": "int"},
    "dbo.UCSystem": {"SystemName": _T, "InstrumentConfigurationID": "int"},
    "dbo.InstrumentConfiguration": {"InstrumentConfigurationID": "int", "FirmwareID": "int"},
    "dbo.Firmware": {"FirmwareID": "int", "Data": "varbinary"},
}
PRIMARY_KEYS = {
    "dbo.Folder": ("FolderID",), "dbo.ResultState": ("ResultStateName",), "dbo.Result": ("ResultID",),
    "dbo.Chromatogram": ("ChromatogramID",), "dbo.Curve": ("ChromatogramID", "ChromatogramPos"),
    "dbo.CurvePoint": ("ChromatogramID", "ChromatogramPos", "IsFullResolution"),
    "dbo.CurveUVInfo": ("ChromatogramID", "ChromatogramPos"),
    "dbo.EventCurve": ("ChromatogramID", "ChromatogramPos"),
    "dbo.Event": ("ChromatogramID", "ChromatogramPos", "EventCurvePos"),
    "dbo.ResultAuditTrail": ("ResultID",), "dbo.ResultSearchCriteria": ("ResultSearchCriteriaID",),
    "dbo.ResultRunInformation": ("ResultID", "RunInformationTypeName"),
    "dbo.ResultDoc_AuditTrail": ("ResultID", "AuditTrailID"), "dbo.AuditTrail": ("AuditTrailID",),
    "dbo.AuditTrailEntry": ("AuditTrailEntryID",), "dbo.DoEResult": ("DoEResultID",),
    "dbo.DoEResult_Result": ("DoEResultID", "ResultID"), "dbo.UCSystem": ("SystemName",),
    "dbo.InstrumentConfiguration": ("InstrumentConfigurationID",), "dbo.Firmware": ("FirmwareID",),
}
_CURVE_KEY = ("ChromatogramID", "ChromatogramPos")
FOREIGN_KEYS = [
    ForeignKey("fk1", "dbo.Result", ("FolderID",), "dbo.Folder", ("FolderID",)),
    ForeignKey("fk2", "dbo.Folder", ("ParentFolderID",), "dbo.Folder", ("FolderID",)),
    ForeignKey("fk3", "dbo.Result", ("ResultStateName",), "dbo.ResultState", ("ResultStateName",)),
    ForeignKey("fk4", "dbo.Chromatogram", ("ResultID",), "dbo.Result", ("ResultID",)),
    ForeignKey("fk5", "dbo.Chromatogram", ("OriginalResultID",), "dbo.Result", ("ResultID",)),
    ForeignKey("fk6", "dbo.Curve", ("ChromatogramID",), "dbo.Chromatogram", ("ChromatogramID",)),
    ForeignKey("fk7", "dbo.Curve", ("InjectionEventCurveChromID", "InjectionEventCurveChromPos"),
               "dbo.EventCurve", _CURVE_KEY),
    ForeignKey("fk8", "dbo.CurvePoint", _CURVE_KEY, "dbo.Curve", _CURVE_KEY),
    ForeignKey("fk9", "dbo.CurveUVInfo", _CURVE_KEY, "dbo.Curve", _CURVE_KEY),
    ForeignKey("fk10", "dbo.EventCurve", ("ChromatogramID",), "dbo.Chromatogram", ("ChromatogramID",)),
    ForeignKey("fk11", "dbo.Event", _CURVE_KEY, "dbo.EventCurve", _CURVE_KEY),
    ForeignKey("fk12", "dbo.ResultAuditTrail", ("ResultID",), "dbo.Result", ("ResultID",)),
    ForeignKey("fk13", "dbo.ResultRunInformation", ("ResultID",), "dbo.Result", ("ResultID",)),
    ForeignKey("fk14", "dbo.ResultSearchCriteria", ("ResultID",), "dbo.Result", ("ResultID",)),
    ForeignKey("fk15", "dbo.ResultDoc_AuditTrail", ("ResultID",), "dbo.Result", ("ResultID",)),
    ForeignKey("fk16", "dbo.ResultDoc_AuditTrail", ("AuditTrailID",), "dbo.AuditTrail", ("AuditTrailID",)),
    ForeignKey("fk17", "dbo.AuditTrailEntry", ("AuditTrailID",), "dbo.AuditTrail", ("AuditTrailID",)),
    ForeignKey("fk18", "dbo.DoEResult_Result", ("ResultID",), "dbo.Result", ("ResultID",)),
    ForeignKey("fk19", "dbo.DoEResult_Result", ("DoEResultID",), "dbo.DoEResult", ("DoEResultID",)),
    ForeignKey("fk20", "dbo.UCSystem", ("InstrumentConfigurationID",), "dbo.InstrumentConfiguration",
               ("InstrumentConfigurationID",)),
    ForeignKey("fk21", "dbo.InstrumentConfiguration", ("FirmwareID",), "dbo.Firmware", ("FirmwareID",)),
]


# cross-references; every other column is NOT NULL
NULLABLE = {
    "dbo.Folder": {"ParentFolderID"},
    "dbo.Chromatogram": {"OriginalResultID"},
    "dbo.Curve": {"InjectionEventCurveChromID", "InjectionEventCurveChromPos"},
}


def _schema(columns=COLUMNS):
    return SchemaGraph({t: OrderedDict(c) for t, c in columns.items()}, dict(PRIMARY_KEYS),
                       _with_implied_keys(columns, PRIMARY_KEYS, FOREIGN_KEYS), NULLABLE)


def _same(a, b):
    # SQL Server's default collation: case-insensitive, trailing spaces ignored
    if isinstance(a, str) and isinstance(b, str):
        return a.rstrip().casefold() == b.rstrip().casefold()
    return a == b


class MemoryFetcher:
    def __init__(self, data):
        self.data = data
        self.queried = []

    def rows(self, table, columns, keys):
        keys = list(keys)
        self.queried.append(table)
        return [OrderedDict(r) for r in self.data.get(table, [])
                if any(all(_same(r[c], k) for c, k in zip(columns, key)) for key in keys)]

    def count(self, table, columns, key):
        return sum(1 for r in self.data.get(table, []) if all(_same(r[c], k) for c, k in zip(columns, key)))


def _row(table, **values):
    return OrderedDict((c, values.get(c)) for c in COLUMNS[table])


def _small_database():
    """
    Result 1, plus result 2 holding a copy of result 1's chromatogram and sharing a DoE with it.
    Result 1 also has a chromatogram copied in from result 2. Result 2 was imported: its system
    is not in this database.
    """
    return {
        "dbo.Folder": [_row("dbo.Folder", FolderID=3, Description="Root"),
                       _row("dbo.Folder", FolderID=4, ParentFolderID=3, Description="Sub")],
        "dbo.ResultState": [_row("dbo.ResultState", ResultStateName="Completed")],
        "dbo.Result": [_row("dbo.Result", ResultID=1, FolderID=4, Description="Run", ResultStateName="completed",
                            SystemName="AKTA 1"),
                       _row("dbo.Result", ResultID=2, FolderID=3, Description="Copy", ResultStateName="Completed",
                            SystemName="Elsewhere")],
        "dbo.UCSystem": [_row("dbo.UCSystem", SystemName="akta 1", InstrumentConfigurationID=5)],
        "dbo.InstrumentConfiguration": [_row("dbo.InstrumentConfiguration", InstrumentConfigurationID=5,
                                             FirmwareID=6)],
        "dbo.Firmware": [_row("dbo.Firmware", FirmwareID=6, Data=b"large")],
        "dbo.Chromatogram": [_row("dbo.Chromatogram", ChromatogramID=10, ResultID=1, OriginalResultID=1),
                             _row("dbo.Chromatogram", ChromatogramID=11, ResultID=1, OriginalResultID=2),
                             _row("dbo.Chromatogram", ChromatogramID=20, ResultID=2, OriginalResultID=1)],
        "dbo.EventCurve": [_row("dbo.EventCurve", ChromatogramID=10, ChromatogramPos=12),
                           _row("dbo.EventCurve", ChromatogramID=20, ChromatogramPos=12)],
        "dbo.Curve": [_row("dbo.Curve", ChromatogramID=10, ChromatogramPos=1, InjectionEventCurveChromID=10,
                           InjectionEventCurveChromPos=12),
                      _row("dbo.Curve", ChromatogramID=11, ChromatogramPos=1),
                      _row("dbo.Curve", ChromatogramID=20, ChromatogramPos=1)],
        "dbo.ResultDoc_AuditTrail": [_row("dbo.ResultDoc_AuditTrail", ResultID=1, AuditTrailID=100),
                                     _row("dbo.ResultDoc_AuditTrail", ResultID=2, AuditTrailID=200)],
        "dbo.AuditTrail": [_row("dbo.AuditTrail", AuditTrailID=100), _row("dbo.AuditTrail", AuditTrailID=200)],
        "dbo.AuditTrailEntry": [_row("dbo.AuditTrailEntry", AuditTrailEntryID=1, AuditTrailID=100),
                                _row("dbo.AuditTrailEntry", AuditTrailEntryID=2, AuditTrailID=100),
                                _row("dbo.AuditTrailEntry", AuditTrailEntryID=3, AuditTrailID=200)],
        "dbo.DoEResult": [_row("dbo.DoEResult", DoEResultID=7)],
        "dbo.DoEResult_Result": [_row("dbo.DoEResult_Result", DoEResultID=7, ResultID=1),
                                 _row("dbo.DoEResult_Result", DoEResultID=7, ResultID=2)],
    }


def test_walker_collects_owned_linked_and_referenced_rows():
    tables = collect_result_rows(MemoryFetcher(_small_database()), _schema(), 1)
    relations = {name: t.relation for name, t in tables.items()}
    assert relations == {
        "dbo.Result": "owned", "dbo.Chromatogram": "owned", "dbo.EventCurve": "owned", "dbo.Curve": "owned",
        "dbo.ResultDoc_AuditTrail": "owned", "dbo.DoEResult_Result": "owned",
        "dbo.AuditTrail": "linked", "dbo.AuditTrailEntry": "linked",
        "dbo.Folder": "referenced", "dbo.ResultState": "referenced", "dbo.DoEResult": "referenced",
        "dbo.UCSystem": "referenced", "dbo.InstrumentConfiguration": "referenced",
    }
    # OriginalResultID is a cross-reference: chromatogram 11 came from result 2 but belongs to
    # result 1, and the copy (20) in result 2 points back at result 1 but belongs to result 2
    assert [r["ChromatogramID"] for r in tables["dbo.Chromatogram"].rows] == [10, 11]
    assert {r["ChromatogramID"] for r in tables["dbo.Curve"].rows} == {10, 11}
    assert [r["AuditTrailEntryID"] for r in tables["dbo.AuditTrailEntry"].rows] == [1, 2]
    # shared with result 2: only the row itself, nothing below it
    assert [r["DoEResultID"] for r in tables["dbo.DoEResult"].rows] == [7]
    # the whole folder chain, and lookups matched case-insensitively
    assert {r["FolderID"] for r in tables["dbo.Folder"].rows} == {3, 4}
    assert len(tables["dbo.ResultState"].rows) == 1


def test_walker_skips_a_system_this_database_does_not_have():
    # an imported result: its SystemName matches no dbo.UCSystem row
    fetcher = MemoryFetcher(_small_database())
    tables = collect_result_rows(fetcher, _schema(), 2)
    assert "dbo.UCSystem" not in tables and "dbo.InstrumentConfiguration" not in tables
    assert "dbo.Firmware" not in fetcher.queried


def test_walker_stops_at_the_instrument_configuration():
    fetcher = MemoryFetcher(_small_database())
    collect_result_rows(fetcher, _schema(), 1)
    assert "dbo.Firmware" not in fetcher.queried


def test_walker_raises_for_unknown_result():
    with pytest.raises(LookupError):
        collect_result_rows(MemoryFetcher(_small_database()), _schema(), 99)


def test_text_and_element_match_the_bundle_xml():
    assert _text(True, "bit") == "true"
    assert _text(float(np.float32(0.001666667)), "real") == "0.001666667"
    assert _text(0.0016666666697710752, "real") == "0.0016666667"
    assert _text(float(np.float32(5.027)), "float") == "5.027"  # widened float32
    assert _text(2.0, "real") == "2"
    assert _text(0.1, "float") == "0.1"
    assert _text("", "nvarchar") is None
    assert _text("a\r\nb\r\n", "nvarchar") == "a\nb"
    tz = datetime.timezone(datetime.timedelta(hours=-5))
    assert _text(datetime.datetime(2023, 3, 10, 16, 30, 7, 838000, tzinfo=tz), "datetimeoffset") == \
        "2023-03-10T16:30:07.838"
    assert _element("X", []) is None
    assert _element("X", [1]) == {"X": 1}
    assert _element("X", [1, 2]) == {"X": [1, 2]}


# ---------------------------------------------------------------------------
# samples/sample.zip as database rows
# ---------------------------------------------------------------------------

def _value(text, data_type):
    if text is None:
        return None
    if data_type == "bit":
        return text == "true"
    if data_type == "int":
        return int(text)
    if data_type in ("real", "float"):
        value = float(text)
        return float(np.float32(value)) if data_type == "real" else value
    return text


def _from_xml(table, xml, rename=None, **extra):
    """A database row from a bundle XML element (the reverse of the reader's mapping)."""
    rename = rename or {}
    columns = COLUMNS[table]
    row = OrderedDict((c, None) for c in columns)
    for column, data_type in columns.items():
        key = rename.get(column, column)
        if data_type == "datetimeoffset" and xml.get(key):
            offset = datetime.timezone(datetime.timedelta(minutes=int(xml[key + "UtcOffsetMinutes"])))
            row[column] = datetime.datetime.fromisoformat(xml[key]).replace(tzinfo=offset)
        elif key in xml and not isinstance(xml[key], dict):
            row[column] = _value(xml[key], data_type)
    row.update(extra)
    return row


def _sample_database(run):
    chrom = run.chromatogram()
    chrom_id = 10
    data = {t: [] for t in COLUMNS}
    data["dbo.Folder"] = [_row("dbo.Folder", FolderID=1 + i, ParentFolderID=i or None, Description=name)
                          for i, name in enumerate(run.result["FolderPath"].strip("/").split("/"))]
    result_xml = run.entries["Result.xml"].value["Result"]
    data["dbo.Result"] = [_from_xml("dbo.Result", result_xml, {"Description": "Name", "ResultStateName": "ResultState"},
                                    ResultID=1, FolderID=len(data["dbo.Folder"]))]
    data["dbo.ResultState"] = [_row("dbo.ResultState", ResultStateName=result_xml["ResultState"])]
    data["dbo.Chromatogram"] = [_from_xml("dbo.Chromatogram", chrom.info and run.entries["Chrom.1.Xml"].value["Chromatogram"],
                                          {"Description": "ChromatogramName", "IsReadOnly": "IsReadonly"},
                                          ChromatogramID=chrom_id, ResultID=1, OriginalResultID=1)]
    for curve in chrom.curves.values():
        key = dict(ChromatogramID=chrom_id, ChromatogramPos=curve.curve_number)
        injection = (curve.xml.get("InjectionEventCurve") or {}).get("CurveNumber")
        data["dbo.Curve"].append(_from_xml(
            "dbo.Curve", curve.xml, {"Description": "Name", "IsoCroneTypeName": "IsoChroneType",
                                     "CurveDataTypeName": "@CurveDataType"},
            InjectionEventCurveChromID=chrom_id if injection else None,
            InjectionEventCurveChromPos=int(injection) if injection else None, **key))
        for ps in curve.point_sets:
            data["dbo.CurvePoint"].append(_row("dbo.CurvePoint", IsFullResolution=ps.is_full_resolution,
                                               BinaryCurvePoints=run.entries[ps.file_name].raw, **key))
        if curve.xml.get("CurveUVInfo"):
            data["dbo.CurveUVInfo"].append(_from_xml("dbo.CurveUVInfo", curve.xml["CurveUVInfo"], **key))
    for ec in chrom.event_curves.values():
        key = dict(ChromatogramID=chrom_id, ChromatogramPos=ec.curve_number)
        data["dbo.EventCurve"].append(_from_xml("dbo.EventCurve", ec.xml, {"Description": "Name",
                                                                           "EventCurveTypeName": "@EventCurveType"}, **key))
        for pos, e in enumerate(ec.events):
            data["dbo.Event"].append(_from_xml("dbo.Event", e.xml, {"EventTypeName": "@EventType",
                                                                    "EventSubTypeName": "@EventSubType"},
                                               EventCurvePos=pos, **key))
    columns = dict(COLUMNS)
    columns["dbo.ResultAuditTrail"] = dict(COLUMNS["dbo.ResultAuditTrail"], **{n: "varbinary" for n in run.metadata})
    data["dbo.ResultAuditTrail"] = [OrderedDict([("ResultID", 1)] + [(n, run.entries[n].raw) for n in run.metadata])]
    data["dbo.ResultRunInformation"] = [_row("dbo.ResultRunInformation", ResultID=1, RunInformationTypeName=i["type"],
                                             RunInformation=i["text"]) for i in run.result["run_information"]]
    data["dbo.ResultSearchCriteria"] = [
        _row("dbo.ResultSearchCriteria", ResultSearchCriteriaID=n, ResultID=1, SearchCriteriaName=c.get("Name"),
             KeyWord1=c.get("Keyword1"), KeyWord2=c.get("Keyword2"),
             ExtraDisplayInformation=(c["ExtraDisplayInformation"] or "") if "ExtraDisplayInformation" in c else None)
        for n, c in enumerate(run.result["search_criteria"])]
    data["dbo.ResultDoc_AuditTrail"] = [_row("dbo.ResultDoc_AuditTrail", ResultID=1, AuditTrailID=100)]
    data["dbo.AuditTrail"] = [_row("dbo.AuditTrail", AuditTrailID=100, AuditTrailType="Evaluation")]
    data["dbo.AuditTrailEntry"] = [_row("dbo.AuditTrailEntry", AuditTrailEntryID=1, AuditTrailID=100,
                                        LogEntry="Opened\r\n")]
    return data, columns


def _without_columns(data, columns, keep):
    """The database without every column `keep(table, column)` rejects (another UNICORN version's schema)."""
    columns = {t: OrderedDict((c, k) for c, k in cs.items() if keep(t, c)) for t, cs in columns.items()}
    data = {t: [OrderedDict((c, v) for c, v in r.items() if c in columns[t]) for r in rows] for t, rows in data.items()}
    return data, columns


def _key_columns():
    keys = {(t, c) for t, cs in PRIMARY_KEYS.items() for c in cs}
    keys |= {(fk.child, c) for fk in FOREIGN_KEYS for c in fk.child_columns}
    keys |= {(fk.parent, c) for fk in FOREIGN_KEYS for c in fk.parent_columns}
    return keys


@pytest.mark.parametrize("blobs", [True, False])
def test_run_builds_from_a_database_with_only_key_columns(blobs):
    # Export reads runs through this reader: a database from another UNICORN version that
    # lacks columns must still give a run (with less in it), never fail on a missing column.
    zip_run = open_run(SAMPLE)
    data, columns = _sample_database(zip_run)
    keys = _key_columns()
    data, columns = _without_columns(data, columns, lambda t, c: (t, c) in keys or (blobs and c == "BinaryCurvePoints")
                                     or t == "dbo.ResultAuditTrail")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        run = build_run(collect_result_rows(MemoryFetcher(data), _schema(columns), 1))
    assert run.origin["result_id"] == 1
    chrom, zchrom = run.chromatogram(), zip_run.chromatogram()
    assert len(chrom.curves) == len(zchrom.curves)
    if blobs:  # the points come through; names and types are gone with their columns
        assert sum(len(c.point_sets) for c in chrom.curves.values()) == \
            sum(len(c.point_sets) for c in zchrom.curves.values())


@pytest.fixture(scope="module")
def sample_pair():
    zip_run = open_run(SAMPLE)
    data, columns = _sample_database(zip_run)
    tables = collect_result_rows(MemoryFetcher(data), _schema(columns), 1)
    return zip_run, build_run(tables)


def test_run_from_rows_matches_zip_reader(sample_pair):
    z, d = sample_pair
    assert d.source == "sql" and d.locator == "sql:ResultID=1"
    assert d.origin == {"server": None, "port": None, "database": None, "result_id": 1}
    assert z.origin == {"file_name": "sample.zip", "path": os.path.abspath(SAMPLE)}
    assert d.warnings == z.warnings
    assert d.name == z.name and d.result["FolderPath"] == z.result["FolderPath"]
    assert d.result["search_criteria"] == z.result["search_criteria"]
    assert [(i["type"], i["text"], i["value"]) for i in d.result["run_information"]] == \
        [(i["type"], i["text"], i["value"]) for i in z.result["run_information"]]
    assert d.metadata == z.metadata
    assert all(d.entries[n].raw == z.entries[n].raw for n in z.metadata)
    assert d.evaluation_log[0]["AuditTrailEntries"][0]["LogEntry"] == "Opened"

    dc, zc = d.chromatogram(), z.chromatogram()
    assert list(dc.curves) == list(zc.curves) and list(dc.event_curves) == list(zc.event_curves)
    for name, b in zc.curves.items():
        a = dc.curve(name)
        for attr in ("data_type", "unit", "precision", "curve_number", "time_unit", "volume_unit", "isochrone",
                     "is_original", "column_volume_unit", "uv_info"):
            assert getattr(a, attr) == getattr(b, attr), (name, attr)
        assert np.array_equal(a.values, b.values) and np.array_equal(a.volume, b.volume)
        np.testing.assert_allclose(a.time, b.time, rtol=REL)
        assert a.column_volume == pytest.approx(b.column_volume, rel=REL)
    for name, b in zc.event_curves.items():
        a = dc.event_curves[name]
        assert [(e.text, e.type, e.subtype) for e in a.events] == [(e.text, e.type, e.subtype) for e in b.events]
        np.testing.assert_allclose([e.volume for e in a.events], [e.volume for e in b.events], rtol=REL)
    assert dc.retention_zero["source"] == zc.retention_zero["source"]
    assert dc.retention_zero["volume"] == pytest.approx(zc.retention_zero["volume"], rel=REL)


def test_run_keeps_every_row(sample_pair):
    _, d = sample_pair
    assert d.tables["dbo.CurvePoint"].relation == "owned"
    assert d.tables["dbo.AuditTrailEntry"].rows[0]["LogEntry"] == "Opened\r\n"  # as stored
    frame = d.tables["dbo.Curve"].to_frame()
    assert len(frame) == len(d.chromatogram().curves)


def test_run_to_res3_dict_matches_zip(sample_pair):
    z, d = sample_pair
    zd, dd = z.to_res3_dict(), d.to_res3_dict()
    assert list(dd) == list(zd)
    for chrom in zd:
        assert list(dd[chrom]) == list(zd[chrom])
        for name, entry in zd[chrom].items():
            if "unit" in entry:  # curves: the same float32 values
                assert dd[chrom][name] == entry


# ---------------------------------------------------------------------------
# Parity with the real database (opt-in)
# ---------------------------------------------------------------------------

def _matched_runs():
    runs = [open_run(p) for p in sorted(glob.glob(os.path.join(TEST_FILES, "U7.*", "*.zip")))]
    for path in sorted(glob.glob(os.path.join(TEST_FILES, "UFOL", "*.UFol"))):
        runs += open_folder(path)
    return runs


@pytest.fixture(scope="module")
def database():
    from pycorn.db_connector import DEFAULT_ENV_FILE, PyCORNDatabase
    if not DEFAULT_ENV_FILE.exists():
        pytest.skip("no sql/.env")
    db = PyCORNDatabase()
    if not db.test_connection(quiet=True):
        pytest.skip("database not reachable")
    return db


def _num_equal(a, b):
    if isinstance(a, dict) and isinstance(b, dict):  # element order differs between the sources
        return dict(a) == dict(b)
    if isinstance(a, bool) or isinstance(b, bool) or not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        return a == b
    return a == pytest.approx(b, rel=REL, nan_ok=True)


def test_database_matches_zip_exports(database):
    runs = _matched_runs()
    if not runs:
        pytest.skip("no matched exports in unicorn_test_files/")
    failures = []
    conn = database._connect()
    try:
        cursor = conn.cursor()
        for index, z in enumerate(runs):
            cursor.execute("SELECT ResultID FROM dbo.Result WHERE Description = %s AND SystemName = %s "
                           "AND RunIndex = %s", (z.name, z.result.get("SystemName"), z.result.get("RunIndex")))
            ids = [row[0] for row in cursor.fetchall()]
            if len(ids) != 1:
                failures.append("run %d: %d matching results" % (index, len(ids)))
                continue
            d = database.load_run(ids[0])
            failures += ["run %d: %s" % (index, f) for f in _differences(z, d)]
            # compared, never printed: these are connection details
            if d.origin["server"] != database.conn_params.server or not d.origin["database"]                     or d.origin["result_id"] != ids[0]:
                failures.append("run %d: origin" % index)
            if z.origin["file_name"] is None:
                failures.append("run %d: file origin" % index)
    finally:
        conn.close()
    assert not failures, "\n".join(failures)


NUMBER = re.compile(r"-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?")


def _warnings_equal(a, b):
    """Same warnings, numbers within the XML's 7-digit rounding (the ZIP rounds event volumes)."""
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        nx, ny = NUMBER.findall(x), NUMBER.findall(y)
        if NUMBER.sub("#", x) != NUMBER.sub("#", y) or len(nx) != len(ny) or \
                any(float(i) != pytest.approx(float(j), rel=1e-5) for i, j in zip(nx, ny)):
            return False
    return True


def _differences(z, d):
    """What differs between a ZIP run and the database run, by field name only."""
    out = []

    def comparable(ws):
        # where the start is kept to the whole second differs by source (imported results in the
        # database, UNICORN 7.11 exports); the UTC it shifts is compared within 1 s elsewhere
        return [w for w in ws if "is stored to the whole second" not in w]

    if not _warnings_equal(comparable(d.warnings), comparable(z.warnings)):
        out.append("warnings")
    if d.result["search_criteria"] != z.result["search_criteria"]:
        out.append("search criteria")
    if [(i["type"], i["text"]) for i in d.result["run_information"]] != \
            [(i["type"], i["text"]) for i in z.result["run_information"]]:
        out.append("run information")
    if list(d.metadata) != list(z.metadata) or any(d.entries[n].raw != z.entries[n].raw for n in z.metadata):
        out.append("metadata")
    if [[e["LogEntry"] for e in t["AuditTrailEntries"]] for t in d.evaluation_log] != \
            [[e["LogEntry"] for e in t["AuditTrailEntries"]] for t in z.evaluation_log]:
        out.append("evaluation log")
    if list(d.chromatograms) != list(z.chromatograms):
        return out + ["chromatogram names"]
    for chrom_name, zc in z.chromatograms.items():
        dc = d.chromatograms[chrom_name]
        if list(dc.curves) != list(zc.curves):
            out.append("curve names")
            continue
        for name, b in zc.curves.items():
            a = dc.curves[name]
            for attr in ("data_type", "unit", "precision", "curve_number", "time_unit", "volume_unit", "isochrone",
                         "is_original", "uv_info", "column_volume_unit"):
                if getattr(a, attr) != getattr(b, attr):
                    out.append("curve %s: %s" % (b.curve_number, attr))
            if not _num_equal(a.column_volume, b.column_volume):
                out.append("curve %s: column_volume" % b.curve_number)
            for axis in ("values", "volume", "time"):
                x, y = getattr(a, axis), getattr(b, axis)
                exact = axis == "values" or (axis == "volume" and b.isochrone != "Volume")
                if (x is None) != (y is None) or (x is not None and not (
                        np.array_equal(x, y) if exact else np.allclose(x, y, rtol=REL, atol=0))):
                    out.append("curve %s: %s" % (b.curve_number, axis))
        if list(dc.event_curves) != list(zc.event_curves):
            out.append("event curve names")
            continue
        for name, b in zc.event_curves.items():
            a = dc.event_curves[name]
            if [(e.text, e.type, e.subtype, e.instruction_feedback) for e in a.events] != \
                    [(e.text, e.type, e.subtype, e.instruction_feedback) for e in b.events]:
                out.append("events of %s" % b.curve_number)
            elif not all(_num_equal(x.time, y.time) and _num_equal(x.volume, y.volume)
                         for x, y in zip(a.events, b.events)):
                out.append("event axes of %s" % b.curve_number)
        if len(dc.peak_tables) != len(zc.peak_tables):
            out.append("peak tables")
            continue
        for a, b in zip(dc.peak_tables, zc.peak_tables):
            if len(a.peaks) != len(b.peaks) or any(
                    k in p and not _num_equal(p[k], q[k]) for p, q in zip(a.peaks, b.peaks) for k in q):
                out.append("peaks")
            if any(k not in a.info or not _num_equal(a.info[k], b.info[k])
                   for k in b.info if k not in ("Created", "CreatedUtcOffsetMinutes")):
                out.append("peak table fields")
        za, da = zc.retention_zero, dc.retention_zero
        if (za["source"], za["injection_number"]) != (da["source"], da["injection_number"]) or \
                not (_num_equal(za["time"], da["time"]) and _num_equal(za["volume"], da["volume"])):
            out.append("retention zero")
        out += _axes_differences(zc, dc)
    return out


def _close_utc(x, y, seconds=1.0):
    """UTC arrays equal within `seconds` (imported results store whole-second start times), NaT in the same places."""
    if (x is None) != (y is None):
        return False
    if x is None:
        return True
    if len(x) != len(y) or not np.array_equal(np.isnat(x), np.isnat(y)):
        return False
    ok = ~np.isnat(x)
    return bool(np.all(np.abs((x[ok] - y[ok]).astype("timedelta64[us]").astype(np.int64)) <= seconds * 1e6))


def _axes_differences(zc, dc):
    """Vc, column path, timeline, CV and UTC of one chromatogram."""
    out = []
    zi, di = zc.column_volume_info, dc.column_volume_info
    if (zi["unit"], zi["source"]) != (di["unit"], di["source"]) or not _num_equal(zi["value"], di["value"]):
        out.append("column volume")
    zb, db = zc.column_path["bypass"], dc.column_path["bypass"]
    if len(zb) != len(db) or not all(_num_equal(a, b) for x, y in zip(zb, db) for a, b in zip(x, y)):
        out.append("by-pass")
    zt, dt_ = zc.timeline, dc.timeline
    if (zt["quality"], zt["start_source"], len(zt["pauses"])) != (dt_["quality"], dt_["start_source"], len(dt_["pauses"])) \
            or any(abs(p["duration_s"] - q["duration_s"]) > 1.0 or p["source"] != q["source"]
                   for p, q in zip(zt["pauses"], dt_["pauses"])):
        out.append("timeline")
    for name, b in zc.curves.items():
        a = dc.curves[name]
        if (a.cv is None) != (b.cv is None) or (a.cv is not None and not np.allclose(a.cv, b.cv, rtol=REL, atol=1e-9)):
            out.append("curve %s: cv" % b.curve_number)
        if not _close_utc(a.utc, b.utc):
            out.append("curve %s: utc" % b.curve_number)
    for name, b in zc.event_curves.items():
        a = dc.event_curves[name]
        for x, y in zip(a.events, b.events):
            if not _num_equal(x.cv, y.cv) or (x.utc is None) != (y.utc is None) or \
                    (x.utc is not None and abs((x.utc - y.utc) / np.timedelta64(1, "s")) > 1.0):
                out.append("event cv/utc of %s" % b.curve_number)
                break
    return out
