"""
Run identity, copies, file catalogs and batch loading (plan section 5; proposal build step 4):
`pycorn.search.match_key` / `mark_copies`, `pycorn.file_catalog.catalog_from_files`,
`PyCORNDatabase.find(dedupe=)`, `load_many()` and `match_files()`.

Offline tests use synthetic catalogs and `samples/sample.zip`. Live tests (skipped without
sql/.env or the server) match every local export to its database result and compare the
file catalog with `find()`; they report only pass/fail and counts (no names, no connection
values).
"""
import datetime as dt
import math
import os
import shutil
import warnings
import zipfile

import pandas as pd
import pytest

from pycorn import catalog_from_files, open_run
from pycorn.column_geometry import used_columns
from pycorn.file_catalog import FILE_CATALOG_COLUMNS, find_files
from pycorn.search import (CATALOG_COLUMNS, _Params, _match_key_filter, build_query, copy_keys, duplicate_group,
                           mark_copies, match_key, wide)

SAMPLE = os.path.join("..", "samples", "sample.zip")
TEST_FILES = os.path.join("..", "unicorn_test_files")
UTC = dt.timezone.utc


# ---------------------------------------------------------------------------
# Offline: the match key and copy marking
# ---------------------------------------------------------------------------

def test_match_key_cuts_to_whole_seconds_in_utc():
    exact = dt.datetime(2021, 7, 23, 14, 2, 33, 772000, tzinfo=UTC)
    cut = dt.datetime(2021, 7, 23, 10, 2, 33, tzinfo=dt.timezone(dt.timedelta(hours=-4)))  # same second, local
    assert match_key(exact, "AKTA", 3) == match_key(cut, "AKTA", 3) == "2021-07-23T14:02:33Z|AKTA|3"
    assert match_key(exact + dt.timedelta(seconds=1), "AKTA", 3) != match_key(exact, "AKTA", 3)
    assert match_key(exact, "AKTA", 4) != match_key(exact, "AKTA", 3)
    assert match_key(exact, None, None) == "2021-07-23T14:02:33Z||"
    assert match_key(pd.NaT, "AKTA", 3) is None
    assert match_key(None, "AKTA", 3) is None


def _catalog(rows):
    cols = ["source", "database", "result_id", "name", "first_chromatogram_utc", "system", "run_index", "modified_utc"]
    cat = pd.DataFrame(rows, columns=cols)
    for c in ("first_chromatogram_utc", "modified_utc"):
        cat[c] = pd.to_datetime(cat[c], utc=True, format="ISO8601")
    cat["match_key"] = [match_key(*v) for v in zip(cat["first_chromatogram_utc"], cat["system"], cat["run_index"])]
    return cat


T0 = "2024-01-02T03:04:05.250Z"


def test_mark_copies_counts_and_groups():
    # an evaluated copy under another name is the same run (owner, 2026-11-08)
    copies = _catalog([
        ("a", "DB1", 1, "Run", T0, "S", 1, "2024-02-01T00:00:00Z"),
        ("a", "DB2", 7, "Run (evaluated)", "2024-01-02T03:04:05Z", "S", 1, "2024-03-01T00:00:00Z"),
        ("a", "DB1", 2, "Run", "2024-01-02T03:04:06Z", "S", 1, "2024-02-01T00:00:00Z"),
    ])
    page = copies.iloc[[0, 2]].reset_index(drop=True)
    out = mark_copies(page, copies)
    assert list(out["n_copies"]) == [2, 1]
    assert out["duplicate_group"].iloc[0] == duplicate_group(page["match_key"].iloc[0])
    assert pd.isna(out["duplicate_group"].iloc[1])
    # dedupe keeps the most recently changed copy (DB2, on another page): the DB1 row goes
    kept = mark_copies(page, copies, dedupe=True)
    assert list(kept["result_id"]) == [2]
    other = mark_copies(copies.iloc[[1]].reset_index(drop=True), copies, dedupe=True)
    assert list(other["result_id"]) == [7]


def test_dedupe_tie_and_unknown_change_time():
    tie = _catalog([
        ("a", "DB1", 1, "Run", T0, "S", 1, "2024-02-01T00:00:00Z"),
        ("b", "DB9", 5, "Run", T0, "S", 1, "2024-02-01T00:00:00Z"),
    ])
    assert list(mark_copies(tie, tie, dedupe=True)["source"]) == ["a"]  # first in search order
    unknown = _catalog([
        ("a", "DB1", 1, "Run", T0, "S", 1, None),
        ("b", "DB9", 5, "Run", T0, "S", 1, "2020-01-01T00:00:00Z"),
    ])
    assert list(mark_copies(unknown, unknown, dedupe=True)["source"]) == ["b"]  # unknown time last


def test_rows_without_a_key_are_kept_and_unknown():
    cat = _catalog([("a", "DB1", 1, "Run", None, "S", 1, None)])
    out = mark_copies(cat, cat, dedupe=True)
    assert len(out) == 1 and pd.isna(out["n_copies"].iloc[0]) and pd.isna(out["duplicate_group"].iloc[0])


def test_copy_lookup_sql():
    cat = _catalog([("a", "DB1", 1, "Run", T0, "AKTA's", None, None), ("a", "DB1", 2, "Copy", T0, "AKTA's", None, None)])
    keys = copy_keys(cat)
    assert keys == [("AKTA's", None, "2024-01-02T03:04:05")]  # distinct, cut to the second, no name
    params = _Params()
    clause = _match_key_filter(keys, params)
    assert "AKTA's" not in clause and list(params.values()) == ["AKTA's", None, "2024-01-02T03:04:05"]
    assert "r.RunIndex IS NULL AND k.i IS NULL" in clause and "r.SystemName IS NULL AND k.s IS NULL" in clause
    assert "DATEADD(second, 1" in clause and "Description" not in clause
    sql, params, _ = build_query("copies", match_keys=keys, name="Run*")
    code = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    assert "TOP" not in code.split("FROM")[0] and "ORDER BY r.ResultID" in code and "k(s, i, t)" in code
    assert _match_key_filter([], _Params()) == "1 = 0"


# ---------------------------------------------------------------------------
# Offline: file catalog
# ---------------------------------------------------------------------------

def test_file_catalog_of_the_sample_matches_the_full_read():
    cat, variables = catalog_from_files(SAMPLE, variables=True)
    assert list(cat.columns) == FILE_CATALOG_COLUMNS and len(cat) == 1
    assert set(CATALOG_COLUMNS) < set(cat.columns)
    row = cat.iloc[0]
    run = open_run(SAMPLE)
    assert row["source"] == "zip" and row["member"] == "" and row["path"] == os.path.abspath(SAMPLE)
    assert row["name"] == run.name and row["system"] == run.result.get("SystemName")
    assert row["run_index"] == int(run.result.get("RunIndex")) and row["folder_path"] == run.result.get("FolderPath")
    assert row["n_chromatograms"] == len(run.chromatograms)
    assert row["has_peak_tables"] == any(c.peak_tables for c in run.chromatograms.values())
    names = [n for n, _, _ in used_columns(run)]
    assert row["column"] == (names[0] if names else None)
    assert row["match_key"] is not None and row["n_copies"] == 1 and pd.isna(row["duplicate_group"])
    assert row["first_chromatogram_utc"].tzinfo is not None and row["run_start_source"] in ("RunStartDate",
                                                                                           "Chromatogram.Created")
    mine = run.variables()
    assert list(variables["variable"].astype(object)) == list(mine["variable"])
    assert list(variables["value"]) == list(mine["value"])
    shown = wide(cat, variables)
    assert len(shown) == 1 and len(shown.columns) > len(cat.columns)


def test_file_catalog_marks_the_same_run_exported_twice(tmp_path):
    for name in ("one.zip", "two.zip"):
        shutil.copy(SAMPLE, tmp_path / name)
    (tmp_path / "notes.txt").write_text("not a result")
    assert len(find_files(tmp_path)) == 2
    cat = catalog_from_files([tmp_path, tmp_path / "one.zip"])  # listed twice, read once
    assert len(cat) == 2 and list(cat["n_copies"]) == [2, 2]
    assert cat["duplicate_group"].nunique() == 1 and cat["duplicate_group"].iloc[0] is not None


def test_find_files_says_why_unicorn_text_exports_are_skipped(tmp_path):
    shutil.copy(SAMPLE, tmp_path / "one.zip")
    (tmp_path / "one.asc").write_text("curves")
    (tmp_path / "notes.txt").write_text("not a result")
    with pytest.warns(UserWarning, match=r"1 \.asc file\(s\) skipped \(UNICORN text exports"):
        assert len(find_files(tmp_path)) == 1
    with pytest.raises(FileNotFoundError, match="no such file or folder"):
        find_files(tmp_path / "gone.zip")


def test_file_catalog_skips_unreadable_files(tmp_path):
    (tmp_path / "broken.zip").write_bytes(b"not a zip")
    with zipfile.ZipFile(tmp_path / "other.zip", "w") as z:  # a zip, but not a UNICORN result
        z.writestr("readme.txt", "hello")
    with pytest.warns(UserWarning) as caught:
        cat = catalog_from_files(tmp_path)
    assert len(cat) == 0 and list(cat.columns) == FILE_CATALOG_COLUMNS
    messages = sorted(str(w.message) for w in caught)
    assert messages[0].startswith("broken.zip not read: not a zip file, or the file is damaged")
    assert messages[1].startswith("other.zip not read: not a UNICORN result")
    assert [(os.path.basename(p), r.split(",")[0]) for p, r in cat.attrs["not_read"]] == [
        ("broken.zip", "not a zip file"), ("other.zip", "not a UNICORN result (no Result.xml and no chromatogram)")]


# ---------------------------------------------------------------------------
# Offline: loading file rows (load_files, load_many with file rows)
# ---------------------------------------------------------------------------

def _ufol(path, members):
    """A .UFol (plain tar) holding `members` ({member path: bundle path})."""
    import tarfile
    with tarfile.open(path, mode="w") as archive:
        for name, bundle in members.items():
            archive.add(bundle, arcname=name)
    return path


def test_run_match_key_equals_the_catalog_row():
    from pycorn.file_catalog import run_match_key
    assert run_match_key(open_run(SAMPLE)) == catalog_from_files(SAMPLE)["match_key"][0]


def test_load_files_zip_and_ufol_members(tmp_path):
    from pycorn import load_files
    shutil.copy(SAMPLE, tmp_path / "a.zip")
    ufol = _ufol(tmp_path / "f.UFol", {"Top/A.11.Result": SAMPLE, "Top/B.12.Result": SAMPLE})
    cat = catalog_from_files(tmp_path)  # path order: a.zip, f.UFol
    assert list(cat["member"]) == ["", "Top/A.11.Result", "Top/B.12.Result"]
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # nothing changed, nothing missing
        runs = list(load_files(cat))
    assert [r.source for r in runs] == ["zip", "ufol", "ufol"]
    assert [r.origin.get("member", "") for r in runs] == list(cat["member"])
    assert runs[0].origin["path"] == str(tmp_path / "a.zip")
    only = list(load_files(cat[cat["member"] == "Top/B.12.Result"]))
    assert len(only) == 1 and only[0].container["original_id"] == 12


def test_load_files_skips_missing_and_strict_raises(tmp_path):
    from pycorn import load_files
    shutil.copy(SAMPLE, tmp_path / "gone.zip")
    ufol = _ufol(tmp_path / "f.UFol", {"Top/A.11.Result": SAMPLE})
    cat = catalog_from_files([tmp_path / "gone.zip", ufol])
    os.remove(tmp_path / "gone.zip")
    cat.loc[cat["source"] == "ufol", "member"] = "Top/missing.13.Result"
    with pytest.warns(UserWarning) as caught:
        assert list(load_files(cat)) == []
    messages = [str(w.message) for w in caught]
    assert any("gone.zip not found" in m and m.endswith("; skipped") for m in messages)
    assert any("has no member Top/missing.13.Result" in m for m in messages)
    with pytest.raises(LookupError, match="has no member"):  # f.UFol comes first
        list(load_files(cat, strict=True))
    with pytest.raises(LookupError, match="gone.zip not found"):
        list(load_files(cat[cat["source"] == "zip"], strict=True))


def test_load_files_warns_when_a_file_changed(tmp_path):
    from pycorn import load_files
    cat = catalog_from_files(SAMPLE)
    cat.loc[0, "match_key"] = "2000-01-01T00:00:00Z|other|1"
    with pytest.warns(UserWarning, match="changed since the catalog"):
        assert len(list(load_files(cat))) == 1
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert len(list(load_files(cat, check_key=False))) == 1


def test_load_files_refuses_database_rows():
    from pycorn import load_files
    with pytest.raises(ValueError, match="load_many"):
        next(load_files(pd.DataFrame({"source": ["a"], "database": ["db"], "result_id": [1], "path": [None]})))


def _db_with_fake_rows(monkeypatch):
    """A PyCORNDatabase whose database rows load as stand-in runs (no connection)."""
    from types import SimpleNamespace
    from pycorn.db_connector import PyCORNDatabase
    db = PyCORNDatabase.__new__(PyCORNDatabase)
    monkeypatch.setattr(PyCORNDatabase, "_load_db_rows", lambda self, cat, **kw: iter(
        [SimpleNamespace(source="sql", origin={"source": s, "database": d, "result_id": int(r)})
         for s, d, r in zip(cat["source"], cat["database"], cat["result_id"])]))
    return db


def test_load_many_mixed_catalog_keeps_catalog_order(monkeypatch):
    db = _db_with_fake_rows(monkeypatch)
    files = catalog_from_files(SAMPLE)
    rows = pd.DataFrame({"source": ["a", "a"], "database": ["db", "db"], "result_id": [7, 8]})
    mixed = pd.concat([rows.iloc[:1], files, rows.iloc[1:]], ignore_index=True)
    runs = list(db.load_many(mixed))
    assert [r.source for r in runs] == ["sql", "zip", "sql"]
    assert [runs[0].origin["result_id"], runs[2].origin["result_id"]] == [7, 8]
    assert [r.source for r in db.load_many(files)] == ["zip"]


def test_load_many_refuses_rows_without_database_or_path(monkeypatch):
    db = _db_with_fake_rows(monkeypatch)
    with pytest.raises(ValueError, match="database and result_id"):
        next(db.load_many(pd.DataFrame({"source": ["a"], "database": [None], "result_id": [1]})))


# ---------------------------------------------------------------------------
# Live
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def database():
    from pycorn.db_connector import DEFAULT_ENV_FILE, PyCORNDatabase
    if not DEFAULT_ENV_FILE.exists():
        pytest.skip("no sql/.env")
    db = PyCORNDatabase()
    if not db.test_connection(quiet=True):
        pytest.skip("database not reachable")
    return db


@pytest.fixture(scope="module")
def files():
    if not os.path.isdir(TEST_FILES) or not find_files(TEST_FILES):
        pytest.skip("no exports in unicorn_test_files/")
    return catalog_from_files(TEST_FILES, variables=True)


def test_every_export_matches_one_database_result(database, files):
    cat, _ = files
    matches = database.match_files(cat, sources="discover")
    assert set(matches["status"]) <= {"exact", "copy"}, matches["status"].value_counts().to_dict()
    exact = matches[matches["status"] == "exact"]
    assert len(exact) == len(cat) and (exact["n_matches"] == 1).all()
    assert list(exact["path"] + exact["member"]) == list(cat["path"] + cat["member"])
    copies = matches[matches["status"] == "copy"]
    assert (copies["db_name"] != copies["name"]).all()
    assert (matches.groupby(["path", "member"])["n_copies"].first() >= 1).all()
    print("[OK] %d exports, each matched to one database result; %d other-named copies of %d of them"
          % (len(cat), len(copies), copies[["path", "member"]].drop_duplicates().shape[0]))


def test_file_catalog_equals_find(database, files):
    cat, variables = files
    matches = database.match_files(cat, sources="discover")
    matches = matches[matches["status"] == "exact"].reset_index(drop=True)
    bad = []
    for (_, f), (_, m) in zip(cat.iterrows(), matches.iterrows()):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            page = database.find(result_ids=[int(m["result_id"])], sources={m["source"]: [m["database"]]})
        row = page.iloc[0]
        for c in ("name", "system", "system_type", "run_type", "state", "run_index", "created_by", "column",
                  "n_chromatograms", "has_peak_tables", "run_start_source", "match_key"):
            a, b = f[c], row[c]
            if not (a == b or (pd.isna(a) and pd.isna(b))):
                bad.append(c)
        if f["run_start_source"] == "RunStartDate" and f["run_start_utc"] != row["run_start_utc"]:
            bad.append("run_start_utc")
        if abs((f["first_chromatogram_utc"] - row["first_chromatogram_utc"]).total_seconds()) >= 1:
            bad.append("first_chromatogram_utc")
        mine = variables[(variables["path"] == f["path"]) & (variables["member"] == f["member"])]
        theirs = database.variables(page)
        if list(mine["value"]) != list(theirs["value"]) or not all(
                a == b or (math.isnan(a) and math.isnan(b)) for a, b in zip(mine["number"], theirs["number"])):
            bad.append("variables")
    assert not bad, sorted(set(bad))
    print("[OK] %d file catalog rows equal their find() rows" % len(cat))


def test_find_marks_copies_and_dedupe_pages(database):
    pages = list(database.find_pages(page_size=100, sources="discover"))
    cat = pd.concat(pages, ignore_index=True)
    assert cat["n_copies"].dropna().ge(1).all()
    groups = cat["duplicate_group"].dropna()
    # every group is complete: its size equals n_copies of its rows
    for g, part in cat[cat["duplicate_group"].notna()].groupby("duplicate_group"):
        assert (part["n_copies"] == len(part)).all() and part["match_key"].nunique() == 1
    deduped = pd.concat(database.find_pages(page_size=100, sources="discover", dedupe=True), ignore_index=True)
    assert len(deduped) == len(cat) - (len(groups) - groups.nunique())
    assert not deduped["duplicate_group"].dropna().duplicated().any()
    # the copy kept is the most recently changed of its group
    newest = cat[cat["duplicate_group"].notna()].sort_values("modified_utc").groupby("duplicate_group").tail(1)
    kept = deduped[deduped["duplicate_group"].notna()]
    assert sorted(kept["result_id"]) == sorted(newest["result_id"])
    renamed = sum(part["name"].nunique() > 1 for _, part in cat[cat["duplicate_group"].notna()].groupby("duplicate_group"))
    print("[OK] %d results, %d in %d duplicate groups (%d with other names); dedupe keeps %d"
          % (len(cat), len(groups), groups.nunique(), renamed, len(deduped)))


def test_load_many_loads_skips_and_warns(database):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        page = database.find(limit=3, sources="discover")
    runs = list(database.load_many(page))
    assert [r.origin["result_id"] for r in runs] == [int(i) for i in page["result_id"]]
    one = database.load_run(page.iloc[0])
    assert runs[0].name == one.name and list(runs[0].chromatograms) == list(one.chromatograms)
    # a result that is gone is skipped with a warning; strict raises
    gone = page.iloc[[0]].copy()
    gone["result_id"] = 2 ** 31 - 1
    with pytest.warns(UserWarning, match="no longer in the database"):
        assert list(database.load_many(pd.concat([gone, page.iloc[[1]]]))) and True
    with pytest.raises(LookupError):
        list(database.load_many(gone, strict=True))
    # changed since the search: loaded as it is now, with a warning
    changed = page.iloc[[0]].copy()
    changed["tstamp"] = "00"
    with pytest.warns(UserWarning, match="changed since the search"):
        assert len(list(database.load_many(changed))) == 1
