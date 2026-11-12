"""
Run search (`pycorn.search`, `PyCORNDatabase.find`).

Offline tests check the SQL building (wildcards, run start, folders, variables, curve roles)
and the wide pivot. The live test (skipped without sql/.env or the server) finds every
local export in the database and checks each catalog field and filter against the run
read from the file; it reports only pass/fail and counts (no names, no connection values).
"""
import datetime as dt
import glob
import math
import os
import warnings

import pandas as pd
import pytest

from pycorn import open_folder, open_run
from pycorn.column_geometry import used_columns
from pycorn.curve_roles import ROLES
from pycorn.search import (CATALOG_COLUMNS, _Params, _role_condition, build_query, like_pattern, wide)
from pycorn.timeline import _run_start_date

TEST_FILES = os.path.join("..", "unicorn_test_files")


# ---------------------------------------------------------------------------
# Offline: SQL building
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    ("Run*", "Run%"), ("?un", "_un"), ("100%", "100\\%"), ("a_b", "a\\_b"), ("[x]", "\\[x]"), ("c:\\d", "c:\\\\d"),
])
def test_like_pattern_escapes_and_translates_wildcards(text, expected):
    assert like_pattern(text) == expected


def _code(sql):
    """The SQL without its comment lines."""
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def _query(**filters):
    sql, params, notes = build_query("find", **filters)
    return _code(sql), params, notes


def test_no_filter_leaves_only_hidden_folders_out():
    sql, params, _ = _query()
    assert "hidden_folder" in sql and "folder_hit" not in sql
    assert "TOP (%(p0)s)" in sql and params["p0"] == 501
    assert "ORDER BY r.ResultID" in sql
    sql, _, _ = _query(include_hidden=True)
    assert "hidden_folder" not in sql and "WITH" not in sql


def test_values_are_parameters_never_inlined():
    sql, params, _ = _query(name="x'; DROP TABLE dbo.Result --", system=["A*", "B"])
    assert "DROP" not in sql and "x'; DROP TABLE dbo.Result --" in params.values()
    assert "A%" in params.values() and "B" in params.values()


def test_limit_and_paging():
    sql, params, _ = _query(limit=10, after=99)
    assert 11 in params.values() and 99 in params.values() and "r.ResultID >" in sql
    with pytest.raises(ValueError):
        _query(limit=0)
    with pytest.raises(ValueError):
        _query(limit=100000)
    sql = _code(build_query("count")[0])
    assert "COUNT(*) AS n" in sql and "ORDER BY r.ResultID" not in sql and "TOP (" not in sql


def test_folder_path_follows_matching_parts_only():
    sql, params, _ = _query(folder="/Home/Pro*/X/**")
    assert ["Home", "Pro%", "X"] == [v for v in params.values() if isinstance(v, str)][:3]
    assert "m.depth < 3" in sql and "JOIN folder_hit h" in sql  # subtree after '**'
    sql, _, _ = _query(folder="Home")
    assert "JOIN folder_hit h" not in sql and "folder_match m" not in sql
    for bad in ("", "/", "/a/**/b", "/a//b"):
        with pytest.raises(ValueError):
            _query(folder=bad)


def test_run_start_local_and_utc():
    sql, params, _ = _query(run_start=("2019-01-01", "2019-06-30"))
    values = [v for v in params.values() if isinstance(v, str)]
    assert "2019-01-01T00:00:00.000" in values and "2019-07-01T00:00:00.000" in values  # end day included
    assert "CONVERT(datetime2, rs.run_start) >=" in sql and "CONVERT(datetime2, rs.run_start) <" in sql
    sql, params, _ = _query(run_start=(dt.datetime(2019, 1, 1, 12, tzinfo=dt.timezone(dt.timedelta(hours=-5))), None))
    assert "2019-01-01T17:00:00.000+00:00" in params.values() and "rs.run_start >= CONVERT(datetimeoffset" in sql
    sql, params, _ = _query(run_start=(None, "2019-06-30T14:00:00"))
    assert "2019-06-30T14:00:00.000" in params.values() and "<=" in sql
    with pytest.raises(ValueError):
        _query(run_start="2019")
    with pytest.raises(ValueError):
        _query(run_start=("June", None))


def test_variable_specs():
    sql, params, _ = _query(variables={"Flow rate": (">=", 1.0), "Position": "Pos*", "Sample_ID": None, "UV1": 280})
    assert sql.count("EXISTS (SELECT 1 FROM dbo.ResultSearchCriteria") == 4
    assert "Pos%" in params.values() and 280.0 in params.values()
    sql, _, _ = _query(variables={"Flow rate": [(">=", 1.0), ("<", 2.0)]})
    assert sql.count("dbo.ResultSearchCriteria") == 2
    sql, _, _ = _query(variables={"Pos": ("!=", "A*")})
    assert "NOT LIKE" in sql
    for bad in ({"x": ("~", 1)}, {"x": True}, {"x": ("<", "a")}, {"x": (">", 1, "furlongs")}, {"x": object()}):
        with pytest.raises(ValueError):
            _query(variables=bad)


def test_unit_filter_converts_the_value_into_every_stored_unit():
    sql, params, notes = _query(variables={"Flow rate": (">=", 1.0, "ml/min")})
    pairs = {}
    names = list(params)
    for i, key in enumerate(names):
        if params[key] in ("ml/min", "L/h", "l/min", "ul/min"):
            pairs[params[key]] = params[names[i - 1]]  # the threshold is added just before its unit
    assert pairs["ml/min"] == 1.0 and pairs["L/h"] == pytest.approx(0.06) and pairs["l/min"] == pytest.approx(0.001)
    assert pairs["ul/min"] == pytest.approx(1000.0)
    assert notes and notes[0][0] == "Flow rate" and "L/h" in notes[0][2] and "MPa" not in notes[0][2]
    assert "COLLATE Latin1_General_BIN2" in sql  # 'M' and 'm' are different units


def test_curve_role_from_stored_combinations():
    kinds = [("UV", "mAU", "UV 1_280"), ("UV", "mAU", "UV 2_260"), ("Other", "%", "Conc B"),
             ("Other", "%", "Conc Q1"), ("Conduction", "%", "% Cond"), ("Conduction", "mS/cm", "Cond")]
    p = _Params()
    uv = _role_condition("uv", kinds, p)
    assert "c.Description" not in uv  # every UV/mAU curve is UV: no name list
    p = _Params()
    conc_b = _role_condition("conc_b", kinds, p)
    assert "c.Description IN" in conc_b and "Conc B" in p.values() and "Conc Q1" not in p.values()
    p = _Params()
    cond = _role_condition("conductivity", kinds, p)
    assert "mS/cm" in p.values() and "%" not in p.values()
    assert _role_condition("ph", kinds, _Params()) is None
    sql, _, _ = _query(has_curve_roles=["ph"], curve_kinds=kinds)
    assert "1 = 0" in sql
    with pytest.raises(KeyError):
        _role_condition("nope", kinds, _Params())
    with pytest.raises(ValueError):
        _query(has_curve_roles=["uv"])  # needs curve_kinds


def test_column_filter_matches_the_stored_xml_name():
    _, params, _ = _query(column="A&B*")
    assert "%<name>A&amp;B%</name>%" in params.values()


def test_wide_pivot():
    cat = pd.DataFrame({"result_id": [1, 2, 3], "name": ["a", "b", "c"]})
    v = pd.DataFrame({
        "result_id": [1, 1, 2, 2, 2, 3],
        "kind": ["VariableValue", "VariableValue", "VariableValue", "ScoutingVariableValue", "QuestionAnswer",
                 "VariableValue"],
        "variable": ["Flow", "Pos", "Flow", "Pos", "Q", "Flow"],
        "value": ["1.0", "P1", "0.060", "P2", "yes", "7"],
        "number": [1.0, math.nan, 0.06, math.nan, math.nan, 7.0],
        "unit": ["ml/min", "", "ml/min", "", None, "L/h"],
        "scouting": [False, False, False, True, False, False],
    })
    w = wide(cat, v)
    assert list(w.columns) == ["result_id", "name", "Flow", "Flow (number)", "Flow (unit)", "Pos", "Pos (number)"]
    assert list(w["Flow"]) == ["1.0", "0.060", "7"]  # text kept as stored
    assert list(w["Flow (unit)"]) == ["ml/min", "ml/min", "L/h"]
    assert w["Pos"].tolist()[:2] == ["P1", "P2"] and pd.isna(w["Pos"].iloc[2])
    one_unit = wide(cat, v[v["result_id"] < 3])
    assert "Flow [ml/min]" in one_unit.columns and "Flow [ml/min] (number)" in one_unit.columns
    assert list(wide(cat, v.iloc[0:0]).columns) == ["result_id", "name"]


# ---------------------------------------------------------------------------
# Live: every local export, found and checked in the database
# ---------------------------------------------------------------------------

def _local_runs():
    runs = [open_run(p) for p in sorted(glob.glob(os.path.join(TEST_FILES, "U7.*", "*.zip")))]
    for path in sorted(glob.glob(os.path.join(TEST_FILES, "UFOL", "**", "*.UFol"), recursive=True)):
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


def _roles(run):
    return {role for role in ROLES for chrom in run.chromatograms.values() if chrom.curves_with_role(role)}


def test_catalog_and_filters_match_the_exports(database):
    runs = _local_runs()
    if not runs:
        pytest.skip("no exports in unicorn_test_files/")
    failures, found = [], 0
    for index, z in enumerate(runs):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cat = database.find(name=z.name.replace("[", "?"), system=z.result.get("SystemName"),
                                run_index=int(z.result.get("RunIndex")))
        if len(cat) != 1:
            failures.append("run %d: %d catalog rows" % (index, len(cat)))
            continue
        found += 1
        row = cat.iloc[0]
        rid = int(row["result_id"])
        bad = []
        folder = z.result.get("FolderPath")
        if row["folder_path"] != folder:
            # an export taken elsewhere and imported into another folder: the database's own path
            folder = database.load_run(rid).result.get("FolderPath")
            if row["folder_path"] != folder:
                bad.append("folder_path")
        if row["n_chromatograms"] != len(z.chromatograms):
            bad.append("n_chromatograms")
        names = [n for n, _, _ in used_columns(z)]
        if (row["column"] if row["column"] is not None else None) != (names[0] if names else None):
            bad.append("column")
        start = _run_start_date(z)
        if start is not None and (row["run_start_utc"].to_pydatetime() != start[2]
                                  or row["run_start_local"].to_pydatetime() != start[0]):
            bad.append("run_start")
        if bool(row["has_peak_tables"]) != any(c.peak_tables for c in z.chromatograms.values()):
            bad.append("has_peak_tables")
        # the variables of the page equal the run's own
        v = database.variables(cat)
        mine = z.variables()
        if (list(v["kind"].astype(object)) != list(mine["kind"]) or list(v["variable"].astype(object)) != list(mine["variable"])
                or list(v["value"]) != list(mine["value"])
                or not all((a == b) or (math.isnan(a) and math.isnan(b)) for a, b in zip(v["number"], mine["number"]))):
            bad.append("variables")
        # each filter, given this run's own values, finds it; a contradicting one doesn't
        checks = {
            "folder": dict(folder=folder.replace("[", "?")),
            "folder subtree": dict(folder="/" + folder.strip("/").split("/")[0] + "/**"),
            "state": dict(state=z.result.get("ResultState")),
            "run_type": dict(run_type=z.result.get("RunType")),
            "has_peak_tables": dict(has_peak_tables=bool(row["has_peak_tables"])),
            "n_chromatograms": dict(n_chromatograms=len(z.chromatograms)),
        }
        if start is not None:
            day = start[0].date()
            checks["run_start local day"] = dict(run_start=(day.isoformat(), day.isoformat()))
            checks["run_start utc"] = dict(run_start=(start[2] - dt.timedelta(seconds=1), start[2] + dt.timedelta(seconds=1)))
        if names:
            checks["column"] = dict(column=names[0].replace("[", "?"))
        numeric = mine[(mine["kind"] == "VariableValue") & mine["number"].notna()]
        if len(numeric):
            first = numeric.iloc[0]
            checks["variable =="] = dict(variables={first["variable"]: float(first["number"])})
            checks["variable range"] = dict(variables={first["variable"]: [(">=", first["number"]), ("<=", first["number"])]})
            if first["unit"]:
                try:
                    checks["variable unit"] = dict(variables={first["variable"]: ("==", float(first["number"]), first["unit"])})
                except Exception:
                    pass
        text = mine[(mine["kind"].isin(["VariableValue", "ScoutingVariableValue"])) & mine["number"].isna()
                    & mine["value"].notna() & (mine["value"] != "")]
        if len(text):
            checks["variable text"] = dict(variables={text.iloc[0]["variable"]: text.iloc[0]["value"].replace("[", "?")})
        roles = _roles(z)
        if roles:
            checks["roles present"] = dict(has_curve_roles=sorted(roles))
        for label, f in checks.items():
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                if database.count(result_ids=[rid], **f) != 1:
                    bad.append("filter " + label)
        for role in set(ROLES) - roles:
            if database.count(result_ids=[rid], has_curve_roles=[role]) != 0:
                bad.append("absent role " + role)
        if database.count(result_ids=[rid], has_peak_tables=not bool(row["has_peak_tables"])) != 0:
            bad.append("filter has_peak_tables negated")
        failures += ["run %d: %s" % (index, b) for b in bad]
    assert found, "no export found in the database"
    assert not failures, "\n".join(failures)


def test_paging_and_pick_lists(database):
    total = database.count()
    pages = list(database.find_pages(page_size=7))
    ids = [i for p in pages for i in p["result_id"]]
    assert len(ids) == total == len(set(ids)) and ids == sorted(ids)
    assert all(list(p.columns) == CATALOG_COLUMNS for p in pages)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        first = database.find(limit=3)
    assert len(first) == min(3, total) and (total <= 3 or any("more than 3" in str(w.message) for w in caught))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        following = database.find(limit=3, after=first)
    assert following.empty or following["result_id"].min() > first["result_id"].max()
    for field in ("folder", "system", "system_type", "run_type", "state", "created_by", "column", "variable"):
        frame = database.pick_list(field)
        assert len(frame) and "n_results" in frame.columns, field
    folders = database.pick_list("folder")
    assert folders["n_results"].sum() == total and folders["value"].str.startswith("/").all()
    for path, n in zip(folders["value"].head(5), folders["n_results"].head(5)):
        assert database.count(folder=path.replace("[", "?"), include_hidden=True) >= n
    roles = database.pick_list("curve_role")["value"].tolist()
    assert "uv" in roles
    name = database.pick_list("variable")["value"].iloc[0]
    values = database.pick_list("variable_value", variable=name)
    assert values["n_results"].sum() >= 1
