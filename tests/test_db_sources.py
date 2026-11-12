"""
Several sources and databases (`pycorn.db_sources`, `PyCORNDatabase` with `sources=`).

Offline tests check the `.env` layout, the search scope, paging across databases, merged
pick lists and the keyed `wide()` pivot. The live tests (skipped without sql/.env or the
server) search every UNICORN database the login can see (`sources="discover"`) and check
paging, counts, pick lists, variables and `load_run(row)` against single-database
searches. Output is pass/fail and counts only: no server or database names.
"""
import warnings

import pandas as pd
import pytest

from pycorn.db_connector import ConnectionParams, PyCORNDatabase, _parse_port
from pycorn.db_sources import SchemaProfile, plan_scope, resolve_databases, sources_from_env
from pycorn.search import CATALOG_COLUMNS, merge_pick_lists, page_across, wide


def _sources(**env):
    return sources_from_env({k.upper(): v for k, v in env.items()}, ConnectionParams, _parse_port)


# ---------------------------------------------------------------------------
# Offline: .env layout
# ---------------------------------------------------------------------------

def test_single_connection_is_the_default_source():
    sources = _sources(unicorn_server="db.example.invalid", unicorn_database="DB_A", unicorn_port="14330")
    assert list(sources) == ["default"]
    assert sources["default"].databases == ["DB_A"]
    assert sources["default"].params.database == "DB_A" and sources["default"].params.port == 14330


def test_database_list_star_and_singular_plus_plural():
    assert _sources(unicorn_server="h", unicorn_databases="*")["default"].databases == "*"
    s = _sources(unicorn_server="h", unicorn_database="DB_A", unicorn_databases="db_a, DB_B,")["default"]
    assert s.databases == ["DB_A", "DB_B"]  # deduplicated, case-insensitive
    assert _sources(unicorn_server="h")["default"].databases is None  # the login's default database


def test_aliases_use_their_own_variables_in_listed_order():
    sources = _sources(unicorn_sources="lab2, default", unicorn_server="h1", unicorn_database="DB_A",
                       unicorn_lab2_server="h2", unicorn_lab2_port="14331", unicorn_lab2_databases="X,Y",
                       unicorn_lab2_user="reader", unicorn_lab2_password="not-a-secret")
    assert list(sources) == ["lab2", "default"]
    lab2 = sources["lab2"]
    assert (lab2.params.server, lab2.params.port, lab2.databases, lab2.params.user) == ("h2", 14331, ["X", "Y"], "reader")
    text = str(lab2)
    assert "lab2" in text and "h2" not in text and "not-a-secret" not in text and "reader" not in text


@pytest.mark.parametrize("env,needle", [
    ({"unicorn_sources": "lab2"}, "UNICORN_LAB2_SERVER"),
    ({"unicorn_sources": "default,lab2", "unicorn_server": "h"}, "UNICORN_LAB2_SERVER"),
    ({"unicorn_sources": "lab-2", "unicorn_server": "h"}, "not a valid source alias"),
    ({"unicorn_sources": "discover", "unicorn_server": "h"}, "not a valid source alias"),
    ({"unicorn_sources": "a,A", "unicorn_a_server": "h"}, "twice"),
    ({"unicorn_sources": "lab2", "unicorn_lab2_server": "h", "unicorn_lab2_port": "99999"}, "UNICORN_LAB2_PORT"),
])
def test_bad_env_names_the_variable_not_the_value(env, needle):
    with pytest.raises(ValueError) as error:
        _sources(**env)
    assert needle in str(error.value) and "99999" not in str(error.value)


def test_env_file_with_sources(tmp_path):
    path = tmp_path / ".env"
    path.write_text("UNICORN_SERVER=h1\nUNICORN_DATABASE=DB_A\nUNICORN_USER=u\nUNICORN_PASSWORD=\n"
                    "UNICORN_SOURCES=default,lab2\nUNICORN_LAB2_SERVER=h2\nUNICORN_LAB2_DATABASES=*\n")
    db = PyCORNDatabase(env_file=str(path))
    assert list(db.sources) == ["default", "lab2"] and db.default_source == "default"
    assert db.conn_params is db.sources["default"].params and db.conn_params.database == "DB_A"


# ---------------------------------------------------------------------------
# Offline: scope
# ---------------------------------------------------------------------------

def _two_sources():
    return _sources(unicorn_sources="default,lab2", unicorn_server="h1", unicorn_database="DB_A",
                    unicorn_lab2_server="h2", unicorn_lab2_databases="X,Y")


def test_plan_scope_narrows_and_widens():
    sources = _two_sources()
    assert plan_scope(sources) == [("default", ["DB_A"]), ("lab2", ["X", "Y"])]
    assert plan_scope(sources, "discover") == [("default", "*"), ("lab2", "*")]
    assert plan_scope(sources, "LAB2") == [("lab2", ["X", "Y"])]
    assert plan_scope(sources, {"lab2": ["Z"], "default": "*"}) == [("lab2", ["Z"]), ("default", "*")]
    assert plan_scope(sources, {"lab2": None}) == [("lab2", ["X", "Y"])]
    with pytest.raises(KeyError):
        plan_scope(sources, "nowhere")
    with pytest.raises(ValueError):
        plan_scope(sources, ["lab2", "lab2"])


def test_resolve_databases_uses_server_spelling_and_marks_missing():
    available = [("DB_A", True), ("master_copy", False), ("db_b", True)]
    assert resolve_databases(available, "*") == [(1, "DB_A"), (2, "db_b")]
    assert resolve_databases(available, ["DB_B", "nope"]) == [(1, "db_b"), (2, None)]


def test_schema_profile_lists_missing_columns():
    profile = SchemaProfile("DB_A", 100, {"Result": {"ResultID", "Description"}})
    assert profile.missing({"Result": ("ResultID", "BatchId"), "Folder": ("FolderID",)}) == \
        ["Result.BatchId", "Folder.FolderID"]


# ---------------------------------------------------------------------------
# Offline: columns another UNICORN version may lack
# ---------------------------------------------------------------------------

def _profile(drop=(), only=None):
    """A profile with every column find_results reads, minus `drop` ('Table.Column'), or `only` these."""
    from pycorn.search import _SELECT, _CRITERIA, _CURVES, _FOLDER_TREE, _RUN_INFO, _UNIT_COLUMN
    needs = set(_CRITERIA + _CURVES + _FOLDER_TREE + _RUN_INFO + _UNIT_COLUMN)
    needs |= {n for _, _, ns in _SELECT for n in ns}
    needs |= {("Folder", "IsHidden"), ("Curve", "IsOriginalData"), ("Chromatogram", "Created"),
              ("ResultSearchCriteria", "ResultSearchCriteriaID"), ("Result", "ResultID")}
    columns = {}
    for table, column in needs:
        name = "%s.%s" % (table, column)
        if (only is None and name not in drop) or (only is not None and name in only):
            columns.setdefault(table, set()).add(column)
    return SchemaProfile("DB_X", 100, columns)


def test_resolve_blocks_keeps_if_or_else_and_nests():
    from pycorn.search import resolve_blocks
    sql = "A /*IF T.x*/B /*IF T.y*/C/*ELSE*/D/*END*/ E/*ELSE*/F/*END*/ G"
    full = SchemaProfile("d", 100, {"T": {"x", "y"}})
    assert resolve_blocks(sql, full) == "A B C E G"
    assert resolve_blocks(sql, SchemaProfile("d", 100, {"T": {"X"}})) == "A B D E G"  # names without case
    assert resolve_blocks(sql, SchemaProfile("d", 100, {})) == "A F G"
    assert resolve_blocks(sql) == "A B C E G"  # no profile: every column is there
    for bad in ("/*IF T.x*/a", "a/*END*/", "/*IF x*/a/*END*/"):
        with pytest.raises(ValueError):
            resolve_blocks(bad, full)


def test_missing_catalog_columns_become_null_and_their_filters_match_nothing():
    from pycorn.search import build_query
    profile = _profile(drop={"Result.SystemTypeName", "Folder.IsHidden"})
    with pytest.warns(UserWarning) as record:
        sql, params, _ = build_query("find", columns=profile, label="lab2#2", system_type="X", system="Y")
    assert "NULL AS [system_type]" in sql and "r.SystemTypeName" not in sql
    assert "r.SystemName AS [system]" in sql and "r.SystemName LIKE" in sql  # what is there still works
    assert "1 = 0" in sql and "hidden_folder" not in sql
    text = " ".join(str(w.message) for w in record)
    assert "lab2#2" in text and "Result.SystemTypeName" in text and "Folder.IsHidden" in text and "DB_X" not in text


def test_run_start_falls_back_to_chromatogram_created():
    from pycorn.search import build_query
    profile = _profile(drop={"ResultRunInformation.RunInformation"})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        sql, _, _ = build_query("find", columns=profile, run_start=("2019-01-01", None))
    assert "ri.RunInformation" not in sql and "CAST(NULL AS nvarchar(max)) AS stored" in sql
    assert "rs.run_start" in sql and "1 = 0" not in sql and "MIN(ch.Created)" in sql


def test_only_result_id_still_gives_a_query():
    from pycorn.search import build_query, empty_catalog_columns
    profile = _profile(only={"Result.ResultID"})
    with pytest.warns(UserWarning):
        sql, _, _ = build_query("find", columns=profile, folder="/Home/**", variables={"Flow": 1.0},
                                has_peak_tables=True, n_chromatograms=1)
    code = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
    for table in ("Folder", "Chromatogram", "PeakTable", "ResultRunInformation", "ResultSearchCriteria"):
        assert "dbo.%s" % table not in code
    assert code.count("1 = 0") == 4
    empty = empty_catalog_columns(profile)
    assert {"name", "folder_path", "run_start_utc", "column", "n_chromatograms", "has_peak_tables",
            "tstamp"} <= set(empty) and "result_id" not in empty and "database" not in empty


def test_variables_and_pick_lists_fall_back():
    from pycorn.search import fetch_variables, pick_list

    class Cursor:
        def __init__(self):
            self.sql, self.description = [], []

        def execute(self, sql, params=None):
            self.sql.append(sql)
            self.description = [(n,) for n in ("value", "unit", "n_results", "n_numeric")]

        def fetchall(self):
            return []

    cursor = Cursor()
    profile = _profile(drop={"ResultSearchCriteria.ExtraDisplayInformation", "ResultSearchCriteria.ResultSearchCriteriaID"})
    fetch_variables(cursor, [1, 2], columns=profile)
    assert "ExtraDisplayInformation" not in cursor.sql[-1] and "ResultSearchCriteriaID" not in cursor.sql[-1]
    pick_list(cursor, "variable", columns=profile)
    assert "ExtraDisplayInformation" not in cursor.sql[-1]
    before = len(cursor.sql)
    with pytest.warns(UserWarning):
        table = fetch_variables(cursor, [1], columns=_profile(drop={"ResultSearchCriteria.KeyWord2"}), label="x#1")
        listed = pick_list(cursor, "system_type", columns=_profile(drop={"Result.SystemTypeName"}), label="x#1")
    assert len(cursor.sql) == before and table.empty and listed.empty  # no query sent


# ---------------------------------------------------------------------------
# Offline: paging across databases
# ---------------------------------------------------------------------------

class _FakeScope:
    def __init__(self, source, database, ids):
        self.source, self.database, self.ids = source, database, ids


def _fetcher(calls):
    def fetch(scope, limit, after_id):
        calls.append((scope.database, limit, after_id))
        ids = [i for i in scope.ids if after_id is None or i > after_id]
        rows = ids[:limit]
        frame = pd.DataFrame({c: [None] * len(rows) for c in CATALOG_COLUMNS})
        frame["source"], frame["database"], frame["result_id"] = scope.source, scope.database, rows
        return frame, len(ids) > limit
    return fetch


def _scopes():
    return [_FakeScope("default", "A", [1, 2, 3]), _FakeScope("default", "B", [1, 2]),
            _FakeScope("lab2", "C", []), _FakeScope("lab2", "D", [5])]


def _walk(limit):
    pages, after = [], None
    while True:
        frame, more = page_across(_scopes(), _fetcher([]), limit, after)
        pages.append(list(zip(frame["database"], frame["result_id"])))
        if not more:
            return pages
        last = frame.iloc[-1]
        after = (last["source"], last["database"], int(last["result_id"]))


def test_page_fills_one_database_at_a_time():
    everything = [("A", 1), ("A", 2), ("A", 3), ("B", 1), ("B", 2), ("D", 5)]
    for limit in (1, 2, 3, 4, 5, 6, 100):
        pages = _walk(limit)
        assert [row for page in pages for row in page] == everything
        assert all(pages) and all(len(p) == limit for p in pages[:-1])


def test_full_page_probes_next_databases_with_one_row():
    calls = []
    frame, more = page_across(_scopes()[:2], _fetcher(calls), 3)
    assert len(frame) == 3 and more and calls == [("A", 3, None), ("B", 1, None)]
    calls = []
    frame, more = page_across(_scopes()[:3], _fetcher(calls), 5)
    assert len(frame) == 5 and not more and calls[-1] == ("C", 1, None)


@pytest.mark.parametrize("limit", [0, -1, 5001])
def test_page_limit_is_checked_before_any_query(limit):
    calls = []
    with pytest.raises(ValueError, match="limit must be 1-5000"):
        page_across(_scopes(), _fetcher(calls), limit)
    assert calls == []


def test_after_continues_in_its_database_or_raises():
    calls = []
    frame, _ = page_across(_scopes(), _fetcher(calls), 2, ("default", "b", 1))  # case-insensitive name
    assert list(frame["result_id"]) == [2, 5] and calls[0] == ("B", 2, 1)
    frame, _ = page_across(_scopes(), _fetcher([]), 2, ("lab2", None, 0))  # None: the source's first database
    assert list(frame["result_id"]) == [5]
    with pytest.raises(ValueError):
        page_across(_scopes(), _fetcher([]), 2, ("default", "Z", 1))


# ---------------------------------------------------------------------------
# Offline: merged pick lists, keyed wide()
# ---------------------------------------------------------------------------

def test_merged_pick_list_sums_per_value_and_folders_by_path():
    a = pd.DataFrame({"folder_id": [10, 11], "value": ["/H/x", "/H/y"], "n_results": [3, 1]})
    b = pd.DataFrame({"folder_id": [99], "value": ["/H/y"], "n_results": [5]})
    merged = merge_pick_lists([("default", "A", a), ("lab2", "B", b)], "folder")
    assert list(merged.columns) == ["value", "n_results"]
    assert merged.values.tolist() == [["/H/y", 6], ["/H/x", 3]]
    per = merge_pick_lists([("default", "A", a), ("lab2", "B", b)], "folder", by_database=True)
    assert list(per.columns[:2]) == ["source", "database"] and len(per) == 3
    with pytest.warns(UserWarning):
        assert len(merge_pick_lists([("default", "A", a), ("lab2", "B", b)], "folder", limit=1)) == 1


def test_merged_variable_pick_list_keeps_units_apart():
    a = pd.DataFrame({"value": ["Flow", "Flow"], "unit": ["ml/min", "cm/h"], "n_results": [2, 1], "n_numeric": [2, 1]})
    b = pd.DataFrame({"value": ["Flow"], "unit": ["ml/min"], "n_results": [4], "n_numeric": [3]})
    merged = merge_pick_lists([("s", "A", a), ("s", "B", b)], "variable")
    assert merged.values.tolist() == [["Flow", "ml/min", 6, 5], ["Flow", "cm/h", 1, 1]]


def test_wide_matches_rows_on_source_database_and_result_id():
    catalog = pd.DataFrame({"source": ["default", "lab2"], "database": ["A", "B"], "result_id": [7, 7]})
    variables = pd.DataFrame({
        "source": ["default", "lab2"], "database": ["A", "B"], "result_id": [7, 7], "kind": ["MethodVariable"] * 2,
        "variable": ["Flow"] * 2, "value": ["1", "2"], "number": [1.0, 2.0], "unit": ["ml/min"] * 2,
        "scouting": [False, False]})
    for c in ("source", "database", "kind", "variable", "unit"):
        variables[c] = variables[c].astype("category")
    out = wide(catalog, variables, kinds=None)
    assert out["Flow [ml/min]"].tolist() == ["1", "2"]  # same ResultID, different databases


# ---------------------------------------------------------------------------
# Offline: a source that cannot connect
# ---------------------------------------------------------------------------

def test_failing_source_is_skipped_with_alias_only_or_raises_when_strict(tmp_path):
    path = tmp_path / ".env"
    path.write_text("UNICORN_SOURCES=lab2\nUNICORN_LAB2_SERVER=db.example.invalid\nUNICORN_LAB2_DATABASES=X\n")
    db = PyCORNDatabase(env_file=str(path))

    def refuse(source=None):
        raise ConnectionError("cannot reach [redacted]")
    db._connect = refuse
    with pytest.warns(UserWarning) as record:
        assert db.count() == 0
        assert len(db.find()) == 0
    text = " ".join(str(w.message) for w in record)
    assert "'lab2'" in text and "db.example.invalid" not in text
    with pytest.raises(ConnectionError):
        db.count(strict=True)


# ---------------------------------------------------------------------------
# Live: every UNICORN database the login can see
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def database():
    from pycorn.db_connector import DEFAULT_ENV_FILE
    if not DEFAULT_ENV_FILE.exists():
        pytest.skip("no sql/.env")
    db = PyCORNDatabase()
    if not db.test_connection(quiet=True):
        pytest.skip("database not reachable")
    return db


@pytest.fixture(scope="module")
def discovered(database):
    names = database.discover_databases()
    print("[INFO] UNICORN databases the login can see: %d" % len(names))
    return names


def test_discover_includes_the_env_database(database, discovered):
    listed = database.sources[database.default_source].databases
    if isinstance(listed, list):
        assert all(n.lower() in {d.lower() for d in discovered} for n in listed)
    profile = database.schema_profile()
    assert profile.compatibility_level and "Result" in profile.columns


def test_counts_add_up_over_databases(database, discovered):
    per = database.count(sources="discover", by_database=True)
    assert len(per) == len(discovered) and list(per["source"].unique()) == [database.default_source]
    assert database.count(sources="discover") == per["n"].sum()
    for _, row in per.iterrows():
        alone = database.count(sources={row["source"]: [row["database"]]})
        assert alone == row["n"]


def test_pages_cover_every_database_in_order(database, discovered):
    if len(discovered) < 2:
        pytest.skip("only one UNICORN database visible")
    per = database.count(sources="discover", by_database=True)
    page_size = 100 if per["n"].iloc[0] % 100 else 99  # so a page spans two databases
    pages = list(database.find_pages(page_size=page_size, sources="discover"))
    catalog = pd.concat(pages, ignore_index=True)
    assert len(catalog) == per["n"].sum()
    assert all(len(p) == page_size for p in pages[:-1])
    assert any(p["database"].nunique() > 1 for p in pages)
    assert not catalog.duplicated(["source", "database", "result_id"]).any()
    # database order as discovered, ResultID order within each
    order = {n.lower(): i for i, n in enumerate(discovered)}
    positions = catalog["database"].str.lower().map(order)
    assert positions.is_monotonic_increasing
    for name, part in catalog.groupby("database", sort=False):
        assert part["result_id"].is_monotonic_increasing
        alone = pd.concat(database.find_pages(page_size=5000, sources={database.default_source: [name]}))
        assert list(alone["result_id"]) == list(part["result_id"])
    print("[OK] %d results in %d pages over %d databases" % (len(catalog), len(pages), len(discovered)))


def test_after_int_is_refused_over_several_databases(database, discovered):
    if len(discovered) < 2:
        pytest.skip("only one UNICORN database visible")
    with pytest.raises(ValueError):
        database.find(after=1, sources="discover")


def test_merged_pick_list_equals_sum_per_database(database, discovered):
    per = database.pick_list("system", sources="discover", by_database=True)
    merged = database.pick_list("system", sources="discover")
    expected = per.groupby("value", dropna=False)["n_results"].sum()
    assert merged.set_index("value")["n_results"].sort_index().to_dict() == expected.sort_index().to_dict()
    assert database.pick_list("curve_role", sources="discover")["value"].tolist()
    variables = database.pick_list("variable", sources="discover")
    # both are counts of results, though one result can store a variable twice (method + scouting)
    assert (variables["n_numeric"] <= variables["n_results"]).all()


def _reduced(profile, drop):
    """The live database's profile without `drop` ('Table.Column'): it acts as an older UNICORN version."""
    columns = {t: {c for c in cs if "%s.%s" % (t, c) not in drop} for t, cs in profile.columns.items()}
    return SchemaProfile(profile.database, profile.compatibility_level, columns)


def test_fallback_sql_runs_on_the_server(database):
    # The SQL written for missing columns must be valid T-SQL: run it on the real database
    # with columns hidden from its profile, and compare with the full search.
    from pycorn import search
    full = database.schema_profile()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        reference = database.find(limit=50)
    conn = database._connect()
    try:
        cursor = conn.cursor()
        drop = {"Result.SystemTypeName", "Folder.IsHidden", "ResultRunInformation.RunInformation",
                "Chromatogram.Created", "PeakTable.ChromatogramID", "ResultSearchCriteria.ExtraDisplayInformation",
                "Curve.IsOriginalData"}
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            page, _ = search.fetch_page(cursor, limit=50, columns=_reduced(full, drop), include_hidden=True)
            assert search.count(cursor, columns=_reduced(full, drop), system_type="*") == 0
            minimal, _ = search.fetch_page(cursor, limit=50, columns=_reduced(full, {
                "%s.%s" % (t, c) for t, cs in full.columns.items() for c in cs} - {"Result.ResultID"}))
            kinds = search.read_curve_kinds(cursor, _reduced(full, drop))
            roles, _ = search.fetch_page(cursor, limit=5, columns=_reduced(full, drop), has_curve_roles=["uv"],
                                         curve_kinds=kinds)
            variables = search.fetch_variables(cursor, page, columns=_reduced(full, drop))
            listed = search.pick_list(cursor, "variable", columns=_reduced(full, drop))
    finally:
        conn.close()
    assert page["system_type"].isna().all() and page["has_peak_tables"].isna().all()
    assert page["run_start_utc"].isna().all() and page["first_chromatogram_utc"].isna().all()
    assert page["column"].isna().all() and page["name"].notna().any()
    # plain values only in these asserts: a failing frame would print its database name
    n_minimal, n_all = len(minimal), int(database.count(include_hidden=True))
    assert n_minimal == min(50, n_all)
    assert list(minimal["result_id"]) == sorted(minimal["result_id"])
    assert bool(minimal.drop(columns=["source", "database", "result_id"]).isna().all().all())
    assert len(roles) > 0 and bool(variables["unit"].isna().all()) and bool(listed["unit"].isna().all())
    # what the full search shows (hidden folders left out) up to the same ResultID is in the reduced page
    last = int(page["result_id"].max())
    assert {i for i in reference["result_id"] if i <= last} <= set(page["result_id"])


def test_variables_and_load_run_follow_each_rows_database(database, discovered):
    if len(discovered) < 2:
        pytest.skip("only one UNICORN database visible")
    per = database.count(sources="discover", by_database=True)
    first = int(per["n"].iloc[0])
    # a page holding the last results of the first database and the first of the next
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        head = database.find(limit=max(first - 3, 1), sources="discover")
        page = database.find(limit=6, after=head, sources="discover")
    assert page["database"].nunique() == 2
    table = database.variables(page)
    assert set(zip(table["source"], table["database"], table["result_id"])) <= \
        set(zip(page["source"], page["database"], page["result_id"]))
    shown = wide(page, table)
    for (source, name), part in page.groupby(["source", "database"], sort=False):
        alone = database.variables(part)
        alone_wide = wide(part, alone)
        common = [c for c in alone_wide.columns if c in shown.columns and c not in CATALOG_COLUMNS]
        mine = shown[(shown["source"] == source) & (shown["database"] == name)]
        pd.testing.assert_frame_equal(mine[common].reset_index(drop=True), alone_wide[common].reset_index(drop=True),
                                      check_dtype=False)
    for _, row in page.groupby("database", sort=False).head(1).iterrows():
        run = database.load_run(row)
        assert list(run.origin)[0] == "source" and run.origin["source"] == row["source"]
        assert run.origin["database"].lower() == row["database"].lower()
        assert run.origin["result_id"] == row["result_id"]
