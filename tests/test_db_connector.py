"""
Connector settings: UNICORN_PORT reaches the driver (several services can share one host);
where the .env is found, environment variables over the file, the no-settings case.
"""
import os
import warnings

import pytest

from pycorn import db_connector
from pycorn.db_connector import (ENV_FILE_VAR, ConnectionParams, PyCORNDatabase, find_env_file,
                                 get_connection_params, load_env, read_env_file)


@pytest.fixture(autouse=True)
def _no_settings_from_outside(monkeypatch, tmp_path):
    """No UNICORN_* or PYCORN_ENV_FILE from the shell, no repository sql/.env, empty working folder."""
    for key in list(os.environ):
        if key.upper().startswith("UNICORN_") or key.upper() == ENV_FILE_VAR:
            monkeypatch.delenv(key)
    monkeypatch.setattr(db_connector, "DEFAULT_ENV_FILE", tmp_path / "checkout" / "sql" / ".env")
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)


class _FakePymssql:
    """Records the keyword arguments of connect() instead of connecting."""

    def __init__(self):
        self.calls = []

    def connect(self, **kwargs):
        self.calls.append(kwargs)
        return object()


def _env_file(tmp_path, port_line):
    path = tmp_path / ".env"
    lines = ["UNICORN_SERVER=db.example.invalid", "UNICORN_DATABASE=TEST_DB",
             "UNICORN_USER=reader", "UNICORN_PASSWORD=not-a-secret"]
    path.write_text("\n".join(lines + ([port_line] if port_line is not None else [])) + "\n")
    return str(path)


def _connect_kwargs(env_file):
    db = PyCORNDatabase(env_file=env_file)
    db._pymssql = fake = _FakePymssql()
    db._connect()
    return db, fake.calls[0]


def test_port_from_env_file_is_passed_to_driver(tmp_path):
    db, kwargs = _connect_kwargs(_env_file(tmp_path, "UNICORN_PORT=14330"))
    assert db.conn_params.port == 14330
    assert kwargs["port"] == 14330


@pytest.mark.parametrize("port_line", [None, "UNICORN_PORT=", "UNICORN_PORT"])
def test_no_port_uses_driver_default(tmp_path, port_line):
    db, kwargs = _connect_kwargs(_env_file(tmp_path, port_line))
    assert db.conn_params.port is None
    assert "port" not in kwargs


def test_port_from_process_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("UNICORN_SERVER", "db.example.invalid")
    monkeypatch.setenv("UNICORN_PORT", "14331")
    db, kwargs = _connect_kwargs(str(tmp_path / "missing.env"))
    assert kwargs["port"] == 14331


@pytest.mark.parametrize("bad", ["abc", "0", "65536", "-1", "1433.5"])
def test_invalid_port_raises_without_echoing_value(bad):
    with pytest.raises(ValueError) as error:
        ConnectionParams(server="db.example.invalid", database="TEST_DB", port=bad)
    assert bad not in str(error.value)


def test_str_reports_port_as_set_only():
    text = str(ConnectionParams(server="db.example.invalid", database="TEST_DB", port="14330"))
    assert "port=set" in text and "14330" not in text


def test_functional_api_passes_port():
    params = get_connection_params("db.example.invalid", "TEST_DB", "reader", "not-a-secret", port="14330")
    assert params["port"] == 14330
    assert "port" not in get_connection_params("db.example.invalid", "TEST_DB", "reader", "not-a-secret")


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_env_file_lookup_order(tmp_path, monkeypatch):
    checkout = _write(db_connector.DEFAULT_ENV_FILE, "UNICORN_SERVER=checkout\n")
    assert find_env_file() == checkout
    dot_env = _write(tmp_path / "work" / ".env", "UNICORN_SERVER=dot\n")
    assert find_env_file() == dot_env
    sql_env = _write(tmp_path / "work" / "sql" / ".env", "UNICORN_SERVER=sql\n")
    assert find_env_file() == sql_env
    named = _write(tmp_path / "named.env", "UNICORN_SERVER=named\n")
    monkeypatch.setenv(ENV_FILE_VAR, str(named))
    assert find_env_file() == named
    explicit = tmp_path / "explicit.env"
    assert find_env_file(str(explicit)) == explicit  # the argument wins, even before it exists


def test_missing_named_env_file_warns_and_falls_through(tmp_path, monkeypatch):
    sql_env = _write(tmp_path / "work" / "sql" / ".env", "UNICORN_SERVER=sql\n")
    monkeypatch.setenv(ENV_FILE_VAR, str(tmp_path / "missing.env"))
    with pytest.warns(UserWarning, match=ENV_FILE_VAR):
        assert find_env_file() == sql_env


def test_environment_overrides_file_per_key(tmp_path, monkeypatch):
    path = _write(tmp_path / "s.env", "UNICORN_SERVER=db.example.invalid\nUNICORN_DATABASE=FROM_FILE\n"
                                      "UNICORN_USER=reader\n")
    monkeypatch.setenv("UNICORN_DATABASE", "FROM_ENV")
    env = load_env(str(path))
    assert env["UNICORN_DATABASE"] == "FROM_ENV"
    assert env["UNICORN_SERVER"] == "db.example.invalid" and env["UNICORN_USER"] == "reader"
    assert PyCORNDatabase(env_file=str(path)).conn_params.database == "FROM_ENV"


def test_read_env_file_quotes_comments_and_bom(tmp_path):
    path = tmp_path / "q.env"
    path.write_bytes("﻿UNICORN_SERVER=db.example.invalid  # a comment\n"
                     'UNICORN_PASSWORD="not # a comment"  # but this is\n'
                     "UNICORN_USER='reader'\n"
                     "UNICORN_PORT\n".encode("utf-8"))
    assert read_env_file(path) == {"UNICORN_SERVER": "db.example.invalid",
                                   "UNICORN_PASSWORD": "not # a comment",
                                   "UNICORN_USER": "reader", "UNICORN_PORT": ""}


def test_no_settings_and_no_local_server_raises(monkeypatch):
    monkeypatch.setattr(db_connector, "_local_server_answers", lambda port: False)
    with pytest.raises(ValueError, match="UNICORN_SERVER"):
        load_env()


def test_no_settings_uses_local_server_with_warning(monkeypatch):
    ports = []
    monkeypatch.setattr(db_connector, "_local_server_answers", lambda port: ports.append(port) or True)
    monkeypatch.setenv("UNICORN_PORT", "14330")
    with pytest.warns(UserWarning, match="localhost"):
        env = load_env()
    assert env["UNICORN_SERVER"] == "localhost" and ports == [14330]


def test_configured_server_never_probes_localhost(tmp_path, monkeypatch):
    def probe(port):
        raise AssertionError("probed localhost although a server is set")
    monkeypatch.setattr(db_connector, "_local_server_answers", probe)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert load_env(_env_file(tmp_path, None))["UNICORN_SERVER"] == "db.example.invalid"


FOLDER_SUMMARY_COLUMNS = [  # pycorn/sql/unicorn_cte.sql, folderSummary
    "FolderID", "ParentFolderID", "Description", "IsGlobal", "IsHidden", "Created", "CreatedBy",
    "LastModified", "LastModifiedBy", "tstamp", "Level", "FolderPath",
    "ResultCount", "MethodCount", "LatestResultCreated", "LatestResultCreatedBy",
    "LatestMethodCreated", "LatestMethodCreatedBy", "TotalResultCount", "TotalMethodCount",
    "RollupLatestResultCreated", "RollupLatestMethodCreated"]


def test_folder_summary_column_groups():
    result = db_connector.DataFrameResult([], FOLDER_SUMMARY_COLUMNS, groups=db_connector.folder_summary_group)
    assert list(result.tables) == PyCORNDatabase.AVAILABLE_TABLES
    assert sum(map(len, result.tables.values())) == len(FOLDER_SUMMARY_COLUMNS)  # each column once
    assert result.get_table_columns("folder") == FOLDER_SUMMARY_COLUMNS[:12]
    assert result.get_table_columns("results") == ["ResultCount", "LatestResultCreated", "LatestResultCreatedBy",
                                                   "TotalResultCount", "RollupLatestResultCreated"]
    assert result.get_table_columns("methods") == ["MethodCount", "LatestMethodCreated", "LatestMethodCreatedBy",
                                                   "TotalMethodCount", "RollupLatestMethodCreated"]
    with pytest.raises(KeyError, match="folder, results, methods"):
        result.get_table_columns("curve")


def test_custom_query_result_has_no_groups():
    result = db_connector.DataFrameResult([], ["ResultID", "Description"])
    assert result.tables == {}
    with pytest.raises(KeyError, match="none"):
        result.get_table_columns("results")


def test_missing_pymssql_raises_with_install_hint(monkeypatch):
    import sys
    monkeypatch.setitem(sys.modules, "pymssql", None)  # makes `import pymssql` fail
    with pytest.raises(ImportError, match=r"pycorn\[database\]"):
        db_connector._import_pymssql()


def test_connection_error_redacts_port():
    class _Failing:
        def connect(self, **kwargs):
            raise RuntimeError("cannot reach db.example.invalid:14330")

    with pytest.raises(ConnectionError) as error:
        db_connector._connect_safely(_Failing(), server="db.example.invalid", port=14330)
    assert "14330" not in str(error.value) and "db.example.invalid" not in str(error.value)
