"""
Track 9 (agentic): the public API (`__all__`, `__version__`), `pycorn --version`,
`pycorn schema [--json]`, and `pycorn sources` / `pycorn-setup --test-only` testing the
settings `pycorn find` reads (every source alias; never a value; no server needed).
"""
import json
import os
from pathlib import Path

import pytest

import pycorn
from pycorn import browser, cli, db_connector, setup_env
from pycorn.db_connector import ENV_FILE_VAR

VALUES = ("good.example.invalid", "bad.example.invalid", "TEST_DB", "reader", "not-a-secret")


# ---------------------------------------------------------------------------
# A1: version and API surface
# ---------------------------------------------------------------------------

def test_every_public_name_imports():
    for name in pycorn.__all__:
        assert getattr(pycorn, name) is not None, name
    namespace = {}
    exec("from pycorn import *", namespace)
    assert set(pycorn.__all__) <= set(namespace)
    for leaked in ("os", "io", "struct", "ZipFile", "ElementTree", "OrderedDict", "xmltodict", "codecs"):
        assert leaked not in namespace


def test_names_existing_code_imports_still_work():
    from pycorn import (PcRes3, PcUni6, Run, open_run, open_folder, catalog_from_files, load_files,  # noqa: F401
                        match_zip_to_db, export_cadet, PyCORNDatabase, execute_cte_query, fetch_results,
                        test_connection, return_on_failure)
    from pycorn.run_export import ExportOptions, write_run
    assert pycorn.write_run is write_run and pycorn.ExportOptions is ExportOptions


def test_version(capsys):
    from pycorn.version import __version__, version_text
    assert pycorn.__version__ == __version__
    with pytest.raises(SystemExit) as stop:
        cli.main(["--version"])
    assert stop.value.code == 0
    assert capsys.readouterr().out.strip() == version_text()


# ---------------------------------------------------------------------------
# A2: pycorn schema
# ---------------------------------------------------------------------------

def test_schema_json_lists_tables_groups_roles_units_exit_codes(capsys):
    from pycorn.curve_roles import ROLES
    from pycorn.export_tables import TABLES
    from pycorn.messages import EXIT_CODES
    from pycorn.run_export import TABLE_GROUPS
    from pycorn.units import UNITS
    assert cli.main(["schema", "--json"]) == 0
    out = capsys.readouterr().out
    out.encode("ascii")
    doc = json.loads(out)
    assert doc["schema"] == 1 and doc["version"] == pycorn.__version__
    assert [t["key"] for t in doc["tables"]] == list(TABLES)
    phases = next(t for t in doc["tables"] if t["key"] == "phases")
    assert phases["group"] == "key" and phases["csv_file"] == "<chromatogram>_phases.csv"
    assert phases["level"] == "chromatogram"
    assert {"column", "origin", "meaning", "check"} <= set(phases["columns"][0])
    assert next(t for t in doc["tables"] if t["key"] == "variables")["level"] == "run"
    curves = next(t for t in doc["tables"] if t["key"] == "curves")
    assert any(c["column"].startswith("<curve>: time") for c in curves["columns"])
    assert [(g["name"], tuple(g["tables"])) for g in doc["groups"]] == [(g, tuple(k)) for g, k in TABLE_GROUPS]
    assert [r["role"] for r in doc["roles"]] == list(ROLES)
    assert {u["unit"] for u in doc["units"]} == set(UNITS)
    assert set(doc["exit_codes"]) == {str(c) for c in EXIT_CODES} >= {"0", "1", "2", "3"}


def test_schema_text_is_ascii(capsys):
    assert cli.main(["schema"]) == 0
    out = capsys.readouterr().out
    out.encode("ascii")
    assert "Exit codes" in out and "Curve roles" in out


def test_page_names_come_from_the_schema():
    from pycorn.export_tables import TABLES
    names = json.loads(browser.names())
    assert list(names) == ["tables", "groups", "origins", "version", "cadet_experimental"]
    assert set(names["tables"]) == set(TABLES)
    assert set(names["tables"]["phases"]) == {"title", "origin", "note", "shown", "freeze_through"}


# ---------------------------------------------------------------------------
# A6: the settings `pycorn find` reads, tested per source; no value printed
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _no_settings_from_outside(monkeypatch, tmp_path):
    """No UNICORN_* or PYCORN_ENV_FILE from the shell, no repository sql/.env, empty working folder."""
    for key in list(os.environ):
        if key.upper().startswith("UNICORN_") or key.upper() == ENV_FILE_VAR:
            monkeypatch.delenv(key)
    monkeypatch.setattr(db_connector, "DEFAULT_ENV_FILE", tmp_path / "checkout" / "sql" / ".env")
    monkeypatch.setattr(db_connector, "_local_server_answers", lambda port: False)
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)


class _FakePymssql:
    """connect() succeeds for good.example.invalid and fails like the driver for any other server."""

    def __init__(self):
        self.servers = []

    def connect(self, **kwargs):
        self.servers.append(kwargs["server"])
        if kwargs["server"] != "good.example.invalid":
            raise RuntimeError("Unable to connect: Adaptive Server is unavailable or does not exist (%s)"
                               % kwargs["server"])

        class _Conn:
            def close(self):
                pass
        return _Conn()


@pytest.fixture
def fake_driver(monkeypatch):
    fake = _FakePymssql()
    monkeypatch.setattr(db_connector, "_import_pymssql", lambda: fake)
    return fake


def _two_sources(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("UNICORN_SOURCES=default,lab2\nUNICORN_SERVER=good.example.invalid\nUNICORN_DATABASE=TEST_DB\n"
                    "UNICORN_USER=reader\nUNICORN_PASSWORD=not-a-secret\n"
                    "UNICORN_LAB2_SERVER=bad.example.invalid\nUNICORN_LAB2_DATABASES=*\n"
                    "UNICORN_LAB2_USER=reader\nUNICORN_LAB2_PASSWORD=not-a-secret\n", encoding="utf-8")
    return path


def _no_values(text):
    for value in VALUES:
        assert value not in text


def test_test_only_tests_every_source_find_reads(fake_driver, capsys):
    _two_sources(Path.cwd() / "sql" / ".env")  # ./sql/.env: what `pycorn find` reads here
    assert setup_env.main(["--test-only"]) == 1
    out = capsys.readouterr().out
    assert "[OK] connected to source 'default'" in out
    assert "[FAIL] could not connect to source 'lab2': cannot reach the SQL Server" in out
    assert "password set" in out
    assert fake_driver.servers == ["good.example.invalid", "bad.example.invalid"]
    _no_values(out)


def test_test_only_with_environment_variables_only(fake_driver, monkeypatch, capsys):
    monkeypatch.setenv("UNICORN_SERVER", "good.example.invalid")
    monkeypatch.setenv("UNICORN_USER", "reader")
    monkeypatch.setenv("UNICORN_PASSWORD", "not-a-secret")
    assert setup_env.main(["--test-only"]) == 0
    out = capsys.readouterr().out
    assert "settings file: none" in out and "UNICORN_PASSWORD" in out and "[OK] connected" in out
    _no_values(out)


def test_test_only_follows_pycorn_env_file(fake_driver, monkeypatch, tmp_path, capsys):
    path = tmp_path / "elsewhere" / "settings.env"
    path.parent.mkdir()
    path.write_text("UNICORN_SERVER=good.example.invalid\n", encoding="utf-8")
    monkeypatch.setenv(ENV_FILE_VAR, str(path))
    assert setup_env.main(["--test-only"]) == 0
    assert setup_env._home_relative(path) in capsys.readouterr().out  # '~\...' when under the home folder


def test_test_only_without_any_settings(fake_driver, capsys):
    assert setup_env.main(["--test-only"]) == 1
    out = capsys.readouterr().out
    assert "[FAIL] the settings could not be read" in out and "pycorn-setup" in out


def test_sources_command_json_and_test(fake_driver, tmp_path, capsys):
    path = _two_sources(tmp_path / "s.env")
    assert cli.main(["--env-file", str(path), "sources", "--json"]) == 0
    out = capsys.readouterr().out
    doc = json.loads(out)
    assert doc["schema"] == 1 and doc["tested"] is False and fake_driver.servers == []
    assert [s["alias"] for s in doc["sources"]] == ["default", "lab2"]
    assert doc["sources"][1]["databases"] == "all UNICORN databases"
    assert doc["sources"][0]["settings"] == {"server": "set", "port": "empty", "database": "set",
                                             "databases": "empty", "user": "set", "password": "set"}
    _no_values(out)
    assert cli.main(["--env-file", str(path), "sources", "--test", "--json"]) == 1
    out = capsys.readouterr().out
    doc = json.loads(out)
    assert [s["connected"] for s in doc["sources"]] == [True, False]
    _no_values(out)


def test_settings_file_under_home_has_no_user_name(monkeypatch, tmp_path):
    home = tmp_path / "home" / "someone"
    path = _two_sources(home / "pycorn" / "s.env")
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    shown = setup_env.source_status(str(path))["settings_file"]
    assert shown == str(Path("~") / "pycorn" / "s.env") and "someone" not in shown
    other = _two_sources(tmp_path / "shared" / "s.env")
    assert setup_env.source_status(str(other))["settings_file"] == str(other)


def test_sources_command_missing_file_is_exit_2(tmp_path, capsys):
    assert cli.main(["--env-file", str(tmp_path / "none.env"), "sources"]) == 2
    assert "not found" in capsys.readouterr().err
