"""
pycorn-setup: answers written to a .env read_env_file() reads back, existing files updated in
place, values never printed, the per-user file found last.
"""
from pathlib import Path

import pytest

from pycorn import db_connector
from pycorn.db_connector import find_env_file, read_env_file
from pycorn.setup_env import format_value, main, run_setup, update_env_text

SECRET = "pa ss #word"


def _setup(path, answers, password, test=False):
    out = []
    answers = iter(answers)
    code = run_setup(path, test=test, ask=lambda q: next(answers), ask_hidden=lambda q: password,
                     say=out.append)
    return code, "\n".join(out)


def test_new_file_round_trips_and_prints_no_values(tmp_path):
    path = tmp_path / "cfg" / ".env"
    code, out = _setup(path, ["db.example.invalid", "", "TEST_DB", "reader"], SECRET)
    assert code == 0
    env = read_env_file(path)
    assert env["UNICORN_SERVER"] == "db.example.invalid"
    assert env["UNICORN_PASSWORD"] == SECRET
    assert "UNICORN_PORT" not in env  # a new file gets no empty optional lines
    assert [line.split("=", 1)[0] for line in path.read_text(encoding="utf-8").splitlines()
            if line and not line.startswith("#")] == ["UNICORN_SERVER", "UNICORN_DATABASE", "UNICORN_USER",
                                                      "UNICORN_PASSWORD"]
    for value in ("db.example.invalid", "TEST_DB", "reader", SECRET):
        assert value not in out
    assert "UNICORN_PASSWORD set" in out and "UNICORN_PORT empty" in out


def test_update_keeps_other_lines_and_current_values(tmp_path):
    path = tmp_path / ".env"
    path.write_text("# mine\nUNICORN_SERVER=old.invalid\nUNICORN_SOURCES=default,lab2\n"
                    "UNICORN_PASSWORD=keep-me\nUNICORN_SERVER=dup.invalid\n", encoding="utf-8")
    code, out = _setup(path, ["new.invalid", "", "", "reader"], "")  # Enter keeps the password
    text = path.read_text(encoding="utf-8")
    env = read_env_file(path)
    assert code == 0 and text.startswith("# mine\n")
    assert env["UNICORN_SERVER"] == "new.invalid" and text.count("UNICORN_SERVER=") == 1
    assert env["UNICORN_SOURCES"] == "default,lab2"
    assert env["UNICORN_PASSWORD"] == "keep-me"
    assert "keep-me" not in out


def test_required_answer_and_bad_port_are_asked_again(tmp_path):
    code, out = _setup(tmp_path / ".env", ["", "db.example.invalid", "99999", "1444", "", "reader"], "x")
    assert code == 0
    assert read_env_file(tmp_path / ".env")["UNICORN_PORT"] == "1444"
    assert "needed" in out and "valid TCP port" in out


@pytest.mark.parametrize("value", ["plain", " lead", 'has "quote" #', "a #b", "'q"])
def test_format_value_round_trips(tmp_path, value):
    path = tmp_path / ".env"
    path.write_text(update_env_text("", {"UNICORN_PASSWORD": value}), encoding="utf-8")
    assert read_env_file(path)["UNICORN_PASSWORD"] == value


def test_format_value_rejects_unstorable():
    with pytest.raises(ValueError):
        format_value(" both ' and \" ")


def test_user_file_is_looked_for_last(tmp_path, monkeypatch):
    monkeypatch.setattr(db_connector, "DEFAULT_ENV_FILE", tmp_path / "checkout" / "sql" / ".env")
    work = tmp_path / "work"
    work.mkdir()
    monkeypatch.chdir(work)
    user = db_connector.user_env_file()
    assert db_connector.env_file_candidates()[-1] == user
    assert find_env_file() is None
    user.parent.mkdir(parents=True)
    user.write_text("UNICORN_SERVER=u.invalid\n", encoding="utf-8")
    assert find_env_file() == user
    (work / ".env").write_text("UNICORN_SERVER=w.invalid\n", encoding="utf-8")
    assert find_env_file() == work / ".env"


def test_test_only_without_file(tmp_path, capsys):
    assert main(["--test-only", "--file", str(tmp_path / "none.env")]) == 1
    assert "run pycorn-setup first" in capsys.readouterr().out


FAKE_ENV = {"UNICORN_SERVER": "YOUR_SERVER_HERE.invalid", "UNICORN_USER": "YOUR_USER_HERE",
            "UNICORN_PASSWORD": "YOUR_PASSWORD_HERE #x", "UNICORN_DATABASE": "YOUR_DATABASE_HERE",
            "UNICORN_PORT": "1444"}


def _set_env(monkeypatch, env):
    for key, value in env.items():
        monkeypatch.setenv(key, value)


def _assert_no_values(text, env):
    for key, value in env.items():
        if key != "UNICORN_PORT":
            assert value not in text
            assert value.split()[0] not in text


def test_from_env_writes_the_user_file_and_prints_names_only(monkeypatch, capsys):
    _set_env(monkeypatch, FAKE_ENV)
    assert main(["--from-env", "--no-test"]) == 0
    path = db_connector.user_env_file()
    env = read_env_file(path)
    for key, value in FAKE_ENV.items():
        assert env[key] == value
    assert "UNICORN_DATABASES" not in env and "UNICORN_SOURCES" not in env
    captured = capsys.readouterr()
    text = captured.out + captured.err
    _assert_no_values(text, FAKE_ENV)
    assert "UNICORN_PASSWORD set" in text
    assert "[FAIL]" not in text and "dated copy" not in text


def test_from_env_new_file_has_no_empty_optional_lines(tmp_path, monkeypatch):
    _set_env(monkeypatch, {k: v for k, v in FAKE_ENV.items() if k in ("UNICORN_SERVER", "UNICORN_USER",
                                                                      "UNICORN_PASSWORD")})
    path = tmp_path / ".env"
    assert main(["--from-env", "--no-test", "--file", str(path)]) == 0
    keys = [line.split("=", 1)[0] for line in path.read_text(encoding="utf-8").splitlines()
            if line and not line.startswith("#")]
    assert keys == ["UNICORN_SERVER", "UNICORN_USER", "UNICORN_PASSWORD"]


@pytest.mark.parametrize("drop", ["UNICORN_SERVER", "UNICORN_USER", "UNICORN_PASSWORD"])
def test_from_env_refuses_without_a_required_setting(monkeypatch, capsys, drop):
    _set_env(monkeypatch, {k: v for k, v in FAKE_ENV.items() if k != drop})
    assert main(["--from-env", "--no-test"]) == 2
    assert not db_connector.user_env_file().exists()
    out = capsys.readouterr().out
    assert drop in out and "nothing was written" in out
    _assert_no_values(out, FAKE_ENV)


def test_from_env_bad_port_writes_nothing(monkeypatch, capsys):
    _set_env(monkeypatch, dict(FAKE_ENV, UNICORN_PORT="99999"))
    assert main(["--from-env", "--no-test"]) == 2
    assert not db_connector.user_env_file().exists()
    assert "valid TCP port" in capsys.readouterr().out


def test_from_env_existing_file_needs_overwrite(tmp_path, monkeypatch, capsys):
    path = tmp_path / ".env"
    path.write_text("# mine\nUNICORN_SERVER=old.invalid\nUNICORN_PORT=1500\nUNICORN_SOURCES=default,lab2\n",
                    encoding="utf-8")
    before = path.read_text(encoding="utf-8")
    env = {k: v for k, v in FAKE_ENV.items() if k != "UNICORN_PORT"}
    _set_env(monkeypatch, env)
    assert main(["--from-env", "--no-test", "--file", str(path)]) == 2
    assert path.read_text(encoding="utf-8") == before
    assert "--overwrite" in capsys.readouterr().out
    assert not list(tmp_path.glob("*.bak"))
    assert main(["--from-env", "--no-test", "--overwrite", "--file", str(path)]) == 0
    text = path.read_text(encoding="utf-8")
    written = read_env_file(path)
    assert text.startswith("# mine\n")
    assert written["UNICORN_SERVER"] == env["UNICORN_SERVER"]
    assert written["UNICORN_PORT"] == "" and written["UNICORN_SOURCES"] == ""
    assert "UNICORN_DATABASES" not in written  # cleared only where the file had the key
    copies = list(tmp_path.glob(".env.*.bak"))
    assert len(copies) == 1 and copies[0].read_text(encoding="utf-8") == before
    out = capsys.readouterr().out
    assert "Kept a dated copy" in out and copies[0].name in out
    _assert_no_values(out, env)
    assert "old.invalid" not in out


def test_dated_copy_name_counter_and_permissions(tmp_path):
    import datetime
    import os
    import re
    import subprocess
    from pycorn.setup_env import keep_dated_copy
    path = tmp_path / ".env"
    path.write_text("UNICORN_PASSWORD=YOUR_PASSWORD_HERE\n", encoding="utf-8")
    when = datetime.datetime(2026, 11, 11, 14, 30, 5)
    first, second = keep_dated_copy(path, now=when), keep_dated_copy(path, now=when)
    assert first.name == ".env.2026-11-11_143005.bak"
    assert second.name == ".env.2026-11-11_143005-1.bak"
    assert second.read_bytes() == path.read_bytes()
    assert re.fullmatch(r"\.env\.\d{4}-\d\d-\d\d_\d{6}\.bak", keep_dated_copy(path).name)
    if os.name != "nt":
        assert (first.stat().st_mode & 0o777) == 0o600
    # *.bak stays out of git: the name must match .gitignore
    ignored = subprocess.run(["git", "check-ignore", "-q", "--no-index", "sql/.env.2026-11-11_143005-1.bak"],
                             cwd=str(Path(__file__).parent.parent), capture_output=True)
    if ignored.returncode in (0, 1):  # 128: no git here
        assert ignored.returncode == 0


@pytest.mark.skipif(__import__("os").name == "nt", reason="file modes are POSIX only")
def test_dated_copy_is_user_only_from_setup(tmp_path, monkeypatch):
    path = tmp_path / ".env"
    path.write_text("UNICORN_SERVER=old.invalid\n", encoding="utf-8")
    path.chmod(0o644)
    _set_env(monkeypatch, FAKE_ENV)
    assert main(["--from-env", "--no-test", "--overwrite", "--file", str(path)]) == 0
    (copy,) = tmp_path.glob(".env.*.bak")
    assert (copy.stat().st_mode & 0o777) == 0o600 and (path.stat().st_mode & 0o777) == 0o600


def test_interactive_update_keeps_dated_copy(tmp_path):
    path = tmp_path / ".env"
    path.write_text("UNICORN_SERVER=old.invalid\nUNICORN_PASSWORD=keep-me\n", encoding="utf-8")
    code, out = _setup(path, ["new.invalid", "", "", "reader"], "")
    (copy,) = tmp_path.glob(".env.*.bak")
    assert code == 0 and read_env_file(copy)["UNICORN_SERVER"] == "old.invalid"
    assert read_env_file(path)["UNICORN_SERVER"] == "new.invalid"
    assert "Kept a dated copy" in out and "keep-me" not in out and "old.invalid" not in out


@pytest.mark.parametrize("from_env", [True, False])
def test_many_dated_copies_warn_from_six(tmp_path, monkeypatch, capsys, from_env):
    path = tmp_path / ".env"
    path.write_text("UNICORN_SERVER=old.invalid\nUNICORN_USER=old-user\nUNICORN_PASSWORD=keep-me\n",
                    encoding="utf-8")
    (tmp_path / ".env.unrelated.bak").write_text("x", encoding="utf-8")  # not a dated copy: not counted
    _set_env(monkeypatch, FAKE_ENV)
    outs = []
    for _ in range(6):
        if from_env:
            assert main(["--from-env", "--no-test", "--overwrite", "--file", str(path)]) == 0
            outs.append(capsys.readouterr().out)
        else:
            code, out = _setup(path, ["", "", "", ""], "")
            assert code == 0
            outs.append(out)
    assert len(list(tmp_path.glob(".env.20*.bak"))) == 6
    assert all("warning: " not in out or "dated copies" not in out for out in outs[:5])
    assert "warning: 6 dated copies of this settings file are kept in" in outs[5]
    assert ".env.20" not in outs[5].split("warning: 6 dated copies")[1]
    for out in outs:
        _assert_no_values(out, FAKE_ENV)
        assert "keep-me" not in out and "old.invalid" not in out and "old-user" not in out


def test_new_file_keeps_no_copy(tmp_path):
    code, out = _setup(tmp_path / ".env", ["db.example.invalid", "", "", "reader"], "x")
    assert code == 0 and not list(tmp_path.glob("*.bak")) and "dated copy" not in out


def test_copy_failure_writes_nothing(tmp_path, monkeypatch, capsys):
    from pycorn import setup_env

    def fail(path, now=None):
        raise PermissionError(13, "Permission denied", str(path))

    monkeypatch.setattr(setup_env, "keep_dated_copy", fail)
    path = tmp_path / ".env"
    path.write_text("UNICORN_SERVER=old.invalid\n", encoding="utf-8")
    before = path.read_text(encoding="utf-8")
    _set_env(monkeypatch, FAKE_ENV)
    assert main(["--from-env", "--no-test", "--overwrite", "--file", str(path)]) == 2
    out = capsys.readouterr().out
    assert path.read_text(encoding="utf-8") == before
    assert "[FAIL] could not keep a dated copy" in out and "nothing was written" in out
    _assert_no_values(out, FAKE_ENV)
    code, out = _setup(path, ["new.invalid", "", "", "reader"], "x")
    assert code == 2 and path.read_text(encoding="utf-8") == before


def test_from_env_writes_every_listed_source(tmp_path, monkeypatch):
    env = {"UNICORN_SOURCES": "default,lab2", "UNICORN_LAB2_SERVER": "YOUR_LAB2_SERVER.invalid",
           "UNICORN_LAB2_DATABASES": "*", "UNICORN_LAB2_USER": "YOUR_LAB2_USER",
           "UNICORN_LAB2_PASSWORD": "YOUR_LAB2_PASSWORD", **FAKE_ENV}
    _set_env(monkeypatch, env)
    path = tmp_path / ".env"
    assert main(["--from-env", "--no-test", "--file", str(path)]) == 0
    written = read_env_file(path)
    for key, value in env.items():
        assert written[key] == value
    monkeypatch.delenv("UNICORN_LAB2_PASSWORD")
    out = []
    from pycorn.setup_env import run_from_env
    assert run_from_env(tmp_path / "other.env", test=False, say=out.append) == 2
    assert "UNICORN_LAB2_PASSWORD" in "\n".join(out) and not (tmp_path / "other.env").exists()


def test_from_env_flag_rules(monkeypatch):
    with pytest.raises(SystemExit) as e:
        main(["--overwrite"])
    assert e.value.code == 2
    with pytest.raises(SystemExit) as e:
        main(["--from-env", "--test-only"])
    assert e.value.code == 2


def test_from_env_connection_test_reports_without_values(tmp_path, monkeypatch, capsys):
    class Broken:
        def connect(self, **kwargs):
            raise RuntimeError("login failed for %s at %s" % (kwargs.get("user"), kwargs.get("server")))

    monkeypatch.setattr(db_connector, "_import_pymssql", lambda: Broken())
    _set_env(monkeypatch, FAKE_ENV)
    assert main(["--from-env", "--file", str(tmp_path / ".env")]) == 1
    captured = capsys.readouterr()
    text = captured.out + captured.err
    assert "[FAIL] could not connect" in text
    _assert_no_values(text, FAKE_ENV)


def test_from_env_connection_test_ok(tmp_path, monkeypatch, capsys):
    class Connection:
        def close(self):
            pass

    class Working:
        def connect(self, **kwargs):
            return Connection()

    monkeypatch.setattr(db_connector, "_import_pymssql", lambda: Working())
    _set_env(monkeypatch, FAKE_ENV)
    assert main(["--from-env", "--file", str(tmp_path / ".env")]) == 0
    text = capsys.readouterr().out
    assert "[OK] connected to source 'default'" in text
    _assert_no_values(text, FAKE_ENV)


def test_connection_failure_is_reported_without_values(tmp_path, monkeypatch):
    class Broken:
        def connect(self, **kwargs):
            raise RuntimeError("login failed for reader at db.example.invalid")

    monkeypatch.setattr(db_connector, "_import_pymssql", lambda: Broken())
    code, out = _setup(tmp_path / ".env", ["db.example.invalid", "", "", "reader"], SECRET, test=True)
    assert code == 1 and "[FAIL] could not connect" in out
    for value in ("db.example.invalid", "reader", SECRET):
        assert value not in out
