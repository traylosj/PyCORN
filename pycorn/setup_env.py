# -*- coding: utf-8 -*-
"""
`pycorn-setup`: write the database settings (.env) by answering a few questions.

For people who do not want to copy and edit sql/.env.example by hand. The file goes to the
per-user place (`db_connector.user_env_file()`: %APPDATA%\\pycorn\\.env on Windows,
~/.config/pycorn/.env elsewhere), which PyCORNDatabase() finds from any working folder, or
to `--file`. Then the connection is tested.

- The password is typed hidden and never printed; no value is ever printed, only whether a
  setting is set or empty (AGENTS.md).
- An existing file is updated in place: Enter keeps the current value, other lines
  (comments, UNICORN_SOURCES, ...) stay as they are.
- Warns, by name only, when another .env or UNICORN_* environment variables win over the
  file written.
- `--test-only` asks nothing: it tests what `pycorn find` reads (`source_status()`: the first
  .env found, UNICORN_* environment variables, every source alias), [OK] / [FAIL] per alias.
  `pycorn sources [--test] [--json]` shows the same. There is no password flag or stdin mode
  (agents use `--env-file` / PYCORN_ENV_FILE on a file a person or `--from-env` wrote).
- `--from-env` asks nothing either: it writes the file from the UNICORN_* environment
  variables an admin or deployment script set (no password on a command line), prints names
  with set / empty only, and refuses (exit 2, nothing written) when the server, user or
  password is empty, or when the file is there without `--overwrite`.
- Before an existing file is changed (either way), a dated copy is kept next to it:
  `<name>.<YYYY-MM-DD_HHMMSS>.bak` (exit 2, nothing written, when it cannot be). Copies are
  never removed; past 5 of them one warning says to remove old ones. A new file gets the
  optional settings that have a value only.
"""
import argparse
import getpass
import os
import sys
from collections import OrderedDict
from pathlib import Path

from . import db_connector
from .db_connector import (UNICORN_DATABASE_VAR, UNICORN_PASSWORD_VAR, UNICORN_PORT_VAR,
                           UNICORN_SERVER_VAR, UNICORN_USER_VAR, _parse_port, find_env_file,
                           read_env_file)

# (key, question, hidden, required)
FIELDS = (
    (UNICORN_SERVER_VAR, "Database server (name or IP address of the UNICORN SQL Server)", False, True),
    (UNICORN_PORT_VAR, "Port (Enter for the default, 1433)", False, False),
    (UNICORN_DATABASE_VAR, "Database name (Enter for the login's default database)", False, False),
    (UNICORN_USER_VAR, "SQL Server login name", False, True),
    (UNICORN_PASSWORD_VAR, "Password (not shown while typing)", True, True),
)

HEADER = ("# UNICORN database settings, written by pycorn-setup.\n"
          "# Keep this file private. Run pycorn-setup again to change it;\n"
          "# sql/.env.example in the PyCORN source lists every setting.\n")


def format_value(value: str) -> str:
    """The value as read_env_file() reads it back: plain, or in quotes when needed."""
    if "\n" in value or "\r" in value:
        raise ValueError("a value holding a line break cannot be stored in a .env")
    if value == value.strip() and " #" not in value and value[:1] not in ('"', "'"):
        return value
    for quote in ('"', "'"):
        if quote not in value:
            return quote + value + quote
    raise ValueError("a value holding both kinds of quote and ' #' or outer spaces cannot be stored in a .env")


def update_env_text(text: str, values: dict) -> str:
    """`text` with each key of `values` set: its line replaced in place, else appended."""
    lines, done = [], set()
    for line in text.splitlines():
        stripped = line.strip()
        key = stripped.split("=", 1)[0].strip().upper().replace("-", "_")
        if stripped and not stripped.startswith("#") and key in values:
            if key in done:
                continue  # a repeated key: read_env_file() would take the last; keep one
            line = "%s=%s" % (key, format_value(values[key]))
            done.add(key)
        lines.append(line)
    lines += ["%s=%s" % (k, format_value(v)) for k, v in values.items() if k not in done]
    return "\n".join(lines) + "\n"


def _ask(key, question, hidden, required, current, ask, ask_hidden, say):
    """One answer; Enter keeps `current` (or leaves an optional setting empty)."""
    keep = " [Enter keeps the current value]" if current else ""
    while True:
        answer = (ask_hidden if hidden else ask)("%s%s: " % (question, keep)).strip()
        if not answer:
            if current or not required:
                return current
            say("  This one is needed.")
            continue
        if key == UNICORN_PORT_VAR:
            try:
                _parse_port(answer)
            except ValueError as e:
                say("  %s" % e)
                continue
        return answer


SETTING_KEYS = ("SERVER", "PORT", "DATABASE", "DATABASES", "USER", "PASSWORD")
GENERAL_ADVICE = "check the server, port, login and password, and that this PC can reach the server"


def _connect_error(e) -> str:
    """A sentence for a failed connect, holding no connection value."""
    if isinstance(e, ConnectionError):  # from _connect_safely: already redacted
        text = str(e)
        return text if text != getattr(e, "detail", None) else "%s (%s)" % (text, GENERAL_ADVICE)
    return "%s (%s)" % (type(e).__name__, GENERAL_ADVICE)


def _home_relative(path) -> str:
    """The path with the home folder as '~' (owner, 2026-11-11: the user name stays out of output
    that may go to a remote model); other paths as they are."""
    path = Path(path)
    try:
        return str(Path("~") / path.resolve().relative_to(Path.home().resolve()))
    except ValueError:
        return str(path)


def source_status(env_file=None, test=False) -> dict:
    """
    The database settings `pycorn find` / `PyCORNDatabase(env_file)` would use, never a value
    (Track 9, A6): {'schema': 1, 'settings_file': the .env read (find_env_file(); under the
    home folder shown as '~/...', so no user name) or None,
    'environment': names of the UNICORN_* environment variables (they override the file),
    'warnings': [text], 'tested': test, 'sources': [{'alias', 'settings': {server, port,
    database, databases, user, password: 'set' / 'empty'}, 'databases': 'login default' /
    'N listed' / 'all UNICORN databases', 'connected': True / False / None (not tested),
    'error': sentence or ''}]}, one per alias in `UNICORN_SOURCES` order.
    With `test`, each source is connected to once (its first listed database, else the
    login's default). Raises FileNotFoundError for an `env_file` that is not there, ValueError
    for settings that cannot be read (no server, a bad alias or port), ImportError without pymssql.
    """
    import warnings
    from .db_connector import ConnectionParams, load_env
    from .db_sources import ALL_DATABASES, env_prefix, sources_from_env
    from .messages import plain_error
    if env_file and not Path(env_file).is_file():
        raise FileNotFoundError(2, "settings file not found", str(env_file))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        path = find_env_file(env_file)
        try:
            env = load_env(env_file)
            sources = sources_from_env(env, ConnectionParams, _parse_port)
        except ValueError as e:
            raise ValueError("database settings: %s" % e) from None
        db = None
        if test:
            from .db_connector import PyCORNDatabase
            db = PyCORNDatabase(env_file=env_file)
    rows = []
    for alias, source in sources.items():
        prefix = env_prefix(alias)
        if source.databases == ALL_DATABASES:
            databases = "all UNICORN databases"
        elif source.databases:
            databases = "%d listed" % len(source.databases)
        else:
            databases = "login default"
        row = OrderedDict([("alias", alias),
                           ("settings", OrderedDict((k.lower(), "set" if (env.get(prefix + k) or "").strip() else "empty")
                                                    for k in SETTING_KEYS)),
                           ("databases", databases), ("connected", None), ("error", "")])
        if db is not None:
            try:
                db._connect(alias).close()
                row["connected"] = True
            except Exception as e:
                row["connected"], row["error"] = False, _connect_error(e)
        rows.append(row)
    texts = []
    for w in caught:
        text = plain_error(w.message) if isinstance(w.message, Exception) else str(w.message)
        if text not in texts:
            texts.append(text)
    return OrderedDict([("schema", 1), ("settings_file", _home_relative(path) if path is not None and path.is_file() else None),
                        ("environment", sorted(k.upper() for k in os.environ if k.upper().startswith("UNICORN_"))),
                        ("warnings", texts), ("tested", bool(test)), ("sources", rows)])


def status_lines(status: dict) -> list:
    """`source_status()` for people: [OK] / [FAIL] per tested source; names and set/empty only."""
    lines = ["settings file: %s" % (status["settings_file"] or "none (environment variables only)")]
    if status["environment"]:
        lines.append("environment variables (they override the file): %s" % ", ".join(status["environment"]))
    lines += ["warning: %s" % text for text in status["warnings"]]
    for row in status["sources"]:
        lines.append("source '%s': %s; databases: %s" % (row["alias"], ", ".join(
            "%s %s" % (k, v) for k, v in row["settings"].items()), row["databases"]))
        if row["connected"]:
            lines.append("[OK] connected to source '%s'" % row["alias"])
        elif row["connected"] is False:
            lines.append("[FAIL] could not connect to source '%s': %s" % (row["alias"], row["error"]))
    return lines


def test_sources(env_file, say) -> bool:
    """Test every source the settings name (as `pycorn find` reads them); True when all connected."""
    from .messages import plain_error
    try:
        status = source_status(env_file, test=True)
    except ImportError as e:
        say("[FAIL] %s" % e)
        return False
    except (ValueError, OSError) as e:  # our own sentences: they name settings, never values
        say("[FAIL] the settings could not be read: %s" % plain_error(e))
        return False
    except Exception as e:
        say("[FAIL] the settings could not be read: %s" % type(e).__name__)
        return False
    for line in status_lines(status):
        say(line)
    return all(row["connected"] for row in status["sources"])


def test_settings(path: Path, say) -> bool:
    """Connect with the file at `path` (UNICORN_* environment variables still override it), every source it names."""
    return test_sources(str(path), say)


def _report_overrides(path: Path, say, environment_warning=True) -> None:
    """Say, by name only, what wins over the file just written."""
    found = find_env_file()
    if found is not None and found.resolve() != path.resolve():
        say("warning: PyCORN reads another .env first (%s); the one written here is used only "
            "where that one is missing, or with PyCORNDatabase(env_file=...)." % _home_relative(found))
    overridden = sorted(k for k in os.environ if k.upper().startswith("UNICORN_"))
    if overridden and environment_warning:
        say("warning: these environment variables override the file: %s" % ", ".join(overridden))


def keep_dated_copy(path: Path, now=None) -> Path:
    """
    Copy `path` to `<name>.<YYYY-MM-DD_HHMMSS>.bak` next to it before it is changed (owner,
    2026-11-11: dated copies, not deletions); `-1`, `-2`, ... before `.bak` when that name is
    taken. Created new (never over another file), user-only (0600) outside Windows, since it
    holds the password. Returns the copy's path; raises OSError when it cannot be written
    (nothing is left behind then).
    """
    import datetime
    path = Path(path)
    data = path.read_bytes()
    stamp = (now or datetime.datetime.now()).strftime("%Y-%m-%d_%H%M%S")
    for n in range(1000):
        copy = path.with_name("%s.%s%s.bak" % (path.name, stamp, "-%d" % n if n else ""))
        try:
            fd = os.open(str(copy), os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0), 0o600)
        except FileExistsError:
            continue
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            if os.name != "nt":
                os.chmod(copy, 0o600)
        except OSError:
            try:
                copy.unlink()
            except OSError:
                pass
            raise
        return copy
    raise FileExistsError(17, "too many dated copies with the same time", str(path))


MAX_QUIET_COPIES = 5  # more dated copies than this: one warning (owner, 2026-11-11); none is ever removed


def dated_copies(path: Path) -> list:
    """The dated copies keep_dated_copy() made of `path` (`<name>.<YYYY-MM-DD_HHMMSS>[-n].bak`)."""
    import re
    path = Path(path)
    pattern = re.compile(re.escape(path.name) + r"\.\d{4}-\d\d-\d\d_\d{6}(-\d+)?\.bak")
    try:
        return sorted(p for p in path.parent.iterdir() if pattern.fullmatch(p.name))
    except OSError:
        return []


def _keep_copy_or_fail(path: Path, say) -> bool:
    """keep_dated_copy() with its path said (home as '~'); False, said as [FAIL], when it cannot be written."""
    from .messages import plain_error
    try:
        copy = keep_dated_copy(path)
    except OSError as e:
        say("[FAIL] could not keep a dated copy of the settings file (%s); nothing was written" % plain_error(e))
        return False
    say("Kept a dated copy of the old file: %s" % _home_relative(copy))
    count = len(dated_copies(path))
    if count > MAX_QUIET_COPIES:
        say("warning: %d dated copies of this settings file are kept in %s; each holds the password: "
            "remove the old ones you no longer need" % (count, _home_relative(path.parent)))
    return True


def run_setup(path: Path, test: bool = True, ask=input, ask_hidden=getpass.getpass, say=print) -> int:
    """Ask, write `path` (a dated copy of an existing one kept first), test; exit code 0 when
    written (and connected, when tested), 2 when the dated copy cannot be written."""
    path = Path(path)
    text = path.read_text(encoding="utf-8-sig") if path.is_file() else ""
    current = read_env_file(path) if text else {}
    say("PyCORN database settings: %s" % path)
    if text:
        say("Updating the existing file: " + ", ".join(
            "%s %s" % (key, "set" if current.get(key) else "empty") for key, *_ in FIELDS))
    values = {}
    for key, question, hidden, required in FIELDS:
        values[key] = _ask(key, question, hidden, required, current.get(key, ""), ask, ask_hidden, say)
    if text and not _keep_copy_or_fail(path, say):
        return 2
    path.parent.mkdir(parents=True, exist_ok=True)
    # A new file gets the optional settings that have a value only (as --from-env)
    written = values if text else {k: v for k, v in values.items() if v}
    path.write_text(update_env_text(text or HEADER, written), encoding="utf-8")
    if os.name != "nt":
        os.chmod(path, 0o600)  # the user only; %APPDATA% is already per user on Windows
    say("Saved (%s)." % ", ".join("%s %s" % (k, "set" if v else "empty") for k, v in values.items()))
    _report_overrides(path, say)
    if not test:
        return 0
    say("Testing the connection...")
    return 0 if test_settings(path, say) else 1


REQUIRED_KEYS = ("SERVER", "USER", "PASSWORD")


def settings_from_environment(environ=None):
    """
    (values, missing) for `--from-env` (Track 9, A11): `values` = the settings keys the readers
    understand (`load_env()` / `sources_from_env()`), in file order: UNICORN_SOURCES when set,
    then <prefix>SERVER, PORT, DATABASE, DATABASES, USER, PASSWORD for every source it lists
    ('default' alone without it; empty = not set). `missing` = the required names (SERVER, USER,
    PASSWORD of each source) that are empty. Other UNICORN_* variables are not written.
    Raises ValueError naming a setting, never a value (a bad alias or port).
    """
    from .db_connector import ConnectionParams
    from .db_sources import SOURCES_VAR, env_prefix, sources_from_env
    environ = os.environ if environ is None else environ
    env = {k.upper(): v for k, v in environ.items() if k.upper().startswith("UNICORN_")}
    sources = sources_from_env(env, ConnectionParams, _parse_port)
    values, missing = OrderedDict(), []
    if env.get(SOURCES_VAR, "").strip():
        values[SOURCES_VAR] = env[SOURCES_VAR].strip()
    for alias in sources:
        prefix = env_prefix(alias)
        for key in SETTING_KEYS:
            value = env.get(prefix + key, "")
            values[prefix + key] = value if key == "PASSWORD" else value.strip()
            if key in REQUIRED_KEYS and not value.strip():
                missing.append(prefix + key)
    return values, missing


def run_from_env(path: Path, test: bool = True, overwrite: bool = False, environ=None, say=print) -> int:
    """
    `pycorn-setup --from-env`: write `path` from the UNICORN_* environment variables, asking
    nothing and printing names with set / empty only. Exit code 2 (nothing written) when a
    required setting is empty, a setting cannot be read or stored, or `path` exists without
    `overwrite`, or its dated copy cannot be written; else 0, or 1 when the connection test
    fails. A new file gets the optional keys that have a value only. With `overwrite` a dated
    copy is kept first (`keep_dated_copy()`), then the file is updated in place as the
    interactive flow does: other lines stay, the keys written here take the environment's
    values, and a key the file has but the environment does not is cleared.
    """
    from .db_sources import SOURCES_VAR
    from .messages import plain_error
    path = Path(path)
    shown = _home_relative(path)
    say("PyCORN database settings from the environment: %s" % shown)
    try:
        values, missing = settings_from_environment(environ)
    except ValueError as e:  # names a setting, never a value
        say("[FAIL] the environment settings could not be read: %s; nothing was written" % e)
        return 2
    if missing:
        say("[FAIL] these environment variables are empty or not set: %s; nothing was written"
            % ", ".join(missing))
        return 2
    exists = path.is_file()
    if exists and not overwrite:
        say("[FAIL] a settings file is already there; add --overwrite to update it, or --file "
            "for another one; nothing was written")
        return 2
    try:
        text = path.read_text(encoding="utf-8-sig") if exists else ""
        in_file = read_env_file(path) if exists else {}
        if SOURCES_VAR not in values and SOURCES_VAR in in_file:
            values[SOURCES_VAR] = ""  # the file's source list would name sources the environment does not
        # An empty optional key is written only to clear one the file has (no empty lines added)
        values = OrderedDict((k, v) for k, v in values.items() if v.strip() or k in in_file)
        new_text = update_env_text(text or HEADER, values)
    except (ValueError, OSError) as e:
        say("[FAIL] the settings could not be written: %s" % (
            e if isinstance(e, ValueError) else plain_error(e)))
        return 2
    if exists and not _keep_copy_or_fail(path, say):
        return 2
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(new_text, encoding="utf-8")
        if os.name != "nt":
            os.chmod(path, 0o600)  # the user only; %APPDATA% is already per user on Windows
    except (ValueError, OSError) as e:
        say("[FAIL] the settings could not be written: %s" % (
            e if isinstance(e, ValueError) else plain_error(e)))
        return 2
    say("%s (%s)." % ("Updated" if exists else "Saved", ", ".join(
        "%s %s" % (k, "set" if v.strip() else "empty") for k, v in values.items())))
    _report_overrides(path, say, environment_warning=False)
    if not test:
        return 0
    say("Testing the connection...")
    return 0 if test_settings(path, say) else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="pycorn-setup",
        description="Write the UNICORN database settings PyCORN uses, then test the connection. "
                    "The password is typed hidden (or taken from UNICORN_PASSWORD with --from-env); "
                    "no value is ever printed. An existing file is first copied to "
                    "<name>.<YYYY-MM-DD_HHMMSS>.bak next to it (never removed; a warning past 5). Exit codes: 0 done, 1 written but not "
                    "connected, 2 nothing written (the dated copy failed; --from-env: a setting empty "
                    "or unreadable, or the file there without --overwrite).")
    parser.add_argument("--file", help="the .env to write (default: %s)" % _home_relative(db_connector.user_env_file()))
    parser.add_argument("--no-test", action="store_true", help="do not test the connection")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--test-only", action="store_true",
                      help="ask nothing; test the settings 'pycorn find' uses (the first .env found, UNICORN_* "
                           "environment variables, every source in UNICORN_SOURCES), or --file with them")
    mode.add_argument("--from-env", action="store_true",
                      help="ask nothing; write the file from the UNICORN_* environment variables (UNICORN_SERVER, "
                           "UNICORN_USER, UNICORN_PASSWORD needed; UNICORN_PORT, UNICORN_DATABASE(S), "
                           "UNICORN_SOURCES and each source's UNICORN_<ALIAS>_* as set), for scripts and "
                           "deployments; prints names with set/empty only. There is no password flag")
    parser.add_argument("--overwrite", action="store_true",
                        help="with --from-env: update a settings file that is already there (a dated "
                             ".bak copy of it is kept first)")
    args = parser.parse_args(argv)
    if args.overwrite and not args.from_env:
        parser.error("--overwrite goes with --from-env")
    path = Path(args.file) if args.file else db_connector.user_env_file()
    try:
        if args.from_env:
            return run_from_env(path, test=not args.no_test, overwrite=args.overwrite)
        if args.test_only:
            if args.file and not path.is_file():
                print("[FAIL] no settings file at %s: run pycorn-setup first" % path)
                return 1
            return 0 if test_sources(args.file, print) else 1
        return run_setup(path, test=not args.no_test)
    except (KeyboardInterrupt, EOFError):
        print("\nStopped; nothing more was written.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
