# -*- coding: utf-8 -*-
"""
Errors in plain words, for people rather than programmers (docs/ROADMAP.md, item 5).

`plain_error(e)` gives one sentence for an exception: what went wrong and, where it is
clear, what to do. PyCORN's own errors already say that and are returned as they are;
errors from the operating system, zip/tar files and memory are reworded here, so the CLI,
warnings and any app show the same sentence. The traceback stays for `--debug`.
"""
import errno
import tarfile
import zipfile

#: the CLI's line for an error nobody worded (exit 3)
REPORT = ("run the same command again with --debug and send the output (and the file, if you can share it) "
          "at https://github.com/traylosj/PyCORN/issues")

#: added to database connection errors
SETUP_HINT = "check the settings with: pycorn-setup --test-only"

#: exit codes of the `pycorn` command (`cli.main`); `pycorn schema` lists them
EXIT_CODES = {
    0: "everything asked for was done",
    1: "it ran, but left something out; the lines above say what (a database not reached, a file not read, "
       "a run not loaded or incomplete, a source that did not connect)",
    2: "error: a flag, a file or folder name, the settings or the connection; nothing was done",
    3: "unexpected error: a fault in PyCORN; run the same command again with --debug and report the output",
    130: "interrupted (Ctrl+C)",
}


#: the `key` of the error object the `pycorn` command prints in --json / --jsonl mode (`error_key`);
#: a key is only ever added, never renamed
ERROR_KEYS = {
    "bad_flag": "a flag or its value is wrong (usage, or a flag's value that cannot be read); exit 2",
    "bad_input": "an input cannot be used (a selection CSV, a combination of inputs); exit 2",
    "no_settings": "no database server is set up: run pycorn-setup (or set UNICORN_* variables); exit 2",
    "bad_settings": "the database settings cannot be read (e.g. a port that is not a number); exit 2",
    "connection_failed": "a database could not be reached or refused the login; exit 2",
    "file_not_found": "a file or folder named on the command line is not there; exit 2",
    "permission_denied": "no permission to read or write a file or folder; exit 2",
    "os_error": "another error of the operating system (disk full, ...); exit 2",
    "out_of_memory": "not enough memory; exit 2",
    "missing_package": "an optional package is not installed (the text names it); exit 2",
    "interrupted": "interrupted (Ctrl+C); exit 130",
    "unexpected": "a fault in PyCORN; run again with --debug and report the output; exit 3",
}


class SettingsError(ValueError):
    """A ValueError about the database settings; `error_key` 'no_settings' or 'bad_settings' (`ERROR_KEYS`)."""

    def __init__(self, text, error_key="bad_settings"):
        super().__init__(text)
        self.error_key = error_key


def error_key(e):
    """The `ERROR_KEYS` key of an exception the `pycorn` command stops at."""
    key = getattr(e, "error_key", None)
    if key in ERROR_KEYS:
        return key
    if isinstance(e, KeyboardInterrupt):
        return "interrupted"
    if isinstance(e, ConnectionError):
        return "connection_failed"
    if isinstance(e, FileNotFoundError):
        return "file_not_found"
    if isinstance(e, PermissionError):
        return "permission_denied"
    if isinstance(e, MemoryError):
        return "out_of_memory"
    if isinstance(e, ImportError):
        return "missing_package"
    if isinstance(e, OSError):
        return "os_error"
    if isinstance(e, (ValueError, KeyError, LookupError)):
        return "bad_flag" if plain_error(e).startswith("--") else "bad_input"
    return "unexpected"


class Message(ValueError):
    """
    A ValueError the in-browser page can translate (`browser.message`): `key` is a `msg.*` key of
    `web/i18n/en.json`, `values` fill its {placeholders}, `str(e)` is the English text (what the
    command line and Python callers see).
    """

    def __init__(self, key, text, **values):
        super().__init__(text)
        self.key, self.values = key, values


def _name(e):
    return getattr(e, "filename", None) or ""


def plain_error(e):
    """One sentence for `e` (no type names or error codes unless nothing else is known)."""
    if isinstance(e, zipfile.BadZipFile):
        return "not a zip file, or the file is damaged (export it again from UNICORN)"
    if isinstance(e, tarfile.TarError):
        return "not a UNICORN folder export (.UFol), or the file is damaged (export it again from UNICORN)"
    if isinstance(e, MemoryError):
        return "not enough memory: export fewer or smaller files at a time"
    if isinstance(e, OSError):
        name = _name(e)
        where = " %s" % name if name else ""
        if isinstance(e, FileNotFoundError):
            return "not found:%s" % where if name else (str(e) or "file or folder not found")
        if isinstance(e, PermissionError):
            return "no permission to use%s (open in another program, such as Excel? read-only folder?)" % where
        if isinstance(e, FileExistsError):
            return "already there:%s" % where if name else (str(e) or "already there")
        if isinstance(e, NotADirectoryError):
            return "a file stands where a folder is needed:%s" % where
        if isinstance(e, IsADirectoryError):
            return "a folder stands where a file is needed:%s" % where
        if e.errno == errno.ENOSPC:
            return "the disk is full%s" % (" (%s)" % name if name else "")
        if e.strerror:
            return "%s%s" % (e.strerror, ":" + where if name else "")
    if isinstance(e, KeyError) and len(e.args) == 1 and isinstance(e.args[0], str):
        return e.args[0]  # str(KeyError) adds quotes
    text = str(e).strip()
    return text or type(e).__name__
