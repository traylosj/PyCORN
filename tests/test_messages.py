# -*- coding: utf-8 -*-
"""Errors in plain words (pycorn.messages, ROADMAP item 5) and plain database connection errors."""
import errno
import tarfile
import zipfile

import pytest

from pycorn import db_connector
from pycorn.messages import plain_error


@pytest.mark.parametrize("error, expected", [
    (zipfile.BadZipFile("File is not a zip file"), "not a zip file, or the file is damaged"),
    (tarfile.ReadError("bad"), "not a UNICORN folder export (.UFol)"),
    (MemoryError(), "not enough memory"),
    (FileNotFoundError(errno.ENOENT, "No such file", "a.zip"), "not found: a.zip"),
    (PermissionError(errno.EACCES, "Access denied", "a.xlsx"), "no permission to use a.xlsx (open in another"),
    (FileExistsError(errno.EEXIST, "exists", "out"), "already there: out"),
    (NotADirectoryError(errno.ENOTDIR, "not a dir", "afile"), "a file stands where a folder is needed: afile"),
    (OSError(errno.ENOSPC, "No space left", "out"), "the disk is full (out)"),
    (KeyError("no curve 'UV' here"), "no curve 'UV' here"),
    (ValueError("--epsilon 'abc': give a number"), "--epsilon 'abc': give a number"),
    (RuntimeError(), "RuntimeError"),
])
def test_plain_error(error, expected):
    assert plain_error(error).startswith(expected)


@pytest.mark.parametrize("driver_text, expected", [
    ("(18456, b\"Login failed for user 'x'.\")", "the server refused the login"),
    ("(4060, b'Cannot open database \"x\" requested by the login.')", "the login cannot open the database"),
    ("(20003, b'Adaptive Server connection timed out')", "no answer from the SQL Server in time"),
    ("(20009, b'Unable to connect: TDS server is unavailable or does not exist (h)\\nNet-Lib error (10061)')",
     "cannot reach the SQL Server"),
])
def test_connection_errors_in_plain_words_keep_the_redacted_detail(driver_text, expected):
    class _Failing:
        def connect(self, **kwargs):
            raise RuntimeError(driver_text.replace("(h)", "(db.example.invalid)"))

    with pytest.raises(ConnectionError) as error:
        db_connector._connect_safely(_Failing(), server="db.example.invalid", user="reader")
    assert str(error.value).startswith(expected)
    assert "db.example.invalid" not in error.value.detail and "reader" not in error.value.detail


def test_unknown_connection_error_keeps_the_driver_text_redacted():
    class _Failing:
        def connect(self, **kwargs):
            raise RuntimeError("odd failure at db.example.invalid")

    with pytest.raises(ConnectionError) as error:
        db_connector._connect_safely(_Failing(), server="db.example.invalid")
    assert str(error.value) == error.value.detail == "odd failure at [redacted]"
