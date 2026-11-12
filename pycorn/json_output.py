# -*- coding: utf-8 -*-
"""
Machine-readable output of the `pycorn` command (Track 9, A3/A4): one place for the JSON
envelope, the JSON lines, the error object, paths under the home folder and the masking of
people in catalog rows. Every command's `--json` / `--jsonl` uses it (`cli.py`); a later
server (A10) can wrap the same contracts. docs/AGENT_GUIDE.md describes them per command.

- `document(command, masked, **payload)`: the one JSON document `--json` prints:
  {'schema': 1, 'version', 'command', 'masked', ...payload}.
- `header(command, masked)`, `line(kind, fields)`: the JSON lines of `--jsonl`: a header
  {'schema', 'version', 'command', 'type': 'header', 'masked'} first, then one object per row
  ('type': 'row', 'run', 'reading', ...), a {'type': 'summary', ...} last.
- `error_document(text, key, exit_code)`: {'error': {'text', 'key', 'exit'}}, what a failed
  command prints to stderr in --json / --jsonl mode (`messages.error_key`, `EXIT_CODES`).
- `home_tilde(path)` / `tilde_text(text)`: the home folder shown as '~'.
- `PeopleMasker`: logins, computer and instrument names and serial numbers as User1,
  Computer1, System1, Serial1 (the same rule as `run_details.Masker`), with one numbering for
  the whole document; `mask_people(rows, masker)` masks the catalog columns that hold people
  (`PEOPLE_COLUMNS`) and the texts built from them (`match_key`). Run, folder, method names
  and variables are not masked: a caller searches and picks by them (owner, 2026-11-11).

`SCHEMA_VERSION` versions every document and line: a key is only ever added; a renamed or
removed key raises the number. Nothing here reads settings: the database is named by its
label ('alias#n') only, never by server or database name.
"""
import datetime as dt
import json
import math
import os
import re
from collections import OrderedDict

SCHEMA_VERSION = 1

#: catalog columns (find, file catalog, export index) that hold people or instruments -> label prefix
PEOPLE_COLUMNS = OrderedDict([("system", "System"), ("created_by", "User")])
#: catalog columns built from a people column, and export_error (a reason that may quote one): the
#: masked values replaced inside them
DERIVED_PEOPLE_COLUMNS = ("match_key", "export_error")
#: catalog columns never written to stdout: `database` is a connection value (the label stands for it)
HIDDEN_COLUMNS = ("database",)
#: catalog columns holding a file path: shown with the home folder as '~'
PATH_COLUMNS = ("path",)


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------

def plain(value):
    """`value` as JSON takes it: numpy numbers and times, datetimes, NaN/NaT/NA (None), bytes (hex), containers."""
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, dict):
        return OrderedDict((str(k), plain(v)) for k, v in value.items())
    if isinstance(value, (list, tuple)):
        return [plain(v) for v in value]
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    import numpy as np
    if isinstance(value, np.datetime64):
        return None if np.isnat(value) else str(np.datetime_as_string(value))
    if isinstance(value, (dt.datetime, dt.date)):
        try:
            import pandas as pd
            if value is pd.NaT:
                return None
        except ImportError:
            pass
        return value.isoformat()
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, (float, np.floating)):
        value = float(value)
        return value if math.isfinite(value) else None
    if isinstance(value, int):
        return value
    if isinstance(value, np.ndarray):
        raise TypeError("an array has no place in a JSON document of PyCORN")
    try:
        import pandas as pd
        if value is pd.NA or value is pd.NaT:
            return None
        if isinstance(value, pd.Timedelta):
            return value.total_seconds()
    except ImportError:
        pass
    return str(value)


def home_tilde(path):
    """`path` (text) with the home folder written '~' when it lies under it; else as given."""
    if path is None:
        return None
    text = str(path)
    home = os.path.expanduser("~")
    if not home or home == "~":
        return text
    try:
        full = os.path.realpath(text) if os.path.isabs(text) else text
    except (OSError, ValueError):
        full = text
    for candidate in (full, text):
        if os.path.normcase(candidate) == os.path.normcase(home):
            return "~"
        prefix = os.path.normcase(home.rstrip("\\/")) + os.sep
        if os.path.normcase(candidate).startswith(prefix):
            return "~" + os.sep + candidate[len(prefix):]
    return text


def tilde_text(text):
    """`text` with every mention of the home folder written '~' (any case, either slash)."""
    if not isinstance(text, str) or not text:
        return text
    home = os.path.expanduser("~").rstrip("\\/")
    if not home or home == "~":
        return text
    pattern = r"[\\/]".join(re.escape(part) for part in re.split(r"[\\/]", home))
    return re.sub(pattern + r"(?![A-Za-z0-9_.-])", "~", text, flags=re.I if os.name == "nt" else 0)


# ---------------------------------------------------------------------------
# Documents, lines, errors
# ---------------------------------------------------------------------------

def _envelope(command, masked):
    from .version import __version__
    return OrderedDict([("schema", SCHEMA_VERSION), ("version", __version__), ("command", command)])


def document(command, masked, **payload):
    """The one JSON document of `--json`: {'schema': 1, 'version', 'command', 'masked', ...payload}."""
    out = _envelope(command, masked)
    out["masked"] = bool(masked)
    out.update(payload)
    return out


def header(command, masked):
    """The first line of `--jsonl`: {'schema': 1, 'version', 'command', 'type': 'header', 'masked'}."""
    out = _envelope(command, masked)
    out["type"] = "header"
    out["masked"] = bool(masked)
    return out


def line(kind, fields=None, **more):
    """One `--jsonl` line after the header: {'type': kind, ...fields}."""
    out = OrderedDict([("type", kind)])
    out.update(fields or {})
    out.update(more)
    return out


def dumps_document(doc):
    """A document as `--json` prints it (indented; ASCII: non-ASCII characters as \\u escapes)."""
    return json.dumps(plain(doc), indent=1)


def dumps_line(obj):
    """One `--jsonl` line (ASCII, no newline inside)."""
    return json.dumps(plain(obj))


def error_document(text, key, exit_code, **more):
    """{'error': {'text', 'key', 'exit', ...more}}: a failed command in --json / --jsonl mode (stderr)."""
    error = OrderedDict([("text", tilde_text(text)), ("key", key), ("exit", int(exit_code))])
    error.update((k, tilde_text(v)) for k, v in more.items())
    return OrderedDict([("error", error)])


class JsonLines:
    """Writes `--jsonl`: the header at once, `write(kind, fields)` per row, `summary(**fields)` last."""

    def __init__(self, stream, command, masked):
        self.stream = stream
        self.emit(header(command, masked))

    def emit(self, obj):
        self.stream.write(dumps_line(obj) + "\n")
        self.stream.flush()

    def write(self, kind, fields=None, **more):
        self.emit(line(kind, fields, **more))

    def summary(self, **fields):
        self.emit(line("summary", fields))


# ---------------------------------------------------------------------------
# People in catalog rows
# ---------------------------------------------------------------------------

class PeopleMasker:
    """
    Logins, computer and instrument names and serial numbers -> User1, Computer1, System1,
    Serial1, numbered once per document (`run_details.Masker`'s rule: built-in accounts
    Default and System stay; a login's parts 'DOMAIN\\name' are masked too; serial numbers
    shorter than `run_details.MIN_SERIAL` stay). `add(value, prefix)` learns a value;
    `absorb(run)` everything `Masker(run)` finds; `text(value)` replaces every learned value
    (whole words, any case).
    """

    def __init__(self):
        self.names = OrderedDict()
        self._known = {}  # lower case -> label
        self._counts = {}
        self._free = set()
        self._pattern = None

    def add(self, value, prefix):
        """Learn `value` as a `prefix` ('User', 'Computer', 'System', 'Serial'); returns its label (or the value)."""
        from .run_details import BUILTIN_ACCOUNTS, MIN_SERIAL
        if value is None or (not isinstance(value, str) and plain(value) is None):
            return value
        text = str(value).strip()
        if not text or text.lower() in BUILTIN_ACCOUNTS or (prefix == "Serial" and len(text) < MIN_SERIAL):
            return value
        known = self._known.get(text.lower())
        if known is not None:  # the same name in another case: the same label
            return known
        self._counts[prefix] = self._counts.get(prefix, 0) + 1
        self.names[text] = self._known[text.lower()] = "%s%d" % (prefix, self._counts[prefix])
        self._pattern = None
        if prefix == "User":
            for part in re.split(r"[\\/@]+", text):
                if part and part != text:
                    self.add(part, prefix)
        return self.names[text]

    def absorb(self, run):
        """Learn what `run_details.Masker(run)` finds in a loaded run (people, hosts, instruments, serials, notes)."""
        from .run_details import Masker
        masker = Masker(run)
        for value, label in masker.names.items():
            self.add(value, label.rstrip("0123456789"))
        self._free |= masker._free

    def text(self, value):
        from .run_details import FREE_TEXT_MASK
        if not isinstance(value, str) or not value:
            return value
        if value.strip() in self._free:
            return FREE_TEXT_MASK
        if not self.names:
            return value
        if self._pattern is None:
            keys = sorted(self.names, key=len, reverse=True)
            self._pattern = re.compile(r"(?<![A-Za-z0-9])(%s)(?![A-Za-z0-9])" % "|".join(map(re.escape, keys)), re.I)
        return self._pattern.sub(lambda m: self._known[m.group(1).lower()], value)


def mask_people(row, masker):
    """
    One catalog row (dict) with `PEOPLE_COLUMNS` replaced by labels and the values replaced
    inside `DERIVED_PEOPLE_COLUMNS`; other columns as given. Returns a new dict.
    """
    out = OrderedDict(row)
    for column, prefix in PEOPLE_COLUMNS.items():
        if column in out:
            out[column] = masker.add(out[column], prefix)
    for column in DERIVED_PEOPLE_COLUMNS:
        if column in out:
            out[column] = masker.text(out[column])
    return out


def learn_catalog(frame, masker):
    """Learn the people columns of a catalog, in row order, before any run is loaded (labels in catalog order)."""
    if masker is None:
        return
    for column, prefix in PEOPLE_COLUMNS.items():
        if column in frame.columns:
            for value in frame[column]:
                masker.add(value, prefix)


def catalog_rows(frame, masker=None, labels=None):
    """
    The rows of a catalog or export index (DataFrame) as JSON-ready dicts for stdout:
    `database` left out (`labels`, one per row, go in as `db_label` when the frame has no such
    column), paths with the home folder as '~', people masked by `masker` (None: as stored).
    """
    rows = []
    columns = [c for c in frame.columns if c not in HIDDEN_COLUMNS]
    add_label = labels is not None and "db_label" not in frame.columns
    for i, values in enumerate(frame[columns].itertuples(index=False, name=None)):
        row = OrderedDict()
        if add_label:
            row["db_label"] = labels[i]
        for column, value in zip(columns, values):
            value = plain(value)
            if column in PATH_COLUMNS:
                value = home_tilde(value)
            row[str(column)] = value
        rows.append(mask_people(row, masker) if masker is not None else row)
    return rows


def stdout_frame(frame, masker=None):
    """A catalog DataFrame as `--csv -` writes it: `database` left out, paths with '~', people masked."""
    import pandas as pd
    out = frame.drop(columns=[c for c in HIDDEN_COLUMNS if c in frame.columns])
    for column in PATH_COLUMNS:
        if column in out.columns:
            out[column] = [home_tilde(v) if isinstance(v, str) else v for v in out[column]]
    if masker is None or not len(out):
        return out
    out = out.copy()
    for column, prefix in PEOPLE_COLUMNS.items():
        if column in out.columns:
            out[column] = pd.Series([masker.add(v, prefix) for v in out[column]], index=out.index, dtype=object)
    for column in DERIVED_PEOPLE_COLUMNS:
        if column in out.columns:
            out[column] = pd.Series([masker.text(v) for v in out[column]], index=out.index, dtype=object)
    return out


def write_csv(frame, stream):
    """`frame` as CSV text on `stream` (UTF-8 bytes when it has a byte layer; no BOM)."""
    text = frame.to_csv(index=False, lineterminator="\n")
    buffer = getattr(stream, "buffer", None)
    if buffer is not None:
        stream.flush()
        buffer.write(text.encode("utf-8"))
        buffer.flush()
    else:
        stream.write(text)
        stream.flush()
