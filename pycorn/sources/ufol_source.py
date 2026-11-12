"""
Reader for UNICORN folder exports (.UFol).

A .UFol is an uncompressed POSIX tar holding the exported folder tree. Each result is a member
`<folder path>/<result name>.<original result id>.Result`, which is an ordinary UNICORN 6+
result bundle (the same ZIP as a .zip export). `systemnames.xml` sits at the top level.
UNICORN writes a `<file>.UFol.export.txt` summary next to it with each result's original
database path. Members are read in memory; nothing is extracted to disk.
"""
import io
import os
import re
import tarfile
from collections import OrderedDict

from .zip_source import file_origin, read_zip

_RESULT_SUFFIX = ".Result"
_ORIGINAL_ID = re.compile(r"^(?P<name>.*)\.(?P<id>\d+)\.Result$")


def read_ufol(path, adjust_axes=True, axes_options=None):
    """
    Read every result in a UNICORN folder export into a list of `Run`s, in archive order.

    Each run has `source` 'ufol', `locator` '<.UFol path>/<member path>', `origin` with the
    .UFol's `file_name` and `path` and the `member`, and `container` with
    the export's `member` path, `folder` (exported folder path), `name` (result name),
    `original_id` (result id in the source database) and `original_path` (from the
    .export.txt summary when it is next to the file, else None).
    """
    return list(iter_ufol(path, adjust_axes=adjust_axes, axes_options=axes_options))


def iter_ufol(path, adjust_axes=True, axes_options=None, members=None):
    """
    `read_ufol()` one run at a time (a generator), so only one is in memory. `members`
    (member paths, as `run.container['member']` gives them) reads only those.
    """
    original_paths = _read_export_summary(str(path) + ".export.txt")
    with tarfile.open(path, mode="r:") as archive:
        for member in archive:
            if not (member.isfile() and member.name.endswith(_RESULT_SUFFIX)):
                continue
            if members is not None and member.name not in members:
                continue
            data = archive.extractfile(member).read()
            folder, base = member.name.rpartition("/")[::2]
            match = _ORIGINAL_ID.match(base)
            name = match.group("name") if match else base[:-len(_RESULT_SUFFIX)]
            exported = (folder + "/" if folder else "") + name
            run = read_zip(io.BytesIO(data), adjust_axes=adjust_axes, axes_options=axes_options)
            run.source = "ufol"
            run.locator = "%s/%s" % (path, member.name)
            run.container = OrderedDict([
                ("member", member.name),
                ("folder", folder),
                ("name", name),
                ("original_id", int(match.group("id")) if match else None),
                ("original_path", original_paths.get(exported)),
            ])
            run.origin = file_origin(path)
            run.origin["member"] = member.name
            yield run


def _read_export_summary(path):
    """Map exported path -> original database path from a .UFol.export.txt summary."""
    if not os.path.exists(path):
        return {}
    out = {}
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            fields = dict(part.split(": ", 1) for part in line.rstrip("\r\n").split("\t")
                          if ": " in part)
            if "Exported path" in fields and "Original path" in fields:
                out[fields["Exported path"]] = fields["Original path"]
    return out
