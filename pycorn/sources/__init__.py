"""
Readers that load a UNICORN result into a `pycorn.model.Run`.

Files: `open_run()` and `open_folder()` below. Database: `sql_source.read_result()`, or
`PyCORNDatabase.load_run()`, which connects with the credentials in sql/.env.
"""
import os

from .ufol_source import read_ufol
from .zip_source import read_zip


def _axes_options(column_volume, column_volume_unit, use_name_hint):
    return {"column_volume": column_volume, "column_volume_unit": column_volume_unit, "use_name_hint": use_name_hint}


def open_run(path, adjust_axes=True, column_volume=None, column_volume_unit=None, use_name_hint=False):
    """
    Load a UNICORN result file into a `Run`, keeping all of its contents.

    Supported: UNICORN 6+ result bundles (.zip, or a .Result taken out of a folder export).

    The stored axes (`Curve.time`, `Curve.volume`, event `time`/`volume`) are never changed.
    By default the `*_adjusted` axes are zeroed at the injection UNICORN uses (see
    `Chromatogram.retention_zero`). With `adjust_axes=False` they equal the stored axes,
    so every axis is exactly as stored.

    Every curve and event also gets `cv` (column volumes through the column, by-pass not
    counted) and `utc` (wall clock rebuilt from the run log's pauses); see `pycorn.axes`.
    `column_volume` (in `column_volume_unit`, else the run's volume unit) overrides the stored
    column volume; `use_name_hint=True` also accepts a volume found in the run or folder name.
    """
    ext = os.path.splitext(str(path))[1].lower()
    if ext in (".zip", ".result"):
        return read_zip(path, adjust_axes=adjust_axes,
                        axes_options=_axes_options(column_volume, column_volume_unit, use_name_hint))
    if ext == ".ufol":
        raise ValueError("%s is a folder export holding several results; use open_folder()" % path)
    raise ValueError("unsupported result file: %s (expected a UNICORN 6+ .zip)" % path)


def open_folder(path, adjust_axes=True, column_volume=None, column_volume_unit=None, use_name_hint=False):
    """
    Load every result in a UNICORN folder export (.UFol) into a list of `Run`s.

    Each run is read exactly as `open_run()` reads a .zip; `run.container` adds where it sat
    in the export (folder, original result id, original database path).
    """
    if os.path.splitext(str(path))[1].lower() != ".ufol":
        raise ValueError("unsupported folder export: %s (expected a UNICORN .UFol)" % path)
    return read_ufol(path, adjust_axes=adjust_axes,
                     axes_options=_axes_options(column_volume, column_volume_unit, use_name_hint))
