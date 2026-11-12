"""PyCORN - UNICORN result file parser and SQL Server database connector.

File parsing: PcRes3 (.res, UNICORN 3-5), PcUni6 (.zip, UNICORN 6+).
Full run model: open_run() loads everything in a UNICORN 6+ .zip into a Run;
PyCORNDatabase.load_run() loads a result from the database into the same Run.
Database: PyCORNDatabase and helpers using pymssql (no ODBC).

`__all__` names the supported public API (docs/AGENT_GUIDE.md); everything else is reached by
module path (`pycorn.run_export`, `pycorn.units`, ...) and may change between releases.
"""

from .version import __version__, version_text
from .pycorn import PcRes3, PcUni6
# not public API, kept importable for code written against older releases
from .pycorn import return_on_failure, NrbfError, decode_nrbf  # noqa: F401
from .model import Run, Chromatogram, Curve, CurvePointSet, EventCurve, Event, PeakTable, Entry, DbTable
from .sources import open_run, open_folder
from .file_catalog import catalog_from_files, load_files, match_zip_to_db
from .exporters.cadet.api import export_cadet

# loaded on first use: `python -m pycorn.db_connector` runs without a second copy, and
# `import pycorn` stays light (name -> module)
_LAZY = {name: "db_connector" for name in (
    "PyCORNDatabase", "DataFrameResult", "ConnectionParams", "get_cte_query", "execute_cte_query",
    "fetch_results", "test_connection", "get_connection_params")}
_LAZY.update({
    "write_run": "run_export", "ExportOptions": "run_export", "export_selection": "run_export",
    "bypass_uv_readings": "uv_calibration",
    "parse_windows": "window_stats", "phase_windows": "window_stats", "window_stats_rows": "window_stats",
    "curves_with_role": "curve_roles", "ROLES": "curve_roles",
    "build_schema": "schema", "run_summary": "summary",
    "source_status": "setup_env",
    "plain_error": "messages",
})
_DB_NAMES = tuple(n for n, m in _LAZY.items() if m == "db_connector")  # kept for older code

__all__ = [
    # version
    "__version__", "version_text",
    # reading runs
    "open_run", "open_folder", "Run", "Chromatogram", "Curve", "CurvePointSet", "EventCurve", "Event",
    "PeakTable", "Entry", "DbTable", "PcRes3", "PcUni6",
    # file catalogs
    "catalog_from_files", "load_files", "match_zip_to_db",
    # database (`test_connection` is left out: test runners would collect it from `import *`)
    "PyCORNDatabase", "DataFrameResult", "ConnectionParams", "get_cte_query", "execute_cte_query",
    "fetch_results", "get_connection_params", "source_status",
    # exporting
    "write_run", "export_selection", "ExportOptions", "export_cadet",
    # analysis
    "curves_with_role", "ROLES", "bypass_uv_readings", "parse_windows", "phase_windows", "window_stats_rows",
    # discovery and messages
    "build_schema", "run_summary", "plain_error",
]


def __getattr__(name):
    if name in _LAZY:
        import importlib
        return getattr(importlib.import_module("." + _LAZY[name], __name__), name)
    raise AttributeError("module 'pycorn' has no attribute %r" % name)


def __dir__():
    return sorted(set(globals()) | set(_LAZY))
