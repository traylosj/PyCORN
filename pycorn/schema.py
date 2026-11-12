# -*- coding: utf-8 -*-
"""
What PyCORN writes and understands, as data (Track 9, A2): `pycorn schema [--json]`.

`build_schema()` returns one dictionary, built live from the modules that define it, so it
never drifts from the code:

- `tables`: every exported table (`export_tables.TABLES`): key, title, CSV file, level
  (one per chromatogram or one per run), download group, origin, note, the columns the page
  shows first, and every listed column with its origin, meaning and check. Columns a table
  holds that are not listed take `other_columns`. The curves table's columns follow patterns
  (`<curve>: time (<unit>)`, ...), given in `columns` with `<curve>` and `<unit>` placeholders.
- `groups`: the download groups (`run_export.TABLE_GROUPS`, the page's Download panel and
  `pycorn export --tables`), the tables always written and the ones every group keeps.
- `roles`: curve roles (`curve_roles.ROLES`): stored data type, unit rule and name pattern.
- `units`: every unit spelling (`units.UNITS`) with its quantity and factor to the canonical
  unit; `quantities`: canonical and SI unit per quantity.
- `exit_codes`: the `pycorn` command's exit codes (`messages.EXIT_CODES`).
- `error_keys`: the `key` of the error object a command prints to stderr under --json /
  --jsonl (`messages.ERROR_KEYS`; docs/AGENT_GUIDE.md).

No run data and no settings are read, so the output holds nothing identifying. `"schema": 1`
versions the layout: a key is only ever added; a renamed or removed key raises the number.
`browser.names()` (the page's table names) is built from the same dictionary (`page_names()`).
"""
from collections import OrderedDict

SCHEMA_VERSION = 1
_XLSX_ONLY = ("summary", "chart")  # sheets of the workbook only (excel_export), no CSV file

# the curve roles' unit rules are functions; their meaning in words (a role a caller adds shows "a rule in code")
_UNIT_RULES = {
    "conductivity": "any unit except %",
    "conductivity_percent": "%",
    "conc_b": "%",
    "conc_q": "%",
    "flow": "a volumetric flow unit (ml/min, L/h, ...)",
    "sample_flow": "a volumetric flow unit (ml/min, L/h, ...)",
}
# curves table columns: (column as written, with placeholders; an example column to look up its meaning)
_CURVE_PATTERNS = (("<curve>: time (<unit>)", "c: time (min)"), ("<curve>: volume (<unit>)", "c: volume (ml)"),
                   ("<curve>: CV", "c: CV"), ("<curve>: UTC", "c: UTC"), ("<curve>: value (<unit>)", "c: value (mAU)"),
                   ("<curve> (mg/ml): value (<unit>)", "c (mg/ml): value (mg/ml)"))


def _column(key, column, example=None):
    from .export_tables import TABLES
    from .export_tables import _column as lookup
    origin, meaning, check = lookup(key, example or column) if key in TABLES else ("", "", "")
    return OrderedDict([("column", column), ("origin", origin), ("meaning", meaning), ("check", check)])


def _tables():
    from .export_tables import TABLES, csv_file, file_stem
    from .run_export import ALWAYS_TABLES, RUN_TABLES, TABLE_GROUPS, WITH_GROUPS
    group_of = {key: group for group, keys in TABLE_GROUPS for key in keys}
    out = []
    for key, table in TABLES.items():
        per_chrom = key not in RUN_TABLES
        if key in _XLSX_ONLY:
            group = "xlsx only"
        elif key in group_of:
            group = group_of[key]
        elif key in ALWAYS_TABLES:
            group = "always"
        elif key in WITH_GROUPS:
            group = "with every group"
        else:
            group = ""
        if key == "curves":
            columns = [_column(key, pattern, example) for pattern, example in _CURVE_PATTERNS]
        else:
            columns = [_column(key, name) for name in table.columns]
        origin, meaning, check = table.default
        out.append(OrderedDict([
            ("key", key), ("title", table.title), ("file_stem", file_stem(key)),
            ("csv_file", "" if key in _XLSX_ONLY else csv_file(key, per_chrom)), ("level", "chromatogram" if per_chrom else "run"),
            ("group", group), ("origin", table.origin), ("note", table.note), ("shown", list(table.shown)),
            ("freeze_through", table.freeze_through), ("columns", columns),
            ("other_columns", OrderedDict([("origin", origin), ("meaning", meaning), ("check", check)])),
        ]))
    return out


def _roles():
    from .curve_roles import LEGACY_KEYS, ROLES
    out = []
    for role, spec in ROLES.items():
        if "unit" in spec:
            unit = _UNIT_RULES.get(role, "a rule in code")
        else:
            unit = "any"
        out.append(OrderedDict([("role", role), ("data_type", spec.get("data_type") or "any"), ("unit", unit),
                                ("name_pattern", spec.get("name") or ""),
                                ("legacy_keys", [k for k, r in LEGACY_KEYS.items() if r == role])]))
    return out


def _units():
    from .units import CANONICAL, DISPLAY_DECIMALS, SI_UNITS, UNITS
    units = [OrderedDict([("unit", unit), ("quantity", q), ("factor", factor), ("canonical", CANONICAL.get(q, ""))])
             for unit, (q, factor) in UNITS.items()]
    quantities = OrderedDict((q, OrderedDict([("canonical", c), ("si", SI_UNITS.get(q, ""))]))
                             for q, c in CANONICAL.items())
    return units, quantities, dict(DISPLAY_DECIMALS)


def build_schema():
    """The schema dictionary (module docstring); JSON-ready."""
    from .exporters.cadet.api import CADET_EXPERIMENTAL_NOTE
    from .export_tables import ORIGIN_NOTES
    from .messages import ERROR_KEYS, EXIT_CODES
    from .run_export import ALWAYS_TABLES, FORMATS, TABLE_GROUPS, WITH_GROUPS
    from .version import __version__, version_text
    units, quantities, decimals = _units()
    return OrderedDict([
        ("schema", SCHEMA_VERSION),
        ("version", __version__),
        ("version_text", version_text()),
        ("formats", list(FORMATS)),
        ("origins", OrderedDict(ORIGIN_NOTES)),
        ("tables", _tables()),
        ("groups", [OrderedDict([("name", g), ("tables", list(keys))]) for g, keys in TABLE_GROUPS]),
        ("always_tables", list(ALWAYS_TABLES)),
        ("with_every_group", list(WITH_GROUPS)),
        ("roles", _roles()),
        ("units", units),
        ("quantities", quantities),
        ("display_decimals", decimals),
        ("exit_codes", OrderedDict((str(code), text) for code, text in EXIT_CODES.items())),
        ("cadet_experimental", CADET_EXPERIMENTAL_NOTE),
        ("error_keys", OrderedDict(ERROR_KEYS)),
    ])


def page_names(schema=None):
    """
    What `browser.names()` returns (before JSON): {'tables': {key: {'title', 'origin', 'note',
    'shown', 'freeze_through'}}, 'groups': [[group, [table key, ...]]], 'origins', 'version'
    (version text), 'cadet_experimental'}, from `build_schema()`.
    """
    schema = schema or build_schema()
    keep = ("title", "origin", "note", "shown", "freeze_through")
    return {"tables": {t["key"]: {k: t[k] for k in keep} for t in schema["tables"]},
            "groups": [[g["name"], list(g["tables"])] for g in schema["groups"]],
            "origins": dict(schema["origins"]), "version": schema["version_text"],
            "cadet_experimental": schema["cadet_experimental"]}


def schema_lines(schema=None):
    """The schema for people (`pycorn schema` without --json): one line per table, group, role, unit and exit code."""
    s = schema or build_schema()
    lines = ["PyCORN %s, schema %d (pycorn schema --json: every column with its origin and meaning)"
             % (s["version_text"], s["schema"]), "", "Tables (key: title; CSV file; group; origin)"]
    lines += ["  %s: %s; %s; %s; %s" % (t["key"], t["title"], t["csv_file"] or "-", t["group"] or "-", t["origin"])
              for t in s["tables"]]
    lines += ["", "Download groups (pycorn export --tables)"]
    lines += ["  %s: %s" % (g["name"], ", ".join(g["tables"])) for g in s["groups"]]
    lines += ["  always written: %s" % ", ".join(s["always_tables"]),
              "  kept with every group: %s" % ", ".join(s["with_every_group"])]
    lines += ["", "Curve roles (role: stored data type; unit; name pattern)"]
    lines += ["  %s: %s; %s%s" % (r["role"], r["data_type"], r["unit"],
                                  "; " + r["name_pattern"] if r["name_pattern"] else "") for r in s["roles"]]
    lines += ["", "Units by quantity (canonical first; SI)"]
    by_quantity = OrderedDict((q, []) for q in s["quantities"])
    for u in s["units"]:
        by_quantity.setdefault(u["quantity"], []).append(u["unit"])
    for q, names in by_quantity.items():
        info = s["quantities"].get(q, {})
        lines.append("  %s: %s%s" % (q, ", ".join(names), "; SI %s" % info["si"] if info.get("si") else ""))
    lines += ["", "Exit codes"]
    lines += ["  %s: %s" % (code, text) for code, text in s["exit_codes"].items()]
    lines += ["", "Error keys (--json / --jsonl: {\"error\": {\"text\", \"key\", \"exit\"}} on stderr)"]
    lines += ["  %s: %s" % (key, text) for key, text in s.get("error_keys", {}).items()]
    return lines
