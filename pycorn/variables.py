# -*- coding: utf-8 -*-
"""
Method variables of a run: the values the run used, from the result's search criteria.

UNICORN stores one search-criteria row per variable (`ResultSearchCriteria`; `Result.xml`
in a bundle): `Name` (kind), `Keyword1` (variable name), `Keyword2` (value as text) and
`ExtraDisplayInformation` (unit, when stored). A scouted variable is stored only as a
`ScoutingVariableValue` row, holding the scouting value; there is no `VariableValue` row for
it. So every stored row is the value the run used (tested against a lab environment
and confirmed by the run log). The method's programmed value is
only in the `MethodData` blob and is not read here: search must stay on indexed tables.

Variable names are the method author's and differ between methods: take them from the
caller, never hardcode them. Kinds are not checked against a fixed list.

Each value keeps its stored text (`value`, significant figures intact) next to the parsed
number (`number`, NaN when the text is not a number). Parsing follows SQL Server's
`TRY_CAST(... AS float)`, so local filters and database filters agree; the one exception is
empty text, which SQL Server casts to 0 and this module leaves as NaN.
"""
import math
import re
from typing import NamedTuple, Optional

# kinds that hold a method variable (others, e.g. 'QuestionAnswer', are start-protocol answers)
VARIABLE_KINDS = ("VariableValue", "ScoutingVariableValue")
SCOUTING_KIND = "ScoutingVariableValue"

# what SQL Server's TRY_CAST(text AS float) accepts (checked against SQL Server): digits with an optional
# point and exponent ('e' or 'd'), any leading white space, trailing spaces only (not tabs);
# no 'inf', 'nan', '1_000' or decimal commas; values beyond float range give NULL
_NUMBER = re.compile(r"^\s*([+-]?(?:\d+\.?\d*|\.\d+))(?:[eEdD]([+-]?\d+))? *$")

COLUMNS = ["kind", "variable", "value", "number", "unit", "scouting"]


class Variable(NamedTuple):
    kind: Optional[str]
    variable: Optional[str]
    value: Optional[str]  # exactly as stored
    number: float  # NaN when `value` is not a number
    unit: Optional[str]  # ExtraDisplayInformation as stored ('' and None both occur)
    scouting: bool  # True for a scouting value


def parse_number(text):
    """The number in `text` as SQL Server's TRY_CAST to float reads it, else NaN (also for empty text)."""
    match = _NUMBER.match(text) if text is not None else None
    if not match:
        return math.nan
    number = float(match.group(1) + ("e" + match.group(2) if match.group(2) else ""))
    return number if math.isfinite(number) else math.nan


def _text(criterion, key):
    """
    The stored text: '' for an empty element (the XML reader gives None, the database stores
    ''), None only when the field is absent (NULL in the database).
    """
    value = criterion.get(key)
    return "" if value is None and key in criterion else value


def rows(run):
    """One `Variable` per stored search-criteria row, in stored order."""
    out = []
    for c in run.result.get("search_criteria") or []:
        kind = c.get("Name")
        value = _text(c, "Keyword2")
        out.append(Variable(kind, c.get("Keyword1"), value, parse_number(value),
                            _text(c, "ExtraDisplayInformation"), kind == SCOUTING_KIND))
    return out


def to_frame(run):
    """`rows(run)` as a DataFrame with `COLUMNS`."""
    import pandas as pd
    found = rows(run)
    # column by column: built from row tuples, pandas turns a stored None into NaN
    data = {c: pd.Series([getattr(r, c) for r in found], dtype=dtype)
            for c, dtype in zip(COLUMNS, [object, object, object, float, object, bool])}
    return pd.DataFrame(data, columns=COLUMNS)


def variable(run, name):
    """
    The method variable `name` as the run used it. UNICORN stores each variable once; should
    a name ever appear twice, the scouting value wins (it overrides the method value).
    """
    found = [r for r in rows(run) if r.kind in VARIABLE_KINDS and r.variable == name]
    if not found:
        names = [r.variable for r in rows(run) if r.kind in VARIABLE_KINDS]
        raise KeyError("no method variable %r in %s; available: %s" % (name, run.name, ", ".join(names)))
    return next((r for r in found if r.scouting), found[0])
