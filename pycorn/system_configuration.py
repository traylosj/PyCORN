# -*- coding: utf-8 -*-
"""
Which inlet valves and pumps the system had, from the configuration each result stores
(owner, 2026-11-11: docs/ROADMAP.md, "Run structure", inlets from the system configuration;
docs/RUN_STRUCTURE_REVIEW.md 3.10, Decide 22; D27 (4)).

A result keeps a snapshot of its system: `SystemData` -> `Systems/System/EnabledComponents`
(every configurable component by name, enabled or not) and `StrategyData` ->
`StrategyInformation/ComponentStructure` (the same names with the hardware they stand for in
`@DisplayName`, e.g. 'Sample inlet (V9-IS)'). Zip, .UFol and database runs read alike
(`run.metadata`).

Roles (`ROLES`): inlet valve A, inlet valve B, sample inlet valve, sample pump, quaternary
valve. A component gets its role from its name (`ROLE_NAMES`) or from the hardware code in
its display name (`ROLE_CODES`). The names differ by system family (one family names the
B valve 'Inlet V9-IB' under 'Inlet B' and has 'Inlet B none' for no valve, another names it
'Inlet valve B'); the hardware codes are shared. Rules key on the installed hardware the
result lists, never on the system model: any valve can be missing on one system and
installed on many others. A name in neither table has role None ('not known'): never a guess.

Per role (`RoleState.state`):

- 'installed': a component of the role is enabled (present = True)
- 'none': the configuration says there is none ('Inlet B none' enabled; present = False)
- 'disabled': the role's components are listed, none enabled (present = False)
- 'not listed': no component of the role in the configuration (present = None, not known)
- 'conflict': a 'none' entry and a valve both enabled (present = None, not known)
- 'no configuration': the result stores no component list (present = None, not known)

`FIRST_POSITION` is each valve's first position as the run log names it, the position taken
(and labelled assumed) while the run log has not set one yet.
"""
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Optional

ROLES = ("inlet_valve_a", "inlet_valve_b", "sample_inlet_valve", "sample_pump", "quaternary_valve")
ROLE_TEXT = OrderedDict([("inlet_valve_a", "inlet valve A"), ("inlet_valve_b", "inlet valve B"),
                         ("sample_inlet_valve", "sample inlet valve"), ("sample_pump", "sample pump"),
                         ("quaternary_valve", "quaternary valve")])
# the line each inlet role serves ('A', 'B', 'sample': the keys of `Section.inlets`)
LINE_ROLES = OrderedDict([("A", "inlet_valve_a"), ("B", "inlet_valve_b"), ("sample", "sample_inlet_valve")])
# the valve's first position as the run log names it ('Inlet B B1', 'Sample inlet Buffer')
FIRST_POSITION = {"A": "A1", "B": "B1", "sample": "Buffer"}

# component names per role, as results store them (two system families seen; tested against a
# lab environment). Parents ('Inlet A') and sub-components ('Inlet V9-IA') both count.
ROLE_NAMES = OrderedDict([
    ("inlet_valve_a", ("Inlet A", "Inlet V9-IA", "Inlet A V9-IAB", "Inlet valve A", "Inlet A GUI",
                       "2nd Inlet valve A")),
    ("inlet_valve_b", ("Inlet B", "Inlet V9-IB", "Inlet B V9-IAB", "Inlet valve B", "Inlet B GUI",
                       "2nd Inlet valve B")),
    ("sample_inlet_valve", ("Sample inlet valve", "Sample inlet GUI", "2nd Inlet sample valve")),
    ("sample_pump", ("Sample pump",)),
    # 'Quatenary' (sic) as stored
    ("quaternary_valve", ("Quatenary gradient", "Quaternary gradient", "Quaternary valve")),
])
# names that say the role has no hardware ('no inlet valve on line B')
NONE_NAMES = OrderedDict([("inlet_valve_a", ("Inlet A none",)), ("inlet_valve_b", ("Inlet B none",)),
                          ("sample_inlet_valve", ("Sample inlet none",))])
# hardware codes in `ComponentStructure` display names ('V9-IA (7-ports)', 'Sample pump (P9-S)')
ROLE_CODES = OrderedDict([
    ("inlet_valve_a", re.compile(r"\bV9-IA\b(?!B)|\bV9-IAB part A\b|\bV9-A2\b")),
    ("inlet_valve_b", re.compile(r"\bV9-IB\b|\bV9-IAB part B\b|\bV9-B2\b")),
    ("sample_inlet_valve", re.compile(r"\bV9-IS\b|\bV9-S2\b")),
    ("sample_pump", re.compile(r"\bP9-S\b|\bS9\b")),
    ("quaternary_valve", re.compile(r"\bQ9\b")),
])
NOT_KNOWN = "not known"


@dataclass
class Component:
    name: str
    enabled: Optional[bool]  # None: listed in the structure only
    display_name: Optional[str]
    role: Optional[str]  # one of ROLES, 'none:<role>' for a 'none' entry, None: not known


@dataclass
class RoleState:
    role: str
    present: Optional[bool]  # True installed, False not installed, None not known
    state: str  # 'installed', 'none', 'disabled', 'not listed', 'conflict', 'no configuration'
    components: list = field(default_factory=list)  # the names that decided it

    @property
    def text(self):
        what = ROLE_TEXT[self.role]
        if self.present is True:
            return "%s installed (%s)" % (what, ", ".join(self.components))
        if self.present is False:
            return "no %s (%s)" % (what, "configured as none" if self.state == "none" else "not enabled")
        return "%s: %s (%s)" % (what, NOT_KNOWN, self.state)


@dataclass
class SystemConfiguration:
    roles: "OrderedDict[str, RoleState]"
    components: list  # every Component listed, in stored order
    listed: bool  # the result stores a component list

    def role(self, role):
        return self.roles[role]

    def line(self, line):
        """The RoleState of the inlet valve on `line` ('A', 'B' or 'sample')."""
        return self.roles[LINE_ROLES[line]]

    def present(self, role):
        return self.roles[role].present

    def summary(self):
        """{role: {'present', 'state', 'components'}}: plain data (JSON, the CADET experiment)."""
        return OrderedDict((r, OrderedDict([("present", s.present), ("state", s.state),
                                            ("components", list(s.components))]))
                           for r, s in self.roles.items())


def _list(x):
    return x if isinstance(x, list) else ([] if x is None else [x])


def _flag(value):
    text = str(value).strip().lower()
    return True if text == "true" else False if text == "false" else None


def _display_names(node, out):
    """name -> @DisplayName of every Component / SubComponent in a ComponentStructure."""
    if isinstance(node, dict):
        name = node.get("@Name")
        if name is not None and "@DisplayName" in node:
            out.setdefault(str(name), str(node["@DisplayName"]))
        for v in node.values():
            if isinstance(v, (dict, list)):
                _display_names(v, out)
    elif isinstance(node, list):
        for v in node:
            _display_names(v, out)
    return out


def _metadata(run, key, *path):
    node = (getattr(run, "metadata", None) or {}).get(key)
    for p in path:
        node = node.get(p) if isinstance(node, dict) else None
    return node


def role_of(name, display_name=None):
    """The role of component `name` (`ROLES`), 'none:<role>' for a 'none' entry, or None (not known)."""
    for role, names in NONE_NAMES.items():
        if name in names:
            return "none:" + role
    for role, names in ROLE_NAMES.items():
        if name in names:
            return role
    for role, code in ROLE_CODES.items():
        if display_name and code.search(display_name):
            return role
    return None


def system_configuration(run):
    """The `SystemConfiguration` of `run` (see the module docstring); never raises on odd data."""
    system = _metadata(run, "SystemData", "Systems", "System")
    if isinstance(system, list):  # one system per result; take the first if more are stored
        system = next((s for s in system if isinstance(s, dict)), None)
    entries = (system.get("EnabledComponents") if isinstance(system, dict) else None) or {}
    entries = _list(entries.get("EnabledComponent")) if isinstance(entries, dict) else []
    displays = _display_names(_metadata(run, "StrategyData", "StrategyInformation", "ComponentStructure"), {})
    components = []
    for e in entries:
        if not isinstance(e, dict) or e.get("Name") is None:
            continue
        name = str(e.get("Name"))
        components.append(Component(name=name, enabled=_flag(e.get("IsEnabled")), display_name=displays.get(name),
                                    role=role_of(name, displays.get(name))))
    listed = bool(components)
    roles = OrderedDict()
    for role in ROLES:
        if not listed:
            roles[role] = RoleState(role, None, "no configuration")
            continue
        on = [c.name for c in components if c.role == role and c.enabled]
        none = [c.name for c in components if c.role == "none:" + role and c.enabled]
        mine = [c.name for c in components if c.role in (role, "none:" + role)]
        if on and none:
            roles[role] = RoleState(role, None, "conflict", on + none)
        elif on:
            roles[role] = RoleState(role, True, "installed", on)
        elif none:
            roles[role] = RoleState(role, False, "none", none)
        elif mine:
            roles[role] = RoleState(role, False, "disabled", mine)
        else:
            roles[role] = RoleState(role, None, "not listed")
    return SystemConfiguration(roles=roles, components=components, listed=listed)
