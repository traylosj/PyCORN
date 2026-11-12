# -*- coding: utf-8 -*-
"""
PyCORN's version, so every output can be traced to the code that wrote it (owner, 2026-11-09).

- `__version__`: the release, the only place it is set (`setup.py` reads it; CHANGELOG.md
  lists what each release changed; a release is tagged `v<version>` in git).
- `commit()`: the git commit the code came from: read live when PyCORN runs from a git
  checkout (with '.dirty' when tracked files have uncommitted changes), else from
  `_build_info.py`, which the wheel and web-page builds write; None when neither is there.
- `version_text()`: '0.21.0 (commit 1a2b3c4d5e6f)', what outputs record.
"""
import functools
import os
import subprocess

__version__ = "0.37.0"

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _git(*args):
    out = subprocess.run(["git", "-C", _ROOT] + list(args), capture_output=True, text=True, timeout=10, check=True)
    return out.stdout.strip()


def git_commit():
    """The commit of the checkout PyCORN runs from ('<12 hex>' or '<12 hex>.dirty'), else None."""
    if not os.path.exists(os.path.join(_ROOT, ".git")):
        return None
    try:
        head = _git("rev-parse", "--short=12", "HEAD")
        dirty = _git("status", "--porcelain", "--untracked-files=no")
    except (OSError, subprocess.SubprocessError):
        return None
    return head + (".dirty" if dirty else "") if head else None


@functools.lru_cache(maxsize=1)
def commit():
    """The git commit this code came from (see the module docstring), or None."""
    live = git_commit()
    if live:
        return live
    try:
        from ._build_info import COMMIT
    except ImportError:
        return None
    return COMMIT or None


def version_text():
    """'<version> (commit <id>)', or '<version> (commit unknown)'."""
    return "%s (commit %s)" % (__version__, commit() or "unknown")
