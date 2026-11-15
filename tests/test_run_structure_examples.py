"""
The worked examples of docs/RUN_STRUCTURE_RECORD.md (tools/build_run_structure_examples.py):
every example result builds into a page without an error block, every section is there, every
link to the markdown resolves, and the tracked pages in docs/examples/ are what the script
writes now (so a change to a reader that moves a number shows up as a stale file).
"""
import glob
import os
import re
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(ROOT, "tools", "build_run_structure_examples.py")
TRACKED = os.path.join(ROOT, "docs", "examples")
SECTIONS = ["checks", "above", "levels", "setact", "curves", "tables", "path", "facts", "scope", "record", "raw", "warn"]


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    out = tmp_path_factory.mktemp("examples")
    done = subprocess.run([sys.executable, "-I", SCRIPT, "-o", str(out)], capture_output=True, text=True, timeout=900)
    assert done.returncode == 0, done.stderr[-2000:]
    return out, done.stdout


def test_every_example_result_has_a_page(built):
    out, stdout = built
    samples = sorted(os.path.splitext(os.path.basename(p))[0] for p in glob.glob(os.path.join(ROOT, "samples", "*.zip")))
    assert samples
    for name in samples:
        assert (out / (name + ".html")).exists(), name
        assert (out / (name + "_record_prototype.json")).exists(), name
    assert "[FAIL]" not in stdout


def test_pages_have_every_section_and_no_error_block(built):
    out, _ = built
    for page in out.glob("*.html"):
        if page.name == "index.html":
            continue
        text = page.read_text(encoding="utf-8")
        assert 'class="err"' not in text, page.name
        assert "querySelectorAll('.fig')" in text, "the zoom script is missing from " + page.name
        assert "http://" not in text.replace("http://www.w3.org/2000/svg", ""), page.name
        for key in SECTIONS:
            assert 'id="%s"' % key in text, (page.name, key)


def test_links_to_the_markdown_resolve(built):
    _, stdout = built
    assert "did not resolve" not in stdout, stdout[-1500:]


def test_pages_hold_no_machine_path_or_login(built):
    out, _ = built
    for page in list(out.glob("*.html")) + list(out.glob("*.json")):
        text = page.read_text(encoding="utf-8")
        assert not re.search(r"[A-Za-z]:[\\/]Users", text), page.name
        assert "../../../" not in text, page.name


def test_tracked_examples_are_current(built):
    out, _ = built
    for page in sorted(out.iterdir()):
        tracked = os.path.join(TRACKED, page.name)
        assert os.path.exists(tracked), "%s is not in docs/examples: run tools/build_run_structure_examples.py" % page.name
        with open(tracked, "rb") as fh:
            old = fh.read()
        assert old == page.read_bytes(), "docs/examples/%s is stale: run tools/build_run_structure_examples.py and commit" % page.name
