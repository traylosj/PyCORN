#!/usr/bin/env python
"""
Build the in-browser page (docs/ROADMAP.md, item 6): `web/index.html` with the PyCORN wheel
and its pure-Python dependencies put inside it, as one file.

    python tools/build_web_page.py [-o build/web/index.html]

The built page needs only the Pyodide and Plotly files from their CDNs (pinned in the
template); numpy and pandas come with Pyodide. Put it on GitHub Pages (the `pages` workflow
does) or on a shared drive and open it in a browser. Needs pip and internet access (the
wheel build and the dependency wheels); builds from a copy of the sources, so it writes
nothing into the checkout but the output.
"""
import argparse
import base64
import importlib.util
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEMPLATE = os.path.join(ROOT, "web", "index.html")
I18N = os.path.join(ROOT, "web", "i18n")
MARKER = "__PYCORN_WHEELS__"
STRINGS_MARKER = "__PYCORN_STRINGS__"
HASH_MARKER = "__PYCORN_SCRIPT_HASH__"  # in the Content-Security-Policy of the template
# PyCORN's pure-Python dependencies that Pyodide does not ship (numpy, pandas it does);
# pinned so a page is the same whenever it is built
DEPENDENCIES = ["xmltodict==1.0.4", "xlsxwriter==3.3.1"]


def source_copy(folder):
    """What setup.py packages, copied to `folder`: setuptools then builds there, so no stale
    `build/lib` of the checkout gets into the wheel and the checkout gets no build files."""
    for name in ("setup.py", "README.md", os.path.join("examplescripts", "pycorn-bin.py")):
        os.makedirs(os.path.dirname(os.path.join(folder, name)), exist_ok=True)
        shutil.copy2(os.path.join(ROOT, name), os.path.join(folder, name))
    shutil.copytree(os.path.join(ROOT, "pycorn"), os.path.join(folder, "pycorn"),
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "_build_info.py"))
    # the copy has no .git: record the checkout's commit, so the page and its exports can say it
    # (version.py alone: importing the package would need its dependencies here)
    spec = importlib.util.spec_from_file_location("pycorn_version", os.path.join(ROOT, "pycorn", "version.py"))
    version = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(version)
    with open(os.path.join(folder, "pycorn", "_build_info.py"), "w", encoding="utf-8") as f:
        f.write("# written by tools/build_web_page.py\nCOMMIT = %r\n" % version.git_commit())
    return folder


def wheels(folder):
    """Build the PyCORN wheel and fetch the dependency wheels into `folder`; their paths."""
    pip = [sys.executable, "-m", "pip", "--disable-pip-version-check", "-q"]
    source = source_copy(os.path.join(folder, "source"))
    subprocess.run(pip + ["wheel", "--no-deps", "-w", folder, source], check=True)
    subprocess.run(pip + ["download", "--no-deps", "--only-binary=:all:", "-d", folder] + DEPENDENCIES, check=True)
    found = sorted(os.path.join(folder, n) for n in os.listdir(folder) if n.endswith(".whl"))
    if not any(os.path.basename(p).lower().startswith("pycorn-") for p in found):
        raise SystemExit("[FAIL] no PyCORN wheel was built")
    for p in found:
        if not p.endswith("-none-any.whl"):
            raise SystemExit("[FAIL] %s is not pure Python; Pyodide cannot install it" % os.path.basename(p))
    return found


def page_strings():
    """The page's texts by language code, from `web/i18n/<code>.json` (en.json is the reference)."""
    found = {}
    for name in sorted(os.listdir(I18N)):
        if name.endswith(".json"):
            with open(os.path.join(I18N, name), encoding="utf-8") as f:
                found[name[:-5]] = json.load(f)
    if "en" not in found:
        raise SystemExit("[FAIL] %s needs en.json (the reference and fallback language)" % I18N)
    return found


def script_hash(page):
    """The Content-Security-Policy source ('sha256-...') of the page's one program script: the
    text between `<script>` and `</script>`, byte for byte, as the browser hashes it. The JSON
    script elements (wheels, strings) are data, not run, so the policy does not list them."""
    found = re.findall(r"<script>(.*?)</script>", page, re.S)
    if len(found) != 1:
        raise SystemExit("[FAIL] the page must hold exactly one <script> (found %d)" % len(found))
    digest = hashlib.sha256(found[0].encode("utf-8")).digest()
    return "'sha256-%s'" % base64.b64encode(digest).decode("ascii")


def build(out):
    with open(TEMPLATE, encoding="utf-8") as f:
        page = f.read()
    for marker in (MARKER, STRINGS_MARKER, HASH_MARKER):
        if page.count(marker) != 1:
            raise SystemExit("[FAIL] %s must hold %s once" % (TEMPLATE, marker))
    with tempfile.TemporaryDirectory() as tmp:
        embedded = {}
        for path in wheels(tmp):
            with open(path, "rb") as f:
                embedded[os.path.basename(path)] = base64.b64encode(f.read()).decode("ascii")
    # inside <script type="application/json">: '</' must not appear (base64 and names have none)
    text = json.dumps(embedded)
    if "</" in text:
        raise SystemExit("[FAIL] wheel names would end the script element")
    strings = json.dumps(page_strings())  # ASCII-escaped: safe inside the script element
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="\n") as f:
        built = page.replace(MARKER, text).replace(STRINGS_MARKER, strings.replace("</", "<\\/"))
        f.write(built.replace(HASH_MARKER, script_hash(built)))
    print("[OK] %s (%d KB): %s" % (out, os.path.getsize(out) // 1024, ", ".join(sorted(embedded))))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    parser.add_argument("-o", "--output", default=os.path.join(ROOT, "build", "web", "index.html"),
                        help="where to write the page (default: build/web/index.html, gitignored)")
    build(parser.parse_args(argv).output)


if __name__ == "__main__":
    main()
