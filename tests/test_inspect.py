# -*- coding: utf-8 -*-
"""`pycorn inspect` and `pycorn.summary.run_summary` (Track 9, A3): masked by default, JSON, no arrays or IP."""
import json
import os
import re
import zipfile

import pytest

import pycorn
from pycorn import cli, open_run
from pycorn.summary import SCHEMA_VERSION, SummaryNames, run_summary, summary_lines

import multichrom_fixture

SAMPLES = os.path.abspath("../samples")
# made up for the tests: what a real result holds in these fields
PLANTED = {"user": "LAB\\jdoe", "computer": "LABPC-0042", "system": "AKTA-Bay7", "icu": "SACX0001",
           "ip": "10.20.30.40"}
IP = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")


def load(name="u73_blank.zip"):
    return open_run(os.path.join(SAMPLES, name))


def planted_run():
    """u73_blank with identifying values put back where UNICORN stores them."""
    run = load()
    run.result["CreatedBy"] = PLANTED["user"]
    run.result["SystemName"] = PLANTED["system"]
    system = run.metadata["SystemData"]["Systems"]["System"]
    system.update(Name=PLANTED["system"], ComputerName=PLANTED["computer"], ICUSerialNumber=PLANTED["icu"],
                  ICUIPAddress=PLANTED["ip"])
    for chrom in run.chromatograms.values():
        for ec in chrom.event_curves.values():
            for e in ec.events:
                if (e.text or "").startswith("Method Run"):
                    e.text = "Method Run 1/1/2021 Method: Method1 Result: /Folder1/Run1 User %s at computer %s " \
                             "is in control" % (PLANTED["user"], PLANTED["computer"])
    run.warnings.append("planted note naming %s on %s" % (PLANTED["computer"], PLANTED["system"]))
    return run


def planted_zip(path, run_name=None, folder=None):
    """u73_blank.zip with a planted instrument name (and run name, folder) in Result.xml (for the command line)."""
    with zipfile.ZipFile(os.path.join(SAMPLES, "u73_blank.zip")) as z:
        members = [(i.filename, z.read(i)) for i in z.infolist()]
    planted = {"SystemName": PLANTED["system"], "Name": run_name, "FolderPath": folder}
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as out:
        for name, raw in members:
            if name == "Result.xml":
                for tag, value in planted.items():
                    if value is not None:
                        raw = re.sub(rb"<%s>[^<]*</%s>" % (tag.encode(), tag.encode()),
                                     b"<%s>%s</%s>" % (tag.encode(), value.encode(), tag.encode()), raw, count=1)
            out.writestr(name, raw)
    return str(path)


# made up for the tests: names a project would put in run names, folders and file names
SECRET_RUN, OTHER_RUN, SECRET_FOLDER = "Alpha Secret Run", "Beta Hidden Run", "/DefaultHome/Gamma Project"


def planted_names(tmp_path):
    """
    Two files: a multi-chromatogram result named SECRET_RUN (its COPY chromatogram a copy of
    itself, IMPORTED from OTHER_RUN), listed first, then the result OTHER_RUN; both in SECRET_FOLDER.
    """
    base = planted_zip(tmp_path / "base.zip", SECRET_RUN, SECRET_FOLDER)
    first = tmp_path / ("%s.zip" % SECRET_RUN)
    multichrom_fixture.build(first, base=base)
    with zipfile.ZipFile(first) as z:
        members = [(i.filename, z.read(i)) for i in z.infolist()]
    with zipfile.ZipFile(tmp_path / "multi.zip", "w", zipfile.ZIP_DEFLATED) as out:
        for name, raw in members:
            out.writestr(name, raw.replace(multichrom_fixture.SOURCE_NAME.encode(), OTHER_RUN.encode()))
    os.replace(tmp_path / "multi.zip", first)
    second = planted_zip(tmp_path / ("%s.zip" % OTHER_RUN), OTHER_RUN, SECRET_FOLDER)
    return [str(first), second]


def _arrays(value):
    """True when `value` holds a list of numbers longer than any summary's (a curve array)."""
    if isinstance(value, dict):
        return any(_arrays(v) for v in value.values())
    if isinstance(value, list):
        if len(value) > 50 and all(isinstance(v, (int, float)) for v in value):
            return True
        return any(_arrays(v) for v in value)
    return False


def _ip(value, key=""):
    """True when a text in `value` looks like an IP address (the UNICORN version, a.b.c.d too, is skipped)."""
    if isinstance(value, dict):
        return any(_ip(v, k) for k, v in value.items())
    if isinstance(value, list):
        return any(_ip(v, key) for v in value)
    return isinstance(value, str) and key != "unicorn_version" and bool(IP.search(value))


def test_summary_is_json_and_holds_what_the_run_has():
    run = load("u711_proa_day12.zip")
    s = run_summary(run)
    text = json.dumps(s)  # serialisable as is
    assert json.loads(text) == json.loads(json.dumps(s))
    assert s["masked"] is True and s["source"]["kind"] == "zip" and s["source"]["file"] == "File1.zip"
    assert s["run_start"]["source"] == "RunStartDate" and s["run_start"]["utc"]
    chrom = s["chromatograms"][0]
    assert chrom["kind"] == "original" and chrom["default"] is True
    assert chrom["column_volume"]["value"] == pytest.approx(run.chromatogram().column_volume_info["value"])
    # curves by role, never by name: every UV curve of the run is listed with role 'uv'
    uv = run.chromatogram().curves_with_role("uv", original_only=False)
    listed = [c for c in chrom["curves"] if c["role"] == "uv"]
    assert uv and len(listed) == len(uv)
    # recorded curves keep their names; an evaluated one (a name a user set) is Curve1, ...
    assert {c["name"] for c in listed if c["original"]} == {c.name for c in uv if c.is_original is not False}
    evaluated = [c["name"] for c in listed if c["original"] is False]
    assert evaluated and all(re.fullmatch(r"Curve\d+", n) for n in evaluated)
    assert run_summary(run, mask=False)["chromatograms"][0]["curves"] == [
        dict(c, name=n) for c, n in zip(chrom["curves"], [c.name for c in run.chromatogram().curves.values()])]
    first = chrom["curves"][0]
    assert first["points"] > 50 and "time" in first["axes"] and set(first) >= {"name", "role", "unit", "original"}
    for key, kind in (("phases", "phase"), ("injections", "injection"), ("fractions", "fraction")):
        assert len(chrom[key]) == len(run.chromatogram().table_rows(kind))
    assert chrom["phases"] and "start_volume" in chrom["phases"][0] and "text" not in chrom["phases"][0]
    assert s["variables"] and {"name", "value", "unit"} <= set(s["variables"][0])
    assert not _arrays(s)


def test_masked_by_default_no_mask_shows_never_ip():
    masked = json.dumps(run_summary(planted_run()))
    for key in ("user", "computer", "system", "ip"):
        assert PLANTED[key] not in masked, key
    assert "jdoe" not in masked and "System1" in masked and "Computer1" in masked
    plain = json.dumps(run_summary(planted_run(), mask=False))
    assert PLANTED["system"] in plain and PLANTED["computer"] in plain
    assert PLANTED["ip"] not in plain and not _ip(json.loads(plain))
    assert not re.search(r"(?i)\bport\b", plain)


def test_chromatogram_kinds(tmp_path):
    run = open_run(multichrom_fixture.build(tmp_path / "multi.zip"))
    s = run_summary(run, mask=False)
    kinds = {c["name"]: c for c in s["chromatograms"]}
    assert kinds["Chrom.1"]["kind"] == "original" and kinds["Chrom.1"]["default"] is True
    imported = kinds[multichrom_fixture.IMPORTED]
    assert imported["kind"] == "imported" and imported["default"] is False
    assert str(imported["source_result_id"]) == str(multichrom_fixture.SOURCE_ID)
    assert imported["copy_of_run"] is False and kinds[multichrom_fixture.COPY]["copy_of_run"] is True
    assert kinds[multichrom_fixture.DERIVED]["kind"] == "derived"
    assert all(c["original"] is False for c in imported["curves"])
    lines = summary_lines(s)
    assert all(line.encode("ascii", "backslashreplace") for line in lines)


def test_public_api():
    assert "run_summary" in pycorn.__all__ and pycorn.run_summary is run_summary


def test_cli_json_masked_by_default(tmp_path, capsys):
    path = planted_zip(tmp_path / "planted.zip")
    assert cli.main(["inspect", path, "--json"]) == 0
    out = capsys.readouterr().out
    doc = json.loads(out)
    assert doc["schema"] == SCHEMA_VERSION == 1 and doc["command"] == "inspect" and doc["masked"] is True
    assert len(doc["runs"]) == 1 and doc["not_read"] == [] and doc["problems"] == []
    assert PLANTED["system"] not in out and doc["runs"][0]["system"]["name"].startswith("System")
    assert not _arrays(doc) and not _ip(doc)
    out.encode("ascii")  # one ASCII document


def test_cli_no_mask_and_text(tmp_path, capsys):
    path = planted_zip(tmp_path / "planted.zip")
    assert cli.main(["inspect", path, "--json", "--no-mask"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc["masked"] is False and doc["runs"][0]["system"]["name"] == PLANTED["system"]
    assert cli.main(["inspect", path]) == 0
    text = capsys.readouterr().out
    assert text.startswith("Run: ") and "curves:" in text and "masked" in text and PLANTED["system"] not in text
    text.encode("ascii")


def test_cli_from_catalog_and_exit_codes(tmp_path, capsys):
    from pycorn import catalog_from_files
    catalog = catalog_from_files([os.path.join(SAMPLES, "u73_blank.zip"), os.path.join(SAMPLES, "sample.zip")])
    csv = tmp_path / "files.csv"
    catalog.to_csv(csv, index=False)
    assert cli.main(["inspect", "--from", str(csv), "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)["runs"]) == 2
    bad = tmp_path / "bad.zip"
    bad.write_bytes(b"not a zip")
    assert cli.main(["inspect", str(bad), os.path.join(SAMPLES, "u73_blank.zip"), "--json"]) == 1
    captured = capsys.readouterr()
    doc = json.loads(captured.out)  # stdout stays one JSON document; the problem goes to stderr
    assert len(doc["runs"]) == 1 and doc["problems"]
    assert re.fullmatch(r"File\d+\.zip", doc["not_read"][0]["file"])  # masked: the file name too
    assert "incomplete" in captured.err and "bad.zip" not in captured.err + captured.out
    assert cli.main(["inspect", str(bad), "--json", "--no-mask"]) == 1
    assert json.loads(capsys.readouterr().out)["not_read"][0]["file"] == "bad.zip"
    assert cli.main(["inspect"]) == 2  # neither files nor --from
    assert cli.main(["inspect", "--from", str(tmp_path / "none.csv")]) == 2


def test_single_run_names_masked(tmp_path):
    run = open_run(planted_names(tmp_path)[0])
    run.warnings.append("note on %s in %s" % (SECRET_RUN, SECRET_FOLDER))
    masked = run_summary(run)
    text = json.dumps(masked)
    for secret in (SECRET_RUN, OTHER_RUN, SECRET_FOLDER, "Gamma Project"):
        assert secret.lower() not in text.lower(), secret
    assert masked["name"] == "Run1" and masked["folder"] == "Folder1" and masked["source"]["file"] == "File1.zip"
    assert "note on Run1 in Folder1" in masked["warnings"]
    # stored order: the original, IMPORTED, COPY, DERIVED (their names masked: Chrom1, ...)
    _, imported, copy, _ = masked["chromatograms"]
    assert copy["source_result_name"] == "Run1"  # a copy of itself
    assert imported["source_result_name"] == "Result1" and imported["label"] == "imported from Result1"
    assert str(imported["source_result_id"]) == str(multichrom_fixture.SOURCE_ID)  # IDs are numbers: kept
    plain = json.dumps(run_summary(run, mask=False))
    for secret in (SECRET_RUN, OTHER_RUN, SECRET_FOLDER):
        assert secret in plain, secret


def test_document_labels_stable_and_nothing_leaks(tmp_path, capsys):
    files = planted_names(tmp_path)
    assert cli.main(["inspect"] + files + ["--json"]) == 0
    captured = capsys.readouterr()
    doc = json.loads(captured.out)
    for secret in (SECRET_RUN, OTHER_RUN, SECRET_FOLDER, "Gamma Project", PLANTED["system"]):
        assert secret.lower() not in (captured.out + captured.err).lower(), secret
    first, second = doc["runs"]
    assert (first["name"], second["name"]) == ("Run1", "Run2")
    assert first["folder"] == second["folder"] == "Folder1"  # the same folder, the same label
    assert (first["source"]["file"], second["source"]["file"]) == ("File1.zip", "File2.zip")
    _, imported, copy, _ = first["chromatograms"]  # stored order: the original, IMPORTED, COPY, DERIVED
    # imported from the run listed second: its label, though that run was summarised later
    assert imported["source_result_name"] == "Run2" and imported["label"] == "imported from Run2"
    assert copy["source_result_name"] == "Run1" and copy["label"] == "imported from Run1 (a copy of this run)"
    assert cli.main(["inspect"] + files) == 0  # the text output: the same labels
    text = capsys.readouterr().out
    assert "Run: Run1" in text and "Run: Run2" in text and SECRET_RUN not in text
    assert cli.main(["inspect"] + files + ["--json", "--no-mask"]) == 0
    out = capsys.readouterr().out
    for secret in (SECRET_RUN, OTHER_RUN, SECRET_FOLDER, PLANTED["system"], os.path.basename(files[0])):
        assert secret in out, secret


# made up for the tests: a method name, a text variable value, a chromatogram and a curve name a user set
SECRET_METHOD, SECRET_TEXT, SECRET_CHROM, SECRET_CURVE = ("Epsilon Method", "Eta Secret Column",
                                                          "Theta Secret Chrom", "Zeta Curve Secret")


def _rewrite(path, changes):
    """Rewrite the zip at `path` with byte replacements per member: {member: [(old, new), ...]}."""
    with zipfile.ZipFile(path) as z:
        members = [(i.filename, z.read(i)) for i in z.infolist()]
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as out:
        for name, raw in members:
            for old, new in changes.get(name, ()):
                assert old in raw, (name, old)
                raw = raw.replace(old, new)
            out.writestr(name, raw)


def planted_free_text(tmp_path):
    """u711_proa_day12 with SECRET_* planted where UNICORN stores them, plus the multi-chromatogram copies."""
    base = tmp_path / "base.zip"
    with zipfile.ZipFile(os.path.join(SAMPLES, "u711_proa_day12.zip")) as z, zipfile.ZipFile(base, "w") as out:
        for i in z.infolist():
            out.writestr(i.filename, z.read(i))
    original = run_summary(open_run(str(base)), mask=False)["chromatograms"][0]["curves"]
    evaluated = next(c["name"] for c in original if c["original"] is False).encode()
    _rewrite(base, {"Chrom.1.Xml": [(b"Method: Method1", b"Method: " + SECRET_METHOD.encode()),
                                    (evaluated, SECRET_CURVE.encode())],
                    "Result.xml": [(b"HiTrap", SECRET_TEXT.encode())]})
    path = tmp_path / "planted_text.zip"
    multichrom_fixture.build(path, base=base)
    _rewrite(path, {"Chrom.2.Xml": [(multichrom_fixture.IMPORTED.encode(), SECRET_CHROM.encode())]})
    return str(path)


def test_method_variables_user_names_masked(tmp_path, capsys):
    path = planted_free_text(tmp_path)
    assert cli.main(["inspect", path, "--json", "--no-mask"]) == 0
    plain = capsys.readouterr().out
    for secret in (SECRET_METHOD, SECRET_TEXT, SECRET_CHROM, SECRET_CURVE):
        assert secret in plain, secret
    for args in (["--json"], []):
        assert cli.main(["inspect", path] + args) == 0
        captured = capsys.readouterr()
        for secret in (SECRET_METHOD, SECRET_TEXT, SECRET_CHROM, SECRET_CURVE):
            assert secret not in captured.out + captured.err, (args, secret)
    assert cli.main(["inspect", path, "--json"]) == 0
    masked, shown = json.loads(capsys.readouterr().out)["runs"][0], json.loads(plain)["runs"][0]
    chroms = masked["chromatograms"]
    assert [c["name"] for c in chroms] == ["Chrom.1", "Chrom1", "Chrom2", "Chrom3"]  # the original's name stays
    assert re.search(r"Method: Method\d+ Result: Folder\d+/Run\d+ ", chroms[0]["phases"][0]["name"])
    curves = {c["name"] for c in chroms[0]["curves"]}
    assert "Curve1" in curves and SECRET_CURVE not in curves
    recorded = [c["name"] for c in shown["chromatograms"][0]["curves"] if c["original"]]
    assert recorded and set(recorded) <= curves  # curves as UNICORN recorded them keep their names
    assert set(recorded) <= {c["name"] for c in chroms[1]["curves"]}  # also as copied into an import
    variables = {v["name"]: v for v in masked["variables"]}
    for v in shown["variables"]:  # names and numbers stay; text values are masked
        m = variables[v["name"]]
        assert m["number"] == v["number"]
        assert m["value"] == (v["value"] if v["number"] is not None or not v["value"] else "(text, masked)")
    assert any(v["value"] == "(text, masked)" for v in masked["variables"])


def test_summary_names_shared():
    names = SummaryNames(["A run", "B run"])
    assert names.result("B run") == "Run2" and names.result("C run") == "Result1"
    assert names.folder("/x/y") == names.folder("/x/y") == "Folder1"
    assert names.file("A run.UFol") == "File1.UFol"
    assert names.text("Result: /x/y/A run User Default") == "Result: Folder1/Run1 User Default"
    # a run named like a label: replaced once, the label it gets is not replaced again
    assert names.text("Result: /x/y/Folder1 User Default") == "Result: Folder1/Run3 User Default"
