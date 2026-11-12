"""
A synthetic result with several chromatograms (W17), built from the anonymized `samples/sample.zip`
(no data from a database): the run's own chromatogram plus copies that look as UNICORN stores
chromatograms added in Evaluation.

- 'Chrom.1': the original (unchanged).
- IMPORTED: imported from another result: `OriginalResultID` / `OriginalResultName`, every curve
  and event curve non-original, the run log as an `ImportedLogbook` event curve. Like a source
  result that had overlays of its own, it also carries an injection curve of another method run
  (`OTHER_RUN_INJECTION`: another `MethodStartTime`) and a pooled fraction curve
  (`POOLED_FRACTION`, made in the source's evaluation); neither belongs in its tables. Its second
  UV curve is one of another method run (`OTHER_START`): kept as a curve, without a role.
- COPY: imported from this same run (`OriginalResultName` = the run's name; a .zip stores no
  ResultID of its own, so the name tells).
- DERIVED: made in Evaluation: every curve and event curve non-original, no original result.

Each copy has its own entry (`Chrom.<n>.Xml`) and its own curve point files (`Chrom.<n>_...`).
"""
import re
import zipfile
from pathlib import Path

BASE = Path(__file__).parent.parent / "samples" / "sample.zip"
IMPORTED, COPY, DERIVED = "Imported run", "Copy of the run", "Derived"
SOURCE_ID, SOURCE_NAME = 4711, "Other run"
OTHER_RUN_INJECTION, POOLED_FRACTION = "Injection of another run", "Fraction,POOL"
OTHER_START = "2001-02-03T04:05:06.7"  # the method start of the other run (made up)


def _event_curve(text, event_type):
    """The whole `<EventCurve EventCurveType="...">` element of `event_type` in `text`."""
    m = re.search(r'<EventCurve EventCurveType="%s">.*?</EventCurve>' % event_type, text, re.S)
    return m.group(0)


def _renamed(element, name, number):
    element = re.sub(r"<Name>[^<]*</Name>", "<Name>%s</Name>" % name, element, count=1)
    return re.sub(r"<CurveNumber>(\d+)</CurveNumber>(?=<ColumnVolume>)", "<CurveNumber>%d</CurveNumber>" % number,
                  element, count=1)


def _other_start(element):
    return re.sub(r"<MethodStartTime>[^<]*</MethodStartTime>", "<MethodStartTime>%s</MethodStartTime>" % OTHER_START,
                  element, count=1)


def _overlay_curves(text):
    """
    An injection curve of another method run and a pooled fraction curve, added after the stored
    ones, and the second UV curve made one of another method run (its points stay).
    """
    other = _other_start(_renamed(_event_curve(text, "Injection"), OTHER_RUN_INJECTION, 98))
    pooled = _renamed(_event_curve(text, "Fraction"), POOLED_FRACTION, 99)
    last = _event_curve(text, "Logbook")
    text = text.replace(last, last + other + pooled, 1)
    uv = [m.group(0) for m in re.finditer(r'<Curve CurveDataType="UV">.*?</Curve>', text, re.S)][1]
    return text.replace(uv, _other_start(uv), 1)


def _copy_xml(text, number, name, original=None, overlays=False):
    """Chrom.1.Xml as chromatogram `number` called `name`; `original`: (id, name) for an imported one."""
    head = "<ChromatogramName>%s</ChromatogramName>" % name
    if original is not None:
        head += "<OriginalResultID>%s</OriginalResultID><OriginalResultName>%s</OriginalResultName>" % original
    text = text.replace("<ChromatogramName>Chrom.1</ChromatogramName>", head, 1)
    text = re.sub(r"<ChromatogramID>\d+</ChromatogramID>", "<ChromatogramID>%d</ChromatogramID>" % (900 + number),
                  text, count=1)
    text = text.replace("<IsOriginalData>true</IsOriginalData>", "<IsOriginalData>false</IsOriginalData>")
    if overlays:
        text = _overlay_curves(text)
    if original is not None:
        text = text.replace('EventCurveType="Logbook"', 'EventCurveType="ImportedLogbook"')
    return text.replace("Chrom.1_", "Chrom.%d_" % number)


def build(path, base=BASE):
    """Write the synthetic multi-chromatogram result to `path`; returns `path`."""
    with zipfile.ZipFile(base) as z:
        members = [(i.filename, z.read(i)) for i in z.infolist()]
    data = dict(members)
    from pycorn import open_run
    run_name = open_run(str(base)).name
    text = data["Chrom.1.Xml"].decode("utf-8-sig")
    copies = [(2, IMPORTED, (SOURCE_ID, SOURCE_NAME)), (3, COPY, (SOURCE_ID + 1, run_name)), (4, DERIVED, None)]
    # the manifest lists every file: the copies too (each Chrom.1 line again, renamed)
    manifest = data["Manifest.xml"].decode("utf-8-sig")
    lines = re.findall(r"<Details><FileName>Chrom\.1[._][^<]*</FileName>.*?</Details>", manifest)
    added = "".join(line.replace("Chrom.1", "Chrom.%d" % n, 1) for n, _, _ in copies for line in lines)
    data["Manifest.xml"] = manifest.replace("</ExportManifest>", added + "</ExportManifest>").encode("utf-8")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as out:
        for name, _ in members:
            out.writestr(name, data[name])
        for number, name, original in copies:
            xml = _copy_xml(text, number, name, original, overlays=name == IMPORTED)
            out.writestr("Chrom.%d.Xml" % number, xml.encode("utf-8"))
            for member, raw in members:
                if member.startswith("Chrom.1_"):
                    out.writestr(member.replace("Chrom.1_", "Chrom.%d_" % number, 1), raw)
    return path
