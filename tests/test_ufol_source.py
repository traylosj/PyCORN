import glob
import io
import os
import re
import tarfile
import zipfile

import numpy as np
import pytest

from pycorn import open_folder, open_run
from pycorn.axes import is_axes_warning
from pycorn.bundle_codecs import read_nested_zip

SAMPLE = os.path.join("..", "samples", "sample.zip")
TEST_FILES = os.path.join("..", "unicorn_test_files")


def _add(archive, name, data=None):
    info = tarfile.TarInfo(name)
    if data is None:
        info.type = tarfile.DIRTYPE
        archive.addfile(info)
    else:
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))


@pytest.fixture
def ufol(tmp_path):
    """A folder export laid out as UNICORN writes it, holding sample.zip twice."""
    with open(SAMPLE, "rb") as f:
        bundle = f.read()
    path = tmp_path / "Service.UFol"
    with tarfile.open(path, "w", format=tarfile.USTAR_FORMAT) as archive:
        _add(archive, "Service/")
        _add(archive, "Service/Run A 001.2.Result", bundle)
        _add(archive, "Service/Sub/")
        _add(archive, "Service/Sub/Run B 001.17.Result", bundle)
        _add(archive, "systemnames.xml", b'<?xml version="1.0" encoding="utf-8"?><ArrayOfString />')
    (tmp_path / "Service.UFol.export.txt").write_text(
        "Export summary\n"
        "Exported:\tFolder\tOriginal path: /DefaultHome/Service\tExported path: Service\tOriginal id: 4\n"
        "Exported:\tResult\tOriginal path: /DefaultHome/Service/Run A 001\t"
        "Exported path: Service/Run A 001\tOriginal id: 2\n", encoding="utf-8")
    return str(path)


def test_folder_export_results_read_like_the_zip(ufol):
    runs = open_folder(ufol)
    reference = open_run(SAMPLE)
    assert [r.container["member"] for r in runs] == ["Service/Run A 001.2.Result",
                                                     "Service/Sub/Run B 001.17.Result"]
    a, b = runs
    assert (a.source, a.locator) == ("ufol", ufol + "/Service/Run A 001.2.Result")
    assert dict(a.container) == {"member": "Service/Run A 001.2.Result", "folder": "Service",
                                 "name": "Run A 001", "original_id": 2,
                                 "original_path": "/DefaultHome/Service/Run A 001"}
    assert dict(a.origin) == {"file_name": "Service.UFol", "path": os.path.abspath(ufol),
                              "member": "Service/Run A 001.2.Result"}
    # not in the summary: no original path
    assert (b.container["folder"], b.container["original_id"], b.container["original_path"]) == \
           ("Service/Sub", 17, None)
    for run in runs:
        assert list(run.entries) == list(reference.entries)
        assert run.warnings == reference.warnings
        for name, curve in reference.chromatogram().curves.items():
            assert np.array_equal(run.chromatogram().curves[name].values, curve.values)
    assert reference.container is None


def test_folder_export_without_summary(ufol):
    os.remove(ufol + ".export.txt")
    assert [r.container["original_path"] for r in open_folder(ufol)] == [None, None]


def test_open_run_and_open_folder_reject_each_others_files(ufol):
    with pytest.raises(ValueError, match="open_folder"):
        open_run(ufol)
    with pytest.raises(ValueError):
        open_folder(SAMPLE)


def test_result_member_opens_with_open_run(ufol, tmp_path):
    with tarfile.open(ufol) as archive:
        data = archive.extractfile("Service/Run A 001.2.Result").read()
    path = tmp_path / "Run A 001.2.Result"
    path.write_bytes(data)
    assert list(open_run(str(path)).entries) == list(open_run(SAMPLE).entries)


def test_amplitude_only_time_grid_curve(tmp_path):
    """
    Curves made in evaluation on a time grid (e.g. UV ratio curves) can store only amplitudes
    and an empty VolumeUnit: time comes from the grid, there is no volume axis, no warning.
    """
    with zipfile.ZipFile(SAMPLE) as src:
        items = [(i, src.read(i)) for i in src.infolist()]
    chrom = open_run(SAMPLE).chromatogram()
    curve = chrom.curves_of_type("UV")[0]
    point_file = curve.point_sets[0].file_name
    path = tmp_path / "bundle.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as dst:
        for info, data in items:
            if info.filename == point_file:
                # keep the stored Amplitudes member, drop Volumes
                buf = io.BytesIO()
                with zipfile.ZipFile(buf, "w") as out:
                    for name, member in read_nested_zip(data).items():
                        if "Volumes" not in name:
                            out.writestr(name, member)
                data = buf.getvalue()
            elif info.filename == chrom.name + ".Xml":
                text = data.decode("utf-8-sig")
                start = text.index("<Name>%s</Name>" % curve.name)
                end = text.index("</Curve>", start)
                block = re.sub(r"<VolumeUnit>[^<]*</VolumeUnit>", "<VolumeUnit></VolumeUnit>",
                               text[start:end])
                data = (text[:start] + block + text[end:]).encode("utf-8")
            dst.writestr(info.filename, data)
    run = open_run(str(path))
    changed = run.chromatogram().curves[curve.name]
    assert changed.volume is None and changed.volume_adjusted is None
    assert np.array_equal(changed.time, curve.time)
    assert np.array_equal(changed.values, curve.values)
    assert run.warnings == open_run(SAMPLE).warnings
    assert curve.name not in run.to_res3_dict()[chrom.name]


# Real folder exports (gitignored; tests skip when absent)

def test_real_folder_exports():
    paths = sorted(glob.glob(os.path.join(TEST_FILES, "UFOL", "*.UFol")))
    if not paths:
        pytest.skip("no .UFol test files in %s" % TEST_FILES)
    for path in paths:
        runs = open_folder(path)
        assert runs
        for run in runs:
            assert run.container["original_id"] is not None
            assert run.container["original_path"].endswith("/" + run.container["folder"] + "/" + run.name)
            assert all(e.error is None for e in run.entries.values())
            # the only expected decoding warning: results with no retention-zero setting at all
            # (Vc/CV/UTC notes are checked in tests/test_axes.py)
            assert [w for w in run.warnings
                    if "no retention-zero setting" not in w and not is_axes_warning(w)] == []
            for curve in run.chromatogram().curves.values():
                if curve.volume is None:
                    assert curve.isochrone == "Time" and not curve.volume_unit and not curve.is_original
                    assert len(curve.time) == len(curve.values) > 0
                else:
                    assert len(curve.volume) == len(curve.values) > 0
