import glob
import os
import re
import struct
import zipfile
from collections import OrderedDict

import numpy as np
import pytest

from pycorn import PcUni6, open_run
from pycorn.axes import is_axes_warning
from pycorn.bundle_codecs import NrbfError, decode_nrbf, read_nested_zip, trim_zip_padding
from pycorn.sources import zip_source

SAMPLE = os.path.join("..", "samples", "sample.zip")

_HEADER = b"\x00\x01\x00\x00\x00\xff\xff\xff\xff\x01\x00\x00\x00\x00\x00\x00\x00"


def _nrbf_floats(values):
    return (_HEADER + b"\x0f\x01\x00\x00\x00" + struct.pack("<iB", len(values), 11)
            + struct.pack("<%df" % len(values), *values) + b"\x0b")


def _nrbf_string(text):
    data = text.encode("utf-8")
    length = len(data)
    prefix = b""
    while True:
        byte = length & 0x7F
        length >>= 7
        prefix += bytes([byte | (0x80 if length else 0)])
        if not length:
            break
    return _HEADER + b"\x06\x01\x00\x00\x00" + prefix + data + b"\x0b"


@pytest.fixture(scope="module")
def run():
    return open_run(SAMPLE)


def test_decode_float_array_keeps_every_value():
    values = [float(i) for i in range(30)]
    assert decode_nrbf(_nrbf_floats(values)) == values
    assert decode_nrbf(_nrbf_floats([])) == []


def test_decode_strings():
    assert decode_nrbf(_nrbf_string("")) == ""
    long_text = "<a>" + "°" * 200 + "</a>"  # multi-byte length prefix, non-ASCII
    assert decode_nrbf(_nrbf_string(long_text)) == long_text


@pytest.mark.parametrize("blob", [
    b"",
    b"<not nrbf>",
    _nrbf_floats([1.0])[:-1],  # no MessageEnd
    _nrbf_floats([1.0]) + b"\x00",  # trailing bytes
    _nrbf_floats([1.0, 2.0])[:-5] + b"\x0b",  # array shorter than its length
])
def test_decode_rejects_malformed(blob):
    with pytest.raises(NrbfError):
        decode_nrbf(blob)


def test_nested_zip_padding():
    with zipfile.ZipFile(SAMPLE) as bundle:
        raw = bundle.read("Chrom.1_1_True")
    assert raw.endswith(b"\x00" * 16)
    assert len(trim_zip_padding(raw)) < len(raw)
    members = read_nested_zip(raw)
    assert set(members) == {"CoordinateData.Volumes", "CoordinateData.VolumesDataType",
                            "CoordinateData.Amplitudes", "CoordinateData.AmplitudesDataType"}
    assert read_nested_zip(b"") is None
    assert read_nested_zip(b"<xml/>") is None


def test_every_entry_is_kept(run):
    with zipfile.ZipFile(SAMPLE) as bundle:
        names = bundle.namelist()
        assert list(run.entries) == names
        for name in names:
            assert run.entries[name].raw == bundle.read(name)
    assert all(e.error is None for e in run.entries.values())
    # decoding warns about nothing; the CV note is about the column valve (tests/test_axes.py)
    assert [w for w in run.warnings if not is_axes_warning(w)] == []
    assert len(run.manifest) == 37
    assert {e.kind for e in run.entries.values()} == {"xml", "zip", "empty"}


def test_curves_have_full_data_and_axes(run):
    chrom = run.chromatogram()
    assert chrom.name == "Chrom.1" and chrom.chromatogram_id == 6670
    assert len(chrom.curves) == 20
    uv = chrom.curves["UV 1_280"]
    assert (uv.data_type, uv.unit, uv.curve_number) == ("UV", "mAU", 1)
    assert len(uv.values) == len(uv.volume) == len(uv.time) == 22597
    assert uv.time[0] == pytest.approx(0.001666667)
    assert uv.time[-1] == pytest.approx(37.6617, abs=1e-3)
    assert uv.volume[-1] == pytest.approx(15.77019, abs=1e-5)
    assert uv.column_volume == 0.7 and uv.column_volume_unit == "ml"
    assert uv.uv_info["UVPathLength"] == pytest.approx(2.079998)
    assert uv.uv_info["IsUVValuesNormalizedToNominalUVPathLength"] is True
    assert chrom.curves["Cond temp"].unit == "°C"
    precisions = {n: c.precision for n, c in chrom.curves.items()}
    assert (precisions["UV 1_280"], precisions["Cond"], precisions["% Cond"]) == (3, 2, 1)
    assert all(isinstance(p, int) for p in precisions.values())
    # values are stored unrounded: UV has digits beyond its 3 decimals
    assert np.abs(uv.values - np.round(uv.values, 3)).max() > 1e-4
    run_end = chrom.event_curves["Run Log"].events[-1].time
    for curve in chrom.curves.values():
        assert curve.values.dtype == np.float32
        assert len(curve.time) == len(curve.volume) == len(curve.values)
        assert np.all(np.diff(curve.volume) >= 0)
        # every curve runs to the end of the method (sampling intervals up to 2 s)
        assert curve.time[-1] == pytest.approx(run_end, abs=0.04)
    assert uv.to_frame().shape == (22597, 7)  # time, volume, both adjusted, cv, utc, value


def test_events_and_peaks(run):
    chrom = run.chromatogram()
    assert [(n, ec.type, len(ec.events)) for n, ec in chrom.event_curves.items()] == [
        ("Fraction", "Fraction", 2), ("Injection", "Injection", 1), ("Run Log", "Logbook", 85)]
    injection = chrom.event_curves["Injection"].events[0]
    assert (injection.time, injection.volume) == (6.365, 13.57932)
    log = chrom.event_curves["Run Log"].to_frame()
    assert log["text"].iloc[-1] == "End 3/10/2023 5:07:51 PM -05:00 (Completed)"
    assert len(chrom.peak_tables) == 1
    table = chrom.peak_tables[0]
    assert table.curve_number == 1 and table.info["ColumnVolume"] == 0.7
    assert len(table.peaks) == 20 and table.peaks[0]["Area"] == pytest.approx(3.3915)
    assert table.to_frame().shape[0] == 20
    assert isinstance(chrom.info["ChromatogramLayoutXml"], dict)


def test_result_and_metadata(run):
    assert run.name == "Run1"  # samples/sample.zip is anonymized (tools/anonymize_zip.py)
    assert run.created == "2023-03-10T16:30:07.331"
    assert run.unicorn_version == "7.3.0.473"
    assert run.result["RunIndex"] == 6
    info = {ri["type"]: ri["value"] for ri in run.result["run_information"]}
    assert info["ColumnInformation"]["usedColumns"]["column"]["volume"] == 0.7
    assert info["InstrumentServerUNICORNVersion"] == "7.3.0.473"
    assert info["RunStartDate"] == "03/10/2023 16:30:07 -05:00"
    col_volume = [c for c in run.result["search_criteria"] if c["Keyword1"] == "ColVolume"][0]
    assert col_volume["Keyword2"] == "0.700"
    assert set(run.metadata) == {
        "CalibrationSettingData", "ColumnIndividualData", "ColumnTypeData", "EvaluationProcedureData",
        "InstrumentConfigurationData", "MethodData", "MethodDocumentationData", "NextBufferPrepData",
        "NextFracData", "ReportFormatData", "StrategyData", "SystemData", "SystemSettingData",
        "VersionInformationData"}
    assert run.metadata["MethodData"]["Method"]["@FormatVersion"] == "8"
    assert run.metadata["StrategyData"]["StrategyInformation"]["StrategyName"] == "AKTApure25_105"
    assert run.metadata["ColumnIndividualData"] is None
    assert len(run.evaluation_log) == 1
    assert len(run.evaluation_log[0]["AuditTrailEntries"]) == 19


def test_retention_zero_matches_unicorn_layout(run):
    chrom = run.chromatogram()
    # layout: AdjustRetentionZeroToInjectionNumber = true, InjectionNumberForRetentionAdjustment = 1
    assert chrom.retention_zero == {"adjusted": True, "injection_number": 1, "time": 6.365,
                                    "volume": 13.57932, "source": "layout"}
    uv = chrom.curves["UV 1_280"]
    assert uv.volume_adjusted == pytest.approx(uv.volume.astype(float) - 13.57932)
    assert uv.time_adjusted == pytest.approx(uv.time - 6.365)
    # pre-flow setup points: kept, at the same (negative) volume, at increasing times
    setup = uv.volume == 0
    assert setup.sum() == 28
    assert np.all(uv.volume_adjusted[setup] == -13.57932)
    assert np.all(np.diff(uv.time_adjusted[setup]) > 0)
    injection = chrom.event_curves["Injection"].events[0]
    assert (injection.time_adjusted, injection.volume_adjusted) == (0.0, 0.0)
    assert chrom.event_curves["Run Log"].events[0].volume_adjusted == -13.57932


def test_peaks_match_unicorn_peak_table(run):
    """UNICORN's peak table is in the injection-adjusted frame; our axis must reproduce it."""
    chrom = run.chromatogram()
    table = chrom.peak_tables[0]
    assert table.info["ZeroAdjustedToInjectionNumber"] == chrom.retention_zero["injection_number"]
    curve = [c for c in chrom.curves.values() if c.curve_number == table.curve_number][0]
    x = curve.volume_adjusted
    for peak in table.peaks:
        # UNICORN's peak maximum lies within one point spacing of a stored point on our axis
        j = np.argmin(np.abs(x - peak["MaxPeakRetention"]))
        spacing = max(x[j + 1] - x[j], x[j] - x[j - 1])
        assert abs(x[j] - peak["MaxPeakRetention"]) <= spacing
        # UNICORN places the maximum between stored points; the raw curve interpolated there
        # gives its height (within 0.01% on this sample)
        raw_height = np.interp(peak["MaxPeakRetention"], x, curve.values)
        assert raw_height == pytest.approx(peak["Height"], rel=1e-3)


def test_res3_view_matches_pcuni6(run):
    legacy = PcUni6(SAMPLE)
    legacy.load_all_xml()
    assert run.to_res3_dict() == {"Chrom.1": legacy["Chrom.1"]}


def test_open_run_rejects_other_files(tmp_path):
    with pytest.raises(ValueError):
        open_run("result.res")
    path = tmp_path / "photos.zip"
    with zipfile.ZipFile(path, "w") as bundle:
        bundle.writestr("readme.txt", "hello")
    with pytest.raises(ValueError, match="photos.zip: not a UNICORN result"):
        open_run(str(path))


def test_png_entry_is_kept_without_padding(tmp_path):
    png = b"\x89PNG\r\n\x1a\n" + b"\x00\x00\x00\x0dIHDR" + b"\x01" * 17 + b"\x00\x00\x00\x00IEND\xaeB`\x82"
    path = tmp_path / "bundle.zip"
    with zipfile.ZipFile(path, "w") as bundle:
        bundle.writestr("Result.xml", "<Result></Result>")
        bundle.writestr("NextFracData", png + b"\x00" * 100)
    run = open_run(str(path))
    entry = run.entries["NextFracData"]
    assert (entry.kind, entry.value, entry.error) == ("png", png, None)
    assert entry.raw == png + b"\x00" * 100
    assert run.metadata["NextFracData"] == png
    assert not [w for w in run.warnings if "NextFracData" in w]


# Real results from UNICORN 7.3 and 7.11 (gitignored; tests skip when absent)
TEST_FILES = os.path.join("..", "unicorn_test_files")


def _bundles(version):
    paths = sorted(glob.glob(os.path.join(TEST_FILES, version, "*.zip")))
    if not paths:
        pytest.skip("no UNICORN %s test files in %s" % (version, TEST_FILES))
    return paths


@pytest.mark.parametrize("version", ["U7.3", "U7.11"])
def test_real_bundles_decode_without_warnings(version):
    for path in _bundles(version):
        run = open_run(path)
        assert run.unicorn_version.startswith(version[1:] + ".")
        assert all(e.error is None for e in run.entries.values())
        assert {e.kind for e in run.entries.values()} <= {"xml", "zip", "png", "empty"}
        # the only expected decoding warning: results with no retention-zero setting at all
        # (Vc/CV/UTC notes are checked in tests/test_axes.py)
        assert [w for w in run.warnings if "no retention-zero setting" not in w and not is_axes_warning(w)] == []
        for curve in run.chromatogram().curves.values():
            assert len(curve.volume) == len(curve.values) > 0


def test_volume_grid_curves_from_7_11_evaluation():
    found = 0
    for path in _bundles("U7.11"):
        chrom = open_run(path).chromatogram()
        for curve in chrom.curves.values():
            if curve.isochrone != "Volume":
                continue
            found += 1
            assert curve.time is None and curve.is_original is False
            assert curve.xml["DerivedCurveOrigin"]["CurveOperation"] == "Baseline"
            step = float(curve.xml["DistanceBetweenPoints"])
            assert curve.volume[0] == pytest.approx(float(curve.xml["DistanceToStartPoint"]))
            assert np.diff(curve.volume) == pytest.approx(step)
            # the baseline spans the integration window, which is in the injection-adjusted frame
            window = chrom.info["AnalysisSettings"]["ChromatogramAnalysisSettings"]["IntegrationSettings"]
            assert curve.volume_adjusted[0] == pytest.approx(window["WindowStart"], abs=1e-4)
            assert curve.volume_adjusted[-1] == pytest.approx(window["WindowEnd"], abs=step)
    if not found:
        pytest.skip("no volume-grid curves in the 7.11 test files")


def test_retention_zero_from_7_11_ui_settings_matches_layout():
    for path in _bundles("U7.11"):
        chrom = open_run(path).chromatogram()
        ui = chrom.info.get("UISettings")
        if ui is None:
            continue
        # same chromatogram with the layout setting removed falls back to the UI settings
        info = dict(chrom.info)
        info.pop("ChromatogramLayoutXml", None)
        stripped = type(chrom)(chrom.name, chrom.chromatogram_id, info, chrom.curves,
                               chrom.event_curves, [], chrom.pool_tables)
        warnings = []
        zero = zip_source._retention_zero(stripped, warnings)
        assert zero["source"] == "ui_settings" and warnings == []
        assert {k: zero[k] for k in ("adjusted", "injection_number", "time", "volume")} == \
               {k: chrom.retention_zero[k] for k in ("adjusted", "injection_number", "time", "volume")}


def _run_key(path):
    name = re.sub(r"(_U7_11| 001)\.zip$", "", os.path.basename(path))
    return re.sub(r"\s+", "", name).lower()


def test_same_run_exported_from_7_3_and_7_11_has_identical_data():
    old = {_run_key(p): p for p in _bundles("U7.3")}
    new = {_run_key(p): p for p in _bundles("U7.11")}
    common = sorted(set(old) & set(new))
    if not common:
        pytest.skip("no run exported from both versions")
    for key in common:
        a, b = open_run(old[key]).chromatogram(), open_run(new[key]).chromatogram()
        for name, curve in a.curves.items():
            other = b.curves[name]
            assert np.array_equal(curve.volume, other.volume) and np.array_equal(curve.values, other.values)
            assert np.array_equal(curve.time, other.time) and curve.unit == other.unit
        for name, ec in a.event_curves.items():
            assert [(e.time, e.volume, e.text) for e in ec.events] == \
                   [(e.time, e.volume, e.text) for e in b.event_curves[name].events]


# UNICORN text exports (.asc) next to their bundle, used to check against UNICORN's own output.
# Row 1: curve names; row 2: x unit, y unit; then x/y column pairs, each curve its own length.

def _read_asc(path):
    with open(path, encoding="utf-8-sig") as f:
        lines = f.read().splitlines()
    names, units = lines[1].split("\t"), lines[2].split("\t")
    columns = {}
    for j in range(0, len(names) - 1, 2):
        if not names[j]:
            continue
        rows = [line.split("\t") for line in lines[3:]]
        pairs = [(r[j], r[j + 1]) for r in rows if j + 1 < len(r) and r[j] != ""]
        columns[names[j]] = (units[j], units[j + 1], [p[0] for p in pairs], [p[1] for p in pairs])
    return columns


def _asc_exports():
    paths = sorted(p for p in glob.glob(os.path.join(TEST_FILES, "*", "*.asc"))
                   if os.path.exists(p[:-4] + ".zip"))
    if not paths:
        pytest.skip("no .asc exports with a matching bundle in %s" % TEST_FILES)
    return [(p, _read_asc(p), open_run(p[:-4] + ".zip").chromatogram()) for p in paths]


def _asc_x_unit(columns):
    return next(xu for xu, _, _, _ in columns.values() if xu)


def test_asc_events_match_bundle():
    for path, columns, chrom in _asc_exports():
        x_unit = _asc_x_unit(columns)
        # an export holds the curves picked in UNICORN; derived curves are never exported
        assert set(columns) <= set(chrom.curves) | set(chrom.event_curves)
        assert all(chrom.curves[n].is_original for n in columns if n in chrom.curves)
        for name, ec in chrom.event_curves.items():
            if name not in columns:
                continue
            _, _, xs, _ = columns[name]
            # event positions in the export are on the method axis (not retention-adjusted)
            expected = [e.time if x_unit == "min" else e.volume for e in ec.events]
            assert np.array(xs, float) == pytest.approx(expected, abs=1e-5)


def test_asc_time_export_matches_raw_curves():
    """
    A time-based export holds the stored samples: a leading point (t0, y0), then sample i at
    t0 + (i + 1) * step, i.e. at the end of its interval (the bundle's own time/volume pairs
    put it at t0 + i * step). The last one or two samples can be omitted. The x axis is
    zeroed at the first injection even when the bundle stores no retention setting.
    """
    checked = 0
    for path, columns, chrom in _asc_exports():
        if _asc_x_unit(columns) != "min":
            continue
        # the export's zero is our adjusted axis (here from the injection-1 default)
        assert chrom.retention_zero["adjusted"] and chrom.retention_zero["injection_number"] == 1
        for name, curve in chrom.curves.items():
            if name not in columns:
                continue
            x_unit, y_unit, xs, ys = columns[name]
            assert y_unit == (curve.unit or "")
            step = float(curve.xml["DistanceBetweenPoints"])
            x = np.r_[curve.time_adjusted[0], curve.time_adjusted + step]
            y = np.r_[curve.values[0], curve.values].astype(float)
            n = len(xs)
            assert len(x) - 2 <= n <= len(x)
            assert np.array(xs, float) == pytest.approx(x[:n], abs=6e-4)  # printed with 3 decimals
            assert np.array(ys, float) == pytest.approx(y[:n], rel=1e-6, abs=1e-6)  # 7 significant digits
            checked += 1
    if not checked:
        pytest.skip("no time-based .asc export")


def test_asc_volume_export_is_resampled_not_raw():
    """
    A volume-based export is not the stored data: each curve is resampled to the same number
    of points on an evenly spaced volume axis, as if the flow had been constant from 0 to the
    final volume (zeroed at the first injection), ignoring the actual flow profile.
    """
    checked = 0
    for path, columns, chrom in _asc_exports():
        if _asc_x_unit(columns) != "ml":
            continue
        injection = chrom.event_curves_of_type("Injection")[0].events[0]
        end = chrom.event_curves_of_type("Logbook")[0].events[-1]
        rate = end.volume / end.time
        for name, curve in chrom.curves.items():
            if name not in columns:
                continue
            _, _, xs, ys = columns[name]
            # an even grid (to the printed 3 decimals) whose spacing is the time step at the
            # average flow rate, starting one spacing after volume 0
            x = np.array(xs, float)
            index = np.arange(len(x))
            slope, intercept = np.polyfit(index, x, 1)
            assert np.abs(x - (slope * index + intercept)).max() <= 6e-4
            step = float(curve.xml["DistanceBetweenPoints"])
            assert slope == pytest.approx(step * rate, rel=1e-3)
            assert x[0] + injection.volume == pytest.approx(slope, abs=slope / 2 + 6e-4)
            checked += 1
        uv = chrom.curves_of_type("UV")[0]
        assert not np.allclose(np.array(columns[uv.name][3][1:], float), uv.values[:-1], rtol=1e-3)
    if not checked:
        pytest.skip("no volume-based .asc export")


def test_retention_zero_defaults_to_injection_1_without_a_setting():
    checked = 0
    for path in _bundles("U7.3"):
        run = open_run(path)
        chrom = run.chromatogram()
        if chrom.retention_zero["source"] != "default":
            continue
        injection = zip_source._injection_events(chrom)[0]
        assert chrom.retention_zero == {"adjusted": True, "injection_number": 1, "time": injection.time,
                                        "volume": injection.volume, "source": "default"}
        assert any("zeroed at injection 1" in w for w in run.warnings)
        checked += 1
    if not checked:
        pytest.skip("no 7.3 bundle without a retention-zero setting")


def test_adjust_axes_false_keeps_every_axis_as_stored(run):
    raw = open_run(SAMPLE, adjust_axes=False)
    chrom, default = raw.chromatogram(), run.chromatogram()
    assert chrom.retention_zero == {"adjusted": False, "injection_number": None, "time": 0.0,
                                    "volume": 0.0, "source": "disabled"}
    assert [w for w in raw.warnings if not is_axes_warning(w)] == []
    for name, curve in chrom.curves.items():
        assert np.array_equal(curve.time_adjusted, curve.time)
        assert np.array_equal(curve.volume_adjusted, curve.volume)
        # the stored axes do not depend on the flag
        assert np.array_equal(curve.time, default.curves[name].time)
        assert np.array_equal(curve.volume, default.curves[name].volume)
    for ec in chrom.event_curves.values():
        for e in ec.events:
            assert (e.time_adjusted, e.volume_adjusted) == (e.time, e.volume)
    assert raw.to_res3_dict() == run.to_res3_dict()


def test_import_xml_as_df_adjust_axes_flag():
    from pycorn.utils import import_xml_as_df
    adjusted = import_xml_as_df(SAMPLE, ["Cond", "UV"], axis="time")
    stored = import_xml_as_df(SAMPLE, ["Cond", "UV"], axis="time", adjust_axes=False)
    assert adjusted.index[0] == pytest.approx(0.001666667 - 6.365, abs=1e-3)
    assert stored.index[0] == pytest.approx(0.001666667, abs=1e-3)


def test_lookups_by_type_and_clear_error_for_unknown_names(run):
    chrom = run.chromatogram()
    assert [c.name for c in chrom.curves_of_type("UV")] == ["UV 1_280", "UV 2_295", "UV 3_0"]
    assert chrom.curves_of_type("NotAType") == []
    assert [ec.name for ec in chrom.event_curves_of_type("Logbook")] == ["Run Log"]
    assert chrom.curve("Cond") is chrom.curves["Cond"]
    with pytest.raises(KeyError, match="no curve 'UV_280' in Chrom.1; available: UV 1_280, "):
        chrom.curve("UV_280")


def test_uv_by_type_skips_derived_and_ratio_curves():
    from pycorn.utils import import_xml_as_df
    for path in _bundles("U7.11"):
        chrom = open_run(path).chromatogram()
        derived = [c.name for c in chrom.curves.values() if c.data_type == "UV" and c.is_original is False]
        if not derived:
            continue
        assert not set(derived) & {c.name for c in chrom.curves_of_type("UV")}
        assert derived[0] in {c.name for c in chrom.curves_of_type("UV", original_only=False)}
        df = import_xml_as_df(path, ["UV"], axis="time")
        assert {curve for _, curve in df.columns} == {c.name for c in chrom.curves_of_type("UV")}
        return
    pytest.skip("no 7.11 bundle with a derived UV curve")


def test_import_xml_as_df_names_missing_curves():
    from pycorn.utils import import_xml_as_df
    with pytest.raises(KeyError, match="no curve or role 'UV_280'.*curves: UV 1_280"):
        import_xml_as_df(SAMPLE, ["UV_280"], axis="time")


def _renamed(chrom, renames):
    """Copy of `chrom` with curves renamed, as another instrument might name them."""
    import copy
    other = copy.copy(chrom)
    other.curves = OrderedDict()
    for name, curve in chrom.curves.items():
        curve = copy.copy(curve)
        curve.name = renames.get(name, name)
        other.curves[curve.name] = curve
    return other


def test_roles_find_curves_whatever_their_names(run):
    from pycorn.curve_roles import resolve_curve_keys
    chrom = _renamed(run.chromatogram(), {"Cond": "Conductivity_1", "% Cond": "Cond %", "UV 1_280": "UV_280",
                                          "Conc B": "Conc_B", "pH": "pH 1"})
    assert [c.name for c in chrom.curves_with_role("conductivity")] == ["Conductivity_1"]
    assert [c.name for c in chrom.curves_with_role("conductivity_percent")] == ["Cond %"]
    assert [c.name for c in chrom.curves_with_role("conc_b")] == ["Conc_B"]
    assert [c.name for c in chrom.curves_with_role("ph")] == ["pH 1"]
    assert chrom.curves_with_role("uv")[0].name == "UV_280"
    # the legacy default keys work on the renamed curves
    assert resolve_curve_keys(chrom, ["Cond", "Conc B", "pH"]) == ["Conductivity_1", "Conc_B", "pH 1"]
    # exact names still win, and roles can be asked for directly
    assert resolve_curve_keys(chrom, ["UV_280", "conductivity"]) == ["UV_280", "Conductivity_1"]
    with pytest.raises(KeyError, match="unknown curve role"):
        chrom.curves_with_role("viscosity")


@pytest.mark.parametrize("name", ["Conc B", "Conc_B", "ConcB", "conc b", "Concentration B", "%B"])
def test_conc_b_name_variants(run, name):
    chrom = _renamed(run.chromatogram(), {"Conc B": name})
    assert [c.name for c in chrom.curves_with_role("conc_b")] == [name]


def test_conc_b_is_not_confused_with_other_percent_curves():
    for version in ("U7.3", "U7.11"):
        for path in _bundles(version):
            chrom = open_run(path).chromatogram()
            assert [c.name for c in chrom.curves_with_role("conc_b")] == ["Conc B"]
            assert all(c.unit != "%" for c in chrom.curves_with_role("conductivity"))
            assert len(chrom.curves_with_role("conductivity")) == 1


def test_several_curves_with_a_role_all_come_back(run):
    from pycorn.curve_roles import resolve_curve_keys
    chrom = _renamed(run.chromatogram(), {"Cond": "Cond 1"})
    second = __import__("copy").copy(chrom.curves["Cond 1"])
    second.name = "Cond 2"
    chrom.curves["Cond 2"] = second
    assert resolve_curve_keys(chrom, ["Cond"]) == ["Cond 1", "Cond 2"]


def test_import_xml_as_df_keeps_legacy_column_order():
    from pycorn.utils import import_xml_as_df
    df = import_xml_as_df(SAMPLE, axis="time")
    assert [curve for _, curve in df.columns] == ["Cond", "Conc B", "UV 1_280", "UV 2_295", "UV 3_0"]
