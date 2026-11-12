import numpy as np
import pytest
import os

from pycorn import PcUni6
from pycorn.utils import import_xml_as_df


def test_import_utility():
    file_path = os.path.join("..", "samples", "sample.zip")
    # index -14 lies before the first point (volume 0 is -13.579 after the injection offset),
    # where UNICORN draws nothing, so the first row is NaN
    reference_data = np.array([[np.nan, np.nan, np.nan, np.nan, np.nan],
                               [1.69039273e+01, 9.91999893e+01, 7.54344136e-01, 8.22319886e-01, 0.00000000e+00],
                               [1.70131853e+01, 9.91999893e+01, 1.12997290e-02, 2.78141668e-02, 0.00000000e+00],
                               [1.70404669e+01, 9.91999893e+01, -4.40397997e-02, -7.03156144e-03, 0.00000000e+00],
                               [1.70849351e+01, 9.91999893e+01, -3.79689479e-02, -1.14569797e-02, 0.00000000e+00]])

    data = import_xml_as_df(file_path, index=np.arange(-14, 1, 3))
    # ((data - data.min()) / (data.max() - data.min())).plot()
    assert data.values == pytest.approx(reference_data, nan_ok=True)


def test_import_utility_axes():
    file_path = os.path.join("..", "samples", "sample.zip")
    by_volume = import_xml_as_df(file_path)
    assert by_volume.index[0] >= -13.57932 and by_volume.index[-1] <= 2.19087
    assert not by_volume.iloc[0].isna().any()
    by_time = import_xml_as_df(file_path, axis="time")
    # time zero is the injection (6.365 min); the first UV point is 1 s after method start
    assert by_time.index[0] == pytest.approx(0.001666667 - 6.365, abs=1e-3)
    assert by_time[("Chrom.1", "UV 1_280")].iloc[0] == pytest.approx(-0.0016426085494458675)
    with pytest.raises(ValueError):
        import_xml_as_df(file_path, axis="cv")


def test_pcuni6_interface():
    file_path = os.path.join("..", "samples", "sample.zip")
    reference_data = [(0.0, -0.0016426085494458675),
                      (0.0, -0.0016426085494458675),
                      (0.0, -0.0016426085494458675),
                      (0.0, -0.0016426085494458675),
                      (0.0, -0.0016426085494458675),
                      (0.0, -0.002494621090590954),
                      (0.0, -0.0035596368834376335),
                      (0.0, -0.004624652676284313),
                      (0.0, -0.00568966893479228),
                      (0.0, -0.006754684261977673)]
    reference_keys = ['CalibrationSettingData', 'Chrom.1.Xml', 'Chrom.1_10_True', 'Chrom.1_13_True', 'Chrom.1_14_True',
                      'Chrom.1_15_True', 'Chrom.1_16_True', 'Chrom.1_1_True', 'Chrom.1_20_True', 'Chrom.1_21_True',
                      'Chrom.1_23_True', 'Chrom.1_2_True', 'Chrom.1_38_True', 'Chrom.1_3_True', 'Chrom.1_4_True',
                      'Chrom.1_5_True', 'Chrom.1_6_True', 'Chrom.1_7_True', 'Chrom.1_89_True', 'Chrom.1_8_True',
                      'Chrom.1_90_True', 'Chrom.1_9_True', 'ColumnIndividualData', 'ColumnTypeData',
                      'EvaluationLog.xml', 'EvaluationProcedureData', 'InstrumentConfigurationData', 'Manifest.xml',
                      'MethodData', 'MethodDocumentationData', 'NextBufferPrepData', 'NextFracData', 'ReportFormatData',
                      'Result.xml', 'StrategyData', 'SystemData', 'SystemSettingData', 'VersionInformationData',
                      'Chrom.1.Xml_dict']

    xml_data = PcUni6(file_path)
    xml_data.load()
    assert reference_keys == list(xml_data.keys())
    xml_data.load_all_xml()
    uv_data = xml_data["Chrom.1"]["UV 1_280"]["data"]
    # the full array: 22,597 points, of which the first 5 hold the same value
    assert len(uv_data) == 22597
    assert uv_data[:5] == [reference_data[0]] * 5
    assert reference_data == uv_data[5:15]
    assert uv_data[-1] == (15.770191192626953, 0.04752480983734131)


def test_pcuni6_keeps_result_metadata():
    file_path = os.path.join("..", "samples", "sample.zip")
    xml_data = PcUni6(file_path)
    xml_data.load_all_xml()
    xml_data.clean_up()  # a second clean_up is harmless
    assert list(xml_data.keys()) == ['Chrom.1.Xml_dict', 'Chrom.1']
    assert xml_data.date == "2023-03-10"

    full = PcUni6(file_path)
    full.load_all_xml(keep_all=True)
    assert full["Chrom.1"]["UV 1_280"]["data"] == xml_data["Chrom.1"]["UV 1_280"]["data"]
    assert full["Result.xml_dict"]["Result"]["RunIndex"] == "6"
    assert full["MethodData"]["Xml"]["Method"]["@UNICORNVersion"] == "7.3.0.473"
    assert full["ColumnIndividualData"]["Xml"] is None
    assert full["Chrom.1"]["Cond temp"]["unit"] == "°C"
