from pathlib import Path

import numpy as np
import pandas as pd

from pycorn import PcUni6
from pycorn.curve_roles import resolve_curve_keys
from pycorn.sources import open_run

# curve names PcUni6 / Run.to_res3_dict() rename
_RES3_RENAMES = {"xUV cell path length": "UV cell path length"}


def get_series_from_data_dict(data_dictionary, target_key, data_key_list):
    """
    Curves of a res3-like chromatogram dict (`PcUni6` after `load_all_xml()`) as DataFrame columns.

    The volume axis is shifted so that zero is the last injection in the chromatogram's
    `Injection` event curve (no shift if there is none). Points sharing a volume are
    merged and the first is kept.

    Parameters
    ----------
    data_dictionary : dict
        A loaded `PcUni6` (or dict of the same form).
    target_key : str
        Chromatogram key, e.g. "Chrom.1".
    data_key_list : list of str
        Curve names in the chromatogram; one column each, named after the curve.

    Returns
    -------
    pd.DataFrame
    """
    try:
        # select the last injection as the injection timestamp
        inject_timestamp = data_dictionary[target_key]["Injection"]["data"][-1][0]
    except KeyError:
        inject_timestamp = 0

    data_series_list = []
    for data_key in data_key_list:
        data_array = np.array(data_dictionary[target_key][data_key]["data"]).astype(float)
        data_series = pd.Series(data=data_array[:, 1], index=data_array[:, 0])
        # remove duplicates
        data_series = data_series[~data_series.index.duplicated()]
        # offset by the injection timestamp
        data_series.index -= inject_timestamp

        data_series_list.append(data_series)

    df = pd.concat(data_series_list, axis=1)
    df.columns = data_key_list
    return df


def get_series_from_chromatogram(chromatogram, data_key_list, axis="volume"):
    """
    Curves of a `pycorn.model.Chromatogram` as DataFrame columns, on the injection-adjusted
    axis UNICORN plots (zero at the injection in `chromatogram.retention_zero`).

    On the volume axis, points that share a volume (e.g. setup points recorded before the
    flow starts) are merged and the first is kept; the time axis keeps every point.
    """
    data_series_list = []
    for data_key in data_key_list:
        curve = chromatogram.curve(_RES3_RENAMES.get(data_key, data_key))
        x = curve.volume_adjusted if axis == "volume" else curve.time_adjusted
        if x is None:
            raise ValueError("%s/%s has no injection-adjusted %s axis" % (chromatogram.name, curve.name, axis))
        data_series = pd.Series(data=curve.values.astype(float), index=x)
        data_series = data_series[~data_series.index.duplicated()]
        data_series_list.append(data_series)

    df = pd.concat(data_series_list, axis=1)
    df.columns = data_key_list
    return df


def import_xml_as_df(file_path: (str | Path), data_key_list: list = None, index: np.ndarray = None,
                     axis: str = "volume", adjust_axes: bool = True) -> pd.DataFrame:
    """
    Import the contents of a Unicorn zip file into a pd.Dataframe

    The x axis matches UNICORN's chromatogram: zero at the injection the result's layout
    adjusts retention to (`Chromatogram.retention_zero`; injection 1 if it stores no setting), so
    data before the injection has negative values. Index points outside the recorded data are NaN.

    Chromatograms with UV data whose names contain "Cond" plus an experiment keyword
    (Tracer, Injection, Chrom, Breakthrough, Elution) are used; if any is a breakthrough,
    only the breakthrough ones. Without a match, every chromatogram with UV data is used.
    With several data keys the columns are (chromatogram, curve) tuples; with one key
    they are chromatogram names.

    Parameters
    ----------
    file_path : str or Path, Path to the zip file.
    data_key_list: list, optional, Keys to include in the DataFrame. Default: ["Cond", "UV", "Conc B"]
        Each key is a curve name, a role ("conductivity", "uv", "conc_b", ...) or a legacy key
        ("Cond", "UV", "Conc B", "pH") standing for a role; a role expands to every matching
        curve, so instruments that name their curves differently work too.
    index: np.ndarray, optional, Array of shape (1, ), to be used as index in the returned pd.DataFrame.
        Units are the same as the original data.
    axis: str, optional, "volume" (default) or "time". On the volume axis, points recorded at the
        same volume (e.g. before the flow starts) are merged; use "time" or `pycorn.open_run` to
        keep every point.
    adjust_axes: bool, optional, False keeps the x axis exactly as stored (zero at method start).

    Returns
    -------
    dataframe : pd.DataFrame or None
        None if the file has no chromatogram with UV data.

    """
    if data_key_list is None:
        data_key_list = ["Cond", "UV", "Conc B"]
    if axis not in ("volume", "time"):
        raise ValueError("axis must be 'volume' or 'time'")

    run = open_run(file_path, adjust_axes=adjust_axes)

    # curve names vary between instruments; find UV data by CurveDataType, not by name
    chromatogram_keys = [key for key, chromatogram in run.chromatograms.items()
                         if chromatogram.curves_of_type("UV")]
    target_key_list = [
        key for key in chromatogram_keys
        if "Cond" in key
           and any(s.lower() in key.lower() for s in ["Tracer", "Injection", "Chrom", "Breakthrough", "Elution"])
    ]
    if len(target_key_list) == 0:
        # chromatograms with default names (e.g. "Chrom.1") match none of the experiment keywords
        target_key_list = chromatogram_keys

    if any("breakthrough" in key.lower() for key in target_key_list):
        target_key_list = [key for key in target_key_list if "breakthrough" in key.lower()]

    if len(target_key_list) == 0:
        return None

    # Keys are curve names, roles or legacy keys ("Cond", "UV", "Conc B") standing for roles, so the
    # same call works whatever the instrument names its curves; see `pycorn.curve_roles`.
    # The UV curves go last, as they always have.
    first = run.chromatograms[target_key_list[0]]
    uv_last = "UV" in data_key_list and "UV" not in first.curves
    data_key_list = resolve_curve_keys(first, [k for k in data_key_list if not (uv_last and k == "UV")])
    if uv_last:
        data_key_list += [n for n in resolve_curve_keys(first, ["UV"]) if n not in data_key_list]

    series_list = [get_series_from_chromatogram(run.chromatograms[chrom], data_key_list, axis)
                   for chrom in target_key_list]

    if index is None:
        # round the ends inwards so the first and last points lie within the data
        start = np.ceil(series_list[0].index.min() * 1000) / 1000
        stop = np.floor(series_list[0].index.max() * 1000) / 1000
        index = np.linspace(start, stop, 100).round(3)

    # align and unify the index
    # drop index points at or beyond the end of the first chromatogram's data
    index = index[index < series_list[0].index.max()]

    if len(data_key_list) > 1:
        column_names = [(target_key, data_key) for target_key in target_key_list for data_key in data_key_list]
        column_names += [("temporary_insert", 0)]
    else:
        column_names = target_key_list
        column_names += ["temporary_insert"]
    # the all-NaN "temporary_insert" column carries the index into the outer join, so
    # every index point gets a row to interpolate into; it is dropped again below

    # combine datapoints into one DataFrame and interpolate onto the unified index
    series_list.append(pd.Series(data=np.nan, index=index))
    dataframe = pd.concat(series_list, axis=1, join="outer")
    dataframe.columns = column_names
    # interpolate between recorded points only: UNICORN draws nothing outside the data
    dataframe = dataframe.sort_index().interpolate("index", limit_area="inside")
    dataframe = dataframe.loc[index, column_names[:-1]]

    return dataframe
