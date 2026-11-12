import numpy as np
import pytest

from pycorn import resample


def peak_curve(n=20000):
    x = np.linspace(0.0, 100.0, n)
    y = np.exp(-((x - 40.0) / 1.5) ** 2) * 100.0 + 0.01 * np.sin(x * 50)
    return x, y


def test_size_control_is_required_and_exclusive():
    x, y = peak_curve(100)
    with pytest.raises(ValueError, match="exactly one"):
        resample.select_index(x, y)
    with pytest.raises(ValueError, match="exactly one"):
        resample.select_index(x, y, max_points=10, step=1.0)
    with pytest.raises(ValueError, match="at least 2"):
        resample.select_index(x, y, max_points=1)
    with pytest.raises(ValueError, match="method"):
        resample.select_index(x, y, max_points=10, method="nope")
    with pytest.raises(ValueError, match="between 0 and 1"):
        resample.select_index(x, y, max_points=10, derivative_share=1.5)


def test_short_curve_is_returned_whole_and_nan_skipped():
    x = np.arange(10.0)
    y = np.arange(10.0)
    y[3] = np.nan
    idx = resample.select_index(x, y, max_points=50)
    assert list(idx) == [0, 1, 2, 4, 5, 6, 7, 8, 9]


@pytest.mark.parametrize("method", ["interval", "derivative", "minmax"])
def test_ends_and_size(method):
    x, y = peak_curve()
    idx = resample.select_index(x, y, max_points=300, method=method)
    assert idx[0] == 0 and idx[-1] == len(x) - 1
    assert len(idx) <= 303 and np.all(np.diff(idx) > 0)


def test_step_size_gives_points_per_x_unit():
    x, y = peak_curve()
    idx = resample.select_index(x, y, step=1.0)
    assert 95 <= len(idx) <= 103  # 101 grid points, nearest stored point for each


def test_derivative_puts_more_points_on_the_peak_than_interval():
    x, y = peak_curve()
    on_peak = lambda i: np.sum((x[i] > 36) & (x[i] < 44))
    flat = resample.select_index(x, y, max_points=200, method="interval")
    steep = resample.select_index(x, y, max_points=200, method="derivative")
    half = resample.select_index(x, y, max_points=200, method="derivative", derivative_share=0.5)
    assert on_peak(flat) < on_peak(half) < on_peak(steep)
    # share 0 is the interval rule
    zero = resample.select_index(x, y, max_points=200, method="derivative", derivative_share=0.0)
    assert list(zero) == list(flat)


def test_minmax_keeps_extremes():
    x, y = peak_curve()
    idx = resample.select_index(x, y, max_points=400, method='minmax')
    assert resample.compare(x, y, x[idx], y[idx])['peak_height_diff'] == 0.0


def test_compare_reports_differences():
    x = np.linspace(0, 10, 1001)
    y = np.exp(-((x - 5) / 0.3) ** 2)
    ix = np.arange(0, 1001, 50)
    c = resample.compare(x, y, x[ix], y[ix])
    assert c["points"] == 1001 and c["points_thinned"] == len(ix)
    assert c["peak_height_diff"] <= 0 and c["retention_shift"] == 0.0


def test_step_signal_keeps_every_change():
    x = np.arange(1000.0)
    y = np.zeros(1000)
    y[200:500] = 1.0
    y[500:] = 3.0
    idx = resample.select_index(x, y, max_points=50, kind="step")
    assert {0, 199, 200, 499, 500, 999} <= set(idx)
    with pytest.raises(ValueError, match="never dropped"):
        resample.select_index(x, y, max_points=3, kind="step")


def test_common_grid_uniform_inside_records_only():
    t1 = np.linspace(0.0, 10.0, 11)
    t2 = np.array([2.0, 5.0, 9.0])
    flow = np.array([0.0, 0.0, 1.0])
    grid, (a, b) = resample.common_grid([(t1, t1 * 2.0, "continuous"), (t2, flow, "step")], step=1.0)
    assert list(grid) == list(np.arange(11.0))
    assert a[3] == 6.0
    assert np.isnan(b[1]) and b[4] == 0.0 and b[5] == 0.0 and b[8] == 0.0 and b[9] == 1.0
    assert np.isnan(b[10])  # beyond its last stored point: nothing extrapolated
    grid, _ = resample.common_grid([(t1, t1, "continuous")], max_points=5)
    assert len(grid) == 5 and grid[0] == 0.0 and grid[-1] == 10.0
    with pytest.raises(ValueError, match="exactly one"):
        resample.common_grid([(t1, t1, "continuous")])


def test_hybrid_index_keeps_steps_square_and_peaks_high():
    x = np.linspace(0.0, 100.0, 22000)
    y = np.where(x < 30, 0.0, np.where(x < 60, 40.0, 100.0)) + np.exp(-((x - 80) / 1.0) ** 2) * 50
    idx = resample.hybrid_index(x, y, 1000)
    assert len(idx) <= 1000 + 100 + 2 and idx[0] == 0 and idx[-1] == len(x) - 1
    jumps = resample.jump_index(x, y)
    assert len(jumps) == 4 and set(jumps) <= set(idx)  # both steps, both sides
    px, py = x[idx], y[idx]
    assert abs(resample.compare(x, y, px, py)["peak_height_rel"]) < 0.05
    # smooth peak is not a step
    gx, gy = peak_curve(2000)
    assert len(resample.jump_index(gx, gy)) == 0


def test_every_index_keeps_ends_and_steps():
    x = np.arange(1000.0)
    y = np.zeros(1000)
    y[500:] = 5.0
    idx = resample.every_index(x, y, 100)
    assert {0, 100, 999, 499, 500} <= set(idx) and len(idx) <= 16
    with pytest.raises(ValueError, match="at least 1"):
        resample.every_index(x, y, 0)


def test_anchors_are_kept():
    x, y = peak_curve()
    idx = resample.select_index(x, y, max_points=50, anchors=[12.3456])
    assert np.min(np.abs(x[idx] - 12.3456)) < 0.01
