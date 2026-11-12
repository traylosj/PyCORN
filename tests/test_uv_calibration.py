"""
`pycorn.uv_calibration` (GitHub issue #1): the extinction coefficient from sample sent to the
column by-pass. Synthetic runs: 2 ml/min, by-pass from 0.5 min, valve at Inject 1-2 min.
"""
import math

import pytest

from pycorn.concentration import ACCURACY_NOTE
from pycorn.uv_calibration import bypass_uv_readings, epsilon_from_injection

from test_run_log_sections import _curve, _e, _run

UV_INFO = {"IsUVValuesNormalizedToNominalUVPathLength": True, "UVPathLength": 2.01, "NominalUVPathLength": 2}
SIGMA_MIN = 0.02  # pulse width; the curve's volume axis is its time axis (1 ml per min)


def _events(bypass=True):
    events = [_e(0.0, "Method Run x", subtype="BlockStart", status=""), _e(0.0, "System flow 2.000 {ml/min} Off"),
              _e(0.0, "Gradient 0.0 {%B} 0.00 {min}"), _e(0.0, "Wavelength 280 {nm} 0 {nm} 260 {nm}")]
    if bypass:
        events.append(_e(0.5, "Column position By-pass Down flow"))
    events += [_e(1.0, "Injection valve Inject"), _e(2.0, "Injection valve Manual load")]
    if bypass:
        events.append(_e(2.5, "Column position Position 1 Down flow"))
    return events + [_e(4.0, "End 1/1/2024", status="")]


def _uv(fn):
    c = _curve("UV 1_280", fn, "mAU", data_type="UV", end_min=4.0)
    c.uv_info = dict(UV_INFO)
    return c


def _plateau(t, top=1000.0):
    return top if 1.1 <= t <= 2.2 else 0.0


def _pulse(t):
    return 500.0 * math.exp(-0.5 * ((t - 1.3) / SIGMA_MIN) ** 2)


PULSE_AREA = 0.5 * SIGMA_MIN * math.sqrt(2 * math.pi)  # AU*ml


def test_plateau_gives_epsilon_from_the_maximum():
    run = _run(_events(), curves=[_uv(_plateau)])
    rows = bypass_uv_readings(run)
    assert len(rows) == 1
    r = rows[0]
    assert (r["kind"], r["curve"], r["wavelength_nm"], r["epsilon_from"]) == ("plateau", "UV 1_280", 280.0,
                                                                            "plateau maximum")
    assert r["valve_ml"] == pytest.approx(2.0) and r["path_length_cm"] == pytest.approx(0.2)
    assert r["absorbance_per_cm"] == pytest.approx(1.0 / 0.2)  # 1000 mAU over 2 mm
    assert r["epsilon_ml_per_mg_cm"] is None  # no sample concentration
    assert r["note"] == ACCURACY_NOTE
    r = bypass_uv_readings(run, sample_concentration=2.0)[0]
    assert r["epsilon_ml_per_mg_cm"] == pytest.approx(5.0 / 2.0)
    r = bypass_uv_readings(run, sample_concentration=2000.0, sample_concentration_unit="ug/ml")[0]
    assert r["epsilon_ml_per_mg_cm"] == pytest.approx(2.5)


def test_flat_top_and_absorbance_limit():
    run = _run(_events(), curves=[_uv(lambda t: _plateau(t, 3000.0))])
    r = bypass_uv_readings(run, sample_concentration=1.0)[0]
    assert r["epsilon_ml_per_mg_cm"] == pytest.approx(15.0)
    assert any(w.startswith("flat top") for w in r["warnings"])
    r = bypass_uv_readings(run, sample_concentration=1.0, absorbance_limit=2.5, absorbance_limit_unit="AU")[0]
    assert r["epsilon_ml_per_mg_cm"] is None and r["absorbance_per_cm"] == pytest.approx(15.0)
    assert any("above the absorbance limit" in w for w in r["warnings"])


def test_pulse_needs_the_loop_volume():
    run = _run(_events(), curves=[_uv(_pulse)])
    r = bypass_uv_readings(run, sample_concentration=1.0)[0]
    assert r["kind"] == "pulse" and r["area_au_ml"] == pytest.approx(PULSE_AREA, rel=5e-3)
    assert r["epsilon_ml_per_mg_cm"] is None
    assert any(w.startswith("pulse: give the loop volume") for w in r["warnings"])
    r = bypass_uv_readings(run, sample_concentration=1.0, loop_volume=50, loop_volume_unit="ul")[0]
    assert r["loop_volume_ml"] == pytest.approx(0.05)
    assert r["epsilon_ml_per_mg_cm"] == pytest.approx(PULSE_AREA / 0.05 / 0.2, rel=5e-3)
    # a loop bigger than what the valve period sent was not emptied
    r = bypass_uv_readings(run, sample_concentration=1.0, loop_volume=5.0)[0]
    assert any("was not emptied" in w for w in r["warnings"])


def test_no_signal_and_no_bypass():
    r = bypass_uv_readings(_run(_events(), curves=[_uv(lambda t: 1.0)]), sample_concentration=1.0)[0]
    assert r["kind"] == "no_signal" and r["epsilon_ml_per_mg_cm"] is None
    assert bypass_uv_readings(_run(_events(bypass=False), curves=[_uv(_plateau)])) == []
    with pytest.raises(ValueError, match="sample concentration"):
        bypass_uv_readings(_run(_events(), curves=[_uv(_plateau)]), sample_concentration=0)


def test_epsilon_from_injection_with_a_window_and_mass():
    run = _run(_events(), curves=[_uv(_pulse)])
    chrom = run.chromatogram()
    got = epsilon_from_injection(chrom, "UV 1_280", 50, "ug", window=(1.0, 2.0))
    assert got["epsilon_ml_per_mg_cm"] == pytest.approx(PULSE_AREA / 0.05 / 0.2, rel=5e-3)
    assert got["window_source"] == "caller" and got["note"] == ACCURACY_NOTE
    # default window: the run's single by-pass interval
    got = epsilon_from_injection(chrom, "UV 1_280", 0.05)
    assert got["window_source"] == "bypass" and got["window_ml"] == (pytest.approx(0.5), pytest.approx(2.5))
    assert got["epsilon_ml_per_mg_cm"] == pytest.approx(PULSE_AREA / 0.05 / 0.2, rel=5e-3)
    assert not any("no clear signal" in w for w in got["warnings"])
    capped = epsilon_from_injection(chrom, "UV 1_280", 0.05, absorbance_limit=100)
    assert capped["epsilon_ml_per_mg_cm"] is None
    # a 2 mAU bump (buffer through the by-pass) still gives a number, but says it is no signal
    weak = _run(_events(), curves=[_uv(lambda t: _pulse(t) / 250)]).chromatogram()
    got = epsilon_from_injection(weak, "UV 1_280", 0.05)
    assert any("no clear signal" in w and "give window=" in w for w in got["warnings"])
    with pytest.raises(ValueError, match="injected mass"):
        epsilon_from_injection(chrom, "UV 1_280", 0)
