from . import compute as c


def test_proxy_beats_unhedged_two_product_beats_proxy():
    r = c.results()
    assert r["resid_single"].std() < r["u"].std() * 0.6
    assert r["resid_double"].std() < r["resid_single"].std()
    assert c.par95(r["u"].mean() + r["resid_single"]) < c.par95(r["u"])
    assert c.par95(r["u"].mean() + r["resid_double"]) < c.par95(r["u"].mean() + r["resid_single"])


def test_residual_basis_grows_with_peak_basis_vol():
    r0 = c.results(peak_sd=0.0)
    r4 = c.results(peak_sd=4.0)
    r8 = c.results(peak_sd=8.0)
    assert r0["resid_single"].std() < r4["resid_single"].std() < r8["resid_single"].std()
    # the single-vs-two-product gap widens with basis vol
    gap0 = r0["resid_single"].std() - r0["resid_double"].std()
    gap8 = r8["resid_single"].std() - r8["resid_double"].std()
    assert gap8 > gap0


def test_measured_numbers():
    r = c.results()
    assert abs(c.par95(r["u"]) - 3_998) < 150
    assert abs(c.par95(r["u"].mean() + r["resid_single"]) - 2_596) < 150
    assert abs(c.par95(r["u"].mean() + r["resid_double"]) - 1_629) < 150
