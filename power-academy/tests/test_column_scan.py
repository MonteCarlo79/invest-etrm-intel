import numpy as np
import pandas as pd

from academy.column.scan import render_scan_md, scan_anomalies


def _df():
    rng = np.random.default_rng(7)
    rows = []
    for prov, base in [("山东", 0.4), ("山西", 0.35), ("广东", 0.5)]:
        for day in range(35):
            for h in range(8, 20):                      # 12 h/day -> 96 week rows
                rows.append({"province": prov,
                             "datetime": pd.Timestamp("2026-09-01") + pd.Timedelta(days=day, hours=h),
                             "rt_price": base + rng.normal(0, 0.02),
                             "da_price": base})
    df = pd.DataFrame(rows)
    # inject a spike week in 山东 during the last 7 days
    mask = (df.province == "山东") & (df.datetime >= "2026-09-30") & (df.datetime.dt.hour == 10)
    df.loc[mask, "rt_price"] = 2.5
    return df


def test_scan_finds_injected_spike():
    res = scan_anomalies(_df(), today="2026-10-05")
    assert res and res[0]["province"] == "山东"
    assert "spike" in res[0]["metric"] or "max" in res[0]["metric"]
    md = render_scan_md(res, today="2026-10-05")
    assert "山东" in md and "2026-10-05" in md
