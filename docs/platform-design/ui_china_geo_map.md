# UI Pattern: China Province Choropleth Map

Reusable matplotlib-based choropleth map for colouring China provinces by a
numeric metric, with price labels at province centroids and a colour-scale
legend bar.

**Reference implementation:** `apps/spot-market/app.py` → `chart_geo_map()`

---

## Why matplotlib, not Plotly

Plotly `go.Choropleth` requires province features to be matched by a string ID
(`featureidkey`). The DataV CDN GeoJSON has `id = null` on every feature — only
`properties.adcode` (an integer) is reliable. Plotly's ID matching is fragile
with integer-valued properties and changed behaviour in Plotly ≥ 6.0.

The matplotlib approach reads `properties.adcode` directly as an integer key and
draws each polygon with `matplotlib.patches.Polygon` — no ID matching step,
no version-sensitivity.

---

## GeoJSON Source

| Property | Value |
|---|---|
| CDN URL | `https://geo.datav.aliyun.com/areas_v3/bound/100000_full.json` |
| Local cache | `apps/{app}/data/china_provinces.geojson` |
| Features | 35 province-level regions |
| ID field | `properties.adcode` (integer, 6-digit) |

Download is automatic on first load and cached to disk. Subsequent loads are
instant (file read only).

```python
_GEO_FILE = Path(__file__).parent / "data" / "china_provinces.geojson"

@st.cache_data(ttl=None, show_spinner=False)
def _load_china_geojson() -> tuple[dict | None, str | None]:
    if _GEO_FILE.exists():
        try:
            return json.loads(_GEO_FILE.read_text(encoding="utf-8")), None
        except Exception:
            pass
    try:
        resp = requests.get(
            "https://geo.datav.aliyun.com/areas_v3/bound/100000_full.json",
            timeout=20,
        )
        resp.raise_for_status()
        gj = resp.json()
        if len(gj.get("features", [])) < 10:
            return None, "GeoJSON has too few features"
        _GEO_FILE.parent.mkdir(parents=True, exist_ok=True)
        _GEO_FILE.write_text(json.dumps(gj), encoding="utf-8")
        return gj, None
    except Exception as exc:
        return None, str(exc)
```

---

## Province → adcode Mapping

Map your data's province name column to the 6-digit adcode string used in
`_PROV_CENTROIDS`. Note that sub-province regions (Hebei-North/South, Mengxi/
Mengdong) intentionally collapse to their parent adcode.

```python
_PROV_ADCODE: dict[str, str] = {
    "Beijing":      "110000", "Tianjin":     "120000",
    "Hebei":        "130000", "Hebei-North": "130000", "Hebei-South": "130000",
    "Shanxi":       "140000",
    "Mengxi":       "150000", "Mengdong":    "150000",
    "Liaoning":     "210000", "Jilin":       "220000", "Heilongjiang": "230000",
    "Shanghai":     "310000", "Jiangsu":     "320000", "Zhejiang":     "330000",
    "Anhui":        "340000", "Fujian":      "350000", "Jiangxi":      "360000",
    "Shandong":     "370000", "Henan":       "410000", "Hubei":        "420000",
    "Hunan":        "430000", "Guangdong":   "440000", "Guangxi":      "450000",
    "Hainan":       "460000", "Chongqing":   "500000", "Sichuan":      "510000",
    "Guizhou":      "520000", "Yunnan":      "530000",
    "Shaanxi":      "610000", "Gansu":       "620000", "Qinghai":      "630000",
    "Ningxia":      "640000", "Xinjiang":    "650000",
}
```

---

## Province Centroid Coordinates

Used to position text labels inside each province. Coordinates are `(lat, lon)`.

```python
_PROV_CENTROIDS: dict[str, tuple[float, float]] = {
    "110000": (39.90, 116.40), "120000": (39.13, 117.20),
    "130000": (38.04, 114.47), "140000": (37.87, 112.56),
    "150000": (44.09, 113.09), "210000": (41.80, 123.43),
    "220000": (43.89, 125.32), "230000": (47.85, 127.57),
    "310000": (31.23, 121.47), "320000": (32.06, 119.59),
    "330000": (30.27, 120.15), "340000": (31.86, 117.29),
    "350000": (26.10, 118.31), "360000": (27.62, 115.70),
    "370000": (36.67, 117.02), "410000": (34.76, 113.75),
    "420000": (30.60, 114.30), "430000": (28.23, 112.94),
    "440000": (23.37, 113.50), "450000": (23.73, 108.38),
    "460000": (20.02, 110.35), "500000": (29.56, 106.54),
    "510000": (30.57, 103.99), "520000": (26.82, 106.83),
    "530000": (25.05, 101.71), "610000": (34.27, 108.95),
    "620000": (36.06, 103.83), "630000": (36.62, 101.74),
    "640000": (38.47, 106.26), "650000": (41.17,  85.29),
}
```

---

## Colour Scale

Define stops as `(normalised_position, hex_colour)`. Position 0.0 = `vmin`,
1.0 = `vmax`.

The spot market uses a green→yellow→red scale for electricity prices (¥/kWh):

```python
_GEO_COLORSCALE = [
    [0.00, "#00aa44"],   # low  (≤ 0.20 ¥/kWh)
    [0.40, "#ffe000"],   # mid  (≈ 0.30 ¥/kWh at vmax=0.5)
    [0.60, "#ff6600"],
    [1.00, "#cc0000"],   # high (≥ 0.50 ¥/kWh)
]
_GEO_ZMIN, _GEO_ZMAX = 0.0, 0.5

def _make_china_cmap() -> mcolors.LinearSegmentedColormap:
    stops = [(pos, mcolors.to_rgb(hex_col)) for pos, hex_col in _GEO_COLORSCALE]
    return mcolors.LinearSegmentedColormap.from_list("china_price", stops)
```

For a different domain, adjust `_GEO_ZMIN`, `_GEO_ZMAX`, and the stop colours.

---

## Core Rendering Function

```python
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Polygon as MplPolygon
import numpy as np

def chart_geo_map(df: pd.DataFrame, metric: str, geojson: dict | None) -> plt.Figure:
    # 1. Aggregate data to adcode level
    agg = _geo_agg(df, metric)          # → DataFrame[adcode, avg, label, price_str]

    cmap = _make_china_cmap()
    norm = mcolors.Normalize(vmin=_GEO_ZMIN, vmax=_GEO_ZMAX)

    # 2. Build adcode(int) → value lookup
    price_map: dict[int, float] = {}
    for _, row in agg.iterrows():
        try:
            price_map[int(row["adcode"])] = float(row["avg"])
        except (ValueError, TypeError):
            pass

    # 3. Draw figure
    fig, ax = plt.subplots(figsize=(9, 6), facecolor="white")
    ax.set_facecolor("#b8d4f0")          # ocean blue

    if geojson:
        for feat in geojson.get("features", []):
            adcode_int = feat.get("properties", {}).get("adcode")
            price = price_map.get(adcode_int)
            fc = cmap(norm(price)) if price is not None else "#d0d0d0"   # grey = no data

            geom = feat.get("geometry", {})
            rings = []
            if geom.get("type") == "Polygon":
                rings = [geom["coordinates"][0]]
            elif geom.get("type") == "MultiPolygon":
                rings = [p[0] for p in geom["coordinates"]]

            for ring in rings:
                coords = np.array(ring)  # [[lon, lat], …]
                ax.add_patch(MplPolygon(
                    coords, closed=True,
                    facecolor=fc, edgecolor="white", linewidth=0.8,
                ))

    # 4. Price labels at centroids
    for _, row in agg.iterrows():
        coord = _PROV_CENTROIDS.get(row["adcode"])
        if coord:
            lat, lon = coord
            ax.text(lon, lat, row["price_str"],
                    ha="center", va="center",
                    fontsize=7, fontweight="bold", color="black")

    # 5. Axes + colorbar
    ax.set_xlim(72, 137)
    ax.set_ylim(16, 54)
    ax.set_aspect("equal")
    ax.axis("off")

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, orientation="vertical",
                        fraction=0.025, pad=0.01, aspect=25)
    cbar.set_label("¥/kWh", fontsize=9)
    cbar.set_ticks([0.0, 0.1, 0.2, 0.3, 0.4, 0.5])
    cbar.set_ticklabels(["0.0", "0.1", "0.2", "0.3", "0.4", "0.5+"])
    cbar.ax.tick_params(labelsize=8)

    ax.set_title("Average Price by Province (¥/kWh)", fontsize=11, pad=10)
    plt.tight_layout(pad=0.5)
    return fig
```

### `_geo_agg` helper

Aggregates a tidy DataFrame (province_en, metric_avg) down to one row per
adcode with a formatted label string.

```python
def _geo_agg(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    avg_col = f"{metric}_avg"
    df2 = df.copy()
    df2["adcode"] = df2["province_en"].map(_PROV_ADCODE)
    df2 = df2.dropna(subset=["adcode", avg_col])
    if df2.empty:
        return pd.DataFrame(columns=["adcode", "avg", "label", "price_str"])
    agg = df2.groupby("adcode", as_index=False)[avg_col].mean()
    agg.columns = ["adcode", "avg"]
    agg["label"]     = agg["adcode"].map(_ADCODE_LABEL)
    agg["price_str"] = agg["avg"].map(lambda v: f"{v:.2f}")
    return agg
```

---

## Rendering in Streamlit

Always call `plt.close(fig)` after `st.pyplot()` to prevent memory leaks.

```python
_geojson, _geo_err = _load_china_geojson()
if _geo_err:
    st.warning(f"Province boundaries unavailable. ({_geo_err})")

col_da, col_rt = st.columns(2)
with col_da:
    st.caption(f"**Day-Ahead (DA)** · {d_start} → {d_end}")
    fig = chart_geo_map(df, "da", _geojson)
    st.pyplot(fig, use_container_width=True)
    plt.close(fig)
with col_rt:
    st.caption(f"**Real-Time (RT)** · {d_start} → {d_end}")
    fig = chart_geo_map(df, "rt", _geojson)
    st.pyplot(fig, use_container_width=True)
    plt.close(fig)
```

---

## Required imports

```python
import json
import requests
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Polygon as MplPolygon
import numpy as np
import pandas as pd
```

---

## Adapting to a New App

| What to change | Where |
|---|---|
| Metric column names | `_geo_agg()` — change `f"{metric}_avg"` to your column |
| Label format | `agg["price_str"]` — e.g. `f"{v:.0f} MW"` |
| Colour range + stops | `_GEO_ZMIN`, `_GEO_ZMAX`, `_GEO_COLORSCALE` |
| Colorbar ticks + label | `cbar.set_ticks`, `cbar.set_label` |
| Map title | `ax.set_title(...)` |
| GeoJSON cache path | `_GEO_FILE` — one per app so they don't share a stale file |
| Province mapping | `_PROV_ADCODE` — add aliases as needed for your data's naming |

The `_PROV_CENTROIDS` and the polygon rendering loop do not need to change.
