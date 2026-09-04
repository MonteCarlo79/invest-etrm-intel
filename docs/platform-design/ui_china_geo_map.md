# UI Pattern: Spot Market Price Charts

Reusable Streamlit + Plotly/matplotlib chart patterns from the China spot
market cockpit. Each section is self-contained and can be lifted into any
time-series price app.

**Reference implementation:** `apps/spot-market/app.py`

---

## Shared: Province Colour Palette

All multi-province Plotly charts use the same deterministic colour assignment
so the same province is always the same colour across tabs.

```python
import plotly.express as px
_PALETTE = px.colors.qualitative.Plotly + px.colors.qualitative.Dark24

def _prov_colour(provinces: list[str]) -> dict[str, str]:
    """Stable colour per province — sorted alphabetically then indexed."""
    return {p: _PALETTE[i % len(_PALETTE)] for i, p in enumerate(sorted(provinces))}
```

---

## 1. Time Series Overview

**Tab:** Overview  
**Function:** `chart_timeseries(df, provinces, metric, show_band)`  
**Library:** Plotly (`go.Scatter`)

Two charts side-by-side: DA and RT. Each province is a line. Optional shaded
band shows the daily min/max range behind the avg line.

### Key design choices

- `hovermode="x unified"` — all provinces shown in a single tooltip at the same x
- Legend below the chart (`y=-0.18`) keeps the plot area full-width
- Band added as a second `go.Scatter` trace with `fill="toself"`, 10% opacity, no legend entry
- `hovertemplate` uses `<extra>` tag to inject the province name cleanly

```python
def chart_timeseries(df, provinces, metric, show_band) -> go.Figure:
    fig = go.Figure()
    colours = _prov_colour(provinces)
    avg_col, max_col, min_col = f"{metric}_avg", f"{metric}_max", f"{metric}_min"

    for prov in sorted(provinces):
        sub = df[df["province_en"] == prov].sort_values("report_date")
        if sub.empty or sub[avg_col].isna().all():
            continue
        col = colours[prov]

        # Min/max band
        if show_band:
            sub_b = sub[sub[avg_col].notna()]
            if sub_b[max_col].notna().any():
                x_band = pd.concat([sub_b["report_date"], sub_b["report_date"].iloc[::-1]])
                y_band = pd.concat([sub_b[max_col], sub_b[min_col].iloc[::-1]])
                fig.add_trace(go.Scatter(
                    x=x_band, y=y_band, fill="toself",
                    fillcolor=col, opacity=0.10,
                    line=dict(width=0), showlegend=False, hoverinfo="skip",
                ))

        fig.add_trace(go.Scatter(
            x=sub["report_date"], y=sub[avg_col],
            name=prov, mode="lines+markers",
            line=dict(color=col, width=1.8), marker=dict(size=4),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.4f} ¥/kWh<extra>" + prov + "</extra>",
        ))

    label = "Day-Ahead (DA)" if metric == "da" else "Real-Time (RT)"
    fig.update_layout(
        height=430,
        title=dict(text=f"{label} Clearing Price  (¥/kWh)", font=dict(size=14)),
        margin=dict(l=10, r=10, t=45, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.18, xanchor="center", x=0.5),
        xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickformat=".3f"),
        plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
    )
    return fig
```

### Rendering

```python
col_da, col_rt = st.columns(2)
with col_da:
    st.plotly_chart(chart_timeseries(df, selected_provs, "da", show_band),
                    use_container_width=True)
with col_rt:
    st.plotly_chart(chart_timeseries(df, selected_provs, "rt", show_band),
                    use_container_width=True)
```

### Sidebar controls

```python
show_band = st.checkbox("Show min/max band", value=True)
```

### Adapting

| What to change | How |
|---|---|
| Metric columns | Replace `f"{metric}_avg/max/min"` with your column names |
| Hover format | Change `%.4f ¥/kWh` to suit your unit |
| Band source | Any high/low pair works — not limited to min/max |

---

## 2. Province Deep-Dive

**Tab:** Province Deep-Dive  
**Function:** `chart_da_rt_overlay(df, province)`  
**Library:** Plotly (`go.Scatter`)

Single-province view showing DA and RT avg on the same axes, each with its own
shaded min/max band. Paired with a raw data table below.

### Key design choices

- Two fixed colours: DA = `#1f77b4` (blue), RT = `#ff7f0e` (orange)
- Band opacity 12% — slightly higher than the overview (10%) because only 2 traces
- Raw table uses `.style.format()` with `na_rep="—"` so nulls display cleanly

```python
def chart_da_rt_overlay(df, province) -> go.Figure:
    sub = df[df["province_en"] == province].sort_values("report_date")
    fig = go.Figure()

    for metric, label, colour in [("da", "DA avg", "#1f77b4"), ("rt", "RT avg", "#ff7f0e")]:
        avg_col, max_col, min_col = f"{metric}_avg", f"{metric}_max", f"{metric}_min"
        if sub[avg_col].isna().all():
            continue
        if sub[max_col].notna().any():
            fig.add_trace(go.Scatter(
                x=pd.concat([sub["report_date"], sub["report_date"].iloc[::-1]]),
                y=pd.concat([sub[max_col], sub[min_col].iloc[::-1]]),
                fill="toself", fillcolor=colour, opacity=0.12,
                line=dict(width=0), showlegend=False, hoverinfo="skip",
            ))
        fig.add_trace(go.Scatter(
            x=sub["report_date"], y=sub[avg_col],
            name=label, mode="lines+markers",
            line=dict(color=colour, width=2), marker=dict(size=4),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.4f} ¥/kWh<extra>" + label + "</extra>",
        ))

    fig.update_layout(
        height=390,
        title=dict(text=f"{province} — DA vs RT  (¥/kWh)", font=dict(size=13)),
        margin=dict(l=10, r=10, t=45, b=60),
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
        xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickformat=".3f"),
        plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
    )
    return fig
```

### Raw data table below the chart

```python
sub = df[df["province_en"] == province].sort_values("report_date").copy()
sub["report_date"] = pd.to_datetime(sub["report_date"]).dt.date
st.dataframe(
    sub[["report_date","da_avg","da_max","da_min","rt_avg","rt_max","rt_min"]]
    .rename(columns={"report_date": "Date"})
    .style.format(
        {c: "{:.4f}" for c in ["da_avg","da_max","da_min","rt_avg","rt_max","rt_min"]},
        na_rep="—",
    ),
    use_container_width=True, hide_index=True,
)
```

### Rendering

```python
dive_prov = st.selectbox("Select province", sorted(selected_provs))
if dive_prov:
    st.plotly_chart(chart_da_rt_overlay(df, dive_prov), use_container_width=True)
```

### Adapting

- Replace the two `(metric, label, colour)` tuples with however many series you need
- The band pattern works for any (high, low) pair — energy generation, load, etc.

---

## 3. Heatmap

**Tab:** Heatmap  
**Function:** `chart_heatmap(df, metric)`  
**Library:** Plotly (`go.Heatmap`)

Province × Date grid coloured by average price. Good for spotting regional
clusters and temporal patterns at a glance.

### Key design choices

- Pivot via `pivot_table` (handles duplicates gracefully with mean aggregation)
- `RdYlGn_r` reversed so red = high price (matches traffic-light intuition)
- `hoverongaps=False` suppresses tooltips on null cells
- Height scales with number of provinces: `max(350, len(pivot) * 24)`
- X-axis shows `MM-DD` only (not full date) to save space

```python
def chart_heatmap(df, metric) -> go.Figure:
    avg_col = f"{metric}_avg"
    pivot = (
        df[["report_date", "province_en", avg_col]]
        .dropna(subset=[avg_col])
        .pivot_table(index="province_en", columns="report_date", values=avg_col)
    )
    if pivot.empty:
        return go.Figure()

    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.strftime("%m-%d"),
        y=pivot.index.tolist(),
        colorscale="RdYlGn_r",
        colorbar=dict(title="¥/kWh", thickness=12),
        hoverongaps=False,
        hovertemplate="Date: %{x}<br>Province: %{y}<br>Price: %{z:.4f} ¥/kWh<extra></extra>",
    ))
    label = "Day-Ahead" if metric == "da" else "Real-Time"
    fig.update_layout(
        height=max(350, len(pivot) * 24),
        title=dict(text=f"{label} Average Clearing Price — Province × Date Heatmap",
                   font=dict(size=13)),
        margin=dict(l=120, r=20, t=45, b=60),
        xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
        yaxis=dict(tickfont=dict(size=11)),
    )
    return fig
```

### Rendering

```python
hm_metric = st.radio("Metric", ["DA", "RT"], horizontal=True)
fig_hm = chart_heatmap(df[df["province_en"].isin(selected_provs)], hm_metric.lower())
if fig_hm.data:
    st.plotly_chart(fig_hm, use_container_width=True)
else:
    st.info("No data for selected range / provinces.")
```

### Adapting

| What to change | How |
|---|---|
| Row dimension | Replace `province_en` with any categorical (asset, market, etc.) |
| Colour scale | Any Plotly named scale; `RdYlGn_r` suits price. `Blues` suits volume |
| Left margin | Increase `l=120` if row labels are longer |
| Row height | Change the `* 24` multiplier if rows feel cramped |

---

## 4. Distributions

**Tab:** Distributions  
**Functions:** `chart_distributions`, `chart_violin`, `_dist_stats`  
**Library:** Plotly (`go.Histogram`, `go.Scatter`, `go.Violin`)

Three-part view per metric: overlapping histograms + optional KDE curves,
violin/box plots, and a descriptive statistics table.

### 4a. Histogram + KDE

```python
def chart_distributions(df, provinces, metric, nbins, show_kde) -> go.Figure:
    avg_col = f"{metric}_avg"
    colours = _prov_colour(provinces)
    fig = go.Figure()

    for prov in sorted(provinces):
        vals = df[df["province_en"] == prov][avg_col].dropna().values
        if len(vals) < 2:
            continue
        col = colours[prov]

        fig.add_trace(go.Histogram(
            x=vals, name=prov, nbinsx=nbins,
            marker_color=col, opacity=0.45,
            histnorm="probability density",
            hovertemplate="Price: %{x:.4f} ¥/kWh<br>Density: %{y:.3f}<extra>" + prov + "</extra>",
        ))

        if show_kde and len(vals) >= 5:
            std = vals.std()
            if std > 0:
                bw = 1.06 * std * len(vals) ** (-0.2)   # Silverman's rule
                x_grid = np.linspace(vals.min() - 2*bw, vals.max() + 2*bw, 300)
                kde = np.zeros_like(x_grid)
                for v in vals:
                    kde += np.exp(-0.5 * ((x_grid - v) / bw) ** 2)
                kde /= len(vals) * bw * np.sqrt(2 * np.pi)
                fig.add_trace(go.Scatter(
                    x=x_grid, y=kde, name=f"{prov} KDE",
                    mode="lines", line=dict(color=col, width=2),
                    showlegend=False,
                ))

    label = "Day-Ahead (DA)" if metric == "da" else "Real-Time (RT)"
    fig.update_layout(
        height=430, barmode="overlay",
        title=dict(text=f"{label} Price Distribution  (¥/kWh)", font=dict(size=14)),
        margin=dict(l=10, r=10, t=45, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.18, xanchor="center", x=0.5),
        xaxis=dict(title="Price (¥/kWh)", showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(title="Probability density", showgrid=True, gridcolor="#f0f0f0"),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    return fig
```

**KDE implementation:** pure NumPy Gaussian kernel, Silverman's bandwidth rule.
No scipy dependency.

### 4b. Violin / Box

```python
def chart_violin(df, provinces, metric) -> go.Figure:
    avg_col = f"{metric}_avg"
    colours = _prov_colour(provinces)
    fig = go.Figure()

    for prov in sorted(provinces):
        vals = df[df["province_en"] == prov][avg_col].dropna().values
        if len(vals) < 3:
            continue
        fig.add_trace(go.Violin(
            y=vals, name=prov,
            box_visible=True, meanline_visible=True,
            fillcolor=colours[prov], opacity=0.65,
            line_color=colours[prov],
        ))

    label = "Day-Ahead (DA)" if metric == "da" else "Real-Time (RT)"
    fig.update_layout(
        height=430, violinmode="group",
        title=dict(text=f"{label} — Violin / Box Plot  (¥/kWh)", font=dict(size=14)),
        margin=dict(l=10, r=10, t=45, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.18, xanchor="center", x=0.5),
        yaxis=dict(title="Price (¥/kWh)", showgrid=True, gridcolor="#f0f0f0", tickformat=".3f"),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    return fig
```

### 4c. Descriptive statistics table

```python
def _dist_stats(df, provinces, metric) -> pd.DataFrame:
    avg_col = f"{metric}_avg"
    rows = []
    for prov in sorted(provinces):
        vals = df[df["province_en"] == prov][avg_col].dropna()
        if vals.empty:
            continue
        rows.append({
            "Province": prov,
            "N":      len(vals),
            "Mean":   f"{vals.mean():.4f}",
            "Median": f"{vals.median():.4f}",
            "Std":    f"{vals.std():.4f}",
            "P10":    f"{vals.quantile(0.10):.4f}",
            "P25":    f"{vals.quantile(0.25):.4f}",
            "P75":    f"{vals.quantile(0.75):.4f}",
            "P90":    f"{vals.quantile(0.90):.4f}",
            "Min":    f"{vals.min():.4f}",
            "Max":    f"{vals.max():.4f}",
        })
    return pd.DataFrame(rows)
```

### Rendering

```python
dc1, dc2, dc3 = st.columns([2, 1, 1])
with dc1:
    dist_metric = st.radio("Market", ["DA", "RT", "Both"], horizontal=True, key="dist_metric")
with dc2:
    nbins = st.slider("Histogram bins", 10, 80, 30, key="dist_bins")
with dc3:
    show_kde = st.checkbox("Overlay KDE curve", value=True, key="dist_kde")

metrics = ["da", "rt"] if dist_metric == "Both" else [dist_metric.lower()]
for m in metrics:
    st.plotly_chart(chart_distributions(df, selected_provs, m, nbins, show_kde),
                    use_container_width=True)
    st.plotly_chart(chart_violin(df, selected_provs, m), use_container_width=True)
    st.dataframe(_dist_stats(df, selected_provs, m),
                 use_container_width=True, hide_index=True)
    if dist_metric == "Both" and m == "da":
        st.divider()
```

### Adapting

| What to change | How |
|---|---|
| Number of metrics | Add more values to the `metrics` loop |
| KDE bandwidth | Replace Silverman's rule with Scott's: `bw = 1.059 * std * n**(-0.2)` |
| Statistics columns | Add/remove keys in `_dist_stats` — e.g. add `"Skew"` via `vals.skew()` |
| Violin grouping | Change `violinmode` to `"overlay"` if provinces overlap too much |

---

## 5. Geo Map (China Province Choropleth)

**Tab:** Geo Map  
**Function:** `chart_geo_map(df, metric, geojson)`  
**Library:** matplotlib (`patches.Polygon`)

### Why matplotlib, not Plotly

Plotly `go.Choropleth` requires province features to be matched by a string ID
(`featureidkey`). The DataV CDN GeoJSON has `id = null` on every feature — only
`properties.adcode` (an integer) is reliable. Plotly's ID matching is fragile
with integer-valued properties and changed behaviour in Plotly ≥ 6.0.

The matplotlib approach reads `properties.adcode` directly as an integer key and
draws each polygon with `matplotlib.patches.Polygon` — no ID matching step,
no version-sensitivity.

### GeoJSON Source

| Property | Value |
|---|---|
| CDN URL | `https://geo.datav.aliyun.com/areas_v3/bound/100000_full.json` |
| Local cache | `apps/{app}/data/china_provinces.geojson` |
| Features | 35 province-level regions |
| ID field | `properties.adcode` (integer, 6-digit) |

Download is automatic on first load and cached to disk.

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

### Province → adcode Mapping

Sub-province regions (Hebei-North/South, Mengxi/Mengdong) collapse to their
parent adcode so they share a colour and label.

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

### Province Centroid Coordinates

`(lat, lon)` used to position text labels inside each province.

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

### Colour Scale

```python
_GEO_COLORSCALE = [
    [0.00, "#00aa44"],   # low  (≤ 0.20 ¥/kWh)
    [0.40, "#ffe000"],   # mid
    [0.60, "#ff6600"],
    [1.00, "#cc0000"],   # high (≥ 0.50 ¥/kWh)
]
_GEO_ZMIN, _GEO_ZMAX = 0.0, 0.5

def _make_china_cmap():
    stops = [(pos, mcolors.to_rgb(c)) for pos, c in _GEO_COLORSCALE]
    return mcolors.LinearSegmentedColormap.from_list("china_price", stops)
```

### Core Rendering Function

```python
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Polygon as MplPolygon
import numpy as np

def chart_geo_map(df, metric, geojson) -> plt.Figure:
    agg = _geo_agg(df, metric)   # → DataFrame[adcode, avg, label, price_str]
    cmap = _make_china_cmap()
    norm = mcolors.Normalize(vmin=_GEO_ZMIN, vmax=_GEO_ZMAX)

    price_map: dict[int, float] = {}
    for _, row in agg.iterrows():
        try:
            price_map[int(row["adcode"])] = float(row["avg"])
        except (ValueError, TypeError):
            pass

    fig, ax = plt.subplots(figsize=(9, 6), facecolor="white")
    ax.set_facecolor("#b8d4f0")   # ocean blue

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
                ax.add_patch(MplPolygon(
                    np.array(ring), closed=True,
                    facecolor=fc, edgecolor="white", linewidth=0.8,
                ))

    for _, row in agg.iterrows():
        coord = _PROV_CENTROIDS.get(row["adcode"])
        if coord:
            lat, lon = coord
            ax.text(lon, lat, row["price_str"],
                    ha="center", va="center", fontsize=7, fontweight="bold", color="black")

    ax.set_xlim(72, 137); ax.set_ylim(16, 54)
    ax.set_aspect("equal"); ax.axis("off")

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, orientation="vertical", fraction=0.025, pad=0.01, aspect=25)
    cbar.set_label("¥/kWh", fontsize=9)
    cbar.set_ticks([0.0, 0.1, 0.2, 0.3, 0.4, 0.5])
    cbar.set_ticklabels(["0.0", "0.1", "0.2", "0.3", "0.4", "0.5+"])
    cbar.ax.tick_params(labelsize=8)

    label = "Day-Ahead (DA)" if metric == "da" else "Real-Time (RT)"
    ax.set_title(f"{label} — Average Price by Province (¥/kWh)", fontsize=11, pad=10)
    plt.tight_layout(pad=0.5)
    return fig
```

### `_geo_agg` helper

```python
def _geo_agg(df, metric) -> pd.DataFrame:
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

### Rendering in Streamlit

Always call `plt.close(fig)` to prevent memory leaks.

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

### Adapting

| What to change | Where |
|---|---|
| Metric column | `_geo_agg()` — change `f"{metric}_avg"` |
| Label format | `agg["price_str"]` — e.g. `f"{v:.0f} MW"` |
| Colour range | `_GEO_ZMIN`, `_GEO_ZMAX`, `_GEO_COLORSCALE` |
| Colorbar ticks + unit | `cbar.set_ticks`, `cbar.set_label` |
| GeoJSON cache path | `_GEO_FILE` — one per app |
| Province aliases | `_PROV_ADCODE` — add aliases for your naming convention |

`_PROV_CENTROIDS` and the polygon loop never need to change.

---

## Required Imports (all charts)

```python
# Plotly charts (1–4)
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import pandas as pd

# Geo map (5)
import json
import requests
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.patches import Polygon as MplPolygon
```
