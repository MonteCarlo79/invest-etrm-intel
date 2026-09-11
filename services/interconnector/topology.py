"""ECharts option builders for S1. Pure dict builders — rendering lives in the tab."""
from __future__ import annotations

import math

KV_ORDER = ["±100","±400","±500","±660","±800","±1100","500kV AC"]
KV_COLORS = {"±100":"#f0f5fc","±400":"#dbe7f7","±500":"#a8c4ea","±660":"#6a99dd",
             "±800":"#2563eb","±1100":"#0f2b6e","500kV AC":"#9aa6b5"}
RECV_COLORS = {"江苏":"#2563eb","上海":"#e11d48","浙江":"#0d9488","福建":"#d97706"}
MUTED = "#5b6675"
LAND = "#e6ebf1"; BORDER = "#c3cfda"; EMPH = "#d7e0e9"

def _geo() -> dict:
    return {"map": "china", "roam": True, "scaleLimit": {"min": 0.8, "max": 6},
            "layoutCenter": ["50%", "52%"], "layoutSize": "102%", "silent": False,
            "itemStyle": {"areaColor": LAND, "borderColor": BORDER, "borderWidth": 0.7},
            "emphasis": {"itemStyle": {"areaColor": EMPH}, "label": {"show": False}}}

def physical_options(channels: list[dict], channel_agg: dict,
                     selected_prov: str | None = None) -> dict:
    max_gw = max((c["gw"] for c in channels), default=1.0) or 1.0
    series = []
    for kv in KV_ORDER:
        data = []
        for c in channels:
            if c["kv"] != kv:
                continue
            linked = (selected_prov is None or c["send_prov"] == selected_prov
                      or c["recv_prov"] == selected_prov)
            agg = channel_agg.get(c["name"], {})
            data.append({"coords": [c["sc"], c["rc"]],
                "lineStyle": {"color": KV_COLORS[kv], "width": 1 + 5.5*math.sqrt(c["gw"]/max_gw),
                              "opacity": 0.85 if linked else 0.07,
                              "type": "dashed" if kv == "500kV AC" else "solid"},
                "meta": dict(c, vol_gwh=agg.get("vol_gwh", 0), land=agg.get("land"),
                             trades=agg.get("trades", 0))})
        if not data:
            continue
        series.append({"name": kv, "type": "lines", "coordinateSystem": "geo",
                       "zlevel": 2, "lineStyle": {"curveness": 0.08}, "data": data})
    stations = []
    for c in channels:
        stations.append({"name": c["send"], "value": c["sc"], "itemStyle": {"color": MUTED}})
        stations.append({"name": c["recv"], "value": c["rc"], "itemStyle": {"color": KV_COLORS[c["kv"]]}})
    series.append({"name": "stations", "type": "scatter", "coordinateSystem": "geo",
                   "zlevel": 3, "symbolSize": 3.5, "data": stations})
    return {"backgroundColor": "transparent",
            "color": [KV_COLORS[k] for k in KV_ORDER],
            "legend": {"top": 6, "left": "center", "icon": "roundRect", "data": KV_ORDER},
            "geo": _geo(), "series": series}

def flows_options(flows: list[dict], coords: dict) -> dict:
    max_vol = max((f["vol_gwh"] for f in flows), default=1.0) or 1.0
    series = []
    for rp, color in RECV_COLORS.items():
        data = [{"coords": [coords[f["send"]], coords[f["recv"]]],
                 "lineStyle": {"color": color,
                               "width": 0.8 + 6.2*math.sqrt(f["vol_gwh"]/max_vol),
                               "opacity": 0.72},
                 "meta": f} for f in flows if f["recv"] == rp]
        series.append({"name": rp, "type": "lines", "coordinateSystem": "geo",
                       "zlevel": 2, "lineStyle": {"curveness": 0.22}, "data": data})
    send_names = sorted({f["send"] for f in flows})
    senders = [{"name": s,
                "value": [*coords[s], sum(f["vol_gwh"] for f in flows if f["send"] == s)],
                "symbolSize": 5 + 9*math.sqrt(sum(f["vol_gwh"] for f in flows if f["send"] == s)/max_vol),
                "itemStyle": {"color": MUTED}} for s in send_names]
    recv_names = [rp for rp in RECV_COLORS if any(f["recv"] == rp for f in flows)]
    receivers = [{"name": rp,
                  "value": [*coords[rp], sum(f["vol_gwh"] for f in flows if f["recv"] == rp)],
                  "symbolSize": 7 + 11*math.sqrt(sum(f["vol_gwh"] for f in flows if f["recv"] == rp)/max_vol),
                  "itemStyle": {"color": RECV_COLORS[rp], "borderWidth": 2},
                  "label": {"show": True, "formatter": rp, "fontWeight": 700, "fontSize": 12}}
                 for rp in recv_names]
    series += [{"name": "senders", "type": "scatter", "coordinateSystem": "geo",
                "zlevel": 3, "data": senders},
               {"name": "receivers", "type": "scatter", "coordinateSystem": "geo",
                "zlevel": 4, "data": receivers}]
    return {"backgroundColor": "transparent",
            "color": list(RECV_COLORS.values()),
            "legend": {"top": 6, "left": "center", "icon": "roundRect",
                       "data": list(RECV_COLORS)},
            "geo": _geo(), "series": series}

# province centroid coords for the flows view (from GeoJSON + sub-province anchors)
FLOW_COORDS = {"蒙东": [122.24, 43.62], "锡盟": [116.07, 43.94]}
