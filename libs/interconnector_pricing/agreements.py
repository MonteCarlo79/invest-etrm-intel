"""2027 agreement seed + congestion analytics for the 跨省绿电 tool (Spec §6).

AGREEMENTS_2027 is the curated, code-seeded layer of 2027 跨省绿电 agreements:
9 path agreements + 1 display-only context row (甘肃 2026 green-deal total,
path_id None — automatically excluded from congestion scoring by the no_path
logic). Every row carries source_type / source_ref / confidence.

path_id is a key into params.PATHS where a preset maps the channel; rows whose
channel has no preset (甘浙线 / 金永直流 is not in PATHS) carry path_id None and
therefore stop with "无路径" in reference_landed and no_path=True in scoring.

Analytics helpers are pure pandas-free functions:
- profile_hours: resource_mix -> RT hour bucket (PROFILE_BUCKETS)
- monthly_avg_rt: (datetime-like, price) rows -> [("YYYY-MM", avg)] per month,
  optionally filtered to an hour-of-day set; months with no matching rows drop out
- congestion_scores: rows sorted by score = spread x volume desc (None last;
  ties broken by wider spread first — same congestion value, bigger per-MWh pain)
- undershoot_flags: trailing-12-month actual < threshold x contracted volume
- seasonality_shares: 12 calendar-month shares; <24 months of data -> flat 1/12
"""

from __future__ import annotations

from .engine import CalcResult, compute_landed
from .params import PATHS, PROFILE_BUCKETS

YEAR = 2027

_INTEL = "用户行业信息 2026-10"

AGREEMENTS_2027: list[dict] = [
    {"sender": "宁夏", "receiver": "福建", "path_id": "西北—灵绍—华东",
     "channel_label": "灵绍直流", "volume_100gwh": 20.0, "green": True,
     "resource_mix": "solar", "source_type": "公告",
     "source_ref": "公告（sohu、解放日报）", "confidence": "公告确认"},
    {"sender": "青海", "receiver": "福建", "path_id": "青豫—华中—宜华—华东",
     "channel_label": "青豫直流", "volume_100gwh": 10.0, "green": True,
     "resource_mix": "solar", "source_type": "行业信息",
     "source_ref": _INTEL, "confidence": "行业信息"},
    {"sender": "新疆", "receiver": "上海", "path_id": "吉泉—华东",
     "channel_label": "吉泉直流", "volume_100gwh": 25.0, "green": True,
     "resource_mix": "solar", "source_type": "行业信息",
     "source_ref": _INTEL + "（公开背景：累计已送20亿）", "confidence": "行业信息"},
    {"sender": "甘肃", "receiver": "上海", "path_id": "西北—庆东—华北—锡泰—华东",
     "channel_label": "庆东直流", "volume_100gwh": 20.0, "green": True,
     "resource_mix": "solar", "source_type": "行业信息",
     "source_ref": _INTEL, "confidence": "行业信息"},
    {"sender": "甘肃", "receiver": "上海", "path_id": "西北—宝合—华东",
     "channel_label": "宝合直流", "volume_100gwh": 20.0, "green": True,
     "resource_mix": "solar", "source_type": "行业信息",
     "source_ref": _INTEL, "confidence": "行业信息"},
    {"sender": "甘肃", "receiver": "上海", "path_id": None,
     "channel_label": "甘浙线", "volume_100gwh": 10.0, "green": True,
     "resource_mix": "solar", "source_type": "公告",
     "source_ref": "公告（陇电入浙 2027 投运，H2-2027 起量）", "confidence": "公告确认"},
    {"sender": "陕西", "receiver": "上海", "path_id": "西北—陕武—华中—葛南—华东",
     "channel_label": "陕武直流", "volume_100gwh": 7.5, "green": True,
     "resource_mix": "mixed", "source_type": "行业信息",
     "source_ref": _INTEL + "（7.5±2.5 区间）", "confidence": "行业信息"},
    {"sender": "山西", "receiver": "上海", "path_id": "雁淮—华东",
     "channel_label": "雁淮直流", "volume_100gwh": 10.0, "green": False,
     "resource_mix": "mixed", "source_type": "行业信息",
     "source_ref": _INTEL + "（煤电+新能源打捆）", "confidence": "行业信息"},
    {"sender": "青海", "receiver": "上海", "path_id": "青豫—华中—宜华—华东",
     "channel_label": "青豫直流", "volume_100gwh": 40.0, "green": True,
     "resource_mix": "solar", "source_type": "公告",
     "source_ref": "公告（北京日报；2026–2028 每年40亿）", "confidence": "公告确认"},
    # Display-only context row: 甘肃 2026 green deals total to 7 provinces.
    # path_id None -> excluded from congestion scoring by no_path logic.
    {"sender": "甘肃", "receiver": "多省", "path_id": None,
     "channel_label": "甘肃2026绿电外送合计", "volume_100gwh": 127.0, "green": True,
     "resource_mix": "solar", "source_type": "公告",
     "source_ref": "公告（甘肃经济网）", "confidence": "公告确认"},
]


def reference_landed(agreement: dict, green_total: float = 240.0,
                     env_price: float = 5.0) -> CalcResult:
    """Landed cost for an agreement at a reference green price.

    province=agreement["sender"], basis=发电侧, path=params.PATHS[path_id].
    path_id None or not in PATHS -> CalcResult(ok=False, stop_reason="无路径").
    """
    path_id = agreement.get("path_id")
    segments = PATHS.get(path_id) if path_id else None
    if segments is None:
        return CalcResult(ok=False, stop_reason="无路径", landed=None, landed_kwh=None,
                          increment=None, tariff_sum=None, loss_sum=None,
                          composite_loss=None, segments=[])
    return compute_landed(province=agreement["sender"], basis="发电侧",
                          path_segments=list(segments),
                          green_total=green_total, env_price=env_price)


def profile_hours(resource_mix: str) -> set[int] | None:
    """Hour-of-day bucket for a resource mix; None = flat (mixed/hydro/unknown)."""
    bucket = PROFILE_BUCKETS.get(resource_mix)
    return set(bucket) if bucket else None


def monthly_avg_rt(rows: list[tuple], hours: set[int] | None = None) -> list[tuple[str, float]]:
    """Average RT price per calendar month from (datetime-like, price) rows.

    hours filters by hour-of-day (None = all hours). Months with no matching
    rows drop out. Input may be unsorted; output is sorted by month.
    """
    acc: dict[str, list[float, int]] = {}
    for ts, price in rows:
        if hours is not None and ts.hour not in hours:
            continue
        key = f"{ts.year:04d}-{ts.month:02d}"
        s, n = acc.get(key, (0.0, 0))
        acc[key] = (s + price, n + 1)
    return [(key, s / n) for key, (s, n) in sorted(acc.items())]


def congestion_scores(agreements: list[dict], spreads: dict[int, float]) -> list[dict]:
    """Score agreements by congestion value: score = spread x volume_100gwh.

    spreads is keyed by agreement INDEX. Rows: {**agreement, "spread", "score",
    "no_path"}. path_id None or unknown -> no_path=True, spread/score None.
    Sorted score desc (None last); equal scores ordered by wider spread first.
    """
    rows = []
    for i, ag in enumerate(agreements):
        path_id = ag.get("path_id")
        no_path = path_id is None or path_id not in PATHS
        spread = None if no_path else spreads.get(i)
        score = spread * ag["volume_100gwh"] if spread is not None else None
        rows.append({**ag, "spread": spread, "score": score, "no_path": no_path})
    rows.sort(key=lambda r: (r["score"] is None,
                             -(r["score"] or 0.0),
                             -(r["spread"] or 0.0)))
    return rows


def undershoot_flags(actual_ttm_100gwh: dict[str, float], agreements: list[dict],
                     threshold: float = 0.8) -> dict[int, bool]:
    """Flag agreements whose trailing-12-month actual < threshold x contracted volume.

    actual keyed by channel_label; missing actual -> False (cannot judge).
    """
    flags = {}
    for i, ag in enumerate(agreements):
        actual = actual_ttm_100gwh.get(ag["channel_label"])
        flags[i] = actual is not None and actual < threshold * ag["volume_100gwh"]
    return flags


def seasonality_shares(monthly_volumes: list[tuple[str, float]]) -> list[float]:
    """12 calendar-month shares from [("YYYY-MM", volume)] history.

    Sums volumes per calendar month across years and normalises. Fewer than
    24 months of data (or zero total) -> flat 1/12 profile.
    """
    flat = [1 / 12] * 12
    if len(monthly_volumes) < 24:
        return flat
    totals = [0.0] * 12
    for ym, vol in monthly_volumes:
        month = int(str(ym)[5:7])
        totals[month - 1] += vol
    grand = sum(totals)
    if grand <= 0:
        return flat
    return [t / grand for t in totals]
