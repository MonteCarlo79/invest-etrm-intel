"""Landed-cost engine for the 跨省绿电 interconnector pricing tool.

Math (Spec §5, fixed by the golden values in tests/interconnector_pricing/test_engine.py):

    电能量价 E = green_total − env_price
    per segment: 损耗折价 G = D × F / (1 − F);  段后 D' = D + tariff + G
    落地价 = D_last + env_price
    composite loss O = 1 − Π(1 − F_i)

发电侧 prepends a 省网：<province> segment from PROVINCE_GRID; 出省节点 does not.

Every missing/invalid input returns CalcResult(ok=False, stop_reason=<中文原因>)
— never a silent zero. Stop reasons mirror the Excel tool's messages.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .params import PROVINCE_GRID, REGIONAL_GRID, SPECIAL_PROJECTS

BASES: tuple = ("发电侧", "出省节点")

_NON_COMPUTABLE_REASONS = {
    "辛洹线": "专项工程「辛洹线」为容量电价（40 元/kW·年），无电量电价，暂不支持自动测算",
    "云霄": "专项工程「云霄」含容量价（115 元/kW·年）且适用边界特殊（非福建/广东交易），暂不支持自动测算",
}


@dataclass
class SegmentDetail:
    label: str
    pre_cost: float
    tariff: float
    loss_rate: float
    loss_cost: float
    post_cost: float


@dataclass
class CalcResult:
    ok: bool
    stop_reason: str | None
    landed: float | None
    landed_kwh: float | None
    increment: float | None
    tariff_sum: float | None
    loss_sum: float | None
    composite_loss: float | None
    segments: list[SegmentDetail] = field(default_factory=list)


def _stop(reason: str) -> CalcResult:
    return CalcResult(ok=False, stop_reason=reason, landed=None, landed_kwh=None,
                      increment=None, tariff_sum=None, loss_sum=None,
                      composite_loss=None, segments=[])


def _resolve_segment(seg: str) -> tuple[str, float, float] | CalcResult:
    """Resolve a path segment label to (label, tariff, loss_rate)."""
    if not isinstance(seg, str) or "：" not in seg:
        return _stop(f"路径段「{seg}」无法识别：应为 区域：<名称> 或 专项：<名称>")
    prefix, _, name = seg.partition("：")
    if prefix == "区域":
        row = REGIONAL_GRID.get(name)
        if row is None:
            return _stop(f"区域电网「{name}」不在参数表中，请核对路径")
        return seg, row["tariff"], row["loss"]
    if prefix == "专项":
        row = SPECIAL_PROJECTS.get(name)
        if row is None:
            return _stop(f"专项工程「{name}」不在参数表中，请核对路径")
        if not row.get("computable", False):
            return _stop(_NON_COMPUTABLE_REASONS.get(
                name, f"专项工程「{name}」暂不支持自动测算"))
        return seg, row["tariff"], row["loss"]
    return _stop(f"路径段「{seg}」前缀无法识别：仅支持 区域：/专项：")


def compute_landed(*, province: str, basis: str, path_segments: list[str],
                   green_total: float, env_price: float) -> CalcResult:
    # 1) inputs present
    if not province or not basis:
        return _stop("请选择送出省与结算基准（发电侧/出省节点）")
    if green_total is None or env_price is None:
        return _stop("请输入绿电总价与环境价（元/MWh）")

    # 2) env_price within [0, green_total]
    if env_price < 0:
        return _stop("环境价不能为负")
    if env_price > green_total:
        return _stop("环境价高于绿电总价：电能量价为负，交易不成立")

    # 3) province known + priced
    grid = PROVINCE_GRID.get(province)
    if grid is None:
        return _stop(f"送出省「{province}」不在参数表中")
    if not grid.get("priced", True):
        return _stop(f"送出省「{province}」核价结果尚未公布，后续按照相关政策执行，暂无法自动测算")

    # 4) basis valid
    if basis not in BASES:
        return _stop(f"结算基准「{basis}」无效：必须为 发电侧 或 出省节点")

    # 5) path non-empty and every segment resolvable + computable
    if not path_segments:
        return _stop("路径不能为空：请选择至少一段输电路径")

    resolved: list[tuple[str, float, float]] = []
    if basis == "发电侧":
        resolved.append((f"省网：{province}", grid["tariff"], grid["loss"]))
    for seg in path_segments:
        r = _resolve_segment(seg)
        if isinstance(r, CalcResult):
            return r
        resolved.append(r)

    # Compute. E = 电能量价 (green_total − env_price) is the starting D.
    energy_price = green_total - env_price
    d = energy_price
    tariff_sum = 0.0
    loss_sum = 0.0
    composite_keep = 1.0
    segments: list[SegmentDetail] = []
    for label, tariff, loss_rate in resolved:
        loss_cost = d * loss_rate / (1 - loss_rate) if loss_rate else 0.0
        post = d + tariff + loss_cost
        segments.append(SegmentDetail(label=label, pre_cost=d, tariff=tariff,
                                      loss_rate=loss_rate, loss_cost=loss_cost,
                                      post_cost=post))
        tariff_sum += tariff
        loss_sum += loss_cost
        composite_keep *= (1 - loss_rate)
        d = post

    landed = d + env_price
    return CalcResult(
        ok=True,
        stop_reason=None,
        landed=landed,
        landed_kwh=landed / 1000,
        increment=landed - green_total,
        tariff_sum=tariff_sum,
        loss_sum=loss_sum,
        composite_loss=1 - composite_keep,
        segments=segments,
    )
