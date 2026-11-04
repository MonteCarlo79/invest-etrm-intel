# services/province_fundamentals/registry.py
"""解析器注册表 — 把月报工作簿派发给该省注册的确定性解析器.

派发规则 (spec 写入路径 1/2: 注册表优先, 未命中走 vision 兜底):
  1. 省份: 已注册省份名出现在文件路径里 (文件名或父目录, 如 <省>月报/);
     多重命中取最长名 (河北南网 不被 河北 截胡);
  2. 格式: 由该省 parser 的 sniff() 探工作簿结构 (sheet 名 + 表头标记),
     永不看文件名模式;
  3. 省份未注册 / sniff 不命中 / 文件不可读 / parser 抛错 → 返回 [] 并 log,
     由调用方 (intake) 走 vision 兜底.

Parser 鸭子类型: .parse(path, province) -> list[FundRow], .sniff(path) -> bool,
.FORMAT_NAME: str. 一个省可注册多个 parser (一省可同时存在多种月报版式,
如安徽 格式 A 月报数据 + 格式 B 装机-月), 按注册顺序逐个 sniff, 首个命中者解析.

全国册 (单文件多省, 如交接表 2025) 走 register_national: 路径无省份名, 省份
嗅探不适用 — 省 parser 全部 miss 后 (或本无省份命中时) 由 national parser 补探,
province 形参传 None (全国册自带省份列, 忽略该形参).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol

from services.province_fundamentals.models import FundRow

log = logging.getLogger(__name__)


class _Parser(Protocol):
    FORMAT_NAME: str

    def sniff(self, path: Path) -> bool: ...

    def parse(self, path: Path, province: str | None) -> list[FundRow]: ...


_REGISTRY: dict[str, list[_Parser]] = {}
_NATIONAL: list[_Parser] = []


def register(province: str, parser: _Parser) -> None:
    _REGISTRY.setdefault(province, []).append(parser)


def register_national(parser: _Parser) -> None:
    _NATIONAL.append(parser)


def known_formats() -> list[str]:
    return sorted({p.FORMAT_NAME for ps in _REGISTRY.values() for p in ps}
                  | {p.FORMAT_NAME for p in _NATIONAL})


def dispatch_all(path) -> list[tuple[str, list[FundRow]]]:
    """Multi-hit dispatch: 一个工作簿可承载多类内容(聚合册的披露 + 省间).

    首个 sniff 命中者先解析(与 dispatch() 兼容), 后续命中者继续追加;
    未命中任何 → []; 单 parser 抛错/空 → 该格式记入结果为空行, 不中断其余.
    (wave-2 修复: 原 dispatch() 首个命中即止, 聚合册的省间解析器永远轮不到.)
    """
    path = Path(path)
    province = _detect_province(path)
    parsers: list[_Parser] = []
    if province is not None:
        parsers.extend(_REGISTRY[province])
    parsers.extend(_NATIONAL)
    out: list[tuple[str, list[FundRow]]] = []
    for parser in parsers:
        try:
            hit = parser.sniff(path)
        except Exception:
            log.exception("dispatch_all: %s sniff failed on %s",
                          parser.FORMAT_NAME, path)
            continue
        if not hit:
            continue
        try:
            out.append((parser.FORMAT_NAME, parser.parse(path, province)))
        except Exception:
            log.exception("dispatch_all: %s parser failed on %s",
                          parser.FORMAT_NAME, path)
            out.append((parser.FORMAT_NAME, []))
    if not out:
        log.info("dispatch_all: no parser sniff-hit %s", path)
    return out


def parse_file(path) -> list[FundRow]:
    """嗅探派发. 任何不命中/失败 → [] 并 log (不抛)."""
    return dispatch(path)[1]


def dispatch(path) -> tuple[str | None, list[FundRow]]:
    """嗅探派发 + 命中格式名 (回填 runner dry-run 判定要打印格式).

    返回 (FORMAT_NAME, rows); 未命中 → (None, []); parser 命中但抛错/空
    → (FORMAT_NAME, []) 并 log. 首个 sniff 命中者解析, 不fall-through.
    """
    path = Path(path)
    province = _detect_province(path)
    parsers: list[_Parser] = []
    if province is not None:
        parsers.extend(_REGISTRY[province])
    parsers.extend(_NATIONAL)
    if not parsers:
        log.info("dispatch: no registered province in path %s", path)
        return None, []
    for parser in parsers:
        if not parser.sniff(path):
            continue
        try:
            return parser.FORMAT_NAME, parser.parse(path, province)
        except Exception:
            log.exception("dispatch: %s parser failed on %s",
                          parser.FORMAT_NAME, path)
            return parser.FORMAT_NAME, []
    if province is not None:
        log.info("dispatch: %s matched province %s but format sniff failed",
                 path.name, province)
    else:
        log.info("dispatch: no parser sniff-hit %s", path)
    return None, []


def _detect_province(path: Path) -> str | None:
    text = str(path)
    hits = [p for p in _REGISTRY if p in text]
    return max(hits, key=len) if hits else None


# 内建注册 (确定性, import 时无 IO)
from services.province_fundamentals.parsers import format_a as _format_a  # noqa: E402
from services.province_fundamentals.parsers import format_b as _format_b  # noqa: E402
from services.province_fundamentals.parsers import jiaojie_2025 as _jiaojie  # noqa: E402
from services.province_fundamentals.parsers import summary_2025 as _summary  # noqa: E402
from services.province_fundamentals.parsers import month_rows as _month_rows  # noqa: E402
from services.province_fundamentals.parsers import prov_guangdong as _prov_gd  # noqa: E402
from services.province_fundamentals.parsers import prov_guangxi as _prov_gx  # noqa: E402
from services.province_fundamentals.parsers import prov_jiangsu as _prov_js  # noqa: E402
from services.province_fundamentals.parsers import prov_interprov as _prov_ip  # noqa: E402
from services.province_fundamentals.parsers import prov_interprov2 as _prov_ip2  # noqa: E402

register("安徽", _format_a)
register("安徽", _format_b)
register("山东", _month_rows.shandong_parser)
register("山西", _month_rows.shanxi_parser)
register("陕西", _month_rows.shaanxi_parser)
register("蒙西", _month_rows.mengxi_parser)
register("辽宁", _month_rows.liaoning_parser)
register("河北南网", _month_rows.jinan_parser)
register("蒙东", _month_rows.mengdong_parser)
register("甘肃", _month_rows.gansu_parser)
register("冀北", _month_rows.jibei_parser)
register("广东", _prov_gd)
register("广西", _prov_gx)
register("江苏", _prov_js)
# 省间旁册 wave 1 (专用 side files): 注册在披露 parser 之后 — 披露册由前者命中,
# 旁册 sheet 集与披露册互不相同 (sniff 隔离, 见 prov_interprov docstring)
register("安徽", _prov_ip.anhui_parser)
register("江苏", _prov_ip.jiangsu_parser)
register("河北南网", _prov_ip.jinan_parser)
register("冀北", _prov_ip.jibei_parser)
# 省间 wave 2 (聚合册内省间 sheet): 注册在披露 parser 之后 — 披露册 dispatch 仍归
# 披露 parser (首个命中), 本波 parser 由直接调用触发; 陕西结算册无披露 parser 命中,
# dispatch 自然落本波. sniff 以册内目标 sheet 名隔离, 见 prov_interprov2 docstring
register("蒙东", _prov_ip2.mengdong_parser)
register("甘肃", _prov_ip2.gansu_parser)
register("山西", _prov_ip2.shanxi_parser)
register("陕西", _prov_ip2.shaanxi_parser)
register("辽宁", _prov_ip2.liaoning_parser)
register("山东", _prov_ip2.shandong_parser)
register("广西", _prov_ip2.guangxi_parser)
register_national(_jiaojie)
register_national(_summary)
