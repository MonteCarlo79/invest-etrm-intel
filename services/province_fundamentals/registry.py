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
.FORMAT_NAME: str. 一个省注册一个 parser (各省版式互不相同但逐月稳定).
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

    def parse(self, path: Path, province: str) -> list[FundRow]: ...


_REGISTRY: dict[str, _Parser] = {}


def register(province: str, parser: _Parser) -> None:
    _REGISTRY[province] = parser


def known_formats() -> list[str]:
    return sorted({p.FORMAT_NAME for p in _REGISTRY.values()})


def parse_file(path) -> list[FundRow]:
    """嗅探派发. 任何不命中/失败 → [] 并 log (不抛)."""
    path = Path(path)
    province = _detect_province(path)
    if province is None:
        log.info("parse_file: no registered province in path %s", path)
        return []
    parser = _REGISTRY[province]
    try:
        if not parser.sniff(path):
            log.info("parse_file: %s matched province %s but format sniff failed",
                     path.name, province)
            return []
        return parser.parse(path, province)
    except Exception:
        log.exception("parse_file: %s parser failed on %s",
                      parser.FORMAT_NAME, path)
        return []


def _detect_province(path: Path) -> str | None:
    text = str(path)
    hits = [p for p in _REGISTRY if p in text]
    return max(hits, key=len) if hits else None


# 内建注册 (确定性, import 时无 IO)
from services.province_fundamentals.parsers import format_a as _format_a  # noqa: E402

register("安徽", _format_a)
