"""Deterministic parser for 江苏电力交易中心 monthly 交易结算单 (discharge side).

Observed on 射阳储能 (B-4) 2024-08..2026-09. Page 1 holds the summary table

    结算科目编码 结算科目 计划电量 结算电量/容量 结算电价/均价 结算电费

with 2-digit level-1 groups (01 电量清分, 02 权益凭证类交易及容量分摊、补偿费用
清分, 03 结算调整) and longer-coded leaves (0101010301 优先发电, 0103010101
调频辅助服务(分摊), 02020301/02020304 储能补偿费用, 0202020401 非计划停运
收益返还, 020601/020602 两个细则 ...), closed by the 合计 row. Later pages
(交易结算明细 / 其他结算明细) restate the leaves per contract / source month.

Vision stored the level-1 row AND its leaf (every month double-counted), so the
rule here is: emit LEAF rows only (a level-1 group with a non-zero amount and no
leaf is emitted as itself), and require sum(amount_cny) == 合计 within 0.01 —
otherwise return [] so nothing is ingested wrongly.

The text layer is interleaved with seal / date-stamp glyphs ("日 10:8:30",
"月5 年9", "è é 2025", "yjsyc n", an IP) that sometimes split a row
character-by-character. Recovery ladder for a leaf whose summary row is
unreadable: (1) its level-1 amount minus the group's readable leaves when it is
the group's only unreadable leaf, else (2) the same code's rows in the detail
pages. 合计 comes from the 合计 row, the header 结算电费, and 电费大写金额 —
at least two must agree.
"""
from __future__ import annotations

import re
from typing import Any

_AMT = re.compile(r"^-?\d+\.\d{2}$")
_VOL = re.compile(r"^-?\d+\.\d{3}$")
_PRICE = re.compile(r"^-?\d+\.\d{6}$")
_LEAF_CODE = re.compile(r"^0\d{5,}$")
_LEVEL1 = {"01": "电量清分", "02": "权益凭证类交易及容量分摊、补偿费用清分", "03": "结算调整"}
_NOISE_CHARS = re.compile(r"[èé±§°]")
_NOISE_TOKEN = re.compile(
    r"^(?:日|[年月]\d*|\d*:\d+:\d+:?|\d+:\d*|yjsyc|n|\d+\.\d+\.\d+\.\d+|2024|2025|2026|024|025|026)$")
_KNOWN_NAMES = {
    "020601": "两个细则_并网运行管理细则结算费用",
    "020602": "两个细则_辅助服务管理细则结算费用",
}


def is_jiangsu_trade_statement(text: str) -> bool:
    # stamps can split the title ("交易\nè\né结算单", 2025-05) — match on compacted text
    compact = re.sub(r"[\sèé±§°]", "", text)
    return "江苏电力交易中心" in compact and "交易结算单" in compact


# --- text cleaning ---------------------------------------------------------

def _tokens(line: str) -> list[str]:
    line = _NOISE_CHARS.sub(" ", line).replace("yjsyc n", " ")
    return [t for t in line.split() if not _NOISE_TOKEN.match(t)]


def _clean(line: str) -> str:
    return " ".join(_tokens(line))


def _sections(text: str) -> tuple[list[str], list[str]]:
    """(summary lines, detail lines), both cleaned."""
    lines = text.splitlines()
    i0 = next((i for i, l in enumerate(lines) if "结算科目编码" in l), None)
    if i0 is None:
        return [], []
    i1 = next((i for i in range(i0, len(lines)) if "交易结算明细" in lines[i]), len(lines))
    i2 = next((i for i in range(i1, len(lines))
               if any(k in lines[i] for k in ("应急分时结算明细", "按日结算明细", "抽水蓄能"))),
              len(lines))
    summary = [_clean(l) for l in lines[i0:i1]]
    detail = [_clean(l) for l in lines[i1:i2]]
    return [l for l in summary if l], [l for l in detail if l]


# --- totals ----------------------------------------------------------------

_CN_DIGIT = {"零": 0, "壹": 1, "贰": 2, "叁": 3, "肆": 4, "伍": 5, "陆": 6, "柒": 7, "捌": 8, "玖": 9}
_CN_UNIT = {"拾": 10, "佰": 100, "仟": 1000}


def _daxie_to_float(s: str) -> float | None:
    s = "".join(c for c in s if c in "零壹贰叁肆伍陆柒捌玖拾佰仟万亿圆元角分整负")
    if not s or not any(c in s for c in "圆元角分"):
        return None
    neg = s.startswith("负")
    s = s.lstrip("负")
    yuan_part, _, frac = s.replace("元", "圆").partition("圆")
    if "圆" not in s.replace("元", "圆"):
        yuan_part, frac = "", s
    total = section = digit = 0
    for c in yuan_part:
        if c in _CN_DIGIT:
            digit = _CN_DIGIT[c]
        elif c in _CN_UNIT:
            section += (digit or 1) * _CN_UNIT[c]
            digit = 0
        elif c == "万":
            total += (section + digit) * 10_000
            section = digit = 0
        elif c == "亿":
            total = (total + section + digit) * 100_000_000
            section = digit = 0
    total += section + digit
    cents = 0
    m = re.search(r"([壹贰叁肆伍陆柒捌玖])角", frac)
    if m:
        cents += _CN_DIGIT[m.group(1)] * 10
    m = re.search(r"([壹贰叁肆伍陆柒捌玖])分", frac)
    if m:
        cents += _CN_DIGIT[m.group(1)]
    val = round(total + cents / 100.0, 2)
    return -val if neg else val


def _total_daxie(text: str) -> float | None:
    lines = text.splitlines()
    for i, l in enumerate(lines):
        if "电费大写金额" in l:
            for cand in [l.split("电费大写金额", 1)[1]] + lines[i + 1:i + 6]:
                v = _daxie_to_float(cand)
                if v is not None:
                    return v
            return None
    return None


def _total_header(text: str) -> float | None:
    head = text.split("电费大写金额", 1)[0]
    for l in head.splitlines():
        toks = _tokens(l)
        for j in range(len(toks) - 1):
            if toks[j] == "-" and _AMT.match(toks[j + 1]):
                return float(toks[j + 1])
    return None


def _total_heji(summary: list[str]) -> float | None:
    for i, l in enumerate(summary):
        if "合计" in l:
            joined = []
            for x in summary[i:i + 8]:
                if x.startswith("备注"):
                    break
                joined.append(x)
            # 合计 <plan> <volume> <price|-> <amount>: plan/volume carry 3 decimals and
            # price 6, so the first 2-decimal token is the amount even when stamp
            # digits land in between (agreement with 大写/header gates it anyway).
            toks = " ".join(joined).split("合计", 1)[1].split()
            return next((float(t) for t in toks if _AMT.match(t)), None)
    return None


def jiangsu_total(text: str) -> float | None:
    """Bill total (合计). Needs two of {合计 row, header 结算电费, 电费大写金额} to agree."""
    summary, _ = _sections(text)
    cands = [v for v in (_total_heji(summary), _total_header(text), _total_daxie(text))
             if v is not None]
    for i, a in enumerate(cands):
        if any(abs(a - b) < 0.005 for b in cands[i + 1:]):
            return a
    return None


# --- rows ------------------------------------------------------------------

def _category(code: str, name: str) -> str:
    if code.startswith("0206") or "两个细则" in name:   # owner 2026-11-19: 两个细则 ≠ 调频
        return "two_rules"
    if "调频" in name:
        return "frequency"
    if "储能补偿" in name:
        return "capacity_compensation"
    if "非计划停运" in name:
        return "rebate"
    if code.startswith("01"):
        return "discharge_energy"
    return "other"


def _row_tail(toks: list[str]) -> tuple[float | None, float | None, float | None, bool]:
    """Parse '... [vol] <price|-> <amount|->' at the end of a token list.

    Returns (vol, price, amount, ok). amount None + ok = printed '-' (zero row).
    """
    if not toks:
        return None, None, None, False
    last = toks[-1]
    if last == "-":
        return None, None, None, len(toks) >= 2 and (toks[-2] == "-" or bool(_PRICE.match(toks[-2])))
    if not _AMT.match(last) or len(toks) < 2:
        return None, None, None, False
    p = toks[-2]
    if p != "-" and not _PRICE.match(p):
        return None, None, None, False
    price = float(p) if p != "-" else None
    vol = float(toks[-3]) if len(toks) >= 3 and _VOL.match(toks[-3]) else None
    return vol, price, float(last), True


def _summary_rows(summary: list[str]) -> tuple[dict, dict, set]:
    """(clean leaves {code: row}, level-1 amounts {code: amt|0.0}, seen-but-unread leaf codes)."""
    leaves: dict[str, dict] = {}
    level1: dict[str, float] = {}
    seen: set[str] = set()
    for i, l in enumerate(summary):
        toks = l.split()
        if not toks:
            continue
        code = toks[0]
        if _LEAF_CODE.match(code):
            name_toks = []
            for t in toks[1:]:
                if t == "-" or re.search(r"\d", t):
                    break
                name_toks.append(t)
            vol, price, amt, ok = _row_tail(toks[1 + len(name_toks):])
            if not ok:
                seen.add(code)
                continue
            name = "".join(name_toks) or _KNOWN_NAMES.get(code) or _wrapped_name(summary, i)
            leaves[code] = {"name": name, "vol": vol, "price": price, "amt": amt}
        elif code in _LEVEL1 and code not in level1:
            # the amount may sit on a later line (stamp-split row): scan to the next row
            for x in summary[i:i + 12]:
                xt = x.split()
                if x is not l and xt and (_LEAF_CODE.match(xt[0]) or xt[0] in _LEVEL1
                                          or "合计" in x):
                    break
                vol, price, amt, ok = _row_tail(xt)
                if ok:
                    level1[code] = amt or 0.0
                    break
    return leaves, level1, seen


def _wrapped_name(summary: list[str], i: int) -> str:
    """Name printed on the lines around a code-only row ('两个细则_…结算' / code / '费用')."""
    parts = []
    for j in (i - 1, i + 1):
        if 0 <= j < len(summary):
            s = summary[j]
            if not re.search(r"\d", s) and "清分" not in s and "合计" not in s:
                parts.append(s.replace(" ", ""))
    return "".join(parts)


def _detail_rows(detail: list[str]) -> dict[str, list[dict]]:
    """Leaf rows from 交易结算明细 / 其他结算明细: code name YYYY-MM-01 ... amount."""
    out: dict[str, list[dict]] = {}
    for l in detail:
        m = re.match(r"^(0\d{5,})\s+(.*?)\s*(\d{4}-\d{2}-\d{2})\s+(.*)$", l)
        if not m:
            continue
        code, name, rest = m.group(1), m.group(2).replace(" ", ""), m.group(4).split()
        # 交易结算明细: plan vol price amount ; 其他结算明细: 单元名称 amount 备注…
        if len(rest) >= 4 and _VOL.match(rest[0]) and _VOL.match(rest[1]) \
                and (rest[2] == "-" or _PRICE.match(rest[2])) and _AMT.match(rest[3]):
            row = {"name": name, "vol": float(rest[1]),
                   "price": float(rest[2]) if rest[2] != "-" else None, "amt": float(rest[3])}
        else:
            amt = next((t for t in rest if _AMT.match(t)), None)
            if amt is None:
                continue
            row = {"name": name, "vol": None, "price": None, "amt": float(amt)}
        out.setdefault(code, []).append(row)
    return out


def _item(code: str, row: dict) -> dict[str, Any]:
    name = row["name"] or _KNOWN_NAMES.get(code) or f"科目{code}"
    cat = _category(code, name)
    energy = cat == "discharge_energy"
    return {
        "category": cat,
        "volume_mwh": row["vol"] if energy else None,
        "price_cny_kwh": round(row["price"] / 1000.0, 6) if energy and row["price"] is not None else None,
        "amount_cny": round(row["amt"], 2),
        "notes": f"放电结算: {name}",
    }


def parse_jiangsu_trade_statement(text: str) -> list[dict]:
    """Leaf items of a 江苏电力交易中心 交易结算单; [] unless they tie to 合计."""
    total = jiangsu_total(text)
    if total is None:
        return []
    summary, detail = _sections(text)
    leaves, level1, seen = _summary_rows(summary)
    drows = _detail_rows(detail)

    # Unreadable summary leaves: codes seen in the summary, plus detail codes absent
    # from the readable summary. A truncated summary code merges into its detail code.
    unresolved = {c for c in drows if c not in leaves}
    for c in seen:
        if c in leaves:
            continue
        full = next((d for d in drows if d != c and d.endswith(c)), None)
        unresolved.add(full or c)

    rows: dict[str, dict] = dict(leaves)
    for grp in _LEVEL1:
        missing = sorted(c for c in unresolved if c[:2] == grp)
        if not missing:
            continue
        known = sum(r["amt"] for c, r in leaves.items() if c[:2] == grp)
        dsum = {c: round(sum(r["amt"] for r in drows.get(c, [])), 2) for c in missing}
        if grp in level1 and len(missing) == 1:
            c = missing[0]
            amt = round(level1[grp] - known, 2)
            d = drows.get(c, [])
            base = d[0] if len(d) == 1 and abs(d[0]["amt"] - amt) < 0.005 else \
                {"name": d[0]["name"] if d else _KNOWN_NAMES.get(c, ""), "vol": None, "price": None}
            rows[c] = {**base, "amt": amt}
        else:
            for c in missing:
                d = drows.get(c)
                if not d:
                    return []
                rows[c] = d[0] if len(d) == 1 else {
                    "name": d[0]["name"], "vol": None, "price": None, "amt": dsum[c]}

    items: list[dict] = []
    for grp, gname in _LEVEL1.items():
        g = {c: r for c, r in rows.items() if c[:2] == grp}
        if not g:
            amt = level1.get(grp) or 0.0
            if abs(amt) >= 0.005:
                items.append(_item(grp, {"name": gname, "vol": None, "price": None, "amt": amt}))
            continue
        for c in sorted(g):
            if abs(g[c]["amt"]) >= 0.005:
                items.append(_item(c, g[c]))

    if abs(sum(i["amount_cny"] for i in items) - total) > 0.01:
        return []
    return items
