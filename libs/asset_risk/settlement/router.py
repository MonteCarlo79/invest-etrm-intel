"""Settlement-PDF parser router — the ordered (detector, parser) table.

Reproduces the parser ladder in ``apps/asset_risk/tab_settlement.py::_process_pdf``
as data: ``route()`` walks ``ROUTES`` top to bottom and returns the first route
on the branch today's filename classification takes whose detector matches.
Labels are the exact ``st.info``/``st.warning`` text the tab shows today (the
two skip routes omit the ``**<filename>**: `` prefix — the caller adds it).

State-dependent decisions stay with the caller (they need the DB):
- voucher fallback: route() returns it for a 发电侧 voucher in upload mode;
  the caller still checks the book+month has no discharge data.
- SDTC 售电侧: parse returns the sides dict; the caller decides whether
  售电侧 duplicates an existing 电费账单.

Parsers are imported lazily inside the callables so importing this module
stays light. No streamlit, no apps.* imports.
"""
from __future__ import annotations

import copy
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Callable


@dataclass(frozen=True)
class BillText:
    filename: str
    path: str                 # temp or real path (parsers that need the PDF take this)
    is_scanned: bool
    full_text: str            # all pages, "\n"-joined ("" when scanned)
    page1_text: str


@dataclass(frozen=True)
class Route:
    label: str
    kind: str                 # "discharge" | "charge" | "voucher" | "skip"
    detect: Callable[[BillText], bool]
    parse: Callable[[BillText], "list[dict] | dict"]
    needs_text_layer: bool = True
    upload_only: bool = False  # voucher fallback


def read_bill(path: str, filename: str) -> BillText:
    """Extract the text a route needs, the way the tab does today.

    Scanned = page 1 has no chars but has images (today's test). Each page's
    text is followed by "\n", matching the tab's ``bill_text`` construction.
    """
    import pdfplumber

    with pdfplumber.open(path) as pdf:
        p0 = pdf.pages[0]
        is_scanned = len(p0.chars) == 0 and len(p0.images) > 0
        if is_scanned:
            return BillText(filename, path, True, "", "")
        pages = [pg.extract_text() or "" for pg in pdf.pages]
    return BillText(filename, path, False, "".join(t + "\n" for t in pages), pages[0])


def _always(_bill: BillText) -> bool:
    return True


# --- lazy detector / parser adapters -------------------------------------

def _gx(name):
    def f(text):
        from services.settlement_ingest import parser_guangxi
        return getattr(parser_guangxi, name)(text)
    return f


def _wb2(name):
    def f(text):
        from services.settlement_ingest import parser_wb2
        return getattr(parser_wb2, name)(text)
    return f


def _sg(name):
    def f(text):
        from services.settlement_ingest import parser_stategrid
        return getattr(parser_stategrid, name)(text)
    return f


def _gs(name):
    def f(text):
        from services.settlement_ingest import parser_gansu
        return getattr(parser_gansu, name)(text)
    return f


def _on_full(fn):
    return lambda bill: fn(bill.full_text)


def _on_page1(fn):
    return lambda bill: fn(bill.page1_text)


def _parse_voucher(bill):
    from services.settlement_ingest.parser_voucher import parse_generation_voucher
    return parse_generation_voucher(bill.path)


def _parse_discharge_vision(bill):
    from services.settlement_ingest.parser_discharge import parse_discharge_settlement_pdf
    return parse_discharge_settlement_pdf(bill.path)


def _parse_scanned_charge(bill):
    from services.settlement_ingest.parser_scan_charge import parse_scanned_charge_bill_vision
    return parse_scanned_charge_bill_vision(bill.path)


def _parse_charge_vision(bill):
    from services.settlement_ingest.parser_vision import parse_charge_bill_vision
    return parse_charge_bill_vision(bill.path)


@lru_cache(maxsize=16)
def _mengxi_regex_cached(path: str, mtime_ns: int, size: int) -> tuple:
    from services.settlement_ingest import parser_charge
    return tuple(parser_charge.parse_charging_cost_pdf(path) or ())


def _mengxi_regex_items(bill: BillText) -> list:
    """parse_charging_cost_pdf(path), run once per file version (detect + parse share it)."""
    st_ = os.stat(bill.path)
    return copy.deepcopy(list(_mengxi_regex_cached(bill.path, st_.st_mtime_ns, st_.st_size)))


def _detect_mengxi_regex(bill: BillText) -> bool:
    # Ruling R3: missing file / unreadable PDF / parser error -> no match.
    try:
        return bool(_mengxi_regex_items(bill))
    except Exception:
        return False


# --- the ladder -----------------------------------------------------------

_VOUCHER_ROUTES: tuple[Route, ...] = (
    Route(
        label="发电侧结算凭证 — no 上网结算单 for this month; using voucher as discharge source.",
        kind="voucher",
        detect=lambda bill: "发电侧" in bill.filename,
        parse=_parse_voucher,
        needs_text_layer=False,
        upload_only=True,
    ),
)

_DISCHARGE_ROUTES: tuple[Route, ...] = (
    Route(
        label=("电网版上网结算单 — content overlaps the "
               "交易中心结算依据; skipped to avoid double-counting. No data written."),
        kind="skip",
        detect=_on_full(_gx("is_guangxi_grid_duplicate")),
        parse=lambda bill: [],
    ),
    Route(
        label=("交易结算凭证 — its 电能电费 and every fee row "
               "duplicate the grid 上网结算单; skipped to avoid double-counting. "
               "No data written."),
        kind="skip",
        detect=_on_full(_wb2("is_wb2_voucher")),
        parse=lambda bill: [],
    ),
    Route(
        label="蒙西上网电费结算单 (成分明细) detected — parsed deterministically (no vision).",
        kind="discharge",
        detect=_on_full(_wb2("is_wb2_discharge_bill")),
        parse=_on_full(_wb2("parse_wb2_discharge")),
    ),
    Route(
        label="广西电力交易结算依据 detected — parsed deterministically (no vision).",
        kind="discharge",
        detect=_on_full(_gx("is_guangxi_discharge_bill")),
        parse=_on_full(_gx("parse_guangxi_discharge_text")),
    ),
    Route(
        label="网上国网发电侧电费账单 detected — parsed deterministically (no vision).",
        kind="discharge",
        detect=_on_full(_sg("is_stategrid_discharge")),
        parse=_on_page1(_sg("parse_stategrid_discharge")),
    ),
    Route(
        label="山东电力交易中心交易结算单 detected — parsed deterministically (no vision).",
        kind="discharge",
        detect=_on_full(_sg("is_sdtc_settlement")),
        parse=_on_full(_sg("parse_sdtc_settlement")),      # sides dict
    ),
    Route(
        label="安徽电力交易中心(统推)结算单 detected — parsed deterministically (no vision).",
        kind="discharge",
        detect=_on_full(_sg("is_ah_sdtc")),
        parse=_on_full(_sg("parse_ah_sdtc")),              # sides dict
    ),
    Route(
        label="国网安徽发电侧电费账单 detected — parsed deterministically (no vision).",
        kind="discharge",
        detect=_on_full(_sg("is_ah_discharge")),
        parse=_on_full(_sg("parse_ah_discharge")),
    ),
    Route(
        label="山东省地方电厂市场化结算单 detected — parsed deterministically (no vision).",
        kind="discharge",
        detect=_on_full(_sg("is_local_discharge")),
        parse=_on_page1(_sg("parse_local_discharge")),
    ),
    Route(
        label="国网甘肃 bill detected — parsed deterministically (no vision).",
        kind="discharge",
        detect=_on_full(_gs("is_gansu_bill")),
        parse=_on_full(_gs("parse_gansu_discharge_text")),
    ),
    Route(
        label="Detected scanned PDF — using AI Vision to extract data...",
        kind="discharge",
        detect=_always,
        parse=_parse_discharge_vision,
        needs_text_layer=False,
    ),
)

_CHARGE_ROUTES: tuple[Route, ...] = (
    Route(
        label="Scanned 电费计算明细 — vision extraction verified against printed totals.",
        kind="charge",
        detect=lambda bill: bill.is_scanned,
        parse=_parse_scanned_charge,
        needs_text_layer=False,
    ),
    Route(
        label="蒙西下网电费核查票 detected — parsed deterministically (no vision).",
        kind="charge",
        detect=_on_full(_wb2("is_wb2_charge_bill")),
        parse=_on_full(_wb2("parse_wb2_charge")),
    ),
    Route(
        label="广西电网电费通知单 detected — parsed deterministically (no vision).",
        kind="charge",
        detect=_on_full(_gx("is_guangxi_charge_bill")),
        parse=_on_full(_gx("parse_guangxi_charge_text")),
    ),
    Route(
        label="网上国网用电侧电费账单 detected — parsed deterministically (no vision).",
        kind="charge",
        detect=_on_full(_sg("is_stategrid_charge")),
        parse=_on_page1(_sg("parse_stategrid_charge")),
    ),
    Route(
        label="国网甘肃 charge bill detected — parsed deterministically (no vision).",
        kind="charge",
        detect=_on_full(_gs("is_gansu_charge_bill")),
        parse=_on_full(_gs("parse_gansu_charge_text")),
    ),
    Route(
        # Today the Mengxi regex path shows no st.info; this label is internal.
        label="Mengxi charge regex (parse_charging_cost_pdf)",
        kind="charge",
        detect=_detect_mengxi_regex,
        parse=_mengxi_regex_items,
    ),
    Route(
        label="Charge regex parser found nothing — trying AI Vision (generic provincial layout)...",
        kind="charge",
        detect=_always,
        parse=_parse_charge_vision,
    ),
)

ROUTES: tuple[Route, ...] = _VOUCHER_ROUTES + _DISCHARGE_ROUTES + _CHARGE_ROUTES

ROUTE_LABELS: tuple[str, ...] = (
    "发电侧结算凭证 — no 上网结算单 for this month; using voucher as discharge source.",
    "电网版上网结算单 — content overlaps the 交易中心结算依据; skipped to avoid double-counting. No data written.",
    "交易结算凭证 — its 电能电费 and every fee row duplicate the grid 上网结算单; skipped to avoid double-counting. No data written.",
    "蒙西上网电费结算单 (成分明细) detected — parsed deterministically (no vision).",
    "广西电力交易结算依据 detected — parsed deterministically (no vision).",
    "网上国网发电侧电费账单 detected — parsed deterministically (no vision).",
    "山东电力交易中心交易结算单 detected — parsed deterministically (no vision).",
    "安徽电力交易中心(统推)结算单 detected — parsed deterministically (no vision).",
    "国网安徽发电侧电费账单 detected — parsed deterministically (no vision).",
    "山东省地方电厂市场化结算单 detected — parsed deterministically (no vision).",
    "国网甘肃 bill detected — parsed deterministically (no vision).",
    "Detected scanned PDF — using AI Vision to extract data...",
    "Scanned 电费计算明细 — vision extraction verified against printed totals.",
    "蒙西下网电费核查票 detected — parsed deterministically (no vision).",
    "广西电网电费通知单 detected — parsed deterministically (no vision).",
    "网上国网用电侧电费账单 detected — parsed deterministically (no vision).",
    "国网甘肃 charge bill detected — parsed deterministically (no vision).",
    "Mengxi charge regex (parse_charging_cost_pdf)",
    "Charge regex parser found nothing — trying AI Vision (generic provincial layout)...",
)


def _branch(kind_hint: str, is_scanned: bool, mode: str) -> tuple[Route, ...]:
    """The ladder segment today's _process_pdf enters for this classification."""
    if kind_hint == "voucher":
        # Fallback is upload-only; scanner mode skips vouchers (no route).
        return _VOUCHER_ROUTES if mode == "upload" else ()
    if kind_hint == "discharge" or (kind_hint == "unknown" and is_scanned):
        return _DISCHARGE_ROUTES
    if kind_hint in ("charge", "unknown"):
        return _CHARGE_ROUTES
    return ()   # "skip" or anything unrecognised


def route(bill: BillText, kind_hint: str, mode: str) -> Route | None:
    """First matching route on today's branch, or None. Never raises on detection.

    kind_hint: classify_pdf(filename) result; mode: "upload" | "scanner".
    """
    for r in _branch(kind_hint, bill.is_scanned, mode):
        if r.upload_only and mode != "upload":
            continue
        if r.needs_text_layer and bill.is_scanned:
            continue
        try:
            hit = r.detect(bill)
        except Exception:
            hit = False
        if hit:
            return r
    return None
