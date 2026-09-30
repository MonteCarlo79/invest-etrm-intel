# services/retail_risk/loader.py
"""Single DB writer for retail-risk ingestion. Parsers never touch the DB."""
from __future__ import annotations

import datetime
import hashlib
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from services.retail_risk import schemas


def get_engine():
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", "config", ".env"), override=False)
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        raise RuntimeError("PGURL or DB_DSN not configured")
    return create_engine(url, pool_pre_ping=True)


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def get_or_create_book(conn, province: str) -> int:
    name = schemas.book_name(province)
    bid = conn.execute(text(
        "SELECT id FROM marketdata.rm_books WHERE name = :n AND book_type = 'load'"
    ), {"n": name}).scalar()
    if bid is not None:
        return bid
    return conn.execute(text(
        "INSERT INTO marketdata.rm_books (name, book_type, asset_id, description) "
        "VALUES (:n, 'load', NULL, :d) RETURNING id"
    ), {"n": name, "d": f"Retail load book for {province} (景融绿色能源售电)"}).scalar()


def rollup_trades_day(df: pd.DataFrame) -> pd.DataFrame:
    """Hour-level trades -> day × channel × direction × counterparty, VWAP price.

    Signed VWAP: negative prices (合同转让 credits) preserved. Status open/closed
    by delivery_date vs today.
    """
    if df.empty:
        return df
    df = df.copy()
    df["delivery_date"] = pd.to_datetime(df["delivery_date"]).dt.date
    df["pv"] = df["volume_mwh"] * df["price_cny_mwh"]
    g = df.groupby(["delivery_date", "channel", "instrument_type", "direction",
                    "counterparty"], dropna=False, as_index=False)
    rolled = g.agg(volume_mwh=("volume_mwh", "sum"), pv=("pv", "sum"))
    rolled["price_cny_mwh"] = rolled["pv"] / rolled["volume_mwh"].replace(0, float("nan"))
    today = datetime.date.today()
    rolled["status"] = rolled["delivery_date"].apply(lambda d: "closed" if d < today else "open")
    return rolled.drop(columns=["pv"])


def write_trades(conn, book_id: int, df: pd.DataFrame, batch_id: str, province: str) -> int:
    rolled = rollup_trades_day(df)
    rolled = rolled[rolled["volume_mwh"] > 0]          # drop zero/NaN-price artefacts
    conn.execute(text(
        "DELETE FROM marketdata.rm_positions WHERE book_id = :b AND upload_batch_id = :bid"
    ), {"b": book_id, "bid": batch_id})
    n = 0
    for r in rolled.itertuples(index=False):
        px = None if pd.isna(r.price_cny_mwh) else r.price_cny_mwh
        conn.execute(text("""
            INSERT INTO marketdata.rm_positions
              (book_id, instrument_type, province, channel, direction, volume_mwh,
               price_cny_mwh, start_date, end_date, counterparty, status, upload_batch_id)
            VALUES (:b, :it, :prov, :ch, :di, :vol, :px, :d, :d, :cp, :st, :bid)
        """), {"b": book_id, "it": r.instrument_type, "prov": province, "ch": r.channel,
               "di": r.direction, "vol": r.volume_mwh, "px": px,
               "d": r.delivery_date, "cp": r.counterparty, "st": r.status, "bid": batch_id})
        n += 1
    return n


def write_volumes(conn, book_id: int, df: pd.DataFrame, batch_id: str) -> int:
    n = 0
    for r in df.itertuples(index=False):
        conn.execute(text("""
            INSERT INTO marketdata.rm_position_volumes
              (book_id, delivery_date, hour, annual_volume_mwh, annual_price_cny_mwh,
               monthly_auction_volume_mwh, monthly_auction_price_cny_mwh,
               monthly_listed_volume_mwh, monthly_listed_price_cny_mwh,
               intramonth_match_volume_mwh, intramonth_match_price_cny_mwh,
               nominated_mwh, settled_mwh, upload_batch_id)
            VALUES (:b, :d, :h, :av, :ap, :mv, :mp, :lv, :lp, :iv, :ip, :nom, :set, :bid)
            ON CONFLICT (book_id, delivery_date, hour) DO UPDATE SET
              annual_volume_mwh = COALESCE(EXCLUDED.annual_volume_mwh, rm_position_volumes.annual_volume_mwh),
              annual_price_cny_mwh = COALESCE(EXCLUDED.annual_price_cny_mwh, rm_position_volumes.annual_price_cny_mwh),
              monthly_auction_volume_mwh = COALESCE(EXCLUDED.monthly_auction_volume_mwh, rm_position_volumes.monthly_auction_volume_mwh),
              monthly_auction_price_cny_mwh = COALESCE(EXCLUDED.monthly_auction_price_cny_mwh, rm_position_volumes.monthly_auction_price_cny_mwh),
              monthly_listed_volume_mwh = COALESCE(EXCLUDED.monthly_listed_volume_mwh, rm_position_volumes.monthly_listed_volume_mwh),
              monthly_listed_price_cny_mwh = COALESCE(EXCLUDED.monthly_listed_price_cny_mwh, rm_position_volumes.monthly_listed_price_cny_mwh),
              intramonth_match_volume_mwh = COALESCE(EXCLUDED.intramonth_match_volume_mwh, rm_position_volumes.intramonth_match_volume_mwh),
              intramonth_match_price_cny_mwh = COALESCE(EXCLUDED.intramonth_match_price_cny_mwh, rm_position_volumes.intramonth_match_price_cny_mwh),
              nominated_mwh = COALESCE(EXCLUDED.nominated_mwh, rm_position_volumes.nominated_mwh),
              settled_mwh = COALESCE(EXCLUDED.settled_mwh, rm_position_volumes.settled_mwh),
              upload_batch_id = EXCLUDED.upload_batch_id
        """), _volume_params(book_id, r, batch_id))
        n += 1
    return n


def _volume_params(book_id, r, batch_id):
    """Map one VOLUMES_COLS row to per-channel column params (channel -> its columns).
    NaN -> None for every numeric (psycopg2 cannot adapt float('nan'))."""
    def _clean(v):
        return None if v is None or pd.isna(v) else v
    p = {"b": book_id, "d": r.delivery_date, "h": int(r.hour), "bid": batch_id,
         "av": None, "ap": None, "mv": None, "mp": None, "lv": None, "lp": None,
         "iv": None, "ip": None,
         "nom": _clean(getattr(r, "nominated_mwh", None)),
         "set": _clean(getattr(r, "settled_mwh", None))}
    prefix = {"annual": "a", "monthly_auction": "m", "monthly_listed": "l",
              "intramonth_match": "i"}.get(r.channel)
    if prefix:
        p[f"{prefix}v"] = _clean(r.volume_mwh)
        p[f"{prefix}p"] = _clean(r.vwap_cny_mwh)
    return p


def write_curves(conn, df: pd.DataFrame) -> int:
    n = 0
    for r in df.itertuples(index=False):
        conn.execute(text("""
            INSERT INTO marketdata.rm_forward_curves
              (province, product, curve_date, delivery_date, delivery_hour, price_cny_kwh, source)
            VALUES (:p, :pr, :cd, :dd, :dh, :px, 'manual')
            ON CONFLICT (province, product, curve_date, delivery_date, delivery_hour, source)
            DO UPDATE SET price_cny_kwh = EXCLUDED.price_cny_kwh
        """), {"p": r.province, "pr": r.product, "cd": r.curve_date,
               "dd": r.delivery_date, "dh": int(r.delivery_hour), "px": r.price_cny_kwh})
        n += 1
    return n


def write_contracts(conn, province: str, df: pd.DataFrame) -> tuple[int, int]:
    n_cust, n_ctr = 0, 0
    for r in df.itertuples(index=False):
        cid = conn.execute(text(
            "SELECT id FROM marketdata.rm_customers WHERE name = :n AND province = :p"
        ), {"n": r.customer_name, "p": province}).scalar()
        if cid is None:
            cid = conn.execute(text("""
                INSERT INTO marketdata.rm_customers (name, province, revenue_share_ratio, status)
                VALUES (:n, :p, :sr, 'active') RETURNING id
            """), {"n": r.customer_name, "p": province,
                   "sr": r.share_ratio if pd.notna(r.share_ratio) else None}).scalar()
            n_cust += 1
        existing = conn.execute(text(
            "SELECT id FROM marketdata.rm_customer_contracts WHERE contract_ref = :cr"
        ), {"cr": r.contract_ref}).scalar()
        if existing is not None:
            continue
        conn.execute(text("""
            INSERT INTO marketdata.rm_customer_contracts
              (customer_id, contract_ref, contract_type, price_cny_mwh,
               start_date, end_date, annual_forecast_mwh, monthly_forecast, contract_status)
            VALUES (:cid, :cr, :ct, :px, :sd, :ed, :am, CAST(:mf AS jsonb), 'active')
        """), {"cid": cid, "cr": r.contract_ref, "ct": r.contract_type,
               "px": r.price_cny_mwh if pd.notna(r.price_cny_mwh) else None,
               "sd": r.start_date, "ed": r.end_date,
               "am": r.annual_mwh if pd.notna(r.annual_mwh) else None,
               "mf": r.monthly_mwh if isinstance(r.monthly_mwh, str) else "{}"})
        n_ctr += 1
    return n_cust, n_ctr


def write_invoice(conn, book_id: int, doc: schemas.InvoiceDoc,
                  file_name: str, file_hash: str) -> int | None:
    """Insert settlement + items. Returns settlement id, or None if hash already ingested.

    Cross-check (hierarchy-aware): the printed 本月 figure is the MARGIN, and naive
    Σitems double counts nested subject lines, so instead: when a '01'-coded top
    line with an amount exists, the midlong+spot top lines (min code length per
    category) must tie it within 1% — mismatch, or zero parsed items, -> 'flagged'.
    """
    from services.retail_risk.reconcile import invoice_by_category_frame

    dup = conn.execute(text(
        "SELECT id FROM marketdata.rm_settlements WHERE raw_data->>'file_hash' = :h"
    ), {"h": file_hash}).scalar()
    if dup is not None:
        return None
    status = "processed"
    if not doc.items:
        status = "flagged"
    else:
        top01 = next((i for i in doc.items
                      if (i.get("notes") or "").startswith("01")
                      and len(i.get("notes") or "") == 2
                      and i.get("amount_cny") is not None), None)
        if top01 is not None:
            by_cat = invoice_by_category_frame(pd.DataFrame(doc.items))
            tie = by_cat.get("midlong_energy", 0.0) + by_cat.get("spot_energy", 0.0)
            if abs(tie - top01["amount_cny"]) > max(0.01 * abs(top01["amount_cny"]), 1.0):
                status = "flagged"
    sid = conn.execute(text("""
        INSERT INTO marketdata.rm_settlements
          (book_id, settlement_month, file_name, file_type, status, total_amount_cny, raw_data)
        VALUES (:b, :m, :f, :ft, :st, :tot, CAST(:raw AS jsonb)) RETURNING id
    """), {"b": book_id, "m": doc.settlement_month, "f": file_name,
           "ft": "pdf" if file_name.lower().endswith(".pdf") else "excel",
           "st": status, "tot": doc.total_amount_cny,
           "raw": '{"file_hash": "' + file_hash + '"}'}).scalar()
    for i in doc.items:
        conn.execute(text("""
            INSERT INTO marketdata.rm_settlement_items
              (settlement_id, category, delivery_date, volume_mwh, price_cny_kwh, amount_cny, notes)
            VALUES (:sid, :cat, :dd, :vol, :px, :amt, :notes)
        """), {"sid": sid, "cat": i["category"], "dd": i.get("delivery_date"),
               "vol": i.get("volume_mwh"),
               "px": (i["price_cny_mwh"] / 1000.0) if i.get("price_cny_mwh") is not None else None,
               "amt": i["amount_cny"],
               "notes": (i.get("label_cn") or "") + ((" | " + i["notes"]) if i.get("notes") else "")})
    return sid


def write_benchmarks(conn, df: pd.DataFrame) -> int:
    n = 0
    for r in df.itertuples(index=False):
        conn.execute(text("""
            INSERT INTO marketdata.rm_market_benchmarks
              (province, channel, month, avg_price_cny_mwh, volume_mwh, source, source_file)
            VALUES (:p, :c, :m, :px, :vol, 'infohub', :sf)
            ON CONFLICT (province, channel, month, source)
            DO UPDATE SET avg_price_cny_mwh = EXCLUDED.avg_price_cny_mwh,
                          volume_mwh = EXCLUDED.volume_mwh,
                          source_file = EXCLUDED.source_file
        """), {"p": r.province, "c": r.channel, "m": r.month, "px": r.avg_price_cny_mwh,
               "vol": r.volume_mwh if pd.notna(r.volume_mwh) else None, "sf": r.source_file})
        n += 1
    return n
