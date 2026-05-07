from __future__ import annotations

from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

if load_dotenv:
    # search upwards from this file to find .env
    here = Path(__file__).resolve()
    for p in [here.parent] + list(here.parents):
        env_path = p / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            break

import argparse
import datetime as dt
import glob
import os
from typing import Dict, List

import yaml

from tools_llm import summarize_highlights
from tools_db import init_db, upsert_da_rows, upsert_rt_rows, upsert_highlights_rows
from tools_pdf import parse_daily_report_multi  # :contentReference[oaicite:0]{index=0}


def load_header(path: str) -> Dict:
    cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        raise ValueError("header yaml must map keys to values")

    if "provinces" not in cfg or not isinstance(cfg["provinces"], dict):
        raise ValueError("header yaml must provide 'provinces' mapping")

    # Default year if not specified
    if "year" not in cfg:
        cfg["year"] = 2025

    # Default pdf_glob: ../spot reports/*.pdf
    if "pdf_glob" not in cfg or not cfg["pdf_glob"]:
        base = Path(__file__).resolve().parent.parent / "spot reports" / "*.pdf"
        cfg["pdf_glob"] = str(base)
        print(f"[INFO] pdf_glob not in YAML, defaulting to: {cfg['pdf_glob']}")
    return cfg


def _parse_month_day(token: str, year: int) -> dt.date | None:
    token = token.strip()
    token = token.replace("月", ".").replace("日", "")
    if "." in token:
        m_str, d_str = token.split(".", 1)
    else:
        if len(token) == 4:
            m_str, d_str = token[:2], token[2:]
        elif len(token) == 3:
            m_str, d_str = token[0], token[1:]
        else:
            return None
    try:
        m = int(m_str)
        d = int(d_str)
        return dt.date(year, m, d)
    except Exception:
        return None


def dates_from_filename(name: str, year: int) -> List[dt.date]:
    import re

    m = re.search(r"[（(]([^()（）]+)[）)]", name)
    if not m:
        return []

    body = m.group(1)
    body = body.replace("～", "-").replace("至", "-").replace("到", "-")

    if "-" in body:
        start_token, end_token = [x.strip() for x in body.split("-", 1)]
        d1 = _parse_month_day(start_token, year)
        d2 = _parse_month_day(end_token, year)
        if not d1 or not d2 or d2 < d1:
            return []
        days = (d2 - d1).days
        return [d1 + dt.timedelta(days=i) for i in range(days + 1)]

    d = _parse_month_day(body, year)
    return [d] if d else []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--header", required=True, help="YAML header config file")
    args = parser.parse_args()

    cfg = load_header(args.header)
    year = int(cfg.get("year", 2025))
    provinces_map: Dict[str, str] = cfg["provinces"]

    # Init DB (create table + index if needed)
    init_db()

    pdf_glob = cfg["pdf_glob"]
    pdf_paths = sorted(glob.glob(pdf_glob))
    print(
        f"[INFO] scanning {os.path.dirname(pdf_glob)}, "
        f"found {len(pdf_paths)} 日报 pdfs matching pattern"
    )

    total_da = total_rt = total_hi = 0

    for pdf_path in pdf_paths:
        name = os.path.basename(pdf_path)
        dates_hint = dates_from_filename(name, year)

        if not dates_hint:
            print(f"[WARN] {name}: cannot infer dates from filename; will rely on page headings")

        if len(dates_hint) > 1:
            print(f"[INFO] {name}: multi-day report hinted by name ({', '.join(str(d) for d in dates_hint)})")

        # Parse per-date content from the PDF itself
        da_by_date, rt_by_date, hi_by_date = parse_daily_report_multi(pdf_path, cfg)

        all_dates = sorted(
            set(da_by_date.keys()) | set(rt_by_date.keys()) | set(hi_by_date.keys())
        )

        if not all_dates:
            print(f"[WARN] {name}: parsed no dated rows; skipped")
            continue

        for report_date in all_dates:
            da_rows_raw = da_by_date.get(report_date, [])
            rt_rows_raw = rt_by_date.get(report_date, [])
            hi_text = hi_by_date.get(report_date, "")

            print(f"[PDF] {name} -> date: {report_date}", end="")

            # --- DA rows ---
            da_rows = []
            for r in da_rows_raw:
                cn = r["province_cn"]
                en = provinces_map.get(cn, cn)
                da_rows.append(
                    {
                        "report_date": report_date,
                        "province_cn": cn,
                        "province_en": en,
                        "da_avg": r.get("da_avg"),
                        "da_max": r.get("da_max"),
                        "da_min": r.get("da_min"),
                    }
                )

            # --- RT rows (same calendar day) ---
            rt_rows = []
            for r in rt_rows_raw:
                cn = r["province_cn"]
                en = provinces_map.get(cn, cn)
                rt_rows.append(
                    {
                        "report_date": report_date,
                        "province_cn": cn,
                        "province_en": en,
                        "rt_avg": r.get("rt_avg"),
                        "rt_max": r.get("rt_max"),
                        "rt_min": r.get("rt_min"),
                    }
                )

            # --- Province highlights via LLM ---
            highlights_rows = []
            raw_hi = (hi_text or "").strip()

            if raw_hi:
                for cn, en in provinces_map.items():
                    # cheap pre-filter: only call LLM if province name appears
                    if cn not in raw_hi:
                        continue

                    summary = summarize_highlights(
                        province_cn=cn,
                        report_date=str(report_date),
                        raw_text=raw_hi,
                    )

                    if summary:
                        highlights_rows.append(
                            {
                                "report_date": report_date,
                                "province_cn": cn,
                                "province_en": en,
                                "highlights": summary,
                            }
                        )

            # Perform DB upsert
            n_da = upsert_da_rows(da_rows)
            n_rt = upsert_rt_rows(rt_rows)
            n_hi = upsert_highlights_rows(highlights_rows)

            total_da += n_da
            total_rt += n_rt
            total_hi += n_hi

            print(
                f", parsed DA rows: {len(da_rows_raw)}, RT rows: {len(rt_rows_raw)}, "
                f"HI text: {len(raw_hi)} chars; upserted DA: {n_da}, RT: {n_rt}, HI: {n_hi}"
            )

    print(f"[DONE] upserted DA rows: {total_da}, RT rows: {total_rt}, highlights: {total_hi}")


if __name__ == "__main__":
    main()
