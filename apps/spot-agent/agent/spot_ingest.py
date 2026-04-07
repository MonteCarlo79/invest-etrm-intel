from __future__ import annotations

import argparse
import datetime as dt
import glob
import logging
import os
import re
import sys
import time
import traceback
from pathlib import Path
from typing import Dict, List, Tuple

import yaml

try:
    from dotenv import load_dotenv

    here = Path(__file__).resolve()
    for p in [here.parent] + list(here.parents):
        env_path = p / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            break
except Exception:
    pass

from tools_db import (
    already_processed,
    init_db,
    log_parse_done,
    log_parse_error,
    log_parse_start,
    sha256_file,
    upsert_da_rows,
    upsert_highlights_rows,
    upsert_rt_rows,
)
from tools_llm import summarize_highlights
from tools_pdf import parse_daily_report_multi
from tools_s3 import list_pdf_objects, stage_pdf_to_temp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("spot_ingest")


def load_header(path: str) -> Dict:
    cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        raise ValueError("header yaml must be a mapping")
    if "provinces" not in cfg or not isinstance(cfg["provinces"], dict):
        raise ValueError("header yaml must provide 'provinces' mapping")
    cfg.setdefault("year", 2025)
    cfg.setdefault("source_mode", "local")
    if cfg["source_mode"] not in {"local", "s3"}:
        raise ValueError("source_mode must be either 'local' or 's3'")
    cfg.setdefault("s3_region", os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"))
    prefixes = cfg.get("s3_prefixes") or []
    if isinstance(prefixes, str):
        prefixes = [prefixes]
    cfg["s3_prefixes"] = prefixes
    return cfg


def _infer_year_from_path(pdf_path: str, default_year: int) -> int:
    match = re.search(r"[/\\](\d{4})[/\\]", pdf_path)
    if match:
        year = int(match.group(1))
        if 2020 <= year <= 2035:
            return year
    return default_year


def _collect_local_candidates(cfg: Dict) -> List[Dict[str, object]]:
    raw = cfg.get("pdf_globs") or []
    if isinstance(raw, str):
        raw = [raw]
    single = cfg.get("pdf_glob")
    if single:
        raw = list(raw) + [single]
    if not raw:
        base = Path(__file__).resolve().parent.parent / "spot reports" / "*.pdf"
        raw = [str(base)]
        log.info("pdf_glob not in YAML, defaulting to: %s", raw[0])

    paths: List[str] = []
    for pattern in raw:
        found = sorted(glob.glob(pattern))
        log.debug("Local glob '%s' -> %d files", pattern, len(found))
        paths.extend(found)

    seen: set[str] = set()
    out: List[Dict[str, object]] = []
    for path in paths:
        normalized = str(Path(path))
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(
            {
                "source_mode": "local",
                "source_path": normalized,
                "display_name": os.path.basename(normalized),
                "year_hint": _infer_year_from_path(normalized, int(cfg.get("year", 2025))),
            }
        )
    return out


def _collect_s3_candidates(cfg: Dict) -> List[Dict[str, object]]:
    bucket = cfg.get("s3_bucket", "")
    prefixes = cfg.get("s3_prefixes", [])
    region = cfg.get("s3_region")
    objects = list_pdf_objects(bucket=bucket, prefixes=prefixes, region=region)
    out: List[Dict[str, object]] = []
    for obj in objects:
        out.append(
            {
                "source_mode": "s3",
                "source_path": obj.uri,
                "display_name": obj.name,
                "year_hint": _infer_year_from_path(obj.key, int(cfg.get("year", 2025))),
                "s3_bucket": obj.bucket,
                "s3_key": obj.key,
            }
        )
    return out


def _discover_candidates(cfg: Dict) -> List[Dict[str, object]]:
    if cfg.get("source_mode", "local") == "s3":
        return _collect_s3_candidates(cfg)
    return _collect_local_candidates(cfg)


def _parse_month_day(token: str, year: int) -> dt.date | None:
    token = token.strip().replace("月", ".").replace("日", "")
    if "." in token:
        month_str, day_str = token.split(".", 1)
    else:
        if len(token) == 4:
            month_str, day_str = token[:2], token[2:]
        elif len(token) == 3:
            month_str, day_str = token[0], token[1:]
        else:
            return None
    try:
        return dt.date(year, int(month_str), int(day_str))
    except Exception:
        return None


def dates_from_filename(name: str, year: int) -> List[dt.date]:
    match = re.search(r"[（(]([^()（）]+)[)）]", name)
    if not match:
        return []
    body = match.group(1).replace("—", "-").replace("至", "-").replace("到", "-")
    if "-" in body:
        start_token, end_token = [x.strip() for x in body.split("-", 1)]
        d1 = _parse_month_day(start_token, year)
        d2 = _parse_month_day(end_token, year)
        if not d1 or not d2 or d2 < d1:
            return []
        return [d1 + dt.timedelta(days=i) for i in range((d2 - d1).days + 1)]
    single = _parse_month_day(body, year)
    return [single] if single else []


def process_pdf(
    pdf_path: str,
    cfg: Dict,
    provinces_map: Dict[str, str],
    *,
    source_path: str | None,
    display_name: str | None,
    year_hint: int | None,
    source_mode: str,
    dry_run: bool,
    use_llm: bool,
    since_date: dt.date | None,
) -> Dict:
    name = display_name or os.path.basename(source_path or pdf_path)
    canonical_source_path = source_path or pdf_path
    year_for_pdf = year_hint or _infer_year_from_path(canonical_source_path, int(cfg.get("year", 2025)))
    cfg_for_pdf = {**cfg, "year": year_for_pdf}

    dates_hint = dates_from_filename(name, year_for_pdf)
    if len(dates_hint) > 1:
        log.info("%s [%s]: multi-day report (%s)", name, canonical_source_path, ", ".join(str(d) for d in dates_hint))

    da_by_date, rt_by_date, hi_by_date = parse_daily_report_multi(pdf_path, cfg_for_pdf)
    all_dates = sorted(set(da_by_date) | set(rt_by_date) | set(hi_by_date))
    if not all_dates:
        log.warning("%s [%s]: no dated rows parsed; skipped", name, canonical_source_path)
        return {"n_dates": 0, "n_da": 0, "n_rt": 0, "n_hi": 0}

    if since_date:
        all_dates = [d for d in all_dates if d >= since_date]
        if not all_dates:
            log.info("%s [%s]: all dates before --since %s; skipped", name, canonical_source_path, since_date)
            return {"n_dates": 0, "n_da": 0, "n_rt": 0, "n_hi": 0}

    total_da = total_rt = total_hi = 0

    for report_date in all_dates:
        da_rows_raw = da_by_date.get(report_date, [])
        rt_rows_raw = rt_by_date.get(report_date, [])
        hi_text = hi_by_date.get(report_date, "")

        da_rows = [
            {
                "report_date": report_date,
                "province_cn": row["province_cn"],
                "province_en": provinces_map.get(row["province_cn"], row["province_cn"]),
                "da_avg": row.get("da_avg"),
                "da_max": row.get("da_max"),
                "da_min": row.get("da_min"),
                "source_file": name,
            }
            for row in da_rows_raw
        ]
        rt_rows = [
            {
                "report_date": report_date,
                "province_cn": row["province_cn"],
                "province_en": provinces_map.get(row["province_cn"], row["province_cn"]),
                "rt_avg": row.get("rt_avg"),
                "rt_max": row.get("rt_max"),
                "rt_min": row.get("rt_min"),
                "source_file": name,
            }
            for row in rt_rows_raw
        ]

        highlights_rows: List[Dict] = []
        raw_hi = (hi_text or "").strip()
        if raw_hi and use_llm:
            for province_cn, province_en in provinces_map.items():
                if province_cn not in raw_hi:
                    continue
                summary = summarize_highlights(
                    province_cn=province_cn,
                    report_date=str(report_date),
                    raw_text=raw_hi,
                )
                if summary:
                    highlights_rows.append(
                        {
                            "report_date": report_date,
                            "province_cn": province_cn,
                            "province_en": province_en,
                            "highlights": summary,
                            "source_file": name,
                        }
                    )

        if dry_run:
            log.info(
                "[DRY-RUN] %s [%s|%s] -> %s: would upsert DA=%d RT=%d HI=%d",
                name,
                source_mode,
                canonical_source_path,
                report_date,
                len(da_rows),
                len(rt_rows),
                len(highlights_rows),
            )
            continue

        n_da = upsert_da_rows(da_rows)
        n_rt = upsert_rt_rows(rt_rows)
        n_hi = upsert_highlights_rows(highlights_rows)
        total_da += n_da
        total_rt += n_rt
        total_hi += n_hi
        log.info(
            "%s [%s|%s] -> %s DA=%d RT=%d HI=%d (raw DA=%d RT=%d hi_chars=%d)",
            name,
            source_mode,
            canonical_source_path,
            report_date,
            n_da,
            n_rt,
            n_hi,
            len(da_rows_raw),
            len(rt_rows_raw),
            len(raw_hi),
        )

    return {"n_dates": len(all_dates), "n_da": total_da, "n_rt": total_rt, "n_hi": total_hi}


def _process_candidate(
    *,
    staged_pdf_path: str,
    source_path: str,
    display_name: str,
    year_hint: int,
    source_mode: str,
    cfg: Dict,
    provinces_map: Dict[str, str],
    force: bool,
    dry_run: bool,
    use_llm: bool,
    since_date: dt.date | None,
) -> Tuple[int, int, int]:
    try:
        file_hash = sha256_file(staged_pdf_path)
    except OSError as exc:
        log.error("Cannot read %s [%s]: %s", display_name, source_path, exc)
        return 0, 0, 1

    log.info("DISCOVER source_mode=%s source=%s sha256=%s", source_mode, source_path, file_hash)

    if not force and not dry_run and already_processed(source_path, file_hash):
        log.info("SKIP %s [%s] reason=already_processed sha256=%s", display_name, source_path, file_hash)
        return 0, 1, 0

    log_id: int | None = None
    if not dry_run:
        try:
            log_id = log_parse_start(source_path, file_hash)
        except Exception as exc:
            log.warning("Could not write parse log start for %s [%s]: %s", display_name, source_path, exc)

    try:
        result = process_pdf(
            staged_pdf_path,
            cfg,
            provinces_map,
            source_path=source_path,
            display_name=display_name,
            year_hint=year_hint,
            source_mode=source_mode,
            dry_run=dry_run,
            use_llm=use_llm,
            since_date=since_date,
        )
        if log_id is not None:
            try:
                log_parse_done(log_id, **result)
            except Exception as exc:
                log.warning("Could not write parse log done for %s [%s]: %s", display_name, source_path, exc)
        return 1, 0, 0
    except Exception as exc:
        log.error("ERROR processing %s [%s]: %s", display_name, source_path, traceback.format_exc())
        if log_id is not None:
            try:
                log_parse_error(log_id, str(exc))
            except Exception as inner:
                log.warning("Could not write parse log error for %s [%s]: %s", display_name, source_path, inner)
        return 0, 0, 1


def _run_once(
    cfg: Dict,
    provinces_map: Dict[str, str],
    *,
    force: bool,
    dry_run: bool,
    use_llm: bool,
    since_date: dt.date | None,
) -> Tuple[int, int, int]:
    candidates = _discover_candidates(cfg)
    if not candidates:
        if cfg.get("source_mode", "local") == "s3":
            log.warning("No PDF files found in S3 source mode; check s3_bucket / s3_prefixes in config")
        else:
            log.warning("No PDF files found; check pdf_glob(s) in config")
        return 0, 0, 0

    n_ok = n_skipped = n_err = 0
    for candidate in candidates:
        source_mode = str(candidate["source_mode"])
        source_path = str(candidate["source_path"])
        display_name = str(candidate["display_name"])
        year_hint = int(candidate["year_hint"])

        if source_mode == "s3":
            try:
                with stage_pdf_to_temp(
                    bucket=str(candidate["s3_bucket"]),
                    key=str(candidate["s3_key"]),
                    region=cfg.get("s3_region"),
                ) as staged_pdf_path:
                    ok, skipped, err = _process_candidate(
                        staged_pdf_path=staged_pdf_path,
                        source_path=source_path,
                        display_name=display_name,
                        year_hint=year_hint,
                        source_mode=source_mode,
                        cfg=cfg,
                        provinces_map=provinces_map,
                        force=force,
                        dry_run=dry_run,
                        use_llm=use_llm,
                        since_date=since_date,
                    )
            except Exception:
                log.error("ERROR staging %s: %s", source_path, traceback.format_exc())
                n_err += 1
                continue
        else:
            ok, skipped, err = _process_candidate(
                staged_pdf_path=source_path,
                source_path=source_path,
                display_name=display_name,
                year_hint=year_hint,
                source_mode=source_mode,
                cfg=cfg,
                provinces_map=provinces_map,
                force=force,
                dry_run=dry_run,
                use_llm=use_llm,
                since_date=since_date,
            )

        n_ok += ok
        n_skipped += skipped
        n_err += err

    return n_ok, n_skipped, n_err


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest China spot power PDF reports into the marketdata DB.\n"
            "Supported data: daily DA/RT price summaries (avg/max/min per province).\n"
            "Hourly series are not supported because the source PDFs store charts as images."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--header", required=True, help="YAML config file")
    parser.add_argument("--watch", action="store_true", help="Run continuously, polling for new PDFs every --interval seconds")
    parser.add_argument("--interval", type=int, default=300, metavar="SECS", help="Polling interval for --watch (default: 300)")
    parser.add_argument("--force", action="store_true", help="Re-process files already marked done in spot_parse_log")
    parser.add_argument("--dry-run", action="store_true", help="Parse PDFs but do not write to DB")
    parser.add_argument("--no-llm", action="store_true", help="Skip LLM highlight summarization")
    parser.add_argument("--since", default=None, metavar="YYYY-MM-DD", help="Only ingest dates on or after this date")
    parser.add_argument("--verbose", action="store_true", help="Enable DEBUG logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    since_date: dt.date | None = None
    if args.since:
        try:
            since_date = dt.date.fromisoformat(args.since)
        except ValueError:
            log.error("Invalid --since date '%s'; expected YYYY-MM-DD", args.since)
            sys.exit(1)

    cfg = load_header(args.header)
    provinces_map: Dict[str, str] = cfg["provinces"]
    use_llm = not args.no_llm

    if not args.dry_run:
        init_db()

    run_kwargs = {
        "force": args.force,
        "dry_run": args.dry_run,
        "use_llm": use_llm,
        "since_date": since_date,
    }

    if args.watch:
        log.info(
            "Watch mode active polling every %d s source_mode=%s force=%s dry_run=%s no_llm=%s",
            args.interval,
            cfg.get("source_mode", "local"),
            args.force,
            args.dry_run,
            args.no_llm,
        )
        try:
            while True:
                start = time.monotonic()
                n_ok, n_skipped, n_err = _run_once(cfg, provinces_map, **run_kwargs)
                elapsed = time.monotonic() - start
                log.info(
                    "Poll complete in %.1fs processed=%d skipped=%d errors=%d sleeping=%ds",
                    elapsed,
                    n_ok,
                    n_skipped,
                    n_err,
                    args.interval,
                )
                time.sleep(args.interval)
        except KeyboardInterrupt:
            log.info("Watch mode stopped by user.")
            sys.exit(0)

    n_ok, n_skipped, n_err = _run_once(cfg, provinces_map, **run_kwargs)
    log.info("Done processed=%d skipped=%d errors=%d", n_ok, n_skipped, n_err)
    if n_err:
        sys.exit(1)


if __name__ == "__main__":
    main()
