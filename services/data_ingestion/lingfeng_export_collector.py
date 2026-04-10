"""
Lingfeng portal collector.
Attempts direct HTTP export first; falls back to Playwright automation.
Stores files in S3 raw landing before parsing.
URL and credentials injected via environment variables.

NOT PRODUCTION READY: LINGFENG_BASE_URL and Playwright selectors are pending
confirmation of the portal URL. Run will fail-fast with a clear error if
LINGFENG_BASE_URL is unset.
"""
from __future__ import annotations
import json
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from services.data_ingestion.shared.context import RunContext
from services.data_ingestion.shared.logging import get_logger
from services.data_ingestion.shared.control import start_run, finish_run, update_dataset_status
from services.data_ingestion.shared.s3 import upload_to_landing

logger = get_logger("lingfeng_collector")
COLLECTOR = "lingfeng"

LINGFENG_BASE_URL = os.environ.get("LINGFENG_BASE_URL", "")
LINGFENG_USERNAME = os.environ.get("LINGFENG_USERNAME", "")
LINGFENG_PASSWORD = os.environ.get("LINGFENG_PASSWORD", "")
PROVINCE_LIST = [
    p.strip()
    for p in os.environ.get("LINGFENG_PROVINCE_LIST", "").split(",")
    if p.strip()
]


def _try_direct_http(target_date: date, province: str, out_dir: Path) -> Path | None:
    """Attempt direct export endpoint. Returns local path if successful, else None."""
    if not LINGFENG_BASE_URL:
        return None
    import requests
    for url_template in [
        f"{LINGFENG_BASE_URL}/export?date={target_date}&province={province}",
        f"{LINGFENG_BASE_URL}/download/{target_date}/{province}.xlsx",
    ]:
        try:
            r = requests.get(url_template, timeout=30,
                             auth=(LINGFENG_USERNAME, LINGFENG_PASSWORD))
            if r.status_code == 200 and len(r.content) > 4096:
                out = out_dir / f"{province}_{target_date}.xlsx"
                out.write_bytes(r.content)
                logger.info(json.dumps({
                    "event": "direct_download_ok",
                    "province": province, "date": str(target_date),
                }))
                return out
        except Exception:
            pass
    return None


def _try_playwright(target_date: date, province: str, out_dir: Path) -> Path | None:
    """Browser automation fallback. Requires LINGFENG_BASE_URL to be set."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.warning(json.dumps({"event": "playwright_not_installed", "action": "skip"}))
        return None

    if not LINGFENG_BASE_URL:
        raise SystemExit("LINGFENG_BASE_URL must be set for Playwright download")

    out = out_dir / f"{province}_{target_date}.xlsx"
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(f"{LINGFENG_BASE_URL}/login")
        page.fill('[name=username]', LINGFENG_USERNAME)
        page.fill('[name=password]', LINGFENG_PASSWORD)
        page.click('[type=submit]')
        page.wait_for_load_state("networkidle")
        page.goto(f"{LINGFENG_BASE_URL}/data?province={province}&date={target_date}")
        with page.expect_download() as dl_info:
            page.click('[data-action=export]')
        download = dl_info.value
        download.save_as(str(out))
        browser.close()
    logger.info(json.dumps({
        "event": "playwright_download_ok",
        "province": province, "date": str(target_date),
    }))
    return out


def run(ctx: RunContext):
    if not LINGFENG_BASE_URL:
        raise SystemExit("LINGFENG_BASE_URL is required for lingfeng_export_collector")

    run_id = start_run(COLLECTOR, ctx.mode, ctx.start_date, ctx.end_date,
                       dry_run=ctx.dry_run)
    logger.info(json.dumps({
        "event": "run_start", "run_id": run_id, "mode": ctx.mode,
        "start": str(ctx.start_date), "end": str(ctx.end_date),
    }))

    if ctx.dry_run:
        logger.info(json.dumps({"event": "dry_run_skip"}))
        finish_run(run_id, "skipped")
        return

    provinces = (
        ctx.dataset_filter.split(",") if ctx.dataset_filter else PROVINCE_LIST
    )
    if not provinces:
        raise SystemExit(
            "No provinces configured. Set LINGFENG_PROVINCE_LIST or --dataset-filter"
        )

    total_rows = 0
    errors = []
    cur = ctx.start_date
    while cur <= ctx.end_date:
        for province in provinces:
            with tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp)
                local = _try_direct_http(cur, province, out_dir)
                if local is None:
                    local = _try_playwright(cur, province, out_dir)
                if local is None:
                    logger.warning(json.dumps({
                        "event": "download_failed",
                        "province": province, "date": str(cur),
                    }))
                    errors.append(f"{province}/{cur}")
                    continue

                s3_uri = upload_to_landing(local, COLLECTOR, str(cur))
                logger.info(json.dumps({"event": "uploaded_to_s3", "uri": s3_uri}))

                try:
                    from services.bess_map.run_all_provinces import main as rap_main
                    sys.argv = [
                        "run_all_provinces.py",
                        "--indir", str(out_dir),
                        "--auto-cols",
                        "--upload-db",
                        "--continue-on-error",
                    ]
                    rap_main()
                    total_rows += 1
                except Exception as e:
                    logger.error(json.dumps({
                        "event": "load_error",
                        "province": province, "date": str(cur), "error": str(e),
                    }))
                    errors.append(f"{province}/{cur}: {e}")

        cur = cur + timedelta(days=1)

    if errors:
        finish_run(run_id, "failed", rows_written=total_rows,
                   error_message="; ".join(errors[:5]))
        update_dataset_status(COLLECTOR, "public.spot_prices_hourly", failed=True)
    else:
        finish_run(run_id, "success", rows_written=total_rows)
        update_dataset_status(COLLECTOR, "public.spot_prices_hourly",
                              last_date=ctx.end_date)


if __name__ == "__main__":
    ctx = RunContext.from_env_and_args(COLLECTOR)
    run(ctx)
