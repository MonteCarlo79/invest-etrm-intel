"""Daily WeCom nomination pipeline: collect → parse zones + nominated MW →
ship to RDS.

Schedule: launchd ai.pjh-etrm.wecom-nominations-daily, 11:00 + 16:00 CST
(files land in the group post-midnight → morning, occasionally afternoon).

Nominated MW (交易员申报计划 column C) feeds market_impact's λ v2 instrument
(marketdata.bess_nomination_96); zones feed marketdata.bess_dispatch_zones.
"""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services.mengxi_nodal.dispatch_zones import (
    consensus_zone_rows,
    parse_nomination_zones,
)
from services.mengxi_nodal.nomination_curves import (
    _ACTUAL_DDL,
    _ACTUAL_UPSERT_SQL,
    _DDL as _NOM_DDL,
    dedupe_rows,
    parse_actual_curves,
    parse_nomination_curves,
)
from services.wecom_nominations import collector
from services.wecom_nominations.collector import collect
from services.wecom_nominations.status import (
    _DDL as _STATUS_DDL,
    _UPSERT_SQL as _STATUS_UPSERT_SQL,
    file_stat,
    parse_error_stat,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wecom_nominations")

FOLDERS = {"谷山梁": "gushanliang", "苏右": "suyou", "杭锦旗": "hangjinqi",
           "四子王旗": "siziwangqi", "巴盟": "bameng", "乌拉特中旗": "wulate",
           "武川": "wuchuan", "阿拉善": "alashan"}
# Nominated/actual MW only (2026-11-11): their workbooks are multi-unit
# (summed by nomination_curves) and their cell colouring has not been
# validated as zone data — no zone votes, and no fleet-consensus rows either.
NO_ZONE_PARSE = {"wuchuan", "alashan"}
# Stations whose own files carry no usable zone colouring (bameng's red
# marks 出清偏差, wulate's sheets are unpainted): their file rows are NOT
# zone data — they vote nothing and get the fleet consensus instead.
NO_ZONE_FILES = {"bameng", "wulate"}
S3_KEY_PREFIX = "zones/"
BUCKET = "bess-uploader-data-chen-singp-2026"
# Bare family name: run-task resolves the latest registered revision. Pinning a
# revision breaks on the ECR keep-5 lifecycle (td:58's v43 image was reaped
# 2026-11-05 → CannotPullContainerError on every ship).
TD = "bess-platform-mengxi-dashboard"
# launchd's PATH is /usr/bin:/bin:/usr/sbin:/sbin — the uv-installed CLI in
# ~/.local/bin is not on it (every scheduled ship failed 2026-11-03→09).
AWS = shutil.which("aws") or str(Path.home() / ".local/bin/aws")


def _launch_writer(writer: str) -> None:
    """Base64-wrap a python payload and launch it as a one-off ECS task."""
    import base64
    b64 = base64.b64encode(writer.encode()).decode()
    overrides = json.dumps({"containerOverrides": [{
        "name": "mengxi-dashboard",
        "command": ["sh", "-c", f"echo {b64} | base64 -d | python -u"]}]})
    out = subprocess.run([
        AWS, "ecs", "run-task", "--cluster", "bess-platform-cluster",
        "--task-definition", TD, "--launch-type", "FARGATE",
        "--region", "ap-southeast-1",
        "--network-configuration",
        "awsvpcConfiguration={subnets=[subnet-08e93f99a9c0aa3a9,subnet-0e8ae30466a9f72b5],"
        "securityGroups=[sg-08576f2bea0274a81],assignPublicIp=DISABLED}",
        "--overrides", overrides,
        "--query", "tasks[0].taskArn", "--output", "text"],
        check=True, capture_output=True, text=True)
    logger.info("ECS writer launched: %s", out.stdout.strip())


def ship_zones(rows: list[dict], tag: str) -> None:
    """Upload zones JSON to S3 and trigger the ECS upsert writer."""
    if not rows:
        return
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(rows, fh, ensure_ascii=False, default=str)
        tmp = fh.name
    key = f"{S3_KEY_PREFIX}zones_daily_{tag}.json"
    subprocess.run([AWS, "s3", "cp", tmp, f"s3://{BUCKET}/{key}",
                    "--region", "ap-southeast-1"], check=True, capture_output=True)
    writer = f"""
import json, os, boto3
from sqlalchemy import create_engine, text
from services.mengxi_nodal.dispatch_zones import _DDL
rows = json.loads(boto3.client('s3', region_name='ap-southeast-1').get_object(
    Bucket='{BUCKET}', Key='{key}')['Body'].read())
engine = create_engine(os.environ['PGURL'])
UPSERT = text('''INSERT INTO marketdata.bess_dispatch_zones (asset_code, data_date, slot, zone, source_file)
    VALUES (:asset_code, :data_date, :slot, :zone, :source_file)
    ON CONFLICT (asset_code, data_date, slot) DO UPDATE SET zone = EXCLUDED.zone, source_file = EXCLUDED.source_file''')
with engine.begin() as c:
    for stmt in _DDL.split(';'):
        if stmt.strip():
            c.execute(text(stmt))
    n = 0
    for i in range(0, len(rows), 5000):
        c.execute(UPSERT, rows[i:i+5000]); n += len(rows[i:i+5000])
print('DONE', n, flush=True)
"""
    _launch_writer(writer)


def ship_nominations(rows: list[dict], tag: str) -> None:
    """Upload nominated-MW JSON to S3 and trigger the ECS upsert writer.
    The DDL is injected from the Mac-side module so the payload is
    self-contained (no image-version coupling)."""
    if not rows:
        return
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(rows, fh, ensure_ascii=False, default=str)
        tmp = fh.name
    key = f"{S3_KEY_PREFIX}nominations_daily_{tag}.json"
    subprocess.run([AWS, "s3", "cp", tmp, f"s3://{BUCKET}/{key}",
                    "--region", "ap-southeast-1"], check=True, capture_output=True)
    writer = f"""
import json, os, boto3
from sqlalchemy import create_engine, text
rows = json.loads(boto3.client('s3', region_name='ap-southeast-1').get_object(
    Bucket='{BUCKET}', Key='{key}')['Body'].read())
engine = create_engine(os.environ['PGURL'])
DDL = '''{_NOM_DDL}'''
UPSERT = text('''INSERT INTO marketdata.bess_nomination_96 (asset_code, data_date, slot, nominated_mw, source_file)
    VALUES (:asset_code, :data_date, :slot, :nominated_mw, :source_file)
    ON CONFLICT (asset_code, data_date, slot) DO UPDATE SET nominated_mw = EXCLUDED.nominated_mw, source_file = EXCLUDED.source_file''')
with engine.begin() as c:
    for stmt in DDL.split(';'):
        if stmt.strip():
            c.execute(text(stmt))
    n = 0
    for i in range(0, len(rows), 5000):
        c.execute(UPSERT, rows[i:i+5000]); n += len(rows[i:i+5000])
print('DONE', n, flush=True)
"""
    _launch_writer(writer)


def ship_status(stats: list[dict], tag: str) -> None:
    """Upload per-file status JSON to S3 and trigger the ECS upsert writer
    (self-contained payload: DDL + upsert interpolated Mac-side)."""
    if not stats:
        return
    from datetime import datetime, timezone
    stamped = [{**s, "shipped_at": datetime.now(timezone.utc).isoformat(),
                "tag": tag} for s in stats]
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(stamped, fh, ensure_ascii=False, default=str)
        tmp = fh.name
    key = f"{S3_KEY_PREFIX}nomination_file_status_{tag}.json"
    subprocess.run([AWS, "s3", "cp", tmp, f"s3://{BUCKET}/{key}",
                    "--region", "ap-southeast-1"], check=True, capture_output=True)
    writer = f"""
import json, os, boto3
from sqlalchemy import create_engine, text
rows = json.loads(boto3.client('s3', region_name='ap-southeast-1').get_object(
    Bucket='{BUCKET}', Key='{key}')['Body'].read())
engine = create_engine(os.environ['PGURL'])
DDL = '''{_STATUS_DDL}'''
UPSERT = text('''{_STATUS_UPSERT_SQL}''')
with engine.begin() as c:
    for stmt in DDL.split(';'):
        if stmt.strip():
            c.execute(text(stmt))
    n = 0
    for i in range(0, len(rows), 500):
        c.execute(UPSERT, rows[i:i+500]); n += len(rows[i:i+500])
print('DONE', n, flush=True)
"""
    _launch_writer(writer)


def ship_actuals(rows: list[dict], tag: str) -> None:
    """Upload metered-actual MW JSON to S3 and trigger the ECS upsert writer
    (bess_actual_96 — replaces ops_bess_dispatch_15min, dead since 2026-04)."""
    if not rows:
        return
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(rows, fh, ensure_ascii=False, default=str)
        tmp = fh.name
    key = f"{S3_KEY_PREFIX}actuals_daily_{tag}.json"
    subprocess.run([AWS, "s3", "cp", tmp, f"s3://{BUCKET}/{key}",
                    "--region", "ap-southeast-1"], check=True, capture_output=True)
    writer = f"""
import json, os, boto3
from sqlalchemy import create_engine, text
rows = json.loads(boto3.client('s3', region_name='ap-southeast-1').get_object(
    Bucket='{BUCKET}', Key='{key}')['Body'].read())
engine = create_engine(os.environ['PGURL'])
DDL = '''{_ACTUAL_DDL}'''
UPSERT = text('''{_ACTUAL_UPSERT_SQL}''')
with engine.begin() as c:
    for stmt in DDL.split(';'):
        if stmt.strip():
            c.execute(text(stmt))
    n = 0
    for i in range(0, len(rows), 5000):
        c.execute(UPSERT, rows[i:i+5000]); n += len(rows[i:i+5000])
print('DONE', n, flush=True)
"""
    _launch_writer(writer)


def main() -> None:
    stats = collect()
    logger.info("collect: %s", stats)
    if stats["scanned"] == 0:
        # The cache always holds hundreds of files; zero means this process
        # cannot list it (macOS privacy: the python binary needs Full Disk
        # Access) — fail loudly, never ship an "empty day".
        raise RuntimeError(f"WeCom cache scan found 0 files: {collector.CACHE_ROOT}")
    logger.info("mirror: %s", collector.mirror_to(collector.ONEDRIVE_ROOT))
    voter_rows: list[dict] = []
    nom_rows: list[dict] = []
    act_rows: list[dict] = []
    file_stats: list[dict] = []
    for folder, code in FOLDERS.items():
        d = collector.NOMINATION_ROOT / folder
        if not d.is_dir():
            continue
        # .xlsx AND legacy .xls (wulate's operator switched to .xls from July
        # 2026 — xlsx-only globbing silently ended its feed at Jun 30)
        files = sorted(set(d.glob("*.xlsx")) | set(d.glob("*.xls")))
        for f in files:
            if (code not in NO_ZONE_FILES and code not in NO_ZONE_PARSE
                    and f.suffix.lower() == ".xlsx"):
                try:
                    voter_rows.extend(parse_nomination_zones(f, code))
                except Exception as ex:
                    logger.warning("zone parse failed %s: %s", f.name, ex)
            try:
                rows = parse_nomination_curves(f, code)
                nom_rows.extend(rows)
                file_stats.append(file_stat(code, f, rows))
            except Exception as ex:
                logger.warning("nomination parse failed %s: %s", f.name, ex)
                file_stats.append(parse_error_stat(code, f, ex))
            try:
                act_rows.extend(parse_actual_curves(f, code))
            except Exception as ex:
                logger.warning("actual parse failed %s: %s", f.name, ex)
    zone_rows = voter_rows + consensus_zone_rows(voter_rows, NO_ZONE_FILES)
    logger.info("parsed %d zone rows (%d fleet-consensus), %d nomination "
                "rows, %d actual rows; shipping",
                len(zone_rows), len(zone_rows) - len(voter_rows),
                len(nom_rows), len(act_rows))
    from datetime import date
    tag = date.today().isoformat()
    ship_zones(zone_rows, tag)
    ship_nominations(dedupe_rows(nom_rows), tag)
    ship_actuals(dedupe_rows(act_rows), tag)
    ship_status(file_stats, tag)


if __name__ == "__main__":
    main()
