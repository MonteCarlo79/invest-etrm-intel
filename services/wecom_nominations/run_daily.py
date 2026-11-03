"""Daily WeCom nomination pipeline: collect → parse zones → ship to RDS.

Schedule: launchd ai.pjh-etrm.wecom-nominations-daily, 11:00 + 16:00 CST
(files land in the group post-midnight → morning, occasionally afternoon).
"""
from __future__ import annotations

import json
import logging
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services.mengxi_nodal.dispatch_zones import parse_nomination_zones
from services.wecom_nominations.collector import collect

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wecom_nominations")

FOLDERS = {"谷山梁": "gushanliang", "苏右": "suyou", "杭锦旗": "hangjinqi",
           "四子王旗": "siziwangqi", "巴盟": "bameng", "乌拉特中旗": "wulate"}
S3_KEY_PREFIX = "zones/"
BUCKET = "bess-uploader-data-chen-singp-2026"
TD = "bess-platform-mengxi-dashboard:58"


def ship_zones(rows: list[dict], tag: str) -> None:
    """Upload zones JSON to S3 and trigger the ECS upsert writer."""
    if not rows:
        return
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
        json.dump(rows, fh, ensure_ascii=False, default=str)
        tmp = fh.name
    key = f"{S3_KEY_PREFIX}zones_daily_{tag}.json"
    subprocess.run(["aws", "s3", "cp", tmp, f"s3://{BUCKET}/{key}",
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
    import base64
    b64 = base64.b64encode(writer.encode()).decode()
    overrides = json.dumps({"containerOverrides": [{
        "name": "mengxi-dashboard",
        "command": ["sh", "-c", f"echo {b64} | base64 -d | python -u"]}]})
    out = subprocess.run([
        "aws", "ecs", "run-task", "--cluster", "bess-platform-cluster",
        "--task-definition", TD, "--launch-type", "FARGATE",
        "--region", "ap-southeast-1",
        "--network-configuration",
        "awsvpcConfiguration={subnets=[subnet-08e93f99a9c0aa3a9,subnet-0e8ae30466a9f72b5],"
        "securityGroups=[sg-08576f2bea0274a81],assignPublicIp=DISABLED}",
        "--overrides", overrides,
        "--query", "tasks[0].taskArn", "--output", "text"],
        check=True, capture_output=True, text=True)
    logger.info("ECS writer launched: %s", out.stdout.strip())


def main() -> None:
    stats = collect()
    logger.info("collect: %s", stats)
    rows: list[dict] = []
    for folder, code in FOLDERS.items():
        d = Path("data/raw/nomination") / folder
        if not d.is_dir():
            continue
        for f in sorted(d.glob("*.xlsx")):
            try:
                rows.extend(parse_nomination_zones(f, code))
            except Exception as ex:
                logger.warning("parse failed %s: %s", f.name, ex)
    logger.info("parsed %d zone rows; shipping", len(rows))
    from datetime import date
    ship_zones(rows, date.today().isoformat())


if __name__ == "__main__":
    main()
