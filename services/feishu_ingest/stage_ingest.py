"""Stage newly exported files for KB ingest and launch the ECS batch task.

Usage: python stage_ingest.py <batch-name> [--min N]
Reads every ~/feishu-ingest/logs/export_results*.jsonl (and logs/runs/), takes OK files not yet
recorded in logs/ingest_state.json, uploads them + the worker scripts to
s3://<bucket>/tmp/<batch-name>/, and launches the batch on the spot-markets task definition.
Prints the ECS task id. Exits without launching when fewer than --min files are pending.
"""
from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import time
from pathlib import Path

H = Path(os.environ.get("FEISHU_INGEST_HOME", Path.home() / "feishu-ingest"))
HERE = Path(__file__).resolve().parent
BUCKET = "bess-uploader-data-chen-singp-2026"
REGION = "ap-southeast-1"
CLUSTER = "bess-platform-cluster"
NET = ("awsvpcConfiguration={subnets=[subnet-08e93f99a9c0aa3a9,subnet-0e8ae30466a9f72b5],"
       "securityGroups=[sg-08576f2bea0274a81],assignPublicIp=DISABLED}")


def aws(*args: str, retries: int = 20) -> str:
    for i in range(retries):
        r = subprocess.run(["aws", *args, "--region", REGION], capture_output=True, text=True)
        if r.returncode == 0:
            return r.stdout.strip()
        time.sleep(20)  # hotspot drops: wait and retry
    raise RuntimeError(f"aws {' '.join(args[:3])} failed: {r.stderr[-300:]}")


def main() -> int:
    batch = sys.argv[1]
    min_n = int(sys.argv[sys.argv.index("--min") + 1]) if "--min" in sys.argv else 1
    state_f = H / "logs/ingest_state.json"
    state = json.load(open(state_f)) if state_f.exists() else {"staged": []}
    staged = set(state["staged"])
    pending: dict[str, str] = {}
    for f in glob.glob(str(H / "logs/export_results*.jsonl")) + glob.glob(str(H / "logs/runs/*.jsonl")):
        for line in open(f):
            r = json.loads(line)
            if not r["result"].startswith("OK "):
                continue
            rel = r["result"][3:].rsplit(" (", 1)[0]
            full = next((str(p) for p in sorted((H / "downloads").glob(f"*/{rel}")) if p.exists()), None)
            if full and full not in staged:
                pending[full] = r["path"]
    print(f"pending {len(pending)}")
    if len(pending) < min_n:
        return 0
    prefix = f"tmp/{batch}/"
    manifest = {}
    for i, (full, path) in enumerate(sorted(pending.items())):
        key = f"{prefix}{i:04d}/{os.path.basename(full)}"
        aws("s3", "cp", "--only-show-errors", full, f"s3://{BUCKET}/{key}")
        manifest[key] = path
    mf = H / f"logs/{batch}_manifest.json"
    json.dump(manifest, open(mf, "w"), ensure_ascii=False, indent=1)
    aws("s3", "cp", "--only-show-errors", str(mf), f"s3://{BUCKET}/{prefix}_manifest.json")
    aws("s3", "cp", "--only-show-errors", str(HERE / "worker/ingest_one.py"), f"s3://{BUCKET}/{prefix}_one.py")
    aws("s3", "cp", "--only-show-errors", str(HERE / "worker/ingest_batch.py"), f"s3://{BUCKET}/{prefix}_run.py")
    td = aws("ecs", "describe-services", "--cluster", CLUSTER, "--services", "bess-platform-spot-markets-svc",
             "--query", "services[0].taskDefinition", "--output", "text")
    cmd = (f"cd /app 2>/dev/null; python -c \"import boto3;boto3.client('s3').download_file("
           f"'{BUCKET}','{prefix}_run.py','/tmp/run.py')\" && PYTHONPATH=$PWD python /tmp/run.py {prefix}")
    ov = json.dumps({"cpu": "2048", "memory": "8192",
                     "containerOverrides": [{"name": "spot-markets", "command": ["sh", "-c", cmd]}]})
    arn = aws("ecs", "run-task", "--cluster", CLUSTER, "--task-definition", td, "--launch-type", "FARGATE",
              "--network-configuration", NET, "--overrides", ov, "--query", "tasks[0].taskArn", "--output", "text",
              retries=3)
    state["staged"] = sorted(staged | set(pending))
    state.setdefault("batches", []).append({"batch": batch, "files": len(pending), "task": arn.rsplit("/", 1)[1]})
    json.dump(state, open(state_f, "w"), ensure_ascii=False, indent=1)
    print(f"TASK {arn.rsplit('/', 1)[1]} files={len(pending)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
