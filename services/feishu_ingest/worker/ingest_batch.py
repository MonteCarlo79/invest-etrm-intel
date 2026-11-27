"""Batch driver: ingest every object under the prefix, one child process each."""
import json, subprocess, sys, boto3
B, P = "bess-uploader-data-chen-singp-2026", sys.argv[1]
s3 = boto3.client("s3")
man = json.loads(s3.get_object(Bucket=B, Key=P + "_manifest.json")["Body"].read())  # {s3 key: feishu path}
s3.download_file(B, P + "_one.py", "/tmp/one.py")
ok = fail = 0
for key, path in man.items():
    try:
        s3.head_object(Bucket=B, Key=key)
    except Exception:
        continue  # already ingested (deleted after success) in an earlier run of this batch
    try:
        r = subprocess.run([sys.executable, "/tmp/one.py", B, key, path], capture_output=True, text=True, timeout=1800)
    except subprocess.TimeoutExpired:
        fail += 1
        print("FAILED " + json.dumps({"key": key, "err": "timeout after 1800 s"}, ensure_ascii=False), flush=True)
        continue
    line = next((l for l in r.stdout.splitlines() if l.startswith("RESULT ")), None)
    if line:
        ok += 1; print(line, flush=True); s3.delete_object(Bucket=B, Key=key)
    else:
        fail += 1
        print("FAILED " + json.dumps({"key": key, "rc": r.returncode, "err": (r.stderr or "")[-300:]}, ensure_ascii=False), flush=True)
print(f"BATCH DONE ok={ok} failed={fail}", flush=True)
