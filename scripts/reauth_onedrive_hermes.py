#!/usr/bin/env python3
"""Re-authorise Hermes' OneDrive access and hand the new refresh token to
the live service — the token is never printed.

    ~/.venvs/bess-platform/bin/python scripts/reauth_onedrive_hermes.py

1. Browser consent (auth-code flow, http://localhost:8080/callback), same app
   + scopes as services/hermes/onedrive_client.py.
2. Verifies the fresh token can actually download file CONTENT
   (etrm/bess-platform/data/电站.xlsx) — the 2026-11-13 failure mode was
   Graph OK but my.microsoftpersonalcontent.com 401 BadContextToken.
3. Ships the token to hermes_settings.onedrive_refresh_token through a
   one-off hermes ECS task (Mac cannot reach RDS): token → private S3 object
   (SSE) → task reads + writes setting + deletes object.
   Restart hermes afterwards so its in-memory clients reload the token.

Needs ONEDRIVE_CLIENT_ID / ONEDRIVE_CLIENT_SECRET (config/.env).
"""
from __future__ import annotations

import base64
import gzip
import http.server
import json
import os
import secrets
import subprocess
import sys
import tempfile
import threading
import urllib.parse
import webbrowser
from pathlib import Path

import requests

REPO = Path(__file__).resolve().parents[1]
ONEDRIVE_ENV = Path.home() / "Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/config/.env"
for env in (REPO / "config" / ".env", ONEDRIVE_ENV):
    if env.exists():
        for line in env.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

CLIENT_ID = os.environ.get("ONEDRIVE_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("ONEDRIVE_CLIENT_SECRET", "")
SCOPES = "Files.ReadWrite offline_access User.Read"   # = onedrive_client._SCOPES
REDIRECT = "http://localhost:8080/callback"
TOKEN_URL = "https://login.microsoftonline.com/consumers/oauth2/v2.0/token"
GRAPH = "https://graph.microsoft.com/v1.0"
PROBE_PATH = "etrm/bess-platform/data/电站.xlsx"
BUCKET, REGION = "bess-uploader-data-chen-singp-2026", "ap-southeast-1"
CLUSTER, SERVICE = "bess-platform-cluster", "bess-platform-hermes-svc"
NET = ("awsvpcConfiguration={subnets=[subnet-08e93f99a9c0aa3a9,subnet-0e8ae30466a9f72b5],"
       "securityGroups=[sg-08576f2bea0274a81],assignPublicIp=DISABLED}")
AWS = os.environ.get("AWS_BIN") or str(Path.home() / ".local/bin/aws")


def consent() -> tuple[str, str]:
    code: list[str] = []
    state = secrets.token_urlsafe(16)

    class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            ok = q.get("state", [""])[0] == state and "code" in q
            if ok:
                code.append(q["code"][0])
            self.send_response(200 if ok else 400)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write("<h2>授权完成，可以关闭此页面。</h2>".encode() if ok
                             else f"<h2>授权失败: {q.get('error_description', ['?'])[0]}</h2>".encode())

        def log_message(self, *_):
            pass

    srv = http.server.HTTPServer(("localhost", 8080), H)
    t = threading.Thread(target=srv.handle_request, daemon=True)
    t.start()
    url = ("https://login.microsoftonline.com/consumers/oauth2/v2.0/authorize?"
           + urllib.parse.urlencode({"client_id": CLIENT_ID, "response_type": "code",
                                     "redirect_uri": REDIRECT, "scope": SCOPES,
                                     "response_mode": "query", "prompt": "select_account",
                                     "state": state}))
    print("正在打开浏览器，请登录微软账号并同意授权…\n若浏览器没有自动打开，请手动访问：\n" + url + "\n")
    webbrowser.open(url)
    t.join(timeout=300)
    srv.server_close()
    if not code:
        sys.exit("ERROR: 5 分钟内没有收到授权码（或 state 不匹配）。")
    r = requests.post(TOKEN_URL, data={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
                                       "code": code[0], "redirect_uri": REDIRECT,
                                       "grant_type": "authorization_code", "scope": SCOPES},
                      timeout=30)
    if not r.ok:
        sys.exit(f"ERROR: token exchange {r.status_code}: {r.json().get('error_description', '')[:300]}")
    j = r.json()
    if "refresh_token" not in j:
        sys.exit("ERROR: no refresh_token returned (offline_access consent missing?)")
    return j["access_token"], j["refresh_token"]


def verify(access_token: str) -> bool:
    h = {"Authorization": f"Bearer {access_token}"}
    me = requests.get(f"{GRAPH}/me", headers=h, timeout=20)
    print(f"Graph /me: {me.status_code} ({me.json().get('userPrincipalName', '?') if me.ok else ''})")
    p = "/".join(urllib.parse.quote(s, safe="") for s in PROBE_PATH.split("/"))
    r = requests.get(f"{GRAPH}/me/drive/root:/{p}:/content", headers=h, timeout=60)
    print(f"下载 {PROBE_PATH}: HTTP {r.status_code}, {len(r.content)} bytes")
    if not r.ok:
        print("  WWW-Authenticate fault:", (r.headers.get("WWW-Authenticate", "")
                                            .split("fault=")[-1].split(",")[0])[:60])
    return r.ok and len(r.content) > 0


def ship(refresh_token: str) -> None:
    key = f"tmp/reauth/{secrets.token_hex(12)}.txt"
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        os.chmod(f.name, 0o600)
        f.write(refresh_token)
        tmp = f.name
    try:
        subprocess.run([AWS, "s3", "cp", tmp, f"s3://{BUCKET}/{key}", "--sse", "AES256",
                        "--region", REGION, "--only-show-errors"], check=True)
    finally:
        os.unlink(tmp)
    payload = f"""
import os, boto3
from services.hermes.onedrive_client import _save_setting, _load_setting
s3 = boto3.client('s3', region_name='{REGION}')
tok = s3.get_object(Bucket='{BUCKET}', Key='{key}')['Body'].read().decode().strip()
_save_setting(os.environ['PGURL'], 'onedrive_refresh_token', tok)
print('STORED' if _load_setting(os.environ['PGURL'], 'onedrive_refresh_token') == tok else 'MISMATCH', len(tok), flush=True)
try:
    s3.delete_object(Bucket='{BUCKET}', Key='{key}')
except Exception as e:
    print('s3 delete skipped:', type(e).__name__, flush=True)
"""
    b64 = base64.b64encode(gzip.compress(payload.encode())).decode()

    def aws(*a):
        return subprocess.run([AWS, *a, "--region", REGION], check=True,
                              capture_output=True, text=True).stdout.strip()

    td = aws("ecs", "describe-services", "--cluster", CLUSTER, "--services", SERVICE,
             "--query", "services[0].taskDefinition", "--output", "text")
    cname = aws("ecs", "describe-task-definition", "--task-definition", td,
                "--query", "taskDefinition.containerDefinitions[0].name", "--output", "text")
    ov = json.dumps({"containerOverrides": [{"name": cname, "command": [
        "sh", "-c", f"cd /app && echo {b64} | base64 -d | gunzip | PYTHONPATH=/app python -u -"]}]})
    task = aws("ecs", "run-task", "--cluster", CLUSTER, "--task-definition", td,
               "--launch-type", "FARGATE", "--network-configuration", NET,
               "--overrides", ov, "--query", "tasks[0].taskArn", "--output", "text")
    print("写入 Hermes 数据库中（一次性 ECS 任务）…")
    aws("ecs", "wait", "tasks-stopped", "--cluster", CLUSTER, "--tasks", task)
    out = aws("logs", "get-log-events", "--log-group-name", "/ecs/bess-platform",
              "--log-stream-name", f"hermes/{cname}/{task.rsplit('/', 1)[-1]}",
              "--query", "events[*].message", "--output", "text")
    print("结果:", "✅ 已写入 hermes_settings" if "STORED" in out else f"❌ {out[-300:]}")
    subprocess.run([AWS, "s3", "rm", f"s3://{BUCKET}/{key}", "--region", REGION,
                    "--only-show-errors"], check=False)   # belt and braces


if __name__ == "__main__":
    if not CLIENT_ID or not CLIENT_SECRET:
        sys.exit("ERROR: ONEDRIVE_CLIENT_ID / ONEDRIVE_CLIENT_SECRET not set (config/.env).")
    at, rt = consent()
    if not verify(at):
        ans = input("\n新令牌仍然无法下载文件内容。仍要写入 Hermes 吗？[y/N] ").strip().lower()
        if ans != "y":
            sys.exit("未写入。请把上面的结果发给 Claude。")
    ship(rt)
    print("\n完成。请回到 Claude 回复“部署”：重启 Hermes 让新令牌生效（同时上线报错打码修复）。")
