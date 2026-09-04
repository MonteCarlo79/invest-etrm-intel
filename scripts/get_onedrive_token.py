#!/usr/bin/env python3
"""
One-shot script to obtain a Microsoft Graph refresh token for personal OneDrive.
Run once, copy the refresh token into Terraform / .env.
Never commit this file after filling in CLIENT_SECRET.
"""
import http.server
import threading
import urllib.parse
import webbrowser
import os
import sys

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests")

CLIENT_ID = "d4b92bf4-b1d6-4772-8d6e-1dede55a863c"
CLIENT_SECRET = os.environ.get("ONEDRIVE_CLIENT_SECRET", "")
TENANT = "consumers"
REDIRECT_URI = "http://localhost:8080/callback"
SCOPES = "Files.ReadWrite offline_access User.Read"

if not CLIENT_SECRET:
    sys.exit(
        "Set ONEDRIVE_CLIENT_SECRET env var before running.\n"
        "  Windows PowerShell:  $env:ONEDRIVE_CLIENT_SECRET='<secret>'\n"
        "  Bash:                export ONEDRIVE_CLIENT_SECRET='<secret>'"
    )

auth_code: list[str] = []


class CallbackHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if "code" in params:
            auth_code.append(params["code"][0])
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h2>Auth code captured. You can close this tab.</h2>")
        else:
            self.send_response(400)
            self.end_headers()

    def log_message(self, *_):
        pass


server = http.server.HTTPServer(("localhost", 8080), CallbackHandler)
t = threading.Thread(target=server.handle_request)
t.start()

auth_url = (
    f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/authorize"
    f"?client_id={CLIENT_ID}"
    f"&response_type=code"
    f"&redirect_uri={urllib.parse.quote(REDIRECT_URI)}"
    f"&scope={urllib.parse.quote(SCOPES)}"
    f"&response_mode=query"
    f"&prompt=select_account"
)

print("Opening browser for Microsoft consent...")
print(auth_url)
webbrowser.open(auth_url)

t.join(timeout=300)
server.server_close()

if not auth_code:
    sys.exit("ERROR: No auth code received within 5 minutes.")

print("\nExchanging auth code for tokens...")
try:
    resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code": auth_code[0],
            "redirect_uri": REDIRECT_URI,
            "grant_type": "authorization_code",
            "scope": SCOPES,
        },
        timeout=30,
    )
    print(f"HTTP {resp.status_code}")
    tokens = resp.json()

    if "refresh_token" not in tokens:
        print("ERROR: No refresh token in response:")
        print(tokens)
        input("\nPress Enter to exit...")
        sys.exit(1)

    refresh_token = tokens["refresh_token"]

    print("\n=== REFRESH TOKEN ===")
    print(refresh_token)
    print("====================")

    # Also write to file so it's not lost
    out_path = os.path.join(os.path.dirname(__file__), "onedrive_refresh_token.txt")
    with open(out_path, "w") as f:
        f.write(refresh_token)
    print(f"\nAlso saved to: {out_path}")
    print(f"Access token expires in: {tokens.get('expires_in', '?')} seconds")

except Exception as e:
    print(f"ERROR during token exchange: {e}")
    if "resp" in dir():
        print(f"Response body: {resp.text}")

input("\nPress Enter to exit...")
