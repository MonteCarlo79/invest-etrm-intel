"""
One-time auth: get a Microsoft refresh token with Mail.Read scope for Outlook.
Uses device code flow. Requires "allowPublicClient": true in the Azure app manifest.

Run once:
    py scripts/auth_microsoft_mail.py

Follow the on-screen instructions (visit a URL, enter a code), then copy
OUTLOOK_REFRESH_TOKEN into your ECS task definition.
"""
import os
import sys
import pathlib
import time

import requests

_repo = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_repo))

try:
    from dotenv import load_dotenv
    for _env in [_repo / "config" / ".env", _repo / ".env"]:
        if _env.exists():
            load_dotenv(_env)
except ImportError:
    pass

CLIENT_ID = os.environ.get("ONEDRIVE_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("ONEDRIVE_CLIENT_SECRET", "")
TENANT = "consumers"
SCOPES = "Mail.Read Mail.ReadWrite offline_access"

if not CLIENT_ID:
    sys.exit("ERROR: ONEDRIVE_CLIENT_ID not set.")

resp = requests.post(
    f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/devicecode",
    data={"client_id": CLIENT_ID, "scope": SCOPES},
    timeout=15,
)
resp.raise_for_status()
data = resp.json()

print("\n" + "=" * 60)
print(data["message"])
print("=" * 60 + "\n")

device_code = data["device_code"]
interval = int(data.get("interval", 5))
deadline = time.time() + int(data.get("expires_in", 900))

while time.time() < deadline:
    time.sleep(interval)
    token_resp = requests.post(
        f"https://login.microsoftonline.com/{TENANT}/oauth2/v2.0/token",
        data={
            "client_id": CLIENT_ID,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_code": device_code,
        },
        timeout=15,
    )
    token_data = token_resp.json()

    if "access_token" in token_data:
        refresh_token = token_data.get("refresh_token", "")
        print("\n\n✅ Auth successful!\n")
        print(f"OUTLOOK_REFRESH_TOKEN={refresh_token}")
        print("\nAdd this to your ECS task definition as OUTLOOK_REFRESH_TOKEN.")
        sys.exit(0)

    error = token_data.get("error", "")
    if error == "authorization_pending":
        print(".", end="", flush=True)
    elif error == "authorization_declined":
        sys.exit("\nAuthorization declined.")
    elif error == "expired_token":
        sys.exit("\nDevice code expired. Run the script again.")
    else:
        sys.exit(f"\nUnexpected error: {token_data}")

sys.exit("Timed out waiting for authorization.")
