#!/bin/bash
# One-time: register the weekly Feishu -> KB re-sync as a macOS LaunchAgent.
# Sunday 01:30 local; if the Mac is asleep it runs at next wake. caffeinate keeps the Mac awake for the
# multi-hour folder listing. Needs ~/feishu-ingest (config.json + state.json from login.py / login_tenant.py).
#   bash services/feishu_ingest/setup_weekly_launchd.sh          register
#   bash services/feishu_ingest/setup_weekly_launchd.sh remove   unregister
set -euo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
LABEL="ai.pjh-etrm.feishu-weekly"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"
PY="${BESS_VENV:-$HOME/.venvs/bess-platform}/bin/python"
LOG="$HOME/feishu-ingest/logs/weekly_launchd.log"

launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1 && launchctl bootout "$DOMAIN/$LABEL"
if [ "${1:-}" = "remove" ]; then rm -f "$PLIST"; echo "Removed $LABEL"; exit 0; fi

cat >"$PLIST" <<PL
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key><array>
    <string>/usr/bin/caffeinate</string><string>-i</string>
    <string>$PY</string><string>$REPO/services/feishu_ingest/weekly_sync.py</string>
  </array>
  <key>WorkingDirectory</key><string>$REPO</string>
  <key>StartCalendarInterval</key><dict>
    <key>Weekday</key><integer>0</integer><key>Hour</key><integer>1</integer><key>Minute</key><integer>30</integer>
  </dict>
  <key>EnvironmentVariables</key><dict>
    <key>PATH</key><string>$(dirname "$(command -v aws)"):/usr/local/bin:/usr/bin:/bin</string>
    <key>HOME</key><string>$HOME</string>
  </dict>
  <key>StandardOutPath</key><string>$LOG</string>
  <key>StandardErrorPath</key><string>$LOG</string>
</dict></plist>
PL
launchctl bootstrap "$DOMAIN" "$PLIST"
echo "Registered $LABEL (Sundays 01:30). Log: $HOME/feishu-ingest/logs/weekly_sync.log"
