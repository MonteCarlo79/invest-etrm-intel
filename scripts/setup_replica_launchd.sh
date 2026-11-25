#!/bin/bash
# One-time: register the nightly local-replica sync as a macOS LaunchAgent.
# Daily 05:30 local (after the 04:00 LingFeng ingest); if the Mac is asleep it
# runs at next wake. The S3 download is the slow step (~1.2 GB).
#   bash scripts/setup_replica_launchd.sh          register
#   bash scripts/setup_replica_launchd.sh remove   unregister
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="ai.pjh-etrm.local-replica"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"
LOG="$HOME/local-replica/logs/sync.log"

launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1 && launchctl bootout "$DOMAIN/$LABEL"
if [ "${1:-}" = "remove" ]; then rm -f "$PLIST"; echo "Removed $LABEL"; exit 0; fi

cat >"$PLIST" <<PL
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key><array><string>/bin/bash</string><string>$REPO/scripts/replica.sh</string><string>sync</string></array>
  <key>StartCalendarInterval</key><dict><key>Hour</key><integer>5</integer><key>Minute</key><integer>30</integer></dict>
  <key>EnvironmentVariables</key><dict><key>PATH</key><string>$(dirname "$(command -v aws)"):/usr/local/bin:/usr/bin:/bin</string><key>HOME</key><string>$HOME</string></dict>
  <key>StandardOutPath</key><string>$LOG</string>
  <key>StandardErrorPath</key><string>$LOG</string>
</dict></plist>
PL
launchctl bootstrap "$DOMAIN" "$PLIST"
echo "Registered $LABEL (daily 05:30). Log: $LOG"
