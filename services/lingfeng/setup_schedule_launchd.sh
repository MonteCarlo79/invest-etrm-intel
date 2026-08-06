#!/bin/bash
# setup_schedule_launchd.sh
# ---------------------------------------------------------------------------
# One-time setup: registers the LingFeng daily collection as a macOS LaunchAgent
# (equivalent of setup_schedule.ps1 on Windows). Runs daily at 04:00 for all
# 29 provinces; if the Mac is asleep, the job runs at next wake.
#
# Run once:
#   bash services/lingfeng/setup_schedule_launchd.sh
#
# Prerequisites:
#   source ~/.venvs/bess-platform/bin/activate   (or: uv pip install playwright)
#   python -m playwright install chromium
#   config/.env must contain LINGFENG_USERNAME / LINGFENG_PASSWORD / PGURL
# ---------------------------------------------------------------------------

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PLIST_SRC="$REPO_ROOT/services/lingfeng/ai.pjh-etrm.lingfeng-daily.plist"
LABEL="ai.pjh-etrm.lingfeng-daily"
PLIST_DST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"

mkdir -p "$REPO_ROOT/logs" "$HOME/Library/LaunchAgents"

# Unload existing registration if present (idempotent re-runs)
if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl bootout "$DOMAIN/$LABEL"
    echo "Unloaded existing agent: $LABEL"
fi

cp "$PLIST_SRC" "$PLIST_DST"

launchctl bootstrap "$DOMAIN" "$PLIST_DST"
echo "Registered: $LABEL (daily 04:00, all 29 provinces, 3 models)"
echo "Plist:      $PLIST_DST"
echo "Wrapper:    $REPO_ROOT/services/lingfeng/run_daily.sh"
echo "Log:        $REPO_ROOT/logs/lingfeng_daily.log"
echo ""
echo "Useful commands:"
echo "  Run now:        launchctl kickstart $DOMAIN/$LABEL   # FULL 29-province run (~1h)"
echo "  Check status:   launchctl print $DOMAIN/$LABEL"
echo "  Unregister:     launchctl bootout $DOMAIN/$LABEL && rm $PLIST_DST"
