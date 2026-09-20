#!/bin/bash
# setup_schedule_launchd.sh — one-time registration of the CITIC Futures
# weekly ingest+digest as a macOS LaunchAgent (daily 09:05).
#
# Run once:
#   bash services/citic_futures/setup_schedule_launchd.sh
#
# Prerequisites:
#   ~/.venvs/bess-platform venv exists
#   config/.env must contain PGURL, ANTHROPIC_API_KEY,
#   FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_OWNER_OPEN_ID
# ---------------------------------------------------------------------------

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PLIST_SRC="$REPO_ROOT/services/citic_futures/ai.pjh-etrm.citic-weekly.plist"
LABEL="ai.pjh-etrm.citic-weekly"
PLIST_DST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"

mkdir -p "$REPO_ROOT/logs" "$HOME/Library/LaunchAgents"

if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl bootout "$DOMAIN/$LABEL"
    echo "Unloaded existing agent: $LABEL"
fi

cp "$PLIST_SRC" "$PLIST_DST"

launchctl bootstrap "$DOMAIN" "$PLIST_DST"
echo "Registered: $LABEL (daily 09:05, ingest + digest new 中信期货周报 folders)"
echo "Plist:      $PLIST_DST"
echo "Wrapper:    $REPO_ROOT/services/citic_futures/run_weekly.sh"
echo "Log:        $REPO_ROOT/logs/citic_weekly.log"
echo ""
echo "Useful commands:"
echo "  Run now:        launchctl kickstart $DOMAIN/$LABEL"
echo "  Dry-run (no send): bash -lc 'cd $REPO_ROOT && set -a && source config/.env && set +a && ~/.venvs/bess-platform/bin/python services/citic_futures/run_weekly.py --no-send'"
echo "  Check status:   launchctl print $DOMAIN/$LABEL"
echo "  Unregister:     launchctl bootout $DOMAIN/$LABEL && rm $PLIST_DST"
