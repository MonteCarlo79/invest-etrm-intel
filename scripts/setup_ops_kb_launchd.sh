#!/bin/bash
# setup_ops_kb_launchd.sh
# ---------------------------------------------------------------------------
# One-time setup / refresh: registers the operating-assets KB ingest as a macOS
# LaunchAgent. Scans assets/operating/复盘/ hourly for drop-in files.
#
# Run once:
#   bash scripts/setup_ops_kb_launchd.sh
#
# Prerequisites: aws CLI not required; python venv at ~/.venvs/bess-platform;
# config/.env with PGURL (loaded by the app modules via dotenv).
# ---------------------------------------------------------------------------

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLIST_SRC="$REPO_ROOT/scripts/ai.pjh-etrm.ops-kb-ingest.plist"
LABEL="ai.pjh-etrm.ops-kb-ingest"
PLIST_DST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"

mkdir -p "$HOME/Library/LaunchAgents" "$HOME/Library/Logs/bess-ops-kb"

# Unload existing registration if present (idempotent re-runs)
if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl bootout "$DOMAIN/$LABEL"
    echo "Unloaded existing agent: $LABEL"
fi

cp "$PLIST_SRC" "$PLIST_DST"

launchctl bootstrap "$DOMAIN" "$PLIST_DST"
echo "Registered: $LABEL (hourly scan of assets/operating/复盘/)"
echo "Plist:      $PLIST_DST"
echo "Logs:       $HOME/Library/Logs/bess-ops-kb/"
echo ""
echo "Useful commands:"
echo "  Run now:        launchctl kickstart $DOMAIN/$LABEL"
echo "  Check status:   launchctl print $DOMAIN/$LABEL"
echo "  Unregister:     launchctl bootout $DOMAIN/$LABEL && rm $PLIST_DST"
