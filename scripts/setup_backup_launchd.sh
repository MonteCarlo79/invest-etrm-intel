#!/bin/bash
# setup_backup_launchd.sh
# ---------------------------------------------------------------------------
# One-time setup / refresh: installs the S3 criticals backup as a macOS
# LaunchAgent. Runs daily at 03:45; if the Mac is asleep, the job runs at
# next wake.
#
#   bash scripts/setup_backup_launchd.sh
#
# What it does:
#   1. installs a COPY of backup_criticals_to_s3.sh to ~/.local/bin/
#      (launchd cannot execute scripts inside ~/Library/CloudStorage —
#      macOS TCC kills it with exit 126, so the repo copy is the source
#      and the installed copy is what actually runs)
#   2. registers ~/Library/LaunchAgents/ai.pjh-etrm.s3-backup.plist
#
# Prerequisites:
#   - aws CLI authenticated (aws sts get-caller-identity succeeds)
#   - bucket created: terraform apply (infra/terraform/s3_backup.tf)
# Re-run after any edit to scripts/backup_criticals_to_s3.sh or the plist.
# ---------------------------------------------------------------------------

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_SRC="$REPO_ROOT/scripts/backup_criticals_to_s3.sh"
SCRIPT_DST="$HOME/.local/bin/bess-backup-criticals.sh"
PLIST_SRC="$REPO_ROOT/scripts/ai.pjh-etrm.s3-backup.plist"
LABEL="ai.pjh-etrm.s3-backup"
PLIST_DST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"

mkdir -p "$HOME/.local/bin" "$HOME/Library/LaunchAgents" "$HOME/Library/Logs/bess-backup"

cp "$SCRIPT_SRC" "$SCRIPT_DST"
echo "Installed: $SCRIPT_DST"

# Unload existing registration if present (idempotent re-runs)
if launchctl print "$DOMAIN/$LABEL" >/dev/null 2>&1; then
    launchctl bootout "$DOMAIN/$LABEL"
    echo "Unloaded existing agent: $LABEL"
fi

cp "$PLIST_SRC" "$PLIST_DST"

launchctl bootstrap "$DOMAIN" "$PLIST_DST"
echo "Registered: $LABEL (daily 03:45)"
echo "Plist:      $PLIST_DST"
echo "Runs:       $SCRIPT_DST (installed copy)"
echo "Log:        $HOME/Library/Logs/bess-backup/s3_backup.log"
echo ""
echo "Useful commands:"
echo "  Run now:        launchctl kickstart $DOMAIN/$LABEL"
echo "  Check status:   launchctl print $DOMAIN/$LABEL"
echo "  Unregister:     launchctl bootout $DOMAIN/$LABEL && rm $PLIST_DST"
