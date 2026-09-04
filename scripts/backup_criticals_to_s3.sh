#!/bin/bash
# backup_criticals_to_s3.sh
# ---------------------------------------------------------------------------
# Off-Mac backup of local-only critical files (the gap git/GitHub does not cover):
#   config/.env                      -> s3://$BUCKET/macbook-criticals/config/.env
#   infra/terraform/terraform.tfvars -> s3://$BUCKET/macbook-criticals/terraform.tfvars
#   infra/terraform/terraform.tfstate-> s3://$BUCKET/macbook-criticals/terraform.tfstate
#   .claude/settings.local.json      -> s3://$BUCKET/macbook-criticals/settings.local.json
#   ~/.claude/settings.json          -> s3://$BUCKET/macbook-criticals/claude-user-settings.json
#   ~/.claude/projects/*/memory/     -> s3://$BUCKET/macbook-criticals/claude-memory/...
#
# Backup semantics: upload only, NEVER delete. Bucket versioning keeps history.
# OneDrive-aware: dataless (cloud-evicted) files are SKIPPED, not waited on —
# reading a dataless file can stall for minutes while the provider is wedged.
#
# Manual run:   bash scripts/backup_criticals_to_s3.sh
# Scheduled:    bash scripts/setup_backup_launchd.sh  (daily 03:45)
# Overrides:    BESS_REPO_ROOT=/path/to/repo BESS_BACKUP_BUCKET=name bash ...
# ---------------------------------------------------------------------------

set -uo pipefail

REPO_ROOT="${BESS_REPO_ROOT:-/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform}"
BUCKET="${BESS_BACKUP_BUCKET:-bess-platform-macbook-backup}"
S3_PREFIX="s3://$BUCKET/macbook-criticals"
# Logs live on LOCAL disk — a backup job must not depend on the sync root
# it protects you from. launchd also cannot reliably write into CloudStorage.
LOG_DIR="${BESS_BACKUP_LOG_DIR:-$HOME/Library/Logs/bess-backup}"
LOG_FILE="$LOG_DIR/s3_backup.log"
CLAUDE_PROJECTS="$HOME/.claude/projects"

mkdir -p "$LOG_DIR"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"; }

# Dataless check is metadata-only (no content read) — safe on evicted OneDrive files.
is_dataless() { /bin/ls -lO "$1" 2>/dev/null | grep -q dataless; }

uploaded=0; skipped=0; failed=0

cp_file() { # $1=local path  $2=s3 key
    local src="$1" dst="$S3_PREFIX/$2"
    if [ ! -f "$src" ]; then log "SKIP (absent)    $src"; return; fi
    if is_dataless "$src"; then log "SKIP (dataless)  $src"; skipped=$((skipped+1)); return; fi
    if aws s3 cp --only-show-errors "$src" "$dst" >>"$LOG_FILE" 2>&1; then
        log "OK               $src"; uploaded=$((uploaded+1))
    else
        log "FAIL             $src"; failed=$((failed+1))
    fi
}

log "=== backup run start (bucket: $BUCKET, repo: $REPO_ROOT) ==="

if ! aws sts get-caller-identity >/dev/null 2>&1; then
    log "ABORT: aws CLI not authenticated"
    exit 1
fi

cp_file "$REPO_ROOT/config/.env"                       "config/.env"
cp_file "$REPO_ROOT/infra/terraform/terraform.tfvars"  "terraform.tfvars"
cp_file "$REPO_ROOT/infra/terraform/terraform.tfstate" "terraform.tfstate"
cp_file "$REPO_ROOT/.claude/settings.local.json"       "settings.local.json"
cp_file "$HOME/.claude/settings.json"                  "claude-user-settings.json"

# Claude auto-memory for every project (tiny; project dir names change if a
# repo moves, so back up all of them rather than hardcoding one).
if [ -d "$CLAUDE_PROJECTS" ]; then
    if aws s3 sync --only-show-errors "$CLAUDE_PROJECTS" "$S3_PREFIX/claude-memory/" \
        --exclude "*" --include "*/memory/*" --exclude "*/.DS_Store" >>"$LOG_FILE" 2>&1; then
        log "OK               $CLAUDE_PROJECTS (*/memory/*)"; uploaded=$((uploaded+1))
    else
        log "FAIL             $CLAUDE_PROJECTS sync"; failed=$((failed+1))
    fi
fi

log "=== done: $uploaded uploaded, $skipped skipped(dataless), $failed failed ==="
[ "$failed" -eq 0 ]
