#!/usr/bin/env bash
# Run the platform dashboards on this Mac against the local replica.
#
#   scripts/local_apps.sh up [name ...]   start apps (default: the core set) and open the portal
#   scripts/local_apps.sh down [name ...] stop apps (default: all running)
#   scripts/local_apps.sh list            what is running, and every app name
#
# Database = local replica (scripts/replica.sh). Anything written locally
# (agent memory, uploads) is lost at the next replica restore. Online, the
# agents call Bedrock from the Mac as usual; offline they fail, the data views do not.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
R="${REPLICA_HOME:-$HOME/local-replica}"
VENV="${BESS_VENV:-$HOME/.venvs/bess-platform}"
ENV_FILE="${BESS_ENV_FILE:-$HOME/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/config/.env}"
RUN="$R/run"; LOGS="$R/logs"
mkdir -p "$RUN" "$LOGS"

# name|entry point|port — from the single app registry (shared/app_registry.py), also used by the portal
APPS="$(cd "$REPO" && /usr/bin/python3 shared/app_registry.py)"
CORE="portal spot-market bess-map mengxi-dashboard asset-risk"

lookup() { echo "$APPS" | awk -F'|' -v n="$1" '$1==n{print $2"|"$3}'; }
alive()  { [ -f "$RUN/$1.pid" ] && kill -0 "$(cat "$RUN/$1.pid")" 2>/dev/null; }

load_env() {
  if [ -f "$ENV_FILE" ]; then
    # read line by line: macOS bash 3.2 cannot source a process substitution
    while IFS= read -r line; do
      case "$line" in [A-Za-z_]*=*) k="${line%%=*}"; v="${line#*=}"; v="${v%\"}"; v="${v#\"}"; export "$k=$v" ;; esac
    done <"$ENV_FILE"
  else
    echo "[warn] $ENV_FILE not found — API keys unset, agents will not answer"
  fi
  "$REPO/scripts/replica.sh" start >/dev/null
  export PGHOST=localhost PGPORT=5433 PGDATABASE=marketdata PGUSER=postgres PGPASSWORD=local PGSSLMODE=disable
  export PGURL="postgresql://postgres@localhost:5433/marketdata"
  export DB_DSN="$PGURL" DATABASE_URL="$PGURL" DB_URL="$PGURL"
  export DB_HOST=localhost DB_PORT=5433 DB_NAME=marketdata DB_USER=postgres DB_PASSWORD=local
  export AUTH_MODE=dev DEV_USER_ROLE=Admin LOCAL_REPLICA=1
  # portal links -> local ports (its built-in dev map misses several apps)
  APP_URL_MAP="$(echo "$APPS" | awk -F'|' '{printf "%s=http://localhost:%s,", $1, $3}')spot-markets=http://localhost:8505"
  export APP_URL_MAP
  export PYTHONPATH="$REPO${PYTHONPATH:+:$PYTHONPATH}"
}

up() {
  load_env
  for n in "$@"; do
    spec=$(lookup "$n"); [ -n "$spec" ] || { echo "unknown app: $n"; continue; }
    if alive "$n"; then echo "$n already running on http://localhost:${spec#*|}"; continue; fi
    pp="$PYTHONPATH"; wd="$REPO"
    if [ "$n" = inner-mongolia ]; then
      # its image ships its own shared/ (no __init__), which the repo-root shared/ package
      # would shadow — mirror the image layout: only auth/ and that shared/ on the path
      mkdir -p "$RUN/im-root"
      ln -sfn "$REPO/apps/bess-inner-mongolia/shared" "$RUN/im-root/shared"
      ln -sfn "$REPO/auth" "$RUN/im-root/auth"
      pp="$RUN/im-root"; wd="$RUN/im-root"   # cwd is on sys.path too
    fi
    # ';' not '&&' before nohup: '&' would background the whole AND-list and $! would be a wrapper subshell
    ( cd "$wd" || exit 1; PYTHONPATH="$pp" nohup "$VENV/bin/streamlit" run "$REPO/${spec%|*}" --server.port "${spec#*|}" \
        --server.headless true --browser.gatherUsageStats false >"$LOGS/$n.log" 2>&1 </dev/null & echo $! >"$RUN/$n.pid" )
    echo "$n → http://localhost:${spec#*|}   (log: $LOGS/$n.log)"
  done
}

down() {
  for n in "$@"; do
    if alive "$n"; then
      pid=$(cat "$RUN/$n.pid"); kill "$pid"
      for _ in 1 2 3 4 5 6 7 8 9 10; do kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
      kill -9 "$pid" 2>/dev/null || true   # Streamlit can sit on SIGTERM while a browser tab is attached
      echo "stopped $n"
    fi
    rm -f "$RUN/$n.pid"
  done
}

running() { for f in "$RUN"/*.pid; do [ -e "$f" ] || continue; n=$(basename "$f" .pid); alive "$n" && echo "$n"; done; }

cmd="${1:-list}"; shift || true
case "$cmd" in
  up)   # shellcheck disable=SC2086
        up ${@:-$CORE}; sleep 3; [ -n "${NO_OPEN:-}" ] || open "http://localhost:8500" 2>/dev/null || true ;;
  down) # shellcheck disable=SC2046
        down ${@:-$(running)} ;;
  list) echo "running:"; for n in $(running); do echo "  $n  http://localhost:$(lookup "$n" | cut -d'|' -f2)"; done
        echo "available: $(echo "$APPS" | cut -d'|' -f1 | tr '\n' ' ')"
        "$REPO/scripts/replica.sh" status ;;
  *)    echo "usage: $0 up [name ...] | down [name ...] | list"; exit 2 ;;
esac
