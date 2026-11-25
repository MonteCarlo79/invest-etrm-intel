#!/usr/bin/env bash
# Start / stop / list the on-demand dashboard services (phase 1 cost plan, 2026-11-25).
#   scripts/svc.sh status
#   scripts/svc.sh up deal-structurer      # ~1-2 min until it answers
#   scripts/svc.sh down deal-structurer
# Names are the part between "bess-platform-" and "-svc". Remember to bring it down again:
# an idle 0.5 vCPU / 1 GB task costs ~$26/month including its public IP.
set -euo pipefail
RG=ap-southeast-1; C=bess-platform-cluster
AWS=${AWS_BIN:-$(command -v aws || echo "$HOME/.local/bin/aws")}
ON_DEMAND="deal-structurer inner-mongolia ph-market po-market crystal-ball crystal-ball-client deeptutor au-market pjm-market ercot-market caiso-market"
cmd=${1:-status}; name=${2:-}
case "$cmd" in
  status)
    for s in $("$AWS" ecs list-services --cluster $C --region $RG --query 'serviceArns[]' --output text | tr '\t' '\n' | sed 's#.*/##' | sort); do
      "$AWS" ecs describe-services --cluster $C --services "$s" --region $RG \
        --query 'services[0].[serviceName,desiredCount,runningCount]' --output text
    done | awk '{n=$1; sub(/^bess-platform-/,"",n); sub(/-svc$/,"",n); printf "%-24s desired %s  running %s\n", n, $2, $3}' ;;
  up|down)
    [ -n "$name" ] || { echo "usage: $0 $cmd <name>   (on-demand: $ON_DEMAND)"; exit 2; }
    n=0; [ "$cmd" = up ] && n=1
    "$AWS" ecs update-service --cluster $C --service "bess-platform-$name-svc" --desired-count $n --region $RG \
      --query 'service.[serviceName,desiredCount]' --output text
    if [ "$cmd" = up ]; then
      echo "waiting for $name to become healthy…"
      "$AWS" ecs wait services-stable --cluster $C --services "bess-platform-$name-svc" --region $RG
      echo "up: https://www.pjh-etrm.ai/$name/   — run '$0 down $name' when done"
    fi ;;
  *) echo "usage: $0 status | up <name> | down <name>"; exit 2 ;;
esac
