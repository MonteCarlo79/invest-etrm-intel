#!/usr/bin/env bash
# Build + push a Docker image INSIDE AWS (CodeBuild project bess-platform-delta-build),
# so base-image layers never cross the China↔ap-southeast-1 link.
#
#   scripts/cloud_build.sh <context_dir> <ecr_repo> <tag> [--extra-tags "latest"] [--smoke "python -c 'import x'"]
#
# <context_dir> holds a Dockerfile (typically `FROM <ecr>/<repo>:<current>` + COPY of the
# changed files — the delta-build pattern). It is zipped to
# s3://bess-uploader-data-chen-singp-2026/codebuild/src/ and built with `docker build --pull`.
# Prints the pushed digest; exits non-zero if the build fails.
# Created 2026-11-13 (Mac ECR layer pulls were failing with EOF; Docker cache pruned).
# Infra (CLI-created, not in terraform): IAM role bess-platform-codebuild (inline policy
# delta-build), CodeBuild project bess-platform-delta-build, logs /codebuild/bess-platform-delta-build.
set -euo pipefail

CTX=${1:?context dir}; REPO=${2:?ecr repo}; TAG=${3:?tag}; shift 3
EXTRA_TAGS=""; SMOKE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --extra-tags) EXTRA_TAGS=$2; shift 2 ;;
    --smoke) SMOKE=$2; shift 2 ;;
    *) echo "unknown arg $1" >&2; exit 2 ;;
  esac
done
[ -f "$CTX/Dockerfile" ] || { echo "no Dockerfile in $CTX" >&2; exit 2; }

RG=ap-southeast-1; B=bess-uploader-data-chen-singp-2026; P=bess-platform-delta-build
AWS=${AWS_BIN:-$(command -v aws || echo "$HOME/.local/bin/aws")}
KEY="codebuild/src/${REPO}-${TAG}-$(date +%Y%m%d%H%M%S).zip"
ZIP=$(mktemp -t cbsrc).zip
(cd "$CTX" && zip -qr "$ZIP" .)
"$AWS" s3 cp "$ZIP" "s3://$B/$KEY" --region $RG --only-show-errors
rm -f "$ZIP"

ENV=$(python3 - "$REPO" "$TAG" "$EXTRA_TAGS" "$SMOKE" <<'PY'
import json, sys
repo, tag, extra, smoke = sys.argv[1:5]
print(json.dumps([{"name": "REPO", "value": repo, "type": "PLAINTEXT"},
                  {"name": "TAG", "value": tag, "type": "PLAINTEXT"},
                  {"name": "EXTRA_TAGS", "value": extra, "type": "PLAINTEXT"},
                  {"name": "SMOKE_CMD", "value": smoke, "type": "PLAINTEXT"}]))
PY
)
ID=$("$AWS" codebuild start-build --region $RG --project-name $P \
      --source-location-override "$B/$KEY" \
      --environment-variables-override "$ENV" \
      --query 'build.id' --output text)
echo "build $ID started ($REPO:$TAG)"

STATUS=IN_PROGRESS; LAST=""
while [ "$STATUS" = "IN_PROGRESS" ]; do
  sleep 15
  read -r STATUS PHASE < <("$AWS" codebuild batch-get-builds --region $RG --ids "$ID" \
      --query 'builds[0].[buildStatus,currentPhase]' --output text)
  [ "$PHASE" != "$LAST" ] && echo "  phase: $PHASE" && LAST=$PHASE
done

STREAM=$("$AWS" codebuild batch-get-builds --region $RG --ids "$ID" \
          --query 'builds[0].logs.streamName' --output text)
LOGS=$("$AWS" logs get-log-events --region $RG --log-group-name /codebuild/$P \
        --log-stream-name "$STREAM" --query 'events[*].message' --output text 2>/dev/null | tr '\t' '\n' || true)
"$AWS" s3 rm "s3://$B/$KEY" --region $RG --only-show-errors || true

if [ "$STATUS" != "SUCCEEDED" ]; then
  echo "BUILD $STATUS — last log lines:"; echo "$LOGS" | grep -v '^\s*$' | tail -25
  exit 1
fi
echo "$LOGS" | grep -E "^SMOKE|^DIGEST" | head -12
echo "BUILD SUCCEEDED: $REPO:$TAG${EXTRA_TAGS:+ (+ $EXTRA_TAGS)}"
