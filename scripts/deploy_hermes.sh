#!/usr/bin/env bash
# Deploy hermes-service to ECR + force-new ECS deployment
# Usage:  bash scripts/deploy_hermes.sh
set -euo pipefail

REGION="ap-southeast-1"
ACCOUNT="319383842493"
REPO="bess-platform-hermes"
CLUSTER="bess-platform-cluster"
SERVICE="bess-platform-hermes-svc"
IMAGE="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com/${REPO}:latest"

cd "$(git rev-parse --show-toplevel 2>/dev/null || pwd)"

echo "=== Logging in to ECR ==="
aws ecr get-login-password --region "$REGION" | \
  docker login --username AWS --password-stdin "${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"

echo "=== Building image ==="
docker build \
  -f apps/hermes-service/Dockerfile \
  -t "$IMAGE" \
  --platform linux/amd64 \
  .

echo "=== Pushing image ==="
docker push "$IMAGE"

echo "=== Updating ECS task definition with new OneDrive env vars ==="
# Fetch current task definition
TASK_DEF_ARN=$(aws ecs describe-services \
  --cluster "$CLUSTER" \
  --services "$SERVICE" \
  --region "$REGION" \
  --query 'services[0].taskDefinition' \
  --output text)

TASK_DEF=$(aws ecs describe-task-definition \
  --task-definition "$TASK_DEF_ARN" \
  --region "$REGION")

# Extract container defs and inject OneDrive env vars
NEW_TASK_DEF=$(echo "$TASK_DEF" | ${PYTHON:-$(command -v python3 || command -v python || command -v py)} -c "
import json, sys, os

data = json.load(sys.stdin)
td = data['taskDefinition']

containers = td['containerDefinitions']
IMAGE = os.environ.get('IMAGE', '$IMAGE')
for c in containers:
    # Always use :latest (never keep a pinned :vN tag — it causes CannotPullContainerError on redeploy)
    c['image'] = IMAGE
    env = {e['name']: e['value'] for e in c.get('environment', [])}
    # Inject OneDrive vars (values come from env or keep existing)
    env['ONEDRIVE_CLIENT_ID']     = os.environ.get('ONEDRIVE_CLIENT_ID',     env.get('ONEDRIVE_CLIENT_ID', ''))
    env['ONEDRIVE_CLIENT_SECRET'] = os.environ.get('ONEDRIVE_CLIENT_SECRET', env.get('ONEDRIVE_CLIENT_SECRET', ''))
    env['ONEDRIVE_REFRESH_TOKEN'] = os.environ.get('ONEDRIVE_REFRESH_TOKEN', env.get('ONEDRIVE_REFRESH_TOKEN', ''))
    # WeCom group webhook for daily ranking report (optional)
    if os.environ.get('WECOM_RANKING_WEBHOOK_URL'):
        env['WECOM_RANKING_WEBHOOK_URL'] = os.environ['WECOM_RANKING_WEBHOOK_URL']
    # Remove legacy WeChat var
    env.pop('WECHAT_OWNER_ID', None)
    c['environment'] = [{'name': k, 'value': v} for k, v in env.items()]

# Build registration payload
out = {
    'family': td['family'],
    'taskRoleArn': td.get('taskRoleArn',''),
    'executionRoleArn': td.get('executionRoleArn',''),
    'networkMode': td.get('networkMode','awsvpc'),
    'containerDefinitions': containers,
    'requiresCompatibilities': td.get('requiresCompatibilities',['FARGATE']),
    'cpu': td.get('cpu','256'),
    'memory': td.get('memory','512'),
}
print(json.dumps(out))
")

NEW_ARN=$(aws ecs register-task-definition \
  --region "$REGION" \
  --cli-input-json "$NEW_TASK_DEF" \
  --query 'taskDefinition.taskDefinitionArn' \
  --output text)

echo "New task definition: $NEW_ARN"

echo "=== Force-deploying ECS service ==="
aws ecs update-service \
  --cluster "$CLUSTER" \
  --service "$SERVICE" \
  --task-definition "$NEW_ARN" \
  --force-new-deployment \
  --region "$REGION" \
  --query 'service.deployments[0].status' \
  --output text

echo "=== Done — deployment started. Monitor at: ==="
echo "  aws ecs describe-services --cluster $CLUSTER --services $SERVICE --region $REGION --query 'services[0].deployments'"
