"""
Register a new hermes ECS task definition with env vars injected,
then force-deploy the service.
Usage:
    set HERMES_DB_URL=postgresql://...
    set ONEDRIVE_CLIENT_ID=...
    set ONEDRIVE_CLIENT_SECRET=...
    set ONEDRIVE_REFRESH_TOKEN=...
    python scripts/update_hermes_taskdef.py
"""
import json
import os
import subprocess
import sys

REGION = "ap-southeast-1"
CLUSTER = "bess-platform-cluster"
SERVICE = "bess-platform-hermes-svc"
FAMILY = "bess-platform-hermes"

ECR_REPO = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-platform-hermes"
IMAGE_TAG = os.environ.get("IMAGE_TAG", "")  # e.g. "v-token-persist"; empty = keep existing


def aws(*args):
    result = subprocess.run(
        ["aws"] + list(args) + ["--region", REGION],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        sys.exit(f"aws error: {result.stderr}")
    return json.loads(result.stdout)


print("Fetching current task definition...")
td_data = aws("ecs", "describe-task-definition", "--task-definition", FAMILY)
td = td_data["taskDefinition"]

# Update environment for each container
for container in td["containerDefinitions"]:
    env = {e["name"]: e["value"] for e in container.get("environment", [])}

    # Remove WeChat (no longer used)
    env.pop("WECHAT_OWNER_ID", None)

    # Inject vars from environment (only if set)
    for key in [
        "ANTHROPIC_API_KEY",
        "DEEPSEEK_API_KEY",
        "HERMES_DB_URL",
        "ONEDRIVE_CLIENT_ID", "ONEDRIVE_CLIENT_SECRET", "ONEDRIVE_REFRESH_TOKEN",
        "FEISHU_APP_ID", "FEISHU_APP_SECRET", "FEISHU_OWNER_OPEN_ID",
        "PGURL",
    ]:
        if os.environ.get(key):
            env[key] = os.environ[key]

    container["environment"] = [{"name": k, "value": v} for k, v in env.items()]

    # Optionally pin the image to a specific tag
    if IMAGE_TAG:
        container["image"] = f"{ECR_REPO}:{IMAGE_TAG}"

# Build registration payload — only include keys aws accepts
payload = {
    "family":                  td["family"],
    "networkMode":             td.get("networkMode", "awsvpc"),
    "containerDefinitions":    td["containerDefinitions"],
    "requiresCompatibilities": td.get("requiresCompatibilities", ["FARGATE"]),
    "cpu":                     td.get("cpu", "256"),
    "memory":                  td.get("memory", "512"),
}
if td.get("taskRoleArn"):
    payload["taskRoleArn"] = td["taskRoleArn"]
if td.get("executionRoleArn"):
    payload["executionRoleArn"] = td["executionRoleArn"]

print("Registering new task definition...")
reg = aws("ecs", "register-task-definition", "--cli-input-json", json.dumps(payload, ensure_ascii=True))
new_arn = reg["taskDefinition"]["taskDefinitionArn"]
print(f"New task def: {new_arn}")

print("Force-deploying service...")
aws("ecs", "update-service",
    "--cluster", CLUSTER,
    "--service", SERVICE,
    "--task-definition", new_arn,
    "--force-new-deployment")

print("Done — deployment started.")
print(f"Monitor: aws ecs describe-services --cluster {CLUSTER} --services {SERVICE} --region {REGION} --query 'services[0].deployments'")
