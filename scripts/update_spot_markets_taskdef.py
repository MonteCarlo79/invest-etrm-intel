"""
Register a new spot-markets ECS task definition with updated image + HERMES_URL,
then force-deploy the service.

Usage:
    python scripts/update_spot_markets_taskdef.py
"""
import json
import os
import subprocess
import sys

REGION = "ap-southeast-1"
CLUSTER = "bess-platform-cluster"
SERVICE = "bess-platform-spot-markets-svc"
FAMILY = "bess-platform-spot-markets"
ECR_REPO = "319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/bess-spot-markets"
IMAGE_TAG = os.environ.get("IMAGE_TAG", "v34")
HERMES_URL = os.environ.get("HERMES_URL", "https://pjh-etrm.ai")


def aws(*args):
    result = subprocess.run(
        ["aws"] + list(args) + ["--region", REGION],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        sys.exit(f"aws error: {result.stderr}")
    return json.loads(result.stdout)


print("Fetching current task definition...")
td_data = aws("ecs", "describe-task-definition", "--task-definition", FAMILY)
td = td_data["taskDefinition"]

for container in td["containerDefinitions"]:
    env = {e["name"]: e["value"] for e in container.get("environment", [])}
    env["HERMES_URL"] = HERMES_URL
    container["environment"] = [{"name": k, "value": v} for k, v in env.items()]
    container["image"] = f"{ECR_REPO}:{IMAGE_TAG}"

payload = {
    "family": td["family"],
    "networkMode": td.get("networkMode", "awsvpc"),
    "containerDefinitions": td["containerDefinitions"],
    "requiresCompatibilities": td.get("requiresCompatibilities", ["FARGATE"]),
    "cpu": td.get("cpu", "256"),
    "memory": td.get("memory", "512"),
}
if td.get("taskRoleArn"):
    payload["taskRoleArn"] = td["taskRoleArn"]
if td.get("executionRoleArn"):
    payload["executionRoleArn"] = td["executionRoleArn"]

print(f"Registering new task definition (image: {ECR_REPO}:{IMAGE_TAG}, HERMES_URL: {HERMES_URL})...")
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
