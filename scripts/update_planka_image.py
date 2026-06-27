"""Pin Planka ECS task definition to a specific image version and force-deploy."""
import json
import subprocess
import sys

REGION = "ap-southeast-1"
CLUSTER = "bess-platform-cluster"
SERVICE = "bess-platform-planka-svc"
FAMILY = "bess-platform-planka"
IMAGE = "ghcr.io/plankanban/planka:1.15.2"


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

# Pin image
for container in td["containerDefinitions"]:
    container["image"] = IMAGE

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

print(f"Pinning image to {IMAGE}...")
reg = aws("ecs", "register-task-definition", "--cli-input-json", json.dumps(payload))
new_arn = reg["taskDefinition"]["taskDefinitionArn"]
print(f"New task def: {new_arn}")

print("Force-deploying Planka service...")
aws("ecs", "update-service",
    "--cluster", CLUSTER,
    "--service", SERVICE,
    "--task-definition", new_arn,
    "--force-new-deployment")

print("Done.")
