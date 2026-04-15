"""
libs/decision_models/runners/ecs_batch.py

Thin ECS RunTask wrapper.

Submits a model run as an ECS Fargate task via boto3.
The ECS task is expected to:
  1. pull inputs from S3 or environment variables
  2. import and run the model via `runners.local.run()`
  3. write outputs back to S3 or RDS

This module handles only the submission side.
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional


def submit_ecs_task(
    model_name: str,
    inputs: Dict[str, Any],
    version: Optional[str] = None,
    cluster: Optional[str] = None,
    task_definition: Optional[str] = None,
    subnet_ids: Optional[List[str]] = None,
    security_group_ids: Optional[List[str]] = None,
    s3_input_uri: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Submit a model run to ECS Fargate.

    Environment variables used as defaults:
        ECS_CLUSTER            - cluster ARN or name
        ECS_TASK_DEFINITION    - task definition family:revision
        ECS_SUBNET_IDS         - comma-separated subnet IDs
        ECS_SECURITY_GROUP_IDS - comma-separated security group IDs

    Returns the boto3 run_task response dict.
    """
    import boto3

    cluster = cluster or os.environ["ECS_CLUSTER"]
    task_definition = task_definition or os.environ["ECS_TASK_DEFINITION"]
    subnet_ids = subnet_ids or os.environ["ECS_SUBNET_IDS"].split(",")
    security_group_ids = (
        security_group_ids or os.environ["ECS_SECURITY_GROUP_IDS"].split(",")
    )

    overrides: Dict[str, Any] = {
        "containerOverrides": [
            {
                "name": "decision-model-runner",
                "environment": [
                    {"name": "MODEL_NAME", "value": model_name},
                    {"name": "MODEL_VERSION", "value": version or "latest"},
                    {"name": "MODEL_INPUTS_JSON", "value": json.dumps(inputs)},
                ],
            }
        ]
    }
    if s3_input_uri:
        overrides["containerOverrides"][0]["environment"].append(
            {"name": "MODEL_INPUTS_S3_URI", "value": s3_input_uri}
        )

    client = boto3.client("ecs")
    response = client.run_task(
        cluster=cluster,
        taskDefinition=task_definition,
        launchType="FARGATE",
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": subnet_ids,
                "securityGroups": security_group_ids,
                "assignPublicIp": "DISABLED",
            }
        },
        overrides=overrides,
    )
    return response
