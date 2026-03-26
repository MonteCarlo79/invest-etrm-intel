# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 11:39:05 2026

@author: dipeng.chen
"""

import os
import boto3

ecs = boto3.client("ecs")

# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 11:39:05 2026

@author: dipeng.chen
"""

import os
import boto3
from datetime import datetime, timedelta, timezone

ecs = boto3.client("ecs")


def handler(event, context):
    start_date = os.environ["DEFAULT_START_DATE"]
    end_date = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

    response = ecs.run_task(
        cluster=os.environ["CLUSTER_ARN"],
        taskDefinition=os.environ["TASK_DEFINITION_ARN"],
        launchType="FARGATE",
        count=1,
        networkConfiguration={
            "awsvpcConfiguration": {
                "subnets": os.environ["SUBNET_IDS"].split(","),
                "securityGroups": [os.environ["SECURITY_GROUP_ID"]],
                "assignPublicIp": "ENABLED"
            }
        },
        overrides={
            "containerOverrides": [
                {
                    "name": os.environ["CONTAINER_NAME"],
                    "environment": [
                        {"name": "RUN_MODE", "value": "reconcile"},
                        {"name": "START_DATE", "value": start_date},
                        {"name": "END_DATE", "value": end_date},
                        {"name": "FORCE_RELOAD", "value": os.environ.get("DEFAULT_FORCE_RELOAD", "true")}
                    ]
                }
            ]
        }
    )

    tasks = response.get("tasks", [])
    failures = response.get("failures", [])

    return {
        "ok": len(failures) == 0,
        "task_count": len(tasks),
        "task_arns": [t.get("taskArn") for t in tasks],
        "start_date": start_date,
        "end_date": end_date,
        "failures": failures,
    }