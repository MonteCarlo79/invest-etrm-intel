"""
ECS service control: toggle market app Streamlit UIs on/off while keeping schedulers running.

Used by:
  - apps/portal/app.py  (Admin Service Control section)
  - services/hermes/agent.py  (SERVICE_CONTROL Feishu action)
"""
from __future__ import annotations

import copy
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ── Service registry ──────────────────────────────────────────────────────────

_INTL_SCHEDULER = [
    "python", "services/intl_market_common/scheduler_service.py",
]

SERVICES: dict[str, dict[str, Any]] = {
    "gb": {
        "svc": "bess-platform-gb-market-svc",
        "family": "bess-platform-gb-market",
        "container": "gb-market",
        "has_scheduler": True,
        "scheduler_cmd": ["python", "apps/gb-market/scheduler_service.py"],
        "web_url": "/gb-market/",
        "label": "GB Market",
    },
    "au": {
        "svc": "bess-platform-au-market-svc",
        "family": "bess-platform-au-market",
        "container": "au-market",
        "has_scheduler": True,
        "scheduler_cmd": _INTL_SCHEDULER + ["--code", "au", "--app-dir", "apps/au-market"],
        "web_url": "/au-market/",
        "label": "AU Market",
    },
    "ercot": {
        "svc": "bess-platform-ercot-market-svc",
        "family": "bess-platform-ercot-market",
        "container": "ercot-market",
        "has_scheduler": True,
        "scheduler_cmd": _INTL_SCHEDULER + ["--code", "ercot", "--app-dir", "apps/ercot-market"],
        "web_url": "/ercot-market/",
        "label": "ERCOT Market",
    },
    "caiso": {
        "svc": "bess-platform-caiso-market-svc",
        "family": "bess-platform-caiso-market",
        "container": "caiso-market",
        "has_scheduler": True,
        "scheduler_cmd": _INTL_SCHEDULER + ["--code", "caiso", "--app-dir", "apps/caiso-market"],
        "web_url": "/caiso-market/",
        "label": "CAISO Market",
    },
    "pjm": {
        "svc": "bess-platform-pjm-market-svc",
        "family": "bess-platform-pjm-market",
        "container": "pjm-market",
        "has_scheduler": True,
        "scheduler_cmd": _INTL_SCHEDULER + ["--code", "pjm", "--app-dir", "apps/pjm-market"],
        "web_url": "/pjm-market/",
        "label": "PJM Market",
    },
    "ph": {
        "svc": "bess-platform-ph-market-svc",
        "family": "bess-platform-ph-market",
        "container": "ph-market",
        "has_scheduler": True,
        "scheduler_cmd": _INTL_SCHEDULER + ["--code", "ph", "--app-dir", "apps/ph-market"],
        "web_url": "/ph-market/",
        "label": "PH Market",
    },
    "po": {
        "svc": "bess-platform-po-market-svc",
        "family": "bess-platform-po-market",
        "container": "po-market",
        "has_scheduler": True,
        "scheduler_cmd": _INTL_SCHEDULER + ["--code", "po", "--app-dir", "apps/po-market"],
        "web_url": "/po-market/",
        "label": "PO Market",
    },
    "bess-map": {
        "svc": "bess-platform-bess-map-svc",
        "family": "bess-platform-bess-map",
        "container": "bess-map",
        "has_scheduler": False,
        "scheduler_cmd": [],
        "web_url": "/bess-map/",
        "label": "BESS Map",
    },
    "im": {
        "svc": "bess-platform-inner-mongolia-svc",
        "family": "bess-platform-inner-mongolia",
        "container": "inner-mongolia",
        "has_scheduler": False,
        "scheduler_cmd": [],
        "web_url": "/inner-mongolia/",
        "label": "Inner Mongolia",
    },
    "mengxi": {
        "svc": "bess-platform-mengxi-dashboard-svc",
        "family": "bess-platform-mengxi-dashboard",
        "container": "mengxi-dashboard",
        "has_scheduler": False,
        "scheduler_cmd": [],
        "web_url": "/mengxi-dashboard/",
        "label": "Mengxi Dashboard",
    },
    "options": {
        "svc": "bess-platform-options-cockpit-svc",
        "family": "bess-platform-options-cockpit",
        "container": "options-cockpit",
        "has_scheduler": False,
        "scheduler_cmd": [],
        "web_url": "/options-cockpit/",
        "label": "Options Cockpit",
    },
    "spot": {
        "svc": "bess-platform-spot-markets-svc",
        "family": "bess-platform-spot-markets",
        "container": "spot-markets",
        "has_scheduler": False,
        "scheduler_cmd": [],
        "web_url": "/spot-markets/",
        "label": "Spot Markets",
    },
    "deal-structurer": {
        "svc": "bess-platform-deal-structurer-svc",
        "family": "bess-platform-deal-structurer",
        "container": "deal-structurer",
        "has_scheduler": False,
        "scheduler_cmd": [],
        "web_url": "/deal-structurer/",
        "label": "Deal Structurer",
    },
    "asset-risk": {
        "svc": "bess-platform-asset-risk-svc",
        "family": "bess-platform-asset-risk",
        "container": "asset-risk",
        "has_scheduler": False,
        "scheduler_cmd": [],
        "web_url": "/asset-risk/",
        "label": "Asset Risk",
    },
    "retail-risk": {
        "svc": "bess-platform-retail-risk-svc",
        "family": "bess-platform-retail-risk",
        "container": "retail-risk",
        "has_scheduler": False,
        "scheduler_cmd": [],
        "web_url": "/retail-risk/",
        "label": "Retail Risk",
    },
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_latest_task_def(ecs_client, family: str) -> dict:
    """Fetch the latest active task definition for *family*."""
    resp = ecs_client.describe_task_definition(taskDefinition=family)
    return resp["taskDefinition"]


def _register_task_def_with_command(ecs_client, td: dict, container_name: str, command: list[str]) -> str:
    """Clone *td*, set command override on *container_name*, register + return new ARN."""
    new_td = {
        "family": td["family"],
        "networkMode": td.get("networkMode", "awsvpc"),
        "requiresCompatibilities": td.get("requiresCompatibilities", ["FARGATE"]),
        "cpu": td["cpu"],
        "memory": td["memory"],
    }
    if td.get("executionRoleArn"):
        new_td["executionRoleArn"] = td["executionRoleArn"]
    if td.get("taskRoleArn"):
        new_td["taskRoleArn"] = td["taskRoleArn"]

    containers = copy.deepcopy(td["containerDefinitions"])
    for c in containers:
        if c["name"] == container_name:
            if command:
                c["command"] = command
            else:
                c.pop("command", None)
            break

    # Strip read-only fields that can't be re-registered
    for c in containers:
        for key in ("environmentFiles",):
            c.pop(key, None)

    new_td["containerDefinitions"] = containers

    if td.get("volumes"):
        new_td["volumes"] = td["volumes"]
    if td.get("placementConstraints"):
        new_td["placementConstraints"] = td["placementConstraints"]

    resp = ecs_client.register_task_definition(**new_td)
    return resp["taskDefinition"]["taskDefinitionArn"]


# ── Public API ────────────────────────────────────────────────────────────────

def get_service_mode(market: str, ecs_client, cluster: str) -> str:
    """Returns 'web' | 'scheduler' | 'stopped'.

    Logic:
      - desiredCount == 0  → 'stopped'
      - desiredCount >= 1 and running task def has command set → 'scheduler'
      - desiredCount >= 1 and no command override  → 'web'
    """
    cfg = SERVICES[market]
    resp = ecs_client.describe_services(cluster=cluster, services=[cfg["svc"]])
    svc = resp["services"][0]

    if svc["desiredCount"] == 0:
        return "stopped"

    # Check the command on the active task def
    td_arn = svc["taskDefinition"]
    td_resp = ecs_client.describe_task_definition(taskDefinition=td_arn)
    td = td_resp["taskDefinition"]
    for c in td["containerDefinitions"]:
        if c["name"] == cfg["container"]:
            if c.get("command"):
                return "scheduler"
            break
    return "web"


def set_service_mode(market: str, mode: str, ecs_client, cluster: str) -> dict:
    """Set a service to 'web', 'scheduler', or 'stop'.

    Returns dict with keys: market, label, mode, web_url.
    """
    cfg = SERVICES[market]

    if mode == "stop":
        ecs_client.update_service(cluster=cluster, service=cfg["svc"], desiredCount=0)
        return {"market": market, "label": cfg["label"], "mode": "stopped", "web_url": cfg["web_url"]}

    if mode == "scheduler":
        if not cfg["has_scheduler"]:
            raise ValueError(f"Market '{market}' has no scheduler — use mode='stop' instead.")
        command = cfg["scheduler_cmd"]
    elif mode == "web":
        command = []  # Clear command override → use Dockerfile default
    else:
        raise ValueError(f"Unknown mode '{mode}'. Use 'web', 'scheduler', or 'stop'.")

    td = _get_latest_task_def(ecs_client, cfg["family"])
    new_arn = _register_task_def_with_command(ecs_client, td, cfg["container"], command)
    logger.info("Registered new task def %s for %s mode=%s", new_arn, market, mode)

    ecs_client.update_service(
        cluster=cluster,
        service=cfg["svc"],
        taskDefinition=new_arn,
        desiredCount=1,
    )

    actual_mode = mode if mode != "web" else "web"
    return {"market": market, "label": cfg["label"], "mode": actual_mode, "web_url": cfg["web_url"]}


def get_all_status(ecs_client, cluster: str) -> list[dict]:
    """Batch-describe all managed services.

    Returns list of {market, label, mode, web_url}.
    Tolerates individual failures (returns 'unknown' for those).
    """
    markets = list(SERVICES.keys())
    svc_names = [SERVICES[m]["svc"] for m in markets]

    # describe_services accepts up to 10 at a time
    all_svcs: dict[str, dict] = {}
    for i in range(0, len(svc_names), 10):
        chunk = svc_names[i:i + 10]
        resp = ecs_client.describe_services(cluster=cluster, services=chunk)
        for s in resp["services"]:
            all_svcs[s["serviceName"]] = s

    result = []
    for market in markets:
        cfg = SERVICES[market]
        svc = all_svcs.get(cfg["svc"])
        if not svc:
            result.append({
                "market": market, "label": cfg["label"],
                "mode": "unknown", "web_url": cfg["web_url"],
            })
            continue

        if svc["desiredCount"] == 0:
            mode = "stopped"
        else:
            try:
                td_arn = svc["taskDefinition"]
                td_resp = ecs_client.describe_task_definition(taskDefinition=td_arn)
                td = td_resp["taskDefinition"]
                mode = "web"
                for c in td["containerDefinitions"]:
                    if c["name"] == cfg["container"]:
                        if c.get("command"):
                            mode = "scheduler"
                        break
            except Exception:
                mode = "web"  # fallback — assume web if we can't check

        result.append({
            "market": market, "label": cfg["label"],
            "mode": mode, "web_url": cfg["web_url"],
        })
    return result
