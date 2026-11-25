"""Service-control registry must only offer scheduler mode where the image can run it."""
from unittest.mock import MagicMock

import pytest

from shared.service_control import SERVICES, set_service_mode


@pytest.mark.parametrize("market", ["ph", "po"])
def test_ph_po_have_no_standalone_scheduler(market):
    # PH/PO images do not ship services/intl_market_common/scheduler_service.py;
    # their jobs run inside the Streamlit process. Scheduler mode crash-loops
    # the service (exit 2), so "Stop Web" must resolve to a plain stop.
    assert SERVICES[market]["has_scheduler"] is False
    assert SERVICES[market]["scheduler_cmd"] == []


@pytest.mark.parametrize("market", ["ph", "po"])
def test_scheduler_mode_refused_without_touching_ecs(market):
    ecs = MagicMock()
    with pytest.raises(ValueError):
        set_service_mode(market, "scheduler", ecs, "cluster")
    ecs.register_task_definition.assert_not_called()
    ecs.update_service.assert_not_called()


# ── 2026-11-25: on-demand start/stop + nightly auto-stop ─────────────────────

class _FakeEcs:
    def __init__(self, desired, command=None):
        self.desired, self.command = dict(desired), command
        self.updates, self.registered = [], 0

    def describe_services(self, cluster, services):
        return {"services": [{"serviceName": s, "desiredCount": self.desired.get(s, 0),
                              "taskDefinition": f"arn:td/{s}:7"} for s in services]}

    def describe_task_definition(self, taskDefinition):
        name = taskDefinition.split("/")[-1].split(":")[0].replace("bess-platform-", "").replace("-svc", "")
        c = {"name": name, "image": "img"}
        if self.command:
            c["command"] = self.command
        return {"taskDefinition": {"family": "fam", "containerDefinitions": [c]}}

    def register_task_definition(self, **kw):
        self.registered += 1
        return {"taskDefinition": {"taskDefinitionArn": "arn:td/new:8"}}

    def update_service(self, **kw):
        self.updates.append(kw)


def test_auto_stop_only_touches_running_on_demand_services():
    from shared import service_control as sc
    ecs = _FakeEcs({"bess-platform-deal-structurer-svc": 1, "bess-platform-deeptutor-svc": 1,
                    "bess-platform-mengxi-dashboard-svc": 1, "bess-platform-crystal-ball-svc": 1,
                    "bess-platform-gb-market-svc": 1, "bess-platform-market-maker-svc": 1})
    stopped = sc.auto_stop_on_demand(ecs, "c")
    assert sorted(stopped) == ["Deal Structurer", "DeepTutor"]
    assert sorted(u["service"] for u in ecs.updates) == ["bess-platform-deal-structurer-svc", "bess-platform-deeptutor-svc"]
    assert all(u["desiredCount"] == 0 for u in ecs.updates)


def test_always_on_services_are_not_on_demand():
    from shared import service_control as sc
    for m in ("mengxi", "spot", "asset-risk", "bess-map", "gb", "crystal-ball", "market-maker", "retail-risk"):
        assert m in sc.SERVICES and m not in sc.ON_DEMAND


def test_web_start_reuses_the_services_own_revision():
    """A plain start must not register a task definition or move the service."""
    from shared import service_control as sc
    ecs = _FakeEcs({})
    r = sc.set_service_mode("deal-structurer", "web", ecs, "c")
    assert r["mode"] == "web" and ecs.registered == 0
    assert ecs.updates == [{"cluster": "c", "service": "bess-platform-deal-structurer-svc", "desiredCount": 1}]
