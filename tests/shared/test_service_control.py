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
