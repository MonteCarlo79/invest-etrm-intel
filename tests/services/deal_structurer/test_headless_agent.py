from unittest.mock import patch, MagicMock
from services.deal_structurer.headless_agent import run_deal_query


def test_run_deal_query_returns_string():
    mock_resp = MagicMock()
    mock_resp.stop_reason = "end_turn"
    mock_resp.content = [MagicMock(text="IRR is 12%")]
    mock_resp.content[0].type = "text"

    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_resp

    with patch("services.deal_structurer.headless_agent._make_client", return_value=mock_client):
        result = run_deal_query("What is the IRR for a 100MWh BESS in 蒙西?", api_key="test-key")

    assert isinstance(result, str)
    assert len(result) > 0
