from unittest.mock import patch, MagicMock
from services.retail_risk.headless_agent import run_retail_risk_query


def test_run_retail_risk_query_returns_string():
    mock_resp = MagicMock()
    mock_resp.stop_reason = "end_turn"
    mock_resp.content = [MagicMock(text="Top customer margin is 200,000 CNY")]
    mock_resp.content[0].type = "text"

    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_resp

    with patch("services.retail_risk.headless_agent._make_client", return_value=mock_client):
        with patch("services.retail_risk.headless_agent._make_engine", return_value=MagicMock()):
            result = run_retail_risk_query("Top 5 customers by margin?", api_key="test-key", pg_url="postgresql://test")

    assert isinstance(result, str)
    assert len(result) > 0
