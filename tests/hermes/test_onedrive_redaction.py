"""OneDrive errors must never carry tempauth URLs (2026-11-13: 401s on
my.microsoftpersonalcontent.com were posted to Feishu with live tokens)."""
from unittest.mock import patch

import pytest
import requests

from services.hermes import onedrive_client as odc


def _resp(status, url):
    r = requests.Response()
    r.status_code, r.url, r.reason = status, url, "Unauthorized"
    return r


def test_error_message_drops_query_string():
    url = ("https://my.microsoftpersonalcontent.com/personal/abc/_layouts/15/download.aspx"
           "?UniqueId=x&tempauth=v1e.SECRETTOKEN")
    with pytest.raises(requests.HTTPError) as ei:
        odc._raise_for_status(_resp(401, url))
    msg = str(ei.value)
    assert "SECRETTOKEN" not in msg and "tempauth" not in msg
    assert "401" in msg and "my.microsoftpersonalcontent.com/personal/abc" in msg
    assert ei.value.response.status_code == 401


def test_ok_response_passes():
    odc._raise_for_status(_resp(200, "https://graph.microsoft.com/v1.0/me/drive"))


def test_read_file_by_path_redacts_redirect_target():
    c = odc.OneDriveClient(client_id="a", client_secret="b", refresh_token="t")
    leaked = "https://my.microsoftpersonalcontent.com/x/download.aspx?tempauth=v1e.LEAK"
    with patch.object(c, "_headers", return_value={}), \
         patch("requests.get", return_value=_resp(401, leaked)):
        with pytest.raises(requests.HTTPError) as ei:
            c.read_file_by_path("etrm/bess-platform/data/电站.xlsx")
    assert "LEAK" not in str(ei.value)
