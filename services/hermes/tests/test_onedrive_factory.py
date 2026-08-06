import os
from unittest.mock import MagicMock, patch

import services.hermes.onedrive_client as odc


def _reset():
    odc._SHARED_CLIENT = None


def test_returns_none_when_unconfigured(monkeypatch):
    _reset()
    monkeypatch.delenv("ONEDRIVE_CLIENT_ID", raising=False)
    monkeypatch.delenv("ONEDRIVE_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("ONEDRIVE_REFRESH_TOKEN", raising=False)
    monkeypatch.setattr(odc, "_load_setting", lambda pg_url, key: "")
    assert odc.get_shared_onedrive_client() is None


def test_builds_client_from_env(monkeypatch):
    _reset()
    monkeypatch.setenv("ONEDRIVE_CLIENT_ID", "cid")
    monkeypatch.setenv("ONEDRIVE_CLIENT_SECRET", "csec")
    monkeypatch.setenv("ONEDRIVE_REFRESH_TOKEN", "rt0")
    monkeypatch.setattr(odc, "_load_setting", lambda pg_url, key: "")
    client = odc.get_shared_onedrive_client()
    assert client is not None
    assert client.client_id == "cid"
    # singleton: second call returns same object
    assert odc.get_shared_onedrive_client() is client
    _reset()


def test_db_token_wins_over_env(monkeypatch):
    _reset()
    monkeypatch.setenv("ONEDRIVE_CLIENT_ID", "cid")
    monkeypatch.setenv("ONEDRIVE_CLIENT_SECRET", "csec")
    monkeypatch.setenv("ONEDRIVE_REFRESH_TOKEN", "rt-env")
    monkeypatch.setattr(odc, "_load_setting", lambda pg_url, key: "rt-db")
    client = odc.get_shared_onedrive_client(pg_url="postgres://x")
    assert client._refresh_token == "rt-db"
    _reset()


def test_set_shared_client_registers_instance(monkeypatch):
    odc._SHARED_CLIENT = None
    sentinel = odc.OneDriveClient(client_id="a", client_secret="b", refresh_token="t")
    odc.set_shared_onedrive_client(sentinel)
    assert odc.get_shared_onedrive_client() is sentinel
    odc._SHARED_CLIENT = None
