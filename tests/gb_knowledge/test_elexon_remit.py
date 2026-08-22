"""Unit tests for the REMIT message mapper (pure function, no network/DB)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services.gb_knowledge.elexon_remit import map_message

DOC_SHAPE = {
    "messageId": "abc-123",
    "publishedDateTime": "2026-08-19T14:23:00Z",
    "eventStart": "2026-08-20T06:00:00Z",
    "eventEnd": "2026-08-22T18:00:00Z",
    "assetName": "Ratcliffe-on-Soar",
    "fuelType": "Coal",
    "affectedCapacity": 500,
    "outageType": "Unplanned",
    "cause": "Boiler tube leak",
}

ALT_SHAPE = {
    "mrid": "def-456",
    "published": "2026-08-19T10:00:00Z",
    "startTime": "2026-08-21T00:00:00Z",
    "asset": "IFA2",
    "fuel": "interconnector",
    "mw": 1000,
    "eventType": "planned outage",
    "reason": "Maintenance",
}


def test_map_documented_shape():
    row = map_message(DOC_SHAPE)
    assert row["message_id"] == "abc-123"
    assert row["published_at"] == "2026-08-19T14:23:00Z"
    assert row["asset_name"] == "Ratcliffe-on-Soar"
    assert row["fuel_type"] == "Coal"
    assert row["affected_mw"] == 500
    assert row["outage_type"] == "unplanned"
    assert row["cause"] == "Boiler tube leak"
    assert row["raw"] is DOC_SHAPE


def test_map_alternative_keys():
    row = map_message(ALT_SHAPE)
    assert row["message_id"] == "def-456"
    assert row["asset_name"] == "IFA2"
    assert row["fuel_type"] == "Interconnector"
    assert row["affected_mw"] == 1000
    assert row["outage_type"] == "planned"
    assert row["event_end"] is None  # missing key tolerated


def test_map_missing_id_returns_none():
    assert map_message({"assetName": "x"}) is None
