"""The app registry is the single source for portal tiles and local ports — keep it consistent."""
from pathlib import Path

from shared.app_registry import APPS, SECTIONS, local_table, online_url
from shared.service_control import SERVICES

REPO = Path(__file__).resolve().parents[2]


def test_keys_unique_and_sections_known():
    keys = [a["key"] for a in APPS]
    assert len(keys) == len(set(keys))
    assert {a["section"] for a in APPS} <= {s for s, _ in SECTIONS}


def test_local_ports_unique_and_entries_exist():
    ports = [a["port"] for a in APPS if a.get("port")] + [8500]
    assert len(ports) == len(set(ports)), "two apps share a local port"
    for a in APPS:
        if a.get("entry"):
            assert (REPO / a["entry"]).exists(), a["entry"]


def test_services_exist_and_every_app_has_an_online_url():
    for a in APPS:
        if a.get("service"):
            assert a["service"] in SERVICES, a["service"]
        assert online_url(a), a["key"]


def test_local_table_feeds_the_launcher():
    lines = local_table().splitlines()
    assert lines[0].startswith("portal|")
    assert all(len(l.split("|")) == 3 for l in lines)
