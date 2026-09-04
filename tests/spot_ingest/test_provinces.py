from services.spot_ingest.provinces import PROVINCES_MAP


def test_map_has_core_provinces():
    assert PROVINCES_MAP["山东"] == "Shandong"
    assert PROVINCES_MAP["河北南网"] == "Hebei-South"
    assert PROVINCES_MAP["蒙西"] == "Mengxi"
    assert PROVINCES_MAP["蒙东"] == "Mengdong"


def test_map_size_unchanged():
    # 35 entries — same as the copy previously in spot_ingest_bridge.py
    assert len(PROVINCES_MAP) == 35


def test_bridge_imports_shared_map():
    from services.hermes import spot_ingest_bridge
    assert spot_ingest_bridge.PROVINCES_MAP is PROVINCES_MAP
