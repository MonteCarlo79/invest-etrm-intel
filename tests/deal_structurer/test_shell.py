# tests/deal_structurer/test_shell.py
import importlib, pathlib

def test_removed_tabs_gone():
    base = pathlib.Path("apps/deal_structurer")
    for gone in ("price_tab.py", "dispatch_tab.py", "mc_tab.py", "screening_tab.py"):
        assert not (base / gone).exists()

def test_all_nav_targets_import():
    for mod in ("intake_tab", "option_tab", "cashflow_tab", "deal_tab", "screen_tab", "committee_tab", "strategist", "session_cache"):
        importlib.import_module(f"apps.deal_structurer.{mod}")

def test_session_cache_keys_match_nav():
    from apps.deal_structurer import session_cache
    assert "mc_result" not in session_cache._KEYS and "price_paths" not in session_cache._KEYS
    assert "ov_result" in session_cache._KEYS

def test_no_dead_session_refs():
    import re
    src = pathlib.Path("apps/deal_structurer/deal_tab.py").read_text()
    assert "mc_result" not in src                          # rewired to ov_result
