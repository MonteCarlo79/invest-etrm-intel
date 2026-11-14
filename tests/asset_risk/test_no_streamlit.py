import importlib, subprocess, sys

MODULES = ["libs.asset_risk.chain", "libs.asset_risk.diagnostics", "libs.asset_risk.waterfall",
           "libs.asset_risk.settlement.analytics", "libs.asset_risk.settlement.ledger",
           "libs.asset_risk.agent_tools", "libs.asset_risk.standard_params"]

def test_libs_asset_risk_imports_without_streamlit():
    code = "import sys; " + "; ".join(f"import {m}" for m in MODULES) + "; sys.exit(1 if 'streamlit' in sys.modules else 0)"
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
