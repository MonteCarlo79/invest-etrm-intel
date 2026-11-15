import subprocess, sys


def test_agent_tools_and_headless_import_without_streamlit_or_apps():
    code = (
        "import sys; import services.retail_risk.agent_tools; import services.retail_risk.headless_agent; "
        "bad = [m for m in sys.modules if m == 'streamlit' or m.startswith('streamlit.') or m == 'apps' or m.startswith('apps.')]; "
        "print(bad); sys.exit(1 if bad else 0)"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_headless_run_does_not_import_apps(monkeypatch):
    import inspect, services.retail_risk.headless_agent as h
    assert "apps.retail_risk" not in inspect.getsource(h)
