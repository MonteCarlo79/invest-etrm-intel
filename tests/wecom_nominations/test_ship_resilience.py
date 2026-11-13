"""WeCom shipping must survive a flaky S3 upload (2026-11-13: nominations and
actuals landed once between 11-04 and 11-12 because one failed `aws s3 cp`
aborted the run after zones had shipped)."""
import subprocess
from types import SimpleNamespace

import pytest

from services.wecom_nominations import run_daily


def test_s3_put_retries_then_succeeds(monkeypatch):
    calls = []

    def fake_run(cmd, **kw):
        calls.append(cmd)
        return SimpleNamespace(returncode=1 if len(calls) < 3 else 0,
                               stderr="Connection reset", stdout="")
    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr("time.sleep", lambda s: None)
    run_daily._s3_put("/tmp/x.json", "zones/x.json")
    assert len(calls) == 3


def test_s3_put_gives_up_after_attempts(monkeypatch):
    monkeypatch.setattr(subprocess, "run",
                        lambda cmd, **kw: SimpleNamespace(returncode=1, stderr="boom", stdout=""))
    monkeypatch.setattr("time.sleep", lambda s: None)
    with pytest.raises(RuntimeError, match="failed after 3 attempts"):
        run_daily._s3_put("/tmp/x.json", "zones/x.json")


def test_one_failed_ship_does_not_skip_the_others(monkeypatch):
    shipped = []

    def ok(name):
        return lambda rows, tag: shipped.append(name)

    def boom(rows, tag):
        raise RuntimeError("s3 down")

    monkeypatch.setattr(run_daily, "collect", lambda: {"scanned": 1})
    monkeypatch.setattr(run_daily.collector, "mirror_to", lambda root: {})
    monkeypatch.setattr(run_daily, "FOLDERS", {})
    monkeypatch.setattr(run_daily, "ship_zones", boom)
    monkeypatch.setattr(run_daily, "ship_nominations", ok("nominations"))
    monkeypatch.setattr(run_daily, "ship_actuals", ok("actuals"))
    monkeypatch.setattr(run_daily, "ship_status", ok("status"))
    with pytest.raises(RuntimeError, match="zones"):
        run_daily.main()
    assert shipped == ["nominations", "actuals", "status"]
