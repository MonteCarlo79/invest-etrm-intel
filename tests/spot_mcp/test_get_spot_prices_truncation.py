"""get_spot_prices must flag when the 5000-row cap drops rows (oldest-first order → latest days lost)."""
import sys
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services.spot_mcp import tools  # noqa: E402


class _Cur:
    def __init__(self, n):
        self.n = n
        self.description = [(c,) for c in ("report_date", "province_en", "province_cn",
                                           "da_avg", "da_max", "da_min", "rt_avg", "rt_max", "rt_min")]

    def execute(self, *a, **k): pass
    def fetchall(self): return [("2026-01-01", "p", "省", 1, 1, 1, 1, 1, 1)] * self.n
    def __enter__(self): return self
    def __exit__(self, *a): return False


class _Conn:
    def __init__(self, n): self.n = n
    def cursor(self, *a, **k): return _Cur(self.n)
    def __enter__(self): return self
    def __exit__(self, *a): return False


def _run(monkeypatch, n):
    import services.knowledge_pool.db as db
    monkeypatch.setattr(db, "get_conn", lambda: _Conn(n))
    return tools.get_spot_prices("2026-01-01", "2026-12-31")


def test_flags_truncation(monkeypatch):
    out = _run(monkeypatch, 5001)
    assert out["truncated"] is True and out["count"] == 5000 and "MISSING" in out["note"]


def test_no_flag_under_cap(monkeypatch):
    out = _run(monkeypatch, 10)
    assert out["truncated"] is False and out["count"] == 10 and "note" not in out
