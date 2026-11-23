# tests/nodal_trading/test_registry_extract.py
from datetime import date
from services.nodal_forecast import registry_extract as rx

def _rows(plant, series):
    return [dict(plant_name=plant, data_date=date(2026, 1, 1), datetime=f"2026-01-01 {h:02d}:00",
                 cleared_energy_mwh=v) for h, v in series]

def test_capacity_estimated_from_max_abs_interval():
    rows = _rows("谷山梁", [(0, -25.0), (1, -25.0), (2, 25.0)])
    plants = rx.extract_bess_plants(rows)
    # the column stores interval average MW (unit contract 2026-11-05): no ×4
    assert plants[0]["capacity_mw_est"] == 25.0

def test_duration_estimated_from_sign_persistence():
    rows = (_rows("谷山梁", [(h, -25.0) for h in range(8)]) +
            _rows("谷山梁", [(8 + h, 25.0) for h in range(4)]))
    plants = rx.extract_bess_plants(rows)
    assert plants[0]["duration_h_est"] == 2.0

def test_write_registry_md_contains_review_marker(tmp_path):
    plants = [dict(plant_name="谷山梁", capacity_mw_est=100.0, duration_h_est=2.0,
                   zone_guess="乌兰察布", first_seen=date(2026,1,1), last_seen=date(2026,9,1))]
    p = tmp_path / "draft.md"
    rx.write_registry_md(plants, p)
    txt = p.read_text()
    assert "REVIEW REQUIRED" in txt and "谷山梁" in txt and "100.0" in txt


class TestUpdateAssetRegistry:
    """User requirement 2026-09-22: registry is living data — upsert new/changed,
    soft-retire (active=FALSE) plants absent from the refresh list. Never hard-delete."""

    def _conn(self):
        class Cur:
            def __init__(self): self.calls = []; self._fetch = []
            def execute(self, sql, params=None): self.calls.append((sql, params))
            def executemany(self, sql, seq): self.calls.append((sql, list(seq)))
            def fetchall(self): return self._fetch
        class Conn:
            def __init__(self): self.cur = Cur()
            def cursor(self): return self.cur
            def commit(self): pass
        return Conn()

    def test_upserts_new_and_updates_changed(self):
        conn = self._conn()
        plants = [
            dict(plant_name="谷山梁", capacity_mw_est=400.0, duration_h_est=4.0,
                 zone_guess="乌兰察布", first_seen=date(2026,1,1), last_seen=date(2026,9,20)),
            dict(plant_name="新场站", capacity_mw_est=200.0, duration_h_est=2.0,
                 zone_guess=None, first_seen=date(2026,6,1), last_seen=date(2026,9,20)),
        ]
        out = rx.update_asset_registry(conn, plants)
        assert out["upserted"] == 2 and out["retired"] == 0
        sql = conn.cur.calls[0][0]
        assert "ON CONFLICT (plant_name) DO UPDATE" in sql

    def test_retires_absent_plants_softly(self):
        conn = self._conn()
        conn.cur._fetch = [("老场站",)]
        out = rx.update_asset_registry(conn, [])
        assert out["retired"] == 1 and out["upserted"] == 0
        sql, params = conn.cur.calls[1]   # calls[0] is the SELECT of existing plants
        assert "ACTIVE = FALSE" in sql.upper() and params == ("老场站",)

    def test_never_hard_deletes(self):
        conn = self._conn()
        rx.update_asset_registry(conn, [])
        for sql, _ in conn.cur.calls:
            assert "DELETE" not in sql.upper()


# ---- 2026-11-23: unit fix + owned ratings -----------------------------------

def test_capacity_estimate_is_mw_not_times_four():
    """cleared_energy_mwh stores interval average MW — the estimate is the
    observed peak itself (the old ×4 seeded 100 MW stations as 400)."""
    from datetime import date as _d
    from services.nodal_forecast.registry_extract import extract_bess_plants
    rows = [{"plant_name": "某某储能电站", "data_date": _d(2026, 11, 1),
             "datetime": f"2026-11-01 0{i}:00:00", "cleared_energy_mwh": v}
            for i, v in enumerate([-100.0, -100.0, 60.0])]
    assert extract_bess_plants(rows)[0]["capacity_mw_est"] == 100.0


def test_owned_ratings_override_estimates_and_split_accounts():
    from services.nodal_forecast.registry_extract import (
        apply_owned_ratings, owned_ratings)
    r = owned_ratings()
    assert r["悦杭独贵储能电站"] == (100.0, 4.0)
    assert r["景怡查干哈达储能电站"] == (1000.0, 4.0)
    assert r["旭纯哈乐储能电站"] == r["旭纯哈乐储能电站二"] == (500.0, 4.0)  # 1 GW site
    assert r["景敖乌珠日储能电站"] == (1000.0, 4.0)
    plants = apply_owned_ratings([
        {"plant_name": "景怡查干哈达储能电站", "capacity_mw_est": 902.0, "duration_h_est": 7.5},
        {"plant_name": "别家储能电站", "capacity_mw_est": 400.0, "duration_h_est": 3.0}])
    assert (plants[0]["capacity_mw_est"], plants[0]["duration_h_est"]) == (1000.0, 4.0)
    assert plants[1]["capacity_mw_est"] == 400.0      # not ours: estimate kept
