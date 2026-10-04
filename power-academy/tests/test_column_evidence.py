import pandas as pd
import pytest
import sqlalchemy as sa

from academy.column.evidence import (MAX_ROWS, build_pack, check_readonly,
                                     license_for, sha256_file)
from academy.column.schema import new_article
from academy.io import dump_yaml, load_yaml


def test_readonly_guard():
    check_readonly("select 1")
    check_readonly("  WITH x AS (SELECT 1) SELECT * FROM x")
    for bad in ["delete from t", "select 1; drop table t",
                "insert into t values (1)", "UPDATE t set a=1",
                "select * from t where x = 'x'; truncate t"]:
        with pytest.raises(ValueError):
            check_readonly(bad)


def test_license_defaults_restricted():
    lic = {"marketdata.spot_prices_hourly": "licensed_restricted",
           "staging.spot_knowledge_docs": "public"}
    assert license_for("marketdata.spot_prices_hourly", lic) == "licensed_restricted"
    assert license_for("staging.spot_knowledge_docs", lic) == "public"
    assert license_for("marketdata.unknown_table", lic) == "licensed_restricted"


def _sqlite():
    e = sa.create_engine("sqlite://")
    with e.begin() as c:
        c.execute(sa.text("create table p (province text, dt text, rt real)"))
        c.execute(sa.text("insert into p values ('山东','2026-10-01 10:00',0.42)"))
    return e


def test_build_pack_writes_snapshot_and_manifest(tmp_path):
    d = new_article(tmp_path, 1, "demo")
    dump_yaml({"queries": [{
        "id": "e_sd_price", "kind": "sql", "source": "marketdata.spot_prices_hourly",
        "sql": "select * from p where province = :prov", "params": {"prov": "山东"}}]},
        d / "evidence" / "queries.yaml")
    entries = build_pack(d, _sqlite(), {"marketdata.spot_prices_hourly": "licensed_restricted"},
                         today="2026-10-04")
    e = entries[0]
    assert e["id"] == "e_sd_price" and e["kind"] == "data"
    assert e["license"] == "licensed_restricted" and e["rows"] == 1
    assert e["retrieved_at"] == "2026-10-04" and len(e["sha256"]) == 64
    csv = (d / "evidence" / "data" / "e_sd_price.csv").read_text(encoding="utf-8")
    assert "山东" in csv and "0.42" in csv
    on_disk = load_yaml(d / "evidence" / "manifest.yaml")["entries"]
    assert on_disk[0]["id"] == "e_sd_price"
    assert sha256_file(d / "evidence" / "data" / "e_sd_price.csv") == e["sha256"]


def test_build_pack_caps_rows(tmp_path, monkeypatch):
    d = new_article(tmp_path, 2, "demo2")
    big = pd.DataFrame({"a": range(MAX_ROWS + 10)})
    import academy.column.evidence as ev
    monkeypatch.setattr(ev, "_run_sql", lambda *a, **k: big)
    dump_yaml({"queries": [{"id": "e_big", "kind": "sql", "source": "t", "sql": "select 1"}]},
              d / "evidence" / "queries.yaml")
    entries = build_pack(d, None, {}, today="2026-10-04")
    assert entries[0]["rows"] == MAX_ROWS and entries[0]["truncated"] is True
