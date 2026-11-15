"""M1: retail-risk procurement coverage counts forward buys only, never market-maker physical rows."""
import importlib.util
from pathlib import Path

from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

# loaded by path: tests/services/ shadows the `services` namespace package under pytest
_PATH = Path(__file__).resolve().parents[2] / "services" / "retail_risk" / "agent_tools.py"
_spec = importlib.util.spec_from_file_location("_retail_agent_tools", _PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
execute_tool = _mod.execute_tool


def test_coverage_excludes_physical_rows():
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _attach(conn, _):
        conn.execute("ATTACH DATABASE ':memory:' AS marketdata")

    with eng.begin() as c:
        c.execute(text("CREATE TABLE marketdata.rm_books (id INTEGER PRIMARY KEY, asset_id INTEGER)"))
        c.execute(text("CREATE TABLE marketdata.rm_customer_contracts (id INTEGER PRIMARY KEY,"
                       " customer_id INTEGER, bound_asset_id INTEGER, contract_status TEXT)"))
        c.execute(text("CREATE TABLE marketdata.rm_customer_profiles (customer_id INTEGER, nominated_mwh REAL)"))
        c.execute(text("CREATE TABLE marketdata.rm_positions (book_id INTEGER, direction TEXT, status TEXT,"
                       " volume_mwh REAL, source TEXT)"))
        c.execute(text("INSERT INTO marketdata.rm_books VALUES (6, 1)"))
        c.execute(text("INSERT INTO marketdata.rm_customer_contracts VALUES (1, 1, 1, 'active')"))
        c.execute(text("INSERT INTO marketdata.rm_customer_profiles VALUES (1, 1000)"))
        c.execute(text("INSERT INTO marketdata.rm_positions VALUES (6,'buy','open',200,NULL),"
                       "(6,'buy','open',100,'bilateral'),(6,'buy','open',5000,'physical')"))
    r = execute_tool("get_procurement_coverage", {"book_id": 6}, eng)
    assert r["forward_bought_mwh"] == 300.0 and r["coverage_ratio"] == 0.3
