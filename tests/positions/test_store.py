from datetime import date

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

from libs.positions.conventions import Position
from libs.positions.store import load_books, load_positions, replace_physical
from scripts.market_maker import migrate


@pytest.fixture
def engine():
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def _attach(conn, _):
        conn.execute("ATTACH DATABASE ':memory:' AS marketdata")

    with eng.begin() as c:
        c.execute(text("CREATE TABLE marketdata.rm_assets (id INTEGER PRIMARY KEY, name TEXT, asset_type TEXT,"
                       " province TEXT, capacity_mw REAL, bess_duration_h REAL, asset_code TEXT)"))
        c.execute(text("CREATE TABLE marketdata.rm_books (id INTEGER PRIMARY KEY, name TEXT, book_type TEXT, asset_id INTEGER)"))
        c.execute(text("CREATE TABLE marketdata.rm_positions (id INTEGER PRIMARY KEY AUTOINCREMENT, book_id INTEGER,"
                       " instrument_type TEXT, province TEXT, channel TEXT, direction TEXT, volume_mwh REAL,"
                       " price_cny_mwh REAL, start_date DATE, end_date DATE, counterparty TEXT, status TEXT,"
                       " uploaded_at TEXT, upload_batch_id TEXT, source TEXT, tou_block TEXT, basis TEXT)"))
    return eng


def _bilateral(c, **kw):
    d = dict(book_id=21, instrument_type="bilateral", province="浙江", channel="annual", direction="buy",
             volume_mwh=90.0, price_cny_mwh=400.0, start_date="2026-01-01", end_date="2026-03-31",
             status="open", source=None, tou_block=None)
    d.update(kw)
    c.execute(text("INSERT INTO marketdata.rm_positions (book_id, instrument_type, province, channel, direction,"
                   " volume_mwh, price_cny_mwh, start_date, end_date, status, source, tou_block) VALUES"
                   " (:book_id,:instrument_type,:province,:channel,:direction,:volume_mwh,:price_cny_mwh,"
                   ":start_date,:end_date,:status,:source,:tou_block)"), d)


def test_multi_month_split(engine):
    with engine.begin() as c:
        _bilateral(c)
    df = load_positions(engine)
    assert list(df.delivery_month) == [date(2026, 1, 1), date(2026, 2, 1), date(2026, 3, 1)]
    assert [round(v, 6) for v in df.volume_mwh] == [31.0, 28.0, 31.0]
    assert set(df.price_cny_mwh) == {400.0}
    assert set(df.source) == {"bilateral"} and set(df.tou_block) == {"base"}


def test_status_and_book_filter(engine):
    with engine.begin() as c:
        _bilateral(c)
        _bilateral(c, book_id=22)
        _bilateral(c, status="closed")
    assert set(load_positions(engine, [22]).book_id) == {22}
    assert len(load_positions(engine, [])) == 0


def test_unknown_province_raises(engine):
    with engine.begin() as c:
        _bilateral(c, province="火星")
    with pytest.raises(ValueError, match="火星"):
        load_positions(engine)
    with pytest.raises(ValueError, match="火星"):
        replace_physical(engine, 1, [Position(1, "火星", "buy", 1.0, date(2026, 1, 1))])


def test_replace_physical_idempotent_and_isolated(engine):
    with engine.begin() as c:
        _bilateral(c, book_id=5, start_date="2026-01-01", end_date="2026-01-31")
    rows = [Position(5, "山东", "buy", 10.0, date(2026, m, 1), source="physical", basis="same_month")
            for m in (1, 2)]
    assert replace_physical(engine, 5, rows) == 2
    assert replace_physical(engine, 5, rows) == 2
    with engine.connect() as c:
        n = c.execute(text("SELECT COUNT(*) FROM marketdata.rm_positions WHERE book_id=5")).scalar()
        phys = c.execute(text("SELECT start_date, end_date, instrument_type, channel, status FROM "
                              "marketdata.rm_positions WHERE source='physical' ORDER BY start_date")).all()
        bil = c.execute(text("SELECT COUNT(*) FROM marketdata.rm_positions WHERE source IS NULL")).scalar()
    assert n == 3 and bil == 1
    assert phys[1][:2] == ("2026-02-01", "2026-02-28")
    assert phys[0][2:] == ("profile", "annual", "open")
    df = load_positions(engine, [5])
    assert sorted(df.source) == ["bilateral", "physical", "physical"]


def test_load_books(engine):
    with engine.begin() as c:
        c.execute(text("INSERT INTO marketdata.rm_assets VALUES (1,'A','bess','inner_mongolia_mengxi',100,2,'WH')"))
        c.execute(text("INSERT INTO marketdata.rm_books VALUES (1,'asset book','asset',1),(21,'load','load',NULL)"))
    df = load_books(engine)
    assert list(df.columns) == ["book_id", "book_name", "book_type", "asset_id", "asset_type", "asset_code",
                                "province", "capacity_mw", "bess_duration_h"]
    assert df.set_index("book_id").province[1] == "蒙西"
    assert df.set_index("book_id").province.isna()[21]


def test_migration_statements():
    st = migrate.build_statements("weird_name")
    assert len(st) == 8
    assert any("DROP CONSTRAINT IF EXISTS weird_name" in s for s in st)
    assert not any("rm_forward_curves_source_check\n" in s and "DROP" in s for s in st)
    assert "delivery_hour IS NULL" in migrate.DUP_CHECK_SQL and "tou_block" not in migrate.DUP_CHECK_SQL
    assert "pg_constraint" in migrate.CONSTRAINT_LOOKUP_SQL


def test_dry_run_and_abort_paths(monkeypatch):
    class Conn:
        def __init__(s, dups): s.dups = dups
        def __enter__(s): return s
        def __exit__(s, *a): pass
        def execute(s, q, *a):
            sql = str(q)
            class R:
                def scalar(_): return s.dups
                def __iter__(_): return iter([("old_chk",)])
            return R()
    class Eng:
        def __init__(s, dups): s.dups = dups
        def connect(s): return Conn(s.dups)
        def begin(s): raise AssertionError("dry-run must not open a write transaction")
    lines = []
    assert migrate.run(Eng(0), apply=False, out=lines.append) == 0
    text_out = "\n".join(lines)
    assert "DROP CONSTRAINT IF EXISTS old_chk" in text_out and "dry-run" in text_out
    lines.clear()
    assert migrate.run(Eng(3), apply=True, out=lines.append) == 1
    assert "3 duplicate" in "\n".join(lines)
