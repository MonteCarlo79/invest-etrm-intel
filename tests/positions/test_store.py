from datetime import date

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool

from libs.positions.conventions import Position
from libs.positions.store import load_book_province, load_books, load_positions, replace_physical
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


def test_split_residual_sums_exactly(engine):
    with engine.begin() as c:
        _bilateral(c, volume_mwh=100.0)
    df = load_positions(engine)
    assert len(df) == 3 and df.volume_mwh.sum() == 100.0


def test_status_and_book_filter(engine):
    with engine.begin() as c:
        _bilateral(c)
        _bilateral(c, book_id=22)
        _bilateral(c, status="closed")
    assert set(load_positions(engine, [22]).book_id) == {22}
    assert len(load_positions(engine, [])) == 0


def test_unknown_province_skipped_on_read_raises_on_write(engine, caplog):
    with engine.begin() as c:
        _bilateral(c, province="火星")
        _bilateral(c, book_id=22)
    with caplog.at_level("WARNING"):
        df = load_positions(engine)
    assert set(df.book_id) == {22}  # I6: unmapped row skipped, not raised
    assert any("火星" in m for m in df.attrs["skipped"]) and "火星" in caplog.text
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


def test_load_books_skips_unmapped_asset_province(engine, caplog):
    with engine.begin() as c:
        c.execute(text("INSERT INTO marketdata.rm_assets VALUES (1,'A','bess','蒙西',100,2,'WH'),"
                       "(2,'B','wind','hubei',50,NULL,'HB')"))
        c.execute(text("INSERT INTO marketdata.rm_books VALUES (1,'a','asset',1),(2,'b','asset',2),"
                       "(21,'load','load',NULL)"))
    with caplog.at_level("WARNING"):
        df = load_books(engine)
    assert df.book_id.tolist() == [1, 21]
    assert df.attrs["skipped"] and "hubei" in df.attrs["skipped"][0] and "hubei" in caplog.text


def test_load_book_province_ignores_unmapped_rows(engine):
    with engine.begin() as c:
        _bilateral(c, province="火星")
        _bilateral(c, province="火星")
        _bilateral(c, province="河北南网")
        _bilateral(c, book_id=30, province="火星")
    assert load_book_province(engine, 21) == "冀南"
    assert load_book_province(engine, 30) is None


def test_migration_statements():
    st = migrate.build_statements("weird_name")
    assert len(st) == 8
    upd = next(i for i, s in enumerate(st) if s.startswith("UPDATE marketdata.rm_positions SET source"))
    assert st[upd + 1] == "ALTER TABLE marketdata.rm_positions ALTER COLUMN source SET DEFAULT 'bilateral'"
    assert any("DROP CONSTRAINT IF EXISTS weird_name" in s for s in st)
    assert not any("rm_forward_curves_source_check\n" in s and "DROP" in s for s in st)
    assert not any("UNIQUE INDEX" in s for s in st)
    assert "pg_constraint" in migrate.CONSTRAINT_LOOKUP_SQL


def test_dry_run_and_multi_constraint_abort():
    class Conn:
        def __init__(s, names): s.names = names
        def __enter__(s): return s
        def __exit__(s, *a): pass
        def execute(s, q, *a): return iter([(n,) for n in s.names])
    class Eng:
        def __init__(s, names): s.names = names
        def connect(s): return Conn(s.names)
        def begin(s): raise AssertionError("dry-run must not write")
    lines = []
    assert migrate.run(Eng(["old_chk"]), apply=False, out=lines.append) == 0
    t = "\n".join(lines)
    assert "DROP CONSTRAINT IF EXISTS old_chk" in t and "dry-run" in t and "DUP" not in t.upper()
    assert migrate.run(Eng(["a", "b"]), apply=True, out=lines.append) == 1


class _RecConn:
    def __init__(s, dialect, log): s.dialect, s.log = type("D", (), {"name": dialect})(), log
    def __enter__(s): return s
    def __exit__(s, *a): pass
    def execute(s, q, *a):
        s.log.append(str(q))
        return iter([])


class _RecEng:
    def __init__(s, dialect): s.dialect, s.log = dialect, []
    def connect(s): return _RecConn(s.dialect, [])
    def begin(s): return _RecConn(s.dialect, s.log)


def test_apply_sets_lock_timeout_first_on_postgres():
    eng = _RecEng("postgresql")
    assert migrate.run(eng, apply=True, out=lambda *_: None) == 0
    assert eng.log[0] == "SET LOCAL lock_timeout = '5s'"
    assert eng.log[1:] == migrate.build_statements(None)
    lite = _RecEng("sqlite")
    migrate.run(lite, apply=True, out=lambda *_: None)
    assert not any("lock_timeout" in q for q in lite.log)


def test_migration_sql_resolves_in_container_layout(tmp_path):
    """Dockerfile copies scripts/market_maker/ and the .sql to the same paths under /app."""
    import shutil
    import subprocess
    import sys
    from pathlib import Path
    root = Path(migrate.__file__).resolve().parents[2]
    (tmp_path / "scripts" / "market_maker").mkdir(parents=True)
    (tmp_path / "db" / "ddl" / "marketdata").mkdir(parents=True)
    shutil.copy(root / "scripts" / "market_maker" / "migrate.py", tmp_path / "scripts" / "market_maker")
    shutil.copy(migrate.SQL_PATH, tmp_path / "db" / "ddl" / "marketdata")
    code = ("import runpy; m = runpy.run_path('scripts/market_maker/migrate.py');"
            "print(len(m['build_statements']()))")
    r = subprocess.run([sys.executable, "-c", code], cwd=tmp_path, capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == "8"
    docker = (root / "apps" / "market_maker" / "Dockerfile").read_text()
    assert "COPY scripts/market_maker/ ./scripts/market_maker/" in docker
    assert ("COPY db/ddl/marketdata/market_maker_migration.sql "
            "./db/ddl/marketdata/market_maker_migration.sql") in docker
    ignore = (root / ".dockerignore").read_text().splitlines()
    assert "!scripts/market_maker/" in ignore
    assert "!db/ddl/marketdata/market_maker_migration.sql" in ignore
