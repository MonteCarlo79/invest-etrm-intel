"""run_task_jobs.py: ensure + seed (local dir and fake S3) against sqlite, dry-run vs --apply."""
import importlib.util
import pathlib

import sqlalchemy
from openpyxl import Workbook
from sqlalchemy import event, text

from test_ic_case import _format_a_sheet
from test_review_parse import BD, OV, _deck

_P = pathlib.Path(__file__).resolve().parents[2] / "scripts" / "asset_risk" / "run_task_jobs.py"
_spec = importlib.util.spec_from_file_location("run_task_jobs", _P)
rtj = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rtj)


def _eng(tmp_path):
    md = tmp_path / "md.db"
    eng = sqlalchemy.create_engine(f"sqlite:///{tmp_path / 'main.db'}")

    @event.listens_for(eng, "connect")
    def _a(conn, _):
        conn.execute(f"ATTACH DATABASE '{md}' AS marketdata")
    return eng


def _tables(eng):
    with eng.connect() as c:
        return {r[0] for r in c.execute(text("SELECT name FROM marketdata.sqlite_master WHERE type='table'"))}


def _count(eng, t):
    with eng.connect() as c:
        return c.execute(text(f"SELECT COUNT(*) FROM marketdata.{t}")).scalar()


def _seed_dir(tmp_path):
    d = tmp_path / "in"
    d.mkdir()
    wb = Workbook(); wb.remove(wb.active)
    _format_a_sheet(wb.create_sheet("武川-IPMT 边界"), 234, (177.73, 275.77), (61.72, 246.24), (61.72, 246.24))
    wb.save(d / "ic.xlsx")
    _deck(d, [OV, BD], name="蒙西储能场站9月内部复盘.pptx")
    (d / "bad.pptx").write_bytes(b"not a zip")
    (d / "readme.txt").write_text("x")
    return d


def test_ensure_dry_run_then_apply(tmp_path):
    eng = _eng(tmp_path)
    out = []
    assert rtj.cmd_ensure(eng, apply=False, out=out.append) == 0
    assert not {"rm_ic_assumptions", "rm_review_metrics"} & _tables(eng)
    assert rtj.main(["ensure", "--apply"], engine=eng) == 0
    assert {"rm_ic_assumptions", "rm_review_metrics"} <= _tables(eng)


def test_seed_local_dir(tmp_path, capsys):
    eng = _eng(tmp_path)
    d = _seed_dir(tmp_path)
    assert rtj.main(["seed", "--local-dir", str(d)], engine=eng) == 1   # bad.pptx -> nonzero
    out = capsys.readouterr().out
    assert "dry-run" in out and "bad.pptx: PARSE ERROR" in out and "readme.txt: skipped" in out
    assert "ic.xlsx: rows=20" in out and "['wuchuan']" in out and "months=['2026-09']" in out
    assert not {"rm_ic_assumptions", "rm_review_metrics"} & _tables(eng)

    rtj.main(["seed", "--local-dir", str(d), "--apply"], engine=eng)
    assert _count(eng, "rm_ic_assumptions") == 20
    n = _count(eng, "rm_review_metrics")
    assert n > 0
    rtj.main(["seed", "--local-dir", str(d), "--apply"], engine=eng)     # idempotent (hash dedup)
    assert "already stored" in capsys.readouterr().out
    assert _count(eng, "rm_ic_assumptions") == 20 and _count(eng, "rm_review_metrics") == n


class _FakeS3:
    def __init__(self, src):
        self.src = src

    def get_paginator(self, _):
        src = self.src

        class _P:
            def paginate(self, Bucket, Prefix):
                assert (Bucket, Prefix) == ("bkt", "ic/")
                yield {"Contents": [{"Key": "ic/"}] + [{"Key": f"ic/{p.name}"} for p in sorted(src.iterdir())]}
        return _P()

    def download_file(self, bucket, key, dest):
        pathlib.Path(dest).write_bytes((self.src / pathlib.Path(key).name).read_bytes())


def test_seed_s3_injectable_client(tmp_path, capsys):
    eng = _eng(tmp_path)
    d = _seed_dir(tmp_path)
    (d / "bad.pptx").unlink()
    assert rtj.main(["seed", "--s3-prefix", "s3://bkt/ic/", "--apply"], engine=eng,
                    s3_client=_FakeS3(d)) == 0
    out = capsys.readouterr().out
    assert "-- ic.xlsx: rows=20" in out and "蒙西储能场站9月内部复盘.pptx: rows=" in out
    assert _count(eng, "rm_ic_assumptions") == 20 and _count(eng, "rm_review_metrics") > 0
    with eng.connect() as c:
        assert c.execute(text("SELECT DISTINCT source_file FROM marketdata.rm_review_metrics")).scalar() \
            == "蒙西储能场站9月内部复盘.pptx"
