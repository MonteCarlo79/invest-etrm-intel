# tests/province_fundamentals/test_backfill.py
"""回填 runner tests — services/province_fundamentals/backfill.py.

规则 (task-5 brief, binding):
  - --dry-run 只打印每文件判定 (格式/行数/跳过原因), 绝不触碰 write_rows;
  - 未知格式文件计入 unknown, 不崩;
  - 写库经 write_rows(..., default_status="confirmed");
  - jiaojie 全国册 (路径无省份名) 也须经 registry 派发识别.

Fixtures 只用 tests/province_fundamentals/fixtures/ 复制件 (OneDrive dataless
live data/ 路径会挂死测试 — 既定裁定).
"""
import shutil
from pathlib import Path
from unittest.mock import MagicMock

import openpyxl
import pytest

from services.province_fundamentals import backfill

FIXTURES = Path(__file__).parent / "fixtures"
FIXTURE_A = FIXTURES / "安徽电力市场信息披露月报数据-截至2026年5月.xlsx"
FIXTURE_JIAOJIE = FIXTURES / "2025年1-12月各省分类型利用小时.xlsx"


def _copy(src: Path, dst_dir: Path) -> Path:
    """复制 fixture 进 tmp 目录 (保文件名 — registry 从文件名嗅探省份)."""
    dst = dst_dir / src.name
    shutil.copy(src, dst)
    return dst


def _junk_xlsx(path: Path) -> Path:
    wb = openpyxl.Workbook()
    wb.active.title = "sheet1"
    wb.active.append(["x"])
    wb.save(path)
    return path


# ── dry-run 闸门 (brief Step 1, binding) ──────────────────────────────────────

def test_dry_run_never_calls_write_rows(tmp_path, monkeypatch):
    """dry-run: 文件解析照常, write_rows 绝不被调用."""
    _copy(FIXTURE_A, tmp_path)
    mock_write = MagicMock(side_effect=AssertionError("write_rows called in dry-run"))
    monkeypatch.setattr(backfill, "write_rows", mock_write)
    summary = backfill.run(tmp_path, dry_run=True, out=lambda *a, **k: None)
    mock_write.assert_not_called()
    assert summary["files"] == 1
    assert summary["parsed"] == 1
    assert summary["unknown"] == 0
    assert summary["rows"] > 0
    assert summary["formats"] == {"format_a": 1}


def test_unknown_files_counted_not_crash(tmp_path):
    """未知格式 (无省份名的乱工作簿 + 非 xlsx 文件) → unknown, 不抛."""
    _junk_xlsx(tmp_path / "乱七八糟.xlsx")
    (tmp_path / "说明.txt").write_text("not a workbook", encoding="utf-8")
    summary = backfill.run(tmp_path, dry_run=True, out=lambda *a, **k: None)
    assert summary["files"] == 2
    assert summary["unknown"] == 2
    assert summary["parsed"] == 0


def test_known_province_but_unknown_format_counts_unknown(tmp_path):
    """省份命中 (安徽在文件名) 但结构不命中任何 parser → unknown, 不崩."""
    _junk_xlsx(tmp_path / "安徽-未知版式.xlsx")
    summary = backfill.run(tmp_path, dry_run=True, out=lambda *a, **k: None)
    assert summary["unknown"] == 1
    assert summary["parsed"] == 0


def test_empty_dir_no_crash(tmp_path):
    summary = backfill.run(tmp_path, dry_run=True, out=lambda *a, **k: None)
    assert summary["files"] == 0
    assert summary["rows"] == 0


# ── jiaojie 全国册派发 (brief: runner 须识别交接表) ─────────────────────────────

def test_jiaojie_workbook_dispatched_via_registry(tmp_path):
    """交接表路径无省份名 — 仍须派发到 jiaojie_2025 并出多省行."""
    _copy(FIXTURE_JIAOJIE, tmp_path)
    summary = backfill.run(tmp_path, dry_run=True, out=lambda *a, **k: None)
    assert summary["parsed"] == 1
    assert summary["formats"] == {"jiaojie_2025": 1}
    assert summary["rows"] > 100          # 31 省 × 多指标, 量级守护


def test_province_filter_keeps_only_that_province(tmp_path, monkeypatch):
    """--province 安徽: 交接表只写安徽行, 其余省计入 filtered."""
    _copy(FIXTURE_JIAOJIE, tmp_path)
    captured = {}

    def fake_write(rows, *, pg_url, default_status):
        captured["rows"] = list(rows)
        captured["pg_url"] = pg_url
        captured["default_status"] = default_status
        return {"written": len(rows), "conflicts": 0, "skipped": 0}

    monkeypatch.setattr(backfill, "write_rows", fake_write)
    summary = backfill.run(tmp_path, province="安徽", dry_run=False,
                           pg_url="postgresql://x", out=lambda *a, **k: None)
    assert captured["rows"], "安徽 rows must reach the writer"
    assert {r.province for r in captured["rows"]} == {"安徽"}
    assert summary["filtered"] > 0
    assert summary["rows"] == len(captured["rows"])


# ── 写库路径 ──────────────────────────────────────────────────────────────────

def test_write_mode_calls_write_rows_confirmed(tmp_path, monkeypatch):
    """非 dry-run: write_rows 收 rows + pg_url, default_status='confirmed' (spec 定案)."""
    _copy(FIXTURE_A, tmp_path)
    mock_write = MagicMock(return_value={"written": 10, "conflicts": 1, "skipped": 2})
    monkeypatch.setattr(backfill, "write_rows", mock_write)
    summary = backfill.run(tmp_path, dry_run=False, pg_url="postgresql://x",
                           out=lambda *a, **k: None)
    mock_write.assert_called_once()
    _, kwargs = mock_write.call_args
    assert kwargs["pg_url"] == "postgresql://x"
    assert kwargs["default_status"] == "confirmed"
    rows_arg = mock_write.call_args.args[0]
    assert rows_arg and all(r.province == "安徽" for r in rows_arg)
    assert summary["written"] == 10
    assert summary["conflicts"] == 1
    assert summary["skipped"] == 2


def test_mixed_dir_summary(tmp_path, monkeypatch):
    """混合目录: format A 解析 + 乱工作簿 unknown + txt unknown."""
    _copy(FIXTURE_A, tmp_path)
    _junk_xlsx(tmp_path / "nobody.xlsx")
    (tmp_path / "notes.md").write_text("# hi", encoding="utf-8")
    monkeypatch.setattr(backfill, "write_rows",
                        MagicMock(return_value={"written": 1, "conflicts": 0, "skipped": 0}))
    summary = backfill.run(tmp_path, dry_run=False, pg_url="postgresql://x",
                           out=lambda *a, **k: None)
    assert summary["files"] == 3
    assert summary["parsed"] == 1
    assert summary["unknown"] == 2


# ── CLI ───────────────────────────────────────────────────────────────────────

def test_main_dry_run_cli(tmp_path, capsys):
    _copy(FIXTURE_A, tmp_path)
    rc = backfill.main(["--dir", str(tmp_path), "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "format_a" in out
    assert FIXTURE_A.name in out


def test_main_requires_pg_url_when_writing(tmp_path, monkeypatch):
    """非 dry-run 且无 PGURL/--pg-url → argparse 报错退出 (不静默 dry)."""
    monkeypatch.delenv("PGURL", raising=False)
    with pytest.raises(SystemExit):
        backfill.main(["--dir", str(tmp_path)])


def test_main_pg_url_from_env(tmp_path, monkeypatch):
    """PGURL 环境变量供给写库连接串."""
    _copy(FIXTURE_A, tmp_path)
    mock_write = MagicMock(return_value={"written": 0, "conflicts": 0, "skipped": 0})
    monkeypatch.setattr(backfill, "write_rows", mock_write)
    monkeypatch.setenv("PGURL", "postgresql://env-dsn")
    rc = backfill.main(["--dir", str(tmp_path)])
    assert rc == 0
    assert mock_write.call_args.kwargs["pg_url"] == "postgresql://env-dsn"
