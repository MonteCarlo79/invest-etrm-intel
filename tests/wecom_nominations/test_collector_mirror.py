"""Local working root + write-only OneDrive mirror (2026-11-09 launchd fix)."""
import importlib

from services.wecom_nominations import collector


def _w(p, text="x"):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text)


def test_mirror_copies_new_files_only(tmp_path):
    src, dst = tmp_path / "local", tmp_path / "onedrive"
    _w(src / "苏右" / "a.xlsx", "new")
    _w(src / "苏右" / "client-reports" / "r.xlsx")
    _w(dst / "苏右" / "a.xlsx", "existing")      # present: never overwritten
    _w(src / ".checkpoint", "123")                  # hidden: never mirrored
    out = collector.mirror_to(dst, src)
    assert out == {"mirror_copied": 1, "mirror_moved": 0}
    assert (dst / "苏右" / "a.xlsx").read_text() == "existing"
    assert (dst / "苏右" / "client-reports" / "r.xlsx").exists()
    assert not (dst / ".checkpoint").exists()


def test_mirror_moves_demoted_root_file_instead_of_duplicating(tmp_path):
    src, dst = tmp_path / "local", tmp_path / "onedrive"
    _w(src / "巴盟" / "archived" / "plan11月(3).xlsx")   # demoted locally
    _w(src / "巴盟" / "plan11月(5).xlsx")                # new fullest version
    _w(dst / "巴盟" / "plan11月(3).xlsx", "old-root")
    out = collector.mirror_to(dst, src)
    assert out == {"mirror_copied": 1, "mirror_moved": 1}
    assert not (dst / "巴盟" / "plan11月(3).xlsx").exists()
    assert (dst / "巴盟" / "archived" / "plan11月(3).xlsx").read_text() == "old-root"
    assert (dst / "巴盟" / "plan11月(5).xlsx").exists()


def test_mirror_is_noop_onto_itself(tmp_path):
    _w(tmp_path / "a" / "x.xlsx")
    assert collector.mirror_to(tmp_path, tmp_path) == {"mirror_copied": 0,
                                                       "mirror_moved": 0}


def test_env_switches_working_root_and_checkpoint(tmp_path, monkeypatch):
    monkeypatch.setenv("WECOM_NOMINATION_ROOT", str(tmp_path))
    mod = importlib.reload(collector)
    try:
        assert mod.NOMINATION_ROOT == tmp_path
        assert mod.STATE_FILE == tmp_path / ".checkpoint"
        assert str(mod.ONEDRIVE_ROOT) == "data/raw/nomination"
    finally:
        monkeypatch.delenv("WECOM_NOMINATION_ROOT")
        importlib.reload(collector)
    assert str(collector.NOMINATION_ROOT) == "data/raw/nomination"


# ------------------------------------------------- month-less accumulating file

def _xlsx(path, sheets):
    import openpyxl
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for s in sheets:
        wb.create_sheet(s)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def test_monthless_plan_is_stamped_with_its_last_day_sheet(tmp_path):
    src = tmp_path / "220kV旭纯哈乐储能电站电站电力交易调度计划表.xlsx"
    _xlsx(src, ["09.22", "10.31", "11.18"])
    name, cls = collector._versioned(src, collector.classify(src.name))
    assert name == "220kV旭纯哈乐储能电站电站电力交易调度计划表11.18.xlsx"
    assert (cls.station, cls.kind, cls.month) == ("武川", "monthly", 11)
    # the stamped name is itself recognised as a dated monthly version
    assert collector.classify(name).kind == "monthly"


def test_versioned_leaves_other_files_alone(tmp_path):
    for fname in ("【5】景蓝乌尔图-电力交易调度计划表11月.xlsx",      # already monthly
                  "旭纯哈乐储能电站电价日报2026.11.03.xlsx",          # daily
                  "资产管理部组织架构261101.pptx"):                 # unclassified
        cls = collector.classify(fname)
        assert collector._versioned(tmp_path / fname, cls) == (fname, cls)


def test_collect_keeps_newer_version_of_monthless_plan(tmp_path, monkeypatch):
    import os
    cache, root = tmp_path / "cache", tmp_path / "root"
    fname = "220kV旭纯哈乐储能电站电站电力交易调度计划表.xlsx"
    old, new = cache / "a" / fname, cache / "b" / fname
    _xlsx(old, ["11.01", "11.04"])
    _xlsx(new, ["11.01", "11.04", "11.18"])
    os.utime(old, (1000, 1000)); os.utime(new, (2000, 2000))
    monkeypatch.setattr(collector, "CACHE_ROOT", cache)
    monkeypatch.setattr(collector, "NOMINATION_ROOT", root)
    monkeypatch.setattr(collector, "STATE_FILE", root / ".checkpoint")
    stats = collector.collect(since=0)
    assert stats["copied"] == 2
    stem = "220kV旭纯哈乐储能电站电站电力交易调度计划表"
    assert (root / "武川" / f"{stem}11.18.xlsx").exists()        # fullest in root
    assert (root / "武川" / "archived" / f"{stem}11.04.xlsx").exists()
    assert not (root / "武川" / f"{stem}11.04.xlsx").exists()
