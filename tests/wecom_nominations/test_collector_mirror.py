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
