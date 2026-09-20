# tests/citic_futures/test_weekly.py
"""Tests for the CITIC Futures weekly ingest + digest."""
from pathlib import Path
from unittest.mock import MagicMock, patch

from services.citic_futures.ingest_weekly import (
    relevant_group, find_pending_weekly_folders, mark_folder_done, _load_state)
from services.citic_futures.weekly_digest import build_prompt, build_card


# ── relevance filter ────────────────────────────────────────────────────────

def test_relevant_group_energy_chain():
    assert relevant_group("【中信期货能源化工（原油）】中东局势持续威胁供应——周报20260913.pdf") == "energy_chain"
    assert relevant_group("【中信期货能源化工（板块策略）】地缘支撑能源价格——周度策略报告20260913.pdf") == "energy_chain"
    assert relevant_group("【中信期货能源化工（LPG）】地缘冲突烈度上升——周报20260913.pdf") == "energy_chain"


def test_relevant_group_new_materials_and_metals():
    assert relevant_group("【中信期货有色与新材料（新能源金属）】市场预期混乱，碳酸锂延续弱势——周报20260913.pdf") == "new_materials"
    assert relevant_group("【中信期货有色与新材料（锂）】锂电公司半年报分析——专题报告20260913.pdf") == "new_materials"
    assert relevant_group("【中信期货有色与新材料（锌）】供应偏紧叠加海外挤仓-20260912.pdf") == "basic_metals"
    assert relevant_group("【中信期货有色与新材料】基本金属或重回震荡整理——周度策略报告20260913.pdf") == "basic_metals"


def test_relevant_group_excludes_black_agri_precious():
    assert relevant_group("【中信期货黑色建材】螺纹反套策略离场——周度策略报告20260913.pdf") is None
    assert relevant_group("【中信期货农业（油脂油料）】油脂油料板块震荡偏弱——周报20260913.pdf") is None
    assert relevant_group("【中信期货贵金属（黄金&白银）】9月加息风险抬升——周报20260913.pdf") is None
    assert relevant_group("【中信期货农业（饲料养殖）】新粮上市后玉米价格走弱——周报20260913.pdf") is None


# ── state file / folder dedup ───────────────────────────────────────────────

def _make_tree(tmp_path: Path) -> Path:
    base = tmp_path / "zhongxin-futures"
    (base / "中信期货周报20260907").mkdir(parents=True)
    (base / "中信期货周报20260914").mkdir(parents=True)
    (base / "random_other_dir").mkdir(parents=True)
    return base


def test_find_pending_excludes_done_and_nonconforming(tmp_path):
    base = _make_tree(tmp_path)
    mark_folder_done(base, base / "中信期货周报20260907")
    pending = find_pending_weekly_folders(base)
    assert [p.name for p in pending] == ["中信期货周报20260914"]


def test_mark_folder_done_persists(tmp_path):
    base = _make_tree(tmp_path)
    mark_folder_done(base, base / "中信期货周报20260914")
    mark_folder_done(base, base / "中信期货周报20260914")  # idempotent
    state = _load_state(base)
    assert state["done_folders"] == ["中信期货周报20260914"]
    assert [p.name for p in find_pending_weekly_folders(base)] == ["中信期货周报20260907"]


# ── digest prompt/card assembly ─────────────────────────────────────────────

def test_build_prompt_groups_content():
    prompt = build_prompt(
        {"energy_chain": [("原油.pdf", "供应紧张格局强化")],
         "new_materials": [("碳酸锂.pdf", "碳酸锂延续弱势")]},
        "中信期货周报20260914",
    )
    assert "中信期货周报20260914" in prompt
    assert "能源化工" in prompt and "新材料(锂/新能源金属)" in prompt
    assert "供应紧张格局强化" in prompt and "碳酸锂延续弱势" in prompt


def test_build_card_structure():
    card = build_card("中信期货周报20260914", "摘要正文", n_docs=12, n_skipped=3)
    assert card["header"]["template"] == "blue"
    body = card["elements"][0]["text"]["content"]
    assert "摘要正文" in body and "12 篇" in body and "3 篇" in body


# ── ingest_folder orchestration (mocked KB) ─────────────────────────────────

def test_ingest_folder_counts_and_relevance(tmp_path):
    folder = tmp_path / "中信期货周报20260914"
    folder.mkdir()
    for name in ["【中信期货能源化工（原油）】a.pdf", "【中信期货黑色建材】b.pdf"]:
        (folder / name).write_bytes(b"%PDF-1.4 fake")

    from services.citic_futures import ingest_weekly as iw

    calls = []

    def fake_register(file_bytes, filename, **kwargs):
        calls.append(filename)
        return (len(calls), True, "futures_weekly")

    with patch("services.knowledge_pool.knowledge_docs.register_and_ingest", side_effect=fake_register):
        out = iw.ingest_folder(folder, api_key="k")

    assert out["ingested"] == 2 and out["skipped_dup"] == 0 and out["failed"] == []
    assert [f for _, f, g in out["relevant_docs"]] == ["【中信期货能源化工（原油）】a.pdf"]
    assert all(g == "energy_chain" for _, _, g in out["relevant_docs"])


def test_ingest_folder_failed_file_isolated(tmp_path):
    folder = tmp_path / "中信期货周报20260914"
    folder.mkdir()
    (folder / "【中信期货能源化工（原油）】a.pdf").write_bytes(b"pdf")
    (folder / "【中信期货能源化工（LPG）】b.pdf").write_bytes(b"pdf")

    from services.citic_futures import ingest_weekly as iw

    def fake_register(file_bytes, filename, **kwargs):
        if "LPG" in filename:
            raise RuntimeError("parse error")
        return (1, True, "futures_weekly")

    with patch("services.knowledge_pool.knowledge_docs.register_and_ingest", side_effect=fake_register):
        out = iw.ingest_folder(folder, api_key="k")

    assert out["ingested"] == 1 and len(out["failed"]) == 1


def test_ingest_folder_keeps_relevant_docs_on_duplicate(tmp_path):
    """Re-run after a crashed digest step must still yield relevant docs."""
    folder = tmp_path / "中信期货周报20260914"
    folder.mkdir()
    (folder / "【中信期货能源化工（原油）】a.pdf").write_bytes(b"pdf")

    from services.citic_futures import ingest_weekly as iw

    with patch("services.knowledge_pool.knowledge_docs.register_and_ingest",
               return_value=(42, False, "futures_weekly")):
        out = iw.ingest_folder(folder, api_key="k")

    assert out["skipped_dup"] == 1
    assert out["relevant_docs"] == [(42, "【中信期货能源化工（原油）】a.pdf", "energy_chain")]
