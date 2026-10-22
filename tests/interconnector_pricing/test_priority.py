"""Clearing-priority anchor map and priority_note generation."""
from libs.interconnector_pricing import agreements as A
from libs.interconnector_pricing import params as P


def test_anchor_map_covers_seed_channels():
    for a in A.AGREEMENTS_2027:
        if a["channel_label"].endswith("合计"):
            continue  # display-only context row has no channel
        assert a["channel_label"] in P.CHANNEL_ANCHOR_PROVINCE, a["channel_label"]


def test_priority_note_anchor_vs_intermediary():
    sx = [a for a in A.AGREEMENTS_2027
          if a["sender"] == "陕西" and a["channel_label"] == "宝合直流"][0]
    assert "锚省优先" in A.priority_note(sx)
    gs = [a for a in A.AGREEMENTS_2027
          if a["sender"] == "甘肃" and a["channel_label"] == "宝合直流"][0]
    note = A.priority_note(gs)
    assert "中转" in note and "陕西" in note and "锚省优先" not in note


def test_priority_note_period_tiers():
    qh = [a for a in A.AGREEMENTS_2027
          if a["sender"] == "青海" and a.get("period") == "2026-2028"][0]
    assert A.priority_note(qh).startswith("存量PPA·优先")
    gs = [a for a in A.AGREEMENTS_2027
          if a["sender"] == "甘肃" and a.get("period") == "2027-2029"][0]
    assert A.priority_note(gs).startswith("新增PPA")


def test_priority_note_xizang():
    xz = [a for a in A.AGREEMENTS_2027 if a["sender"] == "西藏"][0]
    assert "锚省优先" in A.priority_note(xz)   # 拉萨端反向送出锚省


def test_priority_note_unknown_channel_is_tier_only():
    assert A.priority_note({"sender": "X", "channel_label": "不存在直流"}) == "新增PPA"
