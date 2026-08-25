"""Tests for shared/persona_profile.py"""
from shared.persona_profile import profile_section


def test_extracts_domain_judgment_section_from_real_profile():
    """The dipeng-chen profile (repo: skills/colleague/dipeng-chen/work.md)
    must yield its §4 judgment block."""
    text = profile_section("经验知识库")
    assert "重庆" in text
    assert "渗透率" in text
    assert text.startswith("## 4.")


def test_section_stops_at_next_heading():
    text = profile_section("负责范围")
    assert "工作流程" not in text  # next section must not bleed in


def test_missing_slug_returns_empty():
    assert profile_section("经验知识库", slug="no-such-person") == ""


def test_missing_section_returns_empty():
    assert profile_section("nonexistent heading") == ""
