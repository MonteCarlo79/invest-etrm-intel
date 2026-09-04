from services.knowledge_pool import vault_writer


class FakeOneDrive:
    def __init__(self):
        self.uploads = []

    def upload_file(self, folder_path, filename, content, conflict_behavior="replace"):
        self.uploads.append((folder_path, filename, content.decode("utf-8")))


def test_briefing_note_path_and_frontmatter(monkeypatch):
    fake = FakeOneDrive()
    monkeypatch.setattr(vault_writer, "_client", lambda: fake)
    path = vault_writer.write_briefing_note("morning", "# 早报\n今天多云", note_date="2026-08-06")
    assert path == "hermes/briefings/2026-08-06-morning.md"
    folder, filename, text = fake.uploads[0]
    assert folder == "etrm/bess-platform/knowledge/hermes/briefings"
    assert filename == "2026-08-06-morning.md"
    assert "note_type: briefing" in text
    assert "kind: morning" in text
    assert "date: 2026-08-06" in text
    assert "source: hermes" in text
    assert "# 早报" in text


def test_insight_note_goes_to_inbox_with_pending_review(monkeypatch):
    fake = FakeOneDrive()
    monkeypatch.setattr(vault_writer, "_client", lambda: fake)
    path = vault_writer.write_insight_note(
        category="market_view",
        content="山东现货午后低价与光伏出力强相关。",
        source_app="bess_map",
        province="山东",
        confidence="high",
    )
    folder, filename, text = fake.uploads[0]
    assert folder == "etrm/bess-platform/knowledge/hermes/inbox"
    assert filename.startswith("2026-") and filename.endswith(".md")
    assert "山东" in filename
    assert "note_type: insight" in text
    assert "review_status: pending" in text
    assert "category: market_view" in text
    assert "confidence: high" in text
    assert path is not None


def test_write_returns_none_when_no_client(monkeypatch):
    monkeypatch.setattr(vault_writer, "_client", lambda: None)
    assert vault_writer.write_briefing_note("morning", "x") is None
    assert vault_writer.write_insight_note("t", "c", "app") is None


def test_write_returns_none_on_upload_error(monkeypatch):
    class Boom:
        def upload_file(self, *a, **k):
            raise RuntimeError("network down")
    monkeypatch.setattr(vault_writer, "_client", lambda: Boom())
    assert vault_writer.write_briefing_note("morning", "x") is None


def test_insight_filename_has_time_component(monkeypatch):
    import re
    fake = FakeOneDrive()
    monkeypatch.setattr(vault_writer, "_client", lambda: fake)
    vault_writer.write_insight_note(category="t", content="测试内容甲乙丙", source_app="x")
    _, filename, _ = fake.uploads[0]
    assert re.match(r"^\d{4}-\d{2}-\d{2}-\d{6}-.+\.md$", filename)
