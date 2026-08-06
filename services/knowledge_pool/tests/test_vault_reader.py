from services.knowledge_pool import vault_reader


class FakeOneDrive:
    def __init__(self, listing=None, files=None, search_results=None):
        self._listing = listing or {}
        self._files = files or {}
        self._search = search_results or []

    def list_items(self, folder_path="/"):
        return self._listing.get(folder_path, [])

    def read_file_by_path(self, file_path):
        return self._files[file_path].encode("utf-8")

    def search(self, query):
        return self._search


def _fake_client():
    listing = {
        "etrm/bess-platform/knowledge/spot_market/01_daily_reports": [
            {"name": "2026-08-04.md"}, {"name": "2026-08-05.md"},
        ],
        "etrm/bess-platform/knowledge/spot_market/02_provinces": [
            {"name": "山东.md"}, {"name": "山西.md"},
        ],
        "etrm/bess-platform/knowledge/spot_market/03_concepts": [
            {"name": "新能源出力下降.md"}, {"name": "检修.md"},
        ],
        "etrm/bess-platform/knowledge/hermes/briefings": [
            {"name": "2026-08-05-morning.md"},
        ],
    }
    files = {
        "etrm/bess-platform/knowledge/spot_market/01_daily_reports/2026-08-05.md": "8月5日 山东均价0.32",
        "etrm/bess-platform/knowledge/spot_market/02_provinces/山东.md": "# 山东\n光伏大省",
        "etrm/bess-platform/knowledge/spot_market/03_concepts/新能源出力下降.md": "# 新能源出力下降",
    }
    return FakeOneDrive(listing, files)


def test_date_mention_finds_daily_note(monkeypatch):
    monkeypatch.setattr(vault_reader, "_client", lambda: _fake_client())
    hits = vault_reader.search_notes(query="8月5日价格如何 2026-08-05")
    assert hits[0]["path"] == "spot_market/01_daily_reports/2026-08-05.md"
    assert hits[0]["area"] == "daily"


def test_province_name_finds_province_note(monkeypatch):
    monkeypatch.setattr(vault_reader, "_client", lambda: _fake_client())
    hits = vault_reader.search_notes(query="山东的市场结构")
    assert any(h["path"] == "spot_market/02_provinces/山东.md" for h in hits)


def test_read_note_truncates(monkeypatch):
    monkeypatch.setattr(vault_reader, "_client", lambda: _fake_client())
    text = vault_reader.read_note("spot_market/02_provinces/山东.md", max_chars=5)
    assert text.startswith("# 山东")
    assert "[…truncated]" in text


def test_retrieve_context_formats_block(monkeypatch):
    monkeypatch.setattr(vault_reader, "_client", lambda: _fake_client())
    block = vault_reader.retrieve_vault_context("山东 2026-08-05 价格")
    assert "## Vault knowledge (from markdown notes)" in block
    assert "山东均价0.32" in block
    assert "光伏大省" in block


def test_empty_when_unconfigured(monkeypatch):
    monkeypatch.setattr(vault_reader, "_client", lambda: None)
    assert vault_reader.search_notes(query="x") == []
    assert vault_reader.retrieve_vault_context("x") == ""


def test_search_falls_back_to_onedrive_search(monkeypatch):
    fake = _fake_client()
    fake._search = [{
        "name": "2026-08-01.md",
        "parentReference": {"path": "/drive/root:/etrm/bess-platform/knowledge/spot_market/01_daily_reports"},
    }]
    monkeypatch.setattr(vault_reader, "_client", lambda: fake)
    hits = vault_reader.search_notes(query="完全无关的查询词zzz")
    assert hits[0]["area"] == "daily"
