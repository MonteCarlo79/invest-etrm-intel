"""fa_ingest pure routing helpers — synthetic rules, no real personal data."""
from datetime import date

from services.hermes.fa_ingest import dated_name, route_message, uk_tax_year, wanted

RULES = {
    "unsorted": "_inbox_unsorted",
    "watch": ["insureco", "acme bank"],
    "rules": [
        {"name": "ins A", "any": ["insureco"], "all": ["POL-A1"], "dest": "Props/A/insurance"},
        {"name": "agent", "any": ["letting ltd"], "dest": "Props/B/rent", "invoice_dest": "Props/B/bills"},
        {"name": "svc", "any": ["blockmgr"], "dest": "Props/B/service charge"},
        {"name": "svc2", "any": ["house b"], "all": ["service charge"], "dest": "Props/B/service charge"},
        {"name": "tax", "any": ["hmrc"], "dest": "UK tax/{tax_year}"},
        {"name": "bank", "any": ["acme bank"], "dest": "Bank/Acme",
         "subfolders": {"11112222": "11112222", "credit card": "Acme credit"}},
    ],
}
D = date(2025, 11, 1)


def r(sender="", subject="", preview="", atts=(), d=D):
    return route_message(RULES, sender, subject, preview, list(atts), d)


def test_tax_year_boundary_is_6_april():
    assert uk_tax_year(date(2026, 4, 5)) == "2025-2026"
    assert uk_tax_year(date(2026, 4, 6)) == "2026-2027"
    assert r("HMRC noreply", "Self Assessment", d=date(2026, 4, 5)).dest == "UK tax/2025-2026"


def test_all_words_required_else_unsorted():
    assert r("InsureCo", "Renewal POL-A1").dest == "Props/A/insurance"
    # in scope (watch word) but no policy number → unsorted, never guessed
    assert r("InsureCo", "Your renewal").dest == "_inbox_unsorted"


def test_invoice_redirect_and_order():
    assert r("Letting Ltd", "Statement Oct").dest == "Props/B/rent"
    assert r("Letting Ltd", "Statement", atts=["Invoice_123.pdf"]).dest == "Props/B/bills"
    assert r("someone", "House B service charge demand").dest == "Props/B/service charge"


def test_subfolders():
    assert r("Acme Bank", "Statement for 11112222").dest == "Bank/Acme/11112222"
    assert r("Acme Bank", "Your credit card statement").dest == "Bank/Acme/Acme credit"
    assert r("Acme Bank", "Statement").dest == "Bank/Acme"


def test_out_of_scope_ignored():
    assert r("colleague@work.com", "Q3 deck", atts=["deck.pdf"]).dest is None


def test_extensions_and_dated_name():
    exts = ["pdf", "csv", "xls", "xlsx"]
    assert wanted("A.PDF", exts) and wanted("b.xlsx", exts)
    assert not wanted("logo.png", exts) and not wanted("invite.ics", exts)
    assert dated_name("statement.pdf", date(2025, 11, 3)) == "statement_20251103.pdf"


def test_get_retries_transient_graph_errors(monkeypatch):
    import requests
    from services.hermes import fa_ingest
    codes = iter([504, 503, 200])

    def fake_get(url, headers=None, params=None, timeout=None):
        r = requests.Response()
        r.status_code = next(codes)
        return r
    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr("time.sleep", lambda s: None)
    assert fa_ingest._get("u", lambda: {}).status_code == 200
