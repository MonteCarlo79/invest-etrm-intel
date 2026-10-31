"""Tests for services/hermes/sysopfee_extract.py"""
import io
import sys
import types

import pytest

from services.hermes import sysopfee_extract as sx


class _FakeResp:
    def __init__(self, text):
        self.content = [type("B", (), {"text": text})()]


class _FakeClient:
    def __init__(self, text):
        self._text = text

    @property
    def messages(self):
        return self

    def create(self, **kwargs):
        return _FakeResp(self._text)


def _patch_client(monkeypatch, text):
    from shared import anthropic_client as _ac
    monkeypatch.setattr(_ac, "make_client", lambda k: _FakeClient(text))


def test_vision_extract_value(monkeypatch):
    _patch_client(monkeypatch, "0.0357")
    assert sx.extract_from_image_bytes(b"\xff\xd8img", api_key="k") == 0.0357


def test_vision_extract_none(monkeypatch):
    _patch_client(monkeypatch, "NONE")
    assert sx.extract_from_image_bytes(b"\xff\xd8img", api_key="k") is None


def test_vision_extract_messy_response(monkeypatch):
    _patch_client(monkeypatch, "系统运行费用折合度电水平：0.0357 元/千瓦时")
    assert sx.extract_from_image_bytes(b"\xff\xd8img", api_key="k") == 0.0357


def _fake_pdfplumber(monkeypatch, page_texts):
    """Stub pdfplumber with pages carrying the given text layer."""

    class _Page:
        def __init__(self, text):
            self._text = text

        def extract_text(self):
            return self._text

    class _Pdf:
        def __init__(self, *_a, **_k):
            self.pages = [_Page(t) for t in page_texts]

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    fake = types.ModuleType("pdfplumber")
    fake.open = lambda _buf: _Pdf()
    monkeypatch.setitem(sys.modules, "pdfplumber", fake)


def test_pdf_text_layer_extraction(monkeypatch):
    _fake_pdfplumber(monkeypatch, [
        "代理购电价格表\n系统运行费用折合度电水平  0.0357 元/千瓦时",
    ])
    assert sx.extract_from_pdf_bytes(b"%PDF-fake", api_key="k") == 0.0357


def test_pdf_text_layer_variant_label(monkeypatch):
    _fake_pdfplumber(monkeypatch, ["系统运行费折合度电水平：0.0421"])
    assert sx.extract_from_pdf_bytes(b"%PDF-fake", api_key="k") == 0.0421


def test_pdf_no_match_returns_via_vision(monkeypatch):
    _fake_pdfplumber(monkeypatch, ["这是一份没有目标行的文件"])
    _patch_client(monkeypatch, "NONE")

    # vision fallback renders the page — stub to_image on the page objects
    class _Img:
        def save(self, buf, format):
            buf.write(b"\x89PNGfake")

    class _Rendered:
        original = _Img()

    class _Page:
        def extract_text(self):
            return "这是一份没有目标行的文件"

        def to_image(self, resolution):
            return _Rendered()

    class _Pdf:
        def __init__(self):
            self.pages = [_Page()]

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    fake = types.ModuleType("pdfplumber")
    fake.open = lambda _buf: _Pdf()
    monkeypatch.setitem(sys.modules, "pdfplumber", fake)
    assert sx.extract_from_pdf_bytes(b"%PDF-fake", api_key="k") is None


def test_upsert_writes_expected_sql():
    calls = []

    class Cur:
        def execute(self, sql, params):
            calls.append((sql, params))

    sx.upsert_sysopfee(Cur(), "蒙西", "2026-05-01", 0.0357, "test.png")
    sql, params = calls[0]
    assert "province_sysopfee_monthly" in sql
    assert "ON CONFLICT (province, year_month)" in sql
    assert params == ("蒙西", "2026-05-01", 0.0357, "test.png")


# ── article-driven automation ────────────────────────────────────────────────

def test_canon_province_name():
    assert sx._canon_province_name("山东省") == "山东"
    assert sx._canon_province_name("上海市") == "上海"
    assert sx._canon_province_name("广西壮族自治区") == "广西"
    assert sx._canon_province_name("蒙西") == "蒙西"
    assert sx._canon_province_name("内蒙古自治区") is None  # ambiguous 蒙西/蒙东
    assert sx._canon_province_name("") is None


def test_article_gate_rejects_non_price_article():
    art = {"title": "某地风电项目开工", "body": "正文 [IMG:https://mmbiz.qpic.cn/x]"}
    assert sx.process_article_images(art, pg_url="x", api_key="k") == 0


def test_article_gate_requires_title_month():
    art = {"title": "代理购电价格公告", "body": "代理购电 [IMG:https://mmbiz.qpic.cn/x]"}
    assert sx.process_article_images(art, pg_url="x", api_key="k") == 0


def _patch_article_mocks(monkeypatch, vision_results):
    """Mock HTTP fetch (returns fake big image) + vision + psycopg2."""
    import types as _t

    class _Resp:
        content = b"\xff\xd8" + b"0" * 20000  # > _MIN_IMAGE_BYTES

        def raise_for_status(self):
            pass

    class _Session:
        headers = {}
        trust_env = True

        def get(self, url, timeout):
            return _Resp()

    fake_requests = _t.ModuleType("requests")
    fake_requests.Session = lambda: _Session()
    monkeypatch.setitem(sys.modules, "requests", fake_requests)

    calls = []

    def _fake_vision(data, media_type, api_key):
        return vision_results.pop(0)

    monkeypatch.setattr(sx, "_vision_extract_province_fee", _fake_vision)

    upserts = []

    class _Cur:
        def execute(self, sql, params):
            upserts.append(params)

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Conn:
        def cursor(self):
            return _Cur()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    fake_pg = _t.ModuleType("psycopg2")
    fake_requests_connect = lambda url: _Conn()
    fake_psycopg2 = _t.ModuleType("psycopg2")
    fake_psycopg2.connect = fake_requests_connect
    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)
    return upserts


def test_process_article_images_happy_path(monkeypatch):
    upserts = _patch_article_mocks(monkeypatch, [("山东", 0.0357), (None, None)])
    body = ("代理购电价格汇总\n[IMG:https://mmbiz.qpic.cn/mmbiz_png/abc/640]\n"
            "[IMG:https://mmbiz.qpic.cn/mmbiz_png/def/640]\n")
    art = {"title": "2026年5月各省市电网代理购电价格汇总",
           "body": body, "url": "https://mp.weixin.qq.com/s/xyz"}
    n = sx.process_article_images(art, pg_url="pg", api_key="k")
    assert n == 1  # second image had no province → skipped
    assert upserts[0][0] == "山东"
    assert upserts[0][1] == "2026-05-01"
    assert upserts[0][2] == 0.0357
    assert upserts[0][3] == "https://mp.weixin.qq.com/s/xyz"


def test_process_article_images_ignores_non_qpic(monkeypatch):
    upserts = _patch_article_mocks(monkeypatch, [])
    body = "代理购电 [IMG:https://evil.example.com/x.png]"
    art = {"title": "2026年5月代理购电价格", "body": body}
    assert sx.process_article_images(art, pg_url="pg", api_key="k") == 0
    assert upserts == []


def test_real_fixture_mengxi_may2026():
    """Real extraction on the 蒙西 2026-05 fixture — requires ANTHROPIC_API_KEY.

    Self-skips without the key / fixture; run on ECS where Anthropic is reachable.
    """
    import os
    from pathlib import Path

    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        pytest.skip("ANTHROPIC_API_KEY not set")
    fixture = Path("data/raw/各省电网购电信息/蒙西/2026年/5月份.jpg")
    if not fixture.exists():
        pytest.skip(f"fixture missing: {fixture}")
    assert sx.extract_from_image_bytes(fixture.read_bytes(), api_key=key) == pytest.approx(0.0357, abs=1e-6)
