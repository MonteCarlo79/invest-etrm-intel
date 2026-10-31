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


def test_shrink_image_passthrough_for_small_files():
    small = b"\xff\xd8" + b"0" * 1000
    assert sx._shrink_image(small) is small  # untouched under the limit


def test_sniff_media_type():
    assert sx._sniff_media_type(b"\x89PNG\r\n\x1a\nrest") == "image/png"
    assert sx._sniff_media_type(b"RIFF\x00\x00\x00\x00WEBPrest") == "image/webp"
    assert sx._sniff_media_type(b"\xff\xd8\xff\xe0rest") == "image/jpeg"
    assert sx._sniff_media_type(b"GIF89a....") == "image/gif"
    assert sx._sniff_media_type(b"somethingelse") == "image/jpeg"  # default


def test_normalize_fee_fen_to_yuan():
    assert sx._normalize_fee(8.0) == 0.08       # 分/千瓦时 table
    assert sx._normalize_fee(12.0) == 0.12
    assert sx._normalize_fee(0.0357) == 0.0357  # already 元/千瓦时
    assert sx._normalize_fee(0.15) == 0.15
    assert sx._normalize_fee(1.0) == 1.0        # boundary: ≤1 stays as-is


# ── scroll-screenshot extraction ─────────────────────────────────────────────

def _tall_image(width=394, height=5300):
    pytest.importorskip("PIL")
    from PIL import Image
    img = Image.new("RGB", (width, height), "white")
    buf = io.BytesIO()
    img.save(buf, format="JPEG")
    return buf.getvalue()


def test_slice_scroll_geometry():
    chunks = sx.slice_scroll(_tall_image(), chunk_px=2000, overlap_px=300)
    # 5300px tall, step 1700 → starts at 0, 1700, 3400; at y=3400 the chunk
    # reaches the bottom (3400+2000 ≥ 5300) → 3 chunks total
    assert len(chunks) == 3
    from PIL import Image
    h0 = Image.open(io.BytesIO(chunks[0])).size[1]
    h_last = Image.open(io.BytesIO(chunks[-1])).size[1]
    assert h0 == 2000
    assert h_last == 5300 - 3400  # 1900


def test_slice_scroll_short_image_single_chunk():
    chunks = sx.slice_scroll(_tall_image(height=1500), chunk_px=2000, overlap_px=300)
    assert len(chunks) == 1


def test_extract_scroll_rows_merge_and_conflict(monkeypatch):
    responses = [
        [{"province": "山东", "fee": 0.09}, {"province": "蒙西", "fee": 0.0357}],
        [{"province": "蒙西", "fee": 0.0357}, {"province": "北京市", "fee": 12}],  # 分→元 + suffix
        [{"province": "山东", "fee": 0.5}],  # conflict with chunk 0 → 山东 dropped
    ]
    monkeypatch.setattr(sx, "_vision_json", lambda *a, **k: responses.pop(0))
    monkeypatch.setattr(sx, "slice_scroll", lambda b: [b"c0", b"c1", b"c2"])
    rows = sx.extract_scroll_rows(b"img", api_key="k")
    assert rows == {"蒙西": 0.0357, "北京": 0.12}  # 山东 dropped on conflict


def test_extract_scroll_rows_skips_unparseable(monkeypatch):
    monkeypatch.setattr(sx, "_vision_json", lambda *a, **k: None)
    monkeypatch.setattr(sx, "slice_scroll", lambda b: [b"c0"])
    assert sx.extract_scroll_rows(b"img", api_key="k") == {}


def test_identify_scroll_month(monkeypatch):
    monkeypatch.setattr(sx, "_vision_json", lambda *a, **k: {"year": 2026, "month": 6})
    assert sx.identify_scroll_month(_tall_image(), api_key="k") == "2026-06-01"


def test_identify_scroll_month_garbage(monkeypatch):
    monkeypatch.setattr(sx, "_vision_json", lambda *a, **k: {"year": "x"})
    assert sx.identify_scroll_month(_tall_image(), api_key="k") is None


def test_vision_extract_fen_unit_converted(monkeypatch):
    _patch_client(monkeypatch, "8")  # model missed the 分 unit → guard catches it
    assert sx.extract_from_image_bytes(b"\xff\xd8img", api_key="k") == 0.08


def test_shrink_image_reencodes_large_files():
    pytest.importorskip("PIL")
    import random

    from PIL import Image
    random.seed(7)
    img = Image.frombytes(
        "RGB", (3000, 2400),
        bytes(random.getrandbits(8) for _ in range(3000 * 2400 * 3)),
    )
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=95)
    big = buf.getvalue()
    assert len(big) > sx._MAX_IMAGE_BYTES  # noise must not compress under the limit
    shrunk = sx._shrink_image(big)
    assert len(shrunk) <= sx._MAX_IMAGE_BYTES
    out = Image.open(io.BytesIO(shrunk))
    assert max(out.size) <= 2400


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


def test_pdf_text_layer_skips_formula_cell(monkeypatch):
    """The 2026-05 incident: the 计算关系 formula cell (8=9+10+…) sits between
    the label and the value; capturing the first digit reads the formula."""
    _fake_pdfplumber(monkeypatch, [
        "8 系统运行费用折合度电水平 8=9+10+11+12+13+14+15+16 0.044318",   # 上海
    ])
    assert sx.extract_from_pdf_bytes(b"%PDF-fake", api_key="k") == 0.044318


def test_pdf_text_layer_plus_formula(monkeypatch):
    _fake_pdfplumber(monkeypatch, [
        "8 系统运行费用折合度电水平 +12+13+14 0.00674700",               # 北京
    ])
    assert sx.extract_from_pdf_bytes(b"%PDF-fake", api_key="k") == pytest.approx(0.006747)


def test_pdf_text_layer_footnote_sentence(monkeypatch):
    _fake_pdfplumber(monkeypatch, [
        "3.市场化交易用户执行上网环节线损费用折价0.023756元/千瓦时，"
        "执行系统运行费用折合度电水平0.050535元/千瓦时。",                  # 河北
    ])
    assert sx.extract_from_pdf_bytes(b"%PDF-fake", api_key="k") == 0.050535


def test_pdf_text_layer_bare_label_ignored(monkeypatch):
    """A bare label line (table header) must not swallow a later real row."""
    _fake_pdfplumber(monkeypatch, [
        "系统运行费用折合度电水平\n8 系统运行费用折合度电水平 8=9+10 0.150039",  # 吉林
    ])
    assert sx.extract_from_pdf_bytes(b"%PDF-fake", api_key="k") == 0.150039


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
    assert sx._canon_province_name("闽南") is None      # region of 福建, not canonical
    assert sx._canon_province_name("豫北") is None      # not in the canonical set


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
