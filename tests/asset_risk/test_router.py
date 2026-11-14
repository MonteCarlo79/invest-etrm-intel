# tests/asset_risk/test_router.py
import pathlib, pytest
from libs.asset_risk.settlement.router import BillText, ROUTES, ROUTE_LABELS, route

FIX = pathlib.Path(__file__).parent.parent / "settlement_ingest" / "fixtures"
FIX2 = pathlib.Path(__file__).parent / "fixtures" / "router"

def _bill(text, scanned=False, name="x.pdf"):
    return BillText(filename=name, path="/nonexistent.pdf", is_scanned=scanned, full_text=text, page1_text=text.split("\f")[0])

CASES = [  # (fixture, kind_hint, expected label prefix)
    ("wb2_discharge_202503.txt", "discharge", "蒙西上网电费结算单"),
    ("guangxi_discharge_202504.txt", "discharge", "广西电力交易结算依据"),
    ("guangxi_grid_dup_202504.txt", "discharge", "电网版上网结算单"),
    ("stategrid_discharge_202505.txt", "discharge", "网上国网发电侧电费账单"),
    ("sdtc_202504.txt", "discharge", "山东电力交易中心交易结算单"),
    ("ah_sdtc_202602.txt", "discharge", "安徽电力交易中心(统推)结算单"),
    ("ah_discharge_202609.txt", "discharge", "国网安徽发电侧电费账单"),
    ("stategrid_discharge_local_202501.txt", "discharge", "山东省地方电厂市场化结算单"),
    ("wb2_charge_202503.txt", "charge", "蒙西下网电费核查票"),
    ("guangxi_charge_202504.txt", "charge", "广西电网电费通知单"),
    ("stategrid_charge_202505.txt", "charge", "网上国网用电侧电费账单"),
]

@pytest.mark.parametrize("fixture,hint,label", CASES)
def test_route_picks_expected_parser(fixture, hint, label):
    r = route(_bill((FIX / fixture).read_text(encoding="utf-8")), hint, "upload")
    assert r is not None and r.label.startswith(label)

# Ruling R2: the Task 5 gansu fixtures drive the 国网甘肃 routes
GANSU_CASES = [
    ("gansu_bill.txt", "discharge", "国网甘肃 bill detected", "discharge"),
    ("gansu_charge.txt", "charge", "国网甘肃 charge bill detected", "charge"),
]

@pytest.mark.parametrize("fixture,hint,label,kind", GANSU_CASES)
def test_route_picks_gansu_parser(fixture, hint, label, kind):
    r = route(_bill((FIX2 / fixture).read_text(encoding="utf-8")), hint, "upload")
    assert r is not None and r.label.startswith(label) and r.kind == kind

def test_route_order_is_frozen():
    assert tuple(r.label for r in ROUTES) == ROUTE_LABELS

def test_charge_filename_never_routes_to_discharge_parser():
    # Review Focus 1: a 下网 filename with discharge-looking text stays on the charge branch
    r = route(_bill((FIX / "wb2_discharge_202503.txt").read_text(encoding="utf-8")), "charge", "upload")
    assert r is not None and r.kind == "charge"

def test_scanner_mode_never_gets_voucher_fallback():
    # Review Focus 2
    b = _bill("", scanned=True, name="2026-08 发电侧结算凭证.pdf")
    r = route(b, "voucher", "scanner")
    assert r is None or r.kind == "skip"
    r2 = route(b, "voucher", "upload")
    assert r2 is not None and r2.upload_only

def test_scanned_discharge_goes_to_vision():
    r = route(_bill("", scanned=True), "discharge", "upload")
    assert r is not None and r.needs_text_layer is False and r.kind == "discharge"

def test_unknown_text_charge_falls_through_to_generic_vision():
    r = route(_bill((FIX2 / "unknown.txt").read_text(encoding="utf-8")), "charge", "upload")
    assert r is not None and "Vision" in r.label

def test_unknown_hint_follows_scanned_flag():
    # today's tab: "unknown" + scanned -> discharge (vision); "unknown" + text -> charge branch
    assert route(_bill("", scanned=True), "unknown", "upload").kind == "discharge"
    assert route(_bill((FIX2 / "unknown.txt").read_text(encoding="utf-8")), "unknown", "upload").kind == "charge"

def test_scanned_charge_goes_to_verified_vision():
    r = route(_bill("", scanned=True), "charge", "upload")
    assert r is not None and r.needs_text_layer is False and r.label.startswith("Scanned 电费计算明细")

def test_mengxi_charge_regex_route(tmp_path, monkeypatch):
    # The Mengxi regex route detects by running parse_charging_cost_pdf(path);
    # stub the PDF read with the fixture text so no real PDF is needed.
    from services.settlement_ingest import parser_charge
    text = (FIX2 / "mengxi_charge_regex.txt").read_text(encoding="utf-8")
    pdf = tmp_path / "suyou.pdf"
    pdf.write_bytes(b"%PDF-stub")
    monkeypatch.setattr(parser_charge, "parse_charging_cost_pdf",
                        lambda p: parser_charge.parse_charging_text(text))
    b = BillText(filename="suyou.pdf", path=str(pdf), is_scanned=False, full_text=text, page1_text=text)
    r = route(b, "charge", "upload")
    assert r is not None and r.kind == "charge" and "Vision" not in r.label
    assert len(r.parse(b)) == 4

def test_detect_errors_never_raise(monkeypatch):
    # Ruling R3: a path-dependent detector on a missing file, or any detector
    # raising, is treated as "no match" — route() never raises.
    from services.settlement_ingest import parser_wb2
    def boom(_t):
        raise RuntimeError("detector exploded")
    monkeypatch.setattr(parser_wb2, "is_wb2_charge_bill", boom)
    r = route(_bill((FIX2 / "unknown.txt").read_text(encoding="utf-8")), "charge", "upload")
    assert r is not None and "Vision" in r.label

def test_skip_hint_routes_nowhere():
    assert route(_bill("anything"), "skip", "upload") is None
