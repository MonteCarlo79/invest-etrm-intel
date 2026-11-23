"""sa_update pure helpers — synthetic data only."""
from datetime import date
from pathlib import Path

from services.fa_workings.sa_update import amount_in_text, property_for, tax_year, workbook_for


def test_tax_year_label():
    assert tax_year(date(2026, 4, 5)) == "2025-26"
    assert tax_year(date(2026, 4, 6)) == "2026-27"


def test_amount_must_be_printed():
    text = "Rent £2,800\nRenewal Commission (8% Renewal) £2688\nTotal: £112\nGround rent of £213.28 is due"
    assert amount_in_text(2800, text)            # thousands separator, no pence
    assert amount_in_text(2688.00, text)
    assert amount_in_text(213.28, text)
    assert not amount_in_text(280, text)         # not a prefix of 2800
    assert not amount_in_text(2912, text)        # invented / computed number
    assert not amount_in_text(13.28, text)       # not a suffix of 213.28


def test_property_and_workbook_routing():
    root = Path("/x/FA")
    props = {"HouseA": {"owner": "D"}, "HouseC": {"owner": "Q"}}
    assert property_for(root / "UK Properties/HouseA/bills/INV 1.pdf", root, props) == ("HouseA", "D")
    assert property_for(root / "UK insurance/policy.pdf", root, props) == ("", "")
    cfg = {"workbooks": {"2025-26": "A/SA_2025-26.xlsx"}, "default_workbook": "SA_{ty}.xlsx",
           "earliest_tax_year": "2025-26"}
    assert workbook_for("2024-25", cfg) == "A/SA_2025-26.xlsx"   # earlier years → earliest workbook
    assert workbook_for("2025-26", cfg) == "A/SA_2025-26.xlsx"
    assert workbook_for("2026-27", cfg) == "SA_2026-27.xlsx"


def test_clean_items_fixes_seen_model_errors():
    from services.fa_workings.sa_update import clean_items
    stmt = "Rent £2,075.00\nLetting fees £166.00\nPayment to Landlord £1,566.62\nRenewal Commission £2688"
    items = [{"amount": 2075, "category": "rent", "direction": "income", "description": "Rent"},
             {"amount": 1566.62, "category": "other_cost", "direction": "cost", "description": "Payment to Landlord"},
             {"amount": 2688, "category": "other_cost", "direction": "cost", "description": "Renewal Commission (8% Renewal)"}]
    out = clean_items("landlord_statement", items, stmt)
    assert [i["amount"] for i in out] == [2075, 2688]          # net-to-landlord dropped
    assert out[1]["category"] == "agent_fee"                   # commission recategorised
    inv = "EICR £140.83 VAT £28.17 BALANCE DUE GBP £169.00"
    items = [{"amount": 140.83, "category": "repair", "direction": "cost", "description": "EICR"},
             {"amount": 28.17, "category": "repair", "direction": "cost", "description": ""}]
    out = clean_items("invoice", items, inv)
    assert len(out) == 1 and out[0]["amount"] == 169.0         # net + VAT collapsed to printed total
    nototal = "A £10.00 B £20.00"
    out = clean_items("invoice", [{"amount": 10, "direction": "cost", "description": "A"},
                                  {"amount": 20, "direction": "cost", "description": "B"}], nototal)
    assert len(out) == 2                                       # no printed total → keep lines


def test_hints_assign_property_outside_property_folders():
    root = Path("/x/FA")
    props = {"HouseA": {"owner": "D"}, "HouseB": {"owner": "Q"}}
    hints = {"POL-K1": "HouseA", "POL-Q9": "HouseB"}
    assert property_for(root / "UK insurance/POL-K1-schedule.pdf", root, props, "", hints) == ("HouseA", "D")
    assert property_for(root / "UK insurance/x.pdf", root, props, "Policy POL-Q9 renewal", hints) == ("HouseB", "Q")
    assert property_for(root / "UK insurance/x.pdf", root, props, "nothing", hints) == ("", "")


def test_header_lands_in_row_1_and_dup_needs_year_and_date(tmp_path, monkeypatch):
    import openpyxl
    from datetime import date as _d
    from services.fa_workings import sa_update as m
    wb = openpyxl.Workbook(); ws = wb.active; ws.title = "Q 2025-26"
    ws.append(["Item", "Date paid", "Amount £"]); ws.append(["Ground rent", "2025-07-25", 150])
    path = tmp_path / "SA_2025-26.xlsx"; wb.save(path)
    monkeypatch.setattr(m, "ROOT", tmp_path); monkeypatch.setattr(m, "STATE_DIR", tmp_path / "st")
    cfg = {"workbooks": {"2025-26": "SA_2025-26.xlsx"}, "earliest_tax_year": "2025-26"}
    rows = [m.Row("2025-26", "Q", "A", "2025-07-22", "ground_rent", "cost", 150.0, "GR", "yes", "f1", "to review"),
            m.Row("2025-26", "Q", "A", "2026-01-20", "ground_rent", "cost", 150.0, "GR", "yes", "f2", "to review"),
            m.Row("2024-25", "Q", "A", "2024-09-01", "ground_rent", "cost", 150.0, "GR", "yes", "f3", "to review")]
    m.write_rows(cfg, rows, dry_run=False)
    out = openpyxl.load_workbook(path)[m.SHEET]
    assert out["A1"].value == "Added" and out.max_row == 4
    flags = [out.cell(r, 12).value for r in (2, 3, 4)]
    assert flags[0] == "Q 2025-26!2"         # same year, within 60 days
    assert not flags[1] and not flags[2]     # 6 months later / other tax year → not flagged


def test_estimates_are_not_costs():
    from services.fa_workings.sa_update import is_estimate
    assert is_estimate("EST3433.pdf") and is_estimate("estimate_3719.pdf") and is_estimate("Estimate 1347-2893.pdf")
    assert is_estimate("x.pdf", "estimate")
    assert not is_estimate("INV7197.pdf") and not is_estimate("Rental Statement 1351.pdf")
    assert not is_estimate("Interest statement.pdf")   # 'est' inside a word is not an estimate


def test_cross_file_duplicates_marked(tmp_path, monkeypatch):
    import openpyxl
    from services.fa_workings import sa_update as m
    monkeypatch.setattr(m, "ROOT", tmp_path); monkeypatch.setattr(m, "STATE_DIR", tmp_path / "st")
    cfg = {"workbooks": {"2025-26": "W.xlsx"}, "earliest_tax_year": "2025-26"}
    r = lambda src: m.Row("2025-26", "D", "K", "2025-08-10", "agent_fee", "cost", 2688.0, "Renewal", "yes", src, "to review")
    m.write_rows(cfg, [r("a.pdf"), r("b.pdf")], dry_run=False)
    m.write_rows(cfg, [r("c.pdf")], dry_run=False)            # later run sees rows already on the sheet
    ws = openpyxl.load_workbook(tmp_path / "W.xlsx")[m.SHEET]
    status = [ws.cell(i, 14).value for i in (2, 3, 4)]
    assert status[0] == "to review" and all(s.startswith("duplicate") for s in status[1:])
