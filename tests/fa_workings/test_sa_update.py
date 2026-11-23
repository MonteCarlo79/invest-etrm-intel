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
    props = {"Kinsale": {"owner": "D"}, "Aplin": {"owner": "Q"}}
    assert property_for(root / "UK Properties/Kinsale/bills/INV 1.pdf", root, props) == ("Kinsale", "D")
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
    props = {"Kinsale": {"owner": "D"}, "Kingfisher": {"owner": "Q"}}
    hints = {"POL-K1": "Kinsale", "POL-Q9": "Kingfisher"}
    assert property_for(root / "UK insurance/POL-K1-schedule.pdf", root, props, "", hints) == ("Kinsale", "D")
    assert property_for(root / "UK insurance/x.pdf", root, props, "Policy POL-Q9 renewal", hints) == ("Kingfisher", "Q")
    assert property_for(root / "UK insurance/x.pdf", root, props, "nothing", hints) == ("", "")
