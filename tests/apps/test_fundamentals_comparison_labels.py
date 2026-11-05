# tests/apps/test_fundamentals_comparison_labels.py
"""S5 comparison table: every section's column headers must be non-empty and
unique — streamlit/pyarrow raises ValueError on duplicate column names.

Regression for 2026-11-05: header labels resolved via next(iter(pair)) hit the
_full variant first, which has no _CMP_STEM_LABEL entry → every capacity column
header collapsed to ' （万kW）' → Annual + Installed capacity crashed in prod.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "apps" / "spot-market"))


def test_every_metric_tuple_resolves_a_label():
    import fundamentals_tab as ft
    for section, pairs in ft._CMP_METRICS.items():
        for pair in pairs:
            stem = ft.stem_label_key(pair)
            assert stem in ft._CMP_STEM_LABEL, f"{section}/{pair}: no label for any variant"


def test_headers_unique_per_section():
    """Simulate the header build with an identity translator, all metrics present."""
    import fundamentals_tab as ft
    t = lambda k: k  # noqa: E731
    for section, pairs in ft._CMP_METRICS.items():
        present = {m for pair in pairs for m in pair}
        unit = t(ft._CMP_STEM_UNIT[section])
        header = [t("fund_cmp_province")]
        for mkey in ft.metrics_of(pairs, present):
            stem = ft.stem_label_key(mkey)
            if section == "demand" and stem == "demand_yoy":
                header.append(f"{t(ft._CMP_STEM_LABEL['demand_yoy'])} (%)")
            else:
                header.append(f"{t(ft._CMP_STEM_LABEL.get(stem, ''))} ({unit})"
                              if stem in ft._CMP_STEM_LABEL else f"{stem} ({unit})")
        assert len(header) == len(set(header)), f"{section}: duplicate headers {header}"
        assert all(h.strip() for h in header), f"{section}: empty header cell {header}"
