"""Sign contract of load_ops_dispatch_15min (2026-11-28).

The WeCom nomination/actual tables hold positive = discharge. The loader's
contract — shared with the legacy ops table — is negative = discharge, because
_enrich_context_with_ops_dispatch negates once into the LP convention
(positive = discharge). A violation inverted every nominated / cleared_actual
scenario P&L computed from the WeCom tables.
"""
from datetime import date

import pandas as pd
import pytest

from libs.decision_models.resources import bess_context
from libs.decision_models.workflows.daily_strategy_report import (
    _enrich_context_with_ops_dispatch,
)

D = date(2026, 11, 24)


def _wecom_rows():
    # slot 1 = discharging 100 MW, slot 2 = charging 40 MW (table convention: + = discharge)
    return pd.DataFrame({"data_date": [D, D], "slot": [1, 2],
                         "nominated_mw": [100.0, -40.0], "actual_mw": [80.0, -20.0]})


def test_wecom_source_is_inverted_to_the_ops_file_sign(monkeypatch):
    monkeypatch.setattr(bess_context, "_run_query_safe", lambda sql, params=None: (_wecom_rows(), None))
    df, _ = bess_context.load_ops_dispatch_15min("gushanliang", D, D)
    assert list(df["nominated_dispatch_mw"]) == [-100.0, 40.0]     # negative = discharge
    assert list(df["actual_dispatch_mw"]) == [-80.0, 20.0]


def test_discharge_stays_discharge_through_the_enrich_step(monkeypatch):
    """End to end: table +100 MW (discharge) must end as +25 MWh in the LP
    convention that _calc_15min_pnl and the volume split read."""
    monkeypatch.setattr(bess_context, "_run_query_safe", lambda sql, params=None: (_wecom_rows(), None))
    import libs.decision_models.workflows.daily_strategy_report as dsr
    monkeypatch.setattr(dsr, "load_ops_dispatch_15min", bess_context.load_ops_dispatch_15min)
    ctx, ok = _enrich_context_with_ops_dispatch({"asset_code": "gushanliang"}, D)
    assert ok is True
    nom = [r["dispatch_mw"] for r in ctx["nominated_dispatch_15min"]]
    act = [r["dispatch_mw"] for r in ctx["actual_dispatch_15min"]]
    assert nom == pytest.approx([25.0, -10.0])     # + discharge MWh, - charge MWh
    assert act == pytest.approx([20.0, -5.0])


def test_legacy_ops_table_sign_is_untouched(monkeypatch):
    calls = []

    def fake(sql, params=None):
        calls.append(sql)
        if "bess_nomination_96" in sql:
            return pd.DataFrame(), None                      # WeCom tables empty -> fallback
        return pd.DataFrame({"interval_start": ["2026-04-10 00:00:00"],
                             "nominated_dispatch_mw": [-100.0],   # legacy: negative = discharge
                             "actual_dispatch_mw": [-90.0],
                             "nodal_price_excel": [300.0]}), None

    monkeypatch.setattr(bess_context, "_run_query_safe", fake)
    df, _ = bess_context.load_ops_dispatch_15min("suyou", date(2026, 4, 10), date(2026, 4, 10))
    assert any("ops_bess_dispatch_15min" in c for c in calls)
    assert list(df["nominated_dispatch_mw"]) == [-100.0]
    assert list(df["actual_dispatch_mw"]) == [-90.0]
