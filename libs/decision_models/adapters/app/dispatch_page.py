"""
libs/decision_models/adapters/app/dispatch_page.py

Streamlit page wrapper for bess_dispatch_optimization.

Drop this into any Streamlit app page:
    from libs.decision_models.adapters.app.dispatch_page import render_dispatch_page
    render_dispatch_page()

The widget collects battery parameters and a price curve, then calls the
shared model runner. No LP / optimisation logic lives here.
"""
from __future__ import annotations

from typing import List

import libs.decision_models.bess_dispatch_optimization  # noqa: F401 — register model
from libs.decision_models.runners.local import run


def render_dispatch_page() -> None:
    import pandas as pd
    import streamlit as st

    st.header("BESS Dispatch Optimisation — Perfect Foresight")
    st.caption("Source: services/bess_map/optimisation_engine.optimise_day()")

    col1, col2, col3 = st.columns(3)
    power_mw = col1.number_input("Power (MW)", value=100.0, min_value=0.1, step=10.0)
    duration_h = col2.number_input("Duration (h)", value=2.0, min_value=0.1, step=0.5)
    roundtrip_eff = col3.number_input(
        "Round-trip efficiency", value=0.85, min_value=0.1, max_value=1.0, step=0.01
    )

    with st.expander("Optional degradation constraints"):
        col4, col5 = st.columns(2)
        raw_tp = col4.text_input("Max throughput MWh/day (leave blank = no cap)", value="")
        raw_cyc = col5.text_input("Max cycles/day (leave blank = no cap)", value="")
        max_throughput = float(raw_tp) if raw_tp.strip() else None
        max_cycles = float(raw_cyc) if raw_cyc.strip() else None

    st.markdown(
        "**24 hourly prices** (comma-separated, one row = one day; "
        "currently only the first row is used):"
    )
    raw = st.text_area(
        "Price series",
        height=80,
        placeholder="39.5,33.1,30.0,...,34.1  (24 values)",
    )

    if st.button("Run optimisation") and raw.strip():
        try:
            # Parse first row only
            first_row = raw.strip().splitlines()[0]
            prices_24: List[float] = [float(x.strip()) for x in first_row.split(",")]

            result = run("bess_dispatch_optimization", {
                "prices_24": prices_24,
                "power_mw": power_mw,
                "duration_h": duration_h,
                "roundtrip_eff": roundtrip_eff,
                "max_throughput_mwh": max_throughput,
                "max_cycles_per_day": max_cycles,
            })

            st.success(
                f"Solver: **{result['solver_status']}** | "
                f"Daily profit: **{result['profit']:,.1f}** | "
                f"Battery capacity: **{result['energy_capacity_mwh']:.1f} MWh**"
            )

            hours = list(range(24))
            df = pd.DataFrame({
                "hour": hours,
                "price": prices_24,
                "charge_mw": result["charge_mw"],
                "discharge_mw": result["discharge_mw"],
                "dispatch_grid_mw": result["dispatch_grid_mw"],
                "soc_mwh": result["soc_mwh"],
            })
            st.dataframe(df, use_container_width=True)

            st.line_chart(df.set_index("hour")[["charge_mw", "discharge_mw", "soc_mwh"]])

        except ValueError as exc:
            st.error(f"Input error: {exc}")
        except Exception as exc:
            st.error(f"Optimisation failed: {exc}")
