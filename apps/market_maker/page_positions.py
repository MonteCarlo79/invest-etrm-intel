"""Positions page: full book grouped by book, source badges, CSV download."""
from __future__ import annotations

import streamlit as st

from apps.market_maker.data import book_name_map, month_label, selected_positions

_BADGE = {"physical": "blue", "bilateral": "orange", "manual": "green"}
_COLS = ["delivery_month", "province", "tou_block", "direction", "volume_mwh", "price_cny_mwh",
         "source", "basis", "instrument_type", "channel"]


def render_positions(engine, sel: dict) -> None:
    st.subheader("Positions")
    pos = selected_positions(engine, sel)
    if pos.empty:
        st.info("No positions for the selected books and provinces.")
        return
    names = book_name_map(sel)
    st.caption(f"{len(pos):,} rows (month pieces) across {pos['book_id'].nunique()} books. "
               "Load-book physical rows are computed in memory from active retail contracts.")
    for bid, g in pos.groupby("book_id", sort=True):
        counts = g["source"].value_counts()
        badges = " ".join(f":{_BADGE.get(s, 'gray')}-badge[{s} · {n}]" for s, n in counts.items())
        st.markdown(f"**{bid} · {names.get(int(bid), '?')}** &nbsp; {badges}")
        view = g[_COLS].copy()
        view["delivery_month"] = view["delivery_month"].map(month_label)
        view.loc[view["source"] != "physical", "basis"] = ""
        st.dataframe(view.sort_values(["delivery_month", "province", "tou_block"]),
                     hide_index=True, width="stretch",
                     column_config={"volume_mwh": st.column_config.NumberColumn("MWh", format="%.1f"),
                                    "price_cny_mwh": st.column_config.NumberColumn("¥/MWh", format="%.1f")})
    out = pos.assign(book_name=pos["book_id"].map(lambda b: names.get(int(b), "")))
    st.download_button("Download CSV", out.to_csv(index=False).encode("utf-8-sig"),
                       file_name=f"mm_positions_{sel['as_of']}.csv", mime="text/csv")
