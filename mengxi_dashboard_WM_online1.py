# -*- coding: utf-8 -*-
"""
Created on Sat May 17 21:23:59 2025

@author: dipeng.chen
"""

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from io import BytesIO
from fpdf import FPDF
from bess_config import *

# ─── Page config & CSS ─────────────────────────────────────────────────────────
st.set_page_config(page_title="Mengxi Revenue Dashboard", layout="wide")
st.markdown(
    """
    <style>
      /* Hide Streamlit row index */
      div[data-testid="stDataFrame"] table th:first-child,
      div[data-testid="stTable"] table th:first-child {display: none;}
      div[data-testid="stDataFrame"] table td:first-child,
      div[data-testid="stTable"] table td:first-child {display: none;}
    </style>
    """,
    unsafe_allow_html=True,
)
st.title("🔋 Mengxi Revenue Dashboard")

# ─── Helpers ───────────────────────────────────────────────────────────────────
FONT_NAME = "SimHei"
FONT_FILE = r"C:\Windows\Fonts\msyh.ttc"  # Full path to the SimHei font on Windows
FONT_SIZE = 8

# Style function for percent columns
def style_pct(val):
    if isinstance(val, str) and val.endswith("%"):
        num = int(val.rstrip("%"))
        if num > 70:
            return "color: green"
        if num > 50:
            return "color: orange"
        return "color: red"
    return ""

# Generate PNG from DataFrame table
def df_to_png(df: pd.DataFrame) -> BytesIO:
    fig, ax = plt.subplots(
        figsize=(len(df.columns) * 1.5, max(2, len(df)) * 0.5),
        tight_layout=True
    )
    ax.axis("off")
    tbl = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc="center",
        loc="center"
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(10)
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf



# ─── Data Processing ───────────────────────────────────────────────────────────

def load_and_process(csv_path: str, date_col: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = pd.read_csv(csv_path, parse_dates=[date_col])
    # df.to_csv('output.csv') 
    df = df[df[date_col] >= pd.to_datetime("2025-02-01")].copy()

    # convert revenue
    rev_cols = ["actual_revenue", "nominated_revenue", "strategy_revenue", "optimal_revenue"]
    df[rev_cols] = (df[rev_cols] / 1e4).round(0).astype("Int64")

    # Add actual_discharge, formatting as integer with comma
    if "actual_discharge" in df.columns:
        # ensure integer format
        df["discharged volume in MWh"] = df["actual_discharge"].fillna(0).astype(int)
    else:
        df["discharged volume in MWh"] = None

    subsidy_col = "Subsidies"
    df[subsidy_col] = pd.to_numeric(df[subsidy_col], errors="coerce").fillna(0)

# --- Compute new actual_revenue (subsidy revenue) ---
    # Must have "discharged volume in MWh" and "Subsidies" as integers (not strings)
    # _discharged = pd.to_numeric(df["discharged volume in MWh"], errors='coerce').fillna(0)

    numeric_discharged = pd.to_numeric(df["discharged volume in MWh"], errors="coerce").fillna(0)
    numeric_cycles = pd.to_numeric(df["actual_avg_daily_cycles"], errors="coerce").fillna(0)
    
    # _subsidy = pd.to_numeric(df["Subsidies"], errors='coerce').fillna(0)
    # _subsidy.to_csv('output.csv') 
    df["subsidy_revenue"] = (numeric_discharged * df[subsidy_col] / 1e4).round(0).astype("Int64")   # 万元 integer
    
    
    

    # Rename original "actual_revenue" to "market_revenue"
    df.rename(columns={"actual_revenue": "market_revenue"}, inplace=True)


# actual_revenue: sum of market_revenue and subsidy_revenue
    df["actual_revenue"] = (df["market_revenue"] + df["subsidy_revenue"]).round(0).astype("Int64")


    # Revenue
    # rev = df[[date_col] + rev_cols + ["discharged volume in MWh"]].reset_index(drop=True)
    
    # Build revenue table columns in desired order:
    rev_cols_order = [
        date_col, "actual_revenue", "market_revenue", "subsidy_revenue",
        "nominated_revenue", "strategy_revenue", "optimal_revenue", "discharged volume in MWh"
    ]
    rev = df[rev_cols_order].reset_index(drop=True)
    # rev.rename(columns={"subsidy_revenue": "actual_revenue"}, inplace=True)
    
    int_cols = [
    "actual_revenue", "market_revenue", "subsidy_revenue",
    "nominated_revenue", "strategy_revenue", "optimal_revenue"
    ]
    for col in int_cols:
        if col in rev.columns:
            rev[col] = rev[col].apply(lambda x: int(x) if pd.notna(x) else "")

    rev["actual/optimal"] = (
        (rev["actual_revenue"] / rev["optimal_revenue"] * 100)
        .round(0).astype("Int64").astype(str) + "%"
    )
    rev["actual/nominated"] = rev.apply(
        lambda r: f"{round(r['actual_revenue']/r['nominated_revenue']*100)}%" 
        if pd.notna(r['nominated_revenue']) and r['nominated_revenue']>0 else None,
        axis=1
    )

    # Format discharged volume as integer with thousands separator for display
    rev["discharged volume in MWh"] = rev["discharged volume in MWh"].map(lambda x: f"{x:,d}" if pd.notna(x) else "")

    # --- Spread table logic, with cycle spread columns ---
    cycles_numeric = pd.to_numeric(df["actual_avg_daily_cycles"], errors="coerce").fillna(0)
    spr = df[[date_col, "actual_spread", "nominated_spread", "strategy_spread", "optimal_spread", "Subsidies"]].copy()
    spread_cols = ["actual_spread", "nominated_spread", "strategy_spread", "optimal_spread"]
    spr[spread_cols] = spr[spread_cols].round(0).astype("Int64")
    spr["Subsidies"] = spr["Subsidies"].round(0).astype("Int64")
    # Rename actual_spread to market_spread, create new actual_spread as market_spread + Subsidies
    spr.rename(columns={"actual_spread": "market_spread"}, inplace=True)
    spr["actual_spread"] = (spr["market_spread"] + spr["Subsidies"]).round(0).astype("Int64") 

    # Move actual_spread to first column
    cols = [date_col, "actual_spread", "market_spread", "Subsidies", "nominated_spread", "strategy_spread", "optimal_spread"]
    spr = spr[cols]

    # Compute unit cycle spread and expected unit cycle spread
    spr["unit cycle spread"] = (spr["actual_spread"] * cycles_numeric).round(0).astype("Int64")
    spr["expected unit cycle spread"] = ((spr["market_spread"] + 350) * cycles_numeric).round(0).astype("Int64")

    # spr["actual/optimal"] = (
    #     (spr["actual_spread"] / spr["optimal_spread"] * 100)
    #     .round(0).astype("Int64").astype(str) + "%"
    # )
    # spr["actual/nominated"] = spr.apply(
    #     lambda r: f"{round(r['actual_spread']/r['nominated_spread']*100)}%"
    #     if pd.notna(r['nominated_spread']) and r['nominated_spread'] > 0 else None,
    #     axis=1
    # )

    # Cycles
    cyc = df[[date_col, "actual_avg_daily_cycles", "nominated_avg_daily_cycles", "strategy_avg_daily_cycles", "optimal_avg_daily_cycles"]].copy()
    for c in cyc.columns:
        if c != date_col:
            cyc[c] = cyc[c].map(lambda x: f"{x:.1f}" if pd.notna(x) else None)
    cyc["actual/optimal"] = cyc.apply(
        lambda r: f"{round(float(r['actual_avg_daily_cycles'])/float(r['optimal_avg_daily_cycles'])*100)}%" 
        if pd.notna(r['optimal_avg_daily_cycles']) and float(r['optimal_avg_daily_cycles'])>0 else None,
        axis=1
    )
    cyc["actual/nominated"] = cyc.apply(
        lambda r: f"{round(float(r['actual_avg_daily_cycles'])/float(r['nominated_avg_daily_cycles'])*100)}%" 
        if pd.notna(r['nominated_avg_daily_cycles']) and float(r['nominated_avg_daily_cycles'])>0 else None,
        axis=1
    )

    # Efficiency
    eff = df[[date_col, "actual_efficiency"]].copy()
    eff["actual_efficiency"] = (eff["actual_efficiency"] * 100).round(1).astype(str) + "%"
    
    # print(rev.columns)
    # assert rev.columns.is_unique, "Revenue table columns are not unique!"
    
    return rev, spr, cyc, eff, numeric_discharged, numeric_cycles


import numpy as np

def add_avg_sum_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add average and sum rows to the end of the DataFrame.
    Leaves date and string columns blank.
    """
    num_cols = df.select_dtypes(include=[np.number]).columns
    avg_row = {col: df[col].mean().round(1) if col in num_cols else "" for col in df.columns}
    sum_row = {col: df[col].sum().round(1) if col in num_cols else "" for col in df.columns}
    avg_row[df.columns[0]] = "平均"  # or "Average"
    sum_row[df.columns[0]] = "合计"  # or "Sum"
    return pd.concat([df, pd.DataFrame([avg_row, sum_row])], ignore_index=True)


def add_avg_row(df: pd.DataFrame) -> pd.DataFrame:
    num_cols = df.select_dtypes(include=[np.number]).columns
    avg_row = {col: df[col].mean().round(1) if col in num_cols else "" for col in df.columns}
    avg_row[df.columns[0]] = "平均"
    return pd.concat([df, pd.DataFrame([avg_row])], ignore_index=True)


# def add_avg_row(df: pd.DataFrame) -> pd.DataFrame:
#     num_cols = df.select_dtypes(include=[np.number]).columns
#     avg_row = {}
#     for col in df.columns:
#         if col in num_cols:
#             avg_row[col] = df[col].mean().round(1)
#         else:
#             avg_row[col] = ""
#     avg_row[df.columns[0]] = "平均"
#     return pd.concat([df, pd.DataFrame([avg_row])], ignore_index=True)

def add_avg_sum_rows_revenue(df: pd.DataFrame, original_discharge_col: pd.Series) -> pd.DataFrame:
    num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    # Add 'discharged volume in MWh' explicitly if missing
    if "discharged volume in MWh" not in num_cols and "discharged volume in MWh" in df.columns:
        num_cols.append("discharged volume in MWh")
    avg_row = {}
    sum_row = {}
    for col in df.columns:
        if col == "discharged volume in MWh":
            avg_val = original_discharge_col.mean().round(1)
            sum_val = original_discharge_col.sum().round(1)
            # Format as int with thousand separator for display
            avg_row[col] = f"{int(round(avg_val)):,d}"
            sum_row[col] = f"{int(round(sum_val)):,d}"
        elif pd.api.types.is_numeric_dtype(df[col]):
            avg_row[col] = df[col].mean().round(1)
            sum_row[col] = df[col].sum().round(1)
        else:
            avg_row[col] = ""
            sum_row[col] = ""
    avg_row[df.columns[0]] = "平均"
    sum_row[df.columns[0]] = "合计"
    return pd.concat([df, pd.DataFrame([avg_row, sum_row])], ignore_index=True)


def add_avg_row_cycles(df: pd.DataFrame, numeric_cols: dict) -> pd.DataFrame:
    """
    Adds an average row to df.
    numeric_cols: dict of {colname: numeric pd.Series}
    """
    avg_row = {}
    for col in df.columns:
        if col in numeric_cols:
            avg_row[col] = round(numeric_cols[col].mean(), 2)
        elif pd.api.types.is_numeric_dtype(df[col]):
            avg_row[col] = round(df[col].mean(), 2)
        else:
            avg_row[col] = ""
    avg_row[df.columns[0]] = "平均"
    return pd.concat([df, pd.DataFrame([avg_row])], ignore_index=True)


# def add_avg_row_cycles(df: pd.DataFrame, original_cycles_col: pd.Series) -> pd.DataFrame:
#     avg_row = {}
#     for col in df.columns:
#         if col == "actual_avg_daily_cycles":
#             avg_val = original_cycles_col.mean().round(2)
#             avg_row[col] = f"{avg_val:.2f}"
#         elif pd.api.types.is_numeric_dtype(df[col]):
#             avg_row[col] = df[col].mean().round(2)
#         else:
#             avg_row[col] = ""
#     avg_row[df.columns[0]] = "平均"
#     return pd.concat([df, pd.DataFrame([avg_row])], ignore_index=True)



# ─── Load datasets ─────────────────────────────────────────────────────────────
# Weekly
suw_rev_w, suw_spr_w, suw_cyc_w, suw_eff_w, suw_discharged_w, suw_cycles_w = load_and_process("mengxi_suyouweekly_revenue.csv", "week_start")
wul_rev_w, wul_spr_w, wul_cyc_w, wul_eff_w, wul_discharged_w, wul_cycles_w = load_and_process("mengxi_wulateweekly_revenue.csv", "week_start")
suw_rev_m, suw_spr_m, suw_cyc_m, suw_eff_m, suw_discharged_m, suw_cycles_m = load_and_process("mengxi_suyoumonthly_revenue.csv", "month_start")
wul_rev_m, wul_spr_m, wul_cyc_m, wul_eff_m, wul_discharged_m, wul_cycles_m = load_and_process("mengxi_wulatemonthly_revenue.csv", "month_start")




# # For revenue table:
# rev_disp = add_avg_sum_rows(rev)

# # For spread/cycle tables:
# spr_disp = add_avg_row(spr)
# cyc_disp = add_avg_row(cyc)


# ─── Display ───────────────────────────────────────────────────────────────────
tab_su, tab_wu = st.tabs(["🟢 蒙西 苏右", "🔵 蒙西 乌拉特"])

for label, tab, rev_w, spr_w, cyc_w, eff_w, cycles_w, discharge_w, rev_m, spr_m, cyc_m, eff_m, cycles_m, discharge_m in [
    ("苏右", tab_su, suw_rev_w, suw_spr_w, suw_cyc_w, suw_eff_w, suw_cycles_w, suw_discharged_w, suw_rev_m, suw_spr_m, suw_cyc_m, suw_eff_m, suw_cycles_m, suw_discharged_m),
    ("乌拉特", tab_wu, wul_rev_w, wul_spr_w, wul_cyc_w, wul_eff_w, wul_cycles_w, wul_discharged_w, wul_rev_m, wul_spr_m, wul_cyc_m, wul_eff_m, wul_cycles_m, wul_discharged_m),
]:
    with tab:
        sub_weekly, sub_monthly = st.tabs(["周汇总", "月汇总"])
        # Weekly
        with sub_weekly:
            st.subheader(f"Revenue (万元) — {label} — 周")
            rev_disp = add_avg_sum_rows_revenue(rev_w, discharge_w) 
            # rev_disp = add_avg_sum_rows(rev_w)
            
            # Revenue Table (pass numeric discharged column!)
            int_cols = [
                "actual_revenue", "market_revenue", "subsidy_revenue",
                "nominated_revenue", "strategy_revenue", "optimal_revenue"
            ]
            styled = rev_disp.style.format({col: "{:.0f}" for col in int_cols}).map(
                style_pct, subset=["actual/optimal", "actual/nominated"]
            )
            st.dataframe(styled, use_container_width=True)

            st.download_button(
                "📥 Revenue PNG",
                df_to_png(rev_w),
                mime="image/png",
                key=f"{label}_week_rev_png"
            )



            # Weekly Spread
            spread_cols = ["unit cycle spread", "expected unit cycle spread"]
            
            spr_disp = add_avg_row(spr_w)
            st.subheader("Spread — 周（元/MWh)")
            spread_int_cols = ["actual_spread", "market_spread", "Subsidies", "nominated_spread", "strategy_spread", "optimal_spread", "unit cycle spread", "expected unit cycle spread"]
            styled = spr_disp.style.format({col: "{:.0f}" for col in spread_int_cols if col in spr_disp.columns})
            st.dataframe(styled, use_container_width=True)
            
            st.download_button(
                "📥 Spread PNG",
                df_to_png(spr_w),
                mime="image/png",
                key=f"{label}_week_spread_png"
            )
 



            # --- Compute numeric averages for all cycles columns before formatting ---
            cycle_numeric_dict = {
                "actual_avg_daily_cycles": cyc_w["actual_avg_daily_cycles"].astype(float),
                "nominated_avg_daily_cycles": cyc_w["nominated_avg_daily_cycles"].astype(float),
                "strategy_avg_daily_cycles": cyc_w["strategy_avg_daily_cycles"].astype(float),
                "optimal_avg_daily_cycles": cyc_w["optimal_avg_daily_cycles"].astype(float)
            }
            cyc_disp = add_avg_row_cycles(cyc_w, cycle_numeric_dict)
            
            # --- Format as string for display, except the last row (average) ---
            cycle_cols = [col for col in cyc_disp.columns if col != cyc_disp.columns[0]]
            for col in cycle_cols:
                cyc_disp.loc[:-1, col] = cyc_disp.loc[:-1, col].map(lambda x: f"{x:.1f}" if pd.notna(x) and x != "" else "")
            
            st.subheader("Cycles — 周(次/天)")
            st.dataframe(cyc_disp, use_container_width=True)



            st.download_button(
                "📥 Cycles PNG",
                df_to_png(cyc_w),
                mime="image/png",
                key=f"{label}_week_cycle_png"
            )

   

            st.subheader("Efficiency — 周")
            st.dataframe(eff_w, use_container_width=True)

            st.download_button(
                "📥 Efficiency PNG",
                df_to_png(eff_w),
                mime="image/png",
                key=f"{label}_week_efficiency_png"
            )



        # Monthly
        with sub_monthly:
            st.subheader(f"Revenue (万元) — {label} — 月")
            rev_disp = add_avg_sum_rows_revenue(rev_m, discharge_m)
            int_cols = [
                "actual_revenue", "market_revenue", "subsidy_revenue",
                "nominated_revenue", "strategy_revenue", "optimal_revenue"
            ]
            styled = rev_disp.style.format({col: "{:.0f}" for col in int_cols}).map(
                style_pct, subset=["actual/optimal", "actual/nominated"]
            )
            st.dataframe(styled, use_container_width=True)

            st.download_button(
                "📥 Revenue PNG",
                df_to_png(rev_m),
                mime="image/png",
                key=f"{label}_month_rev_png"
            )

 

            spread_cols = ["unit cycle spread", "expected unit cycle spread"]
            st.subheader("Spread — 月（元/MWh)")
            spread_int_cols = ["actual_spread", "market_spread", "Subsidies", "nominated_spread", "strategy_spread", "optimal_spread"]
            styled = spr_m.style.format({col: "{:.0f}" for col in spread_int_cols if col in spr_m.columns})
            st.dataframe(styled, use_container_width=True)
            
            st.download_button(
                "📥 Spread PNG",
                df_to_png(spr_m),
                mime="image/png",
                key=f"{label}_month_spread_png"
            )

            st.subheader("Cycles — 月(次/天)")
            styled = cyc_m.style.map(style_pct, subset=["actual/optimal","actual/nominated"])
            st.dataframe(styled, use_container_width=True)
            st.download_button(
                "📥 Cycles PNG",
                df_to_png(cyc_m),
                mime="image/png",
                key=f"{label}_month_cycle_png"
            )

     
            st.subheader("Efficiency — 月")
            st.dataframe(eff_m, use_container_width=True)
            st.download_button(
                "📥 Efficiency PNG",
                df_to_png(eff_m),
                mime="image/png",
                key=f"{label}_month_efficiency_png"
            )


