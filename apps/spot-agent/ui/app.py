"""
China Spot Power Prices — Streamlit dashboard.

Data source: spot_daily table (populated by spot_ingest.py).
Displays daily DA / RT price summaries (avg / max / min) per province.

NOTE: Hourly data is NOT supported.  The PDFs store hourly prices as chart images;
table-based hourly extraction has not been implemented.
"""

import datetime as dt

import pandas as pd
import requests
import streamlit as st

# ===== Config =====
API_BASE = "http://127.0.0.1:8899"
session = requests.Session()
session.trust_env = False
session.proxies = {}

st.set_page_config(page_title="China Spot Power Prices", layout="wide")

# ===== Helpers =====

@st.cache_data(show_spinner=False, ttl=120)
def fetch_daily(province: str, start: str, end: str) -> pd.DataFrame:
    """Fetch daily DA/RT summaries from /v1/spot/daily."""
    params = {"province": province, "start": start, "end": end}
    r = session.get(f"{API_BASE}/v1/spot/daily", params=params, timeout=60)
    r.raise_for_status()
    df = pd.DataFrame(r.json() or [])
    if not df.empty and "report_date" in df.columns:
        df["report_date"] = pd.to_datetime(df["report_date"])
    return df


def normalize_dates(start_date: dt.date, end_date: dt.date) -> tuple[dt.date, dt.date]:
    return (end_date, start_date) if end_date < start_date else (start_date, end_date)


# ===== UI =====

st.markdown("## China Spot Power Prices")
st.caption("Daily DA/RT price summaries — populated by spot_ingest.py")

col_prov, col_start, col_end = st.columns([2, 2, 2])

with col_prov:
    st.markdown("**Province**")
    province_options = [
        "Shanxi", "Shandong", "Gansu", "HebeiSouth", "Hubei",
        "Mengxi", "Zhejiang", "Shaanxi", "Anhui", "Liaoning",
        "Heilongjiang", "Jiangsu", "Jilin", "Fujian", "Henan",
        "Ningxia", "Jiangxi", "Xinjiang", "Sichuan", "Mengdong",
        "Hunan", "Shanghai", "Qinghai", "Chongqing", "Guangdong",
        "South", "National",
    ]
    province = st.selectbox("", province_options, index=province_options.index("Shanxi"))

with col_start:
    st.markdown("**Start date**")
    start_date = st.date_input("", value=dt.date(2025, 10, 1), key="start_date")

with col_end:
    st.markdown("**End date**")
    end_date = st.date_input("", value=dt.date(2025, 10, 31), key="end_date")

st.write("")
load = st.button("Load data", type="primary")
st.write("")

if load:
    start_date, end_date = normalize_dates(start_date, end_date)

    try:
        df = fetch_daily(province, start_date.isoformat(), end_date.isoformat())

        if df.empty:
            st.info(
                "No data for this selection. "
                "Run spot_ingest.py to populate the database, "
                "or adjust the date range / province."
            )
        else:
            # Rename for display
            display_cols = {
                "report_date": "Date",
                "province":    "Province",
                "province_cn": "省份",
                "da_avg":      "DA Avg",
                "da_max":      "DA Max",
                "da_min":      "DA Min",
                "rt_avg":      "RT Avg",
                "rt_max":      "RT Max",
                "rt_min":      "RT Min",
                "highlights":  "Highlights",
                "source_file": "Source PDF",
            }
            out = df[[c for c in display_cols if c in df.columns]].rename(columns=display_cols)
            st.dataframe(out.sort_values("Date").reset_index(drop=True),
                         use_container_width=True)

            # Simple line chart for RT avg
            if "RT Avg" in out.columns and out["RT Avg"].notna().any():
                st.markdown("**RT Average Price (CNY/MWh)**")
                chart_df = out[["Date", "RT Avg"]].dropna().set_index("Date")
                st.line_chart(chart_df)

    except requests.exceptions.ConnectionError:
        st.error(
            f"Cannot connect to API at {API_BASE}. "
            "Start the API server: `uvicorn api.main:app --port 8899`"
        )
    except requests.exceptions.RequestException as e:
        st.error(f"API error: {e}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")
