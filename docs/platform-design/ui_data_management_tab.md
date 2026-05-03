# UI Pattern: Data Management Tab

Reusable Streamlit pattern for scanning a folder of PDF reports, comparing
against DB coverage, and running selective or full ingestion — with optional
extra processing steps.

**Reference implementation:** `apps/spot-market/app.py` → `tab_mgmt`

---

## Overview

The tab gives operators a self-service way to:

1. See which PDFs exist on disk and what date ranges they cover
2. Identify which dates are missing from (or partial in) the database
3. Choose a run mode and trigger ingestion without touching the CLI
4. Optionally chain extra processing steps (e.g. inter-provincial parsing, AI summaries)

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Controls row                                       │
│   • Year selector                                   │
│   • Mode radio: Fill gaps | Backfill date range     │
│   • Extra-step checkboxes (optional)                │
│   • Date range pickers (start / end)                │
└─────────────────────────────────────────────────────┘
           │ drives
           ▼
┌─────────────────────────────────────────────────────┐
│  Gap analysis table                                 │
│   _scan_pdf_inventory()  ← scans PDF folder        │
│   _db_coverage_detail()  ← queries DB              │
│   joins on date → Missing / Partial / OK status    │
└─────────────────────────────────────────────────────┘
           │ feeds
           ▼
┌─────────────────────────────────────────────────────┐
│  Action button                                      │
│   dynamically labelled with PDF count              │
│   disabled when nothing to do                      │
└─────────────────────────────────────────────────────┘
           │ on click
           ▼
┌─────────────────────────────────────────────────────┐
│  Ingestion loop (per PDF)                           │
│   st.progress() bar                                │
│   core step (parse + upsert)                       │
│   optional steps (interprov, AI, etc.)             │
│   results list → st.dataframe()                    │
│   cache.clear() + st.rerun()                       │
└─────────────────────────────────────────────────────┘
```

---

## Key Functions

### `_scan_pdf_inventory(year) → list[tuple]`

Scans `data/spot reports/{year}/*.pdf`, extracts date range from each filename,
returns `[(filename, start_date, end_date, Path), …]`.

**Filename parsing** is the only domain-specific part — swap `_parse_pdf_date_range()`
for any function that maps a filename stem to `(start_date, end_date)`.

```python
@st.cache_data(ttl=60, show_spinner=False)
def _scan_pdf_inventory(year: int = 2026):
    data_dir = _REPO / "data" / "spot reports" / str(year)
    pdfs = []
    for p in sorted(data_dir.glob("*.pdf")):
        date_range = _parse_pdf_date_range(p.stem, year)
        if date_range:
            pdfs.append((p.name, date_range[0], date_range[1], p))
    return pdfs
```

### `_db_coverage_detail(year) → dict[date, tuple[int,int]]`

Returns `{date: (da_count, rt_count)}` for every date in the year.
Adapt the query to whatever completeness columns your schema has.

```python
@st.cache_data(ttl=30, show_spinner=False)
def _db_coverage_detail(year: int = 2026):
    cur = _conn().cursor()
    cur.execute("""
        SELECT report_date::date, COUNT(da_avg), COUNT(rt_avg)
        FROM spot_daily
        WHERE report_date BETWEEN %s AND %s
        GROUP BY 1
    """, (date(year, 1, 1), date(year, 12, 31)))
    return {r[0]: (r[1], r[2]) for r in cur.fetchall()}
```

For a simpler schema (just presence/absence), replace with:
```python
return {r[0]: True for r in cur.fetchall()}
```

---

## Run Modes

| Mode | PDFs selected |
|---|---|
| **Fill gaps** | PDFs where ≥ 1 date in range is absent from DB |
| **Backfill date range** | All PDFs overlapping the selected date range |

```python
if mgmt_mode.startswith("Fill gaps"):
    pdfs_to_run = [
        (fname, s, e, path) for fname, s, e, path in relevant_pdfs
        if any(
            s + timedelta(days=i) not in existing_dates
            for i in range((e - s).days + 1)
            if bf_start <= s + timedelta(days=i) <= bf_end
        )
    ]
else:
    pdfs_to_run = relevant_pdfs
```

---

## Optional Steps Pattern

Add checkboxes in the controls column for any processing that should be
optional (API costs, slow operations, etc.):

```python
run_interprov = st.checkbox("Parse 省间现货交易 data", value=True, key="mgmt_interprov")
run_ai        = st.checkbox("Generate AI summaries",  value=False, key="mgmt_ai",
                            help="Requires ANTHROPIC_API_KEY")
```

Inside the loop, guard each optional step:
```python
if run_interprov:
    interprov_rows = _parse_interprov(path, pdf_year)
    if interprov_rows:
        interprov_count = _upsert_interprov_rows(interprov_rows)

if run_ai:
    for rdate in sorted(parsed.keys()):
        summary = _gen_summary(rdate, day_prices, day_interprov, fname)
        if summary:
            _upsert_summary(summary)
            ai_count += 1
```

---

## Ingestion Loop Pattern

```python
total    = len(pdfs_to_run)
progress = st.progress(0, text="Starting…")
results  = []

for i, (fname, s, e, path) in enumerate(pdfs_to_run):
    progress.progress(i / total, text=f"Parsing {fname}…")
    try:
        # core step
        n = core_ingest(path)
        # optional steps — update progress mid-PDF if slow
        progress.progress((i + 0.5) / total, text=f"Extra step: {fname}…")
        extra_n = optional_step(path)
        results.append({"PDF": fname, "Rows": n, "Extra": extra_n, "Error": ""})
    except Exception as exc:
        results.append({"PDF": fname, "Rows": 0, "Extra": 0, "Error": str(exc)[:120]})

progress.progress(1.0, text="Done.")
st.success(f"Complete — {total} PDF(s) processed.")
st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)
```

---

## Cache Invalidation

Always clear relevant caches and `st.rerun()` after ingestion so charts
pick up the new data immediately:

```python
load_all.clear()
load_kpis.clear()
_db_coverage.clear()
_db_coverage_detail.clear()
st.rerun()
```

---

## Status Table Styling

```python
inv_df.style.apply(
    lambda col: [
        "background-color: #ffe0e0" if v == "Missing"
        else "background-color: #fff3cd" if v == "Partial"
        else "background-color: #d4edda"
        for v in col
    ],
    subset=["Status"],
)
```

---

## Adapting to a New App

1. **Change the PDF folder path** in `_scan_pdf_inventory`
2. **Change `_parse_pdf_date_range`** to match your filename convention
3. **Change `_db_coverage_detail`** to query your target table + completeness columns
4. **Replace `core_ingest`** with your parse + upsert functions
5. **Add or remove optional-step checkboxes** as needed
6. Keep `cache.clear()` calls in sync with whatever `@st.cache_data` functions your app uses
