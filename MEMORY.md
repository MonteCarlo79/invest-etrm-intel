# MEMORY.md — bess-platform

Read this at the start of every session before doing anything.

---

## 2026-05-06, Spot Market App Architecture

**What was decided:** `apps/spot-market/app.py` is the canonical Pillar 1 (Market Map) app. The old `apps/spot-agent/` china-spot app is retired and deleted. The new app runs on ECS at `/spot-markets` (port 8505, ECR repo `bess-spot-markets`).

**Why:** The new app is a 10-tab cockpit with agent, MCP, geo maps, inter-provincial flow, market fundamentals, load factor, system tightness, and bilingual (EN/ZH) support.

**What was rejected:** Keeping the old app alongside the new one — unnecessary duplication.

---

## 2026-05-06, Bilingual Support Architecture

**What was decided:** Language toggle (English / 中文) in the left sidebar drives all UI label translations via a `_t()` dict lookup. AI-generated summaries (Province Deep-Dive) are translated lazily on demand — per-summary `🌐 翻译` button inside each expander, result cached in `st.session_state["translated_summaries"]`. Agent responds in Chinese when Chinese mode is active.

**Why:** Auto-translating all summaries on language switch caused 30–60s page freeze (sequential API calls before first render). Lazy per-summary translation is instant for the page and ~2s per summary on demand.

**What was rejected:** Batch auto-translate on language switch (caused full-page freeze); `@st.cache_data` spinner approach (caused rerun loops).

---

## 2026-05-06, Geo Map Animation Anti-Pattern Fix

**What was decided:** Use `_anim_loop_rerun` session state flag to control the geo map animation loop. Animation only continues if the rerun was explicitly triggered by the animation itself. Any other user interaction stops the animation.

**Why:** `time.sleep() + st.rerun()` inside tab code runs on every Streamlit rerender (all tab code always executes), causing an infinite rerun loop that greys out the entire app when `anim_playing = True`.

**What was rejected:** Any approach that calls `st.rerun()` unconditionally inside tab render code.

---

## 2026-05-06, Market Fundamentals Tab

**What was decided:** New tab in spot-market app parsing `data/market-fundamentals/2023-2025 全国各省电力市场基础信息汇总2026-03-30.xlsx`. Displays: installed capacity (donut/stacked bar), generation mix, renewables share, peak load ranking table (both years), load factor by fuel type (%), and system tightness ranking.

**Why:** Agent needs structured access to market fundamentals to form complete investment picture. Visual display supports province comparison and ranking.

**What was rejected:** Embedding raw Excel data in the agent prompt — too large and unstructured.

---

## 2026-05-06, System Tightness Definition

**What was decided:** Effective capacity = Σ(Installed capacity 万kW × 10 MW × Standard EOH / 8760). Standard EOH: Wind 2000h, Solar 1100h, Thermal 5500h, Hydro 3500h, Nuclear 7500h. Storage excluded. Tightness = Effective capacity minus avg demand (= total generation / 8760) and minus summer/winter peak. Sorted tightest-first (ascending).

**Why:** Reflects the Chinese power system planning convention. Blended thermal EOH (5500h) used since 火电 data combines coal and gas.

**What was rejected:** Using pandas Styler for colour coding — version-sensitive (`applymap` → `map` in pandas 2.1+); replaced with plain `+`/`−` prefixed string formatting.

---

## Session Summary, 2026-05-06

**Worked on:** `apps/spot-market` — multiple feature additions and bug fixes; ECS deployment troubleshooting.

**Completed:**
- Market Fundamentals tab (capacity, generation, renewables share, peak load, load factor, system tightness)
- Bilingual support (EN/ZH) across all tabs including lazy summary translation
- Geo map animation loop fix (`_anim_loop_rerun` flag)
- CJK font fix in Docker (layer order + `FontManager()` cache rebuild)
- File upload table refresh fix (clear widget state before rerun)
- Peak load table refactored to ranking table with both years
- Load factor table with equivalent hours columns
- Pandas Styler removed in favour of `+`/`−` string formatting
- `--server.fileWatcherType=none` added to Dockerfile CMD
- Deployed to ECS as `bess-spot-markets:v13`

**In progress:** Nothing — all changes deployed.

**Decisions made:** See entries above.

**Next session:** The 5-pillar system is Pillar 1 (Market Map / spot-market) now largely complete. Pillar 2 (Asset Map) is the logical next focus — similar framework to spot-market but modelling asset value by type and region. Pillar 3 (Asset Operations) has a foundation in Inner Mongolia ops (`services/ops_ingestion/`). Consider which pillar to prioritise based on immediate business need.
