Scaffold a new agent tab for a Streamlit app following the bess-platform agent design pattern (v21+).

Gather from the user (if not already provided in the command args):
1. Which app? (spot-market / bess-map / mengxi-dashboard / new app)
2. Agent persona name (e.g. "Trader", "Strategist")
3. What data tools does this agent need? (list the DB tables/queries)
4. What `app` key should be used for agent_memory scoping?

Then generate:
1. Agent tab addition to `st.tabs([...])` declaration
2. Full agent tab `with tab_<name>:` block including:
   - `_ensure_<name>_memory_table()` — idempotent CREATE TABLE IF NOT EXISTS
   - `_load_<name>_memories()` — cached 60s, scoped by app key
   - `_save_<name>_memory()` — insert to agent_memory
   - `_<NAME>_BASE_SYSTEM` — system prompt with domain grounding rule
   - `_build_<name>_system()` — base system + injected memories
   - `_<NAME>_TOOLS` — list of tool schemas
   - `_dispatch_<name>_tool()` — tool dispatcher
   - `_extract_<name>_memories()` — Haiku auto-extraction (returns list of {category, subject, content})
   - `_run_<name>_agent()` — agent loop (max 10 iterations)
   - UI: subheader, caption, clear button, chat history, chat_input, auto-save memory, Memory Management expander

Follow the pattern in apps/spot-market/app.py (v21 auto-save, no confirmation panel).
Memory categories should be domain-specific (not generic). Suggest appropriate categories based on the agent's domain.
