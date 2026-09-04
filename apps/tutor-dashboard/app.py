"""
Kids Study Progress Dashboard
Reads session data from DeepTutor partner API and shows daily learning progress
for Zhuzhu and Chichi.
"""

import re
from datetime import datetime, date, timedelta, timezone

import httpx
import streamlit as st

TUTOR_BASE = "https://tutor.pjh-etrm.ai"
PARTNERS = {
    "Zhuzhu": "zhuzhu",
    "Chichi": "chichi",
}
SUMMARY_RE = re.compile(
    r"=== SESSION SUMMARY FOR (\w+) ===(.*?)=== END SUMMARY ===",
    re.DOTALL | re.IGNORECASE,
)
QUIZ_RE = re.compile(r"Quiz score:\s*(\d+)/(\d+)", re.IGNORECASE)
ASSESS_RE = re.compile(r"Q\d+:\s*(\d+)/5", re.IGNORECASE)
OVERALL_RE = re.compile(r"Overall:\s*(Excellent|Good|Needs more practice)", re.IGNORECASE)
TOPICS_RE = re.compile(r"Topics covered:\s*(.+)", re.IGNORECASE)


# ── API helpers ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def fetch_partner_sessions(partner_id: str) -> list[dict]:
    try:
        r = httpx.get(f"{TUTOR_BASE}/api/v1/partners/{partner_id}/sessions", timeout=10)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else data.get("sessions", [])
    except Exception:
        return []


@st.cache_data(ttl=60)
def fetch_session_history(partner_id: str, session_key: str) -> list[dict]:
    try:
        r = httpx.get(
            f"{TUTOR_BASE}/api/v1/partners/{partner_id}/history",
            params={"session_key": session_key, "limit": 200},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        return data.get("messages", data) if isinstance(data, dict) else data
    except Exception:
        return []


# ── Parsing ────────────────────────────────────────────────────────────────────

def parse_summary(text: str) -> dict | None:
    m = SUMMARY_RE.search(text)
    if not m:
        return None
    body = m.group(2)
    result: dict = {"raw": body.strip()}

    qm = QUIZ_RE.search(body)
    if qm:
        result["quiz_score"] = int(qm.group(1))
        result["quiz_total"] = int(qm.group(2))

    scores = [int(s) for s in ASSESS_RE.findall(body)]
    if scores:
        result["assess_scores"] = scores
        result["assess_avg"] = sum(scores) / len(scores)

    om = OVERALL_RE.search(body)
    if om:
        result["overall"] = om.group(1)

    tm = TOPICS_RE.search(body)
    if tm:
        result["topics"] = tm.group(1).strip()

    return result


def session_date(session: dict) -> date | None:
    ts = session.get("created_at") or session.get("timestamp") or session.get("updated_at")
    if ts:
        try:
            return datetime.fromtimestamp(float(ts), tz=timezone.utc).date()
        except Exception:
            pass
    return None


def get_daily_summaries(partner_id: str, target_date: date) -> list[dict]:
    sessions = fetch_partner_sessions(partner_id)
    summaries = []
    for s in sessions:
        d = session_date(s)
        if d != target_date:
            continue
        key = s.get("session_key") or s.get("id") or s.get("key")
        if not key:
            continue
        messages = fetch_session_history(partner_id, key)
        full_text = " ".join(
            m.get("content", "") or m.get("text", "")
            for m in messages
            if m.get("role") in ("assistant", "agent", None)
        )
        parsed = parse_summary(full_text)
        summaries.append({
            "session_key": key,
            "title": s.get("title", "Study session"),
            "summary": parsed,
            "message_count": s.get("message_count", len(messages)),
        })
    return summaries


# ── UI components ──────────────────────────────────────────────────────────────

def overall_emoji(label: str | None) -> str:
    if not label:
        return "❓"
    l = label.lower()
    if "excellent" in l:
        return "🌟"
    if "good" in l:
        return "👍"
    return "📖"


def render_child_card(name: str, partner_id: str, target_date: date):
    st.subheader(f"{'🌸' if name == 'Zhuzhu' else '🌼'} {name}")

    summaries = get_daily_summaries(partner_id, target_date)

    if not summaries:
        st.info(f"No study sessions recorded for {name} on {target_date.strftime('%d %b %Y')} yet.")
        return

    total_sessions = len(summaries)
    quiz_scores = [s["summary"]["quiz_score"] for s in summaries if s["summary"] and "quiz_score" in s["summary"]]
    quiz_totals = [s["summary"]["quiz_total"] for s in summaries if s["summary"] and "quiz_total" in s["summary"]]
    assess_avgs = [s["summary"]["assess_avg"] for s in summaries if s["summary"] and "assess_avg" in s["summary"]]
    overalls = [s["summary"]["overall"] for s in summaries if s["summary"] and "overall" in s["summary"]]

    # Metric row
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sessions", total_sessions)
    if quiz_scores:
        c2.metric("Quiz Score", f"{sum(quiz_scores)}/{sum(quiz_totals)}")
    if assess_avgs:
        avg = sum(assess_avgs) / len(assess_avgs)
        c3.metric("Avg Assessment", f"{avg:.1f}/5")
    if overalls:
        c4.metric("Overall", f"{overall_emoji(overalls[-1])} {overalls[-1]}")

    # Per-session breakdown
    for i, s in enumerate(summaries, 1):
        with st.expander(f"Session {i}: {s['title']} ({s['message_count']} messages)"):
            parsed = s["summary"]
            if parsed:
                if "topics" in parsed:
                    st.markdown(f"**Topics:** {parsed['topics']}")
                if "quiz_score" in parsed:
                    pct = parsed["quiz_score"] / parsed["quiz_total"] * 100
                    st.progress(pct / 100, text=f"Quiz: {parsed['quiz_score']}/{parsed['quiz_total']} ({pct:.0f}%)")
                if "assess_scores" in parsed:
                    scores_str = "  |  ".join(
                        f"Q{j+1}: {sc}/5" for j, sc in enumerate(parsed["assess_scores"])
                    )
                    st.markdown(f"**Assessment:** {scores_str}")
                if "overall" in parsed:
                    st.markdown(f"**Overall:** {overall_emoji(parsed['overall'])} {parsed['overall']}")
                if "raw" in parsed:
                    with st.expander("Full summary text"):
                        st.text(parsed["raw"])
            else:
                st.caption("Session completed — summary not yet generated (session may still be active).")


# ── Layout ─────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="Kids Study Dashboard", layout="wide", page_icon="📚")
st.title("📚 Kids Study Progress")

# Date picker
col_date, col_refresh = st.columns([3, 1])
with col_date:
    selected_date = st.date_input(
        "Report date",
        value=date.today(),
        max_value=date.today(),
    )
with col_refresh:
    if st.button("🔄 Refresh"):
        st.cache_data.clear()
        st.rerun()

st.divider()

left, right = st.columns(2)
with left:
    render_child_card("Zhuzhu", "zhuzhu", selected_date)
with right:
    render_child_card("Chichi", "chichi", selected_date)

st.divider()

# Weekly overview
st.subheader("📅 This week at a glance")
week_cols = st.columns(7)
today = date.today()
week_start = today - timedelta(days=today.weekday())

for i, col in enumerate(week_cols):
    day = week_start + timedelta(days=i)
    label = day.strftime("%a\n%d")
    is_today = day == today
    is_future = day > today
    with col:
        st.markdown(f"**{label}**" if is_today else label)
        if is_future:
            st.caption("—")
        else:
            for name, pid in PARTNERS.items():
                sessions = [
                    s for s in fetch_partner_sessions(pid)
                    if session_date(s) == day
                ]
                emoji = "✅" if sessions else "⬜"
                st.markdown(f"{emoji} {name[0]}")
