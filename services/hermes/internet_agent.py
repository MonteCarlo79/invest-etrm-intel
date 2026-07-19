"""Headless internet research agent powered by Agent Reach tools.

Routes internet/web queries from Hermes to the appropriate tool:
  - Web pages    → Jina Reader (curl https://r.jina.ai/URL)
  - Web search   → Exa via mcporter
  - GitHub       → gh CLI
  - YouTube      → yt-dlp
  - RSS          → feedparser
  - Bilibili     → bili CLI
  - V2EX         → V2EX public API

Called from market_agent_bridge.py as market='internet'.
"""
from __future__ import annotations

import json
import logging
import subprocess
import sys
from typing import Optional

logger = logging.getLogger(__name__)

_TIMEOUT = 30  # seconds per tool call


def _run(cmd: list[str], timeout: int = _TIMEOUT) -> str:
    """Run a shell command and return stdout, or an error string."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=True, text=True, timeout=timeout,
            env=_env(),
        )
        out = result.stdout.strip()
        err = result.stderr.strip()
        if result.returncode != 0 and not out:
            return f"Error (exit {result.returncode}): {err[:500]}"
        return out[:8000] if out else (err[:500] or "(no output)")
    except subprocess.TimeoutExpired:
        return f"Timed out after {timeout}s"
    except FileNotFoundError as e:
        return f"Tool not found: {e}"
    except Exception as e:
        return f"Error: {e}"


def _env() -> dict:
    """Return os.environ, adding GITHUB_TOKEN if set."""
    import os
    env = dict(os.environ)
    # Ensure PATH includes common tool locations
    paths = ["/usr/local/bin", "/usr/bin", "/bin", env.get("PATH", "")]
    env["PATH"] = ":".join(p for p in paths if p)
    return env


# ── Tool implementations ──────────────────────────────────────────────────────

def read_url(url: str) -> str:
    """Fetch a web page as clean markdown via Jina Reader."""
    jina_url = f"https://r.jina.ai/{url}"
    return _run(["curl", "-s", "-L", "--max-time", "20",
                 "-H", "Accept: text/plain", jina_url])


def web_search(query: str, num_results: int = 5) -> str:
    """Search the web via Exa (mcporter). Falls back to GitHub search on failure."""
    cmd = ["mcporter", "call",
           f"exa.web_search_exa(query: \"{query}\", numResults: {num_results})"]
    result = _run(cmd, timeout=20)
    if "Error" in result or "not found" in result.lower():
        # Fallback: gh search for code/repos
        logger.warning("Exa search failed, falling back to gh: %s", result[:100])
        return _run(["gh", "search", "repos", query, "--limit", str(num_results),
                     "--json", "fullName,description,stargazerCount,url"])
    return result


def search_github(query: str, limit: int = 10, search_type: str = "repos") -> str:
    """Search GitHub repos, code, or issues."""
    valid = {"repos", "code", "issues", "prs", "commits"}
    if search_type not in valid:
        search_type = "repos"
    return _run(["gh", "search", search_type, query,
                 "--limit", str(limit),
                 "--json", "fullName,description,stargazerCount,url,updatedAt"])


def read_github_repo(repo: str) -> str:
    """Read README and metadata of a GitHub repo (owner/repo format)."""
    meta = _run(["gh", "repo", "view", repo,
                 "--json", "name,description,stargazerCount,url,homepageUrl,topics,updatedAt"])
    readme = _run(["gh", "repo", "view", repo, "--readme"])
    return f"=== Repo metadata ===\n{meta}\n\n=== README ===\n{readme[:4000]}"


def get_youtube_info(url: str) -> str:
    """Extract video metadata, description and subtitles from YouTube."""
    info = _run(["yt-dlp", "--dump-json", "--no-playlist", url], timeout=30)
    try:
        d = json.loads(info)
        summary = {
            "title": d.get("title"),
            "uploader": d.get("uploader"),
            "upload_date": d.get("upload_date"),
            "duration": d.get("duration"),
            "view_count": d.get("view_count"),
            "description": (d.get("description") or "")[:1000],
            "url": d.get("webpage_url"),
        }
        return json.dumps(summary, ensure_ascii=False, indent=2)
    except Exception:
        return info[:3000]


def read_rss(url: str, limit: int = 10) -> str:
    """Read an RSS/Atom feed and return recent entries."""
    code = f"""
import feedparser, json, sys
feed = feedparser.parse('{url}')
entries = []
for e in feed.entries[:{limit}]:
    entries.append({{
        'title': getattr(e, 'title', ''),
        'link': getattr(e, 'link', ''),
        'summary': (getattr(e, 'summary', '') or '')[:300],
        'published': getattr(e, 'published', ''),
    }})
print(json.dumps({{'feed_title': feed.feed.get('title',''), 'entries': entries}}, ensure_ascii=False, indent=2))
"""
    return _run([sys.executable, "-c", code], timeout=20)


def search_bilibili(query: str, limit: int = 5) -> str:
    """Search Bilibili videos (bili-cli, no login needed)."""
    return _run(["bili", "search", query, "--type", "video", "-n", str(limit)],
                timeout=20)


def get_v2ex_hot() -> str:
    """Get V2EX hot topics."""
    return _run(["curl", "-s", "--max-time", "10",
                 "https://www.v2ex.com/api/topics/hot.json",
                 "-H", "User-Agent: agent-reach/1.0"])


# ── Tool registry ─────────────────────────────────────────────────────────────

_TOOLS = [
    {
        "name": "read_url",
        "description": "Fetch and read any web page as clean text. Use for news articles, company pages, regulatory documents, etc.",
        "input_schema": {
            "type": "object",
            "properties": {"url": {"type": "string", "description": "Full URL to fetch"}},
            "required": ["url"],
        },
    },
    {
        "name": "web_search",
        "description": "Search the web via Exa AI. Best for recent news, research, and broad topic searches.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "num_results": {"type": "integer", "description": "Number of results (default 5)"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_github",
        "description": "Search GitHub repositories, code, or issues.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "description": "Max results (default 10)"},
                "search_type": {"type": "string", "description": "repos | code | issues | commits"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "read_github_repo",
        "description": "Read a GitHub repository's metadata and README.",
        "input_schema": {
            "type": "object",
            "properties": {"repo": {"type": "string", "description": "owner/repo format"}},
            "required": ["repo"],
        },
    },
    {
        "name": "get_youtube_info",
        "description": "Get YouTube video metadata and description.",
        "input_schema": {
            "type": "object",
            "properties": {"url": {"type": "string", "description": "YouTube video URL"}},
            "required": ["url"],
        },
    },
    {
        "name": "read_rss",
        "description": "Read an RSS or Atom feed and return recent entries.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string"},
                "limit": {"type": "integer", "description": "Max entries (default 10)"},
            },
            "required": ["url"],
        },
    },
    {
        "name": "search_bilibili",
        "description": "Search Bilibili for videos on a topic (no login needed).",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_v2ex_hot",
        "description": "Get current hot topics on V2EX tech forum.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
]


def _dispatch(name: str, inp: dict) -> str:
    try:
        if name == "read_url":
            return read_url(inp["url"])
        elif name == "web_search":
            return web_search(inp["query"], inp.get("num_results", 5))
        elif name == "search_github":
            return search_github(inp["query"], inp.get("limit", 10), inp.get("search_type", "repos"))
        elif name == "read_github_repo":
            return read_github_repo(inp["repo"])
        elif name == "get_youtube_info":
            return get_youtube_info(inp["url"])
        elif name == "read_rss":
            return read_rss(inp["url"], inp.get("limit", 10))
        elif name == "search_bilibili":
            return search_bilibili(inp["query"], inp.get("limit", 5))
        elif name == "get_v2ex_hot":
            return get_v2ex_hot()
    except Exception as e:
        return f"Tool error: {e}"
    return "Unknown tool"


# ── Agent entry point ─────────────────────────────────────────────────────────

_SYSTEM = (
    "You are an internet research assistant for a BESS energy investment professional. "
    "You have tools to search the web, read web pages, search GitHub, get YouTube info, "
    "read RSS feeds, search Bilibili, and check V2EX. "
    "Use tools to fetch real data. Quote sources. "
    "For energy/power market questions, prioritise searching for recent news and regulatory docs. "
    "For Chinese energy content, use Bilibili or web_search with Chinese keywords."
)


def run_internet_query(question: str, api_key: str, pg_url: str = "") -> str:
    """Run the internet research agent and return its answer."""
    import anthropic
from shared.anthropic_client import make_client as _make_anthropic_client
    client = _make_anthropic_client(api_key)

    messages = [{"role": "user", "content": question}]
    while True:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=_SYSTEM,
            tools=_TOOLS,
            messages=messages,
        )
        messages = messages + [{"role": "assistant", "content": resp.content}]
        if resp.stop_reason == "end_turn":
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        tool_results = []
        for block in resp.content:
            if block.type == "tool_use":
                result_str = _dispatch(block.name, block.input)
                tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result_str})
        if not tool_results:
            return next((b.text for b in resp.content if hasattr(b, "text")), "")
        messages = messages + [{"role": "user", "content": tool_results}]
