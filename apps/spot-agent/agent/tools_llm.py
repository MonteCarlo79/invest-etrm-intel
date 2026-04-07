from __future__ import annotations

import logging
import os

from pathlib import Path

try:
    from dotenv import load_dotenv, find_dotenv
    root_env = Path(__file__).resolve().parent.parent / ".env"
    if root_env.exists():
        load_dotenv(root_env)
    else:
        load_dotenv(find_dotenv())
except Exception:
    pass

log = logging.getLogger(__name__)

# --- Lazy OpenAI client — only initialised on first actual LLM call ---
# Module-level import is intentionally deferred so that --no-llm runs work
# without the openai package being installed at all.
_USE_V1: bool | None = None   # None = not yet resolved
_client = None
_openai_legacy = None


def _ensure_client() -> None:
    """Resolve and cache the OpenAI client on first call. Raises if unavailable."""
    global _USE_V1, _client, _openai_legacy
    if _USE_V1 is not None:
        return  # already initialised
    try:
        from openai import OpenAI  # new SDK >=1.x
        _client = OpenAI()
        _USE_V1 = True
    except Exception:
        try:
            import openai as _oa
            _oa.api_key = os.environ.get("OPENAI_API_KEY")
            _openai_legacy = _oa
            _USE_V1 = False
        except Exception as e:
            raise RuntimeError(
                "OpenAI SDK not available. "
                "Install openai>=1.x or run with --no-llm."
            ) from e


DEFAULT_MODEL = os.getenv("SPOT_HI_MODEL", "gpt-4o-mini")

SYSTEM_PROMPT = (
    "你是电力现货日报的分析助手。"
    "输入是一大段从 PDF 报告提取出来的中文文本，里面可能包含："
    "多个省份的评论、表头、单位说明、栏目名、数字表格残留等噪声。\n"
    "你的任务：\n"
    "1) 只保留【目标省份】相关的要点（价格波动、原因、机组/负荷/新能源/检修等）。\n"
    "2) 严格忽略其它省份、全国汇总、表头/栏目名/单位/数据行。\n"
    "3) 如果文本中没有【目标省份】相关信息，输出空字符串。\n"
    "4) 用简短中文总结（不超过 50 字），不要加引号、不要加前缀。"
)


def _chat(messages: list[dict], model: str, temperature: float = 0.1, max_tokens: int = 120) -> str:
    """Single helper that works with both new SDK and legacy SDK."""
    _ensure_client()
    if _USE_V1:
        resp = _client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return (resp.choices[0].message.content or "").strip()
    else:
        resp = _openai_legacy.ChatCompletion.create(
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return (resp["choices"][0]["message"]["content"] or "").strip()


def audit_price_row(
    province_cn: str,
    report_date: str,
    extracted: dict,
    source_row_text: str,
) -> str:
    """
    Ask LLM to sanity-check whether the extracted numbers match the row text.
    Returns a short warning string if a mismatch is likely; otherwise empty string.
    Skipped (returns '') when OPENAI_API_KEY is not set or source_row_text is empty.
    """
    if not source_row_text or not source_row_text.strip():
        return ""

    if not os.getenv("OPENAI_API_KEY"):
        log.debug("audit_price_row: OPENAI_API_KEY not set; skipping audit for %s %s",
                  province_cn, report_date)
        return ""

    user_prompt = (
        f"目标省份：{province_cn}\n"
        f"日期：{report_date}\n\n"
        f"PDF 行文本：\n{source_row_text}\n\n"
        f"我从程序提取的结果：\n{extracted}\n\n"
        "请判断提取是否与行文本一致。\n"
        "- 若一致：返回空字符串。\n"
        "- 若可能有错：用一句话说明哪个字段可能不对。"
    )

    try:
        return _chat(
            messages=[{"role": "user", "content": user_prompt}],
            model=DEFAULT_MODEL,
            temperature=0.0,
            max_tokens=80,
        )
    except Exception as e:
        log.warning("audit_price_row failed for %s %s: %s", province_cn, report_date, e)
        return ""


def summarize_highlights(
    province_cn: str,
    report_date: str,
    raw_text: str,
    model: str | None = None,
) -> str:
    """
    Use LLM to summarise comments for a given province & date.

    :param province_cn: e.g. '山西'
    :param report_date: '2025-11-27'
    :param raw_text:    extracted narrative text block (may contain many provinces)
    :return: short Chinese summary (<= ~50 chars), or '' if API key missing / text empty
    """
    if not os.getenv("OPENAI_API_KEY"):
        log.warning("OPENAI_API_KEY not set; highlights will be empty")
        return ""

    if not raw_text or raw_text.strip() == "":
        return ""

    model = model or DEFAULT_MODEL

    user_prompt = (
        f"报告日期：{report_date}。\n"
        f"目标省份：{province_cn}。\n"
        "下面是从日报中解析出的原始文字，可能混有多个省份和噪声：\n"
        "--------------------\n"
        f"{raw_text}\n"
        "--------------------\n"
        "请只提炼【目标省份】相关的1~2条关键信息；没有则返回空字符串。"
    )

    try:
        return _chat(
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            model=model,
            temperature=0.1,
            max_tokens=120,
        )
    except Exception as e:
        log.warning("LLM highlight summarization failed for %s %s: %s",
                    province_cn, report_date, e)
        return ""
