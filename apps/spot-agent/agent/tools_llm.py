from __future__ import annotations

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


# --- Robust OpenAI client compatibility (new + legacy) ---
_USE_V1 = False
client = None
openai_legacy = None

try:
    from openai import OpenAI  # new SDK
    client = OpenAI()
    _USE_V1 = True
except Exception:
    try:
        import openai as openai_legacy  # legacy SDK
        openai_legacy.api_key = os.environ.get("OPENAI_API_KEY")
        _USE_V1 = False
    except Exception as e:
        raise RuntimeError(
            "OpenAI SDK not available. Install openai>=1.x or configure legacy openai package."
        ) from e


# Choose a model you have access to
# More widely available default than gpt-4.1-mini
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

def audit_price_row(province_cn: str, report_date: str, extracted: dict, source_row_text: str) -> str:
    """
    Ask LLM to sanity check if extracted numbers match the row text.
    Return short warning only if mismatch is likely.
    """
    if not source_row_text:
        return ""

    user_prompt = f"""
目标省份：{province_cn}
日期：{report_date}

PDF 行文本：
{source_row_text}

我从程序提取的结果：
{extracted}

请判断提取是否与行文本一致。
- 若一致：返回空字符串。
- 若可能有错：用一句话说明哪个字段可能不对。
"""

    # call your chat completion like summarize_highlights


def summarize_highlights(
    province_cn: str,
    report_date: str,
    raw_text: str,
    model: str | None = None,
) -> str:
    """
    Use LLM to summarise comments for a given province & date.

    :param province_cn: e.g. '山西'
    :param report_date: '2025-11-27' (用于提示，但不要写进结果里)
    :param raw_text:    extracted narrative text block (may contain many provinces)
    :return: short Chinese summary (<= ~50 chars)
    """

    if not os.getenv("OPENAI_API_KEY"):
        print("[WARN] OPENAI_API_KEY not set; highlights will be empty")
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
        if _USE_V1:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=120,
            )
            return (resp.choices[0].message.content or "").strip()
        else:
            # legacy fallback
            resp = openai_legacy.ChatCompletion.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=120,
            )
            return (resp["choices"][0]["message"]["content"] or "").strip()

    except Exception as e:
        print(f"[WARN] LLM highlight summarization failed for {province_cn} {report_date}: {e}")
        return ""
