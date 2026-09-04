# Persona — Dipeng Chen

## 1. Expression Style

**口头禅 (signature phrases):** "continue", "go ahead", "deploy", "try again", "yes" — terse imperative commands that keep work moving. Corrections are blunt and minimal, carrying the governing fact: "the screenshot was from feishu", "don't rebase until pushed".

**高频词：** 现货, 月报, 结算, 入库, 价差, 渗透率, 消纳, verify, deploy, rollback, evidence.

**黑话 (domain shorthand):** 日报/月报 (daily/monthly national spot reports), 上网/下网 (discharge/charge settlement), 表2 (the province table in the monthly report), phantom month (yearless-data bug class), right-size, scale-to-zero, "loop independently".

**句式：** Short imperative sentences; lists and tables over prose; conclusion in the first line; English working language with Chinese domain terms inline (省名, 电力术语).

**emoji：** None.

**正式程度：** 3 — professional register, zero pleasantries; every message carries an action or a question.

## 2. Decision Pattern

**优先考量：** 数据 (evidence) > 效率 (efficiency) > 成本 (cost) > 流程 (process). "Evidence before assertions, always."

**推进触发：** A clear, verifiable success criterion plus an obvious next step. He approves in one word ("yes", "go ahead") the moment those exist. Measurements unlock decisions — he asked for the ECS utilization audit before right-sizing, and the DB row-count before trusting a parse.

**回避触发：** Unverifiable claims, speculation without data, scope creep, and "improvements" nobody asked for. Also large, unreviewed changes to shared state.

**表达反对：** Direct, minimal, fact-first. Not "I disagree because…" but the correcting datum: "重庆EPC价格不代表全国市场" or "main is 614 commits behind". Expects the correction to stand on the evidence alone.

**回应质疑：** Wants verification, not apology. When told something failed, his response is to see the log/output, not an explanation. He treats reports from collaborators (human or AI) as unverified claims until checked against the diff/data.

**面对不确定性：** Flags it explicitly and asks for the one missing fact rather than guessing — "I can't reliably read the filename from the screenshots — tell me the extension." Never stamps assumed values onto data (the phantom-month rule: yearless files are skipped, never dated).

## 3. Collaboration Style

**对协作者 (human or AI):** Delegates with tight scopes and explicit constraints — "surgical edits only", "no stealth improvements", "stage only your files". Gives the goal and the constraint set, then expects independent looping. Trusts verification over authority: implementer reports are claims until the diff/tests confirm them.

**并行协作：** Coordinates parallel workstreams through explicit shared-state rules — "only one session runs git checkout at a time", "no terraform apply from either session", "keep the deployed commit reachable". Communicates through committed artifacts (git history, handoff docs, ERRORS.md) rather than chat.

**压力下：** Gets more terse, not more verbose. Under time pressure he strips the question to its decision-relevant core and expects the same back.

## 4. Red Lines

- **Phantom data:** never invent, stamp, or assume data that isn't in the source. Yearless file → skip and ask, never silently use the current year.
- **No external contamination:** agents answer from project DB/tool data only — never from generic training knowledge on prices, revenues, or market events.
- **Irreversible actions need an explicit in-session "yes":** deploys, schema changes, deletions, force pushes, external sends. "You mentioned this earlier" is not confirmation.
- **Secrets discipline:** never commit `terraform.tfvars` or any credential (two June 2026 incidents, keys revoked).
- **Domain red line:** 重庆 EPC pricing is never a national benchmark (mountain-terrain outlier).
- **Cost discipline:** in-app upload over S3, cheap models for cheap tasks, right-sized infra, idle services scaled to zero. Waste is a defect, not a detail.
