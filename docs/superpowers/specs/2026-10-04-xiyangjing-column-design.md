# 西洋镜看中国电力市场 (Western Lens on China's Power Markets) — Column Design Spec

Date: 2026-10-04 · Owner: Dipeng Chen · Status: draft for review

## 1. Purpose and scope

A recurring 自媒体 article series (公众号 primary, 知乎 cross-post possible) that uses Western power-market structures, rules, data and quant methods as a lens to comment on Chinese power markets — market design, policy, price data, analytics — and builds an evidence-based case for quant tooling and for futures/derivatives market development.

**Readers:** (A) industry peers and investors; (C) policy-facing readers (exchange, grid, regulator staff, think tanks).

**Cadence/format:** biweekly deep dives, ~4,000–6,000 Chinese characters, 3–5 charts, methods note. ~1–2h owner time per article.

**Identity:** pen-name / independent column first (`byline: pen_name`), possibly real-name later — a per-article front-matter field, not a rewrite. Pen name is an editorial choice, not a disguise: no anti-attribution techniques, and employer-policy, confidentiality and commentary rules apply unchanged.

**Language:** Chinese-first drafting. Western concept references link to the Power Academy graph (EN), with the extended ZH glossary for market terms.

### Non-goals
No API publishing (final paste into the editor is a deliberate manual gate). No live web search for agents (no-external-contamination rule). No changes to `apps/` or `services/` (read-only DB). No spreadsheet-model ingestion from `practice` folders (not approved).

## 2. Pillars touched (read-only)

P1 Strategist data (`marketdata.spot_prices_hourly`, `marketdata.spot_fundamentals_hourly`, `marketdata.province_fundamentals`, `staging.spot_interprov_flow`), P2 Quant (`marketdata.bess_capture_daily`), P4 Knowledge Pool (`staging.spot_knowledge_docs`, `staging.spot_knowledge_chunks`). Whitelist is explicit in `style/licenses.yaml`; anything else needs an edit there first.

**RDS access (verified 2026-10-04):** direct from the Mac via `PGURL` in `config/.env` with Astrill excluding AWS traffic; egress IP is on the RDS security-group allow-list. Connection errors must say "check VPN bypass + SG rule" explicitly. If the ISP IP rotates, one new SG rule is needed — same as before.

## 3. Article folder, brief and evidence manifest

Root: `power-academy/columns/xiyangjing/`

```
columns/xiyangjing/
  README.md
  style/voice.md              # tone rules: constructive, evidence-led, no named-institution criticism
  style/disclaimer.md         # standard footer disclaimer (owner-approved wording)
  style/blocklist.yaml        # names that must never appear: employer, counterparties, SEE deal names, internal figures
  style/licenses.yaml         # table/document source -> own | public | licensed_restricted (unknown defaults to licensed_restricted)
  topics/backlog.yaml         # topic board: editor proposals + owner ideas
  articles/<NNN>-<slug>/
    brief.md                  # front matter + one-paragraph thesis
    evidence/
      manifest.yaml           # one entry per dataset, chart, excerpt, western fact
      queries.yaml            # the build spec for build_evidence.py
      data/<id>.csv           # snapshots (licensed_restricted ones git-ignored)
      charts/<id>.py + <id>.png
      excerpts/<id>.md        # short policy quotes + full citation
    draft.zh.md
    gates.md
    out/<slug>.html
```

`brief.md` front matter:
```yaml
id: 003-merit-order-and-shandong-spot
byline: pen_name              # pen_name | real_name
status: idea | briefed | evidence | drafted | gated | approved | published
thesis: one sentence the article argues
western: {concept_ids: [merit_order], markets: [GB, DE]}
china: {provinces: [山东, 山西], topics: [现货, 中长期]}
derivatives_angle: what price risk the data shows
questions: [data questions the article must answer]
readers: [A, C]
published_url: null
```

`evidence/manifest.yaml` entry:
```yaml
- id: e_price_spread_sd
  kind: data | chart | policy_excerpt | western_fact
  source: <platform table | document id | URL>
  query: <SQL or tool call that produced it>
  retrieved_at: 2026-10-20
  license: own | public | licensed_restricted
  sha256: <hash of the snapshot>
```

**Claim tracing:** every number and policy claim in `draft.zh.md` carries `[[E:<id>]]`. The checker fails the article if a tag resolves to no manifest entry, or a number/percentage/price is uncovered. Mechanism explanations and argument are free text; numbers and policy claims are not.

## 4. Evidence builders

Deterministic scripts; no LLM. `build_evidence.py <article>` reads `queries.yaml` entries (`id`, `kind`: `sql | kb_excerpt | western_fact`, query/pointer, params) and:
- runs SQL via `PGURL`, writes `data/<id>.csv`;
- stores `kb_excerpt` as a short capped quote + citation (title, doc number, date, URL);
- records `western_fact` as concept id + source id citation (never copies content);
- fills the manifest (`retrieved_at`, `sha256`, exact query) and sets `license` from `style/licenses.yaml`;
- `verify` mode re-runs and compares hashes, so post-publication data revisions have a trail.

**Charts:** one script per chart (`charts/<id>.py`) reading only pack snapshots, writing PNG through a shared style module (consistent branding, legible Chinese fonts, 900px). A chart touching a restricted snapshot is marked non-public and blocked from `out/`.

**Drafting:** in a Claude Code session reading the finished pack. Draft asserts facts only through `[[E:id]]` tags.

## 5. Gates

An article may not reach `approved` until all gates pass. `check_gates.py` writes results to `gates.md`; the owner adjudicates flags and initials each line. `status: approved` in code requires all gates `pass` or adjudicated.

| Gate | Check | Automated? |
|---|---|---|
| Fact trace | tags resolve; numbers covered; hashes match | Yes |
| Data license | no restricted-derived chart/table in `out/`; license fields set | Yes |
| Confidentiality | no blocklist names in draft + evidence | Scan + owner read |
| Compliance | risky-pattern flags: instrument buy/sell recommendations, guaranteed-return language, price predictions as fact (将涨到…), named-institution criticism — flags, owner adjudicates | Scan + owner |
| Derivatives framing | argument rests on evidence-tagged price-risk observations, never product promotion | Owner |
| Tone | constructive; states what transfers from Western markets and what doesn't | Owner |
| Terminology | ZH market terms match extended glossary (中长期, 现货, 辅助服务, 容量电价 …) | Yes |

**Render (post-approval only):** `render.py` → `out/<slug>.html`; inline styles, tag whitelist surviving the 公众号 editor (`p,h2,h3,blockquote,strong,table,img`), charts as base64, auto footer (数据来源与口径 from manifest + 方法 note + disclaimer). Owner pastes and publishes.

**Publish workflow:** backlog pick → brief (owner approves thesis — the cheap kill point) → build evidence → spot-check → draft → owner edit (systematic edits feed back into `style/voice.md`) → gates → approved with initials → render → publish → record URL. Learning loop: follow-ups appended to backlog after each article.

## 6. 编辑 (Editor) agent

**Job:** on-demand, local (`propose-topics` CLI). Reads recent developments, proposes 3–5 article topics as structured sketches appended to `topics/backlog.yaml` with `status: idea`. Owner picks; the normal pipeline takes over. It proposes, the owner disposes.

**Inputs (no new feeds, no web search):**
- Hermes daily briefings (`knowledge/hermes/briefings/`) — existing Chinese market news intake, plus international items once workstream §7 lands;
- Knowledge Pool newest policy docs (`staging.spot_knowledge_docs` by recency);
- weekly data-anomaly scan (deterministic script: biggest price spikes, DA/RT basis moves, volatility shifts across provinces, last 7 days) — topics arrive with their evidence path attached;
- Power Academy concept graph (Western lens);
- owner paste-in items (international news not covered by feeds).

**Proposal schema (appended to backlog):**
```yaml
- id: t-2026-10-06-01
  proposed_at: 2026-10-06
  working_title: 从英国容量市场拍卖看山东容量电价
  hook: {source: hermes_briefing_2026-10-05, item: <one-line news item + citation>}
  thesis_hypothesis: one sentence
  western: {concept_ids: [capacity_market], angle: what transfers, what doesn't}
  china: {provinces: [山东], topics: [容量电价, 现货]}
  evidence_candidates: [platform tables/queries that could carry the argument]
  timeliness: why this week
  status: idea
```

Runs with VPN on (Anthropic reachable); no new infrastructure. A scheduled Hermes variant is a later option once the habit sticks.

## 7. Workstream: international feeds into Hermes

**Goal:** the editor (and morning briefings) see major European/American market developments without live web search.

**Design:**
- A feed list in config (`feeds.yaml`): name, type (`rss | modo_api`), URL/endpoint, cadence. Seeded with **Timera Energy** (public RSS) and **Modo Energy** (reuse the existing browser-free NextAuth session flow in `services/gb_knowledge/modo_ai.py`); extensible by editing the list.
- A hermes ingestion job: fetch → dedupe (by URL hash) → store items as markdown under `knowledge/hermes/feeds/YYYY-MM-DD-<source>.md` with title/date/link/summary, and include a short international section in the daily briefing.
- No paywalled-content circumvention: RSS summaries and headline+link for anything gated; Modo uses the existing licensed session.

**Cross-pillar impact:** hermes image change + deploy. Deploy follows CLAUDE.md hermes rules (jq-swap from the service's current tdArn, never family-latest; explicit in-session confirmation). Sequenced after the column tooling; the editor works without it (paste-in covers international until then).

## 8. Risks

| Risk | Mitigation |
|---|---|
| Derivatives/policy commentary exposure (期货投资咨询 rules, 自媒体 financial content rules) | Compliance gate + adjudication per article; pen-name first; disclaimer footer; not legal advice — owner judgment final |
| Employer policy on publishing | Blocklist + confidentiality gate; owner confirms policy position before first publish |
| Data licensing in public charts | `licenses.yaml` with restricted-by-default; license gate blocks restricted-derived charts |
| Numbers drifting from sources | Claim tracing + hash verification; fact-trace gate is hard fail |
| Editor agent proposes stale or thin topics | Data-anomaly scan ties proposals to fresh platform signals; owner picks, never auto-publishes |
| Voice inconsistency across articles | `style/voice.md` fed by owner's edits after each article |
| RDS reachability breaks (IP rotation) | Builder error message names the VPN-bypass + SG-rule check explicitly |

## 9. Build order

1. Scaffolding: folder layout, brief/manifest schemas, `style/*` files, topic backlog seeded (~10 topics).
2. Evidence builder (SQL + KB excerpt + manifest + hashes + verify mode).
3. Chart style module + 2–3 starter chart types (price duration curve, DA/RT spread, provincial comparison).
4. Gates checker.
5. Renderer + footer + disclaimer template.
6. Editor agent (`propose-topics`).
7. Pilot article end-to-end (owner picks: merit-order/price-formation or spot-basis/derivatives-need).
8. Hermes international feeds workstream (separate deploy confirmation).
