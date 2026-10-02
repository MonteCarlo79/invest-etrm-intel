# Power Academy — Design Spec (Phases 0–1)

Date: 2026-10-02 · Owner: Dipeng Chen · Status: draft for review

## 1. Purpose and scope

Build a systematic, bilingual (English + Chinese) training library for power-markets quants covering analytics, trading and risk management, then generate market-simulation games and AI video clips from it.

**Audience:** external/commercial. Channel (corporate training / university courseware / self-serve) is deliberately undecided; the knowledge layer is channel-neutral and packaging is a later decision.

**This spec covers Phase 0 (inventory) and Phase 1 (syllabus + concept graph) in detail.** Phases 2–4 (concept authoring, games, video) each get their own spec once the syllabus exists. Hard gate: no Phase 3 spec until two pilot tracks are `reviewed`.

### Inputs
| Class | Location (OneDrive-Personal) | Treatment |
|---|---|---|
| `library` | `Structure/Asset Modelling/Power` (36 PDF, 15 PPT, 2 DOCX, ~140 spreadsheet models, `workspace/` git repo with PowerLP / CurveShaping / Supply) | Learn from; cite only; never reproduce |
| `practice` | `company/SEE/power` (26 project folders), `company/SEE/ote2/dipeng` (65 folders) | Cleared by owner for use in published content (stated by owner 2026-10-02; recorded per source as `cleared: true`) |

Licensed market data inside `practice` (e.g. Trayport GDEM files) stays out of published labs unless the owner states otherwise; labs use public or synthetic series with the same shape.

### Non-goals
No vector DB / RAG (Stage 4 markdown design). No changes to `apps/` or `services/`. No game or video implementation in this spec.

## 2. Core design

Single source of truth: a **concept graph in markdown**. Games and videos are compiled from `concepts/` and `labs/`; they never read `sources/`.

Content must be **original**. Sources are cited, not reproduced: no slide images, copied figures, or long paraphrase of a single paper.

## 3. Repository structure

New top-level folder `power-academy/` (outside the platform apps; can become its own repo later).

```
power-academy/
  README.md
  sources/index.yaml          # metadata only; paths point into OneDrive; no copies committed
  glossary/terms.yaml         # locked EN↔ZH terminology (see §5)
  inventory/                  # Phase 0
    <source-id>.md            # per-source outline
    coverage_map.md           # topic × source matrix, gaps flagged
  syllabus/                   # Phase 1
    tracks.yaml               # tracks → modules → concept ids
    graph.yaml                # prerequisite edges
  concepts/<track>/
    <id>.md                   # English (authoring language)
    <id>.zh.md                # Chinese
  labs/<id>/                  # runnable code for worked examples
  review/                     # status log, open questions, syllabus_review.md
  cache/                      # git-ignored raw extracted text
```

`sources/index.yaml` entry: stable slug id, path, title, authors, year, market, level, type, `class` (library|practice), `cleared`, `license_risk`.

## 4. Concept file schema

```yaml
id: spark_spread_option
track: valuation
level: foundation | intermediate | advanced
prerequisites: [forward_curves, option_basics]
markets: [EU, GB, US, AU, CN]
status: stub | drafted | reviewed | published
sources:
  - {id: kyos_2011, use: background}
  - {id: toll_deal_x, use: practice_example}
originality: original | synthesized      # never "derived"
translations:
  zh: {status: none | drafted | reviewed, en_hash: <sha of en body at translation time>}
```

Body sections in order: Learning objectives, Intuition, Formal treatment, Worked example (linked to `labs/`), Market variants, Common errors, Assessable questions. Assessable questions seed game scenarios and video scripts.

Enforced rules:
- `reviewed` requires at least one lab, or a stated reason none exists.
- `published` requires owner sign-off (IP gate), per language.
- `markets` is mandatory so China/GB/AU variants are first-class.

## 5. Bilingual requirement (EN + ZH)

- **English is the authoring language; Chinese is a first-class parallel file**, not an afterthought. The `CN` market variants and China practice material may originate in Chinese and are back-translated to English.
- **Locked glossary** `glossary/terms.yaml`: every technical term has an approved EN and ZH form (e.g. spark spread / 火花价差). Translation prompts must inject the glossary; a checker fails the build on unglossed or inconsistent terms.
- **Staleness check:** `en_hash` in the zh front matter is compared to the current English body; mismatch flags the zh file as stale until re-translated and re-reviewed.
- **Independent gates:** zh has its own `drafted → reviewed` status and its own publish sign-off. A concept can publish in EN before ZH.
- **Downstream:** games use per-language string tables (no hard-coded text); video scripts and narration are generated per language from the matching concept file.
- Translation is AI-drafted, owner-reviewed (same Stage 4 pattern). Formulae, code and units are language-neutral and shared.

## 6. Phase 0 — extraction pipeline

Goal: one outline per source and a coverage map, cheaply, so the syllabus is evidence-based.

1. **Register.** Script walks the library and writes `sources/index.yaml`. Covers PDF, PPT(X), DOCX, TXT; skips `.git`, `.metadata`, caches, PNGs, code. Spreadsheet/Python models go to a separate lightweight catalogue (they seed `labs/`, not outlines). `practice` folders are outlined per folder, not per file.
2. **Hydrate safely.** OneDrive files are dataless; read through a throttled single-file queue, log failures, never bulk-touch the tree.
3. **Extract text** (pdftotext/PyMuPDF; python-pptx; python-docx). Scanned PDFs with no text layer are flagged for OCR, not silently skipped. Output to git-ignored `cache/`.
4. **Outline with an LLM** (one call per source; chunk and merge long sources). Output: topic, level, market, year, key concept names with one-line scope, methods, implied prerequisites, presence of worked examples/code. The prompt forbids summarising the author's argument.
5. **Coverage map.** Tag concepts to candidate syllabus nodes; build topic × source matrix with gaps marked. This is the Phase 0 review gate.

**Execution environment:** Anthropic is geo-blocked from the Mac. Steps 1–3 run locally; step 4–5 run as a **one-off Fargate task** (same NAT-routed run-task pattern as other one-off jobs). Extracted text is staged to the task via a mechanism decided in the implementation plan (S3 is permitted here only if in-task upload is infeasible; default is to avoid it per the project's data-upload rule). The task sends `practice` text to the Anthropic API; owner clearance is assumed to cover this.

**Model/cost:** Sonnet-class for outlines, Haiku-class for tagging. Measure on 3 sources before the full run; expected tens of dollars.

## 7. Phase 1 — syllabus and concept graph

Process: draft `tracks.yaml` from the coverage map (each concept a `stub` with id, one-line scope, level, prerequisites, mapped sources); build `graph.yaml`; validator checks cycles, orphans (no path to a foundation concept), and concepts with no source and no declared "original" plan; produce `review/syllabus_review.md`; **owner gate** — edit and approve.

Proposed track skeleton (starting point for the owner to cut/reorder):

| # | Track | Expected coverage |
|---|---|---|
| 1 | Market fundamentals: price formation, merit order, market designs, products | Thin |
| 2 | Price and curve modelling: mean reversion, spikes, forward curves, shaping | Good (+ practice: Curve Shaping) |
| 3 | Asset valuation: spark spreads, tolling, CCGT, real options | Strong (+ practice: European/UK Toll, Locational Spread Options) |
| 4 | Optimisation and dispatch: LP/MIP, start-up value, switching | Strong (+ PowerLP) |
| 5 | Storage and flexibility: hydro, pumped storage, swing, BESS | Mixed; no modern BESS in library |
| 6 | Hedging and trading strategy: plant hedging, delta, rolling intrinsic | Good (KYOS, Likron) |
| 7 | Risk management: PaR, VaR, credit, limits, model risk | Improved by practice (Credit risk model, Risk documents) |
| 8 | Modern markets: renewables-heavy, intraday, imbalance, China spot | Not in library; original content from platform experience |

Each track carries a "from practice" layer: the deal or model that shows the concept in use. Sizing target: 60–100 concept stubs (<50 too coarse for game generation; >120 too granular to review). Proposed pilot: tracks 3 and 6.

## 8. Testing and verification

- Pipeline emits a count report: found, hydrated, extracted, outlined, failed. No silent drops.
- Unit tests: registrar, slug ids, schema validator, graph checks, glossary checker, zh staleness check.
- Outline quality: hand-check 5 sources against originals before the full run; revise prompt if outlines paraphrase instead of naming concepts.
- Phase 2 labs must reproduce a known result from the source model within a stated tolerance; tests live beside each lab.

## 9. Risks

| Risk | Mitigation |
|---|---|
| OneDrive hydration stalls / disk pressure | Throttled single-file queue; failures logged |
| Outlines drift into paraphrase | Concept names + one-line scope only; spot checks |
| Scope creep into games before syllabus is stable | Hard gate: two pilot tracks `reviewed` first |
| Library reads as dated (2009–2017) | Track 8 and practice layer are required |
| Review burden doubles with two languages | Independent per-language gates; review queue sorted by track; EN-first publishing allowed |
| Terminology drift between EN and ZH | Locked glossary + checker |
| Clearance basis for `practice` is an owner statement, not verified | Per-source `cleared` flag; publish gate remains owner sign-off |

## 10. Handoff to later phases

- **Games (Phase 3):** simulation engine (price processes, dispatch/LP core reusing existing PowerLP/platform code, settlement layer) plus scenario packs tied to concept ids; per-language string tables.
- **Video (Phase 4):** script and storyboard generated from one concept + its lab, rendered per language; review pass per clip.

## 11. Deliverables (Phases 0–1)

Inventory scripts and Fargate job definition; `sources/index.yaml`, per-source outlines, `coverage_map.md`; glossary seed; syllabus, graph and validators; README (how to add a source or concept).
