# Power Academy Phase 2 — Concept Authoring + Labs (Pilot Tracks) Design Spec

Date: 2026-10-05 · Owner: Dipeng Chen · Status: draft for review

## 1. Purpose and scope

Turn the approved Phase 1 syllabus stubs into full bilingual concept files with runnable labs for the two pilot tracks: **asset_valuation** (12 stubs) and **hedging_trading** (12 stubs). Output: 24 EN concept files (`drafted → reviewed`), 24 ZH translations (after EN review), 24 worked-example labs (level B), 4–6 anchor model reproductions (level A).

**Owner decisions locked (2026-10-05):** labs are **mixed** (B for every concept + A for 2–3 anchors per track); owner reviews **per batch of 6 concepts** (4 batches total).

**Prerequisites (done):** Phase 0 inventory (126 source outlines, `inventory/`), Phase 1 syllabus (`syllabus/tracks.yaml`, `concepts/<track>/<id>.md` stubs), coverage map (no gaps).

### Non-goals
No games/video work (Phases 3–4). No new sources. No spreadsheet-model ingestion from practice folders (anchor models are re-derived from outline + cached text, not parsed from xlsm). Hermes feeds workstream stays separate.

## 2. Concept authoring standard

Each concept file keeps the Phase 1 front matter; the body fills the seven sections:

| Section | Standard |
|---|---|
| Learning objectives | 3–5 testable statements ("compute X given Y", "explain why Z") |
| Intuition | 1–3 paragraphs, no maths; one concrete market example |
| Formal treatment | Definitions + formulas (LaTeX `$…$`), assumptions stated explicitly, units consistent with the project (元/MWh or €/MWh per market, kWh for China) |
| Worked example | One end-to-end numeric example whose every step is reproducible by `labs/<id>/` |
| Market variants | Per-market differences using the `markets` front-matter field; CN variant required where China practice exists |
| Common errors | 3–5 practitioner mistakes, drawn from practice-layer sources where possible |
| Assessable questions | 3–5 questions mapped to the learning objectives (seed for games/videos) |

**Citation discipline (unchanged):** sources appear in front matter (`id`, `use: background | derivation_reference | practice_example`); the body never quotes or paraphrases a source at length; `originality: synthesized` when sources informed it, `original` otherwise.

**Length target:** 400–800 lines EN per concept is too much — target **150–300 lines** so 24 stay reviewable. Depth over breadth within a section.

## 3. Lab standards

`labs/<id>/` (one folder per concept):
- `compute.py` — the worked example as runnable code; deterministic (seeded RNG); prints the headline quantities.
- `test_lab.py` — pytest asserting the headline numbers match the worked example in the concept file within a stated tolerance.
- Data: synthetic or public only. **No licensed/restricted data in labs** (they ship with the curriculum). Synthetic series must reproduce the shape of the real phenomenon (documented in a comment).
- Level B: ~30–80 lines, one key quantity (e.g. Margrabe spark-spread option value vs the paper's table).
- Level A (anchors): a fuller model port reproducing a known result from the source within tolerance, e.g. toll valuation outputs vs the practice model's headline numbers (re-derived from outline + cached text, not by parsing xlsm).

**Anchor candidates** (owner confirms at spec review):
- asset_valuation: `spark_spread_option` (Margrabe/Kirk vs practice), `ccgt_investment_option_and_optimal_timing` (real-options timing vs Tseng-style result), toll valuation vs Humber/RWE practice outputs.
- hedging_trading: `dispatch_optimisation_and_delta_hedging` (practice: Dispatch and Delta Hedging v2–v4), rolling intrinsic vs KYOS approach, peak delta (Incremental Peak delta2 + AOM caveats).

## 4. Tooling additions (small, to `academy/`)

1. **`concept pack <id>`** — assembles an authoring pack for the session: the stub, the mapped sources' inventory outlines, and a bounded excerpt (≤4 000 chars) of each mapped source's cached text. Keeps authoring grounded and repeatable.
2. **`concept set-status <id> <status>`** — enforces transitions: `reviewed` requires a lab with passing tests (runs pytest on `labs/<id>/`) or `no_lab_reason`; `published` requires `signoff.en` + existing `check_approval`-style gates; refuses and reports otherwise.
3. **`concept translate <id>`** — ZH translation via the locked glossary (`glossary.prompt_block`), stamps `translations.zh.en_hash` with the current EN `body_hash`, sets zh status `drafted`. Runs locally with VPN on (Anthropic) — no Fargate needed for 24 files.

All three are TDD'd CLI additions following the existing `academy/cli.py` patterns.

## 5. Batch workflow

Track-by-track (context stays coherent): AV batches 1–2, then HT batches 3–4.

Per batch of 6 concepts:
1. Author EN bodies + level-B labs + tests (and the track's anchors in its second batch).
2. `pytest` green; `concept set-status … drafted`.
3. **Owner review** — the batch's 6 files; owner edits or comments.
4. Fixes applied; owner moves them to `reviewed` (signoff line per file batch in `review/`).
5. Translate batch to ZH; owner spot-checks ZH (terminology enforced by the glossary checker).

ZH publishes independently per concept (spec §5 of the Phase 0–1 design): EN can be `reviewed` while ZH catches up.

## 6. Testing and verification

- New tooling: RED→GREEN per existing suite conventions.
- Every lab: `pytest labs/<id>/test_lab.py` must pass; the batch isn't `drafted` without it.
- Repo validator (`academy.cli validate`) must stay green after every batch (graph, glossary, zh staleness).
- Spot-check: owner verifies one lab number per batch against their own knowledge/model.

## 7. Risks

| Risk | Mitigation |
|---|---|
| Authoring drifts into paraphrasing sources | Citation discipline + `originality` flags + owner batch review |
| 24 × 300 lines overwhelms owner review | Batches of 6; length target enforced in authoring convention |
| Anchor models can't be reproduced from outline+text alone | Owner confirms anchors at spec review; fallback: owner provides the key outputs manually, lab reproduces *those* |
| ZH translation terminology drift | Locked glossary + checker; owner spot-check per batch |
| Scope creep into 100 concepts | Hard stop at the 24 pilot stubs; remaining tracks need their own phase |

## 8. Deliverables

24 EN concept files (`reviewed`), 24 ZH files (`drafted`+), 24 level-B labs with tests, 4–6 anchor labs, 3 CLI additions, batch review records in `review/`.
