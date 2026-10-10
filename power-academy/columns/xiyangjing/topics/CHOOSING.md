# Topic handoff contract (hermes card → professor session)

The Feishu weekly card proposes 3–5 topics. When the owner picks one, the choice is
written into `backlog.yaml` — that file is the ONLY handoff channel. The professor
session reads entries with `status: chosen` and runs the standard pipeline.

## Entry format

```yaml
- id: t-YYYY-MM-DD-nn
  working_title: <标题草案>
  status: chosen                # idea | chosen | in_progress | published | dropped
  chosen_at: YYYY-MM-DD
  hook: {source: <hermes_briefing | weekly_scan | kb_doc | owner>, item: <one line>}
  thesis_hypothesis: <one-two sentences the article would argue>
  western: {concept_ids: [<curriculum concept ids>], markets: [EU|GB|US|AU|CN]}
  china: {provinces: [...], topics: [...]}
  evidence_candidates: [<tables, docs, or owner-provided files to pull first>]
  notes: <caveats: data gaps, license questions, timing>
```

## Proposal flow (weekly Feishu card, v1)

1. The weekly job (`services/professor/topic_card.py`, Mondays 08:50 Beijing) gathers
   KB recent docs + the weekly price anomaly scan + curriculum concepts, asks the
   editor model for 3-5 proposals, stores them in
   `marketdata.professor_topic_proposals` (week, seq, ...), and sends the Feishu card.
2. The owner picks by replying on the card or telling the professor session the seq.
3. The professor session reads the week's proposals from that table, writes the pick
   into this file with `status: chosen`, and runs the pipeline.

## Rules
- Hermes (or the owner manually) appends with `status: chosen`; it never edits articles.
- Professor, on resume: reads `status: chosen` entries oldest-first, moves the picked
  one to `in_progress`, creates `articles/<NNN>-<slug>/`, and runs
  brief → evidence → draft → gates → render → publish.
- If a topic is not buildable with available data, professor says so and returns it
  to `idea` with a note — never publishes around a data gap.
