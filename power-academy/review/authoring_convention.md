# Authoring convention — concept files

- Body sections in order: Learning objectives, Intuition, Formal treatment,
  Worked example, Market variants, Common errors, Assessable questions.
- Length: 150–300 lines. Depth over breadth.
- Units: state once and keep consistent (CN 元/MWh or 元/kWh per project habit; EU/GB €/£ per MWh).
- The Worked example's every number must be reproducible by `labs/<id>/compute.py`;
  the lab's test asserts the same numbers.
- Never quote or paraphrase a source at length. Cite via front-matter `sources`.
- Write for a quant who knows markets basics; no 科普 filler.
- ZH file (`<id>.zh.md`) is produced only by `concept translate`, never hand-rolled first.
