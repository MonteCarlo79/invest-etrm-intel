# Power Academy

Bilingual (EN/ZH) power-markets quant curriculum. Spec: `docs/superpowers/specs/2026-10-02-power-academy-design.md`.

Run from this folder with `PY=~/.venvs/bess-platform/bin/python`:

- `$PY -m academy.cli register` — index the library + practice folders into `sources/index.yaml`
- `$PY -m academy.cli extract` — extract text into git-ignored `cache/`
- `$PY -m academy.cli validate` — validate concepts, glossary, graph
- LLM stages (`outline`, `coverage`, `syllabus`) run on Fargate; see the plan Task 11.

Add a source: place it in the library, re-run `register`. Add a concept: create `concepts/<track>/<id>.md` (+ `<id>.zh.md`) following the schema in the spec §4, then `validate`.
