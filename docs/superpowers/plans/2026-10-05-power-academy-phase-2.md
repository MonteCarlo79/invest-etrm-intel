# Power Academy Phase 2 — Concept Authoring + Labs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the 24 approved pilot-track stubs (asset_valuation ×12, hedging_trading ×12) into reviewed EN concept files with tested labs, plus ZH translations — via 3 CLI tools and 4 owner-reviewed batches.

**Architecture:** Three small tools in `academy/concepts.py` + `academy/cli.py` (pack assembly, gate-enforced status, glossary-locked translation). Content work happens in-session from authoring packs; every lab is a pytest-tested `labs/<id>/`. Batches of 6 stop for owner review.

**Tech Stack:** Python 3 (venv `~/.venvs/bess-platform`), numpy, pytest, anthropic (translation only, local with VPN).

**Spec:** `docs/superpowers/specs/2026-10-05-power-academy-phase-2-design.md`

## Global Constraints

- Labs use synthetic or public data only — **no licensed/restricted data**; deterministic (seeded RNG).
- Concept bodies: 150–300 lines EN, seven sections in the Phase 1 order, no source quoting/paraphrasing; `originality: synthesized | original`.
- `reviewed` requires a lab with passing tests or `no_lab_reason`; `published` requires `signoff.en`.
- Translation stamps `translations.zh.en_hash = body_hash(EN body)` and sets zh `drafted`; EN-first publishing allowed.
- Batches of 6; **stop for owner review after each batch** — no batch starts before the previous is owner-`reviewed`.
- Worktree `/tmp/bess-pa`, branch `power-academy`; commits end with `Co-Authored-By: Claude Code <noreply@anthropic.com>`.
- Test command (from `power-academy/`): `~/.venvs/bess-platform/bin/python -m pytest tests labs -q`.
- Cached source text lives in git-ignored `cache/<source_id>.txt`; source outlines in `inventory/<source_id>.md`; stubs in `concepts/<track>/<id>.md`; syllabus mapping in `syllabus/tracks.yaml`.

## Review Focus

1. Pack assembly must bound cache excerpts (≤4 000 chars/source) — a 200k-char practice folder must not blow up the authoring context. (Task 1)
2. set-status to `reviewed` without a passing lab must refuse — gate enforcement, not convention. (Task 2)
3. Translation must not silently go stale: zh file stamped with the EN hash at translation time; re-translate required after EN edits. (Task 3)
4. Labs must be deterministic — same seed, same headline numbers, or the concept file and its lab drift apart. (Tasks 4–7)
5. Anchor labs assert against a number traceable to a source/practice result, not a number the author invented. (Tasks 5, 7)

---

### Task 1: `concept pack` tool + authoring convention

**Files:**
- Create: `power-academy/academy/authoring.py`, `power-academy/tests/test_authoring.py`
- Create: `power-academy/review/authoring_convention.md`
- Modify: `power-academy/academy/cli.py` (add `concept` subcommands)

**Interfaces:**
- Consumes: `academy.io.load_yaml`, `academy.concepts.parse_concept`.
- Produces: `authoring.build_pack(root: Path, concept_id: str, max_excerpt: int = 4000) -> str` (markdown pack text); `authoring.find_concept(root, concept_id) -> Path`.

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_authoring.py`:
```python
from academy.authoring import build_pack, find_concept
from academy.concepts import render_concept


def _setup(root):
    (root / "concepts" / "asset_valuation").mkdir(parents=True)
    fm = {"id": "spark_dark_spread_fundamentals", "track": "asset_valuation",
          "level": "foundation", "prerequisites": [], "markets": ["EU"],
          "status": "stub",
          "sources": [{"id": "clewlow", "use": "background"},
                      {"id": "power_european_toll", "use": "practice_example"}],
          "originality": "synthesized",
          "translations": {"zh": {"status": "none", "en_hash": None}}}
    (root / "concepts" / "asset_valuation" / "spark_dark_spread_fundamentals.md"
     ).write_text(render_concept(fm, "## Learning objectives\n"), encoding="utf-8")
    (root / "inventory").mkdir()
    (root / "inventory" / "clewlow.md").write_text("# Clewlow\n\n- **MRJDx** — scope\n",
                                                  encoding="utf-8")
    (root / "cache").mkdir()
    (root / "cache" / "clewlow.txt").write_text("x" * 9000, encoding="utf-8")
    return root


def test_find_concept_locates_file(tmp_path):
    _setup(tmp_path)
    assert find_concept(tmp_path, "spark_dark_spread_fundamentals").name == \
        "spark_dark_spread_fundamentals.md"
    import pytest
    with pytest.raises(FileNotFoundError):
        find_concept(tmp_path, "nope")


def test_pack_contains_stub_outlines_and_bounded_excerpts(tmp_path):
    pack = build_pack(_setup(tmp_path), "spark_dark_spread_fundamentals")
    assert "spark_dark_spread_fundamentals" in pack and "MRJDx" in pack
    assert "clewlow" in pack and "power_european_toll" in pack
    # 9 000-char cache file must be bounded to the 4 000-char excerpt
    assert "x" * 5000 not in pack and ("x" * 4000) in pack
    assert "(no cached text)" in pack          # power_european_toll has no cache file
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_authoring.py -q`
Expected: FAIL (`ModuleNotFoundError: academy.authoring`).

- [ ] **Step 3: Implement**

`power-academy/academy/authoring.py`:
```python
from pathlib import Path

from .concepts import parse_concept


def find_concept(root: Path, concept_id: str) -> Path:
    hits = sorted((Path(root) / "concepts").rglob(f"{concept_id}.md"))
    hits = [h for h in hits if not h.name.endswith(".zh.md")]
    if not hits:
        raise FileNotFoundError(concept_id)
    return hits[0]


def build_pack(root: Path, concept_id: str, max_excerpt: int = 4000) -> str:
    root = Path(root)
    path = find_concept(root, concept_id)
    fm, body = parse_concept(path)
    parts = [f"# Authoring pack: {concept_id}", "", "## Stub front matter", "```yaml"]
    import yaml
    parts += [yaml.safe_dump(fm, allow_unicode=True, sort_keys=False), "```",
              "", "## Stub body", body, ""]
    for src in fm.get("sources", []):
        sid, use = src["id"], src.get("use", "background")
        parts.append(f"## Source: {sid} ({use})")
        outline = root / "inventory" / f"{sid}.md"
        parts.append(outline.read_text(encoding="utf-8")
                     if outline.exists() else "(no outline)")
        cache = root / "cache" / f"{sid}.txt"
        if cache.exists():
            parts += ["", "### Excerpt", cache.read_text(encoding="utf-8")[:max_excerpt]]
        else:
            parts.append("(no cached text)")
        parts.append("")
    return "\n".join(parts)
```
`power-academy/review/authoring_convention.md`:
```markdown
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
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_authoring.py -q`
Expected: 2 passed.

- [ ] **Step 5: Wire CLI + commit**

Append to `academy/cli.py` (imports: `from . import authoring as auth`), inside `main()`:
```python
    con = sub.add_parser("concept").add_subparsers(dest="sub2", required=True)
    pk = con.add_parser("pack")
    pk.add_argument("concept_id")
    pk.set_defaults(fn=lambda a: print(auth.build_pack(ROOT, a.concept_id)))
```
Commit:
```bash
git add power-academy/academy/authoring.py power-academy/academy/cli.py power-academy/tests/test_authoring.py power-academy/review/authoring_convention.md
git commit -m "Add concept authoring pack assembler and authoring convention" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: `concept set-status` with gate enforcement

**Files:**
- Modify: `power-academy/academy/authoring.py`, `power-academy/academy/cli.py`
- Create: `power-academy/tests/test_authoring_status.py`

**Interfaces:**
- Consumes: `academy.concepts.parse_concept/render_concept/validate_concept`, pytest subprocess.
- Produces: `authoring.set_status(root, concept_id, status, labs_root=None) -> None` (raises ValueError with reasons); CLI `concept set-status <id> <status>`.

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_authoring_status.py`:
```python
import pytest

from academy.authoring import set_status
from academy.concepts import parse_concept, render_concept


def _concept(root, cid="c1", status="stub", with_lab=False):
    d = root / "concepts" / "asset_valuation"
    d.mkdir(parents=True, exist_ok=True)
    fm = {"id": cid, "track": "asset_valuation", "level": "intermediate",
          "prerequisites": [], "markets": ["EU"], "status": status,
          "sources": [{"id": "s", "use": "background"}], "originality": "synthesized",
          "translations": {"zh": {"status": "none", "en_hash": None}}}
    (d / f"{cid}.md").write_text(render_concept(fm, "body\n"), encoding="utf-8")
    if with_lab:
        lab = root / "labs" / cid
        lab.mkdir(parents=True)
        (lab / "test_lab.py").write_text("def test_ok():\n    assert True\n")
    return root


def test_reviewed_requires_passing_lab(tmp_path):
    root = _concept(tmp_path)
    with pytest.raises(ValueError, match="lab"):
        set_status(root, "c1", "reviewed", labs_root=tmp_path / "labs")
    root = _concept(tmp_path, with_lab=True)
    set_status(root, "c1", "reviewed", labs_root=tmp_path / "labs")
    fm, _ = parse_concept(tmp_path / "concepts" / "asset_valuation" / "c1.md")
    assert fm["status"] == "reviewed"


def test_reviewed_refused_when_lab_test_fails(tmp_path):
    root = _concept(tmp_path, with_lab=True)
    (tmp_path / "labs" / "c1" / "test_lab.py").write_text(
        "def test_bad():\n    assert False\n")
    with pytest.raises(ValueError, match="lab tests failing"):
        set_status(root, "c1", "reviewed", labs_root=tmp_path / "labs")


def test_published_requires_signoff_en(tmp_path):
    root = _concept(tmp_path, with_lab=True)
    set_status(root, "c1", "reviewed", labs_root=tmp_path / "labs")
    with pytest.raises(ValueError, match="signoff.en"):
        set_status(root, "c1", "published", labs_root=tmp_path / "labs")


def test_backward_status_allowed_without_gates(tmp_path):
    root = _concept(tmp_path)
    set_status(root, "c1", "drafted", labs_root=tmp_path / "labs")
    fm, _ = parse_concept(tmp_path / "concepts" / "asset_valuation" / "c1.md")
    assert fm["status"] == "drafted"
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_authoring_status.py -q`
Expected: FAIL (`set_status` missing).

- [ ] **Step 3: Implement — append to `authoring.py`**

```python
import subprocess
import sys

from .concepts import render_concept

ORDER = ("stub", "drafted", "reviewed", "published")


def set_status(root, concept_id, status, labs_root=None) -> None:
    root = Path(root)
    path = find_concept(root, concept_id)
    fm, body = parse_concept(path)
    if status not in ORDER:
        raise ValueError(f"status must be one of {ORDER}")
    if ORDER.index(status) < ORDER.index(fm["status"]):
        raise ValueError(f"cannot move {fm['status']} -> {status}")
    if status == "reviewed":
        lab = Path(labs_root or root / "labs") / concept_id
        if not fm.get("no_lab_reason"):
            if not (lab / "test_lab.py").exists():
                raise ValueError("reviewed requires a lab (labs/<id>/test_lab.py) or no_lab_reason")
            r = subprocess.run([sys.executable, "-m", "pytest", str(lab), "-q"],
                               capture_output=True, text=True)
            if r.returncode != 0:
                raise ValueError("lab tests failing:\n" + r.stdout[-2000:])
    if status == "published" and not (fm.get("signoff") or {}).get("en"):
        raise ValueError("published requires signoff.en in front matter")
    fm["status"] = status
    path.write_text(render_concept(fm, body), encoding="utf-8")
```
CLI in `main()`:
```python
    ss = con.add_parser("set-status")
    ss.add_argument("concept_id")
    ss.add_argument("status", choices=["stub", "drafted", "reviewed", "published"])
    ss.set_defaults(fn=lambda a: auth.set_status(ROOT, a.concept_id, a.status))
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/authoring.py power-academy/academy/cli.py power-academy/tests/test_authoring_status.py
git commit -m "Add gate-enforced concept status transitions" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: `concept translate`

**Files:**
- Modify: `power-academy/academy/authoring.py`, `power-academy/academy/cli.py`
- Create: `power-academy/tests/test_authoring_translate.py`

**Interfaces:**
- Consumes: `academy.llm.call_json`, `academy.glossary.load_glossary/prompt_block`, `academy.concepts.body_hash/render_concept`.
- Produces: `authoring.translate_concept(root, concept_id, client, model) -> Path` (writes `<id>.zh.md`, stamps hash); CLI `concept translate <id>` (local, VPN on; model from `ACADEMY_EDITOR_MODEL` or default).

- [ ] **Step 1: Write the failing tests**

`power-academy/tests/test_authoring_translate.py`:
```python
import json

from academy.authoring import translate_concept
from academy.concepts import body_hash, parse_concept, render_concept
from academy.io import dump_yaml
from tests.fakes import FakeClient


def _setup(root):
    d = root / "concepts" / "asset_valuation"
    d.mkdir(parents=True, exist_ok=True)
    fm = {"id": "c1", "track": "asset_valuation", "level": "intermediate",
          "prerequisites": [], "markets": ["EU"], "status": "reviewed",
          "sources": [], "originality": "original",
          "translations": {"zh": {"status": "none", "en_hash": None}}}
    body = "## Intuition\nThe spark spread matters.\n"
    (d / "c1.md").write_text(render_concept(fm, body), encoding="utf-8")
    dump_yaml({"terms": [{"en": "spark spread", "zh": "火花价差"}]},
              root / "glossary" / "terms.yaml")
    return root, body


def test_translate_writes_zh_and_stamps_hash(tmp_path):
    root, body = _setup(tmp_path)
    c = FakeClient([json.dumps({"zh_body": "## 直觉\n火花价差很重要。\n"}, ensure_ascii=False)])
    out = translate_concept(root, "c1", c, "m")
    assert out.name == "c1.zh.md"
    _, zh_body = parse_concept(out)
    assert "火花价差" in zh_body
    fm, _ = parse_concept(root / "concepts" / "asset_valuation" / "c1.md")
    assert fm["translations"]["zh"] == {"status": "drafted", "en_hash": body_hash(body)}
    # glossary was injected into the prompt
    assert "火花价差" in c.calls[0]["system"]
```

- [ ] **Step 2: Run to verify failure**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests/test_authoring_translate.py -q`
Expected: FAIL.

- [ ] **Step 3: Implement — append to `authoring.py`**

```python
from .concepts import body_hash
from .glossary import load_glossary, prompt_block
from .llm import call_json

TRANSLATE_SYSTEM = (
    "Translate the following markdown section from English to Chinese for a "
    "power-markets quant curriculum. Keep all markdown structure, formulas and "
    "numbers unchanged. Return ONLY JSON: {\"zh_body\": \"<translated markdown>\"}.\n")


def translate_concept(root, concept_id, client, model) -> Path:
    root = Path(root)
    path = find_concept(root, concept_id)
    fm, body = parse_concept(path)
    terms = load_glossary(root / "glossary" / "terms.yaml")
    data = call_json(client, model, TRANSLATE_SYSTEM + prompt_block(terms), body,
                     max_tokens=8000)
    zh_path = path.with_name(path.stem + ".zh.md")
    zh_path.write_text(data["zh_body"], encoding="utf-8")
    fm["translations"]["zh"] = {"status": "drafted", "en_hash": body_hash(body)}
    path.write_text(render_concept(fm, body), encoding="utf-8")
    return zh_path
```
CLI in `main()`:
```python
    tr = con.add_parser("translate")
    tr.add_argument("concept_id")
    tr.set_defaults(fn=lambda a: print(auth.translate_concept(
        ROOT, a.concept_id, _client(), os.environ.get("ACADEMY_EDITOR_MODEL", OUTLINE_MODEL))))
```

- [ ] **Step 4: Run to verify pass**

Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests -q`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add power-academy/academy/authoring.py power-academy/academy/cli.py power-academy/tests/test_authoring_translate.py
git commit -m "Add glossary-locked concept translation with staleness hash" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: Batch 1 — asset_valuation foundations (6 concepts + anchor)

**Concepts:** `spark_dark_spread_fundamentals`, `heat_rate_and_plant_parameters`, `intrinsic_vs_extrinsic_value`, `stochastic_price_processes_for_power_and_fuel`, `spread_option_pricing_models` ⚓anchor, `real_options_framework_for_generation_assets`.

**Files per concept:** `concepts/asset_valuation/<id>.md` (body written), `labs/<id>/compute.py`, `labs/<id>/test_lab.py`.

**Lab specs (assertion targets):**
| Concept | Lab computes | Test asserts |
|---|---|---|
| spark_dark_spread_fundamentals | spark/dark/clean spreads from synthetic price, fuel, heat rate, emissions | signs + values vs hand-computed (e.g. spark = 0.5 − 8.5×0.04 etc. within 1e-9) |
| heat_rate_and_plant_parameters | heat-rate curve eval; hot/warm/cold start cost selection by offline hours | curve values at 3 load points; correct start tier picked at 6h/20h/60h offline |
| intrinsic_vs_extrinsic_value | perfect-foresight intrinsic on a toy 48h path; extrinsic = MC value − intrinsic | intrinsic equals LP result; extrinsic > 0 |
| stochastic_price_processes_for_power_and_fuel | OU + jump path simulation (seed=7) | sample mean reverts toward θ; jump count ~ Poisson(λT) ±20% |
| spread_option_pricing_models ⚓ | Margrabe closed form + Kirk approximation + MC (seed=11) | Kirk vs Margrabe within 1%; MC vs Margrabe within 2%; matches a paper table value within stated tolerance |
| real_options_framework_for_generation_assets | binomial-tree option to invest on toy GBM | tree value ≥ intrinsic; convergence vs closed-form within 2% |

- [ ] **Step 1: Author concept `spark_dark_spread_fundamentals`**
Run `concept pack spark_dark_spread_fundamentals`, write the EN body per `review/authoring_convention.md`, then `labs/spark_dark_spread_fundamentals/compute.py` + `test_lab.py`.
Verify: `~/.venvs/bess-platform/bin/python -m pytest labs/spark_dark_spread_fundamentals -q` PASS; `concept set-status spark_dark_spread_fundamentals drafted`.
- [ ] **Step 2–6: Same per remaining 5 concepts** (anchor: spread_option_pricing_models gets the fuller level-A lab).
- [ ] **Step 7: Batch verification**
Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests labs -q && ~/.venvs/bess-platform/bin/python -m academy.cli validate`
Expected: all pass; validate `ok`.
- [ ] **Step 8: Commit**
```bash
git add power-academy/concepts/asset_valuation power-academy/labs
git commit -m "Author asset_valuation batch 1 (6 concepts + spread option anchor lab)" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```
- [ ] **Step 9: ⛔ OWNER REVIEW GATE — stop.** Owner reviews the 6 files; fixes applied; owner approves; then `concept translate` each reviewed id (VPN on) and owner spot-checks ZH. Only then start Task 5.

---

### Task 5: Batch 2 — asset_valuation advanced (6 concepts, 2 anchors)

**Concepts:** `monte_carlo_and_lsm_for_generation_valuation`, `piecewise_replication_and_ccgt_intrinsic_modelling`, `power_price_spike_models_and_option_valuation`, `ccgt_investment_option_and_optimal_timing` ⚓, `tolling_agreement_structure_and_valuation` ⚓, `dispatch_optimisation_and_delta_hedging`.

**Lab specs:**
| Concept | Lab computes | Test asserts |
|---|---|---|
| monte_carlo_and_lsm_for_generation_valuation | LSM (Longstaff-Schwartz) exercise value on OU paths (seed=5) | LSM ≥ intrinsic; LSM vs nested-MC benchmark within 3% |
| piecewise_replication_and_ccgt_intrinsic_modelling | piecewise-linear replication portfolio vs direct dispatch intrinsic | replication error < 1% of intrinsic |
| power_price_spike_models_and_option_valuation | jump-diffusion option price (seed=3) | vs published benchmark within tolerance stated in concept |
| ccgt_investment_option_and_optimal_timing ⚓ | real-options entry threshold (Tseng-style) | threshold in the source-result band stated in the concept file |
| tolling_agreement_structure_and_valuation ⚓ | toll value = Σ hourly spread options − fixed costs | reproduces practice-model headline within tolerance (re-derived from outline/cache, stated in concept) |
| dispatch_optimisation_and_delta_hedging | DP dispatch with min up/down + start costs on synthetic week (seed=9) | DP profit ≥ any greedy heuristic; price-delta sensitivity sign correct |

Steps as Task 4 (author → lab → tests → drafted → batch suite+validate → commit → ⛔ owner gate → translate).
Commit message: `Author asset_valuation batch 2 (6 concepts + 2 anchor labs)`.

---

### Task 6: Batch 3 — hedging_trading foundations (6 concepts + anchor)

**Concepts:** `power_plant_economics_and_dispatch`, `forward_curve_structure_and_products`, `profit_at_risk_and_hedging_objective`, `baseload_peak_offpeak_delta_decomposition` ⚓, `delta_sensitivity_and_hedge_volume`, `gradual_linear_and_benchmark_hedging_strategies`.

**Lab specs:**
| Concept | Lab computes | Test asserts |
|---|---|---|
| power_plant_economics_and_dispatch | hourly run/stop decision vs spread with start cost | dispatch matches hand-derived rule on 24h toy path |
| forward_curve_structure_and_products | base/peak curve from synthetic monthly quotes + daily shaping | curve reprices input quotes to <1e-6; shaped profile sums match block energy |
| profit_at_risk_and_hedging_objective | PaR at 95% on simulated earnings (seed=13) | PaR equals empirical quantile of simulated distribution |
| baseload_peak_offpeak_delta_decomposition ⚓ | base/peak/offpeak delta ladder incl. incremental peak delta; AOM caveat case | decomposition sums to total delta; caveat case reproduces the documented pitfall |
| delta_sensitivity_and_hedge_volume | hedge volume from delta ladder | volumes match ladder; residual PaR reduced |
| gradual_linear_and_benchmark_hedging_strategies | linear vs benchmark hedge schedule on synthetic year (seed=17) | both reduce PaR vs unhedged; benchmark ≤ linear PaR on the synthetic set |

Steps as Task 4. Commit: `Author hedging_trading batch 3 (6 concepts + peak delta anchor lab)`.

---

### Task 7: Batch 4 — hedging_trading advanced (6 concepts + anchor)

**Concepts:** `rolling_intrinsic_strategy` ⚓, `static_vs_dynamic_hedging`, `option_greeks_for_power_derivatives`, `hedging_under_incomplete_markets_and_spike_dynamics`, `ppa_structures_and_route_to_market`, `tolling_and_asset_backed_trading`.

**Lab specs:**
| Concept | Lab computes | Test asserts |
|---|---|---|
| rolling_intrinsic_strategy ⚓ | rolling-intrinsic backtest on synthetic year (seed=19): daily re-optimise, take spread when positive | P&L within 5% of the strategy's own theoretical value on the same paths |
| static_vs_dynamic_hedging | outcome variance static vs dynamically rebalanced hedge on MC paths | dynamic variance < static on spike-heavy paths |
| option_greeks_for_power_derivatives | numeric delta/gamma/vega of a spark-spread option | Greeks match finite-difference of the same pricer; delta in [0,1] |
| hedging_under_incomplete_markets_and_spike_dynamics | hedge error when peak product unavailable (proxy hedge) | residual basis > 0 and quantified; proxy hedge still reduces PaR |
| ppa_structures_and_route_to_market | pay-as-produced vs baseload PPA cashflows on synthetic wind year | PaP revenue tracks volume×price; baseload imbalance cost > 0 |
| tolling_and_asset_backed_trading | toll vs merchant P&L distribution on shared MC paths | toll variance ≈ 0; merchant mean ≥ toll fee − risk premium check |

Steps as Task 4. Commit: `Author hedging_trading batch 4 (6 concepts + rolling intrinsic anchor lab)`.

---

### Task 8: ZH completion + final review

- [ ] **Step 1: Translate any remaining reviewed concepts** — `concept translate <id>` for all 24 (VPN on); owner spot-checks one ZH file per batch.
- [ ] **Step 2: Glossary growth** — append any new EN↔ZH terms coined during authoring to `glossary/terms.yaml`; `validate` must stay green.
- [ ] **Step 3: Full verification**
Run: `cd power-academy && ~/.venvs/bess-platform/bin/python -m pytest tests labs -q && ~/.venvs/bess-platform/bin/python -m academy.cli validate`
Expected: all pass; `ok`.
- [ ] **Step 4: Commit + final whole-branch review** (fresh reviewer subagent over the Phase 2 diff), fix pass for Critical/Important, then report.
```bash
git add power-academy/concepts power-academy/labs power-academy/glossary
git commit -m "Complete ZH translations for pilot tracks" -m "Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage:** §2 authoring standard → Task 1 convention + batch steps. §3 lab standards + anchors → Tasks 4–7 lab spec tables (every anchor carries a traceable assertion). §4 tooling → Tasks 1–3. §5 batch workflow → Tasks 4–7 each ending in an owner gate + translate step. §6 verification → per-task suites + Task 8. §7 risks → Review Focus + owner gates.

**Placeholder scan:** batch tasks specify every concept id and every lab assertion target; "author the body" steps name the convention file and the pack command. No TBD.

**Type consistency:** `build_pack(root, concept_id, max_excerpt)` / `find_concept` / `set_status(root, concept_id, status, labs_root)` / `translate_concept(root, concept_id, client, model)` used identically in tests and CLI lambdas. `ORDER` matches `STATUSES` in concepts.py. Lab folder convention `labs/<id>/test_lab.py` matches Task 2's gate.
