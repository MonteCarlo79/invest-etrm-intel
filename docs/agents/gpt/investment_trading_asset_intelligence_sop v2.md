# SOP: Investment–Trading–Asset Intelligence System

## 1. Purpose

This SOP defines how to operate the investment–trading–asset intelligence system using:
- GPT
- OpenClaw
- Codex
- Claude Code

The goal is to turn the platform into a repeatable closed loop for:
1. ingesting and validating daily data
2. updating apps and analytical tables
3. generating daily / weekly / monthly reports
4. improving strategy, execution, and capital-allocation decisions

This SOP starts with two implementation tracks:
- Track A: Inner Mongolia BESS trading reports
- Track B: New app factory for file intake, dashboards, and recurring reports

---

## 2. Operating model

### GPT
Role:
- architecture
- business-logic definition
- SOP design
- task allocation
- realism review
- prompt/spec writing
- report template definition

### OpenClaw
Role:
- main operating interface
- workflow dispatcher
- status monitor
- build / run / validation coordinator
- report-run orchestrator
- handoff manager across tools

### Claude Code
Role:
- main implementation engine for substantial code work
- multi-file feature development
- app creation
- service-layer development
- report-generation pipelines
- refactors and integration work

### Codex
Role:
- daily operations coding engine
- invoice / PDF / Excel readers
- parsers
- ETL helpers
- recurring utility scripts
- low-cost bounded maintenance tasks

### Governance rule
- one AI tool writes to one active branch at a time
- OpenClaw routes and monitors work
- GPT decides allocation when ambiguous
- Claude Code owns major feature implementation
- Codex owns recurring operational coding tasks

---

## 3. Core SOP cycle

Every production workflow follows the same cycle:

1. **Data intake**
   - files arrive through ingestion routine, watched folder, upload path, or scheduled source pull
2. **Validation**
   - completeness, freshness, schema checks, duplication checks, file-quality checks
3. **Transformation**
   - parse Excel/PDF, map fields, normalize tables, enrich metadata
4. **Persistence**
   - store in marketdata / staging / reporting tables
5. **Application refresh**
   - app tables and service-layer outputs are refreshed
6. **Analytics generation**
   - KPI calculations, rankings, comparisons, anomalies, attribution logic
7. **Report generation**
   - daily / weekly / monthly report artifacts generated as PDF and exposed through app/download link
8. **Review and exception handling**
   - OpenClaw monitors health and flags failures
9. **Decision support**
   - outputs feed strategy, execution, and portfolio decisions

---

## 4. Track A: Inner Mongolia BESS daily reporting SOP

## 4.1 Objective

Produce recurring Inner Mongolia BESS reports covering all BESS assets and the most recent day’s performance across the key indicators.

Outputs required:
- daily report PDF
- weekly report PDF
- monthly report PDF
- downloadable links inside the app
- optional historical report archive

---

## 4.2 Business output definition

The report should answer:
- how did all BESS assets perform yesterday?
- who were top and bottom performers?
- what changed versus prior day / 7-day / MTD trend?
- where is monetisation strongest or weakest?
- are there data-quality gaps that reduce confidence?
- what deserves investigation by trading or asset teams?

---

## 4.3 Minimum report sections

### Daily report
1. report date and data freshness status
2. market summary for the latest day
3. fleet summary across all BESS assets
4. asset ranking table
5. KPI distribution and outlier section
6. daily vs prior-day comparison
7. operational/data-quality exceptions
8. key observations: observed / proxy-based / heuristic inference

### Weekly report
1. rolling 7-day summary
2. ranking stability and volatility
3. monetisation trend
4. consistency / dispersion analysis
5. exceptions and anomalies
6. weekly management note

### Monthly report
1. month-to-date and completed-month summary
2. asset league table
3. value capture and missed-value discussion
4. operational / strategy observations
5. recurring weaknesses and improvement opportunities
6. archive-ready management summary

---

## 4.4 Example KPI categories

Use the exact fields that already exist in your Inner Mongolia data model and app first; expand only after validation.

KPI categories may include:
- cleared charge/discharge energy
- gross margin / trading income / settlement outcome
- cycling-related indicators
- utilization / run days / dispatch participation
- spread capture / theoretical vs realised capture
- peer ranking metrics
- volatility / consistency metrics
- data completeness flags

Rule:
- start from observed available columns
- do not invent unsupported KPIs
- explicitly mark inferred metrics

---

## 4.5 Tool allocation for Track A

### GPT
- define report structure
- define KPI semantics and evidence labeling
- define management-summary wording style
- review whether reported conclusions are decision-useful

### OpenClaw
- schedule recurring runs
- monitor ingestion completion
- trigger report workflow only after freshness checks pass
- coordinate build / run / validation
- publish job status and error summaries

### Claude Code
- implement report-generation service modules
- add report pages / download links in app
- build PDF generation workflow
- add archive/index page if needed
- integrate service-layer calculations and front-end outputs

### Codex
- implement or maintain parsers / file collectors / helper scripts
- support recurring ETL utility work
- build small formatting scripts and housekeeping logic

---

## 4.6 Technical implementation phases for Track A

### Phase A1: Data and KPI foundation
Owner: Claude Code

Tasks:
- inspect current ingestion outputs and report-ready tables
- define canonical daily reporting dataset
- create service-layer query module for daily/weekly/monthly report inputs
- define KPI dictionary and availability map
- classify each KPI as observed / proxy / heuristic

Deliverables:
- report dataset query layer
- KPI dictionary file
- validation notes on missing columns / gaps

### Phase A2: Report calculation layer
Owner: Claude Code

Tasks:
- implement aggregation logic for daily / weekly / monthly summaries
- implement ranking, comparison, and anomaly logic
- produce JSON/report payload outputs from service layer

Deliverables:
- report payload generator
- comparison and ranking helpers
- unit-level sanity checks if practical

### Phase A3: PDF rendering and artifact generation
Owner: Claude Code

Tasks:
- implement PDF templates
- produce branded, readable report layouts
- save PDFs to predictable location
- expose file metadata and download paths

Deliverables:
- daily PDF generator
- weekly PDF generator
- monthly PDF generator
- artifact storage pattern

### Phase A4: App integration
Owner: Claude Code

Tasks:
- add report center page in Inner Mongolia app
- show latest report status
- show download links
- optionally show quick in-app HTML summary before PDF download

Deliverables:
- report center UI
- latest run status block
- download links and archive view

### Phase A5: Operationalization
Owner: OpenClaw

Tasks:
- define run order:
  1. ingestion
  2. validation
  3. table refresh
  4. report generation
  5. publish links
- set schedules for daily / weekly / monthly
- define rerun rules and failure notifications

Deliverables:
- scheduled workflow
- monitoring summary
- rerun SOP

---

## 4.7 Operational schedule recommendation for Track A

### Daily
Run after the ingestion and table-update process is confirmed complete.

Suggested sequence:
1. freshness check
2. data completeness check
3. daily KPI generation
4. PDF generation
5. publish download link
6. send status summary

### Weekly
Run once after the final daily ingestion of the reporting week is complete.

### Monthly
Run after month-close logic is stable enough for management use.
If settlement finalization lags, publish:
- preliminary monthly report
- final monthly report later if needed

---

## 4.8 Failure-handling SOP for Track A

If a daily report fails:
1. OpenClaw identifies failure stage
   - ingestion failure
   - parse failure
   - DB write failure
   - KPI logic failure
   - PDF rendering failure
   - app publishing failure
2. if operational only, OpenClaw reruns safely
3. if parser/helper issue, assign Codex
4. if feature/service-layer issue, assign Claude Code
5. if business meaning is unclear, escalate to GPT
6. report confidence must be downgraded if data completeness is partial

---

## 5. Track B: New app factory SOP

## 5.1 Objective

Create a repeatable pattern for launching new asset or workflow apps that:
- collect Excel/PDF files from a specific location
- parse and normalize the data
- present a dashboard
- generate recurring daily / weekly / monthly reports
- use one or more of the 4 business agents to interpret outputs

---

## 5.2 Standard app factory pattern

Every new app should follow this sequence:

1. **Source definition**
   - where do files come from?
   - who uploads them?
   - what is the naming convention?
   - what is the arrival frequency?

2. **Intake design**
   - watched folder / app upload / scheduled pull
   - file registry and dedup rules

3. **Parsing design**
   - Excel mapping
   - PDF extraction strategy
   - error handling and partial-read logic

4. **Staging and normalization**
   - raw landing table
   - cleaned staging table
   - reporting-ready table(s)

5. **Service layer**
   - app queries
   - KPI generation
   - report payload generation

6. **Dashboard layer**
   - overview page
   - drilldown page
   - report center
   - exception page

7. **Agent layer**
   - which of the 4 business agents should interpret outputs?

8. **Reporting layer**
   - daily / weekly / monthly PDFs
   - archive and download links

9. **Operational layer**
   - schedules
   - reruns
   - health checks
   - alerting

---

## 5.3 Which business agents should be used for new apps?

### Market Strategy & Investment Intelligence Agent
Use when the app helps identify where value pools exist.
Examples:
- region attractiveness
- node opportunity screening
- policy-driven market opportunity analysis

### Enterprise Portfolio, Risk & Capital Allocation Agent
Use when the app helps explain realised economics and risk.
Examples:
- portfolio performance dashboard
- asset/book P&L explain
- capital-allocation comparison

### Trading, Dispatch & Execution Agent
Use when the app helps improve monetisation and value capture.
Examples:
- daily dispatch performance
- strategy diagnostics
- settlement reconciliation
- realised vs theoretical opportunity

### Platform Reliability, Data Quality & Control Agent
Use when the app depends on reliable daily operational status.
Examples:
- ingestion health
- missing file alerts
- stale report tracking
- rerun queue management

Rule:
Most production apps will use at least:
- one business-facing agent
- plus the Platform Reliability, Data Quality & Control Agent

---

## 5.4 Tool allocation for new apps

### GPT
Use for:
- defining app purpose
- KPI/business logic definition
- deciding which business agents apply
- report template design
- prioritization and scope control

### OpenClaw
Use for:
- intake routing
- workflow scheduling
- operational validation
- status reporting
- controlled build/run/test coordination

### Claude Code
Use for:
- service layer
- app UI
- integration
- report-generation pipelines
- multi-file implementation

### Codex
Use for:
- file parsers
- extraction helpers
- ETL utility scripts
- recurring maintenance scripts
- low-cost bounded fixes

---

## 5.5 New app SOP template

For each new app, complete this design brief first:

### App brief
- app name
- business owner
- business question answered
- asset / market scope
- source files and format
- update frequency
- core KPIs
- required business agents
- output reports required
- operational SLA

### Engineering brief
- intake path
- parser approach
- staging tables
- reporting tables
- service modules
- UI pages
- schedules
- failure alerts
- archive retention

---

## 5.6 New app implementation phases

### Phase B1: discovery and scope
Owner: GPT
- define business goal
- define KPI list
- decide the relevant business agents
- define minimum lovable product

### Phase B2: intake and parser
Owner: Codex first, Claude Code if larger integration is needed
- file collector
- parser
- staging write
- validation checks

### Phase B3: app and analytics layer
Owner: Claude Code
- services
- dashboard pages
- report center
- download links

### Phase B4: operationalization
Owner: OpenClaw
- scheduling
- run order
- monitoring
- rerun rules

### Phase B5: review and expansion
Owner: GPT
- review usefulness
- trim noise
- improve management-readability
- decide next feature increment

---

## 6. SOP for task allocation

Use this decision logic:

### Send to Codex when
- task is parser-heavy
- task is fixed-format Excel/PDF extraction
- task is a recurring utility or helper script
- task is small maintenance work
- task is daily ops support coding

### Send to Claude Code when
- task spans multiple files
- task requires integration into app/service layers
- task creates or refactors a dashboard/reporting module
- task changes the report-generation architecture
- task needs higher implementation judgment

### Keep in GPT when
- KPI semantics are ambiguous
- business meaning matters more than coding speed
- prioritization or tradeoff is needed
- you need architecture or realism review

### Keep in OpenClaw when
- scheduling, orchestration, reruns, monitoring, or validation is the main need

---

## 7. Definition of done

A workflow is not done until all of the following are true:
- ingestion is reliable
- validation checks exist
- app queries are separated from UI where possible
- report outputs are downloadable
- evidence labels are explicit
- run order is scheduled
- rerun procedure is documented
- ownership across GPT / OpenClaw / Claude Code / Codex is clear

---

## 8. Immediate next implementation roadmap

### Priority 1: Inner Mongolia report center
1. define KPI dictionary
2. create report dataset service layer
3. implement daily report payload generator
4. implement PDF generation
5. add download page to app
6. schedule recurring runs in OpenClaw

### Priority 2: App factory template
1. define standard intake pattern for Excel/PDF files
2. define parser skeletons for Codex tasks
3. define dashboard/report center scaffold for Claude Code
4. define standard report template pack
5. define OpenClaw routing and monitoring rules

### Priority 3: Expand to the next app
Candidate examples:
- settlement/compensation PDF intake app
- invoice/document ingestion app
- portfolio P&L explain app
- wind or retail intelligence app

---

## 9. Recommended first concrete task packages

### Task Package 1: Inner Mongolia KPI and report dataset spec
Owner: GPT -> Claude Code

### Task Package 2: Inner Mongolia PDF report generation module
Owner: Claude Code

### Task Package 3: report scheduling and artifact publishing workflow
Owner: OpenClaw

### Task Package 4: standard Excel/PDF intake parser framework
Owner: Codex

### Task Package 5: new app starter scaffold
Owner: Claude Code

---

## 10. Git handoff SOP for every task

## 10.1 Purpose

This SOP ensures that AWS OpenClaw always works on the most up-to-date code even though it cannot access local folders on the machines running Claude Code and Codex.

The governing rule is:

**GitHub is the shared source of truth for all code handoffs between local coding agents and AWS OpenClaw.**

OpenClaw must never assume that local unpushed work exists.
Claude Code and Codex must never assume that AWS OpenClaw can see local folders.

---

## 10.2 Core handoff rule

Every implementation task must follow this path:

1. local coding agent works on a dedicated branch
2. local coding agent commits changes locally
3. local coding agent pushes branch to GitHub
4. local coding agent opens or updates a pull request if appropriate
5. task packet is updated with branch name and latest commit SHA
6. OpenClaw fetches the branch from GitHub
7. OpenClaw validates / runs / orchestrates on that GitHub branch
8. merge occurs only after checks and review gates are satisfied

Rule:
A task is **not handoff-ready** until the latest changes are pushed to GitHub.

---

## 10.3 Branch ownership model

### Claude Code
Owns branches for major implementation work such as:
- `feature/...`
- `refactor/...`
- larger `fix/...` branches tied to app/service/reporting logic

### Codex
Owns branches for daily operations coding such as:
- `ops/...`
- parser / ETL helper branches
- invoice / PDF / Excel extraction branches
- bounded maintenance or housekeeping branches

### OpenClaw
Does not act as the default primary coder.
It should normally:
- fetch and inspect branches
- validate builds/runs
- coordinate reruns
- comment on or summarize PRs
- escalate issues

OpenClaw may create its own small operational branches only when clearly necessary, but should not compete with Claude Code or Codex on their active write branches.

---

## 10.4 Protected branch policy

The repository should protect `main` using GitHub branch protection rules so that important branches cannot be force-pushed or merged without required checks and review. GitHub supports branch protection rules that can require pull requests, approvals, and passing status checks before merge. ([docs.github.com](https://docs.github.com/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches?utm_source=chatgpt.com))

Recommended `main` policy:
- no direct agent push to `main`
- require pull request before merge
- require at least one review
- require required status checks to pass
- disable force push
- disable branch deletion unless explicitly needed by repository admins

---

## 10.5 Pull request rule

Pull requests are the default merge vehicle because GitHub uses pull requests as the core mechanism for proposing, reviewing, and discussing code changes before merge. ([docs.github.com](https://docs.github.com/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/about-pull-requests?utm_source=chatgpt.com))

Rule:
- every non-trivial task should end in a PR
- direct branch-to-main pushes are forbidden
- PR description must include business goal, owner, risk, and validation status

---

## 10.6 Required handoff payload

Every task handoff from Claude Code or Codex to OpenClaw must include:

- `task_id`
- `owner_agent`
- `repo`
- `branch`
- `latest_commit_sha`
- `pr_url` if opened
- `business_goal`
- `patch_summary`
- `affected_files`
- `validation_done_locally`
- `known_risks`
- `next_expected_owner`

OpenClaw should refuse to proceed if:
- branch name is missing
- commit SHA is missing
- latest code has not been pushed
- task packet points to stale commit information

---

## 10.7 Standard agent workflow

### Claude Code workflow
Claude Code is available in terminal, IDE, desktop app, and browser, and is suited for codebase-aware multi-file implementation work. ([code.claude.com](https://code.claude.com/docs/en/overview?utm_source=chatgpt.com))

For each task:
1. create or switch to dedicated branch
2. implement code changes
3. run local checks where practical
4. commit changes
5. push to GitHub
6. open or update PR
7. update task packet
8. notify OpenClaw via task state / PR / run queue

### Codex workflow
Codex CLI runs locally from the terminal in the selected directory and can read, change, and run code on the machine where it is launched. The CLI and IDE extension also share configuration layers through `config.toml`. ([developers.openai.com](https://developers.openai.com/codex/cli/?utm_source=chatgpt.com))

For each task:
1. create or switch to dedicated ops/helper branch
2. implement bounded change
3. run local checks where practical
4. commit changes
5. push to GitHub
6. open or update PR if not trivial
7. update task packet
8. notify OpenClaw

### OpenClaw workflow
For each handoff:
1. read task packet
2. fetch from GitHub
3. checkout exact branch or commit SHA
4. run validation / orchestration / report generation / build checks
5. write run result
6. escalate to Claude Code, Codex, or GPT as needed

---

## 10.8 GitHub fetch rule for OpenClaw

OpenClaw should always work from GitHub, never from assumed local state.

Minimum fetch sequence:

```bash
git fetch origin
git checkout <branch>
git pull --ff-only origin <branch>
```

For validation-sensitive work, OpenClaw should also record the exact commit SHA being tested.

---

## 10.9 Status checks policy

GitHub status checks can be required on protected branches, and required checks must pass before merge into the protected branch. ([docs.github.com](https://docs.github.com/articles/about-status-checks?utm_source=chatgpt.com))

Recommended required checks:
- syntax / lint
- unit or smoke test
- app startup or build check where relevant
- OpenClaw operational validation check

If a check is not relevant for a given task, the PR should explain why.

---

## 10.10 PR template policy

The repository should use a PR template so every agent-generated PR has the same structure. GitHub supports repository pull-request templates to standardize submissions. ([docs.github.com](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/getting-started/managing-and-standardizing-pull-requests?utm_source=chatgpt.com))

Required PR fields:
- business goal
- task id
- branch owner
- affected files / modules
- local validation done
- runtime risks
- required OpenClaw validation
- merge recommendation

---

## 10.11 Definition of handoff-ready

A task is handoff-ready only when all of the following are true:
- branch exists on GitHub
- latest commit is pushed
- commit SHA is recorded
- PR is opened or consciously waived for trivial internal branch checks
- task packet is updated
- working tree is clean enough that the pushed code reflects the intended handoff

---

## 10.12 Definition of merge-ready

A task is merge-ready only when all of the following are true:
- PR exists
- required reviews are complete
- required checks are green
- OpenClaw validation has completed
- no unresolved business-logic ambiguity remains
- merge target is correct

---

## 10.13 Exception policy

### Allowed emergency exception
A temporary direct push to a non-protected branch may be allowed for urgent recovery work, but it must still be pushed to GitHub before OpenClaw acts on it.

### Forbidden exceptions
- asking OpenClaw to work on unpushed local code
- letting two agents write to the same active branch concurrently
- direct agent push to protected `main`
- merging without required checks when branch protection is intended to block it

---

## 10.14 Recommended branch naming convention

### Claude Code
- `feature/...`
- `refactor/...`
- `fix/...` for feature or service logic

### Codex
- `ops/...`
- `parser/...`
- `etl/...`
- `fix/...` for small operational maintenance

### OpenClaw if needed
- `ops-validate/...`
- `ops-rerun/...`

---

## 10.15 Windows-side push checklist for Claude Code and Codex

Before handoff to OpenClaw, the local agent operator should verify:

1. correct branch checked out
2. `git status` reviewed
3. intended files staged
4. commit message written clearly
5. `git push origin <branch>` completed successfully
6. branch visible on GitHub
7. PR created or updated
8. task packet updated with commit SHA

---

## 10.16 OpenClaw validation checklist

Before OpenClaw starts any run:

1. confirm repo and branch from task packet
2. fetch latest origin state
3. confirm checked-out commit SHA matches handoff packet
4. run required validation/build/report task
5. record result against the tested SHA
6. publish summary to task state or PR notes

---

## 10.17 Repo setup tasks to implement this SOP

1. enable branch protection on `main`
2. define required status checks
3. add PR template
4. define branch naming conventions in repo docs
5. update task packet format to include branch and commit SHA
6. make OpenClaw reject unpushed or branchless tasks

---

## 10.18 Standard command pattern

### Local coding agents
```bash
git checkout -b <branch>
# make changes
# run checks
git add .
git commit -m "<message>"
git push origin <branch>
```

### OpenClaw
```bash
git fetch origin
git checkout <branch>
git pull --ff-only origin <branch>
# run validation / build / report workflow
```

---

## 10.19 Governing principle

The system does not synchronize code through local folders.
It synchronizes code through GitHub branches, pull requests, commit SHAs, and validation checks.

---

## 11. Governing principle

This SOP exists to make the platform operate like a closed-loop intelligence system, not a collection of disconnected dashboards.

Every workflow should help the platform do one or more of the following better:
- identify value
- validate opportunity
- monetise assets and books
- explain realised outcomes
- improve future strategy and capital allocation

---

## 11. Execution artifact A: Inner Mongolia KPI dictionary

## 11.1 Purpose

This artifact defines the first production KPI dictionary for the Inner Mongolia BESS reporting workflow.

It is intentionally structured to separate:
- business meaning
- calculation source
- confidence / evidence level
- reporting usage

Rule:
Do not finalize the KPI list from memory alone.
Claude Code should inspect the actual available report-ready tables and app query outputs first, then confirm which KPIs are truly available.

---

## 11.2 KPI dictionary schema

Each KPI entry should contain the following fields:

- `kpi_code`
- `kpi_name`
- `business_definition`
- `grain`
  - asset-day / asset-week / asset-month / fleet-day / fleet-month
- `source_table`
- `source_fields`
- `calculation_logic`
- `unit`
- `directionality`
  - higher_is_better / lower_is_better / neutral
- `evidence_level`
  - observed / proxy-based / heuristic_inference
- `required_data_quality_checks`
- `null_handling_rule`
- `report_sections_used_in`
- `notes`

---

## 11.3 Minimum KPI groups

### Group A: Commercial outcome KPIs
Candidate examples:
- daily gross margin / revenue / settlement outcome
- month-to-date revenue / margin
- revenue per MW or per MWh where valid

### Group B: Dispatch and activity KPIs
Candidate examples:
- charge energy
- discharge energy
- number of active intervals
- participation days / active days
- utilization or throughput

### Group C: Strategy/value-capture KPIs
Candidate examples:
- spread capture
- realised vs theoretical capture
- value capture efficiency
- peer-relative monetisation score

### Group D: Stability and consistency KPIs
Candidate examples:
- day-over-day change
- 7-day consistency score
- volatility / dispersion measure
- rank stability

### Group E: Quality and confidence KPIs
Candidate examples:
- data completeness flag
- stale data flag
- anomalous value flag
- settlement completeness flag

Rule:
The first production release should use only KPIs that are directly supportable from existing Inner Mongolia tables or existing validated derivations.

---

## 11.4 First-pass KPI dictionary template

Use the following template format for implementation:

```yaml
inner_mongolia_kpis:
  - kpi_code: daily_discharge_mwh
    kpi_name: Daily Discharge Energy
    business_definition: Total cleared or realised discharge energy for an asset on the report date.
    grain: asset-day
    source_table: TBD_AFTER_TABLE_INSPECTION
    source_fields: [TBD]
    calculation_logic: Sum discharge energy for the asset and report date.
    unit: MWh
    directionality: neutral
    evidence_level: observed
    required_data_quality_checks:
      - report_date_present
      - asset_code_present
      - non_negative_value
    null_handling_rule: show_null_and_flag_missing
    report_sections_used_in:
      - daily_asset_table
      - daily_fleet_summary
    notes: confirm exact field naming from existing Inner Mongolia result tables

  - kpi_code: daily_margin_yuan
    kpi_name: Daily Margin
    business_definition: Daily realised or modeled commercial outcome attributable to the asset for the report date.
    grain: asset-day
    source_table: TBD_AFTER_TABLE_INSPECTION
    source_fields: [TBD]
    calculation_logic: TBD_AFTER_TABLE_INSPECTION
    unit: CNY
    directionality: higher_is_better
    evidence_level: observed
    required_data_quality_checks:
      - report_date_present
      - asset_code_present
    null_handling_rule: show_null_and_flag_missing
    report_sections_used_in:
      - daily_asset_table
      - top_bottom_performers
    notes: distinguish realised from proxy if this comes from modeled values

  - kpi_code: capture_vs_peer_rank
    kpi_name: Peer Capture Rank
    business_definition: Relative ranking of the asset against the Inner Mongolia BESS peer set for the selected period.
    grain: asset-day
    source_table: TBD_AFTER_TABLE_INSPECTION
    source_fields: [TBD]
    calculation_logic: Rank assets by selected monetisation KPI within the peer set.
    unit: rank
    directionality: lower_is_better
    evidence_level: proxy-based
    required_data_quality_checks:
      - peer_set_size_valid
      - ranking_metric_available
    null_handling_rule: suppress_if_peer_set_invalid
    report_sections_used_in:
      - daily_rank_table
      - weekly_rank_stability
    notes: confirm whether rank is based on realised metric or proxy metric
```

---

## 11.5 Required output for artifact A

Claude Code should produce:
1. a machine-readable KPI dictionary file
2. a short human-readable KPI glossary
3. a KPI availability table showing:
   - available now
   - derivable now
   - blocked / missing source field
4. explicit evidence labeling for every KPI

---

## 12. Execution artifact B: Claude Code task package for Inner Mongolia report generation

## 12.1 Objective

Implement the first production report-generation workflow for Inner Mongolia BESS daily / weekly / monthly reporting using the existing platform with minimal disruption.

---

## 12.2 Task owner

Primary owner: Claude Code

Support roles:
- GPT: KPI semantics / report structure review
- OpenClaw: validation and operationalization
- Codex: bounded helper/parser tasks only if needed

---

## 12.3 Branch rule

Claude Code should own a dedicated implementation branch.
Suggested branch name:
- `feature/inner-mongolia-report-center`

Do not share this active write branch with Codex or OpenClaw.

---

## 12.4 Business goal

Build a report center inside the Inner Mongolia app that can generate and expose daily / weekly / monthly PDF reports covering all BESS assets using the latest validated data.

---

## 12.5 Scope

In scope:
- inspect current Inner Mongolia report-ready data structures
- create report dataset service layer
- implement KPI aggregation for daily / weekly / monthly windows
- implement PDF generation pipeline
- add report center page to the app
- expose latest report metadata and download links
- support archive/index of prior reports if low effort

Out of scope for first version unless trivial:
- full redesign of existing app navigation
- major infra rewrite
- advanced LLM-authored commentary blocks
- speculative KPIs unsupported by the current schema

---

## 12.6 Expected file areas

Likely affected areas may include:
- `services/bess_inner_mongolia/`
- `apps/bess-inner-mongolia/im/`
- shared reporting / utility modules if needed
- storage path configuration for generated PDFs

Claude Code should preserve existing service-layer patterns and avoid burying logic directly in Streamlit pages.

---

## 12.7 Deliverables

### Deliverable 1: report dataset query layer
Implement a dedicated query/service layer that returns report-ready data by:
- report date
- date range
- asset
- fleet summary

### Deliverable 2: KPI payload generators
Implement payload builders for:
- daily report
- weekly report
- monthly report

Each payload should contain:
- metadata
- freshness flags
- fleet summary
- asset table
- ranking table
- anomaly / exception section
- evidence labels

### Deliverable 3: PDF rendering module
Implement a PDF generator that can output:
- daily PDF
- weekly PDF
- monthly PDF

Output expectations:
- readable layout
- clear title and date window
- summary section first
- tables and highlights next
- explicit data-quality / confidence notes

### Deliverable 4: report center UI
Add an Inner Mongolia app page showing:
- latest daily report
- latest weekly report
- latest monthly report
- download links
- last successful generation time
- failure or stale-data warning if applicable

### Deliverable 5: artifact metadata
Persist or expose metadata for generated reports, including:
- run timestamp
- report type
- date window covered
- generation status
- file path / URL

---

## 12.8 Implementation sequence

### Step 1: inspect current data model
- inspect actual report-ready tables and existing query functions
- document which KPI inputs are available now
- identify gaps without inventing new unsupported structures

### Step 2: define report payload contract
Create a stable internal payload structure for the three report types.

### Step 3: implement calculation layer
- asset-level KPI calculations
- fleet summary calculations
- ranking logic
- day-over-day / 7-day / month-to-date comparisons
- anomaly flags

### Step 4: implement PDF renderer
Prefer a maintainable renderer over a fragile visual hack.
The first version should prioritize readability and stable generation.

### Step 5: integrate app page
Expose current report artifacts and statuses in the Inner Mongolia app.

### Step 6: prepare for OpenClaw orchestration
Ensure report generation can be triggered by a stable entry point such as a script, service method, or command.

---

## 12.9 Required output format from Claude Code

For the implementation task, Claude Code should return:
- affected files
- patch summary
- assumptions
- runtime risks
- how to generate each report
- what OpenClaw still needs to validate operationally

---

## 12.10 Acceptance criteria

The task is accepted only if:
- daily report can be generated for the latest valid day
- weekly report can be generated from a rolling 7-day window
- monthly report can be generated for MTD or completed month
- PDFs are downloadable
- report center page renders successfully
- evidence labels are visible
- failures degrade cleanly and do not silently present bad data as valid

---

## 13. Execution artifact C: OpenClaw runbook for report scheduling and publication

## 13.1 Objective

Define how OpenClaw should orchestrate the Inner Mongolia reporting workflow safely and repeatedly.

---

## 13.2 OpenClaw role in this runbook

OpenClaw is the operating shell.
It should:
- monitor prerequisites
- trigger report generation only when valid
- publish status
- coordinate reruns
- escalate implementation issues to Claude Code or Codex
- escalate business-logic ambiguity to GPT

---

## 13.3 Workflow stages

### Stage 1: ingestion prerequisite check
Before any report generation, OpenClaw checks:
- latest ingestion job status
- expected source file presence if relevant
- row-count or freshness sanity indicators
- absence of critical ingestion errors

If this stage fails:
- mark report run as blocked
- do not publish stale report as if it were fresh
- raise operational exception

### Stage 2: table refresh / data readiness check
Check that the app/report tables required for reporting are updated and queryable.

If this stage fails:
- attempt safe rerun if operationally appropriate
- otherwise escalate

### Stage 3: report generation trigger
Trigger report generation in this order:
1. daily
2. weekly if scheduled date/time condition matches
3. monthly if scheduled date/time condition matches

### Stage 4: artifact verification
After generation, verify:
- PDF file exists
- file size is non-zero
- metadata written successfully
- app download path resolves correctly

### Stage 5: publication
Publish report metadata to the app/report index and optionally notify status channels if you later add them.

### Stage 6: summary and audit log
Record:
- run time
- report types attempted
- success/failure by type
- freshness state
- warnings
- rerun actions taken

---

## 13.4 Suggested scheduling logic

### Daily report schedule
Run only after the daily Inner Mongolia ingestion and table refresh are confirmed complete.

Suggested practical logic:
- ingestion completion event or polling-based readiness check
- then trigger daily report generation once per reporting day

### Weekly report schedule
Run once after the last reporting day of the chosen weekly window is available.

### Monthly report schedule
Run in two possible modes:
- preliminary MTD / month-close preview
- final monthly report after required close conditions are satisfied

---

## 13.5 Status model

Each report run should have one of these statuses:
- `queued`
- `running`
- `blocked`
- `failed`
- `published`
- `published_with_warnings`

Each status update should include:
- timestamp
- stage
- message
- next action

---

## 13.6 Rerun policy

### Safe auto-rerun allowed when
- transient file-read issue
- transient DB connectivity issue
- artifact write timing issue
- delayed but ultimately successful upstream completion

### Manual or escalated rerun required when
- source schema changed
- key KPI inputs are missing
- report payload logic broke
- PDF rendering repeatedly fails
- app publishing path is invalid

Routing rules:
- parser/helper issue -> Codex
- service/report-generation implementation issue -> Claude Code
- business interpretation or KPI meaning issue -> GPT

---

## 13.7 Exception messages

OpenClaw should use structured exception summaries:
- failure stage
- observed symptoms
- likely owner
- rerun possible? yes/no
- confidence in diagnosis
- business impact

Example:
- stage: artifact_verification
- observed: daily report PDF missing after successful payload generation
- likely_owner: claude_code
- rerun_possible: yes
- confidence: medium
- business_impact: no fresh daily report published

---

## 13.8 Minimum artifact registry fields

The report registry should track at least:
- report_id
- report_type
- date_window_start
- date_window_end
- generated_at
- status
- warnings
- file_path_or_url
- freshness_label
- source_run_reference

---

## 13.9 First production runbook sequence

1. check latest ingestion status
2. check required reporting tables
3. check freshness thresholds
4. trigger daily report generation
5. verify artifact exists
6. publish metadata and download link
7. if scheduled weekly/monthly window matches, run additional report types
8. write audit summary
9. expose operational status block in app if feasible

---

## 13.10 Immediate task tickets to create

### Ticket A
Name: Inspect Inner Mongolia report-ready tables and define KPI availability map
Owner: Claude Code

### Ticket B
Name: Implement Inner Mongolia report payload and PDF generation workflow
Owner: Claude Code

### Ticket C
Name: Add report center page with latest report links and status block
Owner: Claude Code

### Ticket D
Name: Define OpenClaw scheduling, run-state tracking, and rerun flow for report publication
Owner: OpenClaw

### Ticket E
Name: Build parser/helper utilities only if upstream file-intake gaps are discovered
Owner: Codex

