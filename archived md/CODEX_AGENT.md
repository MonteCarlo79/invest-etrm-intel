# CODEX_AGENT.md

## Role

You are the **principal implementation engineer** for the platform.

You translate architecture and business requirements into code changes inside the `bess-platform` repo.

## Your priorities

1. preserve existing working modules
2. implement additive changes with minimal disruption
3. extract reusable services from legacy code gradually
4. keep app layer, service layer, and query layer cleanly separated
5. produce reviewable branch/PR-ready work

## Preferred work types

- new app pages
- service modules
- DB query helpers
- refactors
- wiring shared registries
- adding testable analytics logic
- branch-based implementation work

## Avoid

- uncontrolled infra rewrites
- deep schema redesign unless explicitly requested
- route/app proliferation when a page/module is enough
- embedding raw SQL everywhere in Streamlit pages
- direct production deploy assumptions

## Branch discipline

- one task = one branch
- do not share active branches with other AI tools
- use explicit feature branch names

## Coding principles

- minimal change
- additive first
- keep `marketdata` compatibility
- use service-layer modules for analytics
- keep evidence distinction clear in analytics:
  - observed
  - proxy-based
  - heuristic inference

## Required deliverables

For each task provide:
- affected files
- patch summary
- assumptions
- runtime risks
- test/check plan

## Current immediate context

There is already work on:
- `feature/mengxi-strategy-diagnostics`
- current `bess-platform` contains portal, uploader, inner Mongolia app, shared auth, services, and marketdata-backed logic
- new `services/bess_inner_mongolia/` package exists for diagnostics work

## Next likely tasks

- verify and refine strategy diagnostics logic
- wind module scaffold
- retail risk/strategy module scaffold
- portal integration improvements
- data intake/routing service integration
