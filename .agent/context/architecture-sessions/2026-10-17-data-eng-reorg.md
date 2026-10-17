---
type: architecture-session
engagement: migration
slug: data-eng-reorg
started: 2026-10-17
status: complete
decision_map: ""
---

# Architecture Session: Data-eng reorg (transform-v2)

## Goal

Reorganize `data-eng/` so day-to-day work follows one ownership story: Python ingestion writes `landing_dev` and `raw_dev`; dbt treats `raw_dev` as the primordial source through `mart_dev`; Airbyte is removed from the active path; cut **transform-v2** without rewriting existing dbt model SQL.

## Engagement

- Type: migration (from as_is / target_design)
- Roles run: context-and-constraints, domain-architecture, data-architecture, application-architecture (light)

## Timeline

- Opened session — ownership locked (ingest writes `landing_dev` + `raw_dev`; transform `raw_dev` → `mart_dev`)
- Soft migration rules — docs/layout/version cut before SQL
- Baseline tag `transform-v1.0.0`; contract cut `transform-v2.0.0`
- Synthesis + ADRs + PRD/issues handoff

## Role outputs

### Context and constraints

- Working tree already mid-migration from bronze naming to `*_dev`.
- Generated `dbt/models/sources.yml` (~37MB) blocks IDE/agent workflow.
- Stale Airbyte-first docs contradict Python `ingestion/` package.
- Constraint: do not rewrite staging/intermediate/mart SQL; leave landing/raw_dev dbt runners in place.

### Domain architecture

- Glossary terms pinned in `data-eng/CONTEXT.md`.
- `raw_dev` is written by ingestion and is primordial for transformation.

### Data architecture

- Flow: external → ingest → `landing_dev` → `raw_dev` → dbt → `staging_dev` → `intermediate_dev` → `mart_dev`.
- Dataset `landing` (no `_dev`) left untouched in dbt for soft cutover.

### Application architecture

- Deep modules: `ingestion/`, `dbt/` (versioned), `local/` (path rename deferred if risky), `infra/`, `ci/`.
- Airbyte tree archived; not an active load path.

## Cross-cutting synthesis

- Risks: mega sources.yml removal from git requires regenerate-on-clone; Airflow DAGs that called Airbyte need archive/replace later.
- Open questions: none blocking v2.0.0 contract cut.
- Recommended ADRs: transform-v2 ownership; Python ingest only; generated catalog placement.
- CONTEXT.md: `data-eng/CONTEXT.md`.

## Links

- ADRs: `data-eng/docs/adr/`
- CONTEXT: `data-eng/CONTEXT.md`

## Next step

Publish PRD + vertical-slice issues on the GitHub tracker (`ready-for-agent`).
