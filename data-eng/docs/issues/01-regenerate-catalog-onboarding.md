## Parent

PRD: data-eng transform-v2 reorganization (`docs/prd/transform-v2-reorg.md`)

## What to build

Make post-clone onboarding reliably regenerate `dbt/catalog/sources.generated.yml` and document the one command in first-time setup / Makefile so a fresh clone can run dbt without the old mega `models/sources.yml`.

## Acceptance criteria

- [ ] `make` (or documented script) runs `generate_dbt_sources.py --refresh` when `BQ_PROJECT` is set
- [ ] FIRST_TIME_SETUP and dbt README both point only at `catalog/sources.generated.yml`
- [ ] No instructions tell engineers to hand-edit generated sources

## Blocked by

None - can start immediately
