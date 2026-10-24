# Data engineering (OpenTrace)

Analytics engineering context for loading external agricultural and food-security data into BigQuery and transforming it for BI and downstream ML.

## Language

**LandingDev** (`landing_dev`):
Ingest-owned BigQuery dataset where load jobs write first (sandbox; partitions may expire).
_Avoid_: bronze landing, Airbyte landing, durable landing-as-primary-target

**RawDev** (`raw_dev`):
Ingest-owned, source-aligned BigQuery dataset. Ingestion writes it. Transformation treats it as the primordial source.
_Avoid_: bronze (as team language), “dbt-owned raw”

**StagingDev** (`staging_dev`):
Transform-owned cleaned entities and conformed grain after RawDev.
_Avoid_: silver (as team language)

**IntermediateDev** (`intermediate_dev`):
Transform-owned enrichment and business logic between staging and marts.

**MartDev** (`mart_dev`):
Transform-owned analytics-ready outputs for BI and consumers.
_Avoid_: gold (as team language)

**Ingestion**:
The module and jobs that write LandingDev and RawDev from external sources.
_Avoid_: Airbyte (retired from the active path)

**IngestNotebook**:
A notebook under `ingestion/notebooks/` used to try or demonstrate a load.
_Avoid_: data-pipelines (retired), top-level `notebooks/` for ingest, treating notebooks as production transforms

**IngestScript**:
A reusable runner under `ingestion/scripts/` (or Make) for repeatable loads.
_Avoid_: copying package internals into ad-hoc one-offs without a script

**IngestPackage**:
The installable CLI and source modules under `ingestion/package/` (`python -m ingestion`).
_Avoid_: presenting the package tree as the primary engineer entry point

**Transformation**:
The dbt pipeline from RawDev through MartDev (transform-v2 contract).
_Avoid_: medallion notebooks as production, bronze→silver→gold naming

**TransformV1**:
Baseline dbt pipeline frozen at git tag `transform-v1.0.0` before the ownership contract cut.

**TransformV2**:
Current dbt pipeline version (`dbt_project` 2.x, tag `transform-v2.*`) under the ingest-writes-raw / primordial-raw contract. Model SQL may match v1 at cutover.
