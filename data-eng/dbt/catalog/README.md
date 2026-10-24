# Generated dbt source catalog

`sources.generated.yml` is produced from the BigQuery schema catalog. It is **gitignored**.

```bash
cd data-eng
python data/local/scripts/generate_dbt_sources.py --refresh
```

`dbt_project.yml` includes this directory in `model-paths` so dbt loads the generated sources alongside hand-written YAML under `models/` (`sources_ingestion.yml`, `sources_geoboundaries.yml`, …).

Do not hand-edit the generated file. Do not commit it.
