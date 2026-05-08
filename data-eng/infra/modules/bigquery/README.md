# BigQuery datasets

Creates the standard pipeline datasets:

- `landing` (shared ingestion from Airbyte)
- Dev: `raw_dev`, `staging_dev`, `mart_dev`
- Prod: `raw_prod`, `staging_prod`, `mart_prod`

## Usage

See [`environments/dev/main.tf`](../environments/dev/main.tf) for module wiring.

```bash
cd data-eng/infra/environments/dev
cp terraform.tfvars.example terraform.tfvars   # set project_id
terraform init
terraform plan
```

Apply **before** [`../phase2_airbyte_composer`](../phase2_airbyte_composer/README.md) so dataset `landing` exists for IAM binding (or rely on dataset IAM by ID if created elsewhere).
