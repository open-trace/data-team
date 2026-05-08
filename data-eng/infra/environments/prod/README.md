# `prod` Terraform root module

Manages the **production** transformation datasets in the same GCP project:

- `raw_prod`
- `staging_prod`
- `mart_prod`

(`landing` is managed by `environments/dev` by design.)

## Usage

```bash
cd data-eng/infra/environments/prod
cp terraform.tfvars.example terraform.tfvars   # set project_id — file is gitignored
terraform init
terraform plan -var='project_id=opentrace-prod-5ga4'
```

If the datasets already exist, import them into **prod state**:

```bash
terraform import -var='project_id=opentrace-prod-5ga4' \
  'module.bigquery.google_bigquery_dataset.pipeline["raw_prod"]' \
  projects/opentrace-prod-5ga4/datasets/raw_prod

terraform import -var='project_id=opentrace-prod-5ga4' \
  'module.bigquery.google_bigquery_dataset.pipeline["staging_prod"]' \
  projects/opentrace-prod-5ga4/datasets/staging_prod

terraform import -var='project_id=opentrace-prod-5ga4' \
  'module.bigquery.google_bigquery_dataset.pipeline["mart_prod"]' \
  projects/opentrace-prod-5ga4/datasets/mart_prod
```

Then rerun `terraform plan` and ensure it shows **0 to add / 0 to destroy**.

