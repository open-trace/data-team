# `dev` Terraform root module

Creates BigQuery datasets via [`../../modules/bigquery`](../../modules/bigquery).

```bash
cp terraform.tfvars.example terraform.tfvars   # set project_id — file is gitignored
terraform init
terraform plan
terraform apply
```

**Phase 2** (Airbyte VM + Composer): instantiate [`../../modules/phase2_airbyte_composer`](../../modules/phase2_airbyte_composer) here after datasets exist; see module README for required variables (`composer_service_account_email`, `composer_subnet_cidr`, `network`).
