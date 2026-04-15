# Phase 2: Composer + Airbyte VM (Option A networking)

Terraform module: `phase2_airbyte_composer`

Creates:

- Service account **`sa-airbyte-bq-writer`** (configurable) with:
  - project-level `roles/bigquery.jobUser`
  - dataset **`landing`**: `roles/bigquery.dataEditor`
- Secret **`AIRBYTE_API_TOKEN`** (empty placeholder — set the version out-of-band or add `google_secret_manager_secret_version`)
- IAM: Composer SA → `secretAccessor` on that secret
- Firewall: **Composer subnet CIDR → VM tag**, TCP port **8000** (configurable)

**Does not** create the GCE VM or install Airbyte — provision the VM separately and attach `airbyte_vm_network_tag`.

## Usage

 Instantiate from `environments/dev` or a prod root module with:

```hcl
module "phase2" {
  source = "../../modules/phase2_airbyte_composer"

  project_id                     = var.project_id
  landing_dataset_id            = "landing"
  composer_service_account_email = "composer-env@....iam.gserviceaccount.com"
  composer_subnet_cidr           = "10.1.0.0/22"  # example — use your Composer worker subnet
  network                        = "default"      # your VPC network name
}
```

After apply: download a JSON key for `airbyte_bq_writer` **only if** your Airbyte deployment requires key-based BigQuery auth; prefer workload identity where supported.
