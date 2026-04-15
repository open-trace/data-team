terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

module "bigquery" {
  source = "../../modules/bigquery"

  project_id = var.project_id
  location   = var.region
  dataset_ids = [
    "raw_prod",
    "staging_prod",
    "mart_prod",
  ]
}

output "bigquery_dataset_ids" {
  value = module.bigquery.dataset_ids
}

