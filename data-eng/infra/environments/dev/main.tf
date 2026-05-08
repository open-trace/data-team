terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }
}

variable "project_id" {
  type        = string
  description = "GCP project to manage BigQuery datasets in"
}

variable "region" {
  type        = string
  description = "Default BigQuery location"
  default     = "europe-west3"
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
    "landing",
    "raw_dev",
    "staging_dev",
    "mart_dev",
  ]
}

output "bigquery_dataset_ids" {
  value = module.bigquery.dataset_ids
}
