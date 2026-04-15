variable "project_id" {
  type        = string
  description = "GCP project ID (BigQuery datasets are created here)."
}

variable "location" {
  type        = string
  description = "BigQuery dataset location (e.g. europe-west3)."
  default     = "europe-west3"
}

variable "dataset_labels" {
  type        = map(string)
  description = "Labels applied to every dataset."
  default     = {}
}

variable "dataset_ids" {
  type        = list(string)
  description = "BigQuery dataset IDs to manage in this Terraform state."
}

locals {
  datasets = toset(var.dataset_ids)
}

resource "google_bigquery_dataset" "pipeline" {
  for_each   = local.datasets
  project    = var.project_id
  dataset_id = each.key
  location   = var.location

  labels = var.dataset_labels

  lifecycle {
    prevent_destroy = true
  }
}

output "dataset_ids" {
  description = "Created dataset IDs"
  value       = [for d in google_bigquery_dataset.pipeline : d.dataset_id]
}
