variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "region" {
  type        = string
  default     = "europe-west3"
  description = "Used for provider default region."
}

# Airbyte → BigQuery (landing only)
variable "landing_dataset_id" {
  type        = string
  default     = "landing"
  description = "Dataset Airbyte writes to."
}

variable "airbyte_bq_writer_sa_id" {
  type        = string
  default     = "sa-airbyte-bq-writer"
  description = "Service account ID (email prefix) for BigQuery destination in Airbyte."
}

variable "composer_service_account_email" {
  type        = string
  description = "Composer environment worker / scheduler SA email (for Secret Manager accessor)."
}

variable "composer_subnet_cidr" {
  type        = string
  description = "CIDR of the subnet where Composer workers run (source range for firewall to Airbyte VM)."
}

variable "airbyte_vm_network_tag" {
  type        = string
  default     = "airbyte-server"
  description = "Network tag on the GCE VM running Airbyte."
}

variable "airbyte_api_port" {
  type        = number
  default     = 8000
  description = "TCP port exposed for Airbyte API (Option A internal access)."
}

variable "create_airbyte_api_token_secret" {
  type        = bool
  default     = true
  description = "Create a Secret Manager secret placeholder for the Airbyte API token."
}

variable "network" {
  type        = string
  description = "VPC network name (same project) for the firewall rule."
}

resource "google_service_account" "airbyte_bq_writer" {
  project      = var.project_id
  account_id   = var.airbyte_bq_writer_sa_id
  display_name = "Airbyte BigQuery writer (landing only)"
}

resource "google_project_iam_member" "airbyte_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airbyte_bq_writer.email}"
}

resource "google_bigquery_dataset_iam_member" "airbyte_landing_editor" {
  project    = var.project_id
  dataset_id = var.landing_dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.airbyte_bq_writer.email}"
}

resource "google_secret_manager_secret" "airbyte_api_token" {
  count     = var.create_airbyte_api_token_secret ? 1 : 0
  project   = var.project_id
  secret_id = "AIRBYTE_API_TOKEN"

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_iam_member" "composer_reads_airbyte_token" {
  count     = var.create_airbyte_api_token_secret ? 1 : 0
  project   = var.project_id
  secret_id = google_secret_manager_secret.airbyte_api_token[0].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${var.composer_service_account_email}"
}

resource "google_compute_firewall" "composer_to_airbyte_api" {
  name    = "composer-to-airbyte-api"
  network = var.network
  project = var.project_id

  description = "Option A: allow Composer subnet to reach Airbyte API on the VM (internal only)."

  source_ranges = [var.composer_subnet_cidr]
  target_tags   = [var.airbyte_vm_network_tag]

  allow {
    protocol = "tcp"
    ports    = [tostring(var.airbyte_api_port)]
  }
}

output "airbyte_bq_writer_email" {
  value = google_service_account.airbyte_bq_writer.email
}

output "airbyte_api_token_secret_id" {
  value = var.create_airbyte_api_token_secret ? google_secret_manager_secret.airbyte_api_token[0].id : null
}
