terraform {
  required_version = ">= 1.9, < 2.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

variable "project" { type = string }
variable "bucket" { type = string }
variable "region" {
  type    = string
  default = "us-central1"
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_project_service" "api" {
  for_each = toset([
    "batch.googleapis.com", "compute.googleapis.com", "run.googleapis.com",
    "artifactregistry.googleapis.com", "storage.googleapis.com", "logging.googleapis.com",
    "iam.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "data" {
  name                        = var.bucket
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  depends_on                  = [google_project_service.api]
}

resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = "dpd-etl"
  format        = "DOCKER"
  depends_on    = [google_project_service.api]
}

resource "google_service_account" "batch" {
  account_id   = "dpd-batch"
  display_name = "Drug catalogue Batch worker"
  depends_on   = [google_project_service.api]
}
resource "google_service_account" "report" {
  account_id   = "dpd-report"
  display_name = "Drug catalogue read-only reports"
  depends_on   = [google_project_service.api]
}
resource "google_project_iam_member" "batch" {
  for_each = toset(["roles/batch.agentReporter", "roles/logging.logWriter"])
  project  = var.project
  role     = each.value
  member   = "serviceAccount:${google_service_account.batch.email}"
}
resource "google_artifact_registry_repository_iam_member" "pull" {
  location   = var.region
  repository = google_artifact_registry_repository.images.name
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.batch.email}"
}
resource "google_storage_bucket_iam_member" "batch_read" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.batch.email}"
}
resource "google_storage_bucket_iam_member" "batch_write" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.batch.email}"
  condition {
    title      = "attempt_outputs_only"
    expression = "resource.name.startsWith('projects/_/buckets/${var.bucket}/objects/runs/')"
  }
}
resource "google_storage_bucket_iam_member" "report" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.report.email}"
  condition {
    title      = "reports_only"
    expression = "resource.name.startsWith('projects/_/buckets/${var.bucket}/objects/runs/') && resource.name.endsWith('/report.json')"
  }
}
resource "google_compute_network" "batch" {
  name                    = "dpd-batch"
  auto_create_subnetworks = false
  depends_on              = [google_project_service.api]
}
resource "google_compute_subnetwork" "batch" {
  name                     = "dpd-batch"
  ip_cidr_range            = "10.80.0.0/24"
  region                   = var.region
  network                  = google_compute_network.batch.id
  private_ip_google_access = true
}

output "bucket" { value = google_storage_bucket.data.name }
output "batch_identity" { value = google_service_account.batch.email }
output "report_identity" { value = google_service_account.report.email }
