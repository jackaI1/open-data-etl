mock_provider "google" {}

variables {
  project = "sample-project"
  bucket  = "sample-dpd-data"
  region  = "us-central1"
}

run "private_storage_and_scoped_identities" {
  command = plan

  assert {
    condition = (
      google_storage_bucket.data.uniform_bucket_level_access &&
      google_storage_bucket.data.public_access_prevention == "enforced" &&
      !google_storage_bucket.data.force_destroy
    )
    error_message = "Catalogue storage must block public access and accidental data deletion."
  }
  assert {
    condition     = google_storage_bucket_iam_member.batch_write.role == "roles/storage.objectCreator"
    error_message = "Batch must create attempts without overwrite or delete access."
  }
  assert {
    condition = (
      google_storage_bucket_iam_member.report.role == "roles/storage.objectViewer" &&
      strcontains(google_storage_bucket_iam_member.report.condition[0].expression, "endsWith('/report.json')")
    )
    error_message = "The web service must only read reports."
  }
  assert {
    condition     = google_compute_subnetwork.batch.private_ip_google_access
    error_message = "The Batch subnet must enable access to Google APIs."
  }
}
