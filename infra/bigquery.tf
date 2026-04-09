# Dataset
resource "google_bigquery_dataset" "dw" {
  dataset_id                 = var.bq_dataset_id
  friendly_name              = "${var.project} multi layer"
  description                = var.bq_dataset_description
  location                   = var.bq_location
  delete_contents_on_destroy = var.delete_contents_on_destroy

  labels = merge(
    {
      project     = var.project_name
      environment = var.environment
    },
    var.additional_tags
  )

  lifecycle {
    prevent_destroy = true
    ignore_changes  = [labels]
  }
}

# Service account for Airbyte and Airflow to load data into BigQuery
resource "google_service_account" "pipeline_sa" {
  account_id   = "${var.project_name}-pipeline-sa"
  display_name = "${var.project_name} pipeline service account"
  description  = "Used by Airbyte and Airflow to load data into BigQuery and dbt for transformation"
}

# Grant the service account BigQuery Data Editor on the dataset
resource "google_bigquery_dataset_iam_member" "pipeline_sa_editor" {
  dataset_id = google_bigquery_dataset.dw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Grant BigQuery Job User at project level so it can run load jobs
resource "google_project_iam_member" "pipeline_sa_job_user" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Generate and store the service account key
resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
}

# Store the key in AWS SSM so Airflow and Airbyte can retrieve it
resource "aws_ssm_parameter" "bq_service_account_key" {
  name  = "/${var.project_name}/bq_service_account_key"
  type  = "SecureString"
  value = base64decode(google_service_account_key.pipeline_sa_key.private_key)

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
    },
    var.additional_tags
  )
}