# S3 Outputs
output "raw_bucket_name" {
  description = "Name of the raw ingestion bucket"
  value       = aws_s3_bucket.raw_data.id
}

output "raw_bucket_arn" {
  description = "ARN of the raw ingestion bucket"
  value       = aws_s3_bucket.raw_data.arn
}

output "raw_bucket_region" {
  description = "Region where the raw bucket is deployed"
  value       = aws_s3_bucket.raw_data.region
}


# IAM Outputs
output "pipeline_user_name" {
  description = "IAM username for the data engineer"
  value       = var.create_data_engineer_user ? aws_iam_user.pipeline_user[0].name : null
}

output "pipeline_user_arn" {
  description = "ARN of the data engineer IAM user"
  value       = var.create_data_engineer_user ? aws_iam_user.pipeline_user[0].arn : null
}


# SSM Outputs
output "ssm_key_id_path" {
  description = "SSM path to retrieve the access key ID"
  value       = var.create_data_engineer_user ? aws_ssm_parameter.access_key_id[0].name : null
}

output "ssm_secret_path" {
  description = "SSM path to retrieve the secret access key"
  value       = var.create_data_engineer_user ? aws_ssm_parameter.secret_access_key[0].name : null
}

# BigQuery Outputs
output "bq_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.dw.dataset_id
}

output "bq_dataset_location" {
  description = "BigQuery dataset location"
  value       = google_bigquery_dataset.dw.location
}

output "pipeline_sa_email" {
  description = "Service account email for the pipeline"
  value       = google_service_account.pipeline_sa.email
}

output "bq_sa_key_ssm_path" {
  description = "SSM path to retrieve the BigQuery service account key"
  value       = aws_ssm_parameter.bq_service_account_key.name
}