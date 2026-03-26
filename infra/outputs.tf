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