
variable "project_name" {
  description = "Project name for resource naming"
  type        = string
}

variable "environment" {
  description = "Environment name (dev)"
  type        = string
}

variable "data_layer" {
  description = "Data pipeline layer (raw, staging, curated)"
  type        = string
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
}

# Tagging
variable "additional_tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}

# S3 Bucket Configuration
variable "raw_bucket_name" {
  description = "S3 bucket name for raw data"
  type        = string
}

variable "raw_etak_bucket_name" {
  description = "S3 bucket name for raw data"
  type        = string
}

variable "enable_versioning" {
  description = "Enable S3 bucket versioning"
  type        = bool
  default     = true
}

variable "enable_encryption" {
  description = "Enable S3 bucket encryption"
  type        = bool
  default     = true
}

# Force destroy (use with caution)
variable "force_destroy_buckets" {
  description = "Allow destroying buckets with content (DANGEROUS)"
  type        = bool
  default     = false
}

# Lifecycle Configuration
variable "lifecycle_standard_ia_days" {
  description = "Days before transitioning to STANDARD_IA"
  type        = number
  default     = 30
}

variable "lifecycle_glacier_days" {
  description = "Days before transitioning to Glacier"
  type        = number
  default     = 90
}

variable "lifecycle_deep_archive_days" {
  description = "Days before transitioning to Glacier Deep Archive"
  type        = number
  default     = 180
}

variable "lifecycle_expiration_days" {
  description = "Days before expiring objects"
  type        = number
  default     = 365
}

# IAM Configuration
variable "create_data_engineer_user" {
  description = "Whether to create data engineer IAM user"
  type        = bool
  default     = true
}

variable "data_engineer_username" {
  description = "Username for data engineer IAM user"
  type        = string
}

variable "data_engineer_2_username" {
  description = "Username for data engineer IAM user"
  type        = string
}

variable "profile" {
  description = "AWS CLI profile to use"
  type        = string
}