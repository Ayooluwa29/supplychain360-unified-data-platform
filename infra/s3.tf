resource "aws_s3_bucket" "raw_data" {
  bucket        = var.raw_bucket_name
  force_destroy = var.force_destroy_buckets

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      DataLayer   = var.data_layer
      Purpose     = "raw-data-ingestion"
    },
    var.additional_tags
  )
}

resource "aws_s3_bucket_versioning" "raw_versioning" {
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_encryption" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.raw_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw_block" {
  bucket                  = aws_s3_bucket.raw_data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    id     = "${var.project_name}-${var.data_layer}-lifecycle"
    status = "Enabled"

    filter {}

    transition {
      days          = var.lifecycle_standard_ia_days
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = var.lifecycle_glacier_days
      storage_class = "GLACIER"
    }

    transition {
      days          = var.lifecycle_deep_archive_days
      storage_class = "DEEP_ARCHIVE"
    }

    expiration {
      days = var.lifecycle_expiration_days
    }
  }
}

# etak raw bucket
# ============================================================================

resource "aws_s3_bucket" "raw_etak_data" {
  bucket        = var.raw_etak_bucket_name
  force_destroy = var.force_destroy_buckets

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      DataLayer   = var.data_layer
      Purpose     = "raw-etak-data-ingestion"
    },
    var.additional_tags
  )
}

resource "aws_s3_bucket_versioning" "raw_etak_versioning" {
  bucket = aws_s3_bucket.raw_etak_data.id

  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Suspended"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_etak_encryption" {
  count  = var.enable_encryption ? 1 : 0
  bucket = aws_s3_bucket.raw_etak_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw_etak_block" {
  bucket                  = aws_s3_bucket.raw_etak_data.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_etak_lifecycle" {
  bucket = aws_s3_bucket.raw_etak_data.id

  rule {
    id     = "${var.project_name}-${var.data_layer}-lifecycle"
    status = "Enabled"

    filter {}

    transition {
      days          = var.lifecycle_standard_ia_days
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = var.lifecycle_glacier_days
      storage_class = "GLACIER"
    }

    transition {
      days          = var.lifecycle_deep_archive_days
      storage_class = "DEEP_ARCHIVE"
    }

    expiration {
      days = var.lifecycle_expiration_days
    }
  }
}