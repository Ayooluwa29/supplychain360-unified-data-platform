resource "aws_iam_user" "pipeline_user" {
  count = var.create_data_engineer_user ? 1 : 0
  name  = var.data_engineer_username

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      DataLayer   = var.data_layer
    },
    var.additional_tags
  )
}

resource "aws_iam_access_key" "pipeline_user" {
  count = var.create_data_engineer_user ? 1 : 0
  user  = aws_iam_user.pipeline_user[0].name
}


resource "aws_iam_user_policy" "s3_raw_access" {
  count = var.create_data_engineer_user ? 1 : 0
  name  = "${var.project_name}-${var.data_layer}-s3-policy"
  user  = aws_iam_user.pipeline_user[0].name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListRawBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = ["arn:aws:s3:::${var.raw_bucket_name}"]
      },
      {
        Sid    = "ReadWriteRawObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = ["arn:aws:s3:::${var.raw_bucket_name}/*"]
      }
    ]
  })
}

resource "aws_ssm_parameter" "access_key_id" {
  count = var.create_data_engineer_user ? 1 : 0
  name  = "/${var.project_name}/${var.data_layer}/aws_access_key_id"
  type  = "SecureString"
  value = aws_iam_access_key.pipeline_user[0].id

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      DataLayer   = var.data_layer
    },
    var.additional_tags
  )
}

resource "aws_ssm_parameter" "secret_access_key" {
  count = var.create_data_engineer_user ? 1 : 0
  name  = "/${var.project_name}/${var.data_layer}/aws_secret_access_key"
  type  = "SecureString"
  value = aws_iam_access_key.pipeline_user[0].secret

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      DataLayer   = var.data_layer
    },
    var.additional_tags
  )
}

# etak user data_engineer_2
# =========================
resource "aws_iam_user" "engineer_2_user" {
  count = var.create_data_engineer_user ? 1 : 0
  name  = var.data_engineer_2_username

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      DataLayer   = var.data_layer
    },
    var.additional_tags
  )
}

# Enable Console Access
resource "aws_iam_user_login_profile" "engineer_2_login" {
  count                   = var.create_data_engineer_user ? 1 : 0
  user                    = aws_iam_user.engineer_2_user[0].name
  password_reset_required = true
}

# Allow User to Change Their Own Password
resource "aws_iam_user_policy_attachment" "engineer_2_change_password" {
  count      = var.create_data_engineer_user ? 1 : 0
  user       = aws_iam_user.engineer_2_user[0].name
  policy_arn = "arn:aws:iam::aws:policy/IAMUserChangePassword"
}

resource "aws_iam_access_key" "engineer_2_user" {
  count = var.create_data_engineer_user ? 1 : 0
  user  = aws_iam_user.engineer_2_user[0].name
}


resource "aws_iam_user_policy" "engineer_2_s3_raw_access" {
  count = var.create_data_engineer_user ? 1 : 0
  name  = "${var.project_name}-${var.data_layer}-permission-policy"
  user  = aws_iam_user.engineer_2_user[0].name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "arn:aws:s3:::${var.raw_etak_bucket_name}",
          "arn:aws:s3:::${var.raw_etak_bucket_name}/*"
        ]
      },
      {
        Sid    = "CreateWithTags"
        Effect = "Allow"
        Action = [
          "iam:CreateUser",
          "iam:TagUser",
          "s3:CreateBucket",
          "s3:PutBucketTagging"
        ]
        Resource = "*"
        Condition = {
          "StringEquals" = { "aws:RequestTag/Owner" = "Engineer2" }
        }
      },
      {
        Sid    = "ManageOwnResources"
        Effect = "Allow"
        Action = [
          "iam:DeleteUser", "iam:GetUser", "iam:UpdateUser",
          "iam:CreateAccessKey", "iam:DeleteAccessKey",
          "iam:AttachUserPolicy", "iam:PutUserPolicy",
          "s3:DeleteBucket", "s3:GetBucketLocation"
        ]
        Resource = "*"
        Condition = {
          "StringEquals" = { "aws:ResourceTag/Owner" = "Engineer2" }
        }
      }
    ]
  })
}

resource "aws_ssm_parameter" "engineer_2_access_key_id" {
  count = var.create_data_engineer_user ? 1 : 0
  name  = "/${var.project_name}/${var.data_layer}/engineer_2/aws_access_key_id"
  type  = "SecureString"
  value = aws_iam_access_key.engineer_2_user[0].id

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      DataLayer   = var.data_layer
    },
    var.additional_tags
  )
}

resource "aws_ssm_parameter" "engineer_2_secret_access_key" {
  count = var.create_data_engineer_user ? 1 : 0
  name  = "/${var.project_name}/${var.data_layer}/engineer_2/aws_secret_access_key"
  type  = "SecureString"
  value = aws_iam_access_key.engineer_2_user[0].secret

  tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      DataLayer   = var.data_layer
    },
    var.additional_tags
  )
}

# Output the Console Password
output "engineer_2_console_password" {
  description = "The temporary password for console login"
  value       = try(aws_iam_user_login_profile.engineer_2_login[0].password, "N/A")
  sensitive   = true
}