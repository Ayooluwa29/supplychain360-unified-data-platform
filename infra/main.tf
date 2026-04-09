terraform {
  required_version = ">= 1.10.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.profile
}

provider "google" {
  project     = var.gcp_project_id
  region      = var.gcp_region
  credentials = file(var.gcp_credentials_file)
}