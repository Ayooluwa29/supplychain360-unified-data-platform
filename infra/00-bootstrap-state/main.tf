provider "aws" {
  region  = "eu-west-2"
  profile = "terraform"
}


resource "aws_s3_bucket" "terraform_state" {
  bucket = "supplychain360-butstrp-terraform-state"

}