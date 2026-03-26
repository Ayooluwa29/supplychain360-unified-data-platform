terraform {
  backend "s3" {
    bucket       = "supplychain360-butstrp-terraform-state"
    key          = "supplychain360/raw/terraform.tfstate"
    region       = "eu-west-2"
    use_lockfile = true
    profile      = "terraform"
  }
}