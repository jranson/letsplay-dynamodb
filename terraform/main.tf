terraform {
  required_version = ">= 1"
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

# allows terraform to interpolate the account_id
data "aws_caller_identity" "current" {}
