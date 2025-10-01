terraform {
  backend "s3" {
    bucket  = "terraform-tf-state-github"
    key     = "emr/terraform.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}