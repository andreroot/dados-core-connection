terraform {
#criar dentro do workspace definido no init
#{workspace_key_prefix}/{workspace_name}/{key}
  backend "s3" {
    bucket         = "terraform-tf-state-github"
    key            = "webhooks/proj_webhook_integ/terraform_ecr.tfstate"
    region         = "us-east-1"
    workspace_key_prefix = "proj_webhook_integ"
   }
}