variable "GIT_REPOSITORY_NAME" {
    default = "dados_webhook_integ"
    type = string
}

variable "region" {
  default = "us-east-1"
  type    = string
}

variable "environment" {
}

# variable "bucket_for_glue" {
#   description = "Bucket for AWS Glue..."
#   default = "safira-pipeline-webhook"
# }

# variable "s3_safira_lib" {
#   description = "Bucket for AWS Glue..."
#   default = "safira-lib"
# }
variable "ECR_REPOSITORY_REGISTRY" {
  default = "967201331463.dkr.ecr.us-east-1.amazonaws.com"
  type        = string
}

variable "ECR_REPOSITORY_NAME" {
  default = "dados_webhook_integ"
  type        = string
}