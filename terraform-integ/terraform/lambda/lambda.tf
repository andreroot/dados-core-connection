data "aws_s3_bucket" "bucket" {
  bucket = "safira-pipeline-webhook"
}

data "aws_s3_bucket" "bucket_pipeline_wh" {
  bucket = "safira-pipeline-webhook"
}

data "aws_ecr_image" "repo_image" {
  repository_name = "dados_webhook_integ"
  image_tag = "latest"
}

# Our Lambda function
resource "aws_lambda_function" "lambda-webhook" {
  function_name = var.GIT_REPOSITORY_NAME
  role          = aws_iam_role.lambda_dados_webhook_integ_execution_role.arn
  timeout       = 120
  image_uri     = "967201331463.dkr.ecr.us-east-1.amazonaws.com/dados_webhook_integ:latest"
  package_type  = "Image"
  kms_key_arn   = aws_kms_key.key.arn
  source_code_hash    = trimprefix(data.aws_ecr_image.repo_image.id, "sha256:")
  #"${filebase64sha256("../ecr/src/rdstation/webhook.py")}"
  environment {
    variables = {
      BUCKET_NAME = "${data.aws_s3_bucket.bucket.id}",
      GIT_REPOSITORY_NAME = "${var.GIT_REPOSITORY_NAME}"
    }
  }
  tags = {
            "Key": "Name",
            "Value": "lambda-webhook-integ"
        }
}

# A ZIP archive containing python code
# data "archive_file" "lambda-webhook" {
#   type        = "zip"
#   source_dir  = "../lambda/webhook/"
#   output_path = "../lambda/webhook/webhook.zip"
# }

# Our public HTTPS endpoint
resource "aws_lambda_function_url" "lambda_function_url" {
  function_name      = aws_lambda_function.lambda-webhook.arn
  authorization_type = "NONE"
}

output "webhook_endpoint" {
  description = "Function URL."
  value       = aws_lambda_function_url.lambda_function_url.function_url
}

# A Cloudwatch Log Group to be able to see Lambda's logs
resource "aws_cloudwatch_log_group" "lambda-webhook" {
  name              = "/aws/lambda/${aws_lambda_function.lambda-webhook.function_name}"
  retention_in_days = 3
}

# A KMS Key to encrypt / decryt environment variables
resource "aws_kms_key" "key" {
  description             = "KMS key for Lambda Webhook"
  deletion_window_in_days = 7
}
