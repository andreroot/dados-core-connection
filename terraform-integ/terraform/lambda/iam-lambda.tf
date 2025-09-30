
# IAM Role for Lambda
resource "aws_iam_role" "lambda_dados_webhook_integ_execution_role" {
  name               = "lambda_dados_webhook_integ_execution_role"
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
        "Action": "sts:AssumeRole",
        "Principal": {
            "Service": "lambda.amazonaws.com"
        },
        "Effect": "Allow",
        "Sid": ""
    }
  ]
}
EOF
}

# IAM Policy for our Lambda
resource "aws_iam_policy" "iam_for_lambda_policy" {
  name = "iam_for_lambda_policy_dados_webhook_integ"
  policy = jsonencode(
    {
      "Version" : "2012-10-17",
      "Statement" : [
        {
          "Effect" : "Allow",
          "Action" : "logs:CreateLogGroup",
          "Resource" : "*"
        },
        {
          "Effect" : "Allow",
          "Action" : [
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ],
          "Resource" : [
            "*"
          ]
        },
        {
          "Effect" : "Allow",
          "Action" : "kms:Decrypt",
          "Resource" : "${aws_kms_key.key.arn}"
        },
        {
          "Action" : [
            "s3:PutObject",
            "s3:GetObject",
          ],
          "Resource" : [
            "arn:aws:s3:::${data.aws_s3_bucket.bucket.id}",
            "arn:aws:s3:::${data.aws_s3_bucket.bucket.id}/*",
            "arn:aws:s3:::${data.aws_s3_bucket.bucket_pipeline_wh.id}",
            "arn:aws:s3:::${data.aws_s3_bucket.bucket_pipeline_wh.id}/*"
          ],
          "Effect" : "Allow",
        },
        {
            "Effect": "Allow",
            "Action": "secretsmanager:GetSecretValue",
            "Resource": "*"
        }
      ]
    }
  )
}

resource "aws_iam_policy_attachment" "policy_attachment_lambda" {
  name       = "attachment_lambda_dados_webhook_integ"
  roles      = [aws_iam_role.lambda_dados_webhook_integ_execution_role.id]
  policy_arn = aws_iam_policy.iam_for_lambda_policy.arn
}



# resource "aws_iam_role_policy_attachment" "glue_service_attachment" {
#   #name = "AWSGlueServiceRole-stream"
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
#   role = aws_iam_role.aws_iam_glue_role_webhook.id
# }


# resource "aws_s3_bucket" "bucket_for_glue" {
#   bucket = var.bucket_for_glue
#   force_destroy = true
# }