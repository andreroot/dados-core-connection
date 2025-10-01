provider "aws" {
  region = "us-east-1"
}

# Screts Manager Read policy
data "aws_iam_policy_document" "this" {
  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "this" {
  name        = "GetSecrets"
  description = "Policy to allow GetSecretValue from Secrets Manager "
  policy      = data.aws_iam_policy_document.this.json
}

# necessary roles
resource "aws_iam_role" "job_flow_role" {
  name = var.job_flow_role_name

  assume_role_policy = jsonencode({
    "Version" : "2008-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "ec2.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "job_flow_role_policy_attachment" {
  role       = aws_iam_role.job_flow_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_role_policy_attachment" "secretmanager_policy_attachment" {
  role       = aws_iam_role.job_flow_role.name
  policy_arn = aws_iam_policy.this.arn
}

resource "aws_iam_instance_profile" "job_flow_inst_profile" {
  name = var.job_flow_role_name

  role = aws_iam_role.job_flow_role.name
}

resource "aws_iam_role" "service_role" {
  name = var.service_role_name

  assume_role_policy = jsonencode({
    "Version" : "2008-10-17",
    "Statement" : [
      {
        "Sid" : "",
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "elasticmapreduce.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "service_role_policy_attachment" {
  role       = aws_iam_role.service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# security groups
data "aws_vpc" "default" {
  default = true
}

resource "aws_security_group" "manager" {
  name        = "${var.security_group_name}-Manager"
  description = "EMR manager group."
  vpc_id      = data.aws_vpc.default.id
}

resource "aws_security_group" "worker" {
  name        = "${var.security_group_name}-Worker"
  description = "EMR worker group."
  vpc_id      = data.aws_vpc.default.id
}
