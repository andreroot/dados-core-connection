resource "aws_secretsmanager_secret" "lambda-webhook" {
    name = "${var.GIT_REPOSITORY_NAME}_authorization_token"
    force_overwrite_replica_secret = true
}

resource "aws_secretsmanager_secret_version" "lambda-webhook" {
    secret_id = aws_secretsmanager_secret.lambda-webhook.id
    secret_string = var.AUTHORIZATION_TOKEN
    # secret_string = <<EOF
    # {
    #     "test-key": "test-value"
    # }
    # EOF
}

#aws secretsmanager delete-secret --secret-id dados_webhook_integ_authorization_token --force-delete-without-recovery --region us-east-1
