# data "aws_iam_role" "this" {
#   name = "ecs-execution-role"
# }

resource "aws_ecr_repository" "dados_webhook_integ" {
  name = var.ECR_REPOSITORY_NAME

	  image_scanning_configuration {
	    scan_on_push = true
	  }
    tags = {
          "Key": "Name",
          "Value": "ecr-webhook-integ"
      }
}

# data "aws_ecr_image" "dados_webhook_integ" {
#   repository_name = var.ECR_REPOSITORY_NAME
#   image_tag       = "latest"
# }

resource "aws_ecr_lifecycle_policy" "dados_webhook_integ" {
  repository = aws_ecr_repository.dados_webhook_integ.name

  policy = <<EOF
{
  "rules": [
    {
      "rulePriority": 1,
      "description": "Remove untagged images",
      "selection": {
        "tagStatus": "untagged",
        "countType": "sinceImagePushed",
        "countUnit": "days",
        "countNumber": 7
      },
      "action": {
        "type": "expire"
      }
    }
  ]
}
EOF
}

resource "aws_cloudwatch_log_group" "dados_webhook_integ" {
  name              = "/ecs/${var.ECR_REPOSITORY_NAME}"
  retention_in_days = 7
}

resource "aws_ecs_task_definition" "dados_webhook_integ" {
  family                   = var.ECR_REPOSITORY_NAME
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = aws_iam_role.ecs_dados_webhook_integ_execution_role.arn
  task_role_arn            = aws_iam_role.ecs_dados_webhook_integ_execution_role.arn
  container_definitions    = <<TASK_DEFINITION
[
  {
    "name": "${var.ECR_REPOSITORY_NAME}",
    "image": "967201331463.dkr.ecr.us-east-1.amazonaws.com/${var.ECR_REPOSITORY_NAME}:latest",
    "cpu": 1024,
    "memory": 2048,
    "essential": true,
    "environment": [],
    "logConfiguration": {
      "logDriver": "awslogs",
      "options": {
        "awslogs-group": "${aws_cloudwatch_log_group.dados_webhook_integ.name}",
        "awslogs-region": "us-east-1",
        "awslogs-stream-prefix": "ecs"
      }
    }
  }
]
TASK_DEFINITION

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  #depends_on = [aws_cloudwatch_log_group.dados_webhook_integ]
}

resource "null_resource" "docker_packaging" {
	
	  provisioner "local-exec" {
	    command = <<EOF
      aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 967201331463.dkr.ecr.us-east-1.amazonaws.com
	    docker build -t ${var.ECR_REPOSITORY_NAME}:latest -f Dockerfile.wh .
      docker tag ${var.ECR_REPOSITORY_NAME}:latest 967201331463.dkr.ecr.us-east-1.amazonaws.com/${var.ECR_REPOSITORY_NAME}
	    docker push 967201331463.dkr.ecr.us-east-1.amazonaws.com/${var.ECR_REPOSITORY_NAME}
	    EOF
	  }
	

	  triggers = {
	    "run_at" = timestamp()
	  }
	

	  #depends_on = [aws_ecr_repository.dados_webhook_integ]
}