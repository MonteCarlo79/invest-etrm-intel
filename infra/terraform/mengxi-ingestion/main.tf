#####################################
# Existing ECS Cluster
#####################################

data "aws_ecs_cluster" "target" {
  cluster_name = var.ecs_cluster_name
}

#####################################
# CloudWatch Logs
#####################################

resource "aws_cloudwatch_log_group" "ingestion" {
  name              = "/ecs/bess-mengxi-ingestion"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "mengxi_launcher" {
  name              = "/aws/lambda/bess-mengxi-launcher"
  retention_in_days = 14
}

#####################################
# Security Group for ECS task
#####################################

resource "aws_security_group" "ecs_ingestion" {
  name        = "ecs-mengxi-ingestion-sg"
  description = "Security group for Mengxi ingestion ECS task"
  vpc_id      = var.vpc_id

  egress {
    description = "Allow all outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "ecs-mengxi-ingestion-sg"
  }
}

resource "aws_security_group_rule" "ecs_to_rds_ingestion" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = var.rds_security_group_id
  source_security_group_id = aws_security_group.ecs_ingestion.id
  description              = "Postgres from Mengxi ECS task"
}

#####################################
# ECS Task Definition - Daily Ingestion
#####################################

resource "aws_ecs_task_definition" "mengxi_ingestion" {
  depends_on = [
    aws_security_group_rule.ecs_to_rds_ingestion
  ]

  family                   = "bess-mengxi-ingestion"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"

  cpu    = "512"
  memory = "1024"

  task_role_arn      = var.ecs_task_role_arn
  execution_role_arn = var.ecs_task_execution_role_arn

  container_definitions = jsonencode([
    {
      name      = "mengxi-ingestion"
      image     = var.container_image
      essential = true

      environment = [
        {
          name  = "PGURL"
          value = var.pgurl
        },
        {
          name  = "DB_SCHEMA"
          value = var.db_schema
        },
        {
          name  = "PROVINCE"
          value = var.province
        },
        {
          name  = "RUN_MODE"
          value = "daily"
        },
        {
          name  = "START_DATE"
          value = var.start_date
        },
        {
          name  = "FORCE_RELOAD"
          value = var.force_reload
        },
        {
          name  = "ALERT_WEBHOOK_URL"
          value = var.alert_webhook_url
        },
        {
          name  = "ALERT_CONTEXT"
          value = var.alert_context
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ingestion.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  tags = {
    Name = "bess-mengxi-ingestion"
  }
}

#####################################
# ECS Task Definition - Reconciliation
#####################################

resource "aws_ecs_task_definition" "mengxi_reconcile" {
  depends_on = [
    aws_security_group_rule.ecs_to_rds_ingestion
  ]

  family                   = "bess-mengxi-reconcile"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"

  cpu    = "1024"
  memory = "2048"

  task_role_arn      = var.ecs_task_role_arn
  execution_role_arn = var.ecs_task_execution_role_arn

  container_definitions = jsonencode([
    {
      name      = "mengxi-reconcile"
      image     = var.container_image
      essential = true

      environment = [
        {
          name  = "PGURL"
          value = var.pgurl
        },
        {
          name  = "DB_SCHEMA"
          value = var.db_schema
        },
        {
          name  = "PROVINCE"
          value = var.province
        },
        {
          name  = "RUN_MODE"
          value = "reconcile"
        },
        {
          name  = "START_DATE"
          value = var.start_date
        },
        {
          name  = "END_DATE"
          value = var.end_date
        },
        {
          name  = "RECONCILE_DAYS"
          value = tostring(var.reconcile_days)
        },
        {
          name  = "FORCE_RELOAD"
          value = "true"
        },
        {
          name  = "MARKET_LAG_DAYS"
          value = tostring(var.MARKET_LAG_DAYS)
        },
        {
          name  = "REQUEST_DELAY"
          value = tostring(var.REQUEST_DELAY)
        },
        {
          name  = "MAX_DOWNLOAD_WORKERS"
          value = tostring(var.MAX_DOWNLOAD_WORKERS)
        },
        {
          name  = "ALERT_WEBHOOK_URL"
          value = var.alert_webhook_url
        },
        {
          name  = "ALERT_CONTEXT"
          value = var.alert_context
        }
      ]

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ingestion.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ecs"
        }
      }
    }
  ])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  tags = {
    Name = "bess-mengxi-reconcile"
  }
}

#####################################
# EventBridge Rule
#####################################

resource "aws_cloudwatch_event_rule" "mengxi_daily" {
  name                = "bess-mengxi-daily-ingestion"
  description         = "Run Mengxi reconciliation from configured start date"
  schedule_expression = var.schedule_expression
  event_bus_name      = "default"

  tags = {
    Name = "bess-mengxi-daily-ingestion"
  }
}

#####################################
# IAM role for Lambda -> ECS
#####################################

resource "aws_iam_role" "lambda_invoke_ecs" {
  name = "bess-platform-lambda-ecs"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

#####################################
# Lambda permissions
#####################################

resource "aws_iam_role_policy" "lambda_invoke_ecs" {
  name = "bess-platform-lambda-ecs-policy"
  role = aws_iam_role.lambda_invoke_ecs.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecs:RunTask"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = [
          var.ecs_task_execution_role_arn,
          var.ecs_task_role_arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

#####################################
# Lambda function
#####################################

resource "aws_lambda_function" "mengxi_launcher" {
  function_name = "bess-mengxi-launcher"
  role          = aws_iam_role.lambda_invoke_ecs.arn
  handler       = "lambda_function.handler"
  runtime       = "python3.11"
  filename      = "${path.module}/lambda_launcher.zip"
  source_code_hash = filebase64sha256("${path.module}/lambda_launcher.zip")
  timeout       = 60

  environment {
    variables = {
      CLUSTER_ARN         = data.aws_ecs_cluster.target.arn
      TASK_DEFINITION_ARN = aws_ecs_task_definition.mengxi_reconcile.arn
      SUBNET_IDS          = join(",", var.private_subnet_ids)
      SECURITY_GROUP_ID   = aws_security_group.ecs_ingestion.id
      CONTAINER_NAME      = "mengxi-reconcile"
        DEFAULT_START_DATE   = "2026-03-12"
       DEFAULT_FORCE_RELOAD = "true"
    }
  }

  depends_on = [
    aws_iam_role_policy.lambda_invoke_ecs,
    aws_cloudwatch_log_group.mengxi_launcher
  ]
}

#####################################
# EventBridge target -> Lambda
#####################################

resource "aws_cloudwatch_event_target" "mengxi_daily_lambda" {
  rule           = aws_cloudwatch_event_rule.mengxi_daily.name
  event_bus_name = "default"
  target_id      = "bess-mengxi-launcher"
  arn            = aws_lambda_function.mengxi_launcher.arn

  depends_on = [
    aws_lambda_function.mengxi_launcher
  ]
}

#####################################
# Allow EventBridge to invoke Lambda
#####################################

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.mengxi_launcher.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.mengxi_daily.arn
}