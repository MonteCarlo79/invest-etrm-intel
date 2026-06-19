# ─────────────────────────────────────────────────────────────────────────────
# deeptutor.tf
# DeepTutor: tutor.pjh-etrm.ai
# 3-container ECS task: nginx proxy (8530) + deeptutor app + pocketbase
# ─────────────────────────────────────────────────────────────────────────────

# ── ECR ──────────────────────────────────────────────────────────────────────
resource "aws_ecr_repository" "deeptutor_nginx" {
  name                 = "deeptutor-nginx"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = false }
  tags = local.tags
}

# ── EFS file system ───────────────────────────────────────────────────────────
resource "aws_efs_file_system" "deeptutor" {
  encrypted = true
  tags      = merge(local.tags, { Name = "${var.name}-deeptutor-efs" })
}

# EFS security group: allow NFS (2049) from ECS tasks SG only
resource "aws_security_group" "deeptutor_efs" {
  name        = "${var.name}-deeptutor-efs-sg"
  description = "EFS mount targets for DeepTutor"
  vpc_id      = var.vpc_id
  tags        = local.tags

  ingress {
    description     = "NFS from ECS tasks"
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [aws_security_group.ecs_tasks.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Mount target in each subnet used by the ECS cluster
resource "aws_efs_mount_target" "deeptutor" {
  for_each = toset(var.private_subnet_ids)

  file_system_id  = aws_efs_file_system.deeptutor.id
  subnet_id       = each.value
  security_groups = [aws_security_group.deeptutor_efs.id]
}

# Access point for DeepTutor app data (/app/data in container)
resource "aws_efs_access_point" "deeptutor_data" {
  file_system_id = aws_efs_file_system.deeptutor.id

  posix_user {
    uid = 0
    gid = 0
  }

  root_directory {
    path = "/deeptutor"
    creation_info {
      owner_uid   = 0
      owner_gid   = 0
      permissions = "755"
    }
  }

  tags = merge(local.tags, { Name = "${var.name}-deeptutor-data" })
}

# Access point for PocketBase data (/pb/pb_data in container)
resource "aws_efs_access_point" "pocketbase_data" {
  file_system_id = aws_efs_file_system.deeptutor.id

  posix_user {
    uid = 0
    gid = 0
  }

  root_directory {
    path = "/pocketbase"
    creation_info {
      owner_uid   = 0
      owner_gid   = 0
      permissions = "755"
    }
  }

  tags = merge(local.tags, { Name = "${var.name}-pocketbase-data" })
}

# ── ALB target group + listener rule ─────────────────────────────────────────
resource "aws_lb_target_group" "deeptutor" {
  name        = "bess-platform-deeptutor"
  port        = 8530
  protocol    = "HTTP"
  vpc_id      = var.vpc_id
  target_type = "ip"

  lifecycle { create_before_destroy = true }

  health_check {
    path                = "/health"
    protocol            = "HTTP"
    matcher             = "200-399"
    interval            = 30
    timeout             = 10
    healthy_threshold   = 2
    unhealthy_threshold = 3
  }

  tags = local.tags
}

# Host-header rule: tutor.pjh-etrm.ai → deeptutor TG
resource "aws_lb_listener_rule" "deeptutor_host" {
  listener_arn = aws_lb_listener.https.arn
  priority     = 60

  action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.deeptutor.arn
  }

  condition {
    host_header {
      values = ["tutor.pjh-etrm.ai"]
    }
  }
}

# ── Route53 DNS ───────────────────────────────────────────────────────────────
data "aws_route53_zone" "pjh_etrm" {
  name = "pjh-etrm.ai."
}

resource "aws_route53_record" "deeptutor" {
  zone_id = data.aws_route53_zone.pjh_etrm.zone_id
  name    = "tutor"
  type    = "A"

  alias {
    name                   = aws_lb.app.dns_name
    zone_id                = aws_lb.app.zone_id
    evaluate_target_health = true
  }
}

# ── ECS task definition ───────────────────────────────────────────────────────
resource "aws_ecs_task_definition" "deeptutor" {
  family                   = "${var.name}-deeptutor"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "2048"
  memory                   = "4096"
  execution_role_arn       = aws_iam_role.task_execution.arn
  task_role_arn            = aws_iam_role.task_role.arn

  # EFS volumes
  volume {
    name = "deeptutor-data"
    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.deeptutor.id
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = aws_efs_access_point.deeptutor_data.id
        iam             = "DISABLED"
      }
    }
  }

  volume {
    name = "pocketbase-data"
    efs_volume_configuration {
      file_system_id     = aws_efs_file_system.deeptutor.id
      transit_encryption = "ENABLED"
      authorization_config {
        access_point_id = aws_efs_access_point.pocketbase_data.id
        iam             = "DISABLED"
      }
    }
  }

  container_definitions = jsonencode([
    # ── Container 1: PocketBase (multi-user auth+storage) ─────────────────
    {
      name      = "pocketbase"
      image     = "ghcr.io/muchobien/pocketbase:latest"
      essential = false

      mountPoints = [
        { sourceVolume = "pocketbase-data", containerPath = "/pb/pb_data", readOnly = false }
      ]

      environment = [
        { name = "PB_SUPERADMIN_EMAIL",    value = var.deeptutor_pb_admin_email },
        { name = "PB_SUPERADMIN_PASSWORD", value = var.deeptutor_pb_admin_password }
      ]

      healthCheck = {
        command     = ["CMD-SHELL", "wget -q --spider http://localhost:8090/api/health || exit 1"]
        interval    = 15
        timeout     = 5
        retries     = 5
        startPeriod = 30
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "deeptutor-pocketbase"
        }
      }
    },

    # ── Container 2: DeepTutor app (Next.js :3782 + FastAPI :8001) ────────
    {
      name      = "deeptutor"
      image     = var.image_deeptutor
      essential = true

      dependsOn = [
        { containerName = "pocketbase", condition = "HEALTHY" }
      ]

      mountPoints = [
        { sourceVolume = "deeptutor-data", containerPath = "/app/data", readOnly = false }
      ]

      environment = [
        { name = "NEXT_PUBLIC_API_BASE_EXTERNAL",          value = "https://tutor.pjh-etrm.ai" },
        { name = "DEEPTUTOR_IGNORE_PROCESS_ENV_OVERRIDES", value = "1" }
      ]

      healthCheck = {
        command     = ["CMD-SHELL", "wget -q --spider http://localhost:8001/ || exit 1"]
        interval    = 30
        timeout     = 10
        retries     = 5
        startPeriod = 120
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "deeptutor-app"
        }
      }
    },

    # ── Container 3: nginx proxy — the only container exposed to the ALB ──
    {
      name      = "deeptutor-nginx"
      image     = var.image_deeptutor_nginx
      essential = true

      portMappings = [{ containerPort = 8530, protocol = "tcp" }]

      dependsOn = [
        { containerName = "deeptutor", condition = "HEALTHY" }
      ]

      healthCheck = {
        command     = ["CMD-SHELL", "wget -q --spider http://localhost:8530/health || exit 1"]
        interval    = 30
        timeout     = 10
        retries     = 3
        startPeriod = 120
      }

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = local.log_group
          awslogs-region        = var.region
          awslogs-stream-prefix = "deeptutor-nginx"
        }
      }
    }
  ])

  lifecycle { ignore_changes = [container_definitions] }
  tags = local.tags
}

# ── ECS service ───────────────────────────────────────────────────────────────
resource "aws_ecs_service" "deeptutor" {
  name            = "${var.name}-deeptutor-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.deeptutor.arn
  desired_count   = var.desired_count_deeptutor
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.private_subnet_ids
    security_groups  = [aws_security_group.ecs_tasks.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.deeptutor.arn
    container_name   = "deeptutor-nginx"
    container_port   = 8530
  }

  depends_on = [aws_lb_listener.https]
  tags       = local.tags

  lifecycle { ignore_changes = [task_definition] }
}
