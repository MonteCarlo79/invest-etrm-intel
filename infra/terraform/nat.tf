# ── NAT gateway: stable egress IP for Hermes (Fengxing IP whitelist) ─────────
#
# The VPC's existing subnets route 0.0.0.0/0 via IGW and every Fargate task
# gets its own ephemeral public IP, so the Fengxing IP whitelist breaks on
# every deploy. This adds a NAT gateway + two dedicated private subnets for
# the Hermes service, giving it one stable egress IP.
#
# Existing subnets, route tables, ALB, and all other services are untouched.
# The Hermes ECS service network flip is done out-of-band (aws ecs
# update-service), matching how container definitions are managed — see
# hermes.tf. Apply ONLY with -target for these resources; never a blanket
# `terraform apply` (state has intentional drift).

resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = merge(local.tags, { Name = "${var.name}-nat-eip" })
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = var.public_subnet_ids[0] # existing IGW-routed subnet
  tags          = merge(local.tags, { Name = "${var.name}-nat" })
}

# Dedicated private subnets for Hermes tasks. CIDRs verified free in
# 172.31.0.0/16 (existing subnets occupy only 172.31.0/16 .0/.16/.32 /20s).
locals {
  hermes_private_subnets = {
    "ap-southeast-1b" = "172.31.48.0/24"
    "ap-southeast-1c" = "172.31.49.0/24"
  }
}

resource "aws_subnet" "hermes_private" {
  for_each          = local.hermes_private_subnets
  vpc_id            = var.vpc_id
  availability_zone = each.key
  cidr_block        = each.value
  tags              = merge(local.tags, { Name = "${var.name}-hermes-private-${each.key}" })
}

resource "aws_route_table" "hermes_private" {
  vpc_id = var.vpc_id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }

  tags = merge(local.tags, { Name = "${var.name}-hermes-private-rt" })
}

resource "aws_route_table_association" "hermes_private" {
  for_each       = aws_subnet.hermes_private
  subnet_id      = each.value.id
  route_table_id = aws_route_table.hermes_private.id
}

output "nat_gateway_public_ip" {
  value       = aws_eip.nat.public_ip
  description = "Stable egress IP for Hermes — whitelist this at Fengxing"
}
