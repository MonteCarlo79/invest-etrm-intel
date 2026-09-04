# ─────────────────────────────────────────────────────────────────────────────
# ECR Lifecycle Policies
# Retain the 5 most recent images per repository.
# ECR charges $0.10/GB/month — old images accumulate quickly on active repos.
#
# NOTE: inner_mongolia, inner_pipeline, mengxi_dashboard, model_catalogue,
# strategy_agent, portfolio_agent, execution_agent, it_dev_agent already have
# lifecycle policies defined in main.tf and are not repeated here.
# ─────────────────────────────────────────────────────────────────────────────

locals {
  ecr_lifecycle_policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 5 }
      action       = { type = "expire" }
    }]
  })
}

# Defined in main.tf — no lifecycle policy there yet
resource "aws_ecr_lifecycle_policy" "spot_markets" {
  repository = aws_ecr_repository.spot_markets.name
  policy     = local.ecr_lifecycle_policy
}

# Defined uniquely in services-new.tf
resource "aws_ecr_lifecycle_policy" "ph_market" {
  repository = aws_ecr_repository.ph_market.name
  policy     = local.ecr_lifecycle_policy
}

resource "aws_ecr_lifecycle_policy" "po_market" {
  repository = aws_ecr_repository.po_market.name
  policy     = local.ecr_lifecycle_policy
}

resource "aws_ecr_lifecycle_policy" "crystal_ball_client" {
  repository = aws_ecr_repository.crystal_ball_client.name
  policy     = local.ecr_lifecycle_policy
}
