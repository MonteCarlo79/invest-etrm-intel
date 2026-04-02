# Mengxi P&L Attribution image deploy notes

Recommended dedicated ECR repository name: `bess-pnl-attribution`.

From repository root, build/tag/push:

```bash
aws ecr create-repository --repository-name bess-pnl-attribution --region ap-southeast-1 || true

aws ecr get-login-password --region ap-southeast-1 \
  | docker login --username AWS --password-stdin <ACCOUNT_ID>.dkr.ecr.ap-southeast-1.amazonaws.com

docker build -f apps/trading/bess/mengxi/pnl_attribution/Dockerfile -t bess-pnl-attribution:latest .

docker tag bess-pnl-attribution:latest <ACCOUNT_ID>.dkr.ecr.ap-southeast-1.amazonaws.com/bess-pnl-attribution:latest

docker push <ACCOUNT_ID>.dkr.ecr.ap-southeast-1.amazonaws.com/bess-pnl-attribution:latest
```

Set Terraform variable `pnl_attribution_image` to:

`<ACCOUNT_ID>.dkr.ecr.ap-southeast-1.amazonaws.com/bess-pnl-attribution:latest`

Terraform toggles:

- `enable_pnl_attribution_service=true`
- `pnl_attribution_image=<ECR image URI>`
- `pnl_attribution_pgurl` is optional now; if omitted, Terraform uses the stack RDS DSN.

Optional scheduled refresh jobs (same stack):

- `enable_trading_bess_mengxi_schedules=true`
- `image_trading_jobs=<ECR image URI containing services/loader and services/trading code>`
- `trading_jobs_db_dsn` optional; defaults to the same stack RDS DSN when omitted.
