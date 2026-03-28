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
