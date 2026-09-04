Check the current state of the repo and produce a deployment checklist for the app that was most recently modified. 

Steps:
1. Run `git status` and `git diff --stat HEAD` to see what changed.
2. Identify which app(s) were modified (spot-market / bess-map / mengxi-dashboard / portal / other).
3. For each modified app, show the deployment commands:

```bash
# Build
docker build -f apps/<app>/Dockerfile -t <ecr-repo>:<next-version> .

# Tag
docker tag <ecr-repo>:<next-version> 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/<ecr-repo>:<next-version>

# Push (requires ECR login)
docker push 319383842493.dkr.ecr.ap-southeast-1.amazonaws.com/<ecr-repo>:<next-version>

# Update tfvars
# Edit infra/terraform/terraform.tfvars: set image_<app> = "<next-version>"

# Apply
cd infra/terraform && terraform apply
```

4. Check `infra/terraform/terraform.tfvars` to determine the current version tag and suggest the next version number.
5. Remind: "All deployments require explicit in-session confirmation. Do not deploy without a 'yes' from the user in the current message."
6. List any DB migrations needed (new tables, columns) and remind that migrations are applied on first container startup via `CREATE TABLE IF NOT EXISTS`.
