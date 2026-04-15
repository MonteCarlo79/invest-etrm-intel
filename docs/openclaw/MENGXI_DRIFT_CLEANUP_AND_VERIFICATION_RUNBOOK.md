# MENGXI DRIFT CLEANUP AND VERIFICATION RUNBOOK

## Purpose

Provide a short operator command sequence for cleaning up the known Mengxi drift artefacts and verifying that the next reconcile run behaves correctly.

This is a docs-only operational runbook.

---

## Known Terraform-owned names

These names are repo-backed and should remain after cleanup:

- region: `ap-southeast-1`
- ECS cluster: `bess-platform-cluster`
- EventBridge rule: `bess-mengxi-daily-ingestion`
- Terraform-owned EventBridge target ID: `bess-mengxi-launcher`
- Lambda launcher: `bess-mengxi-launcher`
- ECS task families:
  - `bess-mengxi-ingestion`
  - `bess-mengxi-reconcile`
- CloudWatch log group: `/ecs/bess-mengxi-ingestion`
- DB tables used for verification:
  - `marketdata.md_load_log`
  - `marketdata.data_quality_status`

The rogue ECS service name, orphan target ID, and optional CloudFormation stack name must be discovered live before deletion.

---

## 1. Set shell variables

```powershell
$env:AWS_REGION = "ap-southeast-1"
$CLUSTER = "bess-platform-cluster"
$RULE = "bess-mengxi-daily-ingestion"
$GOOD_TARGET_ID = "bess-mengxi-launcher"
$LAUNCHER_FN = "bess-mengxi-launcher"
$LOG_GROUP = "/ecs/bess-mengxi-ingestion"
```

---

## 2. Discover the rogue ECS service and orphan target

List Mengxi-related ECS services in the cluster:

```powershell
aws ecs list-services `
  --region $env:AWS_REGION `
  --cluster $CLUSTER `
  --query "serviceArns[?contains(@, 'mengxi')]" `
  --output text
```

Inspect the suspected rogue service:

```powershell
$ROGUE_SERVICE = "<rogue-service-name>"
aws ecs describe-services `
  --region $env:AWS_REGION `
  --cluster $CLUSTER `
  --services $ROGUE_SERVICE `
  --output json
```

List the EventBridge targets on the Mengxi rule:

```powershell
aws events list-targets-by-rule `
  --region $env:AWS_REGION `
  --event-bus-name default `
  --rule $RULE `
  --output table
```

Expected post-cleanup state:

- the valid target ID should be `bess-mengxi-launcher`
- any extra target ID is the orphan target to remove

---

## 3. Stop the rogue ECS service

Scale the rogue service to zero first:

```powershell
aws ecs update-service `
  --region $env:AWS_REGION `
  --cluster $CLUSTER `
  --service $ROGUE_SERVICE `
  --desired-count 0
```

Verify it is no longer running tasks:

```powershell
aws ecs describe-services `
  --region $env:AWS_REGION `
  --cluster $CLUSTER `
  --services $ROGUE_SERVICE `
  --query "services[0].{status:status,desiredCount:desiredCount,runningCount:runningCount,pendingCount:pendingCount}" `
  --output table
```

If the service is confirmed rogue and not Terraform-owned, optional hard cleanup:

```powershell
aws ecs delete-service `
  --region $env:AWS_REGION `
  --cluster $CLUSTER `
  --service $ROGUE_SERVICE `
  --force
```

---

## 4. Remove the orphan EventBridge target

Remove the non-Terraform-owned target ID from the Mengxi rule:

```powershell
$ORPHAN_TARGET_ID = "<orphan-target-id>"
aws events remove-targets `
  --region $env:AWS_REGION `
  --event-bus-name default `
  --rule $RULE `
  --ids $ORPHAN_TARGET_ID
```

Verify only the Terraform-owned target remains:

```powershell
aws events list-targets-by-rule `
  --region $env:AWS_REGION `
  --event-bus-name default `
  --rule $RULE `
  --query "Targets[*].{Id:Id,Arn:Arn}" `
  --output table
```

Expected result:

- only target ID `bess-mengxi-launcher` should remain

---

## 5. Optionally delete the ECS Console-created CloudFormation stack

If the rogue ECS service was wrapped in a console-created CloudFormation stack, discover the stack from the physical resource:

```powershell
aws cloudformation describe-stack-resources `
  --region $env:AWS_REGION `
  --physical-resource-id $ROGUE_SERVICE `
  --output table
```

If a console-created stack is confirmed and is not needed:

```powershell
$ROGUE_STACK = "<console-created-stack-name>"
aws cloudformation delete-stack `
  --region $env:AWS_REGION `
  --stack-name $ROGUE_STACK

aws cloudformation wait stack-delete-complete `
  --region $env:AWS_REGION `
  --stack-name $ROGUE_STACK
```

---

## 6. Trigger and verify the next reconcile run

Trigger one controlled reconcile run through the Terraform-owned launcher:

```powershell
$invokeOut = Join-Path $env:TEMP "bess-mengxi-launcher-invoke.json"
aws lambda invoke `
  --region $env:AWS_REGION `
  --function-name $LAUNCHER_FN `
  --payload "{}" `
  $invokeOut

Get-Content $invokeOut
```

The launcher should start a `bess-mengxi-reconcile` task using the Terraform-owned path.

Tail the Mengxi ECS log group to watch the run:

```powershell
aws logs tail $LOG_GROUP `
  --region $env:AWS_REGION `
  --since 30m `
  --follow
```

Success indicators:

- no fresh `RuntimeError: Database not reachable`
- no repeated `timeout expired`
- the run proceeds past DB connectivity and into downloader/load stages

---

## 7. Verify DB state after the reconcile run

Assume `PGURL` is injected securely in the operator shell.

Check the latest load-log rows:

```powershell
psql $env:PGURL -c "
SELECT file_date, file_name, status, loaded_at, message
FROM marketdata.md_load_log
ORDER BY loaded_at DESC
LIMIT 20;
"
```

Check the latest data-quality status:

```powershell
psql $env:PGURL -c "
SELECT province, data_date, is_complete, actual_intervals, interval_coverage, check_time, source_file
FROM marketdata.data_quality_status
WHERE province = 'mengxi'
ORDER BY data_date DESC
LIMIT 20;
"
```

Minimal verification result:

- fresh `success` rows appear in `marketdata.md_load_log`
- `marketdata.data_quality_status` shows recent Mengxi dates updating
- CloudWatch no longer shows the DB timeout failure pattern

---

## Final reporting rule

When using this runbook, the final operator note should still state explicitly:

- branch
- exact commits
- pushed status
- deploy/apply status
