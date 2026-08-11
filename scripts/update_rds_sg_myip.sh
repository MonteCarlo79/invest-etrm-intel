#!/usr/bin/env bash
# Update the bess-platform-pg RDS security group with this machine's CURRENT public IP.
# The home ISP rotates IPs frequently (daily); run this whenever RDS connections start timing out.
#
# Usage:  bash scripts/update_rds_sg_myip.sh
#
# Only manages rules tagged "macbook home auto" — yesterday's auto-rule is revoked,
# manual/dated entries (e.g. "macbook ib-platform 2026-07-01") are never touched.

set -euo pipefail

REGION="ap-southeast-1"
SG="sg-0a060f8f8d1c62c35"   # bess-platform-pg
TAG="macbook home auto"

MYIP=$(curl -s --max-time 10 ifconfig.me)
if [ -z "$MYIP" ]; then
  echo "ERROR: could not determine public IP (ifconfig.me unreachable)" >&2
  exit 1
fi
echo "Current public IP: $MYIP"

# Revoke previous auto rules whose IP differs from the current one
OLD=$(aws ec2 describe-security-group-rules --region "$REGION" \
  --filters "Name=group-id,Values=$SG" \
  --query "SecurityGroupRules[?Description=='$TAG' && CidrIpv4!='${MYIP}/32'].SecurityGroupRuleId" \
  --output text)
for rule in $OLD; do
  echo "Revoking stale auto rule: $rule"
  aws ec2 revoke-security-group-ingress --region "$REGION" --group-id "$SG" \
    --security-group-rule-ids "$rule" --output text > /dev/null
done

# Check whether the current IP is already covered by ANY rule (manual or auto)
EXISTING=$(aws ec2 describe-security-group-rules --region "$REGION" \
  --filters "Name=group-id,Values=$SG" \
  --query "SecurityGroupRules[?CidrIpv4=='${MYIP}/32'].SecurityGroupRuleId" \
  --output text)

if [ -n "$EXISTING" ]; then
  echo "Already covered by existing rule: $EXISTING — nothing to do."
else
  aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$SG" \
    --ip-permissions "IpProtocol=tcp,FromPort=5432,ToPort=5432,IpRanges=[{CidrIp=${MYIP}/32,Description='$TAG'}]" \
    --output text > /dev/null
  echo "Added: ${MYIP}/32 → ${SG} :5432"
fi
