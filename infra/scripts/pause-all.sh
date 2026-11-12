#!/usr/bin/env bash
# Pause everything that bills by the hour while idle, except MSK.
#   1. Stop the simulator EC2 (disk is kept; registry auto-starts on next boot)
#   2. Delete the NAT gateway + release its Elastic IP (nat-pause.sh)
#   3. Report anything still running in Databricks
# MSK Provisioned can't be stopped, only deleted; we keep it (see note at the end).
source "$(dirname "$0")/common.sh"
DIR="$(dirname "$0")"

echo "== 1. Simulator EC2"
IID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$SIM_NAME_TAG" Name=instance-state-name,Values=pending,running \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)
if [ "$IID" = "None" ]; then
  echo "   already stopped"
else
  aws ec2 stop-instances --instance-ids "$IID" >/dev/null
  aws ec2 wait instance-stopped --instance-ids "$IID"
  echo "   $IID stopped"
fi

echo "== 2. NAT gateway"
"$DIR/nat-pause.sh"

echo "== 3. Databricks (profile rs-dev)"
if command -v databricks >/dev/null 2>&1; then
  RUNNING=$(databricks clusters list --profile rs-dev 2>/dev/null | grep -E 'RUNNING|PENDING|RESIZING' || true)
  [ -z "$RUNNING" ] && echo "   no running clusters" || { echo "   STILL RUNNING:"; echo "$RUNNING"; }
  WH=$(databricks warehouses list --profile rs-dev 2>/dev/null | grep -E 'RUNNING|STARTING' || true)
  [ -z "$WH" ] && echo "   no running SQL warehouses" || { echo "   STILL RUNNING:"; echo "$WH"; }
else
  echo "   databricks CLI not found; check Compute and SQL Warehouses in the UI"
fi

echo
echo "Paused. Still billing: MSK rs-msk (can't be stopped), S3 storage, EC2 disk (cents)."
echo "Resume with: ./infra/scripts/resume-all.sh"
