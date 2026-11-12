#!/usr/bin/env bash
# Undo pause-all.sh: NAT first (the EC2 needs internet to reach Systems Manager),
# then start the simulator EC2 and wait until Systems Manager can reach it.
source "$(dirname "$0")/common.sh"
DIR="$(dirname "$0")"

echo "== 1. NAT gateway"
"$DIR/nat-resume.sh"

echo "== 2. Simulator EC2"
IID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$SIM_NAME_TAG" Name=instance-state-name,Values=stopped,stopping,pending,running \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)
[ "$IID" != "None" ] || { echo "No instance tagged Name=$SIM_NAME_TAG"; exit 1; }
aws ec2 wait instance-stopped --instance-ids "$IID" 2>/dev/null || true
STATE=$(aws ec2 describe-instances --instance-ids "$IID" --query 'Reservations[0].Instances[0].State.Name' --output text)
if [ "$STATE" != "running" ]; then
  aws ec2 start-instances --instance-ids "$IID" >/dev/null
  aws ec2 wait instance-running --instance-ids "$IID"
fi
echo "   $IID running"

echo -n "   waiting for Systems Manager agent "
for _ in $(seq 1 40); do
  PING=$(aws ssm describe-instance-information \
    --filters "Key=InstanceIds,Values=$IID" \
    --query 'InstanceInformationList[0].PingStatus' --output text 2>/dev/null || echo None)
  [ "$PING" = "Online" ] && { echo " online"; break; }
  echo -n "."; sleep 6
done
[ "$PING" = "Online" ] || { echo " not online yet; retry run-on-sim in a minute"; exit 1; }

echo
echo "Resumed. Check the registry with:"
echo "  ./infra/scripts/run-on-sim.sh setup/install_schema_registry.sh"
