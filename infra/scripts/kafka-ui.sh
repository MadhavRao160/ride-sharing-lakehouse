#!/usr/bin/env bash
# Open Kafka UI on the laptop at http://localhost:8080 through an SSM tunnel.
# No public IP, no inbound firewall rule: traffic goes through Systems Manager.
# Keep this terminal open while you use the UI; Ctrl+C closes the tunnel.
source "$(dirname "$0")/common.sh"
LOCAL_PORT="${1:-8080}"

command -v session-manager-plugin >/dev/null 2>&1 || {
  echo "Missing Session Manager plugin. Install: brew install --cask session-manager-plugin"; exit 1; }

IID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$SIM_NAME_TAG" Name=instance-state-name,Values=running \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)
[ "$IID" != "None" ] || { echo "No running instance tagged Name=$SIM_NAME_TAG. Run resume-all.sh."; exit 1; }

echo "Tunnel to $IID. Open http://localhost:$LOCAL_PORT   (Ctrl+C to close)"
aws ssm start-session --target "$IID" \
  --document-name AWS-StartPortForwardingSession \
  --parameters "portNumber=8080,localPortNumber=$LOCAL_PORT"
