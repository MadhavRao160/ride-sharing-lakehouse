#!/usr/bin/env bash
# Run a script from the repo's sim_host/ folder on the simulator EC2.
# The laptop never connects to Kafka directly: it ships the code via S3
# and asks AWS Systems Manager to run it inside the VPC.
#
# Usage:  ./infra/scripts/run-on-sim.sh <script-under-sim_host> [args...]
# Example: ./infra/scripts/run-on-sim.sh kafka/apply_topics.sh
source "$(dirname "$0")/common.sh"

[ $# -ge 1 ] || { echo "Usage: $0 <script-under-sim_host> [args...]"; exit 1; }
SCRIPT="$1"; shift
ARGS="$*"
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
[ -f "$REPO_ROOT/sim_host/$SCRIPT" ] || { echo "Not found: sim_host/$SCRIPT"; exit 1; }

IID=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=$SIM_NAME_TAG" Name=instance-state-name,Values=running \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)
[ "$IID" != "None" ] || { echo "No running instance tagged Name=$SIM_NAME_TAG. Start it first."; exit 1; }

echo "== Shipping sim_host/ to s3://$ARTIFACTS_BUCKET/sim_host/"
aws s3 sync "$REPO_ROOT/sim_host" "s3://$ARTIFACTS_BUCKET/sim_host" --delete \
  --exclude '*.pyc' --exclude '__pycache__/*' --only-show-errors

PARAMS=$(python3 - "$ARTIFACTS_BUCKET" "$AWS_REGION" "$SCRIPT" "$ARGS" <<'PY'
import json, sys
bucket, region, script, args = sys.argv[1:5]
cmds = [
    "set -e",
    "mkdir -p /opt/rs",
    f"aws s3 sync s3://{bucket}/sim_host /opt/rs/sim_host --delete --region {region} --only-show-errors",
    "find /opt/rs/sim_host -name '*.sh' -exec chmod +x {} +",
    "chown -R ec2-user:ec2-user /opt/rs",
    f"sudo -u ec2-user -i bash /opt/rs/sim_host/{script} {args}",
]
print(json.dumps({"commands": cmds, "executionTimeout": ["3600"]}))
PY
)

CMD_ID=$(aws ssm send-command --instance-ids "$IID" \
  --document-name AWS-RunShellScript \
  --comment "rs: $SCRIPT" \
  --parameters "$PARAMS" \
  --query Command.CommandId --output text)
echo "== Running sim_host/$SCRIPT on $IID (command $CMD_ID)"

STATUS=Pending
for _ in $(seq 1 180); do
  sleep 5
  STATUS=$(aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$IID" \
    --query Status --output text 2>/dev/null || echo Pending)
  case "$STATUS" in Pending|InProgress|Delayed) printf '.';; *) break;; esac
done
echo

aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$IID" \
  --query StandardOutputContent --output text
ERR=$(aws ssm get-command-invocation --command-id "$CMD_ID" --instance-id "$IID" \
  --query StandardErrorContent --output text)
[ -z "$ERR" ] || [ "$ERR" = "None" ] || { echo "--- stderr ---"; echo "$ERR"; }

echo "== Status: $STATUS"
[ "$STATUS" = "Success" ]
