#!/usr/bin/env bash
# Pause the NAT gateway for rs-vpc (Sydney) to stop idle charges.
# Deletes the NAT gateway and releases its Elastic IP.
# The 0.0.0.0/0 route in rs-rtb-private becomes "blackhole" until nat-resume.sh runs.
set -euo pipefail
export AWS_PROFILE=rs-personal
export AWS_REGION=ap-southeast-2
VPC=vpc-016f7949c5d1773f4
EXPECTED_ACCOUNT=605499974190

# Safety: refuse to run against any account except the personal project account.
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
if [ "$ACCOUNT" != "$EXPECTED_ACCOUNT" ]; then
  echo "STOP: profile $AWS_PROFILE points at account $ACCOUNT, expected $EXPECTED_ACCOUNT."
  exit 1
fi

NAT=$(aws ec2 describe-nat-gateways \
  --filter Name=vpc-id,Values=$VPC Name=state,Values=available \
  --query 'NatGateways[0].NatGatewayId' --output text)
if [ "$NAT" = "None" ]; then
  echo "No running NAT gateway in $VPC. Nothing to do."
  exit 0
fi

ALLOC=$(aws ec2 describe-nat-gateways --nat-gateway-ids "$NAT" \
  --query 'NatGateways[0].NatGatewayAddresses[0].AllocationId' --output text)

echo "Deleting NAT gateway $NAT ..."
aws ec2 delete-nat-gateway --nat-gateway-id "$NAT" >/dev/null
aws ec2 wait nat-gateway-deleted --nat-gateway-ids "$NAT"

echo "Releasing Elastic IP $ALLOC ..."
aws ec2 release-address --allocation-id "$ALLOC"

echo "Paused. Private subnets have no internet access until nat-resume.sh runs."
