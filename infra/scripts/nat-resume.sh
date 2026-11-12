#!/usr/bin/env bash
# Resume the NAT gateway for rs-vpc (Sydney).
# Allocates a new Elastic IP, creates the NAT gateway in public subnet 1,
# and re-points 0.0.0.0/0 in rs-rtb-private to it.
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

RUNNING=$(aws ec2 describe-nat-gateways \
  --filter Name=vpc-id,Values=$VPC Name=state,Values=available,pending \
  --query 'NatGateways[0].NatGatewayId' --output text)
if [ "$RUNNING" != "None" ]; then
  echo "NAT gateway $RUNNING already exists. Nothing to do."
  exit 0
fi

SUBNET=$(aws ec2 describe-subnets \
  --filters Name=vpc-id,Values=$VPC "Name=tag:Name,Values=rs-subnet-public1*" \
  --query 'Subnets[0].SubnetId' --output text)
RTB=$(aws ec2 describe-route-tables \
  --filters Name=vpc-id,Values=$VPC Name=tag:Name,Values=rs-rtb-private \
  --query 'RouteTables[0].RouteTableId' --output text)

if [ "$SUBNET" = "None" ] || [ "$RTB" = "None" ]; then
  echo "Could not find public subnet ($SUBNET) or route table ($RTB). Check Name tags."
  exit 1
fi

ALLOC=$(aws ec2 allocate-address --domain vpc \
  --tag-specifications 'ResourceType=elastic-ip,Tags=[{Key=Name,Value=rs-nat-eip},{Key=project,Value=rs}]' \
  --query AllocationId --output text)

NAT=$(aws ec2 create-nat-gateway --subnet-id "$SUBNET" --allocation-id "$ALLOC" \
  --tag-specifications 'ResourceType=natgateway,Tags=[{Key=Name,Value=rs-nat},{Key=project,Value=rs}]' \
  --query NatGateway.NatGatewayId --output text)

echo "Waiting for NAT gateway $NAT to become available (2-5 min) ..."
aws ec2 wait nat-gateway-available --nat-gateway-ids "$NAT"

aws ec2 replace-route --route-table-id "$RTB" \
  --destination-cidr-block 0.0.0.0/0 --nat-gateway-id "$NAT"

echo "Resumed. NAT $NAT in $SUBNET; route in $RTB updated."
