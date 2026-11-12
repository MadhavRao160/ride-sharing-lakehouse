#!/usr/bin/env bash
# Shared settings + safety guard for every infra script.
# Source it:  source "$(dirname "$0")/common.sh"
set -euo pipefail

export AWS_PROFILE=rs-personal
export AWS_REGION=ap-southeast-2
export AWS_PAGER=""   # no "less" pager; print output straight to the terminal
EXPECTED_ACCOUNT=605499974190
ARTIFACTS_BUCKET="rs-artifacts-${EXPECTED_ACCOUNT}"
SIM_ROLE=rs-sim-ec2
SIM_NAME_TAG=rs-sim

# Safety: never act on any account except the personal project account.
ACCOUNT=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)
if [ "$ACCOUNT" != "$EXPECTED_ACCOUNT" ]; then
  echo "STOP: profile $AWS_PROFILE resolves to account '${ACCOUNT:-none}', expected $EXPECTED_ACCOUNT."
  echo "Run: aws login --profile $AWS_PROFILE --region $AWS_REGION"
  exit 1
fi
