#!/usr/bin/env bash
# One-time setup so the laptop can run repo code on the simulator EC2.
#  1. S3 bucket that carries repo files to the EC2 (private, encrypted).
#  2. Extra permissions on the EC2 role: read that bucket, look up the MSK brokers.
# Safe to re-run: every step checks first.
source "$(dirname "$0")/common.sh"

echo "== Artifacts bucket: $ARTIFACTS_BUCKET"
if aws s3api head-bucket --bucket "$ARTIFACTS_BUCKET" 2>/dev/null; then
  echo "   exists"
else
  aws s3api create-bucket --bucket "$ARTIFACTS_BUCKET" \
    --create-bucket-configuration LocationConstraint="$AWS_REGION" >/dev/null
  aws s3api put-public-access-block --bucket "$ARTIFACTS_BUCKET" \
    --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
  aws s3api put-bucket-tagging --bucket "$ARTIFACTS_BUCKET" \
    --tagging 'TagSet=[{Key=project,Value=rs}]'
  echo "   created (private, SSE-S3 default encryption)"
fi

echo "== Inline policy rs-sim-artifacts on role $SIM_ROLE"
POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadRepoArtifacts",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${ARTIFACTS_BUCKET}",
        "arn:aws:s3:::${ARTIFACTS_BUCKET}/*"
      ]
    },
    {
      "Sid": "LookUpMskBrokers",
      "Effect": "Allow",
      "Action": ["kafka:ListClustersV2", "kafka:GetBootstrapBrokers", "kafka:DescribeClusterV2"],
      "Resource": "*"
    }
  ]
}
EOF
)
aws iam put-role-policy --role-name "$SIM_ROLE" \
  --policy-name rs-sim-artifacts --policy-document "$POLICY"
echo "   applied"

echo "== Inline policy rs-sim-msk on role $SIM_ROLE (MSK data access, as code)"
MSK_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ClusterAccess",
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:WriteDataIdempotently"
      ],
      "Resource": "arn:aws:kafka:${AWS_REGION}:${EXPECTED_ACCOUNT}:cluster/rs-msk/*"
    },
    {
      "Sid": "TopicsAndGroups",
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:*Topic*",
        "kafka-cluster:WriteData",
        "kafka-cluster:ReadData",
        "kafka-cluster:AlterGroup",
        "kafka-cluster:DescribeGroup"
      ],
      "Resource": [
        "arn:aws:kafka:${AWS_REGION}:${EXPECTED_ACCOUNT}:topic/rs-msk/*",
        "arn:aws:kafka:${AWS_REGION}:${EXPECTED_ACCOUNT}:group/rs-msk/*"
      ]
    }
  ]
}
EOF
)
aws iam put-role-policy --role-name "$SIM_ROLE" \
  --policy-name rs-sim-msk --policy-document "$MSK_POLICY"
echo "   applied"

echo "Bootstrap done."
