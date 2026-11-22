#!/usr/bin/env bash
# aws-nuke.sh - list (default) or delete every billable resource in the
# project account 605499974190, in every enabled region.
#
#   ./infra/scripts/aws-nuke.sh            # read-only inventory
#   ./infra/scripts/aws-nuke.sh --delete   # delete (asks you to type the account id)
#
# KEPT on purpose: the account itself, AWS-managed roles (AccountFullAccessRole,
# /managed/, /aws-service-role/, AWSReservedSSO_*, OrganizationAccountAccessRole),
# default VPCs, AWS Budgets and Cost Anomaly monitors (free, they protect you).
set -uo pipefail

PROFILE="rs-personal"
GUARD="605499974190"
MODE="list"
[[ "${1:-}" == "--delete" ]] && MODE="delete"

aws() { command aws --profile "$PROFILE" "$@"; }
say() { printf '%s\n' "$*"; }
hdr() { printf '\n=== %s ===\n' "$*"; }
run() { if [[ "$MODE" == "delete" ]]; then say "  DELETE: $*"; "$@" >/dev/null 2>&1 || say "    ! failed (will retry/report)"; fi; }

# ---------- account guard ----------
ACC=$(aws sts get-caller-identity --query Account --output text 2>/dev/null) || { say "Not logged in. Run: aws login --profile $PROFILE"; exit 1; }
if [[ "$ACC" != "$GUARD" ]]; then say "STOP: profile $PROFILE is account $ACC, expected $GUARD"; exit 1; fi
say "Account $ACC OK. Mode: $MODE"

if [[ "$MODE" == "delete" ]]; then
  printf 'This deletes everything listed. Type the account id to continue: '
  read -r ans; [[ "$ans" == "$GUARD" ]] || { say "Aborted."; exit 1; }
fi

REGIONS=$(aws ec2 describe-regions --region ap-southeast-2 --query 'Regions[].RegionName' --output text)

wait_until_empty() { # $1=description $2=max seconds $3...=command that prints remaining ids
  local desc="$1" max="$2"; shift 2; local t=0
  [[ "$MODE" == "delete" ]] || return 0
  while [[ -n "$("$@" 2>/dev/null | tr -d '[:space:]None')" ]]; do
    (( t >= max )) && { say "  ! timed out waiting for $desc"; return 1; }
    say "  ...waiting for $desc (${t}s)"; sleep 20; t=$((t+20))
  done
}

for R in $REGIONS; do
  hdr "Region $R"

  # CloudFormation stacks (skip AWS-managed StackSet stacks)
  for s in $(aws cloudformation list-stacks --region "$R" --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE ROLLBACK_COMPLETE UPDATE_ROLLBACK_COMPLETE --query 'StackSummaries[].StackName' --output text 2>/dev/null); do
    [[ "$s" == StackSet-* || "$s" == AWS* ]] && { say "CFN stack (kept, AWS-managed): $s"; continue; }
    say "CFN stack: $s"; run aws cloudformation delete-stack --region "$R" --stack-name "$s"
  done

  # MSK Connect connectors, plugins, worker configs
  for c in $(aws kafkaconnect list-connectors --region "$R" --query 'connectors[].connectorArn' --output text 2>/dev/null); do
    say "MSK Connect connector: $c"; run aws kafkaconnect delete-connector --region "$R" --connector-arn "$c"
  done
  wait_until_empty "MSK connectors" 900 aws kafkaconnect list-connectors --region "$R" --query 'connectors[].connectorArn' --output text
  for p in $(aws kafkaconnect list-custom-plugins --region "$R" --query 'customPlugins[].customPluginArn' --output text 2>/dev/null); do
    say "MSK Connect plugin: $p"; run aws kafkaconnect delete-custom-plugin --region "$R" --custom-plugin-arn "$p"
  done
  for w in $(aws kafkaconnect list-worker-configurations --region "$R" --query 'workerConfigurations[].workerConfigurationArn' --output text 2>/dev/null); do
    say "MSK Connect worker config: $w"; run aws kafkaconnect delete-worker-configuration --region "$R" --worker-configuration-arn "$w"
  done

  # MSK clusters
  for k in $(aws kafka list-clusters-v2 --region "$R" --query 'ClusterInfoList[].ClusterArn' --output text 2>/dev/null); do
    say "MSK cluster: $k"; run aws kafka delete-cluster --region "$R" --cluster-arn "$k"
  done
  wait_until_empty "MSK clusters" 1800 aws kafka list-clusters-v2 --region "$R" --query 'ClusterInfoList[].ClusterArn' --output text
  for k in $(aws kafka list-configurations --region "$R" --query 'Configurations[].Arn' --output text 2>/dev/null); do
    say "MSK configuration: $k"; run aws kafka delete-configuration --region "$R" --arn "$k"
  done

  # RDS
  for d in $(aws rds describe-db-instances --region "$R" --query 'DBInstances[].DBInstanceIdentifier' --output text 2>/dev/null); do
    say "RDS instance: $d"
    run aws rds modify-db-instance --region "$R" --db-instance-identifier "$d" --no-deletion-protection --apply-immediately
    run aws rds delete-db-instance --region "$R" --db-instance-identifier "$d" --skip-final-snapshot --delete-automated-backups
  done
  for d in $(aws rds describe-db-clusters --region "$R" --query 'DBClusters[].DBClusterIdentifier' --output text 2>/dev/null); do
    say "RDS cluster: $d"; run aws rds delete-db-cluster --region "$R" --db-cluster-identifier "$d" --skip-final-snapshot
  done
  wait_until_empty "RDS instances" 1800 aws rds describe-db-instances --region "$R" --query 'DBInstances[].DBInstanceIdentifier' --output text
  for d in $(aws rds describe-db-snapshots --region "$R" --snapshot-type manual --query 'DBSnapshots[].DBSnapshotIdentifier' --output text 2>/dev/null); do
    say "RDS snapshot: $d"; run aws rds delete-db-snapshot --region "$R" --db-snapshot-identifier "$d"
  done
  for g in $(aws rds describe-db-subnet-groups --region "$R" --query 'DBSubnetGroups[].DBSubnetGroupName' --output text 2>/dev/null); do
    [[ "$g" == default* ]] && continue
    say "RDS subnet group: $g"; run aws rds delete-db-subnet-group --region "$R" --db-subnet-group-name "$g"
  done

  # EC2 instances
  IDS=$(aws ec2 describe-instances --region "$R" --filters Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null)
  for i in $IDS; do say "EC2 instance: $i"; done
  if [[ -n "$IDS" && "$MODE" == "delete" ]]; then
    for i in $IDS; do aws ec2 modify-instance-attribute --region "$R" --instance-id "$i" --no-disable-api-termination >/dev/null 2>&1; done
    run aws ec2 terminate-instances --region "$R" --instance-ids $IDS
    aws ec2 wait instance-terminated --region "$R" --instance-ids $IDS 2>/dev/null
  fi

  # Load balancers
  for l in $(aws elbv2 describe-load-balancers --region "$R" --query 'LoadBalancers[].LoadBalancerArn' --output text 2>/dev/null); do
    say "Load balancer: $l"; run aws elbv2 delete-load-balancer --region "$R" --load-balancer-arn "$l"
  done
  for tg in $(aws elbv2 describe-target-groups --region "$R" --query 'TargetGroups[].TargetGroupArn' --output text 2>/dev/null); do
    say "Target group: $tg"; run aws elbv2 delete-target-group --region "$R" --target-group-arn "$tg"
  done

  # NAT gateways, then Elastic IPs
  for n in $(aws ec2 describe-nat-gateways --region "$R" --filter Name=state,Values=pending,available,failed --query 'NatGateways[].NatGatewayId' --output text 2>/dev/null); do
    say "NAT gateway: $n"; run aws ec2 delete-nat-gateway --region "$R" --nat-gateway-id "$n"
  done
  wait_until_empty "NAT gateways" 900 aws ec2 describe-nat-gateways --region "$R" --filter Name=state,Values=pending,available,deleting --query 'NatGateways[].NatGatewayId' --output text
  for a in $(aws ec2 describe-addresses --region "$R" --query 'Addresses[].AllocationId' --output text 2>/dev/null); do
    say "Elastic IP: $a"
    assoc=$(aws ec2 describe-addresses --region "$R" --allocation-ids "$a" --query 'Addresses[0].AssociationId' --output text 2>/dev/null)
    [[ "$assoc" != "None" && -n "$assoc" ]] && run aws ec2 disassociate-address --region "$R" --association-id "$assoc"
    run aws ec2 release-address --region "$R" --allocation-id "$a"
  done

  # VPC endpoints
  for e in $(aws ec2 describe-vpc-endpoints --region "$R" --query 'VpcEndpoints[].VpcEndpointId' --output text 2>/dev/null); do
    say "VPC endpoint: $e"; run aws ec2 delete-vpc-endpoints --region "$R" --vpc-endpoint-ids "$e"
  done

  # Secrets, ECR, log groups
  for s in $(aws secretsmanager list-secrets --region "$R" --query 'SecretList[].ARN' --output text 2>/dev/null); do
    say "Secret: $s"; run aws secretsmanager delete-secret --region "$R" --secret-id "$s" --force-delete-without-recovery
  done
  for r in $(aws ecr describe-repositories --region "$R" --query 'repositories[].repositoryName' --output text 2>/dev/null); do
    say "ECR repo: $r"; run aws ecr delete-repository --region "$R" --repository-name "$r" --force
  done
  for g in $(aws logs describe-log-groups --region "$R" --query 'logGroups[].logGroupName' --output text 2>/dev/null); do
    say "Log group: $g"; run aws logs delete-log-group --region "$R" --log-group-name "$g"
  done

  # AMIs, snapshots, leftover volumes
  for im in $(aws ec2 describe-images --region "$R" --owners self --query 'Images[].ImageId' --output text 2>/dev/null); do
    say "AMI: $im"; run aws ec2 deregister-image --region "$R" --image-id "$im"
  done
  for sn in $(aws ec2 describe-snapshots --region "$R" --owner-ids self --query 'Snapshots[].SnapshotId' --output text 2>/dev/null); do
    say "EBS snapshot: $sn"; run aws ec2 delete-snapshot --region "$R" --snapshot-id "$sn"
  done
  for v in $(aws ec2 describe-volumes --region "$R" --query 'Volumes[].VolumeId' --output text 2>/dev/null); do
    say "EBS volume: $v"; run aws ec2 delete-volume --region "$R" --volume-id "$v"
  done

  # Non-default VPCs and everything inside them
  for V in $(aws ec2 describe-vpcs --region "$R" --filters Name=is-default,Values=false --query 'Vpcs[].VpcId' --output text 2>/dev/null); do
    say "VPC: $V (with its subnets, route tables, security groups, IGW, NACLs, ENIs)"
    [[ "$MODE" == "delete" ]] || continue
    wait_until_empty "network interfaces in $V" 600 aws ec2 describe-network-interfaces --region "$R" --filters Name=vpc-id,Values="$V" Name=status,Values=in-use --query 'NetworkInterfaces[].NetworkInterfaceId' --output text
    for eni in $(aws ec2 describe-network-interfaces --region "$R" --filters Name=vpc-id,Values="$V" --query 'NetworkInterfaces[].NetworkInterfaceId' --output text); do
      run aws ec2 delete-network-interface --region "$R" --network-interface-id "$eni"
    done
    for sg in $(aws ec2 describe-security-groups --region "$R" --filters Name=vpc-id,Values="$V" --query "SecurityGroups[?GroupName!='default'].GroupId" --output text); do
      for rid in $(aws ec2 describe-security-group-rules --region "$R" --filters Name=group-id,Values="$sg" --query 'SecurityGroupRules[?IsEgress==`false`].SecurityGroupRuleId' --output text); do
        run aws ec2 revoke-security-group-ingress --region "$R" --group-id "$sg" --security-group-rule-ids "$rid"; done
      for rid in $(aws ec2 describe-security-group-rules --region "$R" --filters Name=group-id,Values="$sg" --query 'SecurityGroupRules[?IsEgress==`true`].SecurityGroupRuleId' --output text); do
        run aws ec2 revoke-security-group-egress --region "$R" --group-id "$sg" --security-group-rule-ids "$rid"; done
    done
    for sg in $(aws ec2 describe-security-groups --region "$R" --filters Name=vpc-id,Values="$V" --query "SecurityGroups[?GroupName!='default'].GroupId" --output text); do
      run aws ec2 delete-security-group --region "$R" --group-id "$sg"; done
    for sub in $(aws ec2 describe-subnets --region "$R" --filters Name=vpc-id,Values="$V" --query 'Subnets[].SubnetId' --output text); do
      run aws ec2 delete-subnet --region "$R" --subnet-id "$sub"; done
    for rt in $(aws ec2 describe-route-tables --region "$R" --filters Name=vpc-id,Values="$V" --query 'RouteTables[?!(Associations[?Main==`true`])].RouteTableId' --output text); do
      run aws ec2 delete-route-table --region "$R" --route-table-id "$rt"; done
    for ig in $(aws ec2 describe-internet-gateways --region "$R" --filters Name=attachment.vpc-id,Values="$V" --query 'InternetGateways[].InternetGatewayId' --output text); do
      run aws ec2 detach-internet-gateway --region "$R" --internet-gateway-id "$ig" --vpc-id "$V"
      run aws ec2 delete-internet-gateway --region "$R" --internet-gateway-id "$ig"; done
    for acl in $(aws ec2 describe-network-acls --region "$R" --filters Name=vpc-id,Values="$V" --query 'NetworkAcls[?IsDefault==`false`].NetworkAclId' --output text); do
      run aws ec2 delete-network-acl --region "$R" --network-acl-id "$acl"; done
    run aws ec2 delete-vpc --region "$R" --vpc-id "$V"
  done
done

# ---------- global: S3 ----------
hdr "S3 buckets (global)"
for B in $(aws s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null); do
  [[ "$B" == aws-* ]] && { say "Bucket (kept, AWS-managed): $B"; continue; }
  say "Bucket: $B"
  [[ "$MODE" == "delete" ]] || continue
  BR=$(aws s3api get-bucket-location --bucket "$B" --query LocationConstraint --output text 2>/dev/null); [[ "$BR" == "None" || -z "$BR" ]] && BR=us-east-1
  aws s3 rm "s3://$B" --recursive --region "$BR" >/dev/null 2>&1
  while :; do   # remove old versions and delete markers, 1000 at a time
    tmp=$(mktemp)
    aws s3api list-object-versions --bucket "$B" --region "$BR" --max-items 1000 \
      --query '{Objects: [Versions[].{Key:Key,VersionId:VersionId}, DeleteMarkers[].{Key:Key,VersionId:VersionId}][] }' --output json > "$tmp" 2>/dev/null
    grep -q '"Key"' "$tmp" || { rm -f "$tmp"; break; }
    aws s3api delete-objects --bucket "$B" --region "$BR" --delete "file://$tmp" >/dev/null 2>&1 || { rm -f "$tmp"; break; }
    rm -f "$tmp"
  done
  run aws s3api delete-bucket --bucket "$B" --region "$BR"
done

# ---------- global: IAM (only things we created, path "/") ----------
hdr "IAM (global)"
for role in $(aws iam list-roles --path-prefix / --query 'Roles[?Path==`/`].RoleName' --output text); do
  case "$role" in AccountFullAccessRole|OrganizationAccountAccessRole|AWSReservedSSO_*) say "Role (kept): $role"; continue;; esac
  say "Role: $role"
  [[ "$MODE" == "delete" ]] || continue
  for ip in $(aws iam list-instance-profiles-for-role --role-name "$role" --query 'InstanceProfiles[].InstanceProfileName' --output text); do
    run aws iam remove-role-from-instance-profile --instance-profile-name "$ip" --role-name "$role"; done
  for pa in $(aws iam list-attached-role-policies --role-name "$role" --query 'AttachedPolicies[].PolicyArn' --output text); do
    run aws iam detach-role-policy --role-name "$role" --policy-arn "$pa"; done
  for pn in $(aws iam list-role-policies --role-name "$role" --query 'PolicyNames[]' --output text); do
    run aws iam delete-role-policy --role-name "$role" --policy-name "$pn"; done
  run aws iam delete-role --role-name "$role"
done
for ip in $(aws iam list-instance-profiles --query 'InstanceProfiles[?Path==`/`].InstanceProfileName' --output text); do
  say "Instance profile: $ip"; run aws iam delete-instance-profile --instance-profile-name "$ip"
done
for pa in $(aws iam list-policies --scope Local --query 'Policies[?Path==`/`].Arn' --output text); do
  say "Customer policy: $pa"
  if [[ "$MODE" == "delete" ]]; then
    for v in $(aws iam list-policy-versions --policy-arn "$pa" --query 'Versions[?IsDefaultVersion==`false`].VersionId' --output text); do
      run aws iam delete-policy-version --policy-arn "$pa" --version-id "$v"; done
    run aws iam delete-policy --policy-arn "$pa"
  fi
done
for u in $(aws iam list-users --query 'Users[].UserName' --output text); do say "IAM user (listed only, not deleted): $u"; done

hdr "Done ($MODE)"
[[ "$MODE" == "list" ]] && say "Nothing was changed. Re-run with --delete to remove the items above."
[[ "$MODE" == "delete" ]] && say "Re-run without --delete to confirm nothing billable is left."
