#!/usr/bin/env bash
#
# Deploy (or tear down) the GlassDB S3 benchmark stack.
#
# Usage:
#   deploy.sh deploy     # build binary, create/update stack, upload binary
#   deploy.sh teardown   # empty the bucket and delete the stack
#   deploy.sh results    # list result prefixes in the bucket
#
# Configuration via environment variables (all optional):
#   STACK_NAME         CloudFormation stack name      (default: glassdb-bench)
#   AWS_REGION         AWS region                     (default: from aws config)
#   INSTANCE_TYPE      EC2 instance type              (default: c7i.2xlarge)
#   MAX_DBS            rw9010 max concurrent DBs       (default: 50)
#   NUM_KEYS           rw9010 number of keys           (default: 50000)
#   RUN_DURATION       rw9010 per-step duration        (default: 60s)
#   DEADLOCK_DURATION  deadlock per-config duration    (default: 20s)
#   AUTO_STOP          stop instance when done         (default: true)
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$DIR/../.." && pwd)"

STACK_NAME="${STACK_NAME:-glassdb-bench}"
INSTANCE_TYPE="${INSTANCE_TYPE:-c7i.2xlarge}"
MAX_DBS="${MAX_DBS:-50}"
NUM_KEYS="${NUM_KEYS:-50000}"
RUN_DURATION="${RUN_DURATION:-60s}"
DEADLOCK_DURATION="${DEADLOCK_DURATION:-20s}"
AUTO_STOP="${AUTO_STOP:-true}"
BINARY_S3_KEY="bin/rtbench"

region_args=()
if [[ -n "${AWS_REGION:-}" ]]; then
  region_args=(--region "$AWS_REGION")
fi

bucket_name() {
  aws cloudformation describe-stacks "${region_args[@]}" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='BucketName'].OutputValue" \
    --output text
}

cmd_deploy() {
  local bin
  bin="$(mktemp)"
  echo ">> building static rtbench binary"
  (cd "$REPO_ROOT" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o "$bin" ./hack/rtbench)

  echo ">> deploying stack $STACK_NAME"
  aws cloudformation deploy "${region_args[@]}" \
    --stack-name "$STACK_NAME" \
    --template-file "$DIR/cloudformation.yaml" \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides \
    "InstanceType=$INSTANCE_TYPE" \
    "MaxDBs=$MAX_DBS" \
    "NumKeys=$NUM_KEYS" \
    "RunDuration=$RUN_DURATION" \
    "DeadlockDuration=$DEADLOCK_DURATION" \
    "AutoStop=$AUTO_STOP" \
    "BinaryS3Key=$BINARY_S3_KEY"

  local bucket
  bucket="$(bucket_name)"
  echo ">> uploading binary to s3://$bucket/$BINARY_S3_KEY"
  aws s3 cp "${region_args[@]}" "$bin" "s3://$bucket/$BINARY_S3_KEY"
  rm -f "$bin"

  echo
  echo "Done. The instance will pull the binary, run the benchmarks, upload"
  echo "results to s3://$bucket/results/<timestamp>/, then stop itself."
  echo "Follow progress with: $0 results"
}

cmd_teardown() {
  local bucket
  bucket="$(bucket_name)"
  if [[ -n "$bucket" && "$bucket" != "None" ]]; then
    echo ">> emptying s3://$bucket"
    aws s3 rm "${region_args[@]}" "s3://$bucket" --recursive || true
  fi
  echo ">> deleting stack $STACK_NAME"
  aws cloudformation delete-stack "${region_args[@]}" --stack-name "$STACK_NAME"
  aws cloudformation wait stack-delete-complete "${region_args[@]}" \
    --stack-name "$STACK_NAME"
  echo ">> done"
}

cmd_results() {
  local bucket
  bucket="$(bucket_name)"
  echo "Bucket: s3://$bucket"
  echo "Result prefixes:"
  aws s3 ls "${region_args[@]}" "s3://$bucket/results/"
}

case "${1:-deploy}" in
deploy) cmd_deploy ;;
teardown) cmd_teardown ;;
results) cmd_results ;;
*)
  echo "usage: $0 {deploy|teardown|results}" >&2
  exit 2
  ;;
esac
