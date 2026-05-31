#!/usr/bin/env bash
#
# Deploy (or tear down) the GlassDB S3 benchmark stack.
#
# Usage:
#   deploy.sh deploy        # build binary, create/update stack, upload binary
#   deploy.sh logs          # stream the live benchmark log over SSM
#   deploy.sh results [ts]  # download a run's CSVs into out/ (latest by default)
#   deploy.sh teardown      # empty the bucket and delete the stack
#
# `logs` needs the AWS Session Manager plugin installed locally:
#   https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
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
OUT_DIR="${OUT_DIR:-$DIR/out}"

region_args=()
if [[ -n "${AWS_REGION:-}" ]]; then
  region_args=(--region "$AWS_REGION")
fi

stack_output() {
  aws cloudformation describe-stacks "${region_args[@]}" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" \
    --output text
}

bucket_name() {
  stack_output BucketName
}

instance_id() {
  stack_output InstanceId
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
  echo "Stream the live log with: $0 logs"
  echo "Download results when done with: $0 results"
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

cmd_logs() {
  local iid
  iid="$(instance_id)"
  echo ">> streaming /var/log/rtbench-bootstrap.log from $iid (Ctrl-C to stop)"
  # -F retries until the file appears (the instance may still be booting) and
  # -n +1 replays the log from the start before following it live.
  aws ssm start-session "${region_args[@]}" \
    --target "$iid" \
    --document-name AWS-StartInteractiveCommand \
    --parameters command="sudo tail -n +1 -F /var/log/rtbench-bootstrap.log"
}

cmd_results() {
  local bucket latest
  bucket="$(bucket_name)"
  # Result timestamps are %Y%m%dT%H%M%S, which sorts chronologically. Use the
  # one given as the second argument, otherwise pick the most recent.
  latest="${2:-}"
  if [[ -z "$latest" ]]; then
    latest="$(aws s3 ls "${region_args[@]}" "s3://$bucket/results/" \
      | awk '/ PRE / {print $2}' | sort | tail -n1)"
  fi
  latest="${latest%/}"
  if [[ -z "$latest" ]]; then
    echo "no results found under s3://$bucket/results/ yet" >&2
    exit 1
  fi
  mkdir -p "$OUT_DIR"
  echo ">> downloading s3://$bucket/results/$latest/ into $OUT_DIR"
  aws s3 cp "${region_args[@]}" \
    "s3://$bucket/results/$latest/" "$OUT_DIR/" --recursive

  # samples.csv is by far the largest file; keep it compressed on disk.
  # plot.py reads samples.csv or samples.csv.xz transparently.
  if [[ -f "$OUT_DIR/samples.csv" ]]; then
    if command -v xz >/dev/null; then
      echo ">> compressing samples.csv -> samples.csv.xz"
      xz -f "$OUT_DIR/samples.csv"
    else
      echo "note: xz not installed; leaving samples.csv uncompressed" >&2
    fi
  fi

  echo ">> render with: uv run hack/aws-bench/plot.py"
}

case "${1:-deploy}" in
deploy) cmd_deploy ;;
logs) cmd_logs ;;
teardown) cmd_teardown ;;
results) cmd_results "$@" ;;
*)
  echo "usage: $0 {deploy|logs|results|teardown}" >&2
  exit 2
  ;;
esac
