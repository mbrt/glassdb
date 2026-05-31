# ADR-003: Reproducing the README performance graphs on real S3

## Status

Accepted

## Context

The top-level `README.md` shows five performance figures (`tx-throughput`,
`tx-latency`, `ops-latency`, `retries`, `deadlock-latency`). They were
originally measured against Google Cloud Storage, and the raw data and plotting
code were not committed. With the S3 backend now supported (see
[ADR on the S3 design](../s3.md)), we want a repeatable way to regenerate those
graphs against a real Amazon S3 bucket — both to validate S3 performance and to
keep the README honest as the code evolves.

Three things were missing: a way to emit graph-ready data from the existing
benchmark, infrastructure to run it against real S3, and a script to turn the
data into the figures. A constraint from the target environment is that the
benchmark must run in a **VPC with private subnets only and private
connectivity** — no Internet Gateway and no NAT Gateway.

## Decision

**1. Extend `hack/rtbench`** rather than write a new benchmark. The `rw9010`
test already matches the README workload (50k keys, 1..50 DBs, 10 parallel
tx/DB, 10/60/30 write/strong-read/weak-read) and already supports `-backend=s3`.
We added:

- a `throughput.csv` output (per-DB transaction count + measured duration), so
  throughput is computed from the real run duration instead of a hard-coded
  constant;
- a `-test-name=deadlock` mode that replays the 5-worker, 1..6 shared-key
  contention scenario and dumps raw latency samples to `deadlock.csv`;
- `-max-dbs` / `-num-keys` / `-duration` flags (defaulting to the existing
  constants) for cheaper smoke runs.

**2. A dedicated, fully private CloudFormation stack**
(`hack/aws-bench/cloudformation.yaml`). It creates its own VPC, a private
subnet, and a private route table with no IGW/NAT. S3 is reached via a gateway
endpoint; shell access and self-stop go through SSM/EC2 interface endpoints.
Because the instance has no internet path, it cannot fetch Go or clone the repo,
so `deploy.sh` cross-compiles a static `rtbench` binary and uploads it to the
bucket; the instance pulls it over the gateway endpoint, runs both benchmarks,
uploads the CSVs to `results/<timestamp>/`, and stops itself. A poll loop in the
instance bootstrap bridges the "instance launched before binary uploaded"
ordering.

**3. A single-file `uv` plotting script** (`hack/aws-bench/plot.py`) using
pandas + seaborn. It reads the four CSVs (optionally downloading them from S3)
and renders the five figures with a median line and a 10th-90th percentile band,
matching the README.

Two interpretation choices are baked in and documented in the script:

- **Total throughput** is estimated as `num_db * median(per-DB rate)`; the
  percentile band comes from the spread between DBs (which widens as conflicts
  increase at high concurrency). This recovers the README's per-type, near-7k
  tx/s total scale from per-DB measurements.
- The **deadlock figure reflects current wound-wait behavior**, not the
  historical timeout-only behavior the committed README image shows. Under
  wound-wait, contention latency is bounded by a few storage operations instead
  of the multi-second timeout stalls (see [ADR-002](002-wound-wait-locking.md)),
  so the reproduced curve is expected to be much lower and flatter.

## Consequences

- The README graphs can be regenerated end-to-end with `deploy.sh deploy` ->
  wait -> `plot.py --s3 ...`, on any AWS account, without pre-existing
  networking.
- The benchmark itself stays backend-agnostic; only output and CLI surface grew.
- Real costs apply while the stack exists: S3 storage/requests, the EC2 run, and
  four hourly-billed interface endpoints. `deploy.sh teardown` removes them; the
  README spells out the cleanup.
- The reproduced `deadlock-latency.png` will not match the committed image
  pixel-for-pixel by design. Regenerating it with `--write-docs` would document
  the post-wound-wait behavior; we keep the historical image in `docs/img` and
  leave overwriting to a deliberate choice.
