# ADR-004: Fake S3 backend fidelity for benchmarking

## Status

Accepted

## Context

[ADR-003](003-s3-benchmark-reproduction.md) added a way to run the `rw9010` and
`deadlock` benchmarks against a real Amazon S3 bucket, and committed the
resulting reference CSVs under [`hack/aws-bench/out/`](../../hack/aws-bench/out/)
(`throughput.csv`, `stats.csv`, `deadlock.csv`).

Running those benchmarks against real S3 is slow and costs money (a private VPC,
four interface endpoints, an EC2 run). For day-to-day work we want to model S3
locally with the **fake backend**: the in-memory backend (`memory.New()`)
wrapped by `middleware.NewDelayBackend(..., middleware.S3Delays)`, which adds
simulated per-operation latency. The goal is not a pixel-perfect replay of one
real run but a model good enough to **design and test algorithm changes** — so
it must track real S3 in *shape and relative behavior*, and be built from
explainable S3 properties rather than constants fitted to a single run.

The comparable metrics (the only ones for which committed real-S3 data exists)
are **transaction throughput**, **retries per transaction**, and **deadlock
latency**. A first run with the original `S3Delays` revealed two gaps:

1. **No request-rate ceiling.** Real S3 throughput is roughly flat once
   concurrency saturates the bucket: per-DB strong-read throughput stays
   ~110 tx/s up to 10 DBs, then falls as DBs are added so that *total*
   throughput plateaus in the low thousands of tx/s. The fake backend, being
   CPU-free sleeps with no shared bottleneck, instead scaled **linearly** —
   ~2.6× the real total at 50 DBs — and its retry rate climbed with the excess
   throughput.
2. **Latency calibration.** At a single DB (no contention), every transaction
   type was uniformly ~25 % slower than real S3, and deadlock-resolution latency
   under contention was far too high (the 2-key/100 %-overlap median was ~2.3 s
   versus ~0.3 s on real S3).

## Decision

### 1. Expose the S3 delay profile in `rtbench`

`hack/rtbench` previously hard-wired the memory backend to `GCSDelays`. Added a
`-delays gcs|s3` flag (default `gcs`, preserving existing behaviour) so the same
binary can emulate either cloud, e.g. `rtbench -backend=memory -delays=s3`.

### 2. Model S3's documented per-prefix request-rate limit

`middleware.DelayBackend` already had a per-*object* write limiter
(`SameObjWritePs`), which for S3 is effectively disabled. That does not capture
S3's real per-prefix bottleneck: a partitioned prefix sustains at least 5,500
GET/HEAD and 3,500 PUT/COPY/POST/DELETE per second, returning 503 SlowDown above
that (see [docs/s3.md](../s3.md), "Request Throttling & Hot Prefixes").

We added a continuous **token-bucket prefix limiter** to `DelayBackend`, with
three new `DelayOptions` fields:

- `PrefixReadPs` / `PrefixWritePs` — GET/HEAD and PUT/POST/DELETE request-rate
  caps applied per prefix. A request that would exceed the rate is *delayed*
  (not failed) until the bucket refills, so the cap bounds throughput without
  inflating transaction-retry counts.
- `PrefixDepth` — how many leading `/`-separated path segments define a prefix,
  i.e. the partition granularity.

The existing `rateLimiter` (tuned for infrequent per-object writes, with its
documented burst-then-debt semantics) was left untouched; the new limiter is a
separate, standard token bucket that behaves correctly under thousands of
concurrent acquisitions per second.

**We deliberately use the documented S3 numbers** (`PrefixReadPs: 5500`,
`PrefixWritePs: 3500`) rather than a value fitted to one benchmark run. An
earlier iteration fitted the cap to a measured plateau (~7.6k GET/HEAD, ~1.8k
PUT/s) and reproduced the reference run closely (throughput geomean 1.02). But
that number is **not explainable**: the reference runs recorded *zero* 503
SlowDown and only ~13 % client CPU, so the plateau was never S3 throttling — it
was the latency of GlassDB's multi-round-trip write-commit path. A cap fitted to
that plateau silently bakes in the *current* commit path: optimize the algorithm
(say, fewer sequential round-trips) and the fitted cap would clamp the fake to
the old throughput and hide the win — exactly where you would want to measure it.
Since the only purpose of this backend is to **design and test such changes**
(see [ADR-005](005-s3-write-commit-throughput.md), which proposes exactly such
commit-path round-trip reductions and validates them on this backend), faithful
*relative* behavior is worth more than matching one run's absolute numbers, and
the model must rest on an S3 invariant rather than a GlassDB artifact. We accept
that absolute throughput is now only approximate.

The documented limit is *per partitioned prefix*, so `PrefixDepth` controls how
many partitions we model. GlassDB lays objects out as `<db>/_t/<txid>`
(transaction log) and `<db>/_c/<coll>/_k/<key>` (data), so depth 2 throttles the
transaction-log subtree (`<db>/_t`) and the data subtree (`<db>/_c`) as two
independent prefixes — the granularity at which a commit-path change that shifts
work between them becomes visible. `rtbench` exposes `-prefix-depth` to vary
this per run; S3 auto-partitions hot prefixes, so a higher depth models more
partitions and more aggregate throughput. The benchmark funnels every database
through one shared `bench` root, which is the extreme low-partition case.

### 3. Recalibrate the latency means

The per-operation latency means were lowered so that the uncontended (1 DB)
throughput matches real S3 within a few percent, and so that the *parallel*
lock-acquisition path beats GlassDB's deadlock-timeout heuristic
(`min(4·lockLatency·keys, 5 s)`, where `lockLatency` is a fixed 90 ms estimate
in `internal/trans/algo.go`). With the original, higher write latencies the
parallel path overran that timeout and fell back to a slow serial restart, which
is what produced the multi-second deadlock medians. Lowering the means so the
fake's effective lock latency is consistent with the 90 ms assumption — as real
S3's latency evidently was — removed that pathology *without* touching the
production constant.

| Operation | Original mean | Calibrated mean |
|---|---|---|
| MetaRead  | 28 ms | 21 ms |
| MetaWrite | 100 ms | 75 ms |
| ObjRead   | 30 ms | 22 ms |
| ObjWrite  | 74 ms | 55 ms |
| List      | 30 ms | 22 ms |

### 4. A comparison script

Added [`hack/aws-bench/compare.py`](../../hack/aws-bench/compare.py) (a
single-file `uv` script, like `plot.py`). It reads two result directories and
reports, per concurrency level, the fake/real ratio for total throughput (per
tx-type), retries per transaction, and deadlock p50/p90 per contended-key count,
and writes overlay PNGs. A ratio near 1.0 means a faithful match.

## Results

The latency means (§3) keep the **uncontended** region accurate: at 10–50
concurrent transactions the fake tracks real within a few percent (ratio 1.12 →
1.02). The documented per-prefix limit then binds at higher concurrency and the
throughput holds a flat plateau, trading absolute accuracy for an explainable,
algorithm-sensitive ceiling.

Full-scale `compare.py` run (`-max-dbs=50 -num-keys=50000 -duration=60s`;
deadlock `-duration=20s`) against the real-S3 data. Ratios are fake/real.

| Metric | Fitted cap (7600/1800, superseded) | Documented limit (5500/3500, depth 2) |
|---|---|---|
| Throughput (per tx-type) | geomean 1.02 (0.93–1.12) | geomean **0.82** (0.68–1.12) |
| Retries / tx | geomean 0.89 | geomean **0.60** |
| Deadlock p50 | geomean 1.75 | geomean **1.75** (unchanged) |

The data prefix saturates at the documented 5,500 GET/s, so the fake plateaus
near ~1.16k strong-read tx/s versus real's ~1.5–1.7k — about 30 % low at high
concurrency, with retries low in proportion (fewer commits → fewer conflicts).
That gap is **expected and accepted**: real S3 sustained ~7.6k GET/s with *zero*
throttling because it had auto-partitioned the hot prefix, which a single
`/`-delimited prefix at the documented per-prefix rate cannot represent. The
point of the change is that the ceiling is now an S3 invariant, not a fitted
constant — an algorithm that issues fewer requests per transaction, or spreads
keys across more prefixes, moves the plateau up the way it would on real S3,
whereas the fitted cap would have hidden the change. Deadlock latency is
unaffected (the deadlock workload's request rate is far below any cap).

## Consequences

- The `rw9010` and `deadlock` benchmarks can be reproduced locally with no AWS
  access via `rtbench -backend=memory -delays=s3`, and quantitatively compared
  to the committed real-S3 data with `compare.py`.
- `S3Delays` carries S3's documented per-prefix request-rate limit (5,500
  GET/HEAD, 3,500 PUT/s per prefix). This does not affect the in-package tests,
  which run small, time-compressed workloads via `Scale` (the limiter scales its
  rate by `Scale`, and the workloads stay far below the caps).
- The limit is a known S3 invariant rather than a fitted number, so it predicts
  the *direction* of algorithm changes: reducing requests per transaction or
  partitioning the key space raises throughput, as on real S3. This is the whole
  point — the backend exists to design and test such changes, and a cap fitted
  to the current commit path would have masked exactly those improvements.
- **Known limitations**, called out so the model is not over-trusted:
  - Absolute throughput is ~30 % low at high concurrency. Real S3 sustained
    ~7.6k GET/s against the hot prefix with zero 503s because it auto-partitions
    a hot prefix into several; a single `/`-delimited prefix at the documented
    5,500 GET/s cannot reproduce that. Calibrate expectations to *shape and
    relative change*, not absolute tx/s.
  - `PrefixDepth` is coarse for this benchmark specifically: every transaction
    uses one shared collection, so all data keys collapse to one prefix
    (`bench/_c/<coll>`) until the depth reaches the per-key segment, at which
    point the read cap stops binding entirely. Intermediate depths only
    partition meaningfully for workloads with many collections or databases.
  - Deadlock latency is reproduced in *shape* (tens of ms single-key, a few
    hundred ms multi-key) at ~1.8× the real median; the multi-key medians remain
    a bit high because the fixed `lockLatency` heuristic in the algorithm cannot
    be matched exactly by a single latency distribution.
