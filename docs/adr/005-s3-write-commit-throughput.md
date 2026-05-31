# ADR-005: Lifting the S3 write-commit throughput ceiling

## Status

Proposed

## Context

[ADR-003](003-s3-benchmark-reproduction.md) reproduced the README graphs on real
S3, and [ADR-004](004-fake-s3-backend-fidelity.md) calibrated a local fake S3
backend to match them. Both observed that real-S3 `rw9010` throughput
**plateaus** once concurrency passes ~150-200 concurrent transactions, and
ADR-004 attributed the plateau (and a slight post-peak roll-off) to **CPU
saturation on the fixed-size EC2 host**. That explanation was never directly
measured.

To test it, we added client-side instrumentation to `hack/rtbench`
(`client-stats.csv`): process CPU time via `getrusage`, HTTP request / throttle
(503 SlowDown, 429) / 5xx / connection counters via `httptrace`, and a peak
goroutine sampler. We then re-ran the full 1..50 DB sweep on a **48-vCPU
`c7i.12xlarge`** — 6× the vCPUs of the original 8-vCPU `c7i.2xlarge`.

The data refutes the CPU hypothesis:

| concurrent tx | cpu-util | S3 req/s | throttle | 5xx | strong-read p50 | write p50 / p90 |
| ------------- | -------- | -------- | -------- | --- | --------------- | --------------- |
| 100 (10 db)   | 7%       | 6,648    | 0        | 0   | 40 ms           | 140 / 208 ms    |
| 200 (20 db)   | 12%      | 9,595    | 0        | 0   | 39 ms           | 214 / 392 ms    |
| 500 (50 db)   | 13%      | 10,361   | 0        | 37  | 43 ms           | 494 / 955 ms    |

- **CPU peaked at ~13%** of 48 cores (~6 cores of real work). The client is
  nowhere near saturation.
- **Zero S3 throttling** across the whole sweep (`throttle=0`; 5xx ≈ 0.006%), so
  the backend is not rate-limiting us either.
- **6× the vCPUs did not raise throughput.** Strong-read throughput at 200
  concurrent transactions is **1534 tx/s**, essentially identical to the
  **1513 tx/s** ADR-004 measured on the 8-vCPU host. If CPU were the ceiling,
  quadrupling the cores would have moved it; it didn't.
- **Reads are flat, writes inflate.** Strong-read p50 stays ~39 ms and weak-read
  ~25 ms at every concurrency level, proving S3, the network, and the connection
  pool all have headroom. Only write p50 climbs (140 → 494 ms; p90 178 → 955 ms).

The ceiling is therefore the **write-commit path**, not any client resource.
Tracing it (`internal/trans/algo.go` → `tlocker.go` → `internal/storage/locker.go`)
for the benchmark's write (reads 2 keys, writes those same 2 keys):

1. Lock both keys (parallel `fanout`). Each `lockWrite`:
   - `GetMetadata` (HEAD) to read current lock tags — usually cache-served;
   - `SetTagsIf` to stamp the write-lock tag, which **on S3 is a GET + PUT**
     (`backend/s3/s3.go`): the GET only fetches the body so the PUT can
     re-upload it unchanged, because an S3 PUT replaces the whole object and S3
     has no metadata-only update;
   - `GetMetadata` again (HEAD) to re-check the version after locking.
2. `commitWrites` → `mon.CommitTx`: 1 PUT of the transaction-log object.
3. Async cleanup (background, off the latency path but still on the request
   budget): write the 2 final values, each a PUT that also clears its lock tag.

Empirically (`stats.csv`), per committed write:

| concurrent tx | obj-write/w | meta-write/w | (strong-read: meta-read/sr, obj-read/sr) |
| ------------- | ----------- | ------------ | ---------------------------------------- |
| 10            | 3.00        | 2.00         | 2.03, 2.47                               |
| 200           | 3.27        | 3.35         | 2.05, 2.62                               |
| 500           | 3.25        | 3.40         | 2.06, 2.79                               |

`obj-write ≈ 3` is the tlog PUT plus 2 value PUTs. `meta-write` is the lock
stamps — **each a GET+PUT on S3** — and it climbs 2.0 → 3.4 as contention forces
extra lock round-trips, while the read side stays flat (`meta-read ≈ 2.05`
throughout). That asymmetry is exactly why write latency inflates and read
latency does not. For contrast, GCS implements `SetTagsIf` as a single
metadata-only `obj.Update` PATCH (`backend/gcs/gcs.go`), so the same lock stamp
costs one round-trip there versus two on S3.

## Decision

This ADR records the diagnosis and the direction. It implements **only** the two
low-risk changes already needed to study the problem; the write-path
optimizations are proposed, not implemented.

### Proposed

Optimizations to the write-commit path, biggest lever first:

1. **Drop the GET in the S3 lock stamp.** During write-lock acquisition the
   object value is unchanged and the transaction already holds it (it just read
   the key, and `storage.Local` caches it). Plumbing that value into the stamp so
   it becomes a single conditional PUT (`WriteIf`, which the locker already uses
   when a value is present) removes one of the two round-trips per write-lock —
   the single largest contributor, since `meta-write` is 2-3.4 GET+PUT pairs per
   write.
2. **Reuse the metadata the lock PUT returns** instead of the follow-up HEAD.
   `lockValidateFoundKey` issues a `GetMetadata` after `lockWrite` to re-check
   the version, but `SetTagsIf`/`WriteIf` already return the new
   `backend.Metadata` (currently discarded in `applyLockTags`). Returning and
   using it removes a round-trip per read-and-written key.
3. **Cut contention round-trips.** Under contention `fetchLockersState` reads the
   current holder's status / committed value (`TxStatus` / `CommittedValue`),
   which is what pushes `meta-write/write` from 2.0 to 3.4. Caching tx status
   more aggressively in the monitor reduces these added reads on hot keys.

### Validation plan

We iterate entirely on the fake S3 backend for now
(`rtbench -backend=memory -delays=s3`, per ADR-004), refining it as needed to
model the commit round-trips faithfully. Each change is measured there for the
round-trip reduction while keeping the throughput / retry ratios calibrated.
Success is aggregate tx/s rising past 200 concurrent transactions and write
p50/p90 falling, with read latency and the retry rate unchanged. A real-S3 run is
deferred until the direction is proven on the fake backend.

## Consequences

- Effort is aimed at the measured ceiling (commit round-trips) instead of host
  size; the instance downsize already cuts benchmark cost roughly in half.
- The lock/commit path is correctness-sensitive: removing the GET must preserve
  the `If-Match`/ETag CAS and never rewrite a stale body, and caching tx status
  must not return stale commit state under wound-wait
  ([ADR-002](002-wound-wait-locking.md)) or break crash recovery via the tlog.
  Every change must keep the existing fuzz and integration tests green.
- This refines [ADR-004](004-fake-s3-backend-fidelity.md): the plateau **level**
  is set by the write-commit path, not CPU; CPU only adds a mild roll-off on
  small hosts. Prompted by this, ADR-004 switched the fake's per-prefix ceiling
  from a cap *fitted* to the current plateau to S3's *documented* per-prefix
  limit, precisely so that reducing round-trips lifts the fake's throughput
  instead of being clamped to the old level. The per-operation latencies still
  model the GET+PUT lock-stamp cost, so dropping a round-trip shows up as fewer
  requests per write rather than needing a recalibration of the caps.
- The biggest win is specific to S3 (its lack of a metadata-only update); GCS
  already pays one round-trip per lock stamp, so these changes narrow the S3/GCS
  gap rather than speeding up GCS.
