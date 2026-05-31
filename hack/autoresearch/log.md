# Autoresearch experiment log

The running record of autoresearch experiments. The agent appends one entry per
experiment (kept or discarded) following the format in
[`program.md`](program.md). Autoresearch experiments do not get ADRs; this log
is the record of what was tried and why.

Each entry looks like:

```markdown
## <n>. <short title> - KEPT | DISCARDED
- Hypothesis: <what you expected and why>
- Change: <files / approach>
- Correctness: fast gate <pass/fail>, judge <approved/rejected[: reason]>
- Primary: <best> -> <new> (<+/-%>)
- Secondary: alloc <..>, ns <..>, cpu <..>, mutexWait <..>
- Outcome & why: <why kept or discarded; what was learned>
- Commit: <hash if kept>
```

---

<!-- Experiment entries are appended below. -->

## 0. Baseline - SETUP BLOCKED (baseline `--full` gate FAILS)

Setup could not be completed: `hack/autoresearch/check.sh --full` fails at the
serializability fuzzer, so the experiment loop was **not** started, per the
"baseline `--full` gate fails" stop condition in `program.md`. No implementation
files were modified.

- Tree/branch: clean working tree on `autoresearch` (verified before and after).
- Baseline primary score (deterministic, **informational only**): **434.99**
  (geomean over the 5 workloads). Per-workload cost/tx: singleRMW 71.6,
  multiRMW10 1085.7, batchRead10 313.7, batchWrite100 20191.4, readRepeat 31.5.
  This is **not** a valid baseline to optimize against, because correctness is
  violated (below). `baseline.json` / `best.json` were intentionally NOT written.

### Correctness failure (strict serializability violated)

`go test -run='^$' -fuzz='^FuzzConcurrentTx$' -fuzztime=120s .` reliably fails
within ~18s with a lost-update violation:

```
fuzz_test.go:151: Not equal: expected: 7, actual: 4  (k1 mismatch)
```

- Reproduced on two independent fresh fuzz runs (~18s each); always the same
  symptom on `k1`: the final stored value (4) is less than the sum of the
  increments the test observed committing successfully (7) - 3 increments lost.
  A lost update is a strict-serializability violation, the one guarantee the
  mission calls non-negotiable.
- The individual failing inputs the fuzzer saved
  (`702cf6a2542b43f8`, `947ad560aee13e61`) do **not** reproduce deterministically
  on replay (`-run=FuzzConcurrentTx/<id> -count=200 -race` passes). The fuzzer
  runs inside a `synctest` bubble, but execution is not fully deterministic per
  input: transaction IDs carry an 8-byte random prefix (`data.NewTId`) and
  validation iterates Go maps (`initValidation`, `Tx.collectAccesses`) in random
  order. So the bug is timing/ID dependent, but the fuzzer finds *an* offending
  schedule on every run. The two untracked reproducers generated during this run
  were removed to keep the suite from going permanently flaky; the bug
  reproduces from any fresh fuzz run regardless.
- Implication: the fast gate (`check.sh`, 30s fuzz) can hit this too; it cannot
  be trusted as green, so no experiment could ever be legitimately *kept*.

### Root-cause hypothesis (for the human)

`k1` is the only key exercised by overlapping single-key RMW and multi-key RMW
from two DB clients, which points at the interaction between the single-RW CAS
fast path (`commitSingleRW` in `internal/trans/algo.go` - commits with a
`WriteIf` keyed on possibly-stale cached metadata and writes no tx log) and the
write-lock + async write-back path used by multi-key transactions. The most
recent algorithm change, `aa062b5 "Use wound-wait for conflict resolution
(#71)"`, reworked exactly this conflict-resolution/locking path and is the prime
suspect for the regression.

## Session summary

- Experiments run: **0** (setup blocked by failing baseline correctness gate).
- Kept: 0.
- Baseline score -> best score: **n/a** - no valid baseline established because
  the baseline fails the strict-serializability gate. Informational
  deterministic score: 434.99.
- Action needed before autoresearch can resume: fix the lost-update /
  strict-serializability bug surfaced by `FuzzConcurrentTx` (suspect:
  wound-wait conflict resolution `aa062b5` and/or the single-RW fast path).

### Resolution

The bug was fixed (see [ADR-007](../../docs/adr/007-single-rw-cache-lost-update.md)).
Root cause: validation (`validateLockedRead` / `validateReadNotFound`) cached a
*guessed* value paired with the real writer ID when that writer committed via the
single-RW fast path and therefore had no transaction log for `CommittedValue` to
resolve. That poisoned cache entry later let a stale single-RW commit pass
`checkReadVersionUnlocked` and clobber a newer value. The fix only refreshes the
cache when `CommittedValue` resolves authoritatively, and otherwise invalidates
and re-reads from storage. The fuzzer was also made deterministic (injectable
tx-ID source, stable map-derived ordering, no backoff jitter) so the bug could be
diagnosed and regression-tested. After the fix, a 120k-schedule replay and a
~1.1M-exec fuzz run pass. The autoresearch baseline can now be re-established.

---

# Session 2 (after the lost-update fix)

## 1. Baseline - ESTABLISHED

The full gate now passes (`check.sh --full`: `make test` + 120s FuzzConcurrentTx
+ 120s FuzzAlgoConcurrentTx, all green), so the experiment loop can run.

- Tree/branch: clean working tree on `autoresearch`, baseline taken at commit
  `97e23b3` (the lost-update fix).
- Baseline primary score (median of 3): **435.23**. Per-workload cost/tx:
  singleRMW 72.07, multiRMW10 1086.01, batchRead10 313.66, batchWrite100
  20191.38, readRepeat 31.51.
- Secondary (per tx): allocBytes 21545, allocs 272.6, ns 47733, cpuNs 111112,
  mutexWaitNs 1249.
- `baseline.json` and `best.json` written (both = 435.23). New experiments must
  beat `best.json`.

## 2. Skip metadata fetch on the create-lock path - KEPT
- Hypothesis: blind writes to non-existent keys do two backend metadata reads
  per key - one in the failed write-lock attempt (round 1) and one in
  `lockCreate`'s `fetchLockInfo` (round 2). The create read is wasted:
  `ComputeLockUpdate` ignores current lock info for create (it always resolves
  to a single `WriteIfNotExists`), and `needsProcessing` already filters
  already-held locks. So skipping it should halve `batchWrite100` metaReads
  (200->100/tx) with no behavior change.
- Change: `internal/trans/tlocker.go` `doLockOp` - for `LockTypeCreate` requests
  skip `fetchLockInfo`/`fetchLockersState` and feed an empty (None) lock info to
  `ComputeLockUpdate`. Create requests never merge with unlocks, so there is no
  unlock to compute.
- Correctness: fast gate pass, full gate pass (make test + 120s FuzzConcurrentTx
  + 120s FuzzAlgoConcurrentTx), judge approved.
- Primary: 435.23 -> 420.51 (-3.38%).
- Secondary: alloc ~flat, ns 47733 -> 42656 (-10.6%), cpu 111112 -> 99679
  (-10.3%), mutexWait 1249 -> 688 (-44.9%). No regressions.
- Outcome & why: KEPT. `batchWrite100` cost dropped 15.4% (20191 -> 17091),
  exactly the ~100 metaReads/tx removed; other workloads unchanged (they create
  few keys in the measured body). Confirms the create path's metadata read was
  pure overhead. Lesson: the lock layer fetches metadata uniformly even when the
  computed update doesn't depend on it - other always-one-shot operations may
  have similar redundant reads.
- Commit: f56a4a3

## 3. Run single-element fanouts inline - KEPT (secondary)
- Hypothesis: `Algo.fanout` spawns a goroutine + errgroup even for one item.
  Single-element fanouts happen every transaction (`validateReadonly` for a
  1-key read; `lockCollections` for the single collection of multiRMW /
  batchRead / batchWrite), so a `num==1` inline fast path should cut per-tx
  goroutine churn (CPU, ns, allocs, mutex wait) with no change to backend ops
  (primary stays flat: fanout is execution strategy, not an op generator).
- Change: `internal/trans/algo.go` `fanout` - return `fn(ctx, 0)` directly when
  `num==1`. Equivalent behavior: with no siblings there is nothing to run
  concurrently or to cancel.
- Correctness: fast + full gate pass, judge approved.
- Primary: 420.51 -> 420.56 (+0.01%, noise; op counts unchanged - verified
  singleRMW/multiRMW counts identical across runs, only async-cleanup metaWrite
  +-1 jitter).
- Secondary: allocs 272.4 -> 255.9 (-6.2%), allocBytes -6.4%, ns 42656 -> 30371
  (-20%+), cpuNs 99679 -> 66745 (-27%+), mutexWait 688 -> 528 (-24%). No
  regressions.
- Outcome & why: KEPT under the secondary-axis rule (primary within noise, every
  secondary axis clearly improves, none regress). The single-element goroutine
  spawn was pure overhead on the hot per-tx path. Lesson: fan-out helpers should
  short-circuit trivial sizes; worth auditing the background fanout in
  asyncCleanup too (left for later - it is off the measured critical path).
- Commit: 42e2696
