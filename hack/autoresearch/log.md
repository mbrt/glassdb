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

## Analyzed but not attempted (would compromise correctness)

After exp 2-3 the primary is at the safe floor for these workloads; the op
counts of singleRMW (1 objWrite), multiRMW (10 metaWrite locks + 10 objWrite
write-backs + 1 log), batchRead/readRepeat (1 metaRead/key validation) are
minimal for strict-serializable S2PL. The only workload with reducible ops is
`batchWrite100` (100 metaReads to discover absence + 100 objWrites to create +
100 objWrites to write back). The two levers there were rejected:

- **Write the value into the create-lock placeholder** (turning the write-back
  objWrite into a cheap SetTagsIf). Unsafe: `reader.handleLockCreate` uses
  "non-empty value => committed" (`len(rv.Value) > 0`) to avoid an extra
  metadata read on every read. A non-empty create placeholder would be returned
  as a committed value, exposing uncommitted data (serializability violation).
  Fixing that would add a metadata read to every read, negating the gain.
- **Drop the create placeholder; create with value at write-back via
  WriteIfNotExists.** Removes the path reservation that guards against the
  collection-lock release racing a concurrent creator, and complicates crash
  recovery. Both make the serializability reasoning harder, which the mission
  says is not worth it.

Remaining ideas are allocation micro-opts in the concurrency-critical dedup/lock
path (per-op `*call` and interface-boxing allocations); uncertain payoff and
real regression risk in correctness-sensitive code, so not pursued in this
session.

## Session 2 summary

- Experiments run: 3 (after the lost-update fix). Kept: 2, discarded: 0.
- Baseline -> best primary score: **435.23 -> 420.56 (-3.4%)**, from skipping the
  redundant create-lock metadata read (exp 2). Exp 3 kept primary flat while
  cutting secondary axes substantially (allocs -6%, ns -20%+, cpuNs -27%, mutex
  wait -24%).
- Correctness: every kept change passed the full gate (make test + 120s
  FuzzConcurrentTx + 120s FuzzAlgoConcurrentTx) and the test-integrity judge.
- Stopped because the safe primary-optimization space for the current workloads
  is exhausted (see "Analyzed but not attempted"); further gains would require
  protocol changes that weaken strict-serializability reasoning.

---

# Session 3 (budget: 25 experiments or 3 hours)

## 1. Baseline - ESTABLISHED
- Tree/branch: clean working tree on `autoresearch-2` (the working branch for
  this run; HEAD carries Session 2's kept commits). Baseline taken after a
  passing full gate (`check.sh --full`: build + `make test` + 120s
  FuzzConcurrentTx, all green; took ~3.5 min).
- Baseline primary score (median of 3): **420.87**. Per-workload cost/tx:
  singleRMW 72.23, multiRMW10 1082.6, batchRead10 313.66, batchWrite100
  17091.38, readRepeat 31.51.
- Per-workload backend ops/tx (deterministic): singleRMW 1 objW; multiRMW10
  ~10 metaW (locks) + ~11 objW (10 write-back + 1 log); batchRead10 10 metaR
  (per-key read validation); batchWrite100 100 metaR (absence probe) + ~199
  objW (100 create placeholders + ~99 write-back + log); readRepeat 1 metaR.
- Secondary (per tx, geomean): allocBytes 20179, allocs 255.6, ns 36285,
  cpuNs 79269, mutexWait 659.
- `baseline.json` and `best.json` written (both = 420.87). New experiments must
  beat `best.json`.

## 2. Blind-write create-first (skip absence probe) - KEPT
- Hypothesis: `batchWrite100` does ~100 backend metadata reads/tx. Each blind
  write to a fresh key first attempts a write-lock, whose `fetchLockInfo`
  GetMetadata only discovers the key is absent before the code falls back to
  create. For a key that is written-not-read with no cached evidence it exists,
  going straight to the create path (collection lock + WriteIfNotExists) skips
  that wasted read. (The `algo.go` "Blind write optimization" TODO.)
- Change: `internal/trans/algo.go` `lockValidateFoundKey` - for a pure blind
  write (`Write && !Read && !NotFound`) whose key the local cache does not show
  present, mark it NotFound and route to the existing create path
  (needs-collection-lock, then `lockValidateNotFoundKey` -> `lockCreate`). If
  the key actually exists, `lockCreate`'s WriteIfNotExists fails precondition
  and falls back to a write-lock, so the guess is self-correcting and never
  unsafe. New helper `localIndicatesPresent`.
- Correctness: fast gate pass, full gate pass (build + make test + 120s
  FuzzConcurrentTx), judge approved (general fast path, reuses create logic, no
  test weakening).
- Primary: 420.87 -> 404.11 (-3.98%).
- Secondary: allocBytes 20179 -> 19940, allocs 255.6 -> 250.8 (-1.9%), ns ~flat
  (noisy), cpu ~flat (noisy), mutexWait 659 -> 448. No regressions.
- Outcome & why: KEPT. `batchWrite100` metaReads dropped 100->0/tx (cost
  17091->13991, -18%); every other workload unchanged (none do pure blind
  writes). Confirms the absence probe was pure overhead for inserts. The
  collection lock is taken for blind writes regardless (create needs it), so the
  only added cost on the rare "blind write to an uncached existing key" path is
  one failed WriteIfNotExists, which is acceptable.
- Commit: b2c586e

## 3. Resolve cache-hit reads inline in ReadMulti - KEPT (secondary)
- Hypothesis: `ReadMulti` spawns a goroutine per key (via `conc.WaitGroup`)
  even when the value is already in the local cache - the steady state for
  `multiRMW10` and `batchRead10`. A cache hit issues no backend op, so the
  goroutine is pure scheduling overhead. Resolving cached reads inline should
  cut allocs/CPU/ns/mutex on those two workloads with the primary unchanged
  (cache hits do no backend ops either way).
- Change: `internal/trans/reader.go` adds `Reader.ReadCached` (local-only fast
  path: returns a present, non-empty, non-outdated cached value; ok=false for
  miss/outdated/deleted/empty so the caller falls back to `Read`). `tx.go`
  `ReadMulti` calls it inline before spawning, spawning goroutines only for
  reads that need the backend.
- Correctness: fast gate pass, full gate pass, judge approved (general cache
  fast path, matches Read's non-empty cache behavior, no test changes).
- Primary: 404.11 -> 404.31 (+0.05%, noise; op counts identical).
- Secondary (fair back-to-back control vs new, medians): ns ~55k -> ~42k
  (-24%), cpu ~128k -> ~93k (-27%), allocs 251.1 -> 246.9 (-1.7%),
  allocBytes ~flat-down, mutexWait ~1119 -> ~946 (-15%). No regressions (the
  448 in the prior best.json was a lucky low reading; the back-to-back control
  put exp2 at ~1119).
- Outcome & why: KEPT under the secondary-axis rule. The per-key goroutine on
  the cache-hit read path was pure overhead. Lesson: like the earlier
  single-element fanout inline, hot-path fan-out should short-circuit work that
  is already local.
- Commit: 5a7cfbc

## 4. Single-locker fast path in applyLockTags - DISCARDED (no effect)
- Hypothesis: `applyLockTags` runs per lock/unlock op (core, all backends) and
  builds an intermediate `[]string` + `strings.Join` for the `locked-by` tag.
  Special-casing the common single-locker case should cut per-lock allocations
  on lock-heavy workloads (multiRMW10, batchWrite100).
- Change: `internal/storage/locker.go` - replace the slice+Join with a
  `joinLockers` helper that returns `tidToTag(lockers[0])` directly for one
  locker (and "" for none).
- Correctness: not gated (discarded on measurement; change is a pure encoding
  refactor with identical output).
- Primary: 404.31 -> 404.31 (op counts identical).
- Secondary: allocs/op unchanged (10RMW 125->125, 100W 6899->6899); B/op flat.
- Outcome & why: DISCARDED. No measurable effect. `strings.Join` already skips
  allocation for a single-element slice (returns s[0]), and the dominant
  allocation in `applyLockTags` is the 2-entry `newTags` map, which this change
  does not touch. Lesson: the lock-path allocation cost is the tag map + the
  memory backend's defensive `copyTags`, not the locker-id encoding; a real
  win would need to avoid the per-op map, which the backend API requires.

## 5. Skip dedup map in initValidation for read-only / write-only tx - KEPT (secondary)
- Hypothesis: `initValidation` always allocates an intermediate
  `map[string]pathState` to merge reads and writes that touch the same path.
  But `collectAccesses` already yields each path at most once within reads and
  within writes, so the merge is only needed when a tx has BOTH reads and
  writes (the only overlap source). Read-only (batchRead10, readRepeat) and
  write-only (batchWrite100) transactions can build the `pathState` slice
  directly and skip the map entirely. Pre-sizing the map in the remaining
  read+write case (singleRMW, multiRMW10) also avoids growth reallocs.
- Change: `internal/trans/algo.go` `initValidation` - when `len(reads)==0 ||
  len(writes)==0`, append directly into a pre-sized slice (no map). Otherwise
  use the map path but pre-size it to `len(reads)+len(writes)`.
- Correctness: fast gate pass, full gate pass (build + make test + 120s
  FuzzConcurrentTx), judge approved (honest fast path, preserves merge path
  when both reads and writes present, no test changes).
- Primary: 404.31 -> 404.06 (-0.06%, noise; op counts identical).
- Secondary (fair back-to-back control vs new): allocBytes ~19660-20450 ->
  ~19170-19220 (-4%, consistent), allocs 246-251 -> 245 (-1.5..2.5%), ns/cpu
  flat-or-better, mutexWait flat. Internal benches confirm: 10RMW B/op
  10897->9261 (-15%, from map pre-size), 100W B/op 558783->543413 (-2.75%, map
  skipped). No regressions.
- Outcome & why: KEPT under the secondary-axis rule. allocBytes is the clearest
  and most consistent signal; the map was avoidable scheduling/heap overhead on
  every tx. Lesson: dedup structures are only worth their allocation when a
  collision can actually occur; bound them by the access pattern.
- Commit: c6067f8

## 6. Presize commit slices in toLog and collectAccesses - KEPT (secondary)
- Hypothesis: alloc profiling of batchWrite100 (the biggest allocator) shows
  `toLog` (41MB flat) and `Tx.collectAccesses` (33MB flat) among the top
  non-backend allocators. Both build slices via `append` from a source of known
  length (`writes` and the `staged`/`reads` maps), so a multi-key commit regrows
  the backing array log2(n) times. Presizing to the exact upper bound replaces
  that with a single allocation.
- Change: `internal/trans/algo.go` `toLog` sets
  `Writes: make([]storage.TxWrite, 0, len(writes))`; `tx.go` `collectAccesses`
  presizes `reads`/`writes` to `len(t.reads)`/`len(t.staged)` (exact upper bound
  - each staged/read entry yields at most one access).
- Correctness: fast gate pass, full gate pass (build + make test + 120s
  FuzzConcurrentTx), judge approved (honest preallocation, no test edits). Both
  are single-threaded per-tx paths, so no concurrency risk; empty non-nil slices
  behave identically to nil downstream (len-based logic, incl. exp5).
- Primary: 404.06 -> 404.00 (noise; op counts identical).
- Secondary: deterministic internal benches: 10RMW B/op 9277->8121 (-12.5%),
  allocs 122->118; batchWrite100 B/op 543419->519418 (-4.4%), allocs 6876->6862.
  Autoresearch allocBytes ~3-4% lower across back-to-back pairs; ns/cpu
  flat-or-better; no regressions.
- Outcome & why: KEPT under the secondary-axis rule. Cheap, obviously-correct
  capacity hints on the commit hot path. Lesson: when a slice is built from a
  collection of known size, presize it - the regrowth copies dominate bytes even
  when they barely move the allocation count.
- Commit: f534ed8

## 7. Co-allocate value and metadata in WriteWithMeta - KEPT (secondary)
- Hypothesis: `storage.Local.WriteWithMeta` is the 2nd-largest non-backend
  allocator in the profile (81MB flat) and runs on every backend write and
  read-through miss. It allocates the `cacheValue` and `cacheMeta` as two
  separate heap objects (`&cacheValue{}`, `&cacheMeta{}`). Since both are always
  written together here, backing them with a single heap object halves the
  allocation count on this path.
- Change: `internal/storage/local.go` adds an unexported `cacheValueMeta{v,m}`
  struct; `WriteWithMeta` allocates one `&cacheValueMeta` and points the entry's
  `V`/`M` at its fields. The cache's copy-on-write updates (`MarkValueOutated`,
  `SetMeta`) replace one pointer with a fresh allocation while the other keeps
  the shared backing struct alive, so no aliasing hazard is introduced.
- Correctness: fast gate pass, full gate pass (build + make test + 120s
  FuzzConcurrentTx), judge approved (hot-path co-allocation, no test edits).
- Primary: 404.00 -> 403.71 (noise; op counts identical).
- Secondary: deterministic internal bench batchWrite100 allocs 6862 -> 6661
  (-201/tx, -2.9%), singleRMW 39 -> 38; B/op flat everywhere. Autoresearch
  allocsPerTx ~249 -> ~242-246 (consistent across back-to-back pairs),
  allocBytes flat-or-lower, ns/cpu flat-or-better. No regressions.
- Outcome & why: KEPT under the secondary-axis rule. This is the first change to
  move the allocation *count* (vs bytes), because it removes a whole object per
  cache   write rather than just right-sizing one. Lesson: when two values share a
  lifetime and are always set together, co-allocate them.
- Commit: feade9f

## 8. Presize res.Writes/res.Locks in TLogger.Get - DISCARDED (below aggregate noise)
- Hypothesis: `TLogger.Get` (61MB flat in the profile) decodes a committed log
  and builds `res.Writes`/`res.Locks` via `append` without presizing; a
  multi-key transaction reads its whole log back on the writeback path, so a
  100-write log regrows the slice ~7 times. Presizing (count entries in a cheap
  first pass over the decoded collections) should cut allocBytes on batchWrite.
- Change: `internal/storage/tlogger.go` `Get` - sum `len(cw.GetWrites())` and
  lock counts first, then `make([]TxWrite,0,n)` / `make([]PathLock,0,m)`.
- Correctness: not fully gated (discarded on measurement); change is a pure
  capacity hint with identical output.
- Primary: 404.00 -> ~404 (op counts identical).
- Secondary: deterministic internal bench confirms the win - batchWrite100 B/op
  519411 -> 500756 (-3.6%), allocs 6661 -> 6647; all other workloads and op
  counts flat. BUT the harness's reported secondary is a geomean across 5
  workloads, and only batchWrite benefits, so the aggregate allocBytesPerTx moves
  only ~0.7% - below the run-to-run noise band (~+/-3-4%). Back-to-back
  count-3 pairs overlapped (exp8 18495/19204 vs ctrl 18623/18624).
- Outcome & why: DISCARDED. The change is real and strictly safe (less memory,
  identical behavior), but the acceptance rule requires a secondary axis to
  *clearly* improve in the reported metric, and a single-workload byte win is
  diluted below noise by the geomean. Contrast exp6, which also improved 10RMW
  and so showed a detectable aggregate move. Lesson: to clear the secondary bar,
  a change must help a workload whose metric carries enough geomean weight, or
  help several workloads at once - an isolated batchWrite-only byte win will not
  register.

## 9. Skip cache write in read-only commit validation - KEPT (secondary)
- Hypothesis: on the read-only commit path, `validateRead`/`validateReadNotFound`
  fetch the freshest metadata via `Global.GetMetadata`, which also calls
  `Local.SetMeta` to populate the cache. But that cached metadata is never
  reused - the next validation always re-reads fresh from the backend - so the
  SetMeta write is pure overhead: it allocates a cacheMeta + copies the cache
  entry, and (worse) takes the cache shard mutex from each of the parallel
  per-key validation goroutines. Skipping it should cut allocs and, because it
  removes shard-lock contention, ns/cpu on batchRead10 and readRepeat.
- Change: `internal/storage/global.go` adds `GetMetadataUncached` (backend
  GetMetadata with no SetMeta); `internal/trans/algo.go` `validateRead` and
  `validateReadNotFound` use it. No backend op changes (GetMetadata already
  always hits the backend here), so the primary is unaffected; the cached value
  stays consistent (its writer is unchanged, so reads still hit cache).
- Correctness: fast gate pass, full gate pass (build + make test + 120s
  FuzzConcurrentTx), judge approved (still reads backend metadata, just skips
  cache population; no test edits). Safe under concurrency: the cache is only a
  perf hint and staleness is always caught by fresh validation reads and
  conditional lock writes.
- Primary: 404.06 -> 403.79 (noise; op counts identical).
- Secondary (back-to-back x3): allocsPerTx ~241-246 -> ~221-225 (-8.5%,
  consistent), nsPerTx ~32-36k -> ~24-31k (-20%+), cpuNsPerTx ~69-74k -> ~49-66k
  (-20%+), allocBytes flat-or-lower, mutexWait flat. No regressions.
- Outcome & why: KEPT - the strongest secondary win of the session. The lesson:
  a "free" cache write on a hot parallel path is not free - the allocation is
  minor next to the shard-mutex contention it creates across fan-out goroutines.
  Validation should read, not write.

- Commit: 50e480c

## 10. Skip per-tx logger clone when logging is off - KEPT (secondary)
- Hypothesis: alloc profiling shows `slog.Logger.clone` and `argsToAttrSlice`
  among the per-tx allocators. `Algo.Begin` (and `Rebegin`) do
  `t.log.With("tx", txLog(tid))` for every transaction, cloning the logger and
  formatting the id - even when the logger discards everything (the bench uses a
  nil handler; production typically runs at Info+). All tx logs in this package
  are Debug or Error level, so "a log will actually fire" is exactly
  `Enabled(LevelError)`; gate the clone on that.
- Change: `internal/trans/algo.go` adds `handleLog(ctx, tid)` returning the base
  logger when `!t.log.Enabled(ctx, slog.LevelError)`, else the `With`-tagged
  clone; `Begin`/`Rebegin` use it (Rebegin now uses its ctx).
- Correctness: fast gate pass, full gate pass (build + make test + 120s
  FuzzConcurrentTx), judge approved (legitimate gating, no test edits). Behavior
  is identical whenever any log fires (Enabled(LevelError) true => clone as
  before); the only change is skipping work when nothing would be logged.
- Primary: 403.79 -> 404.04 (noise; op counts identical).
- Secondary (back-to-back x3): allocsPerTx ~220-225 -> ~209-211 (-5%,
  consistent), allocBytes ~17.9-18.6k -> ~17.6k (-2..5%), ns/cpu flat-or-lower.
  No regressions.
- Outcome & why: KEPT. Hits every workload, so even a few allocs/tx compound in
  the geomean (the low-alloc workloads readRepeat/singleRMW gain the most
  percentage). Lesson: eager per-tx structured-logging setup is a real cost on
  short transactions; make it pay-for-what-you-log.
- Commit: 05d7884

## 11. Skip errgroup SetLimit when fan-out fits the limit - DISCARDED (below noise)
- Hypothesis: `concurr.Fanout.Spawn` always calls `errgroup.SetLimit(o.limit)`,
  which allocates a semaphore channel. The limit (algoConcurrency=10) only binds
  when there are more tasks than the limit; the common fan-outs (validate/lock
  <=10 keys, e.g. multiRMW10, batchRead10) have num<=limit, so the channel is
  allocated but never used. Skip SetLimit when `num <= o.limit`.
- Change: `internal/concurr/fanout.go` `Spawn` - guard `g.SetLimit` with
  `if num > o.limit`.
- Correctness: not fully gated (discarded on measurement); behavior is identical
  for num<=limit (the limit never binds, all tasks run immediately either way).
- Primary: flat (op counts identical).
- Secondary (back-to-back x3): allocsPerTx consistently ~1/tx lower (~209.3-210.0
  vs 210.6-215.1) and allocBytes ~0.4% lower, but the magnitude is tiny and
  ns/cpu ran flat-to-slightly-higher than the control's best runs (within noise).
- Outcome & why: DISCARDED. The saved semaphore channel (cap-10 of struct{}) is
  a single small allocation per fan-out, diluted to ~1 alloc/tx in the geomean -
  below the secondary noise band, and with no ns/cpu benefit to corroborate it.
  A real but immeasurable micro-win does not clear the "clearly improves" bar.
  Lesson: errgroup's per-call setup (cancel ctx + optional semaphore) is small
  relative to the fan-out's actual work; shaving one channel is not enough to
  register.

## 12. Intrusive LRU cache (drop container/list) - KEPT
- Hypothesis: the cache shard backs its LRU with `container/list`, which (a)
  allocates a `list.Element` per insert and (b) boxes the `entry{key,value}`
  struct into the element's `any` field on every `update` - including updates
  to keys that already exist. Existing-key updates happen on every transaction
  (lock SetMeta + value write-back per key), so this boxing is a per-tx,
  per-key allocation that buys nothing. An intrusive doubly-linked list whose
  nodes hold key/value/prev/next directly removes both: inserts allocate one
  node, and in-place updates allocate nothing.
- Change: rewrote `internal/cache/cache.go`'s `cacheShard` to use a sentinel
  circular doubly-linked list of `*node` (key, value, prev, next) plus the
  existing `map[string]*node`. Public API (`Cache`, `New`, `Get/Set/Update/
  Delete/SizeB`), `newShard`, shard method signatures, and eviction semantics
  (never evict the MRU entry; bounded overshoot) are all unchanged. No test
  edits: the existing `cache_test.go` shard-level tests cover get/set/update/
  delete/eviction and all pass under -race.
- Correctness: fast gate PASS (incl. FuzzConcurrentTx), judge APPROVED, full
  gate PASS (make test + 2m fuzz).
- Primary: 404.36 -> 403.7 (flat, within noise; op counts unchanged).
- Secondary (same-machine, back-to-back, count=5): allocs/tx geomean
  210.6 -> 203.5 (-3.4%); allocBytes/tx 17610 -> 17368 (-1.4%). Per-workload
  allocs/tx: multiRMW 721.6 -> 689.4 (-32), batchWrite 6760.4 -> 6461.2 (-299),
  singleRMW 36.2 -> 34.1 (-2.1), readRepeat 20.9 -> 20.5; batchRead flat (reads
  are gets, no update). ns/cpu/mutexWait within noise (mixed small deltas).
- Outcome & why: KEPT. Primary flat with a clear, deterministic allocation
  reduction on every write-touching workload and no regression on the others -
  this is the acceptance case "primary within noise AND a secondary axis
  clearly improves without regressing the rest". The win is broad because the
  eliminated interface-boxing happens on the common existing-key update path,
  not just inserts. Lesson: `container/list` is convenient but its `any`-typed
  element value forces a heap box for any multi-word entry on every mutation;
  an intrusive list is the idiomatic fix for hot LRU paths.
- Commit: 910b401

## 13. Encode storage-path base64 into the buffer - KEPT
- Hypothesis: `paths.encode` (under FromKey/FromCollection/FromTransaction)
  built every storage path by calling `encoding.EncodeToString(a)` - which
  allocates a fresh base64 string - and then immediately copied that string
  into the pooled `bytes.Buffer` and threw it away. An alloc_objects profile of
  singleRMW showed `base64.EncodeToString` + `bytes.Buffer.String` together at
  ~43% of all allocations. This is the single hottest universal path: every
  key/collection/tx path is built here, so every read, write, and commit pays
  it. Encoding directly into the buffer's spare capacity should remove one
  allocation per encoded path across all workloads.
- Change: in `internal/data/paths/paths.go`, replace
  `buf.WriteString(encoding.EncodeToString(a))` with
  `dst := buf.AvailableBuffer(); dst = encoding.AppendEncode(dst, a); buf.Write(dst)`.
  Output is byte-for-byte identical (same encoding, same layout); only the
  intermediate allocation is removed.
- Correctness: fast gate PASS (incl. FuzzConcurrentTx), judge APPROVED, full
  gate PASS (make test + 2m fuzz). Encode/decode round-trip is exercised
  end-to-end by the storage/trans tests and the fuzzer.
- Primary: ~404 -> ~404 (flat, within noise; paths are unchanged).
- Secondary: measured on the stable internal benches (3000x, deterministic
  allocs/op): singleRMW 33 -> 31 allocs/op (-2, -6%), batchWrite100
  6356 -> 6253 allocs/op (-103, -1.6%); bytes/op also lower on both
  (2670 -> 2657, 509711 -> 508573). Harness allocs geomean ~203 -> ~198.
- Outcome & why: KEPT. Primary flat with a clear, deterministic allocation-
  count reduction on every encoded path, helping even the smallest workloads
  (which dominate the geomean). NOTE on methodology: the harness's MemStats
  allocBytes geomean is noisy (+/-4% run-to-run for identical code) and on one
  count=5 run appeared to rise; the deterministic testing.B per-op numbers
  (bytes down on both representative workloads) are the reliable signal and
  show no regression. Lesson: when a secondary axis looks like it regressed,
  cross-check against the deterministic per-op benchmark before trusting the
  harness's MemStats deltas. Also: EncodeToString-then-copy is a common
  hidden allocation; AppendEncode into spare capacity is the idiomatic fix.
- Commit: fff599b

## Primary-score wins considered but deferred (risk-bounded)
- batchWrite100 dominates the cost (~14k/tx) via ~2 objWrites per created key:
  an empty create-lock placeholder (WriteIfNotExists) plus a value write-back.
  Writing the value directly into the create placeholder would replace the
  write-back objWrite (70) with a cheap untag metaWrite (31), ~-28% on the
  workload (~-6% primary). It is NOT safe under the current read protocol:
  `Reader.handleLockCreate` treats any non-empty value as committed and only
  probes the create lock for empty values, so a non-empty placeholder would let
  a concurrent reader return uncommitted data. Making it safe requires surfacing
  tags from `Global.Read` and changing the read path to detect create-locks by
  tag for every read - a core-protocol change whose serializability cannot be
  validated within one experiment slot. Deferred as too risky for the budget.
- Read-set validation (batchRead10: 10 metaReads/tx; readRepeat: 1/tx) and the
  RMW write protocol (1 lock metaWrite + 1 write-back objWrite per key) are at
  the OCC protocol's theoretical minimum; List-based batch validation was
  rejected earlier on object-store list-consistency grounds. No safe primary win
  found here beyond exp2's metaRead elimination.
