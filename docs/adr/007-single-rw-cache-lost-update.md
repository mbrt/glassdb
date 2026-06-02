# ADR-007: Fix lost update from caching unresolved single-RW writes

## Status

Accepted

## Context

`FuzzConcurrentTx` (the serializability fuzzer) found a strict-serializability
violation: two clients incrementing the same keys could lose an update —
e.g. key `k1` ended at `4` when every committed increment summed to `7`. One
transaction's committed write was silently overwritten by another transaction
that had read a stale value yet passed commit-time validation.

### Root cause

The bug is an interaction between two existing mechanisms:

1. **The single read-modify-write fast path** (`commitSingleRW`). A transaction
   that only reads and writes a single key commits with one conditional backend
   write and writes **no transaction-log object** (`_t/<tx-id>`). This is a large
   latency win, but it means the transaction's commit status and committed value
   cannot be recovered from the log afterwards.

2. **Validation refreshing the local cache.** When a transaction reads a value
   and then finds, at validation time, that the key was written by someone else,
   `validateLockedRead` / `validateReadNotFound` ask `Monitor.CommittedValue` for
   the writer's committed value and store it in the local cache, tagged with that
   writer's ID, so the retry can revalidate quickly.

When the relevant writer committed through the single-RW fast path,
`CommittedValue` has no log to consult and returns a non-`OK` status with an
**unresolved / empty value**. The old code only skipped the cache update when the
value was literally `NotWritten`; for an unresolved-but-present status it cached
the *guessed* value bytes paired with the *real* writer ID.

That produces a poisoned cache entry: value bytes from one version, writer tag
from a newer version. A later single-RW commit on that key validates with
`checkReadVersionUnlocked`, which trusts the entry because its writer tag matches
the live `last-writer` — even though the value is stale. The conditional write
then succeeds and clobbers the newer value: a lost update, and a strict
serializability violation.

Wound-wait (ADR-002) did not introduce the bug but made it far more frequent: the
extra aborts and restarts under contention multiply the number of times a
transaction validates against a single-RW writer's key.

### Why it was hard to reproduce

The fuzzer was effectively non-deterministic: replaying a saved failing input
reproduced the failure well under 10% of the time, so it could neither be
minimized nor turned into a regression test. Three independent sources of
nondeterminism, none of them controlled by `testing/synctest`, were responsible:

- **Random transaction-ID prefixes.** `data.NewTId` draws its prefix from
  `crypto/rand`, so IDs (and therefore tx-log object keys, and the order tied to
  them) differed on every run.
- **Go map iteration order.** Several commit-path steps built slices by ranging
  over a map, so the order of backend operations a transaction issued varied run
  to run.
- **Backoff jitter.** `cenkalti/backoff` randomized retry intervals using the
  process-global `math/rand`, which `synctest` does not virtualize, so retry
  timing — and thus the interleaving — drifted.

## Decision

### Fix the lost update

In `validateLockedRead` and `validateReadNotFound`, only refresh the local cache
with a writer's value when `CommittedValue` resolved it **authoritatively**
(`Status == OK` and the value is written). Otherwise — the writer used the
single-RW fast path and its value is unresolvable from the log — do not cache a
guessed value. `validateLockedRead` instead invalidates the stale entry
(`local.MarkValueOutated`) so the retry re-reads the authoritative value straight
from storage; `validateReadNotFound` simply retries without touching the cache.

This removes the only paths that paired a value with a writer that did not
produce it, so `checkReadVersionUnlocked` can no longer be fooled into accepting a
stale read. Correctness comes from re-reading storage on retry, which already
returns the genuine committed value (its `last-writer` tag still names the
single-RW writer), so no extra log lookup or backend round-trip is needed on the
common path.

### Make the fuzzer deterministic

Determinism is a prerequisite for diagnosing and regression-testing concurrency
bugs, so the three nondeterminism sources above were removed:

- **Injectable ID source.** `data.TxIDSource` wraps an `io.Reader` (defaulting to
  `crypto/rand`) and mints IDs via `New` / `Renew`; `Renew` preserves the
  timestamp (and therefore wound-wait priority) so only the prefix entropy
  changes. The source is configured once per DB through `glassdb.Options.Rand`
  and held by `Algo`, the same construction-time plumbing used for retry timing
  — no per-call context lookup. The fuzzer opens each DB with a seeded reader.
  (`trans.CtxWithTxID` remains a separate, minimal context override used only by
  the wound-wait tests to pin a transaction's priority.)
- **Stable ordering.** Commit-path slices built from maps are now sorted by path
  (`initValidation`, `collectionsLocks`, `tlocker.LockedPaths`), so the sequence
  of backend operations no longer depends on map iteration order.
- **Inlined backoff with an injectable jitter source.** The minimal exponential
  backoff `cenkalti/backoff` provided is now inlined in `internal/concurr`
  (`Retrier`, `RetryConfig`, `Permanent`), dropping the dependency and, more
  importantly, the hidden coupling to the process-global `math/rand`. Jitter now
  draws from a `RetryConfig.Rand io.Reader` (nil disables it), which
  `glassdb.Options` wires to the DB's own `Rand` source — the same knob used for
  transaction IDs — defaulting to `crypto/rand`. Jitter stays on in production
  (it spreads retries to avoid thundering-herd contention); the fuzzer opens its
  DBs with `RetryOptions.DisableJitter`.

  Re-enabling jitter does **not** let the fuzzer keep it on, even with a seeded
  `Rand`. The retrier runs inside background goroutines (the remote-status poll
  worker spawned by `Monitor.WaitTx`, and the tx-refresh worker), and
  `testing/synctest` does not serialize the interleaving of concurrently-runnable
  goroutines. Those goroutines would therefore consume jitter bytes from the
  shared seeded reader in a run-dependent order, so wake-up times — and the
  schedule — would drift again. With jitter off, `NextBackOff` consumes no
  randomness and intervals are purely exponential, which is what keeps replay
  deterministic.

The two context keys in `internal/trans` were also changed from bare `struct{}{}`
values (which compare equal, so they collided) to a private `ctxKey` type with
distinct constants.

## Consequences

- The fuzzer no longer reproduces the lost update: a 120k-schedule replay harness
  and a 3-minute (~1.1M-exec) fuzz run both pass, where they previously failed.
- `TestSingleRWLostUpdate` (in `internal/trans/algo_test.go`) is a deterministic
  regression test that recreates the exact poisoning sequence: a logless
  single-RW writer, a victim with a stale read, and a pending lock that forces
  validation down the unresolvable branch. It asserts the victim subsequently
  reads the authoritative value rather than the cached guess; it fails against
  the pre-fix validation code and passes after it.
- The local cache can no longer hold a value whose bytes and writer tag come from
  different versions via the validation paths, closing the lost-update hole.
- On the uncommon path where a single-RW writer's value is unresolvable, a
  transaction does one extra retry (re-reading from storage) instead of trusting a
  cached guess. This trades a small amount of work under contention for
  correctness.
- The transaction-ID prefix is now an injectable seam (`glassdb.Options.Rand`
  backed by `data.TxIDSource`) used by deterministic tests; production leaves it
  unset and gets `crypto/rand`.
