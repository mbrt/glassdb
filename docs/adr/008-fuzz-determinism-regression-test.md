# ADR-008: Regression test for fuzz outcome determinism

## Status

Accepted

## Context

ADR-007 removed three gratuitous sources of nondeterminism from the
serializability fuzzer (`FuzzConcurrentTx`) so that a failing schedule could be
minimized and turned into a deterministic regression test: random
transaction-ID prefixes, map-iteration order in commit-path slices, and backoff
jitter drawn from the process-global `math/rand`.

We wanted a standing test that guards those settings against regression. The
obvious approach — replay a schedule twice and assert the two runs issue an
identical sequence of backend operations — does **not** work, and understanding
why shaped this decision.

The algorithm is not, and is not meant to be, deterministic at the level of
backend-operation ordering:

- Multi-key work is dispatched through concurrent fan-out
  (`internal/concurr.Fanout`, backed by `errgroup`), and the fuzz workload runs
  two clients in parallel. The interleaving of those goroutines is the behaviour
  under test.
- `testing/synctest` virtualizes the clock but schedules **real** goroutines;
  the Go runtime does not order concurrently-runnable goroutines
  deterministically. Replaying the same schedule diverges in operation order
  even at `GOMAXPROCS=1`.
- The `ScheduledBackend` only *influences* interleaving by delaying each
  operation; when two operations draw equal delays, or when goroutines
  synchronize on something other than a backend call, the tie is broken
  nondeterministically. Because the fake clock advances based on which
  goroutines are blocked, the priority timestamp baked into each transaction ID
  (`internal/data.TxID`) — and therefore tx-log object paths and object
  versions — also differs run to run.

Byte-for-byte execution determinism would require a different architecture
entirely: a deterministic-simulation harness with a single cooperative
scheduler that intercepts every concurrency/sync point and picks the next
goroutine from the fuzz input (as FoundationDB and TigerBeetle do). That is a
large, invasive change and out of scope here.

What the deterministic settings *do* guarantee, together with the
serializability guarantee, is that a given schedule always commits the **same
result**. That is the property the fuzzer's invariant check actually depends
on, and it is exactly what the ADR-007 lost update broke: under contention a
committed increment could be silently dropped, so the same schedule could commit
different totals depending on the interleaving.

## Decision

Add `TestConcurrentTxDeterministicOutcome` (in `fuzz_test.go`) as the
determinism regression test, guarding the committed **outcome** rather than the
execution trace.

- The two-client workload body was extracted from `fuzzConcurrentTx` into
  `fuzzConcurrentTxWorkload`, which now returns a `fuzzOutcome` (the committed
  value of every key plus the increment totals the clients tracked). The fuzzer
  and the regression test share this exact workload.
- The test replays a fixed set of schedules many times, each in its own
  `synctest` bubble, and asserts every replay commits an identical `fuzzOutcome`.
  Because each replay interleaves the two clients differently, an outcome that
  depends on the interleaving makes the replays disagree and fails the test.
- The schedules are chosen to span a range of interleavings, including heavy
  contention, which is where the aborts, retries, and background
  status/refresh workers that exercise the relevant code paths occur.

This is a probabilistic guard, like the fuzzer it protects: it cannot prove
determinism, but replaying several contended schedules many times gives a high
chance of surfacing a nondeterministic-outcome regression.

## Consequences

- A regression that reintroduces interleaving-dependent committed state (the
  ADR-007 lost-update class) makes replays of the same schedule disagree and
  fails the test, in addition to `fuzzConcurrentTxWorkload`'s per-run check that
  the committed values equal the tracked increments.
- The test is fast and deterministic in CI: the committed result is fixed by the
  workload, so a correct implementation always passes.
- The test does **not** guard execution-level reproducibility (operation order,
  tx-ID timestamps, object versions); those are intentionally nondeterministic
  and asserting on them would be flaky. It also does not, by itself, catch a
  change that merely unseeds the ID source or re-enables jitter, since those
  affect reproducibility rather than the committed outcome; ADR-007 remains the
  reference for why those knobs exist.
- `fuzzConcurrentTxWorkload` returning `fuzzOutcome` gives future tests a single
  reusable definition of the workload's observable result.
