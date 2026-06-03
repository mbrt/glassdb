# ADR-006: Autoresearch performance loop

## Status

Accepted. The change-size and test-editing policy described under "Test-integrity
judge" was later loosened by [ADR-009](009-autoresearch-bigger-changes.md): the
loop may now make large, multi-file changes and freely edit the unit tests in
subpackages, while only the autoresearch infrastructure and the repo-root
verification tests stay frozen.

## Context

Glass DB's performance "could definitely improve" (per the README), and the cost
model is unusual: latency is dominated by object-storage round-trips, so the
lever that matters most is the **number of backend operations per transaction**,
not raw CPU. Optimizing this is a large search space of small, independent
changes (caching, batching, fewer metadata round-trips, lock-acquisition cost),
each of which must preserve **strict serializability** - the one property we
cannot trade away.

This is a good fit for an autonomous, iterative agent loop in the spirit of
[karpathy/autoresearch](https://github.com/karpathy/autoresearch): let a coding
agent propose one change at a time, measure it against a fixed metric, keep it
only if it helps, and repeat. The hard requirements are (1) a metric that is
cheap, reproducible, and aligned with real cost; (2) an unfakeable correctness
gate; and (3) a guard so the agent cannot make progress by quietly weakening
tests.

## Decision

Add an autoresearch setup under `hack/autoresearch/`, driven by the Cursor CLI.

### Single self-looping agent, human-edited `program.md`

Mirroring autoresearch, a single long-lived `cursor-agent` session runs the
loop, guided entirely by `hack/autoresearch/program.md`. The human iterates on
`program.md` (the "research org code"); the agent iterates on the glassdb
implementation (the analog of `train.py`). The fixed infrastructure (the metric
and the gates, analogous to `prepare.py`) is off-limits to the agent.

### Primary metric: deterministic backend-op cost

`hack/autoresearch/bench` runs a fixed suite of **single-client** workloads
(single/multi-key RMW, batch read, batch write, repeated read) against the
in-memory backend and computes a weighted cost from the `Stats` counters
(`ObjReads`, `ObjWrites`, `MetaReads`, `MetaWrites`, `ObjLists`) normalized per
transaction. Weights are the mean object-storage latencies from the README
(object read ~57ms, write ~70ms, metadata ~31ms). The score is the geomean of
per-workload per-tx costs - lower is better.

Single-client op counts are timing-independent, so the primary score is
reproducible across machines and runs (observed run-to-run spread <1%). This is
the analog of autoresearch's `val_bpb`: one number, lower is better, comparable
across changes. Using a fixed iteration count (not a wall-clock budget) keeps it
deterministic.

### Secondary axes: memory, CPU/runtime, lock contention

The same harness also reports `allocBytesPerTx`, `allocsPerTx`, `nsPerTx`,
`cpuNsPerTx`, and `mutexWaitNsPerTx` (via `runtime.MemStats`,
`syscall.Getrusage`, and the `runtime/metrics` mutex-wait metric). These are
noisier, so they are recorded as **tie-breakers**: an experiment that leaves the
primary within noise but clearly improves a secondary axis (without regressing
others) may still be kept. The primary may never regress.

### Correctness gate (strict serializability)

`hack/autoresearch/check.sh` is the mandatory gate. The fast tier (every
experiment) builds, runs `go test -race ./...`, and runs the serializability
fuzzers (`FuzzConcurrentTx`, `FuzzAlgoConcurrentTx`) for a bounded time. The full
tier (`--full`, before a change is kept/committed) runs `make test` plus longer
fuzzing. Any failure discards the experiment.

### Test-integrity judge (separate sub-agent)

The implementation agent IS allowed to edit tests for mechanical, build-fixing
reasons (signature/import/type updates), but must not disable, delete, or weaken
them. This is enforced by a **separate, read-only judge sub-agent**
(`hack/autoresearch/evaluate.sh` + `evaluator.md`), invoked as
`cursor-agent -p --mode ask --model auto`. It reviews the experiment diff and
returns a strict JSON verdict `{"approved": bool, "reason": "..."}`; rejection
discards the experiment. Running the judge as a distinct agent (and on the
cheaper `auto` model, since judging a diff is lighter than implementing) stops
the implementation agent from grading its own homework. The judge fails closed:
an unparseable verdict is treated as a rejection.

### Record of experiments

Autoresearch experiments do **not** produce ADRs (this ADR documents the loop
itself, once). Instead, each experiment appends a reasoning entry to
`hack/autoresearch/log.md` (hypothesis, change, gate result, judge verdict,
primary/secondary deltas, outcome and why). Kept experiments are committed on a
dedicated branch with the `Includes-AI-Code: true` trailer; the durable record
is the commits plus `log.md`. Transient score files (`baseline.json`,
`best.json`) are gitignored.

## Consequences

- There is now a cheap, reproducible, cost-aligned score the loop optimizes, and
  an unfakeable correctness gate, so performance work can run unattended.
- The primary score is a proxy: it measures single-client backend-op cost on the
  in-memory backend. It does not capture contention-driven retries or real
  network latency tails; those remain the domain of `hack/rtbench` and the
  secondary axes. The weights are a fixed, human-tunable approximation.
- The harness and gates are load-bearing and off-limits to the agent; the
  `Stats` counters in `stats.go` are now part of the metric contract.
- Adding workloads or changing weights changes the score's meaning, so baselines
  are only comparable within the same harness version.
- The loop depends on the Cursor CLI being installed and authenticated, and the
  judge step spends model budget per experiment (mitigated by the `auto` model).
