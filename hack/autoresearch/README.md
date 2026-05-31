# Autoresearch loop for glassdb

An autonomous performance-research loop in the spirit of
[karpathy/autoresearch](https://github.com/karpathy/autoresearch), driven by the
[Cursor CLI](https://cursor.com/docs/cli). A single long-lived `cursor-agent`
session self-loops, guided entirely by [`program.md`](program.md): it forms a
hypothesis, edits the glassdb implementation, proves correctness, measures a
score, then keeps or discards the change - and repeats.

You program the loop by editing `program.md`, not by touching the database code.

## Pieces

| File | Role | Editable by the agent? |
|------|------|------------------------|
| [`program.md`](program.md) | The instructions the agent follows (the "brain") | No - the human edits this |
| [`bench/`](bench) | Scoring harness; defines the primary + secondary metrics | No (off-limits) |
| [`check.sh`](check.sh) | Correctness gate (race tests + serializability fuzzers) | No (off-limits) |
| [`evaluate.sh`](evaluate.sh) + [`evaluator.md`](evaluator.md) | Read-only judge sub-agent enforcing test integrity | No (off-limits) |
| `log.md` | The running experiment log (the "morning log") | Yes - appended each experiment |
| `baseline.json`, `best.json` | Baseline and best-kept scores (gitignored) | Yes - bookkeeping |

The implementation files (`db.go`, `tx.go`, `internal/**`, `backend/**`, ...)
are what the agent actually optimizes.

## The metric

The primary score is a weighted count of backend operations per transaction
(object/metadata reads and writes), aggregated as a geomean over a fixed suite
of single-client workloads. It is deterministic and reflects the real cost
driver - object-storage round-trips. Secondary axes (memory, CPU/runtime, lock
contention) are tie-breakers. Lower is better.

```bash
go run ./hack/autoresearch/bench -json -count 3   # machine-readable median
go run ./hack/autoresearch/bench                  # human-readable table
```

## Correctness

Every experiment must pass the gate before it can be kept; this is what protects
strict serializability.

```bash
hack/autoresearch/check.sh          # fast: build + race tests + bounded fuzz
hack/autoresearch/check.sh --full   # full: make test + longer fuzz (before keeping)
```

Tunable via `FUZZTIME` / `FULL_FUZZTIME`.

## Running it

Prerequisites: `cursor-agent` authenticated (`cursor-agent login`), Go, `jq`.
Work on a dedicated branch (e.g. `autoresearch`) so kept experiments accumulate
as commits.

### Interactive (recommended to start)

```bash
cd glassdb
cursor-agent --force
```

Then prompt:

```
Have a look at hack/autoresearch/program.md and let's kick off a new experiment.
Do the setup first, then start the loop.
```

### Headless / unattended

```bash
cd glassdb
cursor-agent -p --force \
  "Follow hack/autoresearch/program.md exactly. Do the setup, then run experiments
   in a loop until you have completed 25 experiments or 3 hours have passed.
   Keep only improvements; log every experiment."
```

Resume a previous session to keep going:

```bash
cursor-agent --resume        # pick a session, or
cursor-agent -p --continue "Continue the autoresearch loop."
```

You wake up to a series of commits (the kept improvements) plus `log.md`, which
explains what was tried and why.
