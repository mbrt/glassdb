# ADR-009: Loosen autoresearch change-size and test-editing policy

## Status

Accepted. Amends the "Test-integrity judge" decision in
[ADR-006](006-autoresearch-loop.md).

## Context

ADR-006 set up the autoresearch loop with a deliberately conservative policy: an
experiment was "exactly ONE concrete, small, reviewable change", and the agent
could edit `*_test.go` files only for **mechanical, coverage-preserving**
reasons (updating call sites after a signature change, fixing imports, renaming
a symbol). A judge sub-agent (`evaluator.md`) rejected any diff that deleted,
skipped, or weakened any test.

That policy made the loop safe but boxed it in. The conservative search
(experiments 2-3 in `log.md`) quickly exhausted the small, obviously-correct
metadata/fan-out wins and then stalled, because the remaining levers require
restructuring the locking, validation, or commit protocol - changes that are
inherently multi-file and that legitimately invalidate the **internal unit
tests** (e.g. `internal/trans/algo_test.go`, `internal/storage/locker_test.go`),
since those tests assert on internals that the restructuring removes or
reshapes. Under the old rules every such change was either too big to attempt or
blocked by the judge the moment it touched a unit test for a non-mechanical
reason.

The key realisation is that the internal unit tests are not what guarantees
strict serializability. That guarantee is carried by the **repo-root tests** -
`fuzz_test.go` (the `FuzzConcurrentTx` serializability fuzzer and
`TestConcurrentTxDeterministicOutcome`), `glassdb_test.go`, `bench_test.go`, and
`version_test.go` - which exercise the database through its public API and do
not depend on any internal shape. `check.sh` runs those tests, the fuzzer, and
`make test` on every kept change. So freezing the root tests (and the
autoresearch infrastructure) is sufficient to protect correctness; freezing the
internal unit tests on top of that only blocks legitimate refactors.

## Decision

Loosen the policy so the loop can pursue bigger ideas, while tightening the
frozen set to exactly what defines the metric and the correctness contract.

### Bigger changes are allowed

`program.md` no longer requires "exactly ONE small change". An experiment must
still pursue a **single, clearly stated hypothesis**, but the change it implies
may be large and span multiple files (reworking locking, validation, the commit
protocol, recovery). The "keep it small and reviewable" instruction becomes
"keep it focused on the one hypothesis".

### Unit tests are agent-editable; only root tests and infra are frozen

The frozen set is now precisely:

1. The autoresearch infrastructure: `hack/autoresearch/bench/**`, `check.sh`,
   `evaluate.sh`, `evaluator.md`, `program.md`.
2. The repo-root verification/integration tests: every `*_test.go` directly in
   the repository root.

Everything else is fair game, including the unit tests under `internal/**` and
`backend/**`. When a change reshapes internals, the agent may rewrite, add,
remove, or re-assert those unit tests to match.

The judge (`evaluator.md`) is reframed accordingly. It REJECTS on two grounds:
(A) the diff touches a file in the frozen set - a mostly mechanical path check
(no `/` in a `*_test.go` path means a root verification test, off-limits; a path
under a subdirectory means an editable unit test), failing closed for the frozen
set; or (B) the diff shows **clear reward hacking** - disabling a unit test that
should still pass, hardcoding or special-casing the benchmark inputs, faking the
backend operations the metric counts, or loosening an assertion to match a bug.
Otherwise it APPROVES, even sweeping implementation changes or large, honest
rewrites of unit tests. Ground B asks the judge for intent judgement, but only
to catch blatant gaming: a plausibly genuine refactor or real optimisation gets
the benefit of the doubt.

### Failed experiments still get logged

Because bigger changes mean more discards, the discard path is made robust about
the log. The agent appends the `log.md` entry **before** reverting, and the
revert is scoped to preserve the log:

```bash
git restore --staged --worktree -- . ':!hack/autoresearch/log.md'
git clean -fd
```

`git clean` never removes the tracked log, and the pathspec exclude keeps
`git restore` off it, so a discarded or gate-failed experiment always leaves its
reasoning in `log.md`.

## Consequences

- The loop can attempt protocol-level optimisations it previously could not,
  reopening the search space that the conservative policy had exhausted.
- Internal unit tests are no longer a safety net the judge enforces; the safety
  net is the immutable root tests plus the fuzzer plus `make test`. If those are
  ever insufficient to catch a regression that a deleted unit test would have
  caught, the fix is to strengthen a root test (a human action), not to re-freeze
  the unit tests.
- The judge's frozen-set check is mechanical and predictable (a path check), no
  longer hinging on model judgement about whether a routine test edit "weakens
  coverage". It still applies judgement, but only to reject **clear** reward
  hacking (disabling a test that should pass, hardcoding benchmark responses,
  faking the counted backend work); honest refactors and optimisations pass.
- Discarded and failed experiments are now reliably recorded, so the morning log
  is a complete account of what was tried, not just what was kept.
- The frozen set is defined by location ("repo-root `*_test.go`"), so adding a
  new root-level test file extends the contract automatically; conversely, the
  serializability guarantee now rests on those root tests being comprehensive.
