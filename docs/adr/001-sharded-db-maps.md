# ADR-001: Sharded DB-level maps to reduce lock contention

## Status

Accepted

## Context

Several DB-level data structures are shared across the whole database instance
and were each guarded by a single `sync.Mutex`. Every key- or transaction-keyed
operation across all goroutines serialized on that one lock, making it a
contention bottleneck under concurrent load. The affected structures were:

- `internal/cache.Cache` - the LRU object/metadata cache, hit on every
  read/write through `storage.Local`.
- `internal/concurr.Dedup` - the singleflight-style request de-duplicator used
  by the locker.
- `internal/trans.Locker.tlocks` - per-transaction held-lock tracking.
- `internal/trans.Monitor` - the `localTx`, `waiters`, and `unknownTx`
  transaction-state maps.

All four are keyed by a single key (object path) or transaction ID per
operation and are never iterated globally, which makes them amenable to
partitioning.

## Decision

Partition each structure into `N` independent shards selected by a hash of the
key, mirroring [tarndt/shardedsingleflight](https://github.com/tarndt/shardedsingleflight).
`N = nextPow2(GOMAXPROCS)` is computed once at construction, so the shard index
reduces to a bit mask.

A new `internal/shard` package provides the shared primitives:

- `Count()` - shard count (next power of two `>= GOMAXPROCS`).
- `Index(key, n)` - inline FNV-1a hash masked to `[0, n)`.
- A thin generic `Sharded[T]` container (`New`/`For`/`Each`/`Len`) that owns the
  shards and routes keys, leaving each component free to define its own bespoke
  per-shard struct.

Each component keeps its own shard type and per-shard mutex:

- `cacheShard` (LRU list + entries + byte budget),
- `controller` (the existing dedup controller, now one per shard),
- `lockerShard` (the `tlocks` map),
- `monitorShard` (the three transaction maps grouped under one lock so their
  cross-map updates for a given transaction stay atomic).

The cache's byte budget is split evenly across shards (`maxSizeB / N`). To avoid
a freshly written entry larger than the per-shard budget being evicted
immediately - which previously starved the locker that reads back its own
writes and could livelock - the per-shard LRU never evicts the
most-recently-used entry. Overshoot is therefore bounded to one entry per shard,
and the effective maximum cacheable entry size returns to roughly `maxSizeB`.

## Consequences

- Lock contention on the hot DB-level maps drops roughly proportionally to the
  shard count, since unrelated keys/transactions no longer share a lock.
- The routing and shard-count logic lives in one tested place (`internal/shard`)
  and is reused by all four components, avoiding duplicated boilerplate.
- Cache eviction is now per-shard (approximate LRU) rather than global, and the
  global byte budget is approximate (bounded overshoot of one entry per shard).
  This is acceptable for a best-effort cache.
- Public constructors (`cache.New`, `concurr.NewDedup`, `trans.NewLocker`,
  `trans.NewMonitor`) keep their signatures, so callers are unaffected.
- Behavior depends on `GOMAXPROCS`: very small cache sizes are split into very
  small per-shard budgets, so tests that need deterministic LRU semantics drive
  a single `cacheShard` directly.
