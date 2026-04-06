# Architecture

This document describes the architecture and design choices of GlassDB. For the
full design narrative and motivation, see the companion blog post:
[Transactional Object Storage](https://blog.mbrt.dev/posts/transactional-object-storage).
For usage, performance benchmarks, and examples, see the [README](../README.md).

## Design Goals & Tradeoffs

GlassDB is designed around a specific set of constraints:

- **Stateless clients, no server component.** The entire database is a
  client-side Go library. There is no server to deploy, no coordinator, and no
  direct communication between clients. All coordination happens through object
  storage.
- **Optimistic locking.** Optimized for workloads where conflicts between
  transactions are rare. Readers are rarely blocked.
- **Strict serializability.** The strongest isolation level — transactions
  behave as if executed one at a time, in an order consistent with real time.
- **Throughput over latency.** Object storage is slow (50–150 ms per
  operation), but highly scalable. GlassDB leverages that parallelism.
- **Object storage as the only dependency.** Requires strong consistency and
  conditional writes (available in GCS and S3).

The explicit tradeoffs are:

- When transactions race, it's better to be slow than incorrect.
- High throughput is preferred over low latency.
- Values are expected in the 1 KB – 1 MB range.
- Stale reads are allowed if explicitly requested, but strong consistency is the
  default.

## High-Level Architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Client A   │  │  Client B   │  │  Client C   │
│ ┌─────────┐ │  │ ┌─────────┐ │  │ ┌─────────┐ │
│ │ App     │ │  │ │ App     │ │  │ │ App     │ │
│ │ Code    │ │  │ │ Code    │ │  │ │ Code    │ │
│ ├─────────┤ │  │ ├─────────┤ │  │ ├─────────┤ │
│ │ GlassDB │ │  │ │ GlassDB │ │  │ │ GlassDB │ │
│ │ Library │ │  │ │ Library │ │  │ │ Library │ │
│ └────┬────┘ │  │ └────┬────┘ │  │ └────┬────┘ │
└──────┼──────┘  └──────┼──────┘  └──────┼──────┘
       │                │                │
       └────────────────┼────────────────┘
                        │
                        ▼
              ┌───────────────────┐
              │  Object Storage   │
              │  (e.g. GCS, S3)   │
              └───────────────────┘
```

Each client embeds GlassDB as a library. Clients are completely independent and
ephemeral — they can scale to zero and back without any coordination. The only
shared state is the object storage bucket, which provides strong consistency for
single-object operations and conditional writes for atomic state transitions.

## Package Structure

```
glassdb/
├── db.go, tx.go, collection.go   Root package: public API (DB, Tx, Collection)
├── iter.go                        Iterators over keys and collections
├── stats.go                       Transaction and backend statistics
├── backend/
│   ├── backend.go                 Backend interface definition
│   ├── gcs/                       Google Cloud Storage implementation
│   ├── memory/                    In-memory implementation (testing)
│   └── middleware/                 Logging and delay injection wrappers
└── internal/
    ├── trans/
    │   ├── algo.go                Transaction algorithm (commit, validate)
    │   ├── tlocker.go             Per-key distributed lock manager
    │   ├── monitor.go             Transaction lifecycle tracking
    │   ├── reader.go              Read path with cache integration
    │   └── gc.go                  Transaction log garbage collection
    ├── storage/
    │   ├── global.go              Backend read/write-through cache layer
    │   ├── local.go               In-memory local cache with staleness
    │   ├── locker.go              Lock state management via object tags
    │   ├── tlogger.go             Transaction log persistence (protobuf)
    │   └── version.go             Version tracking (backend + local)
    ├── cache/                     Generic LRU cache
    ├── data/                      Core types: TxID, path encoding
    ├── concurr/                   Background tasks, work deduplication
    ├── proto/                     Protobuf definitions for transaction logs
    ├── errors/                    Error wrapping utilities
    ├── stringset/                 String set type
    ├── testkit/                   Test utilities and fake backends
    └── trace/                     Context-based tracing
```

The public API surface is small: `DB`, `Tx`, and `Collection` in the root
package, plus the `Backend` interface in `backend/`. Everything under `internal/`
is implementation detail.

## Backend Abstraction

The `backend.Backend` interface defines the contract with object storage:

```go
type Backend interface {
    Read(ctx context.Context, path string) (ReadReply, error)
    ReadIfModified(ctx context.Context, path string, version int64) (ReadReply, error)
    GetMetadata(ctx context.Context, path string) (Metadata, error)
    SetTagsIf(ctx context.Context, path string, expected Version, t Tags) (Metadata, error)
    Write(ctx context.Context, path string, value []byte, t Tags) (Metadata, error)
    WriteIf(ctx context.Context, path string, value []byte, expected Version, t Tags) (Metadata, error)
    WriteIfNotExists(ctx context.Context, path string, value []byte, t Tags) (Metadata, error)
    Delete(ctx context.Context, path string) error
    DeleteIf(ctx context.Context, path string, expected Version) error
    List(ctx context.Context, dirPath string) (ListIter, error)
}
```

### Key concepts

**Versions.** Every object has a two-part version:

- `Contents` — incremented on data writes.
- `Meta` — incremented on metadata/tag changes.

These are assigned by the storage backend and used for conditional operations.

**Tags.** Key-value string pairs stored in object metadata. GlassDB uses tags to
store lock state (`lock-type`, `locked-by`, `last-writer`) without modifying the
object's contents. Tags can be updated atomically and conditionally via
`SetTagsIf`.

**Conditional operations.** `WriteIf`, `WriteIfNotExists`, `SetTagsIf`, and
`DeleteIf` all take an expected version and fail with `ErrPrecondition` if the
object has been modified since. This is the fundamental building block for
distributed coordination — it provides compare-and-swap (CAS) semantics.

**Error semantics:**

- `ErrNotFound` — object does not exist.
- `ErrPrecondition` — conditional operation failed (version mismatch).

### Implementations

| Backend | Purpose | Notes |
|---------|---------|-------|
| `gcs/` | Production | Maps to GCS objects; uses object metadata for versions and custom metadata for tags |
| `memory/` | Testing | In-process hash map simulating GCS semantics |
| `middleware/` | Debugging | Wrappers for logging all operations and injecting artificial delays |

## Transaction Algorithm

### Isolation & Consistency

GlassDB targets **strict serializability** — the combination of serializable
isolation and linearizable consistency. This is the strongest guarantee: all
transactions appear to execute one at a time, in an order consistent with real
time. No anomalies of any kind are possible.

This is achieved by combining two properties:

1. **Linearizable consistency**, provided natively by object storage (GCS, S3):
   any read initiated after a successful write returns that write's contents.
2. **Serializable isolation**, enforced by a modified Strict Two-Phase Locking
   (S2PL) protocol: all locks are held until after commit, preventing
   interleaving.

For a deeper discussion of isolation vs. consistency levels — including
comparisons with Postgres, Spanner, CockroachDB, and others — see the
[blog post](https://blog.mbrt.dev/posts/transactional-object-storage).

### Transaction Lifecycle

```
    ┌───────┐
    │ Begin │  Assign transaction ID, create Handle
    └───┬───┘
        │
        ▼
    ┌─────────┐
    │ Execute │  User code runs: reads (tracked), writes (staged locally)
    └───┬─────┘
        │
        ▼
    ┌──────────┐
    │ Validate │  Acquire locks, verify read versions unchanged
    └───┬──────┘
        │
     conflict?
     ╱       ╲
   yes        no
    │          │
    ▼          ▼
 ┌───────┐  ┌────────┐
 │ Retry │  │ Commit │  Write transaction log atomically
 └───────┘  └───┬────┘
                │
                ▼
           ┌─────────┐
           │ Cleanup │  Async: write values back to keys, unlock, GC log
           └─────────┘
```

A transaction `Handle` progresses through three internal states:

- **New** — transaction is executing user code.
- **Validating** — locks are being acquired and reads verified.
- **Committed** — transaction log written; commit is durable.

During **Execute**, reads go through the cache and are tracked (path + version).
Writes are staged in memory. No locks are held in this phase.

During **Validate**, the algorithm acquires locks and checks that every read
version still matches the current state. If any key was modified by a concurrent
transaction, the current transaction retries — but crucially, it retries with
locks still held, so the second attempt is guaranteed to succeed (at most one
retry).

After **Commit**, the transaction log is the durable record. The async cleanup
phase writes the new values back to keys, releases locks, and schedules the
transaction log for garbage collection.

### Optimistic Concurrency Control

The core idea: **transactions run without locks until commit time.** This means
non-conflicting transactions never interfere with each other.

```
Transaction A (keys 1, 2)         Transaction B (keys 3, 4)
─────────────────────────         ─────────────────────────
Read key 1                        Read key 3
Read key 2                        Read key 4
Stage write to key 1              Stage write to key 3
  ── validate ──                    ── validate ──
Lock key 1, key 2                 Lock key 3, key 4
Verify versions                   Verify versions
Write tx log                      Write tx log
  ── commit ──                      ── commit ──
```

Since A and B touch different keys, they proceed fully in parallel — no waiting,
no retries. Locks are held only for the brief validate-and-commit window.

When transactions *do* conflict:

1. Both reach the validate phase and try to lock overlapping keys.
2. One wins the lock; the other detects a version mismatch.
3. The loser retries with its locks held (pessimistic fallback), guaranteeing
   progress.

### Distributed Locks

Lock state is stored in the **object metadata tags** of each key:

| Tag | Values | Purpose |
|-----|--------|---------|
| `lock-type` | `r`, `w`, `c`, `-` | Current lock type (read, write, create, none) |
| `locked-by` | base64 tx IDs (comma-separated) | Which transactions hold the lock |
| `last-writer` | base64 tx ID | Transaction that last wrote this key |

Lock acquisition is a compare-and-swap on the metadata: read the current tags
and version, compute the new lock state, and conditionally write the updated tags
using `SetTagsIf`. If the version changed (another transaction modified the
tags), the operation retries.

**Compatibility rules:**

| Requested | Current: None | Current: Read | Current: Write | Current: Create |
|-----------|:---:|:---:|:---:|:---:|
| Read | ✓ | ✓ | wait | wait |
| Write | ✓ | upgrade if sole holder | wait | wait |
| Create | ✓ | wait | wait | wait |

- Multiple transactions can hold **read** locks simultaneously.
- **Write** locks are exclusive. A read lock can be upgraded to write only if
  the requesting transaction is the sole holder.
- **Create** locks are used when a key doesn't yet exist, to prevent concurrent
  creation.

### Transaction Logs

Each transaction gets its own log object, stored at a deterministic path based
on the transaction ID:

```
<db-prefix>/_t/<base64-encoded-tx-id>
```

The log is serialized as a Protocol Buffer and contains:

- **Status**: pending, committed, or aborted.
- **Timestamp**: when the log was last updated.
- **Writes**: list of (path, value, deleted, previous writer) entries.
- **Locks**: list of (path, lock type) entries.

The transaction log serves two critical purposes:

1. **Atomic commit point.** A transaction is committed if and only if its log
   object exists with status "committed". All the multi-key writes become
   durable in a single object write.
2. **Crash recovery synchronization.** Other transactions can inspect a log to
   determine whether a lock holder is still active, and can attempt to abort
   an expired transaction by conditionally writing to its log.

### Commit Protocol

The validate-and-commit sequence:

1. **Parallel lock acquisition.** Lock all read and written keys in parallel,
   with a 5-second deadlock timeout. If the timeout fires, fall back to serial
   locking.

2. **Version verification.** For each read key, check that the version hasn't
   changed since the transaction read it. If it has, the transaction retries
   with locks held.

3. **Write transaction log.** Write the log object atomically. After this point,
   the transaction is considered committed.

4. **Async write-back.** Write the new values to each modified key and release
   locks. This can happen asynchronously because the transaction log is the
   source of truth. If the client crashes, another transaction can read the log
   and complete the write-back (or just observe the committed values from the
   log).

### Optimizations

#### Read-only transactions

If a transaction only reads, it can skip locking entirely on the happy path:

1. Read all keys, tracking their versions.
2. After the last read, verify that all versions are still current and no keys
   are write-locked.
3. If verification passes: return immediately. No locks acquired, no log
   written.
4. If verification fails (concurrent write detected): retry once with the full
   locking protocol as a fallback.

This makes read-heavy workloads very efficient — the happy path requires only
one value read plus one metadata read per key, with zero writes.

#### Single read-modify-write

Transactions that read and write exactly one key use a native CAS shortcut:

1. Read the key's contents and metadata.
2. If the key is locked, fall back to the full protocol.
3. Otherwise, do a conditional write (`WriteIf`) with both the content and
   metadata versions as preconditions.

This avoids the lock-verify-log-writeback cycle entirely for the common case
of updating a single key.

#### Retry with locks held

When a transaction fails validation and must retry, it does so with its locks
still held. This means the second attempt runs under pessimistic locking and is
guaranteed to succeed — no further conflicts are possible. This bounds the
maximum number of retries to one per conflict.

### Deadlock Handling

GlassDB uses timeout-based deadlock detection:

1. **Parallel validation** attempts to lock all keys concurrently with a
   5-second timeout (`maxDeadlockTimeout`).
2. If the timeout expires (suspected deadlock), the transaction falls back to
   **serial validation**: locks are acquired one at a time in sorted path order.
   Total ordering prevents cycles in the wait-for graph, guaranteeing no
   further deadlocks.

This is a deliberate simplicity tradeoff: the naive approach is slow when
deadlocks do occur (tens of seconds in the worst case), but deadlocks should be
rare in the target workloads.

### Crash Recovery

If a client crashes mid-transaction, other clients can recover:

1. **Lock TTLs.** While holding locks, a transaction periodically refreshes its
   transaction log with a new timestamp. If the timestamp becomes stale (the
   transaction hasn't refreshed within the timeout), competing transactions
   consider the lock expired.

2. **Transaction log as arbiter.** To take over an expired lock, a competing
   transaction attempts to **conditionally write** to the expired transaction's
   log, marking it as aborted. If this CAS succeeds, the old transaction is
   officially aborted and its locks are invalid. If the CAS fails (the
   transaction refreshed or committed in the meantime), the competitor waits
   longer.

3. **Safe even with races.** If a crashed transaction's commit write races with
   a competitor's abort write, the CAS semantics ensure exactly one succeeds.
   The loser observes the version mismatch and backs off.

## Storage & Caching

GlassDB uses a three-layer caching architecture to minimize backend calls:

```
┌───────────────────────────────────────┐
│           Transaction Code            │
└─────────────────┬─────────────────────┘
                  │ tx.Read / tx.Write
                  ▼
┌───────────────────────────────────────┐
│         Local Cache (per-DB)          │
│  Staleness tracking, outdated flags   │
│  Separate caches for values & metadata│
└─────────────────┬─────────────────────┘
                  │ cache miss or stale
                  ▼
┌───────────────────────────────────────┐
│     Global Cache (read-through)       │
│  Uses ReadIfModified to avoid full    │
│  downloads when version unchanged     │
└─────────────────┬─────────────────────┘
                  │ version changed or absent
                  ▼
┌───────────────────────────────────────┐
│         Backend (Object Storage)      │
└───────────────────────────────────────┘
```

**LRU Cache** (`internal/cache/`). A thread-safe, size-based LRU cache (default
512 MiB, configurable via `Options.CacheSize`). Entries are evicted
least-recently-used first when the total size exceeds the limit.

**Local Cache** (`internal/storage/local.go`). Wraps the LRU cache with
staleness awareness. Each entry tracks when it was last updated and whether it
has been marked outdated (e.g., because a concurrent transaction invalidated
it). Separate entries are maintained for values and metadata.

**Global Cache** (`internal/storage/global.go`). A read-through and
write-through layer over the backend. On reads, it uses `ReadIfModified` to
avoid re-downloading objects that haven't changed. On writes, it updates the
local cache with the new value and version immediately.

After a transaction commits, its written values are cached locally. Subsequent
transactions on the same client can read them without hitting the backend,
unless another client modifies the same keys.

## Data Model

### Path Encoding

Storage paths follow a hierarchical scheme with type markers:

```
<db-prefix>/<type>/<base64-encoded-name>
```

| Type Marker | Meaning | Example |
|-------------|---------|---------|
| `_k` | Key (user data) | `mydb/coll/_k/dXNlcl9rZXk` |
| `_c` | Collection (sub-collection) | `mydb/root/_c/c2V0dGluZ3M` |
| `_t` | Transaction log | `mydb/root/_t/dHhfMTIz` |
| `_i` | Collection info (metadata) | `mydb/root/_i` |

Key names and collection names are base64-encoded in the path to avoid
conflicts with the type markers and to support arbitrary byte sequences.

### Collections

A `Collection` is a scoped namespace for keys, similar to a table or a prefix.
Each collection has a pseudo-key (its collection info object, `_i`) that
represents the "list of keys" in the collection. This pseudo-key is locked when
keys are created or deleted, ensuring consistency for key enumeration:

- **Create a key**: lock the collection info in write + lock the new key in
  create.
- **Delete a key**: lock the collection info in write + lock the key in write.
- **Iterate keys**: lock the collection info in read.
- **Read/write an existing key**: no collection lock needed.

### Versioning

The `Version` type in `internal/storage/` combines two sources of truth:

- **Backend version** (`backend.Version`): the version assigned by object
  storage, used for conditional operations.
- **Local writer** (`data.TxID`): if the value was written by a local
  (not-yet-committed) transaction, this tracks which one.

During validation, the algorithm checks both: the backend version must match
what was read, and the last writer (from the `last-writer` tag) must match
expectations.

## Garbage Collection

Committed and aborted transaction logs are no longer needed once all their locks
are released and values written back. The `GC` component
(`internal/trans/gc.go`) handles cleanup:

- **Scheduled cleanup.** After a transaction completes, its log is queued for
  deletion with a 1-minute delay (`cleanupInterval`). The delay ensures
  in-flight readers can still inspect the log.
- **Bounded queue.** The cleanup queue holds at most 1024 items
  (`sizeLimit`). If the queue is full, further items are dropped (they'll be
  cleaned up eventually by future transactions or restarts).
- **Background execution.** Cleanup runs asynchronously and does not block
  transaction processing.
