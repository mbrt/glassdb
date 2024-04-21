# Bugs

## Concurrent lock unlock of the same transaction

Example trace:

```log
2023/03/15 09:55:23 Tx: a5b5d4507d2366f79e31a44ec9884d22, LockWrite("example/_c/O6KhQmpYQqlg/_k/PqKtC0") BEGIN
2023/03/15 09:55:23 Backend: WriteIf("example/_c/O6KhQmpYQqlg/_k/PqKtC0", val[size]=1, expv={Contents:574 Meta:133}, t=map[last-writer:2-5ygdNxvw0zuVOMI4qF3A== lock-type:w locked-by:pbXUUH0jZveeMaROyYhNIg==]) = {Tags:map[] Version:{Contents:0 Meta:0}}, err = context canceled
2023/03/15 09:55:23 Tx: a5b5d4507d2366f79e31a44ec9884d22, LockWrite("example/_c/O6KhQmpYQqlg/_k/PqKtC0") END error = context canceled
2023/03/15 09:55:23 Tx: a5b5d4507d2366f79e31a44ec9884d22, Unlock("example/_c/O6KhQmpYQqlg/_k/PqKtC0", committed = false) BEGIN
2023/03/15 09:55:23 Backend: GetMetadata("example/_c/O6KhQmpYQqlg/_k/PqKtC0") = {Tags:map[last-writer:mWM_ZtvtYPzaBVtyTfYg2Q== lock-type:w locked-by:2-5ygdNxvw0zuVOMI4qF3A==] Version:{Contents:574 Meta:133}}, err = <nil>
2023/03/15 09:55:23 Tx: a5b5d4507d2366f79e31a44ec9884d22, Unlock("example/_c/O6KhQmpYQqlg/_k/PqKtC0") END err = <nil>
2023/03/15 09:55:23 Backend: GetMetadata("example/_c/O6KhQmpYQqlg/_k/PqKtC0") = {Tags:map[last-writer:2-5ygdNxvw0zuVOMI4qF3A== lock-type:w locked-by:pbXUUH0jZveeMaROyYhNIg==] Version:{Contents:591 Meta:134}}, err = <nil>
```

With some translation:

* `mWM_ZtvtYPzaBVtyTfYg2Q==` -> `99633f66dbed60fcda055b724df620d9`
* `2-5ygdNxvw0zuVOMI4qF3A==` -> `dbee7281d371bf0d33b9538c238a85dc`
* `pbXUUH0jZveeMaROyYhNIg==` -> `a5b5d4507d2366f79e31a44ec9884d22`

```log
2023/03/15 09:55:23 Tx: txid, LockWrite("key") BEGIN
2023/03/15 09:55:23 Backend: WriteIf("key", val[size]=1, expv={Contents:574 Meta:133}, t=map[last-writer:tx2 lock-type:w locked-by:txid]) = err = context canceled
2023/03/15 09:55:23 Tx: txid, LockWrite("key") END error = context canceled
2023/03/15 09:55:23 Tx: txid, Unlock("key", committed = false) BEGIN
2023/03/15 09:55:23 Backend: GetMetadata("key") = {Tags:map[last-writer:last-w-1 lock-type:w locked-by:tx2] Version:{Contents:574 Meta:133}}, err = <nil>
2023/03/15 09:55:23 Tx: txid, Unlock("key") END err = <nil>
2023/03/15 09:55:23 Backend: GetMetadata("key") = {Tags:map[last-writer:tx2 lock-type:w locked-by:txid] Version:{Contents:591 Meta:134}}, err = <nil>
```

* The `LockWrite` starts but times out.
* Then `Unlock` starts, but it reads the old value, so exits without action.
* The `LockWrite` actually completes and locks the value.

## Data race in local storage

```log
WARNING: DATA RACE
Write at 0x00c0005ce319 by goroutine 655:
  github.com/mbrt/glassdb/internal/storage.Local.MarkValueOutated.func1()
      /ws/glassdb/internal/storage/local.go:115 +0x1b3
  github.com/mbrt/glassdb/internal/cache.(*Cache).Update()
      /ws/glassdb/internal/cache/cache.go:54 +0x4cb
  github.com/mbrt/glassdb/internal/storage.Local.MarkValueOutated()
      /ws/glassdb/internal/storage/local.go:107 +0xac
  github.com/mbrt/glassdb/internal/trans.Algo.commitSingleRW()
      /ws/glassdb/internal/trans/algo.go:249 +0xcaa
  github.com/mbrt/glassdb/internal/trans.Algo.commitRound()
      /ws/glassdb/internal/trans/algo.go:137 +0x227
  github.com/mbrt/glassdb/internal/trans.Algo.Commit()
      /ws/glassdb/internal/trans/algo.go:106 +0x217
  github.com/mbrt/glassdb.(*DB).txImpl()
      /ws/glassdb/db.go:169 +0xf4a
  github.com/mbrt/glassdb.(*DB).Tx()
      /ws/glassdb/db.go:107 +0xf4
  github.com/mbrt/glassdb_test.rmw()
      /ws/glassdb/glassdb_test.go:243 +0x152
  github.com/mbrt/glassdb_test.TestConcurrentRMW.func2()
      /ws/glassdb/glassdb_test.go:313 +0x147
  golang.org/x/sync/errgroup.(*Group).Go.func1()
      /go/pkg/mod/golang.org/x/sync@v0.1.0/errgroup/errgroup.go:75 +0x82

Previous read at 0x00c0005ce319 by goroutine 1253:
  github.com/mbrt/glassdb/internal/storage.cacheEntry.isValueOutdated()
      /ws/glassdb/internal/storage/local.go:201 +0x48
  github.com/mbrt/glassdb/internal/storage.Local.Read()
      /ws/glassdb/internal/storage/local.go:37 +0x164
  github.com/mbrt/glassdb/internal/trans.(*Monitor).CommittedValue()
      /ws/glassdb/internal/trans/monitor.go:231 +0x10e
  github.com/mbrt/glassdb/internal/trans.(*Locker).fetchLockersState()
      /ws/glassdb/internal/trans/tlocker.go:247 +0x11d
  github.com/mbrt/glassdb/internal/trans.(*Locker).doLockOp()
      /ws/glassdb/internal/trans/tlocker.go:194 +0x1c5
  github.com/mbrt/glassdb/internal/trans.lockerWorker.Work()
      /ws/glassdb/internal/trans/tlocker.go:300 +0x27b
  github.com/mbrt/glassdb/internal/concurr.(*controller).Do()
      /ws/glassdb/internal/concurr/dedup.go:62 +0x80c
  github.com/mbrt/glassdb/internal/concurr.(*Dedup).Do()
      /ws/glassdb/internal/concurr/dedup.go:23 +0x413
  github.com/mbrt/glassdb/internal/trans.(*Locker).pushRequest()
      /ws/glassdb/internal/trans/tlocker.go:126 +0x30a
  github.com/mbrt/glassdb/internal/trans.(*Locker).Unlock()
      /ws/glassdb/internal/trans/tlocker.go:78 +0x204
  github.com/mbrt/glassdb/internal/trans.Algo.unlock()
      /ws/glassdb/internal/trans/algo.go:907 +0x1b6
  github.com/mbrt/glassdb/internal/trans.Algo.scheduleCleanup.func1()
      /ws/glassdb/internal/trans/algo.go:996 +0x1e4
  github.com/mbrt/glassdb/internal/concurr.(*Fanout).Spawn.func1()
      /ws/glassdb/internal/concurr/fanout.go:26 +0x67
  golang.org/x/sync/errgroup.(*Group).Go.func1()
      /go/pkg/mod/golang.org/x/sync@v0.1.0/errgroup/errgroup.go:75 +0x82

Goroutine 655 (running) created at:
  golang.org/x/sync/errgroup.(*Group).Go()
      /go/pkg/mod/golang.org/x/sync@v0.1.0/errgroup/errgroup.go:72 +0x12e
  github.com/mbrt/glassdb_test.TestConcurrentRMW()
      /ws/glassdb/glassdb_test.go:312 +0x5c4
  testing.tRunner()
      /usr/lib/go/src/testing/testing.go:1576 +0x216
  testing.(*T).Run.func1()
      /usr/lib/go/src/testing/testing.go:1629 +0x47

Goroutine 1253 (running) created at:
  golang.org/x/sync/errgroup.(*Group).Go()
      /go/pkg/mod/golang.org/x/sync@v0.1.0/errgroup/errgroup.go:72 +0x12e
  github.com/mbrt/glassdb/internal/concurr.(*Fanout).Spawn()
      /ws/glassdb/internal/concurr/fanout.go:25 +0x1be
  github.com/mbrt/glassdb/internal/trans.(*Scheduler).BackgroundFanout.func1()
      /ws/glassdb/internal/trans/scheduler.go:70 +0x10b
  golang.org/x/sync/errgroup.(*Group).TryGo.func1()
      /go/pkg/mod/golang.org/x/sync@v0.1.0/errgroup/errgroup.go:104 +0x82
==================
--- FAIL: TestConcurrentRMW (0.13s)
```

The problem was that `MarkValueOutdated` was modifying the cached value
directly, which was a pointer. This was done outside locking. The proper fix
was to return a copy of the value instead.

## Deadlock in DB Close

Goroutines state:

| Goroutine | Context                    | Function      | Resource   |
|-----------|----------------------------|---------------|------------|
|        20 | (Scheduler Close)          | Group.Wait    | semacquire |
|      1263 | scheduler BackgroundFanout | Group.Wait    | semacquire |
|      1626 | (scheduleCleanup)          | Unlock        | chan send  |
|      6259 | (scheduleCleanup)          | Unlock        | chan send  |
|      1264 | AndThen                    | asyncRes.Wait | chan recv  |
|      1265 | (scheduleCleanup)          | asyncRes.Wait | chan recv  |
|      5280 | scheduler BackgroundFanout | Group.Wait    | semacquire |
|      5821 | AndThen                    | asyncRes.Wait | chan recv  |
|      6232 | (scheduleCleanup)          | Unlock nextCh | chan send  |
|      6230 | (scheduleCleanup)          | Unlock nextCh | chan send  |
|      5822 | (scheduleCleanup)          | asyncRes.Wait | chan recv  |

The problem was that in `Dedup`, multiple producers would try to notify a
smaller number of consumers on `OnNextDo`. The race happened because
notifications were sent while unlocked and so consumers waiting could already
have left.

The solution was to use a non-blocking send. When the channel is full, nothing
would be sent. This is also done while keeping the lock.

```go
if c.NextCh != nil {
	select {
	case c.NextCh <- struct{}{}:
	default:
	}
}
```

## Wrong update to local cache in lock readonly

Logs demonstrating the problem:


|    A | transaction                      | event                 | path                               | error                                       | extra                                                                                                                                                                                                     |
|------|----------------------------------|-----------------------|------------------------------------|---------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2323 | 7505d4a89111181d381bd1c5179af897 | NewVstate             | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             | readv:{B:{Contents:244 Meta:0} Writer:c995e7fcc49e4481a31e9ab2e6f46681};atype:r---                                                                                                                        |
| 2339 | 7505d4a89111181d381bd1c5179af897 | Commit round BEGIN    |                                    |                                             |                                                                                                                                                                                                           |
| 2343 | 7505d4a89111181d381bd1c5179af897 | Commit readonly BEGIN |                                    |                                             |                                                                                                                                                                                                           |
| 2373 | 815182aba20f4fdd5d7646fd57c75c83 | NewVstate             | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             | readv:{B:{Contents:0 Meta:0} Writer:2c88b2e4f1cd76a036c0043d3f748d18};atype:r-w-                                                                                                                          |
| 2390 | 815182aba20f4fdd5d7646fd57c75c83 | Commit round BEGIN    |                                    |                                             |                                                                                                                                                                                                           |
| 2391 | 815182aba20f4fdd5d7646fd57c75c83 | Commit parallel BEGIN |                                    |                                             |                                                                                                                                                                                                           |
| 2404 | 815182aba20f4fdd5d7646fd57c75c83 | LockWrite BEGIN       | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             |                                                                                                                                                                                                           |
| 2463 | 815182aba20f4fdd5d7646fd57c75c83 | LockWrite END         | example/_c/O6KhQmpYQqlg/_k/PqKtBIB | <nil>                                       |                                                                                                                                                                                                           |
| 2464 | 815182aba20f4fdd5d7646fd57c75c83 | CompareVersion        | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             | rv:{B:{Contents:0 Meta:0} Writer:2c88b2e4f1cd76a036c0043d3f748d18};gmeta:{Tags:map[last-writer:LIiy5PHNdqA2wAQ9P3SNGA== lock-type:w locked-by:gVGCq6IPT91ddkb9V8dcgw==] Version:{Contents:257 Meta:21}}   |
| 2482 | 7505d4a89111181d381bd1c5179af897 | UpdateLocal           | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             | vers:{B:{Contents:0 Meta:0} Writer:815182aba20f4fdd5d7646fd57c75c83};w:{Path:example/_c/O6KhQmpYQqlg/_k/PqKtBIB Val:[16] Delete:false}                                                                    |
| 2492 | 7505d4a89111181d381bd1c5179af897 | Commit round END      |                                    | stale read of 15/15 keys: retry transaction |                                                                                                                                                                                                           |
| 2529 | 7505d4a89111181d381bd1c5179af897 | NewVstate             | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             | readv:{B:{Contents:0 Meta:0} Writer:815182aba20f4fdd5d7646fd57c75c83};atype:r---                                                                                                                          |
| 2537 | 7505d4a89111181d381bd1c5179af897 | Commit round BEGIN    |                                    |                                             |                                                                                                                                                                                                           |
| 2539 | 7505d4a89111181d381bd1c5179af897 | Commit parallel BEGIN |                                    |                                             |                                                                                                                                                                                                           |
| 2557 | 7505d4a89111181d381bd1c5179af897 | LockRead BEGIN        | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             |                                                                                                                                                                                                           |
| 2576 | 815182aba20f4fdd5d7646fd57c75c83 | Commit round END      |                                    | <nil>                                       |                                                                                                                                                                                                           |
| 2577 | 7505d4a89111181d381bd1c5179af897 | LockRead END          | example/_c/O6KhQmpYQqlg/_k/PqKtBIB | <nil>                                       |                                                                                                                                                                                                           |
| 2578 | 7505d4a89111181d381bd1c5179af897 | CompareVersion        | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             | rv:{B:{Contents:0 Meta:0} Writer:815182aba20f4fdd5d7646fd57c75c83};gmeta:{Tags:map[last-writer:gVGCq6IPT91ddkb9V8dcgw== lock-type:r locked-by:dQXUqJERGB04G9HFF5r4lw==] Version:{Contents:263 Meta:22}}   |
| 2595 | 815182aba20f4fdd5d7646fd57c75c83 | Unlock BEGIN          | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             |                                                                                                                                                                                                           |
| 2634 | 815182aba20f4fdd5d7646fd57c75c83 | Unlock END            | example/_c/O6KhQmpYQqlg/_k/PqKtBIB | <nil>                                       |                                                                                                                                                                                                           |
| 2682 | 7505d4a89111181d381bd1c5179af897 | Commit round END      |                                    | stale read of 14/15 keys: retry transaction |                                                                                                                                                                                                           |
| 2709 | 7505d4a89111181d381bd1c5179af897 | NewVstate             | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             | readv:{B:{Contents:263 Meta:0} Writer:815182aba20f4fdd5d7646fd57c75c83};atype:r---                                                                                                                        |
| 2713 | 7505d4a89111181d381bd1c5179af897 | Commit round BEGIN    |                                    |                                             |                                                                                                                                                                                                           |
| 2714 | 7505d4a89111181d381bd1c5179af897 | Commit parallel BEGIN |                                    |                                             |                                                                                                                                                                                                           |
| 2739 | 7505d4a89111181d381bd1c5179af897 | LockRead BEGIN        | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             |                                                                                                                                                                                                           |
| 2740 | 7505d4a89111181d381bd1c5179af897 | LockRead END          | example/_c/O6KhQmpYQqlg/_k/PqKtBIB | <nil>                                       |                                                                                                                                                                                                           |
| 2755 | 7505d4a89111181d381bd1c5179af897 | CompareVersion        | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             | rv:{B:{Contents:263 Meta:0} Writer:815182aba20f4fdd5d7646fd57c75c83};gmeta:{Tags:map[last-writer:gVGCq6IPT91ddkb9V8dcgw== lock-type:r locked-by:dQXUqJERGB04G9HFF5r4lw==] Version:{Contents:263 Meta:23}} |
| 2761 | 7505d4a89111181d381bd1c5179af897 | Commit round END      |                                    | <nil>                                       |                                                                                                                                                                                                           |
| 2793 | 7505d4a89111181d381bd1c5179af897 | Unlock BEGIN          | example/_c/O6KhQmpYQqlg/_k/PqKtBIB |                                             |                                                                                                                                                                                                           |
| 2794 | 7505d4a89111181d381bd1c5179af897 | Unlock END            | example/_c/O6KhQmpYQqlg/_k/PqKtBIB | context canceled                            |                                                                                                                                                                                                           |

Here the readonly transaction updates the local cache with the new value, but
incorrectly sets the currently uncommitted transaction holding the lock as the
last writer.

## Race in tx after close

```log
panic: send on closed channel

goroutine 763298 [running]:
github.com/mbrt/glassdb/internal/trans.(*Monitor).BeginTx(0xc000000780, {0xe486a0, 0xc000a560a0}, {0xc0007048a0, 0x10, 0x10})
        /ws/glassdb/internal/trans/monitor.go:60 +0x1ff
github.com/mbrt/glassdb/internal/trans.Algo.Commit({{0xe4aaf0, 0xc0008daae0}, {{0xe4c640, 0xc0008dad20}, {0xc0008dac30, {0xe4aaf0, 0xc0008daae0}}, {0xe4aaf0, 0xc0008daae0}}, {0xc0008dac30, ...}, ...}, ...)
        /ws/glassdb/internal/trans/algo.go:93 +0x70
github.com/mbrt/glassdb.(*DB).txImpl(0xc000004c00, {0xe486a0?, 0xc000a560a0}, 0xc000533f40, 0xc000533ea0)
        /ws/glassdb/db.go:185 +0xa6e
github.com/mbrt/glassdb.(*DB).Tx(0xc000004c00, {0xe486a0, 0xc000a560a0}, 0xc0007f1f58?)
        /ws/glassdb/db.go:110 +0xcf
github.com/mbrt/glassdb_test.BenchmarkSharedR.func2({0xc00108c110?, 0xc000be62d0?, 0x4433e5?})
        /ws/glassdb/bench_test.go:226 +0x93
github.com/mbrt/glassdb_test.BenchmarkSharedR.func3()
        /ws/glassdb/bench_test.go:237 +0x66
created by github.com/mbrt/glassdb_test.BenchmarkSharedR
        /ws/glassdb/bench_test.go:235 +0x5ac
```

This happens because transactions run after `db.Close()`. We need to make sure
we wait for goroutines doing transactions, before closing.

## Truncated objects on context cancellation

The GCS client is bugged and sometimes sends truncated objects on uploads when
the context is cancelled. The bad part of this is that the request is completely
valid (i.e. the multipart request has correct boundaries).

For example, see this valid request for an empty object:

```
--aaccafdc8317d6a4188884ac408a4340098c98e92255d9e356d476cadf02
Content-Type: application/json

{"bucket":"test","contentType":"application/octet-stream","metadata":{"version":"v0"},"name":"example/glassdb"}

--aaccafdc8317d6a4188884ac408a4340098c98e92255d9e356d476cadf02
Content-Type: application/octet-stream


--aaccafdc8317d6a4188884ac408a4340098c98e92255d9e356d476cadf02--
```

And this is an invalid one:

```
--d87765176f7f38893708a80d88ca609b6a89a89df86a974a9fd9eff55e8a
Content-Type: application/json

{"bucket":"test","contentType":"application/octet-stream","metadata":{"last-writer":"dwvFcuzuwDPwwgteAmFfpQ==","lock-type":"w","locked-by":"xAlD8zfOPL8cJUjq02tV1g=="},"name":"example/_c/RaprALB/_k/PqKtBV"}

--d87765176f7f38893708a80d88ca609b6a89a89df86a974a9fd9eff55e8a
Content-Type: application/octet-stream


--d87765176f7f38893708a80d88ca609b6a89a89df86a974a9fd9eff55e8a--
```

Completely indistinguishable.

As the client is very complicated, a quick fix for this was to just always send
the CRC32 field along with every upload.

## Deadlock in ReadMulti + Write

This is happening in `TestReadMulti`, which reads and writes 15 keys in the same
transaction. There are no parallel transactions and these are executed 30 times
in a row.

The observed deadlock is:

```log
panic: test timed out after 2m0s
running tests:
	TestReadMulti (1m48s)
	TestReadMulti/gcs (1m48s)

goroutine 124957 [chan receive, 1 minutes]:
testing.(*T).Run(0xc000552b60, {0xd7a349?, 0xeb1f80?}, 0xc000618360)
	/usr/lib/go/src/testing/testing.go:1750 +0x3ab
github.com/mbrt/glassdb_test.TestReadMulti(0xc000552b60)
	/ws/glassdb/glassdb_test.go:479 +0xf0
testing.tRunner(0xc000552b60, 0xdcfc98)
	/usr/lib/go/src/testing/testing.go:1689 +0xfb
created by testing.(*T).Run in goroutine 1
	/usr/lib/go/src/testing/testing.go:1742 +0x390

goroutine 125011 [semacquire, 1 minutes]:
sync.runtime_Semacquire(0xc00062c4c0?)
	/usr/lib/go/src/runtime/sema.go:62 +0x25
sync.(*WaitGroup).Wait(0xc00062c4c0?)
	/usr/lib/go/src/sync/waitgroup.go:116 +0x48
golang.org/x/sync/errgroup.(*Group).Wait(0xc00062c4c0)
	/ws/go/pkg/mod/golang.org/x/sync@v0.7.0/errgroup/errgroup.go:56 +0x25
github.com/mbrt/glassdb/internal/trans.Algo.fanout({{0xeb1f80, 0xc00099d7d0}, {{0xeb2ff0, 0xc0004fc2d0}, {0xc0004fc240, {0xeb1f80, 0xc00099d7d0}}, {0xeb1f80, 0xc00099d7d0}}, {0xc0004fc240, ...}, ...}, ...)
	/ws/glassdb/internal/trans/algo.go:1064 +0x4e
github.com/mbrt/glassdb/internal/trans.Algo.unlockAll({{0xeb1f80, 0xc00099d7d0}, {{0xeb2ff0, 0xc0004fc2d0}, {0xc0004fc240, {0xeb1f80, 0xc00099d7d0}}, {0xeb1f80, 0xc00099d7d0}}, {0xc0004fc240, ...}, ...}, ...)
	/ws/glassdb/internal/trans/algo.go:997 +0x1f2
github.com/mbrt/glassdb/internal/trans.Algo.serialValidate({{0xeb1f80, 0xc00099d7d0}, {{0xeb2ff0, 0xc0004fc2d0}, {0xc0004fc240, {0xeb1f80, 0xc00099d7d0}}, {0xeb1f80, 0xc00099d7d0}}, {0xc0004fc240, ...}, ...}, ...)
	/ws/glassdb/internal/trans/algo.go:660 +0x110
github.com/mbrt/glassdb/internal/trans.Algo.validateReadWrite({{0xeb1f80, 0xc00099d7d0}, {{0xeb2ff0, 0xc0004fc2d0}, {0xc0004fc240, {0xeb1f80, 0xc00099d7d0}}, {0xeb1f80, 0xc00099d7d0}}, {0xc0004fc240, ...}, ...}, ...)
	/ws/glassdb/internal/trans/algo.go:215 +0x13b
github.com/mbrt/glassdb/internal/trans.Algo.validateRound({{0xeb1f80, 0xc00099d7d0}, {{0xeb2ff0, 0xc0004fc2d0}, {0xc0004fc240, {0xeb1f80, 0xc00099d7d0}}, {0xeb1f80, 0xc00099d7d0}}, {0xc0004fc240, ...}, ...}, ...)
	/ws/glassdb/internal/trans/algo.go:187 +0x25b
github.com/mbrt/glassdb/internal/trans.Algo.Commit({{0xeb1f80, 0xc00099d7d0}, {{0xeb2ff0, 0xc0004fc2d0}, {0xc0004fc240, {0xeb1f80, 0xc00099d7d0}}, {0xeb1f80, 0xc00099d7d0}}, {0xc0004fc240, ...}, ...}, ...)
	/ws/glassdb/internal/trans/algo.go:122 +0x11c
github.com/mbrt/glassdb.(*DB).txImpl.func3(...)
	/ws/glassdb/db.go:219
github.com/mbrt/glassdb/internal/trace.WithRegion(...)
	/ws/glassdb/internal/trace/notrace.go:30
github.com/mbrt/glassdb.(*DB).txImpl(0xc000236008, {0xeadec0, 0x14b9840}, 0xc000661f00, 0xc0006605b0)
	/ws/glassdb/db.go:218 +0xa48
github.com/mbrt/glassdb.(*DB).Tx(0xc000236008, {0xeadec0, 0x14b9840}, 0xc000661f00)
	/ws/glassdb/db.go:134 +0xb2
github.com/mbrt/glassdb_test.TestReadMulti.func1(0xc000413040)
	/ws/glassdb/glassdb_test.go:505 +0x3db
testing.tRunner(0xc000413040, 0xc000618360)
	/usr/lib/go/src/testing/testing.go:1689 +0xfb
created by testing.(*T).Run in goroutine 124957
	/usr/lib/go/src/testing/testing.go:1742 +0x390

goroutine 134014 [select, 1 minutes]:
github.com/mbrt/glassdb/internal/concurr.(*controller).Do(0xc0003fe070, {0xeadfd8, 0xc00022e5f0}, {0xc00022d0e0, 0x22}, {0xeaa070, 0xc0005bc7c0}, {0xea4340, 0xc000556100})
	/ws/glassdb/internal/concurr/dedup.go:101 +0x3a5
github.com/mbrt/glassdb/internal/concurr.(*Dedup).Do(...)
	/ws/glassdb/internal/concurr/dedup.go:37
github.com/mbrt/glassdb/internal/trans.(*Locker).pushRequest(0xc000556100, {0xeadfd8, 0xc00022e5f0}, {0xc00022d0e0, 0x22}, 0x1, {0xc00043d7f0, 0x10, 0x10})
	/ws/glassdb/internal/trans/tlocker.go:169 +0x24c
github.com/mbrt/glassdb/internal/trans.(*Locker).Unlock(...)
	/ws/glassdb/internal/trans/tlocker.go:100
github.com/mbrt/glassdb/internal/trans.Algo.unlock.func1(...)
	/ws/glassdb/internal/trans/algo.go:949
github.com/mbrt/glassdb/internal/trace.WithRegion(...)
	/ws/glassdb/internal/trace/notrace.go:30
github.com/mbrt/glassdb/internal/trans.Algo.unlock({{0xeb1f80, 0xc00099d7d0}, {{0xeb2ff0, 0xc0004fc2d0}, {0xc0004fc240, {0xeb1f80, 0xc00099d7d0}}, {0xeb1f80, 0xc00099d7d0}}, {0xc0004fc240, ...}, ...}, ...)
	/ws/glassdb/internal/trans/algo.go:948 +0x165
github.com/mbrt/glassdb/internal/trans.Algo.unlockAll.func1({0xeadfd8?, 0xc00022e5f0?}, 0xe)
	/ws/glassdb/internal/trans/algo.go:999 +0xc5
github.com/mbrt/glassdb/internal/concurr.Fanout.Spawn.func1()
	/ws/glassdb/internal/concurr/fanout.go:40 +0x26
golang.org/x/sync/errgroup.(*Group).Go.func1()
	/ws/go/pkg/mod/golang.org/x/sync@v0.7.0/errgroup/errgroup.go:78 +0x56
created by golang.org/x/sync/errgroup.(*Group).Go in goroutine 125011
	/ws/go/pkg/mod/golang.org/x/sync@v0.7.0/errgroup/errgroup.go:75 +0x96

goroutine 133846 [select]:
github.com/mbrt/glassdb/internal/trans.(*Monitor).refreshPending(0xc00068a0c0, {0xeae160, 0xc00042c8e8}, {0xc00043d7f0, 0x10, 0x10}, {0xeac300, 0xc00054b220})
	/ws/glassdb/internal/trans/monitor.go:503 +0x265
github.com/mbrt/glassdb/internal/trans.(*Monitor).StartRefreshTx.func1({0xeae160, 0xc00042c8e8})
	/ws/glassdb/internal/trans/monitor.go:102 +0xe5
github.com/mbrt/glassdb/internal/concurr.(*Background).Go.func1()
	/ws/glassdb/internal/concurr/background.go:58 +0x6a
github.com/sourcegraph/conc/panics.(*Catcher).Try(0x0?, 0xc0005ddfd0?)
	/ws/go/pkg/mod/github.com/sourcegraph/conc@v0.3.0/panics/panics.go:23 +0x48
github.com/sourcegraph/conc.(*WaitGroup).Go.func1()
	/ws/go/pkg/mod/github.com/sourcegraph/conc@v0.3.0/waitgroup.go:32 +0x53
created by github.com/sourcegraph/conc.(*WaitGroup).Go in goroutine 133845
	/ws/go/pkg/mod/github.com/sourcegraph/conc@v0.3.0/waitgroup.go:30 +0x73
```

* One goroutine is blocked inside `unlockAll`, which unlocks all the keys in
  parallel before acquiring all the locks again, but in a well defined order.
* Another is inside `dedup.controller.Go` (internal/concurr/dedup.go:101),
  waiting to be notified and perform the unlock.
* Another is in `refreshPending` (internal/trans/monitor.go:503), keeping the
  transaction alive.

It's unlikely that the third goroutine has some problem. The biggest suspicion
goes to the `dedup` controller, which is quite complicated.

From the logs, one key is stuck inside `UNLOCK`. The same that timed out while
locking in write.

```csv
tx,msg,path,args,res,err
f107b,Commit round BEGIN,,,,
f107b,LockWrite BEGIN,example/_c/O6KhQmpYQqlg/_k/PqKtBI4,,,
f107b,LockWrite END,example/_c/O6KhQmpYQqlg/_k/PqKtBI4,,,updating lock: context canceled
f107b,Commit round END,,,,retry validation
f107b,Commit round BEGIN,,,,
f107b,Unlock BEGIN,example/_c/O6KhQmpYQqlg/_k/PqKtBI4,,,
f107b,Unlock END,example/_c/O6KhQmpYQqlg/_k/PqKtBI4,,,
f107b,LockWrite BEGIN,example/_c/O6KhQmpYQqlg/_k/PqKtBI4,,,
f107b,LockWrite END,example/_c/O6KhQmpYQqlg/_k/PqKtBI4,,,
107b,Commit round END,,,,
f107b,Unlock BEGIN,example/_c/O6KhQmpYQqlg/_k/PqKtBI4,,,
ce553,Commit round BEGIN,,,,
ce553,LockWrite BEGIN,example/_c/O6KhQmpYQqlg/_k/PqKtBI4,,,
ce553,LockWrite END,example/_c/O6KhQmpYQqlg/_k/PqKtBI4,,,context canceled
f107b,Unlock END,example/_c/O6KhQmpYQqlg/_k/PqKtBI4,,,
ce553,Commit round END,,,,retry validation
ce553,Commit round BEGIN,,,,
ce553,Unlock BEGIN,example/_c/O6KhQmpYQqlg/_k/PqKtBI4,,,
```
