# Glass DB

[![GoDoc](https://pkg.go.dev/badge/github.com/mbrt/glassdb)](https://pkg.go.dev/github.com/mbrt/glassdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/mbrt/glassdb)](https://goreportcard.com/report/github.com/mbrt/glassdb)
![Build](https://github.com/mbrt/glassdb/workflows/Build/badge.svg)

Glass DB is a pure Go key/value store on top of object storage (e.g. Google
Cloud Storage or Azure Blob Service) that is _stateless_ and supports _ACID
transactions_. Clients import Glass DB as a library and don't need to deploy,
nor depend on any additional services. Everything is built on top of object
storage.

The interface is inspired by [BoltDB](https://github.com/boltdb/bolt) and
Apple's [FoundationDB](https://github.com/apple/foundationdb).

## Project status

**Warning:** This is still alpha software.

Transactions _should_ be working correctly and performance could definitely
improve. Interfaces and file formats are _not_ stable and can still change at
any point. Documentation is almost nil, but I'm planning to work on it very
soon.

Note also that currently we only support
[Google GCS](https://cloud.google.com/storage/), but adding
[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs/)
should be very easy. More work is required for
[Amazon S3](https://aws.amazon.com/s3/) which lacks request preconditions on
uploads.[^1]

## Usage example

This short example demonstrates how a transaction can read and modify a single
key atomically. The function also returns the value read before the change.

```go
import (
	"context"
	"errors"

	"cloud.google.com/go/storage"
	"github.com/mbrt/glassdb"
	"github.com/mbrt/glassdb/backend"
	"github.com/mbrt/glassdb/backend/gcs"
)

func openDB(ctx context.Context, bucket, dbName string) (*glassdb.DB, error) {
	// See https://pkg.go.dev/cloud.google.com/go/storage for how to initialize
	// a Google Cloud Storage client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	backend := gcs.New(client.Bucket(bucket))
	return glassdb.Open(ctx, dbName, backend)
}

func example(db *glassdb.DB) (string, error) {
	ctx := context.Background()
	coll := db.Collection([]byte("my-collection"))
	key := []byte("key")
	var res string

	err := db.Tx(ctx, func(tx *glassdb.Tx) error {
		b, err := tx.Read(coll, key)
		// The first time around there's no key, so here we would get an error.
		// In that case we continue below and just write the first 'Hello'.
		if err != nil && !errors.Is(err, backend.ErrNotFound) {
			return err
		}
		res = string(b)
		if string(b) == "Hello" {
			return tx.Write(coll, key, []byte("world!"))
		}
		return tx.Write(coll, key, []byte("Hello"))
	})

	return res, err
}
```

## Why?

A blog post with more details will come soon, but see below for a preview.

This project makes the following specific tradeoffs:

* Optimizes for rare conflicts between transactions (optimistic locking).
* Readers are rarely blocked.
* Clients are completely stateless and ephemeral. For example, they can be
  scaled down to zero. We avoid explicit coordination between clients (e.g.
  there's no need for consensus messages).
* Requires access to object storage (the lowest latency the better) with
  requests preconditions (both Google GCS and AWS S3 meet the requirements).
* Assumes that, when transactions race each other, it's better to be slow than
  to be incorrect.
* High throughput is better than low latency.
* Allows stale reads if explicitly requested, but defaults to strong consistency
  in all cases.
* Values are in the range 1KB to 1MB.

Glass DB makes sense in contexts where there are many writers that rarely write
to the same keys or reads are more frequent than writes.

### Example 1: User settings

One example could be storing user settings. Every key is
dedicated to one user and the value contains all the settings. This way we can
update each user independently (and scale horizontally). In the rare case where
two updates for the same user arrive concurrently, we _don't_ produce an
inconsistent result but retry the transaction.

### Example 2: Low frequency updates

The application serves low traffic (e.g. one query per minute). What are the
choices today?

* Single machine / VM mostly idle.
* "Serverless" function with a managed database (for example Google Cloud Run +
  Cloud SQL, or fly.io).

Neither seem cost effective in the scenario. We are talking about $10 a month,
which is not huge, but can we do better?

Yes. With Glass DB you only pay for each query and long term storage. In the
case of GCS (as of 2023) we are talking about:

* $0.020 per GB per month
* $0.05 per 10k write / list ops
* $0.004 per 10k read ops

At a rate of one write per minute this would be around $2 a month. Less usage?
Even less money.

### Example 3: Analytics

Data ingestion can usually be done in parallel and designed in such a way that
different processes write independently.

A compaction process can run in parallel to the ingestion, bringing the data in
a shape better suited for the query layer.

Compaction and ingestion are mostly independent, but we must make sure to be
robust to crashes and restarts (avoiding e.g. double-counting or event
duplicates). This can be ensured with transactions provided by Glass DB. If most
transactions don't conflict with each other, the throughput will scale mostly
linearly (See [Performance](#performance)).

## Performance

We are obviously bound by object storage's latencies which are typically:

Operation | Size     | Mean (ms) | Std Dev (ms) | Median (ms) | 90th % (ms)
----------|----------|-----------|--------------|-------------|------------
Download  |    1 KiB |      57.4 |          6.6 |       56.8  |       64.8
Download  |  100 KiB |      55.4 |          6.7 |       53.3  |       63.1
Download  |    1 MiB |      56.7 |          3.8 |       57.7  |       59.9
Metadata  |    1 KiB |      31.5 |          8.0 |       28.1  |       41.3
Upload    |    1 KiB |      70.4 |         17.3 |       64.7  |       88.8
Upload    |  100 KiB |      88.9 |         14.6 |       83.1  |      105.0
Upload    |    1 MiB |     117.5 |         12.6 |      115.9  |      131.0

This is a lot slower than most databases, but still has a few advantages:

1. Throughput: we can leverage object storage scalability by reading and writing
   many objects in parallel. In this way we can perform many transactions per
   second (scale linearly). We would only be limited by bandwidth (see [GCS
   quotas](https://cloud.google.com/storage/quotas#bandwidth)).

1. Size scalability: object storage scales to petabytes and probably more, as
   cloud providers keep working on making them faster and more scalable.

See how this translates in a dataset of 50k keys, where we vary the number of
concurrent clients. Each client DB is performing 10 transactions in parallel,
split in this way:

* 10% updates (i.e. read + write) two separate random keys.
* 60% strong reads to two separate random keys.
* 30% weak reads to one random key (max staleness of 10s).

For example, with 5 concurrent DBs we would have 50 parallel transactions at
every moment.

We did all the tests below by using Google Cloud Storage as a backend.

### Throughput

Glass DB's throughput scales mostly linearly with the number of concurrent
clients and transactions:

![](docs/img/tx-throughput.png)

The graph shows separately the three different types of transactions, where the
bold line is the median number of transactions per second and the error band
includes the 10th and 90th percentiles.

As you can see the median throughput increases linearly (better for reads than
for writes), touching 7k transactions per second with 500 concurrent clients.

To note also the slight performance degradation at the 10th percentile with more
than 30 concurrent DBs. This is due to the increased probability of conflicts
between transactions (e.g. when writers race each other).

See the transaction retries below, hurting performance at the higher
percentiles:

![](docs/img/retries.png)

Since each transaction operates on multiple keys, here is for completeness the
graph of those. Each operation is a key being read or written:


![](docs/img/ops-latency.png)

It's interesting to see that weak reads are losing against strong reads in this
benchmark, for several reasons:

* Given the uniform distribution of reads, it's unlikely that a weak read will
  hit the same key twice within the 10 seconds allowed staleness time frame.
* Weak reads are currently translated into strong reads when the value is not
  present in cache.
* Strong reads operate on two keys in the same transactions, weak reads are a
  "single shot".

Taken all together this means that weak reads in this case translate mostly into
strong reads on a single key. These tend to perform worse (in terms of
throughput) than reading two keys per transactions, because they can be done in
parallel. The advantage of weak reads in this case comes with less sensitivity
to retries, as you can see from the lower variability at higher percentiles.

### Latency

Latency is not Glass DB's forte, but you can see below that it stays mostly flat
as we increase the number of concurrent clients and transactions:

![](docs/img/tx-latency.png)

Here the effects of the retries is more noticeable at higher percentiles, as
expected. Some transactions will start taking longer, as they have to drop their
work and restart after a conflict.

## Deadlocks

Glass DB is currently using a naive approach when it comes to deadlocks. The
only detection mechanism is long timeouts. When a transaction cannot make
progress for several seconds, it releases all the locks and retries by taking
them one by one in a defined order. This is very slow, but ensures that no
further deadlocks can occur.

You can see in the graph below how slow transactions can get when these
situations occur:

![](docs/img/deadlock-latency.png)

In this example, 5 parallel workers keep competing for the same keys (between 1
and 6), with varying degrees of overlap (up to 100%).

As you can see, it's very easy for transactions to deadlock in this situation,
resulting in delays of even 1-2 minutes in the worst case.

This is mostly due to a few factors:

* Timeouts are currently very large (60 seconds).
* Glass DB is not optimized for deadlocks, nor makes sure deadlocked
  transactions can always make progress.
* GCS throttles writes when they happen to the same object multiple times per
  second (some bursting is allowed, but in this case we are consistently trying
  to overwrite the same object).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md) for details.

## License

See [`LICENSE`](LICENSE) for details.

## Disclaimer

This project is not an official Google project. It is not supported by Google
and Google specifically disclaims all warranties as to its quality,
merchantability, or fitness for a particular purpose.

## Footnotes

[^1]: Even though S3
  [is strongly consistent](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel),
  it doesn't support `If-` headers on
  [PUT operations](https://s3.amazonaws.com/doc/s3-developer-guide/RESTObjectPUT.html)
  like it does on [GET](https://s3.amazonaws.com/doc/s3-developer-guide/RESTObjectGET.html).
