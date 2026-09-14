# Aerospike Client Examples

This directory contains the standalone example binaries for the Aerospike C
client. The authoritative runnable registry is stored in
`examples/manifest/examples.json`.

## Build

Build the client library first:

```sh
make
```

Build all examples:

```sh
make -C examples [EVENT_LIB=libev|libuv|libevent]
```

Build one example directly from its leaf directory:

```sh
make -C examples/query_examples/projection [EVENT_LIB=libev|libuv|libevent]
```

If you use async examples, the chosen `EVENT_LIB` must match the client build.
Some platforms also need:

```sh
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/usr/local/lib"
```

This example demonstrates string expression builders using expression read
operations. It follows the Java client's StringExpression example and shows
that modify-style string expressions return transformed values without mutating
the stored bin.


### touch

	aerospike_key_operate()
	aerospike_key_put()

This example writes a record to the database with its TTL set, and reads it back
to show the TTL. It then demonstrates aerospike_key_operate() with a touch
operation, to reset the TTL to a different value. Finally it reads the record
again to show that the TTL value was updated.


### udf (3.0 feature)

	aerospike_key_apply()
	aerospike_key_put()

This example demonstrates the application of UDFs (user defined functions) to a
record in the database. It registers a UDF package and writes a record in the
database. Using aerospike_key_apply() it applies a simple UDF with no arguments
or return value, to perform an arithmetic operation on one of the bin values,
then reads the record back to show the effect. It then applies a UDF with
arguments and return value to perform another arithmetic operation on one of the
bin values, and return the integer result. It again reads the record back to
show the effect.


## Batch Examples

These examples each use multiple records to demonstrate particular API calls.


### get

	aerospike_batch_exists()
	aerospike_batch_get()

This example uses aerospike_batch_exists() to check whether a bunch of records
written to the database exist, and get their metadata (generation and TTL). It
uses aerospike_batch_get() to read all these records. It then deletes a few of
the records from the database and repeats the batch calls, showing that the
calls report the deleted records as not found, while returning all the remaining
records as before.


## Query Examples

These examples each use multiple records to demonstrate particular API calls.


### aggregate (3.0 feature)

	aerospike_index_create()
	as_query_where()
	as_query_apply()
	aerospike_query_foreach()

This example uses aerospike_index_create() to create a numeric secondary
index for a particular bin name, then using aerospike_query_foreach() it
demonstrates an aggregation query - a query which finds all records satisfying a
'where' clause (where the bin's value falls in a specified range), applies a
simple UDF (which just returns the bin's value) then aggregates these results
using a second UDF (that sums the values) before making a callback to report the
final result. The example then performs another query that accomplishes the same
task, but using different UDF internals that do an 'aggregate reduce' instead of
a 'map reduce'. The example then repeats the 'aggregate reduce' query with a UDF
filter applied before aggregation. The filter selects even numbers, showing off
filtering functionality that can't be achieved with a 'where' clause. Finally
the example performs a complex aggregation on a different (string value) bin to
show a case where the aggregation process and the value returned involve a more
complex object - a Map - rather than a simple integer.


### simple (3.0 feature)

	aerospike_index_create()
	as_query_where()
	aerospike_query_foreach()

This example uses aerospike_index_create() to create a numeric secondary
index for a particular bin name, then using aerospike_query_foreach() it
demonstrates a simple query which finds all records satisfying a 'where' clause
(where the bin's value equals a specified value) and makes a callback for each
such record, in this case a single record.


### topk

	as_query_order_by()
	as_query_top_k()
	aerospike_query_foreach()

This example demonstrates Top-K queries: `ORDER BY <bin> LIMIT k`. It writes a
batch of records with an integer "score" bin and a string "name" bin, then runs
two foreground queries - one ordering by score in descending order, and one
ordering by name in ascending, case-insensitive order - each limited to the top
5 results, showing that the callback receives records already fully sorted and
truncated by the client/server, without the caller doing any of its own sorting.


### min_max

	aerospike_query_min()
	aerospike_query_max()

This example demonstrates MIN/MAX aggregation. It writes a batch of records
with an integer "score" bin holding distinct values, then calls
aerospike_query_min() and aerospike_query_max() to find the minimum and
maximum score across every record in the set. These calls hand back just the
winning bin's value (an as_val), not a whole record.


### vector_knn

	as_vector_value_new_float32()
	as_record_set_vector()
	as_exp_vector_dist()
	as_operations_exp_read()
	as_query_order_by()
	as_query_top_k()

This example demonstrates end-to-end vector K-nearest-neighbor search, expressed
as `ORDER BY <distance-expression> LIMIT k`. It writes records each holding a
native VECTOR bin, then runs foreground queries that attach a read-expression op
computing a per-record distance to a fixed query vector - `euclideanDistance`
(ordered ascending: nearest first) and `dotProduct` (ordered descending) - and
returns only the k best, fully ranked. KNN reuses the same Top-K machinery that
orders by any scalar bin.


## Scan Examples

These examples each use multiple records to demonstrate particular API calls.


### background (3.0 feature)

	as_scan_apply_each()
	aerospike_scan_background()
	aerospike_scan_info()

This example uses aerospike_scan_background() to start a background scan of the
database, which applies a UDF that performs a simple arithmetic operation on a
bin's value, for every record. It then uses aerospike_scan_info() to poll and
detect when the scan is complete. Finally, it reads back all the records to show
the effect of the scan.


### standard

	aerospike_scan_foreach()

This example uses aerospike_scan_background() to do a standard foreground scan
of the database, where a callback is made for each record found.


## Async Examples

These examples demonstrate particular asynchronous API calls.

Keep using the per-example leaf workflow if you want:

```sh
make -C examples/basic_examples/get run AS_HOST=127.0.0.1 AS_PORT=3000
```

The top-level orchestrator runs `all`, groups, ids, or tagged subsets and
emits local JUnit XML:

```sh
examples/run_examples all --host 127.0.0.1 --port 3000
examples/run_examples query --namespace test --set demo1
examples/run_examples async --event-lib libuv
examples/run_examples --validate-registry --validation-only
```

The runner auto-probes server version, edition, namespace TTL support, and
namespace strong-consistency facts before evaluating skips. Manual
`--server-version`, `--enterprise`/`--community`,
`--strong-consistency`/`--no-strong-consistency`, and
`--ttl-support`/`--no-ttl-support` flags remain available as overrides when
probing is unavailable.

PR CI uses this runner for a dedicated examples check with a `libev` build on
an explicit examples server defined in `.github/pr_examples_server.json`. That
server currently points at an EE image and a checked-in SC namespace config in
`.github/aerospike/examples-ee-sc.conf`. The check fails on real example
failures or registry drift, while version-gated examples may still be reported
as skipped when the configured server does not satisfy their manifest
requirements.

Pre-launch skip handling currently covers:

- `event_lib`
- `ttl_support`
- `enterprise`
- `strong_consistency`
- `min_server_version`

These manifest requirements remain example-managed setup rather than runner
preflight gates:

- `udf`
- `secondary_index`

## Manifest Registry

<!-- examples-manifest:start -->
- `async.batch_get`
- `async.delay_queue`
- `async.get`
- `async.query`
- `async.scan`
- `async.transaction`
- `basic.append`
- `basic.connect`
- `basic.expire`
- `basic.generation`
- `basic.get`
- `basic.incr`
- `basic.list`
- `basic.map`
- `basic.put`
- `basic.string`
- `basic.string_expression`
- `basic.touch`
- `basic.transaction`
- `basic.udf`
- `batch.get`
- `geospatial.filter`
- `geospatial.simple`
- `query.aggregate`
- `query.projection`
- `query.simple`
- `scan.background`
- `scan.projection`
- `scan.standard`
<!-- examples-manifest:end -->
