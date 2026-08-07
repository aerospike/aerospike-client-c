# Aerospike Client Examples

This directory contains the standalone example binaries for the Aerospike C
client. The authoritative runnable inventory now lives in
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

## Run

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
examples/run_examples --validate-inventory --validation-only
```

The runner auto-probes server version, edition, namespace TTL support, and
namespace strong-consistency facts before evaluating skips. Manual
`--server-version`, `--enterprise`/`--community`,
`--strong-consistency`/`--no-strong-consistency`, and
`--ttl-support`/`--no-ttl-support` flags remain available as overrides when
probing is unavailable.

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

## Manifest Inventory

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
