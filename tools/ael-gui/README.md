# ael-gui — AEL expression + error-details playground

A local browser GUI for experimenting with AEL expressions against a live
server and inspecting the **error details** (field 45) the server returns:
status, subcode, detail message, and the full expression trace — snippet
with `»«` focus marks, op, path, outcome, operands, and the AEL source
span, which the UI highlights directly in your submitted expression.

One C binary that connects to the cluster and serves the UI on
`http://127.0.0.1:8280`. The stock client only folds subcode/message into
`as_error` and skips the trace; this tool issues the read/operate wire
commands itself (same exported `as_command_*` helpers `aerospike_key.c`
uses) with a parse callback that captures the raw field-45 payload and
decodes all of it.

Local dev tool — not part of any build or CI.

## What you need

The fastest path is the **Docker quickstart** below — it needs nothing but
docker and GitHub access. To build and run natively instead:

- **This branch** (`dylan/ael-gui`). It is based on
  `dylan/return-error-messages`, which carries the error-details client
  support (verbosity policy, field-45 decode) the tool builds against.
- **A server built from the error-details branch**:
  `citrusleaf/aerospike-server` branch `dylan/SERVER-1137-exp-build-trace`
  (paired EE branch of the same name; the branch includes AEL via the dsl
  merge). Build and run it however you normally run a dev `asd` — the tool
  just needs a reachable host/port with a `test` namespace (override with
  `-n`). See PRs citrusleaf/aerospike-server#1436 and
  citrusleaf/aerospike-server-enterprise#530 for what the server side does.

## Docker quickstart (no local toolchain needed)

The `docker/` directory holds a self-contained image recipe: it builds the
server (CE) and client from their branches plus the tool, and runs both
`asd` and the GUI in one container. You need docker (BuildKit) and an
ssh-agent holding a GitHub key that can read `citrusleaf/aerospike-server`
— the key is used only to clone during the build and never enters the
image.

```bash
# Build straight from GitHub - no clone needed (10-15 min first time):
docker build --ssh default -f docker/Dockerfile -t ael-gui \
    'https://github.com/aerospike/aerospike-client-c.git#dylan/ael-gui:tools/ael-gui'

# Run self-contained (internal single-node server):
docker run --rm --init -p 127.0.0.1:8280:8280 ael-gui
# then open http://localhost:8280/
```

To point the GUI at an **existing cluster** instead of the internal one,
set `AS_HOST` (and optionally `AS_PORT`, `AS_NS`, `AS_SET`) — the internal
server is then skipped entirely:

```bash
docker run --rm --init -p 127.0.0.1:8280:8280 \
    -e AS_HOST=172.17.0.4 -e AS_PORT=3000 ael-gui
```

The cluster must be reachable *from the container* and be running an
error-details-branch asd: docker-bridge clusters (e.g. aerolab) work as-is;
for a cluster on the host itself use `--network host` (then drop `-p`).
When the branches move, re-clone only what changed by passing the tips as
cache-bust args (falling back to `--no-cache` also works, but rebuilds
everything):

```bash
docker build --ssh default -f docker/Dockerfile -t ael-gui \
    --build-arg CLIENT_REV=$(git ls-remote https://github.com/aerospike/aerospike-client-c.git dylan/ael-gui | cut -f1) \
    --build-arg SERVER_REV=$(git ls-remote git@github.com:citrusleaf/aerospike-server.git dylan/SERVER-1137-exp-build-trace | cut -f1) \
    'https://github.com/aerospike/aerospike-client-c.git#dylan/ael-gui:tools/ael-gui'
```

From a local checkout, build with context `tools/ael-gui` — local edits to
`ael_gui.c`/`ui.html` are included without pushing:

```bash
cd tools/ael-gui && docker build --ssh default -f docker/Dockerfile -t ael-gui .
```

## Build & run

```bash
# 1. Build the client lib (once, from the repo root):
cd ../.. && make && cd tools/ael-gui

# 2. Build the tool:
make

# 3. Run against your cluster (defaults: 127.0.0.1:3000, ns test, set ael-gui):
./target/ael-gui -h <asd-host> -p 3000 -n test
# then open http://127.0.0.1:8280/

# or: make run AS_HOST=<asd-host>
```

Options: `-h host -p port -n namespace -s set -l http-port -u ui.html`.
`ui.html` is read from disk per request, so UI tweaks are just
edit-and-refresh — no rebuild.

## What it does

- Writes a **sample record** (key `sample`) with bins `x` int, `y` double,
  `name` string, `flag` bool, `xs` list, `m` map, `blob` bytes — reference
  them in AEL as `$.x`, `$.name`, … The record panel shows live contents;
  "Reset record" rewrites it.
- **Filter read** mode sends the expression as `filter_exp` on a get of the
  sample record: a passing filter returns the record; FALSE/absent-bin
  gives `FILTERED_OUT` with the explain trace at verbosity 3; parse errors
  give the build trace; eval faults the runtime trace.
- **Read value** mode sends it as an expression-read operation and shows
  the expression's value in bin `result` (with an `EVAL_NO_FAIL` toggle).
- **Verbosity 0–3** maps to the policy `error_detail_verbosity`
  (info4 bits): off / subcode / + message / + trace & explain.
- The Examples menu seeds one expression per interesting code path.

## Version-skew caveat

The server side is **in review** and the field-45 trace key numbering can
still move. If traces start rendering oddly (unknown keys show up as
`key_N` in the raw view), rebuild both sides from the current tips of the
two branches. The trace sub-key list lives in `as/include/base/proto.h`
(`AS_EXP_TRACE_KEY_*`) on the server branch; mirror any change into the
`TRACE_KEY_*` defines in `ael_gui.c`.

## Wire notes (for future maintenance)

- AEL rides as the hand-packed msgpack envelope
  `[EXP_AEL_COMPILE(=128), <source as bin>]` wrapped in a raw client
  `as_exp {packed_sz, packed[]}` — same as the EE tests' `pack_ael()`.
- Field-45 map keys: 1 subcode (absent when `AS_SUB_NONE`), 2 message,
  3 trace. Trace sub-keys: 1 phase, 2 byte_offset, 3 op, 4 depth, 5 path,
  6 snippet, 7 outcome, 8 lang, 9 ael_offset, 10 ael_span, 13 operands.

## probe.py — trace-invariant battery

`./probe.py` drives `/eval` with a 33-case battery and mechanically checks
trace invariants (span↔snippet agreement, depth==|path|, operand gating,
outcome↔message, utf-8 span sanity) plus per-case expectations. Run it after
any change to the trace machinery; `-e '<ael>'` probes a single expression,
`-v` prints passing traces too, `-b` targets a non-default GUI port. The
same cases live in the UI's examples dropdown under "Trace-machinery
cases".
