#!/usr/bin/env bash
# Build the client and run src/test/discovery_env against a running aerospike-client-conformance
# environment: run.sh dns|collision|tls-matching [n17], with the environment's values file loaded
# into the environment. This is the command of conformance test-envs.yaml, which checks out this
# repository without submodules on ubuntu-24.04. Dependencies are installed only under GitHub
# Actions; a local run uses the installed toolchain. DISCOVERY_ENV_WRAPPER, when set, is put in
# front of the binary (for example valgrind).
#
# dns and collision build with EVENT_LIB=sync, tls-matching with libev; an existing
# libaerospike.a of the other flavour stops the run (make does not rebuild it on EVENT_LIB).
# tls-matching also smokes the L5 driver's TLS flags against the proxy cluster. tls-matching n17
# runs only N-17, which fails until the next major release (FR-45): its failure is labelled
# N17 EXPECTED-RED, with a warning annotation and exit 0, when discovery_env printed the FR-45
# marker, and N17 BROKEN otherwise. Exits non-zero when discovery_env or the driver smoke failed,
# except for N17 EXPECTED-RED.
set -euo pipefail

env_name="${1:-}"
suite="${2:-}"

usage() {
	echo "usage: $0 dns|collision|tls-matching [n17]" >&2
	exit 2
}

case "$env_name" in
	dns | collision) event_lib=sync ;;
	tls-matching) event_lib=libev ;;
	*) usage ;;
esac

if [ -n "$suite" ] && { [ "$env_name" != tls-matching ] || [ "$suite" != n17 ]; }; then
	usage
fi
[ $# -le 2 ] || usage

if [ "${ENV_NAME:-}" != "$env_name" ]; then
	echo "$0: ENV_NAME is '${ENV_NAME:-}', not $env_name; load the $env_name environment's values file" >&2
	exit 2
fi

cd "$(dirname "$0")/../../.."
target="target/$(uname -s)-$(uname -m)"
library="$target/lib/libaerospike.a"

if [ -e "$library" ]; then
	# Not grep -q: under pipefail, nm killed by SIGPIPE would read as no match.
	undefined="$(nm "$library" 2>/dev/null | grep -E ' U (ev|uv|event)_' || true)"
	if grep -q ' U ev_' <<< "$undefined"; then
		built=libev
	elif grep -q ' U uv_' <<< "$undefined"; then
		built=libuv
	elif grep -q ' U event_' <<< "$undefined"; then
		built=libevent
	else
		built=sync
	fi
	if [ "$built" != "$event_lib" ]; then
		echo "$0: $library is the other event library ($built, $env_name needs $event_lib); run make clean" >&2
		exit 2
	fi
fi

if [ "${GITHUB_ACTIONS:-}" = true ]; then
	sudo bash .github/script/install_deps.bash ubuntu-24.04 "$event_lib" false false
fi

git submodule update --init --recursive
make -j"$(nproc)" EVENT_LIB="$event_lib"
make EVENT_LIB="$event_lib" discovery-env l5-driver

log="$target/discovery_env-$env_name${suite:+-$suite}.log"
export DISCOVERY_ENV_SUITE="$suite"
status=0
# shellcheck disable=SC2086 # the wrapper is a command line
${DISCOVERY_ENV_WRAPPER:-} "$target/test/discovery_env" 2>&1 | tee "$log" || status=$?
echo "discovery_env $env_name${suite:+ $suite} exit=$status"

if [ "$suite" = n17 ]; then
	if [ "$status" -eq 0 ]; then
		echo "N17 GREEN"
		if [ "${GITHUB_ACTIONS:-}" = true ]; then
			echo "::notice title=N-17 now passes::CN-only certificates are rejected; drop the EXPECTED-RED handling in run.sh"
		fi
	else
		if grep -q '^ENV-N17-RESULT accepted-through-legacy' "$log"; then
			echo "N17 EXPECTED-RED"
			if [ "${GITHUB_ACTIONS:-}" = true ]; then
				echo "::warning title=N-17 CN-only certificate (FR-45)::C keeps CN acceptance, deprecated, until the next major release"
			fi
			status=0
		else
			echo "N17 BROKEN"
			if [ "${GITHUB_ACTIONS:-}" = true ]; then
				echo "::error title=N-17 job broken::discovery_env failed for another reason than the FR-45 CN acceptance; see the log"
			fi
		fi
	fi
	exit "$status"
fi

if [ "$env_name" != tls-matching ]; then
	exit "$status"
fi

# The driver over TLS through the proxy's forwarders: once a snapshot holds both nodes at their
# forwarders with every partition owned, a served reply for every partition; then exit 3 with the
# certificate mismatch logged when the tls-name is the dialed host.
driver="$target/test/l5_driver"
port_map="$PROXY_NODE_A_NAME=$PROXY_NODE_A_HOST:$PROXY_NODE_A_PORT,$PROXY_NODE_B_NAME=$PROXY_NODE_B_HOST:$PROXY_NODE_B_PORT"
driver_args=(--seed "$PROXY_NODE_A_HOST:$PROXY_NODE_A_PORT" --shape port-map --port-map "$port_map"
	--tls-ca-file "$PROXY_CA" --tend-interval 250 --timeout 1000 --namespace "$ENV_NAMESPACE" --set l5-tls)
smoke=PASS
# The harness reads the driver line by line, so only timeout bounds a driver that goes silent.
driver_bound=120
smoke_bound=150
negative_bound=60
smoke_status=0

timeout -k 10 "$smoke_bound" python3 - "$target/l5_driver-tls.jsonl" "$port_map" -- \
	timeout -k 5 "$driver_bound" "$driver" "${driver_args[@]}" --tls-name "$PROXY_TLS_NAME" \
	2> "$target/l5_driver-tls.err" <<'EOF' || smoke_status=$?
import json, subprocess, sys, time

HOLD_BOUND_S = 30
EXIT_BOUND_S = 60

out_path, port_map = sys.argv[1], sys.argv[2]
command = sys.argv[sys.argv.index("--") + 1:]
expected = {}
for entry in port_map.split(","):
    name, address = entry.split("=", 1)
    host, port = address.rsplit(":", 1)
    expected[name] = (host, int(port))

problems = []
ready = held = served = None
deadline = time.monotonic() + HOLD_BOUND_S
p = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)

def send(line):
    try:
        p.stdin.write(line + "\n")
        p.stdin.flush()
        if line == "quit":
            p.stdin.close()
    except (BrokenPipeError, ValueError):
        pass

with open(out_path, "w") as out:
    for text in p.stdout:
        out.write(text)
        line = json.loads(text)
        kind = line.get("type")
        if kind == "ready":
            ready = line
        elif kind == "snapshot" and held is None:
            nodes = {n["name"]: (n["host"], n["port"]) for n in line.get("nodes", []) if n.get("active")}
            if nodes == expected and line.get("unowned") == 0:
                held = line
                send("serve tls")
        elif kind == "served" and served is None:
            served = line
            send("quit")
        if served is None and time.monotonic() > deadline:
            problems.append("no served reply within %d s; held: %s" % (HOLD_BOUND_S, held is not None))
            send("quit")
            deadline = float("inf")
try:
    status = p.wait(timeout=EXIT_BOUND_S)
except subprocess.TimeoutExpired:
    p.kill()
    status = p.wait()
    problems.append("no exit within %d s of quit" % EXIT_BOUND_S)

print("l5_driver --tls-name exit=%d" % status)
if status == 124:
    problems.append("stopped by its time bound")
elif status != 0:
    problems.append("exit %d" % status)
if not ready or ready.get("contract") != 5:
    problems.append("no ready line with contract 5")
if held is None:
    problems.append("no snapshot held %s with unowned 0" % port_map)
else:
    print("DRIVER-TLS held %s unowned=0" % port_map)
if served is not None:
    tally = {k: served.get(k) for k in ("written", "read", "mismatched", "failures")}
    if tally == {"written": 4096, "read": 4096, "mismatched": 0, "failures": 0}:
        print("DRIVER-TLS served written=4096 read=4096 mismatched=0 failures=0")
    else:
        problems.append("served %s" % json.dumps(served))
for problem in problems:
    print("DRIVER-TLS problem: " + problem)
sys.exit(1 if problems else 0)
EOF
if [ "$smoke_status" -ne 0 ]; then
	if [ "$smoke_status" -eq 124 ]; then
		echo "DRIVER-TLS problem: the smoke did not finish within $smoke_bound s"
	fi
	smoke=FAIL
fi
tail -n 20 "$target/l5_driver-tls.err" >&2 || true

negative_out="$target/l5_driver-tls-dialed.jsonl"
negative_err="$target/l5_driver-tls-dialed.err"
negative_status=0
timeout -k 5 "$negative_bound" "$driver" "${driver_args[@]}" --tls-name "$PROXY_NODE_A_HOST" < /dev/null \
	> "$negative_out" 2> "$negative_err" || negative_status=$?
echo "l5_driver --tls-name $PROXY_NODE_A_HOST exit=$negative_status"

if [ "$negative_status" -eq 124 ]; then
	echo "DRIVER-TLS problem: the dialed host as tls-name did not exit within $negative_bound s"
	smoke=FAIL
elif [ "$negative_status" -ne 3 ] || ! grep -q '"type":"error"' "$negative_out"; then
	echo "DRIVER-TLS problem: the dialed host as tls-name gave exit $negative_status, not 3 after an error line"
	smoke=FAIL
elif ! grep -qF "TLS name '$PROXY_NODE_A_HOST' mismatch" "$negative_err"; then
	echo "DRIVER-TLS problem: exit 3 without the TLS name '$PROXY_NODE_A_HOST' mismatch warning"
	smoke=FAIL
fi
echo "DRIVER-TLS-SMOKE $smoke"

if [ "$smoke" != PASS ] && [ "$status" -eq 0 ]; then
	status=1
fi
exit "$status"
