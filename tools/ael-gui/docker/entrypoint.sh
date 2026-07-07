#!/bin/bash
# ael-gui container entrypoint.
#
# Default: start the image's own asd and point the GUI at it.
# Set AS_HOST (and optionally AS_PORT/AS_NS/AS_SET) to skip the internal
# server and connect to an existing cluster instead.

set -euo pipefail

NS="${AS_NS:-test}"
SET="${AS_SET:-ael-gui}"
PORT="${AS_PORT:-3000}"

if [ -z "${AS_HOST:-}" ]; then
    echo "ael-gui: starting internal asd"
    /opt/aerospike/bin/asd \
            --config-file /opt/aerospike/etc/aerospike.conf &
    AS_HOST=127.0.0.1
    PORT=3000
fi

echo "ael-gui: waiting for aerospike at ${AS_HOST}:${PORT}"

ready=
for _ in $(seq 1 60); do
    if timeout 1 bash -c \
            "cat < /dev/null > /dev/tcp/${AS_HOST}/${PORT}" 2>/dev/null; then
        ready=1
        break
    fi
    sleep 1
done

if [ -z "$ready" ]; then
    echo "ael-gui: ${AS_HOST}:${PORT} not reachable after 60s" >&2
    exit 1
fi

# The service port answers slightly before the node is fully ready to take
# writes - give it a beat before the GUI writes its sample record.
sleep 2

echo "ael-gui: open http://localhost:8280/"

exec /opt/aerospike/bin/ael-gui -h "${AS_HOST}" -p "${PORT}" -n "${NS}" \
        -s "${SET}" -b 0.0.0.0 -l 8280 -u /opt/aerospike/ui.html
