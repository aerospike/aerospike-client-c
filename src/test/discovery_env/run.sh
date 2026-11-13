#!/usr/bin/env bash
# Build the client and run src/test/discovery_env against a running aerospike-client-conformance
# environment: run.sh dns|collision, with the environment's values file loaded into the
# environment. This is the command of conformance test-envs.yaml, which checks out this
# repository without submodules on ubuntu-24.04. Dependencies are installed only under GitHub
# Actions; a local run uses the installed toolchain. DISCOVERY_ENV_WRAPPER, when set, is put in
# front of the binary (for example valgrind). Exits with the binary's status.
set -euo pipefail

env_name="${1:-}"

if [ "$env_name" != dns ] && [ "$env_name" != collision ]; then
	echo "usage: $0 dns|collision" >&2
	exit 2
fi

if [ "${ENV_NAME:-}" != "$env_name" ]; then
	echo "$0: ENV_NAME is '${ENV_NAME:-}', not $env_name; load the $env_name environment's values file" >&2
	exit 2
fi

cd "$(dirname "$0")/../../.."

if [ "${GITHUB_ACTIONS:-}" = true ]; then
	sudo bash .github/script/install_deps.bash ubuntu-24.04 sync false false
fi

git submodule update --init --recursive
make -j"$(nproc)"
make discovery-env

status=0
# shellcheck disable=SC2086 # the wrapper is a command line
${DISCOVERY_ENV_WRAPPER:-} "target/$(uname -s)-$(uname -m)/test/discovery_env" || status=$?
echo "discovery_env $env_name exit=$status"
exit "$status"
