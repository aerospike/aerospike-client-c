#!/usr/bin/env bash
# Package Linux release artifacts inside a pre-built Aerospike JFrog build container.
#
# Required environment (injected via build-env in build-artifacts.yml):
#   DISTRO    — e.g. ubuntu-22.04
#   CONTAINER — full image ref, e.g.
#               aerospike.jfrog.io/database-docker-virtual/client-c-build-ubuntu-22.04:ubuntu-22.04
#   ARCH      — amd64 | arm64 (docker --platform)
#   OWN_SOURCE — 1 on ubuntu-22.04/amd64 only (keeps the source zip); 0 elsewhere
#
# install_deps.bash is not invoked here. The JFrog build container images
# (client-c-build-<distro>, built via aerospike-asconfig / .build.yml) already
# provision the toolchain and event libs using install_deps.bash at image build time.
# See .github/script/install_deps.bash.

set -euo pipefail

: "${DISTRO:?DISTRO is required}"
: "${CONTAINER:?CONTAINER is required}"
ARCH="${ARCH:-amd64}"
OWN_SOURCE="${OWN_SOURCE:-0}"

# JFrog CLI is configured by reusable_execute-build before this script runs.
registry_host="${CONTAINER%%/*}"
if command -v jf >/dev/null 2>&1; then
  jf docker login "$registry_host" --interactive=false
fi

docker run --rm \
  --platform "linux/${ARCH}" \
  -e DISTRO="${DISTRO}" \
  -e OWN_SOURCE="${OWN_SOURCE}" \
  -v "$PWD:/workspace" \
  -w /workspace \
  "${CONTAINER}" \
  bash -c '
    set -euo pipefail
    git config --global --add safe.directory /workspace
    make -j"$(nproc)" package
    if [ "${OWN_SOURCE:-0}" != "1" ]; then
      rm -f target/packages/aerospike-client-c-src-*.zip
    fi
  '
