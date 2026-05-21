#!/usr/bin/env bash

set -ue

echo --------------------------------------------------------------------------------
echo WHERE AM I?
echo --------------------------------------------------------------------------------
pwd
echo USER UID: $(id -u)
echo USER GID: $(id -g)
echo ................................................................................
ls -la

### DEBUG ###
echo "=== container: userns + capability state ==="
echo "id: $(id)"
echo "umask: $(umask)"
echo "/proc/self/uid_map:"; cat /proc/self/uid_map || true
echo "/proc/self/gid_map:"; cat /proc/self/gid_map || true
echo "capabilities (from /proc/self/status):"
grep -E '^Cap' /proc/self/status || true
echo "mount info for /artifacts:"
findmnt /artifacts || mount | grep artifacts || true
echo "ls -lan /artifacts (numeric UIDs):"
ls -lan /artifacts | head -25
echo "stat /artifacts/src/include/aerospike/as_error.h:"
stat /artifacts/src/include/aerospike/as_error.h 2>&1 || true
echo "touch+chmod self-test in /artifacts/stage-probe:"
mkdir -p /artifacts/stage-probe && touch /artifacts/stage-probe/probe.h && \
  ls -lan /artifacts/stage-probe/probe.h && \
  (chmod 0644 /artifacts/stage-probe/probe.h && echo "  chmod on freshly-created OK") \
  || echo "  chmod on freshly-created FAILED"
echo "=== end container debug ==="
### END DEBUG ###

echo --------------------------------------------------------------------------------
echo install_libuv - preinstalled from Docker image
echo --------------------------------------------------------------------------------
#./install_libuv

echo --------------------------------------------------------------------------------
echo install_libev - preinstalled from Docker image
echo --------------------------------------------------------------------------------
#./install_libev

echo --------------------------------------------------------------------------------
echo install_libevent - preinstalled from Docker image
echo --------------------------------------------------------------------------------
#./install_libevent

echo --------------------------------------------------------------------------------
echo make clean
echo --------------------------------------------------------------------------------
make clean

echo --------------------------------------------------------------------------------
echo make
echo --------------------------------------------------------------------------------
make

echo --------------------------------------------------------------------------------
echo make install - skipping for now
echo --------------------------------------------------------------------------------
#make install

echo --------------------------------------------------------------------------------
echo make package
echo --------------------------------------------------------------------------------
make package

echo --------------------------------------------------------------------------------
echo Done with container-build-script-el.sh
echo --------------------------------------------------------------------------------
# Artifacts are written directly into the bind-mounted workspace at
# target/packages, so the host workflow can pick them up via
# gh-artifact-directory: target/packages.  Nothing more to copy.
