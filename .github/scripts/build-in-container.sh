#!/usr/bin/env bash
# Build the C client inside a docker container that matches the target distro.
#
# Why a container per distro:
#   - pkg/platform inspects /etc/os-release to name packages (el8, ubuntu24.04, ...).
#   - pkg/package_type invokes the target distro's tooling: rpmbuild for el/fc/amzn,
#     dpkg-deb + fakeroot for debian/ubuntu. Those tools must run inside the matching
#     userland to produce correctly-tagged packages.
#
# Event libraries (libuv, libev, libevent) are built from source via the repo's own
# install_* scripts. That keeps versions identical across distros and avoids the
# EPEL/extra-repo dance for libev on RHEL-family distros.
#
# Usage: build-in-container.sh <distro> <arch>
# Output: target/packages/ in the current working directory (mounted into the container).

set -euo pipefail

DISTRO="${1:?missing distro}"
ARCH="${2:?missing arch}"

WORKDIR="$(pwd)"

case "$DISTRO" in
  el8)         IMAGE="rockylinux:8"     ; FAMILY="rpm" ;;
  el9)         IMAGE="rockylinux:9"     ; FAMILY="rpm" ;;
  amzn2023)    IMAGE="amazonlinux:2023" ; FAMILY="rpm" ;;
  ubuntu22.04) IMAGE="ubuntu:22.04"     ; FAMILY="deb" ;;
  ubuntu24.04) IMAGE="ubuntu:24.04"     ; FAMILY="deb" ;;
  debian12)    IMAGE="debian:12"        ; FAMILY="deb" ;;
  debian13)    IMAGE="debian:13"        ; FAMILY="deb" ;;
  *)
    echo "Unknown distro: $DISTRO" >&2
    exit 1
    ;;
esac

case "$ARCH" in
  x86_64)  PLATFORM="linux/amd64" ;;
  aarch64) PLATFORM="linux/arm64" ;;
  *)
    echo "Unknown arch: $ARCH" >&2
    exit 1
    ;;
esac

# Toolchain install command per family. The install_* scripts use `sudo`; inside
# the container we run as root, so a sudo shim is installed if /usr/bin/sudo is missing.
read -r -d '' INSTALL_RPM <<'INNER' || true
set -euxo pipefail
PKG=dnf
command -v $PKG >/dev/null 2>&1 || PKG=yum

# doxygen lives in an optional repo on RHEL-family distros:
#   - EL9 / Rocky 9: CRB (CodeReady Builder)
#   - EL8 / Rocky 8: PowerTools
#   - Amazon Linux 2023: neither needed; doxygen is in the main repo
# Enable whichever applies; ignore the one that does not exist on this distro.
$PKG -y install dnf-plugins-core
$PKG config-manager --set-enabled crb        2>/dev/null || true
$PKG config-manager --set-enabled powertools 2>/dev/null || true

$PKG -y install \
  gcc make autoconf automake libtool \
  openssl-devel \
  zlib-devel \
  rpm-build \
  doxygen \
  git zip wget tar findutils which

# install_libuv runs autogen.sh which needs autoconf/automake (covered above).
command -v sudo >/dev/null 2>&1 || {
  printf '#!/bin/sh\nexec "$@"\n' > /usr/local/bin/sudo
  chmod +x /usr/local/bin/sudo
}
INNER

read -r -d '' INSTALL_DEB <<'INNER' || true
set -euxo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y --no-install-recommends \
  build-essential autoconf automake libtool \
  libssl-dev \
  zlib1g-dev \
  fakeroot dpkg-dev \
  doxygen \
  git zip wget ca-certificates lsb-release
command -v sudo >/dev/null 2>&1 || {
  printf '#!/bin/sh\nexec "$@"\n' > /usr/local/bin/sudo
  chmod +x /usr/local/bin/sudo
}
INNER

case "$FAMILY" in
  rpm) INSTALL_CMD="$INSTALL_RPM" ;;
  deb) INSTALL_CMD="$INSTALL_DEB" ;;
esac

echo "Pulling $IMAGE ($PLATFORM)..."
docker pull --platform "$PLATFORM" "$IMAGE"

echo "Building in container..."
docker run --rm \
  --platform "$PLATFORM" \
  -v "$WORKDIR:/work" \
  -w /work \
  -e DISTRO="$DISTRO" \
  -e ARCH="$ARCH" \
  "$IMAGE" \
  bash -c "
    $INSTALL_CMD

    # git refuses to operate on a tree it considers untrusted (uid mismatch from the
    # host mount). pkg/version calls 'git describe', so this is required.
    git config --global --add safe.directory /work
    git config --global --add safe.directory '*'

    # Build event libraries from source (identical versions across distros).
    ./install_libuv
    ./install_libev
    ./install_libevent
    ldconfig 2>/dev/null || true

    # pkg/package does the full multi-event-lib loop and produces target/packages/.
    make clean
    make package
  "

echo "Done. Contents of target/packages/:"
ls -la target/packages/
