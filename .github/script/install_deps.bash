#!/usr/bin/env bash
# Install ALL build, test, docs and packaging dependencies for the Aerospike C
# client on a bare distro base image — in one shot, per distro.
#
# This mirrors the aerospike-server repo's install_deps.bash pattern: a single
# entry point that fully provisions the distro, with no build-vs-package
# bifurcation. After this runs, `make all`, `make test`, `make docs` and
# `make package` all work.
#
# Installed per distro:
#   compiler + autotools : gcc, g++, make, autoconf, automake, libtool, m4
#   link-time dev libs    : openssl(-devel), libyaml(-devel), zlib(-devel)
#   one async event lib   : libev | libuv | libevent  (built from source,
#                           versions mirror the repo's ./install_lib* scripts)
#   docs                  : doxygen (+ graphviz) — built from source where the
#                           distro version is too old (Ubuntu) or absent
#                           (RHEL / Amazon Linux); on RHEL flex+bison are built
#                           from source (absent from all UBI repos)
#   packaging             : zip + rpm-build (RHEL/AL) | dpkg-dev + fakeroot (deb)
#
# Lua is NOT installed: the client bundles it via the modules/lua submodule.
#
# Usage: install_deps.bash <distro> [event_lib]
#
#   distro:    ubuntu-22.04 | ubuntu-24.04 | debian-12 | debian-13 |
#              amazonlinux-2023 | rhel-8 | rhel-9 | rhel-10
#   event_lib: libev (default) | libuv | libevent
set -xeuo pipefail

# Event-library versions — kept in sync with the repo's canonical installers
# (./install_libev, ./install_libuv, ./install_libevent), NOT the older 1.8.0
# pinned inside docker/build*/.
LIBEV_VERSION="4.24"
LIBUV_VERSION="1.15.0"
LIBEVENT_VERSION="2.1.12-stable"

# Doxygen built from source where the distro version is too old / missing.
DOXYGEN_VERSION="1.12.0"

SUDO=
if [[ $(id -u) -ne 0 ]] && command -v sudo >/dev/null; then
    SUDO=sudo
fi

# Compiler + autotools + link-time dev libs, common to a distro family.
DEBIAN_DEPS='build-essential autoconf automake libtool make pkg-config git tar wget ca-certificates libssl-dev libyaml-dev zlib1g-dev'

# `which`, `diffutils`, `file`, `findutils`, `gzip` are needed by autotools
# configure scripts and by `tar xzf` on the minimal RHEL/AL images.
EL_DEPS='gcc gcc-c++ make autoconf automake libtool m4 git tar wget which gzip diffutils file findutils openssl openssl-devel libyaml-devel'

# Extra packages for `make docs` + `make package` (see pkg/package*, Makefile).
DEBIAN_PKG_DEPS='graphviz zip dpkg-dev fakeroot'
EL_PKG_DEPS='graphviz zip rpm-build'

# --- Per-distro install functions (full toolchain, single shot) ---------------------

install_deps_ubuntu_2204() { install_ubuntu_common; }
install_deps_ubuntu_2404() { install_ubuntu_common; }

install_deps_debian_12() { install_debian_common; }
install_deps_debian_13() { install_debian_common; }

install_deps_amazonlinux_2023() {
    # shellcheck disable=SC2086
    $SUDO dnf install -y $EL_DEPS zlib-devel \
        $EL_PKG_DEPS cmake flex bison python3
    $SUDO dnf clean all
    build_doxygen
}

install_deps_rhel_8() {
    # ubi8-minimal: bison/flex are absent from all UBI 8 repos (BaseOS, AppStream,
    # CodeReady Builder) → build both from source before doxygen.
    # shellcheck disable=SC2086
    microdnf install -y $EL_DEPS zlib-devel $EL_PKG_DEPS cmake python3
    microdnf clean all
    build_bison 3.4
    build_flex 2.6.1
    build_doxygen
}

install_deps_rhel_9()  { install_el_minimal 3.7.4 2.6.4; }
install_deps_rhel_10() { install_el_minimal 3.7.4 2.6.4; }

# --- Family helpers -----------------------------------------------------------------

# Debian apt ships a current-enough doxygen → no source build needed.
install_debian_common() {
    $SUDO apt-get update
    # shellcheck disable=SC2086
    $SUDO apt-get install -y --no-install-recommends \
        $DEBIAN_DEPS doxygen $DEBIAN_PKG_DEPS
}

# Ubuntu apt doxygen is too old → build from source (cmake/flex/bison).
install_ubuntu_common() {
    $SUDO apt-get update
    # shellcheck disable=SC2086
    $SUDO apt-get install -y --no-install-recommends \
        $DEBIAN_DEPS $DEBIAN_PKG_DEPS cmake flex bison python3
    build_doxygen
}

# ubi9/ubi10-minimal: zlib-devel is not in microdnf's default repos → pull via dnf.
# bison/flex are absent from all UBI repos → built from source before doxygen.
install_el_minimal() {
    local bison_ver="$1" flex_ver="$2"
    # shellcheck disable=SC2086
    microdnf install -y $EL_DEPS $EL_PKG_DEPS cmake python3
    microdnf install -y dnf
    $SUDO dnf install -y zlib-devel --setopt=install_weak_deps=False --nodocs
    $SUDO dnf clean all
    microdnf clean all
    build_bison "$bison_ver"
    build_flex "$flex_ver"
    build_doxygen
}

run_ldconfig() {
    if command -v ldconfig >/dev/null; then
        $SUDO ldconfig
    fi
}

# --- Event library (one of, from source) --------------------------------------------

build_event_lib() {
    local lib="$1"
    case "$lib" in
    libev)    build_libev ;;
    libuv)    build_libuv ;;
    libevent) build_libevent ;;
    all)
        build_libev
        build_libuv
        build_libevent
        ;;
    *)
        echo "Unsupported event_lib: $lib (expected libev|libuv|libevent|all)" >&2
        exit 1
        ;;
    esac
    run_ldconfig
}

build_libev() {
    local dir="libev-${LIBEV_VERSION}"
    local src
    src="$(mktemp -d)"
    pushd "$src" >/dev/null
    # libev is HTTP-only (no upstream HTTPS).
    wget -q "http://dist.schmorp.de/libev/Attic/${dir}.tar.gz"
    tar xzf "${dir}.tar.gz"
    pushd "$dir" >/dev/null
    ./configure -q
    make -j"$(nproc)" CFLAGS=-w
    $SUDO make install
    popd >/dev/null
    popd >/dev/null
    rm -rf "$src"
}

build_libuv() {
    local dir="libuv-v${LIBUV_VERSION}"
    local src
    src="$(mktemp -d)"
    pushd "$src" >/dev/null
    wget -q "http://dist.libuv.org/dist/v${LIBUV_VERSION}/${dir}.tar.gz"
    tar xzf "${dir}.tar.gz"
    pushd "$dir" >/dev/null
    sh autogen.sh
    ./configure -q
    make -j"$(nproc)" CFLAGS=-w
    $SUDO make install
    popd >/dev/null
    popd >/dev/null
    rm -rf "$src"
}

build_libevent() {
    local dir="libevent-${LIBEVENT_VERSION}"
    local src
    src="$(mktemp -d)"
    pushd "$src" >/dev/null
    wget -q "https://github.com/libevent/libevent/releases/download/release-${LIBEVENT_VERSION}/${dir}.tar.gz"
    tar xzf "${dir}.tar.gz"
    pushd "$dir" >/dev/null
    ./configure -q
    make -j"$(nproc)" CFLAGS=-w
    $SUDO make install
    popd >/dev/null
    popd >/dev/null
    rm -rf "$src"
}

# --- Docs toolchain (built from source where packaged version is unusable) ----------

build_doxygen() {
    local dir="doxygen-${DOXYGEN_VERSION}"
    local tag="Release_${DOXYGEN_VERSION//./_}"
    local src
    src="$(mktemp -d)"
    pushd "$src" >/dev/null
    wget -q "https://github.com/doxygen/doxygen/releases/download/${tag}/${dir}.src.tar.gz"
    tar xzf "${dir}.src.tar.gz"
    cmake -S "$dir" -B "$dir/build" -G "Unix Makefiles"
    cmake --build "$dir/build" -j"$(nproc)"
    $SUDO cmake --install "$dir/build"
    popd >/dev/null
    rm -rf "$src"
}

# bison/flex are absent from all UBI repos → built from source on all RHEL variants.
build_bison() {
    local ver="$1"
    local src
    src="$(mktemp -d)"
    pushd "$src" >/dev/null
    wget -q "https://ftp.gnu.org/gnu/bison/bison-${ver}.tar.gz"
    tar xzf "bison-${ver}.tar.gz"
    pushd "bison-${ver}" >/dev/null
    ./configure --prefix=/usr/local
    make MAKEINFO=true
    $SUDO make install
    popd >/dev/null
    popd >/dev/null
    rm -rf "$src"
}

build_flex() {
    local ver="$1"
    local src
    src="$(mktemp -d)"
    pushd "$src" >/dev/null
    wget -q "https://github.com/westes/flex/releases/download/v${ver}/flex-${ver}.tar.gz"
    tar xzf "flex-${ver}.tar.gz"
    pushd "flex-${ver}" >/dev/null
    ./configure --prefix=/usr/local
    make -j"$(nproc)"
    $SUDO make install
    popd >/dev/null
    popd >/dev/null
    rm -rf "$src"
}

# --- Main ---------------------------------------------------------------------------

main() {
    if [[ $# -lt 1 || $# -gt 2 ]]; then
        echo "Usage: install_deps.bash <distro> [event_lib]" >&2
        exit 1
    fi

    local distro="$1"
    local event_lib="${2:-sync}"
    export DEBIAN_FRONTEND=noninteractive

    case "$distro" in
    ubuntu-22.04)     install_deps_ubuntu_2204 ;;
    ubuntu-24.04)     install_deps_ubuntu_2404 ;;
    debian-12)        install_deps_debian_12 ;;
    debian-13)        install_deps_debian_13 ;;
    amazonlinux-2023) install_deps_amazonlinux_2023 ;;
    rhel-8)           install_deps_rhel_8 ;;
    rhel-9)           install_deps_rhel_9 ;;
    rhel-10)          install_deps_rhel_10 ;;
    *)
        echo "Unsupported distro: $distro" >&2
        exit 1
        ;;
    esac

    if [[ "$event_lib" != "sync" ]]; then
        build_event_lib "$event_lib"
    fi

    echo "Dependencies installed for $distro (event_lib=$event_lib)."
}

main "$@"

#ec