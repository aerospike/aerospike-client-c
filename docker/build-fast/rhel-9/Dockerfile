# =============================================================================
# RHEL 9 / UBI 9 Minimal — fast multi-stage build environment
#
# Optimised over docker/build-optimized/rhel-9/Dockerfile:
#
#   BEFORE  compile bison (~5 min) + flex (~3 min) + doxygen -j2 (~35 min)
#   AFTER   amd64: download pre-built doxygen binary (seconds) — bison and
#                  flex skipped entirely (only needed for doxygen configure)
#           arm64: bison + flex still compiled from source (not in UBI minimal
#                  repos); doxygen built with capped cores
#   Event libs: parallel wget, sequential -j$(nproc) builds.
# =============================================================================

# ── Stage 1: doxygen (pre-built or from source) + event libs ──────────────────
FROM registry.access.redhat.com/ubi9/ubi-minimal AS deps-builder

ARG LIBEV_SHA256=973593d3479abdf657674a55afe5f78624b0e440614e2b8cb3a07f16d4d7f821

# cmake/python3 included upfront — available in UBI 9 minimal repos and needed
# by arm64 doxygen build. bison/flex are NOT in UBI minimal; compiled in else branch.
RUN microdnf install -y \
        gcc gcc-c++ make autoconf automake libtool m4 \
        cmake python3 wget tar gzip diffutils \
        openssl-devel \
    && microdnf clean all

WORKDIR /build

# doxygen 1.12.0
#   amd64: pre-built binary — seconds instead of ~35 min.
#   arm64: bison + flex compiled from source (not in UBI minimal repos),
#          then doxygen; parallelism capped to avoid local OOM.
RUN set -euo pipefail && \
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
      wget -q "https://github.com/doxygen/doxygen/releases/download/Release_1_12_0/doxygen-1.12.0.linux.bin.tar.gz" \
        -O doxygen.tar.gz \
      && tar xzf doxygen.tar.gz \
      && install -m 755 doxygen-1.12.0/bin/doxygen /usr/local/bin/doxygen \
      && rm -rf doxygen-1.12.0 doxygen.tar.gz; \
    else \
      wget -q https://ftp.gnu.org/gnu/bison/bison-3.7.4.tar.gz \
      && tar xzf bison-3.7.4.tar.gz \
      && cd bison-3.7.4 && ./configure --prefix=/usr/local \
      && make MAKEINFO=true && make install \
      && cd .. && rm -rf bison-3.7.4* \
      && wget -q https://github.com/westes/flex/releases/download/v2.6.4/flex-2.6.4.tar.gz \
      && tar xzf flex-2.6.4.tar.gz \
      && cd flex-2.6.4 && ./configure --prefix=/usr/local \
      && make -j"$(nproc)" && make install \
      && cd .. && rm -rf flex-2.6.4* \
      && wget -q https://github.com/doxygen/doxygen/releases/download/Release_1_12_0/doxygen-1.12.0.src.tar.gz \
      && tar xzf doxygen-1.12.0.src.tar.gz \
      && cmake -S doxygen-1.12.0 -B doxygen-1.12.0/build -G "Unix Makefiles" \
      && cmake --build doxygen-1.12.0/build -j"$(( $(nproc) > 4 ? 4 : $(nproc) ))" \
      && cmake --install doxygen-1.12.0/build \
      && rm -rf doxygen-1.12.0*; \
    fi

# Parallel downloads of all three event-lib tarballs, then sequential builds.
RUN set -euo pipefail && \
    wget -q https://github.com/libuv/libuv/archive/v1.8.0.tar.gz \
         -O libuv.tar.gz & \
    wget -q http://dist.schmorp.de/libev/Attic/libev-4.24.tar.gz \
         -O libev.tar.gz & \
    wget -q https://github.com/libevent/libevent/archive/refs/tags/release-2.1.12-stable.tar.gz \
         -O libevent.tar.gz & \
    wait && \
    echo "${LIBEV_SHA256}  libev.tar.gz" | sha256sum -c - && \
    tar xzf libuv.tar.gz && tar xzf libev.tar.gz && tar xzf libevent.tar.gz

# libuv 1.8.0
RUN cd libuv-1.8.0 && sh autogen.sh && ./configure \
    && make -j"$(nproc)" && make install \
    && cd .. && rm -rf libuv-1.8.0 libuv.tar.gz

# libev 4.24
RUN cd libev-4.24 && sh autogen.sh && ./configure \
    && make -j"$(nproc)" && make install \
    && cd .. && rm -rf libev-4.24 libev.tar.gz

# libevent 2.1.12
RUN cd libevent-release-2.1.12-stable && sh autogen.sh && ./configure \
    && make -j"$(nproc)" && make install \
    && cd .. && rm -rf libevent-release-2.1.12-stable libevent.tar.gz


# ── Stage 2: final build environment ──────────────────────────────────────────
FROM registry.access.redhat.com/ubi9/ubi-minimal

ARG INSTALL_DEBUG_TOOLS=false

ENV PKG=rpm \
    PATH=$PATH:/work/bin

RUN microdnf install -y \
        git tar wget which \
        gcc gcc-c++ make autoconf automake libtool m4 \
        binutils glibc-devel glibc-headers kernel-headers \
        gettext diffutils patch file \
        libyaml-devel openssl openssl-devel \
        rpm-build \
        graphviz \
    && microdnf clean all

RUN if [ "$INSTALL_DEBUG_TOOLS" = "true" ]; then \
        microdnf install -y gdb strace && microdnf clean all; \
    fi

# ubi9-minimal does not ship zlib; install via dnf
RUN microdnf install -y dnf \
    && dnf install -y zlib-devel --setopt=install_weak_deps=False --nodocs \
    && dnf clean all \
    && microdnf clean all

COPY --from=deps-builder /usr/local/ /usr/local/

ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64

WORKDIR /work/source
