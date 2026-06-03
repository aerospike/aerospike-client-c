# =============================================================================
# Amazon Linux 2023 — fast multi-stage build environment
#
# Optimised over docker/build-optimized/amazonlinux-2023/Dockerfile:
#   amd64: download pre-built doxygen binary (seconds vs ~30 min from source)
#   arm64: cmake/flex/bison/python3 from dnf in common install (no separate
#          dnf invocation inside the else branch); doxygen built with capped cores
#   Event libs: parallel wget downloads, then sequential -j$(nproc) builds.
#
# AL2023 is DNF-based; bison + flex are available in the default AL2023 repos
# (unlike RHEL UBI minimal where they must be compiled from source).
# =============================================================================

# ── Stage 1: doxygen (pre-built or from source) + event libs ──────────────────
FROM amazonlinux:2023 AS deps-builder

ARG LIBEV_SHA256=973593d3479abdf657674a55afe5f78624b0e440614e2b8cb3a07f16d4d7f821

# AL2023 is DNF-based (yum is just an alias). cmake/flex/bison/python3 are
# available in the default repos — no source compilation needed.
RUN dnf install -y \
        gcc gcc-c++ make autoconf automake libtool m4 \
        cmake python3 bison flex \
        wget tar gzip \
        openssl-devel \
    && dnf clean all

WORKDIR /build

# doxygen 1.12.0
#   amd64: pre-built binary from GitHub releases — seconds instead of ~30 min.
#   arm64: build from source; bison/flex already installed above.
RUN set -euo pipefail && \
    ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
      wget -q "https://github.com/doxygen/doxygen/releases/download/Release_1_12_0/doxygen-1.12.0.linux.bin.tar.gz" \
        -O doxygen.tar.gz \
      && tar xzf doxygen.tar.gz \
      && install -m 755 doxygen-1.12.0/bin/doxygen /usr/local/bin/doxygen \
      && rm -rf doxygen-1.12.0 doxygen.tar.gz; \
    else \
      wget -q https://github.com/doxygen/doxygen/releases/download/Release_1_12_0/doxygen-1.12.0.src.tar.gz \
      && tar xzf doxygen-1.12.0.src.tar.gz \
      && cmake -S doxygen-1.12.0 -B doxygen-1.12.0/build -G "Unix Makefiles" \
      && cmake --build doxygen-1.12.0/build -j"$(( $(nproc) > 4 ? 4 : $(nproc) ))" \
      && cmake --install doxygen-1.12.0/build \
      && rm -rf doxygen-1.12.0 doxygen-1.12.0.src.tar.gz; \
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
FROM amazonlinux:2023

ENV PKG=rpm \
    PATH=$PATH:/work/bin

RUN dnf install -y \
        git tar zip wget which \
        gcc gcc-c++ make autoconf automake libtool m4 binutils \
        libyaml-devel openssl openssl-devel zlib-devel \
        rpm-build file \
        graphviz \
    && dnf clean all

COPY --from=deps-builder /usr/local/ /usr/local/

ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64

WORKDIR /work/source
