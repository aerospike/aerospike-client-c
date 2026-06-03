# =============================================================================
# Ubuntu 22.04 (Jammy) — fast multi-stage build environment
#
# Optimised over docker/build-optimized/ubuntu-22.04/Dockerfile:
#
#   BEFORE  cmake --build doxygen-1.12.0/build -j2       ← 2 cores, ~30 min
#   AFTER   amd64: download pre-built doxygen binary    ← seconds
#           arm64: cmake/flex/bison/python3 from apt upfront (no conditional
#                  apt-get update in else branch); doxygen built with capped cores
#   Event libs: three parallel wget downloads, then sequential build.
# =============================================================================

# ── Stage 1: doxygen (pre-built or from source) + event libs ──────────────────
FROM ubuntu:22.04 AS deps-builder

# dash (Ubuntu's /bin/sh) does not support pipefail; switch to bash for RUN.
SHELL ["/bin/bash", "-c"]

ARG LIBEV_SHA256=973593d3479abdf657674a55afe5f78624b0e440614e2b8cb3a07f16d4d7f821

ENV DEBIAN_FRONTEND=noninteractive

# cmake/flex/bison/python3 included upfront — Ubuntu 22.04 ships bison-3.8.2
# and flex-2.6.4, both well above doxygen 1.12.0's requirements (bison >= 2.7,
# flex >= 2.5.35). Single apt-get run avoids a conditional update in the else branch.
RUN apt-get update -y && apt-get install -y --no-install-recommends \
        build-essential automake autoconf libtool wget ca-certificates \
        cmake flex bison python3 \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# doxygen 1.12.0
#   amd64: pre-built binary from GitHub releases — seconds instead of 30+ min.
#   arm64: build from source; cmake/flex/bison already installed above.
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

# Download all three event-lib tarballs in parallel, then build sequentially.
# Parallel wget saves round-trip time on slow network without overloading the CPU.
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
FROM ubuntu:22.04

ENV PKG=deb \
    PATH=$PATH:/work/bin \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get install -y --no-install-recommends \
        build-essential automake autoconf libtool \
        git tar zip \
        lua5.1 libssl-dev libyaml-dev zlib1g-dev \
        graphviz \
        fakeroot \
    && rm -rf /var/lib/apt/lists/*

COPY --from=deps-builder /usr/local/ /usr/local/

ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64

WORKDIR /work/source
