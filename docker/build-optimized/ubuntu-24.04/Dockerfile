# =============================================================================
# Ubuntu 24.04 (Noble) — multi-stage build environment
#
# Stage 1 (deps-builder): installs cmake/flex/bison/python3 from apt, then
#   compiles doxygen 1.9.5 from source (apt version too old) and compiles
#   libuv, libev, libevent from source into /usr/local. All build tools,
#   tarballs, and .o files are discarded with this stage.
#
# Stage 2 (final): fresh base image with GCC toolchain + graphviz.
#   The entire /usr/local tree (doxygen binary + event libs) is COPY'd from
#   stage 1 in one instruction, capturing libs, headers, and pkg-config files.
# =============================================================================

# ── Stage 1: compile doxygen + event libs ─────────────────────────────────────
FROM ubuntu:24.04 AS deps-builder

ARG LIBEV_SHA256=973593d3479abdf657674a55afe5f78624b0e440614e2b8cb3a07f16d4d7f821

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get install -y --no-install-recommends \
        build-essential automake autoconf libtool wget ca-certificates \
        cmake flex bison python3 libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# doxygen 1.9.5
RUN wget -q https://github.com/doxygen/doxygen/releases/download/Release_1_9_5/doxygen-1.9.5.src.tar.gz \
    && tar xzf doxygen-1.9.5.src.tar.gz \
    && cmake -S doxygen-1.9.5 -B doxygen-1.9.5/build -G "Unix Makefiles" \
    && cmake --build doxygen-1.9.5/build -j2 \
    && cmake --install doxygen-1.9.5/build \
    && rm -rf doxygen-1.9.5 doxygen-1.9.5.src.tar.gz

# libuv 1.8.0
RUN wget -q https://github.com/libuv/libuv/archive/v1.8.0.tar.gz \
    && tar xzf v1.8.0.tar.gz \
    && cd libuv-1.8.0 && sh autogen.sh && ./configure \
    && make -j"$(nproc)" && make install \
    && cd .. && rm -rf libuv-1.8.0 v1.8.0.tar.gz

# libev 4.24 (HTTP only — build fails if SHA256 does not match ARG LIBEV_SHA256)
RUN wget -q http://dist.schmorp.de/libev/Attic/libev-4.24.tar.gz \
    && echo "${LIBEV_SHA256}  libev-4.24.tar.gz" | sha256sum -c - \
    && tar xzf libev-4.24.tar.gz \
    && cd libev-4.24 && sh autogen.sh && ./configure \
    && make -j"$(nproc)" && make install \
    && cd .. && rm -rf libev-4.24 libev-4.24.tar.gz

# libevent 2.1.12
RUN wget -q https://github.com/libevent/libevent/archive/refs/tags/release-2.1.12-stable.tar.gz \
    && tar xzf release-2.1.12-stable.tar.gz \
    && cd libevent-release-2.1.12-stable && sh autogen.sh && ./configure \
    && make -j"$(nproc)" && make install \
    && cd .. && rm -rf libevent-release-2.1.12-stable release-2.1.12-stable.tar.gz


# ── Stage 2: final build environment ──────────────────────────────────────────
FROM ubuntu:24.04

ENV PKG=deb \
    PATH=$PATH:/work/bin \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get install -y --no-install-recommends \
        build-essential automake autoconf libtool \
        git tar zip \
        lua5.1 libssl-dev libyaml-dev zlib1g-dev \
        graphviz \
    && rm -rf /var/lib/apt/lists/*

COPY --from=deps-builder /usr/local/ /usr/local/

ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64

WORKDIR /work/source
