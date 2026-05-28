# =============================================================================
# Debian 13 (Trixie) — multi-stage build environment
#
# Stage 1 (deps-builder): compiles libuv, libev, libevent from source into
#   /usr/local. doxygen is NOT built from source — Debian 13 ships a current
#   enough version via apt. All build tools, tarballs, and .o files discarded.
#
# Stage 2 (final): fresh base image with GCC toolchain + apt doxygen/graphviz.
#   The entire /usr/local tree is COPY'd from stage 1 in one instruction.
# =============================================================================

# ── Stage 1: compile event libs ───────────────────────────────────────────────
FROM debian:trixie AS deps-builder

ARG LIBEV_SHA256=973593d3479abdf657674a55afe5f78624b0e440614e2b8cb3a07f16d4d7f821

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get install -y --no-install-recommends \
        build-essential automake autoconf libtool wget ca-certificates \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

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
FROM debian:trixie

ENV PKG=deb \
    PATH=$PATH:/work/bin \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get install -y --no-install-recommends \
        build-essential automake autoconf libtool \
        git tar zip \
        liblua5.1.0-dev zlib1g-dev libyaml-dev libssl-dev \
        doxygen graphviz \
    && rm -rf /var/lib/apt/lists/*

COPY --from=deps-builder /usr/local/ /usr/local/

ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64

WORKDIR /work/source
