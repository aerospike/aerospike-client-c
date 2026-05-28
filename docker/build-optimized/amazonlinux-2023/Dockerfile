# =============================================================================
# Amazon Linux 2023 — multi-stage build environment
#
# Stage 1 (deps-builder): uses yum cmake/flex/bison to compile doxygen 1.9.5
#   from source, then compiles libuv, libev, libevent from source into
#   /usr/local. All build tools, source tarballs, and .o files are discarded.
#
# Stage 2 (final): fresh base image with GCC toolchain + packaging tools.
#   The entire /usr/local tree is COPY'd from stage 1 in one instruction,
#   capturing libs, headers, pkg-config files, and the doxygen binary.
# =============================================================================

# ── Stage 1: compile doxygen + event libs ─────────────────────────────────────
FROM amazonlinux:2023 AS deps-builder

ARG LIBEV_SHA256=973593d3479abdf657674a55afe5f78624b0e440614e2b8cb3a07f16d4d7f821

RUN yum install -y \
        gcc gcc-c++ make autoconf automake libtool m4 \
        cmake flex bison python3 wget tar gzip \
        openssl-devel \
    && yum clean all

WORKDIR /build

# doxygen 1.9.5
RUN wget -q https://www.doxygen.nl/files/doxygen-1.9.5.src.tar.gz \
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
FROM amazonlinux:2023

ENV PKG=rpm \
    PATH=$PATH:/work/bin

RUN yum install -y \
        git tar zip wget which \
        gcc gcc-c++ make autoconf automake libtool m4 binutils \
        libyaml-devel openssl openssl-devel zlib-devel \
        rpm-build file \
        graphviz \
    && yum clean all

# Copy the entire /usr/local tree — captures libs, headers, pkg-config files,
# and the doxygen binary in one shot without fragile per-path enumeration.
COPY --from=deps-builder /usr/local/ /usr/local/

# Explicit library path so the dynamic linker finds /usr/local/lib at runtime.
# Mirrors the -e LD_LIBRARY_PATH=/usr/local/lib passed in docker run by CI.
ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64

WORKDIR /work/source
