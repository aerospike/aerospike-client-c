# =============================================================================
# RHEL 8 / UBI 8 Minimal — multi-stage build environment
#
# Stage 1 (deps-builder): microdnf has cmake but NOT bison or flex, so both
#   are compiled from source first (bison 3.7.4 — 3.4 has a parallel-make bug),
#   then doxygen 1.9.5, then the three event libs. Everything is installed into
#   /usr/local. All build tools, source tarballs, and .o files are discarded.
#
# Stage 2 (final): fresh ubi8-minimal with GCC toolchain + rpm-build.
#   The entire /usr/local tree is COPY'd from stage 1 in one instruction,
#   capturing libs, headers, pkg-config files, and the doxygen binary.
# =============================================================================

# ── Stage 1: compile bison, flex, doxygen + event libs ────────────────────────
FROM registry.access.redhat.com/ubi8/ubi-minimal AS deps-builder

ARG LIBEV_SHA256=973593d3479abdf657674a55afe5f78624b0e440614e2b8cb3a07f16d4d7f821

RUN microdnf install -y \
        gcc gcc-c++ make autoconf automake libtool m4 \
        cmake python3 wget tar gzip diffutils \
        openssl-devel \
    && microdnf clean all

WORKDIR /build

# bison 3.7.4 — not in microdnf repos; required by doxygen configure
# (3.4 has a parallel-make race condition in examples/c/reccalc/scan.stamp)
RUN wget -q https://ftp.gnu.org/gnu/bison/bison-3.7.4.tar.gz \
    && tar xzf bison-3.7.4.tar.gz \
    && cd bison-3.7.4 && ./configure --prefix=/usr/local \
    && make -j"$(nproc)" MAKEINFO=true && make install \
    && cd .. && rm -rf bison-3.7.4 bison-3.7.4.tar.gz

# flex 2.6.1 — not in microdnf repos; required by doxygen
RUN wget -q https://github.com/westes/flex/releases/download/v2.6.1/flex-2.6.1.tar.gz \
    && tar xzf flex-2.6.1.tar.gz \
    && cd flex-2.6.1 && ./configure --prefix=/usr/local \
    && make -j"$(nproc)" && make install \
    && cd .. && rm -rf flex-2.6.1 flex-2.6.1.tar.gz

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
FROM registry.access.redhat.com/ubi8/ubi-minimal

# Set to "true" at build time to include gdb and strace for CI debugging:
#   docker build --build-arg INSTALL_DEBUG_TOOLS=true ...
ARG INSTALL_DEBUG_TOOLS=false

ENV PKG=rpm \
    PATH=$PATH:/work/bin

RUN microdnf install -y \
        git tar wget which \
        gcc gcc-c++ make autoconf automake libtool m4 \
        binutils glibc-devel glibc-headers kernel-headers \
        gettext diffutils patch file \
        libyaml-devel openssl openssl-devel zlib-devel \
        rpm-build \
        graphviz \
    && microdnf clean all

RUN if [ "$INSTALL_DEBUG_TOOLS" = "true" ]; then \
        microdnf install -y gdb strace && microdnf clean all; \
    fi

COPY --from=deps-builder /usr/local/ /usr/local/

ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64

WORKDIR /work/source
