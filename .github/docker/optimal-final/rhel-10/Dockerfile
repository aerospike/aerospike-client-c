# =============================================================================
# RHEL 10 / UBI 10 Minimal — optimal-final build environment
#
# bison 3.7.4 + flex 2.6.4 + doxygen 1.12.0 from source on all arches
# (install_deps_rhel_10). Pre-built doxygen binary needs newer libstdc++.
# =============================================================================

FROM registry.access.redhat.com/ubi10/ubi-minimal AS deps-builder

ARG LIBEV_SHA256=973593d3479abdf657674a55afe5f78624b0e440614e2b8cb3a07f16d4d7f821

RUN microdnf install -y \
        gcc gcc-c++ make autoconf automake libtool m4 \
        cmake python3 wget tar gzip diffutils \
        openssl-devel \
    && microdnf clean all

WORKDIR /build

RUN set -euo pipefail && \
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
    && rm -rf doxygen-1.12.0*

RUN set -euo pipefail && \
    wget -q http://dist.libuv.org/dist/v1.15.0/libuv-v1.15.0.tar.gz -O libuv.tar.gz & \
    wget -q http://dist.schmorp.de/libev/Attic/libev-4.24.tar.gz -O libev.tar.gz & \
    wget -q https://github.com/libevent/libevent/releases/download/release-2.1.12-stable/libevent-2.1.12-stable.tar.gz \
         -O libevent.tar.gz & \
    wait && \
    echo "${LIBEV_SHA256}  libev.tar.gz" | sha256sum -c - && \
    tar xzf libuv.tar.gz && tar xzf libev.tar.gz && tar xzf libevent.tar.gz

RUN cd libuv-v1.15.0 && sh autogen.sh && ./configure -q \
    && make -j"$(nproc)" CFLAGS=-w && make install \
    && cd .. && rm -rf libuv-v1.15.0 libuv.tar.gz

RUN cd libev-4.24 && ./configure -q \
    && make -j"$(nproc)" CFLAGS=-w && make install \
    && cd .. && rm -rf libev-4.24 libev.tar.gz

RUN cd libevent-2.1.12-stable && ./configure -q \
    && make -j"$(nproc)" CFLAGS=-w && make install \
    && cd .. && rm -rf libevent-2.1.12-stable libevent.tar.gz


FROM registry.access.redhat.com/ubi10/ubi-minimal

ARG INSTALL_DEBUG_TOOLS=false

ENV PKG=rpm \
    PATH=$PATH:/work/bin

RUN microdnf install -y \
        git tar wget which gzip diffutils file findutils \
        gcc gcc-c++ make autoconf automake libtool m4 \
        binutils glibc-devel glibc-headers kernel-headers \
        gettext patch \
        openssl openssl-devel libyaml-devel \
        graphviz zip rpm-build \
    && microdnf clean all

RUN if [ "$INSTALL_DEBUG_TOOLS" = "true" ]; then \
        microdnf install -y gdb strace && microdnf clean all; \
    fi

RUN microdnf install -y dnf \
    && dnf install -y zlib-devel --setopt=install_weak_deps=False --nodocs \
    && dnf clean all \
    && microdnf clean all

COPY --from=deps-builder /usr/local/ /usr/local/

ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64

WORKDIR /work/source
