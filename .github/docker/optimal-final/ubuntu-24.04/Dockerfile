# =============================================================================
# Ubuntu 24.04 (Noble) — optimal-final build environment
# =============================================================================

FROM ubuntu:24.04 AS deps-builder

SHELL ["/bin/bash", "-c"]

ARG LIBEV_SHA256=973593d3479abdf657674a55afe5f78624b0e440614e2b8cb3a07f16d4d7f821

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get install -y --no-install-recommends \
        build-essential automake autoconf libtool wget ca-certificates \
        cmake flex bison python3 libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

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


FROM ubuntu:24.04

ENV PKG=deb \
    PATH=$PATH:/work/bin \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && apt-get install -y --no-install-recommends \
        build-essential autoconf automake libtool make pkg-config \
        git tar wget ca-certificates \
        libssl-dev libyaml-dev zlib1g-dev \
        graphviz zip dpkg-dev fakeroot findutils gzip \
    && rm -rf /var/lib/apt/lists/*

COPY --from=deps-builder /usr/local/ /usr/local/

ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64

WORKDIR /work/source
