FROM debian:trixie

ENV PKG=deb \
    PATH=$PATH:/work/bin \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y && \
    apt-get install -y build-essential
RUN apt-get install -y \
    automake autoconf libtool \
    python3 python-is-python3 \
    git-core tar zip wget \
    liblua5.1.0-dev zlib1g-dev
RUN apt-get install -y \
    doxygen libyaml-dev libssl-dev

WORKDIR /work

RUN wget -q https://github.com/libuv/libuv/archive/v1.8.0.tar.gz && \
    tar xzf v1.8.0.tar.gz && \
    cd libuv-1.8.0 && sh autogen.sh && ./configure && make && make install

RUN wget -q http://dist.schmorp.de/libev/Attic/libev-4.24.tar.gz && \
    tar xzf libev-4.24.tar.gz && \
    cd libev-4.24 && sh autogen.sh && ./configure && make && make install

RUN wget -q https://github.com/libevent/libevent/archive/refs/tags/release-2.1.12-stable.tar.gz && \
    tar xzf release-2.1.12-stable.tar.gz && \
    cd libevent-release-2.1.12-stable && sh autogen.sh && ./configure && make && make install

RUN apt-get install -y graphviz

WORKDIR /work/source
