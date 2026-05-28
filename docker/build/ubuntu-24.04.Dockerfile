FROM ubuntu:24.04

ENV PKG=deb \
    PATH=$PATH:/work/bin \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update -y
RUN apt-get install -y -f tzdata
RUN apt-get install -y build-essential checkinstall
RUN apt-get install -y \
    automake autoconf libtool \
    python3 git tar zip wget
RUN apt-get install -y \
    lua5.1 openssl libssl-dev libyaml-dev libz-dev

WORKDIR /work

RUN wget -q https://www.doxygen.nl/files/doxygen-1.9.5.src.tar.gz && \
    gunzip doxygen-1.9.5.src.tar.gz && \
    tar xf doxygen-1.9.5.src.tar
RUN apt-get install -y cmake flex bison
WORKDIR /work/doxygen-1.9.5
RUN mkdir build
WORKDIR /work/doxygen-1.9.5/build
RUN cmake -G "Unix Makefiles" .. && make && make install
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
