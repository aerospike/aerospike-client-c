FROM amazonlinux:2023

ENV PKG=rpm \
    PATH=$PATH:/work/bin

RUN yum groupinstall -y "Development Tools"
RUN yum install -y git tar wget which
RUN yum install -y libyaml-devel openssl openssl-devel

WORKDIR /work

RUN yum install -y python3
RUN wget -q https://github.com/doxygen/doxygen/releases/download/Release_1_9_5/doxygen-1.9.5.src.tar.gz && \
    gunzip doxygen-1.9.5.src.tar.gz && \
    tar xf doxygen-1.9.5.src.tar
RUN yum install -y cmake flex bison
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

RUN yum install -y graphviz

WORKDIR /work/source
