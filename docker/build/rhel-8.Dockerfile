FROM registry.access.redhat.com/ubi8/ubi-minimal

ENV PKG=rpm \
    PATH=$PATH:/work/bin

RUN microdnf install -y \
    git tar wget which gcc gcc-c++ make automake autoconf \
    kernel-headers glibc-devel glibc-headers libtool binutils \
    m4 gettext diffutils patch gdb strace rpm-build file

RUN microdnf install -y \
    libyaml-devel openssl openssl-devel

WORKDIR /work

RUN microdnf install -y python3
RUN wget -q https://www.doxygen.nl/files/doxygen-1.9.5.src.tar.gz && \
    gunzip doxygen-1.9.5.src.tar.gz && \
    tar xf doxygen-1.9.5.src.tar
RUN microdnf install -y cmake
RUN wget -q https://ftp.gnu.org/gnu/bison/bison-3.4.tar.gz && \
    tar -xzf bison-3.4.tar.gz && \
    cd bison-3.4 && ./configure --prefix=/usr/local && make && make install
RUN wget -q https://github.com/westes/flex/releases/download/v2.6.1/flex-2.6.1.tar.gz && \
    tar -xzf flex-2.6.1.tar.gz && \
    cd flex-2.6.1 && ./configure --prefix=/usr/local && make && make install
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

RUN microdnf install -y graphviz

WORKDIR /work/source
