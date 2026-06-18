################################################################################
# Taken from citrusleaf:qe-docker//build/aerospike-client-c:rhel-8
################################################################################

# Base Image
FROM registry.access.redhat.com/ubi8/ubi-minimal

# Environment Variables
ENV PKG=rpm \
    PATH=$PATH:/work/bin

RUN \
  microdnf install -y \
    git \
    tar \
    wget \
    which \
    gcc \
    gcc-c++ \
    make \
    automake \
    autoconf \
    kernel-headers \
    glibc-devel \
    glibc-headers \
    libtool \
    binutils \
    m4 \
    gettext \
    diffutils \
    patch \
    gdb \
    strace \
    rpm-build \
    which \
    file

# Project Specific Packages
# Additional packages should be added in alphabetical order.
RUN microdnf install -y \
  libyaml-devel \
  openssl  \
  openssl-devel

WORKDIR /work

# Install doxygen
RUN microdnf install python3 -y
RUN wget -q https://www.doxygen.nl/files/doxygen-1.9.5.src.tar.gz
RUN gunzip doxygen-1.9.5.src.tar.gz
RUN tar xf doxygen-1.9.5.src.tar
RUN microdnf install -y cmake

RUN wget https://ftp.gnu.org/gnu/bison/bison-3.4.tar.gz
RUN tar -xzf bison-3.4.tar.gz
RUN cd bison-3.4 && ./configure --prefix=/usr/local && make && make install

RUN wget https://github.com/westes/flex/releases/download/v2.6.1/flex-2.6.1.tar.gz
RUN tar -xzf flex-2.6.1.tar.gz
RUN cd flex-2.6.1 && ./configure --prefix=/usr/local && make && make install
RUN /usr/local/bin/flex --version

WORKDIR /work/doxygen-1.9.5
RUN mkdir build
WORKDIR /work/doxygen-1.9.5/build
RUN cmake -G "Unix Makefiles" ..
RUN make
RUN make install
WORKDIR /work

# Install libuv
RUN wget -q https://github.com/libuv/libuv/archive/v1.8.0.tar.gz
RUN \
  tar xzf v1.8.0.tar.gz && \
  cd libuv-1.8.0 && \
  sh autogen.sh && \
  ./configure && \
  make && \
  make install

# Install libev
RUN wget -q http://dist.schmorp.de/libev/Attic/libev-4.24.tar.gz
RUN \
  tar xzf libev-4.24.tar.gz && \
  cd libev-4.24 && \
  sh autogen.sh && \
  ./configure && \
  make && \
  make install

# Install libevent
RUN wget -q https://github.com/libevent/libevent/archive/refs/tags/release-2.1.12-stable.tar.gz
RUN \
  tar xzf release-2.1.12-stable.tar.gz && \
  cd libevent-release-2.1.12-stable && \
  sh autogen.sh && \
  ./configure && \
  make && \
  make install

RUN microdnf install -y graphviz

# Commands are run from this working directory
WORKDIR /work/source

# Add the build script
COPY build.sh /work/bin/build
