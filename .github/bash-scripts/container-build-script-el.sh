#!/usr/bin/env bash

set -ue

sudo yum install -y openssl-devel glibc-devel autoconf automake libtool libz-devel libyaml-devel gcc-c++ graphviz rpm-build

./install_libuv
./install_libev
./install_libevent
make clean
make
sudo make install
make package
