#!/usr/bin/env bash

set -ue

cd /workspace

./install_libuv
./install_libev
./install_libevent
make clean
make
sudo make install
make package
