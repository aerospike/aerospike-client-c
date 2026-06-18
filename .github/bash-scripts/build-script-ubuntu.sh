#!/usr/bin/env bash

set -ue

export DISTRO=$(jq -r '.distro' <<<\"$MATRIX_JSON\")
export ARCH=$(jq -r '.arch' <<<\"$MATRIX_JSON\")
export EMULATED=$EMULATED
echo "DISTRO: $DISTRO"
echo "ARCH: $ARCH"
echo "EMULATED: $EMULATED"
env | sort
ls -l

sudo apt-get install doxygen graphviz zip autoconf automake g++ libc6-dev libssl-dev libtool libyaml-dev zlib1g-dev
sudo apt-get install ncurses-dev
./install_libuv
./install_libev
./install_libevent
make clean
make
sudo make install
make package

