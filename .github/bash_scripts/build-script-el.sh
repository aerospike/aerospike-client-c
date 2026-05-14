#!/usr/bin/env bash
#
# This should work also for Enterprise Linux, CentOS, Amazon Linux, and Fedora.

set -ue

export DISTRO=$(jq -r '.distro' <<<\"$MATRIX_JSON\")
export ARCH=$(jq -r '.arch' <<<\"$MATRIX_JSON\")
export EMULATED=$EMULATED
echo "DISTRO: $DISTRO"
echo "ARCH: $ARCH"
echo "EMULATED: $EMULATED"
env | sort
ls -l

sudo yum install openssl-devel glibc-devel autoconf automake libtool libz-devel libyaml-devel gcc-c++ graphviz rpm-build

./install_libuv
./install_libev
./install_libevent
make clean
make
sudo make install
make package

