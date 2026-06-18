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

# xcode/prepare_xcode breaks and depends on interactive input.
# The only real steps needed are the brew install steps.
# The Lua submodule update/init/build step appears to be dealt
# with in the make file already.

# Install MacOS prerequisites
brew install wget
brew install automake
brew install libtool
brew install openssl
brew install libyaml
brew install doxygen
brew install graphviz

./install_libuv
./install_libev
./install_libevent
make clean
make
sudo make install
make package

