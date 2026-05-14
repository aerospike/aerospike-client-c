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

for cmd in dnf yum; do
	if command -v "$cmd" &>/dev/null; then
		PKGMGR=$cmd
		break
	fi
done

if [ -z "${PKGMGR:-}" ]; then
	echo "ERROR: None of the required package managers were found"
	exit 1
fi

echo "Using package manager: $PKGMGR"

sudo $PKGMGR install openssl-devel glibc-devel autoconf automake libtool libz-devel libyaml-devel gcc-c++ graphviz rpm-build

./install_libuv
./install_libev
./install_libevent
make clean
make
sudo make install
make package

