#!/usr/bin/env bash
#
# This should work also for Enterprise Linux, CentOS, Amazon Linux, and Fedora.
#
# This is a boot-strap script.  Because only ubuntu machines are really
# supported on GitHub Workflows, we use a Podman container to launch a
# suitable RPM-based distro inside a container.  Once the container is
# initialized, then we run the real build script.
#
# We mount the 

set -ue

export DISTRO=$(jq -r '.distro' <<<\"$MATRIX_JSON\")
export ARCH=$(jq -r '.arch' <<<\"$MATRIX_JSON\")
export PODMANIMG=$(jq -r '.podmanimage' <<<\"$MATRIX_JSON\")
export EMULATED=$EMULATED
echo "DISTRO: $DISTRO"
echo "ARCH: $ARCH"
echo "EMULATED: $EMULATED"
echo "PODMAN IMAGE: $PODMANIMG"
env | sort
ls -l

# Use --userns=keep-id so the container user matches the runner UID.  This
# prevents permission denied errors when the container tries to write to the
# mount.

podman run \
	--rm \
	-v "$(pwd):/workspace:rw" \
	--workdir /workspace \
	--userns=keep-id \
	$PODMANIMG \
	/bin/bash -c "
yum install -y openssl-devel glibc-devel autoconf automake libtool libz-devel libyaml-devel gcc-c++ graphviz rpm-build

./install_libuv
./install_libev
./install_libevent
make clean
make
sudo make install
make package
"

