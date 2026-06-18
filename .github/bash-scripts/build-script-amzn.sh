#!/usr/bin/env bash
#
# This should work also for Enterprise Linux, CentOS, Amazon Linux, and Fedora.
#
# This is a boot-strap script.  Because only ubuntu machines are really
# supported on GitHub Workflows, we use a Podman container to launch a
# suitable RPM-based distro inside a container.  Once the container is
# initialized, then we run the real build script.

set -ue

echo --------------------------------------------------------------------------------
export DISTRO=$(printf "%s" "$MATRIX_JSON" | jq -r '.distro')
export ARCH=$(printf "%s" "$MATRIX_JSON" | jq -r '.arch')
export BASEIMG=$(printf "%s" "$MATRIX_JSON" | jq -r '."base-image"')
export EMULATED=$EMULATED

echo "  DISTRO: $DISTRO"
echo "    ARCH: $ARCH"
echo " BASEIMG: $BASEIMG"
echo "EMULATED: $EMULATED"
echo --------------------------------------------------------------------------------
env | sort
echo --------------------------------------------------------------------------------
ls -l
echo --------------------------------------------------------------------------------

# First build our custom Docker image, because we absolutely must have sudo
# access, and the stock images do not include sudo.

echo "FROM $BASEIMG" > Dockerfile
echo "RUN dnf update -y && dnf install -y doxygen git openssl-devel glibc-devel autoconf automake libtool zlib-devel libyaml-devel gcc-c++ graphviz rpm-build wget shadow-utils sudo && dnf clean all" >> Dockerfile
echo 'RUN useradd -m -s /bin/bash dev && echo "dev ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers' >> Dockerfile
echo "USER dev" >> Dockerfile
echo "WORKDIR /workspace" >> Dockerfile
echo 'CMD ["/bin/bash"]' >> Dockerfile

podman build -t al2023-custom .

# Use --userns=keep-id so the container user matches the runner UID.  This
# prevents permission denied errors when the container tries to write to the
# mount.

sudo chmod -R 777 .
podman run \
	--rm \
	-v "$(pwd):/workspace:rw,U" \
	--workdir /workspace \
	--userns=keep-id \
	al2023-custom \
	/bin/bash -c "ls -la; ./.github/bash-scripts/container-build-script-el.sh"

