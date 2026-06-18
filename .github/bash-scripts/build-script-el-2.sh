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

cp ./.github/Dockerfiles/el10.Dockerfile ./Dockerfile
podman build -t server-custom .

# Use --userns=keep-id so the container user matches the runner UID.  This
# prevents permission denied errors when the container tries to write to the
# mount.

podman run --name build-box -d -it server-custom /bin/bash
podman cp . build-box:/workspace
podman exec -w /workspace build-box /bin/bash -c ".github/bash-scripts/container-build-script-el.sh"
podman cp build-box:/workspace/ .
podman cp build-box:/workspace/target/ ./target/
podman cp build-box:/workspace/target/packages/ ./target/packages/

echo Now operating outside of container
echo --------------------------------------------------------------------------------
ls -la
echo --------------------------------------------------------------------------------
ls -la target
echo --------------------------------------------------------------------------------
ls -la target/packages
echo --------------------------------------------------------------------------------

