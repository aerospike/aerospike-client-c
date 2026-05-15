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

# Use --userns=keep-id so the container user matches the runner UID.  This
# prevents permission denied errors when the container tries to write to the
# mount.

podman run \
	--rm \
	-v "$(pwd):/workspace:rw" \
	--workdir /workspace \
	--userns=keep-id \
	$BASEIMG \
	/bin/bash -c "ls -la; ./.github/bash-scripts/container-build-script-el.sh"

