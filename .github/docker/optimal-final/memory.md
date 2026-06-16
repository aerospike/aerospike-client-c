# docker/optimal-final — build environment images

Final optimized multi-stage Dockerfiles for Aerospike C client CI build/test/package.

## Source lineage

- Structure and multi-stage pattern: `docker/build-optimized/`
- Build-time optimizations (pre-built doxygen on amd64, parallel tarball fetch): `docker/build-fast/`
- Library versions and per-distro packages: `.github/script/install_deps.bash`

## Version alignment (install_deps.bash)

| Component | Version |
|-----------|---------|
| libev | 4.24 |
| libuv | 1.15.0 |
| libevent | 2.1.12-stable |
| doxygen | 1.12.0 (source or pre-built binary) |
| bison (rhel-8) | 3.4 |
| flex (rhel-8) | 2.6.1 |
| bison (rhel-9/10) | 3.7.4 |
| flex (rhel-9/10) | 2.6.4 |

Lua is **not** installed (client bundles via modules/lua submodule).

## CI publish

Images are built and pushed by `.github/workflows/build-docker-images.yml` using
`aerospike/shared-workflows` `reusable_docker-build-deploy.yaml` with native
`linux/amd64` + `linux/arm64` runners only (no QEMU).

Image name pattern: `client-c-build-<distro>` in `${JFROG_PROJECT}-docker-dev-local`.
