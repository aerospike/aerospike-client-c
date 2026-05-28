# Docker Build Environment — Change Log

## Overview

This folder (`docker/build-optimized/`) contains optimized multi-stage Dockerfiles for all 8 Linux
OS build environments used by the Aerospike C client CI/CD pipeline.

The originals are preserved untouched in `docker/build/` (flat single-stage `*.Dockerfile` files).

---

## Session Changes — 2026-05-28

### What was created

```
docker/build-optimized/
  amazonlinux-2023/Dockerfile
  debian-12/Dockerfile
  debian-13/Dockerfile
  rhel-8/Dockerfile
  rhel-9/Dockerfile
  rhel-10/Dockerfile
  ubuntu-22.04/Dockerfile
  ubuntu-24.04/Dockerfile
```

### Why a separate folder

- Originals in `docker/build/` are still referenced by `build.yml`, `package.yml`, and
  `run_platform.sh` — they were not changed.
- The new folder can be wired into the publish workflow independently once validated.

---

## Problems Fixed vs. Originals

| Problem | Original | Fixed |
|---------|----------|-------|
| Single-stage flat build | All tools + sources baked into final image | 2-stage: deps-builder discarded |
| 12+ separate RUN layers | Each adds permanent image weight | Consolidated per logical group |
| Build-only tools in final image | cmake, bison, flex, python3, wget stayed | Exist only in stage 1 |
| Source tarballs never deleted | `/work/libuv-1.8.0/`, `/work/doxygen-1.9.5/` etc. left in layers | Deleted in same RUN that builds them |
| No `--no-install-recommends` | Extra apt packages pulled in | Added on every apt-get install |
| No cache cleanup | `apt-get`/`yum` index left in layers | `rm -rf /var/lib/apt/lists/*` / `yum clean all` in same layer |
| Single-threaded compile | `make` without `-j` | `make -j"$(nproc)"` everywhere |
| libev over plain HTTP, no checksum | Vulnerable to MITM | SHA256 verified after download (`973593d3...`) |
| libevent built without OpenSSL | Silent degraded TLS support | `openssl-devel`/`libssl-dev` added to stage 1 |
| Spurious `gzip` package in RHEL stage 1 | Not needed (`tar xzf` handles it internally) | Removed |

---

## Multi-Stage Build Pattern (applied to all 8)

```
Stage 1: deps-builder (DISCARDED)
  Same base OS image
  + build-only tools (cmake, bison, flex, python3, wget)
  + openssl-devel / libssl-dev   ← so libevent builds with full TLS
  Compiles from source into /usr/local:
    doxygen 1.9.5   (where apt/yum version is too old)
    libuv   1.8.0
    libev   4.24    (SHA256 verified — HTTP source)
    libevent 2.1.12
  All source trees, .o files, build tools discarded with this stage.

Stage 2: final (THE IMAGE)
  Same base OS image (fresh — no build pollution)
  + GCC toolchain (to compile Aerospike C client)
  + libyaml-devel, openssl-devel (link-time deps)
  + rpm-build / build-essential (packaging)
  + graphviz (doxygen runtime dep)
  COPY --from=deps-builder /usr/local/ /usr/local/
  ENV LD_LIBRARY_PATH=/usr/local/lib:/usr/local/lib64
  WORKDIR /work/source
```

### Why `COPY /usr/local/ /usr/local/` instead of per-file globs

Copies the entire `/usr/local` tree in one instruction — automatically captures:
- Compiled `.so` / `.a` library files
- Header files (`uv.h`, `ev.h`, `event2/`)
- pkg-config files (`/usr/local/lib/pkgconfig/*.pc`) — granular globs would silently miss these
- doxygen binary + any companion binaries (`doxyindexer`)

### Why `ENV LD_LIBRARY_PATH` instead of `RUN ldconfig`

- `LD_LIBRARY_PATH` is explicit and survives layer ordering edge cases
- Matches exactly what the CI workflows already inject at runtime:
  `-e LD_LIBRARY_PATH=/usr/local/lib` in both `build.yml` and `package.yml`

---

## Per-OS Differences

### Family A — Debian 12 / 13
- `apt` ships a current enough doxygen → **no source build for doxygen in stage 1**
- Stage 1: compile only 3 event libs
- Stage 2: `apt-get install doxygen graphviz`

### Family B — Ubuntu 22.04 / 24.04
- `apt` doxygen is too old for this project → **doxygen compiled from source in stage 1**
- Stage 1: cmake + flex + bison + python3 from apt → doxygen + event libs
- Stage 2: `apt-get install graphviz` only

### Family C — RHEL 8 / 9 / 10
- `microdnf` has cmake but **NOT bison or flex** → both compiled from source first
- RHEL 9/10: `ubi-minimal` omits zlib → `dnf install zlib-devel` in stage 2
- Bison versions: RHEL 8 uses 3.4 · RHEL 9/10 use 3.7.4
- Flex versions:  RHEL 8 uses 2.6.1 · RHEL 9/10 use 2.6.4
- Optional debug tools: `ARG INSTALL_DEBUG_TOOLS=false` (see below)

### Family D — Amazon Linux 2023
- `yum` has cmake + flex + bison → **only doxygen + event libs from source**
- Stage 1: install cmake/flex/bison from yum, compile doxygen + event libs

---

## Debug Tools Flag (RHEL 8 / 9 / 10 only)

`gdb` and `strace` are gated behind a build ARG that defaults to `false`:

```dockerfile
ARG INSTALL_DEBUG_TOOLS=false
...
RUN if [ "$INSTALL_DEBUG_TOOLS" = "true" ]; then \
        microdnf install -y gdb strace && microdnf clean all; \
    fi
```

**Default build** (slim, no debug tools):
```bash
docker build -f docker/build-optimized/rhel-9/Dockerfile .
```

**Debug build**:
```bash
docker build --build-arg INSTALL_DEBUG_TOOLS=true \
  -f docker/build-optimized/rhel-9/Dockerfile .
```

The ARG is in Stage 2 only — Stage 1 is discarded anyway and debug tools are only
useful in the environment where `make all` / `make test` runs.

---

## Decisions NOT applied (user preference)

| Suggestion | Decision |
|------------|----------|
| `ARG` for version pinning (DOXYGEN_VER etc.) | Rejected — keep literal version strings |
| `LABEL` metadata on Stage 2 images | Rejected — no labels |

---

## Files NOT changed

| File | Status |
|------|--------|
| `docker/build/*.Dockerfile` (all 8 originals) | Untouched |
| `.github/workflows/build.yml` | Untouched |
| `.github/workflows/package.yml` | Untouched |
| `run_platform.sh` | Untouched |
| `run_all_platforms.sh` | Untouched |
| `run_local.sh` | Untouched |

Workflows and scripts still reference `docker/build/` originals. The new
`docker/build-optimized/` images will be wired into the publish workflow in Phase 2.

---

## Reminders / Constraints

| Rule | Detail |
|------|--------|
| **Community server only** | `windows.yml` and `build-mac-wind.yml` MUST test against `aerospike/aerospike-server` (community image) only. Do NOT switch to `aerospike-server-enterprise`. Enterprise requires OIDC auth, `features.conf`, and the `aerospike/shared-workflows` setup action — none of which are wired up in the C client CI. |
| **No shared-workflows server action** | The C# client uses `aerospike/shared-workflows/.github/actions/setup-aerospike-server` for enterprise. Do not adopt this for the C client — it is enterprise-only and incompatible with the plain `docker run` community setup. |

---

## Next Steps (Phase 2)

- [ ] `publish-build-images.yml` — build from `docker/build-optimized/` and push to GHCR
      as `:<os>-amd64` and `:<os>-arm64`, then create multi-arch manifests `:<os>`
- [ ] `windows.yml` — NuGet restore, MSBuild matrix (Debug/Release/libuv/libevent),
      Docker Aerospike integration tests
- [ ] `macos.yml` — brew deps, `make all`, Aerospike server, `make test`, `.dylib` artifacts
