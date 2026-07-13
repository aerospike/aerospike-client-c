# syntax=docker/dockerfile:1
###############################################################################
## ael-gui - self-contained playground image.
##
## Builds the error-details server (CE) and the C client lib from their
## branches, plus the ael-gui tool from this checkout, into one image that
## runs both asd and the GUI. See README.md ("Docker quickstart") for the
## build and run commands.
##
## The server repo is private: build with BuildKit and pass your ssh-agent
## (--ssh default). The key is mounted only for the clone step and never
## enters the image or its layer history.
##
## Build context is the tools/ael-gui directory (one level up from this
## file), so local edits to ael_gui.c / ui.html are picked up without
## pushing.
###############################################################################

ARG UBUNTU=ubuntu:24.04

#------------------------------------------------------------------ builder --
FROM ${UBUNTU} AS builder

ARG CLIENT_REPO=https://github.com/aerospike/aerospike-client-c.git
ARG CLIENT_REF=dylan/ael-gui
ARG SERVER_REPO=git@github.com:citrusleaf/aerospike-server.git
ARG SERVER_REF=dylan/SERVER-1137-exp-build-trace

# Content-aware cache-bust for the two clone layers below. Docker caches a
# `git clone` by command string, not by remote content, so a moved branch is
# never re-cloned without --no-cache. Pass the branch's current commit here
# (from `git ls-remote`) and the clone's cache key changes iff the SHA changed:
#   --build-arg CLIENT_REV=$(git ls-remote <client-repo> <CLIENT_REF> | cut -f1)
#   --build-arg SERVER_REV=$(git ls-remote <server-repo> <SERVER_REF> | cut -f1)
# Omit them and the build behaves as before (constant -> always a cache hit).
# A build-arg (not an `ADD https://api.github.com/...commits/<ref>` probe) is
# used because the server repo is private ssh - ADD can't authenticate to it.
ARG CLIENT_REV=
ARG SERVER_REV=

ENV DEBIAN_FRONTEND=noninteractive
ENV AEROSPIKE_TELEMETRY=FALSE

# python3 is assumed by the ICU submodule's data build (and present on any
# dev box, so install-dependencies.sh doesn't list it).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git ca-certificates openssh-client libyaml-dev python3 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Client lib (public repo) - provides libaerospike.a + headers for the tool.
# Submodules are public https, cloned full-depth (shallow submodule fetches
# can fail on unadvertised pins). The CLIENT_REV echo pins this layer's cache
# key to the branch tip (see the ARG note above).
RUN echo "client rev: ${CLIENT_REV}" && \
    git clone --depth 1 --branch "${CLIENT_REF}" --recurse-submodules \
        "${CLIENT_REPO}" aerospike-client-c

# Server (private repo, ssh submodule URLs) - clone + submodules in one
# ssh-mounted step. Full single-branch clone, NOT --depth 1: the version
# stamp needs `git describe` to reach a release tag, and the C client
# rejects nodes whose build version is a bare SHA.
RUN mkdir -p -m 0700 ~/.ssh && ssh-keyscan github.com >> ~/.ssh/known_hosts
RUN --mount=type=ssh \
    echo "server rev: ${SERVER_REV}" && \
    git clone --single-branch --branch "${SERVER_REF}" "${SERVER_REPO}" \
        aerospike-server && \
    cd aerospike-server && git submodule update --init --jobs 4

# Server build deps via the repo's own script (no sudo needed as root),
# then a community build - the error-details + AEL machinery is all
# CE-side, so no EE repo or feature key is needed.
RUN cd aerospike-server && ./bin/install-dependencies.sh && \
    rm -rf /var/lib/apt/lists/*

RUN cd aerospike-server && make -j"$(nproc)"

RUN cd aerospike-client-c && make -j"$(nproc)"

# The tool itself builds from this checkout's source (the build context),
# not from the cloned client repo - local iteration works without pushing.
COPY ael_gui.c ui.html Makefile /src/tool/
RUN cd /src/tool && make AEROSPIKE=/src/aerospike-client-c

# Stage at an arch-independent path (target dir is Linux-x86_64 /
# Linux-aarch64 depending on build host).
RUN cp /src/aerospike-server/target/Linux-*/bin/asd /src/asd

#------------------------------------------------------------------ runtime --
FROM ${UBUNTU}

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libssl3t64 zlib1g libyaml-0-2 libcurl4t64 libldap2 curl \
        ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/asd /opt/aerospike/bin/asd
COPY --from=builder /src/tool/target/ael-gui /opt/aerospike/bin/ael-gui
COPY --from=builder /src/tool/ui.html /opt/aerospike/ui.html
COPY docker/aerospike.conf /opt/aerospike/etc/aerospike.conf
COPY docker/entrypoint.sh /opt/aerospike/entrypoint.sh

RUN chmod +x /opt/aerospike/entrypoint.sh && \
    mkdir -p /opt/aerospike/work/smd /opt/aerospike/work/usr/udf/lua

EXPOSE 8280

HEALTHCHECK --interval=30s --timeout=3s --start-period=60s \
    CMD curl -fsS -o /dev/null http://127.0.0.1:8280/ || exit 1

ENTRYPOINT ["/opt/aerospike/entrypoint.sh"]
