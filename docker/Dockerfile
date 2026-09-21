# Plain multi-stage Docker build, intentionally compatible with Colima's legacy
# builder: it uses no BuildKit-only mounts, secrets, or cache directives.
# `tools/docker_prepare_context.sh` vendors the transitive dependency sources
# and the uvrr-core submodule source (the upstream tag v0.7.4 the manifest's
# dependency names), so no git fetch happens inside Docker. The runtime stage
# carries the diagnosis kit: the NTP/capture/RTT
# tooling packages and the demo crate's binaries from the kit stage.
ARG AOF_IMAGE=ghcr.io/lua-lunet/lunet-locks/tbio-core:v0.17.9-lunet.4-arm64
ARG LUNET_LOCKS_HEAD=unknown
FROM ${AOF_IMAGE} AS aof

FROM rust:1.85-slim AS native-base

ARG APT_REFRESH=off

RUN apt-get update \
    && if [ "$APT_REFRESH" != off ]; then apt-get upgrade -y; fi \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

FROM native-base AS native-deps

COPY --from=aof /out /opt/aof
ENV LUNET_LOCKS_AOF_LIB_DIR=/opt/aof/lib

WORKDIR /app
COPY .cargo/config.toml /usr/local/cargo/config.toml
COPY vendor /app/vendor
COPY ext/uvrr-core/Cargo.toml /app/ext/uvrr-core/Cargo.toml
COPY ext/lunet-locks-aof/Cargo.toml ext/lunet-locks-aof/build.rs /app/ext/lunet-locks-aof/
COPY ext/lunet-locks-aof/src /app/ext/lunet-locks-aof/src
COPY ext/advisory_lock/Cargo.toml ext/advisory_lock/Cargo.lock /app/ext/advisory_lock/
RUN mkdir -p /app/ext/uvrr-core/src /app/ext/advisory_lock/src \
    && printf '' > /app/ext/uvrr-core/src/lib.rs \
    && printf '' > /app/ext/advisory_lock/src/lib.rs \
    && cargo build --release --locked --manifest-path /app/ext/advisory_lock/Cargo.toml

FROM native-deps AS native

WORKDIR /app
COPY ext/uvrr-core /app/ext/uvrr-core
COPY ext/lunet-locks-aof /app/ext/lunet-locks-aof
COPY ext/advisory_lock /app/ext/advisory_lock
RUN cargo clean --release --manifest-path /app/ext/advisory_lock/Cargo.toml \
        --package vrr-core \
    && cargo build --release --locked --manifest-path /app/ext/advisory_lock/Cargo.toml

# The diagnosis-kit stage: the demo crate's binaries (the lease-sequencer
# node, the lease-client control client, the lease-load traffic generator)
# and the std-only rtt_probe. The demo crate needs let-chains (rustc 1.88+),
# so this stage pins its own rust above the cdylib stage's 1.85 and vendors
# from the demo crate's own Cargo.lock. It consumes the same tagged AOF
# artifact as the native adapter stage.
FROM rust:1.98-slim AS demo

ARG LUNET_LOCKS_HEAD=unknown
COPY --from=aof /out /opt/aof
ENV LUNET_LOCKS_AOF_LIB_DIR=/opt/aof/lib
ENV LUNET_LOCKS_HEAD=${LUNET_LOCKS_HEAD}

WORKDIR /app
COPY .cargo/config-demo.toml /usr/local/cargo/config.toml
COPY vendor-demo /app/vendor-demo
COPY ext/uvrr-core /app/ext/uvrr-core
COPY ext/lunet-locks-aof /app/ext/lunet-locks-aof
COPY ext/advisory_lock /app/ext/advisory_lock
COPY examples/lease-sequencer /app/examples/lease-sequencer
COPY tools/rtt_probe.rs /app/tools/rtt_probe.rs
RUN cargo build --release --locked --manifest-path /app/examples/lease-sequencer/Cargo.toml \
    && rustc --edition=2021 -O \
        -o /app/examples/lease-sequencer/target/release/rtt_probe \
        /app/tools/rtt_probe.rs

FROM debian:trixie-slim

ARG LUNET_VERSION=v0.10.0
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libgcc-s1 libluajit-5.1-2 libstdc++6 libuv1 chrony tcpdump iperf3 netcat-openbsd procps \
    && rm -rf /var/lib/apt/lists/* \
    && case "$(dpkg --print-architecture)" in \
        amd64) archive=lunet-linux-amd64.tar.gz; sha=979acc8669d644fd2276d328016f7a54916b444b652452abd6cd16fcbbe8ba94 ;; \
        arm64) archive=lunet-linux-arm64.tar.gz; sha=e651a9d7b75a53be631a74f5f7fffd332f8a2f3e388170385686369fe0c51d3f ;; \
        *) echo "unsupported Linux architecture: $(dpkg --print-architecture)" >&2; exit 1 ;; \
    esac \
    && curl --fail --location --retry 3 "https://github.com/lua-lunet/lunet/releases/download/${LUNET_VERSION}/${archive}" --output /tmp/lunet.tar.gz \
    && echo "${sha}  /tmp/lunet.tar.gz" | sha256sum -c - \
    && mkdir -p /opt/lunet \
    && tar -xzf /tmp/lunet.tar.gz -C /opt/lunet \
    && test -x /opt/lunet/lunet-run \
    && rm -f /tmp/lunet.tar.gz

COPY --from=native /app/ext/advisory_lock/target/release/liblunet_advisory_lock.so /app/lib/
COPY --from=demo /app/examples/lease-sequencer/target/release/lease-sequencer /app/demo/bin/
COPY --from=demo /app/examples/lease-sequencer/target/release/lease-client /app/demo/bin/
COPY --from=demo /app/examples/lease-sequencer/target/release/lease-load /app/demo/bin/
COPY --from=demo /app/examples/lease-sequencer/target/release/rtt_probe /app/demo/bin/
COPY examples/lease-sequencer/config/cluster.jsonl /app/demo/cluster.jsonl
COPY build /app/build
COPY docker/entrypoint.sh /app/docker/entrypoint.sh
COPY docker/cluster.jsonl /app/docker/cluster.jsonl
RUN chmod 755 /app/docker/entrypoint.sh

ENV LUNET_ADVISORY_LOCK_LIB=/app/lib/liblunet_advisory_lock.so
ENTRYPOINT ["/app/docker/entrypoint.sh"]
