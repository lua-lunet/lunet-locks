# Plain multi-stage Docker build, intentionally compatible with Colima's legacy
# builder: it uses no BuildKit-only mounts, secrets, or cache directives.
# `tools/docker_prepare_context.sh` vendors the transitive dependency sources
# and the uvrr-core submodule source (the lunet-locks/learner-era-fold patch
# branch the manifest's [patch] section names), so no git fetch happens inside
# Docker. The runtime stage carries the diagnosis kit: the NTP/capture/RTT
# tooling packages and the demo crate's binaries from the kit stage.
FROM rust:1.85-slim AS native

WORKDIR /app/ext/advisory_lock
COPY .cargo/config.toml /usr/local/cargo/config.toml
COPY vendor /app/vendor
COPY ext/uvrr-core /app/ext/uvrr-core
COPY ext/advisory_lock /app/ext/advisory_lock
RUN cargo build --release --locked

# The diagnosis-kit stage: the demo crate's binaries (the lease-sequencer
# node, the lease-client control client, the lease-load traffic generator)
# and the std-only rtt_probe. The demo crate needs let-chains (rustc 1.88+),
# so this stage pins its own rust above the cdylib stage's 1.85 and vendors
# from the demo crate's own Cargo.lock.
FROM rust:1.98-slim AS demo

WORKDIR /app
COPY .cargo/config-demo.toml /usr/local/cargo/config.toml
COPY vendor-demo /app/vendor-demo
COPY ext/uvrr-core /app/ext/uvrr-core
COPY ext/advisory_lock /app/ext/advisory_lock
COPY examples/lease-sequencer /app/examples/lease-sequencer
COPY tools/rtt_probe.rs /app/tools/rtt_probe.rs
RUN cargo build --release --locked --manifest-path /app/examples/lease-sequencer/Cargo.toml \
    && rustc --edition=2021 -O \
        -o /app/examples/lease-sequencer/target/release/rtt_probe \
        /app/tools/rtt_probe.rs

FROM debian:bookworm-slim

ARG LUNET_VERSION=v0.8.0
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libgcc-s1 libluajit-5.1-2 libstdc++6 libuv1 chrony tcpdump iperf3 netcat-openbsd procps \
    && rm -rf /var/lib/apt/lists/* \
    && case "$(dpkg --print-architecture)" in \
        amd64) archive=lunet-linux-amd64.tar.gz; sha=3f884dbe03d0435af08253970e09eb134a8f1ebefa2c5f8a51ec6a8d31ee3d69 ;; \
        arm64) archive=lunet-linux-arm64.tar.gz; sha=a7b009d31c6677b3aa4243b4811a988fb0d5fe80a8e82bd4a87ec9a83b0249a4 ;; \
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
