# Plain multi-stage Docker build, intentionally compatible with Colima's legacy
# builder: it uses no BuildKit-only mounts, secrets, or cache directives.
# `tools/docker_prepare_context.sh` supplies a Cargo-vendored build context so
# the private vrr-core revision is never fetched from inside Docker.
FROM --platform=linux/arm64 rust:1.85-slim AS native

WORKDIR /app/ext/advisory_lock
COPY .cargo/config.toml /usr/local/cargo/config.toml
COPY vendor /app/vendor
COPY ext/advisory_lock /app/ext/advisory_lock
RUN cargo build --release --locked

FROM --platform=linux/arm64 debian:bookworm-slim

ARG LUNET_VERSION=v0.7.2
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl libgcc-s1 libluajit-5.1-2 libstdc++6 libuv1 \
    && rm -rf /var/lib/apt/lists/* \
    && case "$(dpkg --print-architecture)" in \
        amd64) archive=lunet-linux-amd64.tar.gz; sha=5bba15cf4da965e3aca70d0b92f4bd5183477c3fdce6acb90ea6a26c86e47149 ;; \
        arm64) archive=lunet-linux-arm64.tar.gz; sha=d17548abe23805002fc81f8b9d7f2a6c6f7ac6194a812e089bedfa3b455edf33 ;; \
        *) echo "unsupported Linux architecture: $(dpkg --print-architecture)" >&2; exit 1 ;; \
    esac \
    && curl --fail --location --retry 3 "https://github.com/lua-lunet/lunet/releases/download/${LUNET_VERSION}/${archive}" --output /tmp/lunet.tar.gz \
    && echo "${sha}  /tmp/lunet.tar.gz" | sha256sum -c - \
    && mkdir -p /opt/lunet \
    && tar -xzf /tmp/lunet.tar.gz -C /opt/lunet \
    && test -x /opt/lunet/lunet-run \
    && rm -f /tmp/lunet.tar.gz

COPY --from=native /app/ext/advisory_lock/target/release/liblunet_advisory_lock.so /app/lib/
COPY build /app/build
COPY docker/entrypoint.sh /app/docker/entrypoint.sh
RUN chmod 755 /app/docker/entrypoint.sh

ENV LUNET_ADVISORY_LOCK_LIB=/app/lib/liblunet_advisory_lock.so
ENTRYPOINT ["/app/docker/entrypoint.sh"]
