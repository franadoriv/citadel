# syntax=docker/dockerfile:1.7
#
# The released Citadel server image is a Linux, Lua-capable runtime image. Game
# code, configuration, maps, and database state are mounted at runtime; they do
# not enter this build context (see .dockerignore).

ARG RUST_VERSION=1.92.0
FROM --platform=$TARGETPLATFORM rust:${RUST_VERSION}-bookworm AS builder

ARG RUST_VERSION
# The repository's development toolchain tracks `stable`, but released images
# must stay pinned to the Rust version selected by their base image.
ENV RUSTUP_TOOLCHAIN=${RUST_VERSION}

RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential cmake libclang-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

# Build natively for each Buildx target platform. The checked-in lockfile makes
# a release image resolve the same Rust dependency graph as local CI.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release --bin citadel && \
    install -D -m 0755 target/release/citadel /out/citadel

FROM debian:bookworm-slim AS runtime

ARG VERSION=dev
ARG REVISION=unknown
ARG SOURCE=
ARG LICENSE=NOASSERTION

LABEL org.opencontainers.image.title="Citadel" \
      org.opencontainers.image.description="Rust-native realtime game server" \
      org.opencontainers.image.source="${SOURCE}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="${LICENSE}"

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl libstdc++6 tini && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 10001 citadel && \
    useradd --uid 10001 --gid citadel --home-dir /citadel --shell /usr/sbin/nologin --create-home citadel && \
    mkdir -p /citadel/config /citadel/game /citadel/maps /citadel/data && \
    chown citadel:citadel /citadel/data

COPY --from=builder /out/citadel /citadel/citadel
COPY examples/docker/citadel.toml /citadel/config/citadel.toml

WORKDIR /citadel
USER citadel

EXPOSE 7350/tcp 7351/udp 7352/tcp 7353/udp

HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD curl --fail --silent --show-error http://127.0.0.1:7350/health || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "/citadel/citadel"]
CMD ["--config", "/citadel/config/citadel.toml", "serve"]
