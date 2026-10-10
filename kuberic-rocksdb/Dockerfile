FROM rust:1.98.1-bookworm AS build
ARG CARGO_BUILD_JOBS=2
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}
RUN apt-get update && apt-get install -y --no-install-recommends clang libclang-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY kuberic-page ./kuberic-page
COPY kuberic-rocksdb ./kuberic-rocksdb
COPY kuberic-rustfs ./kuberic-rustfs
RUN cargo build --locked --release -p kuberic-rocksdb

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/kuberic && chown 10001:10001 /var/lib/kuberic
COPY --from=build /src/target/release/kuberic-rocksdb /usr/local/bin/kuberic-rocksdb
USER 10001:10001
EXPOSE 8080 50051 50052
ENTRYPOINT ["/usr/local/bin/kuberic-rocksdb"]
