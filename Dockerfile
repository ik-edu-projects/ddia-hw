FROM rust:1.85-slim-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim AS runtime
COPY --from=builder /app/target/release/ddia-1 /usr/local/bin/ddia-1
ENV BIND_ADDR=0.0.0.0:3030
USER 10001:10001
EXPOSE 3030
CMD ["/usr/local/bin/ddia-1"]
