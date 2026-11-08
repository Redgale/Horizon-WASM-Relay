# horizon-web-relay for Koyeb (or any container host): listens on $PORT, plain WebSockets
# behind the host's TLS (wss://).
FROM rust:1-bookworm AS build
WORKDIR /relay
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /relay/target/release/horizon-web-relay /usr/local/bin/horizon-web-relay
ENV PORT=8000
EXPOSE 8000
CMD ["horizon-web-relay"]
