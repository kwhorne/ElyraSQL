# Multi-stage build producing a small image with a static ElyraSQL binary.
FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev gcc make perl linux-headers

WORKDIR /src
COPY . .
# Alpine's target is already musl -> the binary is statically linked.
RUN cargo build --release --locked -p elyra-cli \
    && strip target/release/elyrasql

FROM alpine:3.24

RUN addgroup -S elyrasql && adduser -S -G elyrasql -H elyrasql \
    && mkdir -p /var/lib/elyrasql && chown elyrasql:elyrasql /var/lib/elyrasql

COPY --from=builder /src/target/release/elyrasql /usr/local/bin/elyrasql

USER elyrasql
VOLUME ["/var/lib/elyrasql"]
EXPOSE 3307

ENV ELYRASQL_DATA=/var/lib/elyrasql/elyra.edb \
    ELYRASQL_LISTEN=0.0.0.0:3307 \
    RUST_LOG=info

ENTRYPOINT ["elyrasql"]
CMD ["serve"]
