# Minimal scratch image: static musl binary, passwd/group for the non-root
# user, a data dir, and a writable /tmp for sort/agg spills. No CA bundle
# needed — ureq verifies against compiled-in webpki-roots.
FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev gcc make perl linux-headers

RUN addgroup -S elyrasql && adduser -S -G elyrasql -H elyrasql \
    && mkdir -p /var/lib/elyrasql && chown elyrasql:elyrasql /var/lib/elyrasql

WORKDIR /src
COPY . .
# musl target => static binary; release profile already strips symbols.
RUN cargo build --release --locked -p elyra-cli

FROM scratch

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder --chown=elyrasql:elyrasql /var/lib/elyrasql /var/lib/elyrasql
# COPY creates the destination 0755 root:root; chown or spills cannot write.
COPY --from=builder --chown=elyrasql:elyrasql /tmp /tmp
COPY --from=builder /src/target/release/elyrasql /usr/local/bin/elyrasql

USER elyrasql
VOLUME ["/var/lib/elyrasql"]
EXPOSE 3307

ENV ELYRASQL_DATA=/var/lib/elyrasql/elyra.edb \
    ELYRASQL_LISTEN=0.0.0.0:3307 \
    RUST_LOG=info

ENTRYPOINT ["elyrasql"]
CMD ["serve"]
