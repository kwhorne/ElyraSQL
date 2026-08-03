# Multi-stage build producing a minimal scratch-based image with a static
# ElyraSQL binary. The runtime stage is `scratch` (empty) and contains only
# the statically linked musl binary, passwd/group entries for the non-root
# user, an empty data directory, and a writable /tmp for sort/aggregation
# spills. No CA certificate bundle is needed: the HTTPS client (ureq)
# verifies against webpki-roots compiled into the binary, not the system store.
FROM rust:1-alpine AS builder

RUN apk add --no-cache musl-dev gcc make perl linux-headers

# Create the non-root user and data dir here so they can be copied into the
# scratch runtime stage. /tmp already exists in rust:1-alpine and is copied
# separately below with ownership fixed for the non-root user.
RUN addgroup -S elyrasql && adduser -S -G elyrasql -H elyrasql \
    && mkdir -p /var/lib/elyrasql && chown elyrasql:elyrasql /var/lib/elyrasql

WORKDIR /src
COPY . .
# Alpine's target is already musl -> the binary is statically linked.
# The release profile already strips symbols (strip = true).
RUN cargo build --release --locked -p elyra-cli

FROM scratch

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder --chown=elyrasql:elyrasql /var/lib/elyrasql /var/lib/elyrasql
# COPY creates the destination fresh as 0755 root:root, so ownership must be
# set here or the non-root user cannot spill sort runs into /tmp.
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
