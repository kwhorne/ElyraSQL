#!/usr/bin/env bash
# ElyraSQL deploy script for Ubuntu 24.04+.
#
# Builds a release binary, installs it, creates a dedicated system user and
# data directory, installs the systemd unit, and starts the service.
#
# Usage (run from the repo root, as a sudo-capable user):
#   sudo ./packaging/deploy.sh
#
# Configure via environment before running, e.g.:
#   ELYRASQL_USER=root ELYRASQL_PASSWORD=secret \
#   ELYRASQL_LISTEN=0.0.0.0:3307 sudo -E ./packaging/deploy.sh

set -euo pipefail

BIN=/usr/local/bin/elyrasql
DATA_DIR=/var/lib/elyrasql
UNIT=/etc/systemd/system/elyrasql.service
SVC_USER=elyrasql

echo "==> Building release binary"
cargo build --release
install -m 0755 target/release/elyrasql "$BIN"

echo "==> Creating system user and data directory"
id -u "$SVC_USER" >/dev/null 2>&1 || \
    useradd --system --home "$DATA_DIR" --shell /usr/sbin/nologin "$SVC_USER"
install -d -o "$SVC_USER" -g "$SVC_USER" "$DATA_DIR"

echo "==> Installing systemd unit"
cp packaging/elyrasql.service "$UNIT"

# Optional: inject credentials/listen from the environment as a drop-in.
DROPIN_DIR=/etc/systemd/system/elyrasql.service.d
mkdir -p "$DROPIN_DIR"
{
    echo "[Service]"
    [ -n "${ELYRASQL_LISTEN:-}" ]   && echo "Environment=ELYRASQL_LISTEN=${ELYRASQL_LISTEN}"
    [ -n "${ELYRASQL_USER:-}" ]     && echo "Environment=ELYRASQL_USER=${ELYRASQL_USER}"
    [ -n "${ELYRASQL_PASSWORD:-}" ] && echo "Environment=ELYRASQL_PASSWORD=${ELYRASQL_PASSWORD}"
    [ -n "${ELYRASQL_TLS_CERT:-}" ] && echo "Environment=ELYRASQL_TLS_CERT=${ELYRASQL_TLS_CERT}"
    [ -n "${ELYRASQL_TLS_KEY:-}" ]  && echo "Environment=ELYRASQL_TLS_KEY=${ELYRASQL_TLS_KEY}"
} > "$DROPIN_DIR/overrides.conf"

echo "==> Starting service"
systemctl daemon-reload
systemctl enable --now elyrasql

echo "==> Status"
systemctl --no-pager status elyrasql || true
echo "Done. ElyraSQL is running. Check logs with: journalctl -u elyrasql -f"
