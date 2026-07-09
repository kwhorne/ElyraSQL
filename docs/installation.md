# Installation

ElyraSQL targets **Ubuntu 24.04+** for production but runs anywhere Rust does.

## Static binaries

Each [release](https://github.com/kwhorne/ElyraSQL/releases) ships fully static
`musl` binaries for `x86_64` and `aarch64` — no libc or other runtime
dependency.

```bash
VER=0.7.0
ARCH=x86_64   # or aarch64
curl -L -o elyrasql.tar.gz \
  https://github.com/kwhorne/ElyraSQL/releases/download/v${VER}/elyrasql-${VER}-linux-${ARCH}.tar.gz
tar xzf elyrasql.tar.gz
cd elyrasql-${VER}-linux-${ARCH}
./elyrasql version
```

Each archive contains the `elyrasql` binary, `README`, `LICENSE`, and a sample
`elyrasql.service` systemd unit. Verify integrity with the published
`.sha256` file.

## Docker

Multi-arch image (`amd64` + `arm64`) on the GitHub Container Registry:

```bash
docker pull ghcr.io/kwhorne/elyrasql:0.7.0   # or :latest
docker run -p 3307:3307 -v elyra:/var/lib/elyrasql ghcr.io/kwhorne/elyrasql:0.7.0
```

The image is ~15 MB, runs as a non-root user, stores data in the
`/var/lib/elyrasql` volume, and is configured via environment variables (see
[Configuration](configuration.md)).

## Build from source

Requires Rust 1.82+.

```bash
git clone https://github.com/kwhorne/ElyraSQL.git
cd ElyraSQL
cargo build --release
./target/release/elyrasql serve
```

To build a static binary yourself:

```bash
rustup target add x86_64-unknown-linux-musl
sudo apt-get install -y musl-tools
cargo build --release --target x86_64-unknown-linux-musl -p elyra-cli
```

## Systemd (Ubuntu)

The repository ships a hardened unit and an install script:

```bash
sudo ./packaging/deploy.sh
# or with credentials + TLS:
ELYRASQL_USER=root ELYRASQL_PASSWORD=secret \
  ELYRASQL_LISTEN=0.0.0.0:3307 sudo -E ./packaging/deploy.sh
```

See [Deployment](deployment.md) for details.
