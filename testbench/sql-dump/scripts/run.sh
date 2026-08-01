#!/usr/bin/env bash
set -euo pipefail

bench_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project="elyra-sql-dump-${UID}-$$"
compose=(docker compose --project-name "${project}" --file "${bench_dir}/compose.yaml")

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    cd "${bench_dir}"
    exec cargo run --locked --quiet -- "$@"
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "error: Docker is required to run the MySQL oracle" >&2
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "error: Docker is installed but its daemon is not reachable" >&2
    exit 1
fi

cleanup() {
    if ! "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1; then
        echo "warning: MySQL cleanup failed; run: docker compose --project-name ${project} --file ${bench_dir}/compose.yaml down --volumes --remove-orphans" >&2
    fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"${compose[@]}" up --detach --wait
binding="$("${compose[@]}" port mysql 3306)"
port="${binding##*:}"
container_id="$("${compose[@]}" ps --quiet mysql)"
image_id="$(docker inspect --format '{{.Image}}' "${container_id}")"

cd "${bench_dir}"
ELYRA_STRESS_MYSQL_URL="mysql://root:stress-secret@127.0.0.1:${port}/stress" \
    ELYRA_STRESS_MYSQL_IMAGE="${image_id}" \
    cargo run --locked --release -- "$@"
