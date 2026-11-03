#!/usr/bin/env bash
# Runs on the VM. Starts the given image and rolls back if the stack doesn't become healthy.
#   bash /opt/crypto-streaming/deploy/deploy.sh ghcr.io/owner/crypto-streaming-platform:<sha>
set -euo pipefail
cd "$(dirname "$0")/.."

NEW_IMAGE="$1"
PREVIOUS_IMAGE="$(grep -E '^APP_IMAGE=' .env | cut -d= -f2- || true)"

compose() { docker compose -f docker-compose.prod.yaml "$@"; }

set_image() {
    sed -i '/^APP_IMAGE=/d' .env
    echo "APP_IMAGE=$1" >> .env
}

start_stack() {
    compose pull --quiet
    compose up -d --no-build --wait --wait-timeout 60 postgres
    # every migration is safe to run again, so just apply them all, in order
    for migration in migrations/*.sql; do
        compose exec -T postgres sh -c 'psql -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < "$migration"
    done
    compose up -d --no-build --remove-orphans --wait --wait-timeout 240
}

set_image "$NEW_IMAGE"
if start_stack; then
    echo "Deployed $NEW_IMAGE"
    docker image prune -f > /dev/null   # old images would slowly fill the disk
    exit 0
fi

echo "New version did not become healthy." >&2
compose ps >&2
if [ -n "$PREVIOUS_IMAGE" ]; then
    echo "Rolling back to $PREVIOUS_IMAGE" >&2
    set_image "$PREVIOUS_IMAGE"
    start_stack
fi
exit 1
