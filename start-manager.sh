#!/usr/bin/env bash

set -euo pipefail

CONTAINER_NAME="evolution_frontend"
IMAGE="evoapicloud/evolution-manager:latest"
PORT="127.0.0.1:3001:80"
NGINX_CONF="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manager-nginx.conf"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found"
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is not available"
  exit 1
fi

if [ ! -f "$NGINX_CONF" ]; then
  echo "manager-nginx.conf not found"
  exit 1
fi

if docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "Removing existing container $CONTAINER_NAME"
  docker rm -f "$CONTAINER_NAME" >/dev/null
fi

echo "Pulling $IMAGE"
docker pull "$IMAGE"

echo "Starting manager on http://127.0.0.1:3001"
docker run -d \
  --name "$CONTAINER_NAME" \
  --restart always \
  -p "$PORT" \
  -v "$NGINX_CONF:/etc/nginx/conf.d/nginx.conf:ro" \
  "$IMAGE" >/dev/null

docker ps --filter "name=^/${CONTAINER_NAME}$" --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
