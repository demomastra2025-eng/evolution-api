#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

LOG_DIR="${EVOLUTION_LOG_DIR:-$ROOT_DIR/logs}"
TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"
LOG_FILE="${EVOLUTION_LOG_FILE:-$LOG_DIR/evolution-direct-${TIMESTAMP}.log}"

mkdir -p "$LOG_DIR"
ln -sfn "$LOG_FILE" "$LOG_DIR/evolution-direct-latest.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "Logging Evolution API output to $LOG_FILE"

if ! command -v node >/dev/null 2>&1; then
  echo "node is not installed"
  exit 1
fi

if ! command -v npm >/dev/null 2>&1; then
  echo "npm is not installed"
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required because postgres/redis are running in docker"
  exit 1
fi

if [ ! -f .env ]; then
  echo ".env not found"
  exit 1
fi

read_env() {
  local key="$1"
  local value

  value="$(grep -E "^${key}=" .env | head -n 1 | cut -d '=' -f 2-)"
  value="${value%\"}"
  value="${value#\"}"
  value="${value%\'}"
  value="${value#\'}"

  printf '%s' "$value"
}

POSTGRES_DATABASE="$(read_env POSTGRES_DATABASE)"
POSTGRES_USERNAME="$(read_env POSTGRES_USERNAME)"
POSTGRES_PASSWORD="$(read_env POSTGRES_PASSWORD)"

if [ -z "$POSTGRES_DATABASE" ] || [ -z "$POSTGRES_USERNAME" ] || [ -z "$POSTGRES_PASSWORD" ]; then
  echo "POSTGRES_DATABASE / POSTGRES_USERNAME / POSTGRES_PASSWORD must be set in .env"
  exit 1
fi

if ! docker inspect onelink-postgres-1 >/dev/null 2>&1; then
  echo "onelink-postgres-1 is not available"
  exit 1
fi

if ! docker inspect onelink-redis-1 >/dev/null 2>&1; then
  echo "onelink-redis-1 is not available"
  exit 1
fi

DB_EXISTS="$(docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" onelink-postgres-1 \
  psql -U "${POSTGRES_USERNAME}" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DATABASE}'" | tr -d '[:space:]')"

if [ "$DB_EXISTS" != "1" ]; then
  echo "Creating database ${POSTGRES_DATABASE}"
  docker exec -e PGPASSWORD="${POSTGRES_PASSWORD}" onelink-postgres-1 \
    psql -U "${POSTGRES_USERNAME}" -d postgres -c "CREATE DATABASE ${POSTGRES_DATABASE}"
fi

if [ ! -d node_modules ]; then
  echo "Installing dependencies"
  npm ci
fi

echo "Generating prisma client"
npm run db:generate

echo "Running database migrations"
npm run db:deploy

echo "Starting Evolution API directly on http://127.0.0.1:8080"
echo "Public manager can be started separately on http://127.0.0.1:3001"
npm run start
