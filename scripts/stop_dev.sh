#!/bin/bash

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

cd "$PROJECT_DIR"

echo "Encerrando ambiente PyPAH..."

docker compose --env-file .env.dev down

echo "Ambiente encerrado."