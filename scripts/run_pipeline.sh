#!/bin/bash

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo
echo "================================="
echo "Pipeline iniciado"
date
echo "================================="

cd "$PROJECT_DIR"

docker compose \
    --env-file .env.dev \
    run --rm pypah-pipeline

echo "Atualizando o cache da API"
date
docker compose \
    --env-file .env.dev \
    restart pypah-api

echo "Atualizando o cache do Dashboard"
date
docker compose \
    --env-file .env.dev \
    restart pypah-app



echo
echo "================================="
echo "Pipeline finalizado"
date
echo "================================="