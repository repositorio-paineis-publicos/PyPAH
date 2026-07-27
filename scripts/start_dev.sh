#!/bin/bash

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=============================="
echo "Iniciando ambiente PyPAH"
echo "=============================="

echo
echo "Verificando Docker..."

if ! docker info >/dev/null 2>&1
then
    echo
    echo "Docker não está rodando."
    echo
    echo "Abra outro terminal e execute:"
    echo
    echo "sudo dockerd"
    echo
    exit 1
fi

echo
echo "Subindo API..."

docker compose \
    --env-file "$PROJECT_DIR/.env.dev" \
    up -d pypah-api

echo
echo "Subindo Dashboard..."

docker compose \
    --env-file "$PROJECT_DIR/.env.dev" \
    up -d pypah-app

echo
echo "=============================="
echo "Dashboard"
echo "http://localhost:8501"
echo
echo "API"
echo "http://localhost:8000/docs"
echo
echo "Portainer"
echo "http://localhost:9000"
echo "=============================="
echo
echo "Para validar tudo: ./scripts/check_install.sh"