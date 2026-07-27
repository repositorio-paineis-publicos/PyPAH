#!/bin/bash

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=============================="
echo "Iniciando ambiente PyPAH"
echo "=============================="

echo
echo "Verificando Docker..."

if ! docker info >/dev/null 2>&1; then
    if command -v docker-start >/dev/null; then
        echo "Docker não está rodando, iniciando via docker-start..."
        sudo docker-start
    else
        echo
        echo "Docker não está rodando e docker-start não foi encontrado."
        echo "Rode ./scripts/install.sh primeiro, ou inicie o Docker manualmente."
        echo
        exit 1
    fi
fi

echo
echo "Subindo Portainer (se ainda não estiver no ar)..."

if ! docker ps --format '{{.Names}}' | grep -q '^portainer$'; then
    docker volume inspect portainer_data >/dev/null 2>&1 || docker volume create portainer_data
    docker run -d \
      --name portainer \
      --restart unless-stopped \
      -p 9000:9000 \
      -v /var/run/docker.sock:/var/run/docker.sock \
      -v portainer_data:/data \
      portainer/portainer-ce:lts
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