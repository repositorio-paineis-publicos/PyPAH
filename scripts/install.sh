#!/bin/bash

set -e

echo "===================================="
echo "PyPAH - Instalação do ambiente"
echo "===================================="

echo
echo "[1/7] Criando diretórios de dados..."

mkdir -p ~/Data/PyPAH
mkdir -p ~/Data/PyPAH/bronze
mkdir -p ~/Data/PyPAH/silver
mkdir -p ~/Data/PyPAH/gold

mkdir -p ~/Scripts

echo
echo "[2/7] Verificando Docker..."

if ! command -v docker >/dev/null; then
    echo "Docker não instalado. Instale o Docker antes de continuar."
    exit 1
fi

echo
echo "[3/7] Verificando Docker Compose..."

docker compose version

echo
echo "[4/7] Criando volume do Portainer..."

docker volume inspect portainer_data >/dev/null 2>&1 || \
docker volume create portainer_data

echo
echo "[5/7] Verificando .env.dev..."

if [ ! -f ".env.dev" ]; then
    cp .env.example.dev .env.dev
    echo "Arquivo .env.dev criado a partir de .env.example.dev"
    echo "Revise as variáveis antes de continuar."
fi

echo
echo "[6/7] Validando docker-compose..."

docker compose --env-file .env.dev config >/dev/null

echo
echo "[7/7] Finalizado."

echo
echo "Instalação concluída."
echo "Próximo passo: ./scripts/check_install.sh"