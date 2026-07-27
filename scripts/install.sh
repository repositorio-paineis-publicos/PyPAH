#!/bin/bash

set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "===================================="
echo "PyPAH - Instalação do ambiente"
echo "===================================="

echo
echo "[1/9] Criando diretórios de dados..."

mkdir -p ~/Data/PyPAH
mkdir -p ~/Data/PyPAH/dims
mkdir -p ~/Data/PyPAH/bronze
mkdir -p ~/Data/PyPAH/silver
mkdir -p ~/Data/PyPAH/gold

echo
echo "[2/9] Verificando Docker..."

if ! command -v docker >/dev/null; then
    echo "Docker não instalado. Veja a seção 'Instalar o Docker Engine' do guia antes de continuar."
    exit 1
fi

echo
echo "[3/9] Instalando o script docker-start..."

if [ -f "$PROJECT_DIR/scripts/docker-start.sh" ]; then
    sudo cp "$PROJECT_DIR/scripts/docker-start.sh" /usr/local/bin/docker-start
    sudo chmod +x /usr/local/bin/docker-start
    echo "docker-start instalado em /usr/local/bin/docker-start"
else
    echo "AVISO: scripts/docker-start.sh não encontrado, pulando esta etapa."
fi

echo
echo "[4/9] Garantindo que o Docker Engine está rodando..."

if command -v docker-start >/dev/null; then
    sudo docker-start
else
    docker info >/dev/null 2>&1 || { echo "Docker não está rodando e docker-start não foi encontrado."; exit 1; }
fi

echo
echo "[5/9] Verificando Docker Compose..."

docker compose version

echo
echo "[6/9] Subindo o Portainer..."

if docker ps --format '{{.Names}}' | grep -q '^portainer$'; then
    echo "Portainer já está rodando."
else
    docker volume inspect portainer_data >/dev/null 2>&1 || \
    docker volume create portainer_data

    docker run -d \
      --name portainer \
      --restart unless-stopped \
      -p 9000:9000 \
      -v /var/run/docker.sock:/var/run/docker.sock \
      -v portainer_data:/data \
      portainer/portainer-ce:lts

    echo "Portainer no ar em http://localhost:9000 — crie o usuário admin no primeiro acesso."
fi

echo
echo "[7/9] Verificando .env.dev..."

if [ ! -f "$PROJECT_DIR/.env.dev" ]; then
    cp "$PROJECT_DIR/.env.example.dev" "$PROJECT_DIR/.env.dev"
    echo "Arquivo .env.dev criado a partir de .env.example.dev"
    echo "Revise as variáveis antes de continuar."
fi

echo
echo "[8/9] Validando docker-compose..."

docker compose --env-file "$PROJECT_DIR/.env.dev" --project-directory "$PROJECT_DIR" config >/dev/null

echo
echo "[9/9] Configurando cron do pipeline..."

CRON_CMD="0 3 20 * * $PROJECT_DIR/scripts/run_pipeline.sh >> $PROJECT_DIR/scripts/pipeline.log 2>&1"

if crontab -l 2>/dev/null | grep -qF "$PROJECT_DIR/scripts/run_pipeline.sh"; then
    echo "Entrada do cron já existe, mantendo como está."
else
    (crontab -l 2>/dev/null; echo "$CRON_CMD") | crontab -
    echo "Cron configurado: dia 20 de cada mês às 3h."
fi

echo
echo "Instalação concluída."
echo
echo "IMPORTANTE: em WSL sem systemd:"
echo "  - o Docker Engine precisa ser (re)iniciado a cada sessão com: sudo docker-start"
echo "  - o serviço de cron também não inicia sozinho: sudo service cron start"
echo
echo "Próximo passo: ./scripts/check_install.sh"