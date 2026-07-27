#!/bin/bash

# Não usamos 'set -e' aqui de propósito: queremos rodar todas as checagens
# e reportar todas as falhas de uma vez, em vez de parar na primeira.

FAIL=0

check() {
    local label="$1"
    shift
    printf "%-28s" "$label"
    if "$@" >/dev/null 2>&1; then
        echo "OK"
    else
        echo "FALHOU"
        FAIL=1
    fi
}

echo
echo "========== CHECK PyPAH =========="
echo

check "Docker Engine........."   docker info
check "Docker CLI............"   docker version
check "Docker Compose........"   docker compose version
check "Compose Config........"   docker compose --env-file .env.dev config

check "Diretório Dados......."   test -d ~/Data/PyPAH
check "Bronze................"   test -d ~/Data/PyPAH/bronze
check "Silver................"   test -d ~/Data/PyPAH/silver
check "Gold.................."   test -d ~/Data/PyPAH/gold

check "Container API........."   bash -c "docker ps --format '{{.Names}}' | grep -q pypah-api"
check "Container Dashboard..."   bash -c "docker ps --format '{{.Names}}' | grep -q pypah-app"
check "Container Portainer..."   bash -c "docker ps --format '{{.Names}}' | grep -q portainer"

check "Cron ativo............"   bash -c "service cron status || pgrep cron"

check "API respondendo......."   curl -fsS http://localhost:8000/api/anos
check "Dashboard acessível..."   curl -fsS http://localhost:8501

echo
if [ "$FAIL" -eq 0 ]; then
    echo "Tudo configurado corretamente."
else
    echo "Uma ou mais checagens falharam. Veja os itens marcados como FALHOU acima."
    echo "Containers e cron só ficam OK depois de rodar ./scripts/start_dev.sh"
    exit 1
fi