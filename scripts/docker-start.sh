#!/bin/bash

# Inicia o Docker Engine manualmente em ambientes onde systemd não está
# disponível (ex.: WSL2 em Windows 10 mais antigos, sem suporte a
# systemd habilitado). Em produção (Linux com systemd), isso não é
# necessário — o Docker já roda como serviço gerenciado pelo systemd.

if pgrep -x dockerd > /dev/null; then
    echo "Docker já está rodando."
    exit 0
fi

echo "Iniciando Docker Engine..."
nohup dockerd >/tmp/dockerd.log 2>&1 &

# Espera o socket do Docker ficar disponível antes de devolver o controle
for i in $(seq 1 15); do
    if docker info >/dev/null 2>&1; then
        echo "Docker Engine no ar."
        exit 0
    fi
    sleep 1
done

echo "Docker não respondeu a tempo. Veja /tmp/dockerd.log para detalhes."
exit 1