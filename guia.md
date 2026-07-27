# Guia de Onboarding — Preparando o ambiente e subindo o PyPAH

Este guia assume uma máquina limpa (Ubuntu/WSL) sem nenhuma configuração prévia do projeto.

## 0. Pré-requisitos

Antes de tudo, garanta que a máquina tem:

```bash
sudo apt update
sudo apt install -y git curl
```

Docker precisa estar instalado. Se ainda não estiver:

```bash
curl -fsSL https://get.docker.com | sh
```

Verifique:

```bash
git --version
docker --version
docker compose version
```

## 1. Clonar o projeto

```bash
git clone https://github.com/repositorio-paineis-publicos/PyPAH.git
cd PyPAH
```

## 2. Dar permissão de execução aos scripts

```bash
chmod +x scripts/*.sh
```

## 3. Instalar o ambiente

```bash
./scripts/install.sh
```

Esse script:
- cria a estrutura de diretórios de dados (`~/Data/PyPAH/bronze`, `silver`, `gold`)
- confirma que Docker e Docker Compose estão presentes
- cria o volume do Portainer
- gera o `.env.dev` a partir de `.env.example.dev` (se ainda não existir)
- valida o `docker-compose` com esse `.env.dev`

Se o `.env.dev` for criado nesse passo, **abra o arquivo e revise as variáveis** (chaves de API, credenciais, etc.) antes de seguir.

## 4. Primeira checagem (pré-start)

```bash
./scripts/check_install.sh
```

Nesse momento, os itens de **Docker Engine**, **estrutura de diretórios** e **Compose Config** devem aparecer `OK`. Os itens de **containers (API/Dashboard/Portainer)**, **cron** e **endpoints** ainda vão aparecer `FALHOU` — isso é esperado, porque nada foi iniciado ainda.

Se o Docker Engine não estiver rodando, abra outro terminal e execute:

```bash
sudo dockerd
```

## 5. Subir o ambiente

```bash
./scripts/start_dev.sh
```

Esse script sobe os containers da API e do Dashboard via Docker Compose e imprime os links de acesso.

## 6. Segunda checagem (pós-start)

```bash
./scripts/check_install.sh
```

Agora todos os itens devem aparecer `OK`, incluindo containers ativos e os endpoints respondendo.

## 7. Acessar os serviços

| Serviço    | URL                              |
|------------|-----------------------------------|
| Dashboard  | http://localhost:8501             |
| API (docs) | http://localhost:8000/docs        |
| Portainer  | http://localhost:9000             |

## 8. Rodar o pipeline manualmente (opcional)

```bash
./scripts/run_pipeline.sh
```

Use esse comando para forçar uma execução do pipeline fora do horário do cron, ou para testar se ele está funcionando corretamente logo após a instalação.

## 9. Encerrar o ambiente

```bash
./scripts/stop_dev.sh
```

Derruba os containers subidos pelo `start_dev.sh`.

---

## Resumo — sequência completa de comandos

```bash
sudo apt update && sudo apt install -y git curl
curl -fsSL https://get.docker.com | sh

git clone <url-do-repositorio>
cd PyPAH

chmod +x scripts/*.sh

./scripts/install.sh
./scripts/check_install.sh      # checagem pré-start (containers ainda vão falhar)

./scripts/start_dev.sh
./scripts/check_install.sh      # checagem pós-start (tudo deve estar OK)

./scripts/run_pipeline.sh       # opcional, roda o pipeline manualmente

./scripts/stop_dev.sh           # ao final, para encerrar
```

## Scripts incluídos

```
scripts/
├── install.sh          # prepara diretórios, .env.dev, volumes e valida o compose
├── check_install.sh    # valida Docker, containers, endpoints, cron e diretórios
├── start_dev.sh        # sobe API e Dashboard
├── stop_dev.sh          # derruba o ambiente
└── run_pipeline.sh     # roda o pipeline (manual ou via cron)
```