# Guia de Onboarding — PyPAH

Este guia cobre dois cenários: uma máquina **totalmente limpa** (sem WSL, sem Docker) e uma máquina que já tem parte do ambiente pronto. Se algum item já estiver instalado, pule para o próximo.

## Compatibilidade

| Item      | Recomendado                    | Observações                                                        |
|-----------|----------------------------------|---------------------------------------------------------------------|
| Windows   | Windows 11 ou Windows 10 22H2+ | Windows 10 21H2 funciona, mas pode não suportar `systemd` no WSL.   |
| WSL       | WSL2                            | Obrigatório.                                                         |
| Docker    | Docker Engine                  | Este guia **não usa Docker Desktop** — instala o Docker Engine direto na distro. |
| Init      | `systemd` ou `docker-start`    | Sem `systemd` disponível, use o script `docker-start` (Parte 1.4).  |
| Portainer | Opcional, recomendado          | Usado para gerenciar os containers visualmente.                     |

Se você não sabe se sua máquina tem `systemd` no WSL, assuma que não tem até confirmar o contrário — os passos abaixo funcionam nos dois casos.

---



# Parte 1 — Preparação da máquina
## Observação:

#### *Caso você já possua algum desses serviços já instalados e configurados, não é necessário que realize todo o passo a passo da Parte 1 (o resto é necessário). No entanto, recomendo que faça para garantia que está tudo operando corretamente.*


## 1.1 Instalar o WSL

No PowerShell, como administrador:

```powershell
wsl --install
```

Se o comando não existir ou falhar (Windows 10 mais antigo), habilite os recursos manualmente:

```powershell
dism /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
dism /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
```

Reinicie a máquina, depois:

```powershell
wsl --install -d Ubuntu
```

Confirme que está em WSL2:

```powershell
wsl -l -v
```

Esperado:

```
Ubuntu    VERSION 2
```

## 1.2 Instalar Git, curl e Docker Engine

Dentro do Ubuntu (WSL):

```bash
sudo apt update
sudo apt install -y git curl cron

curl -fsSL https://get.docker.com | sh

sudo usermod -aG docker $USER
```

Feche e abra o terminal de novo (ou saia e volte no WSL) para o grupo `docker` valer.

**Não rode** `sudo systemctl enable docker` / `sudo systemctl start docker` se sua distro não tem systemd — esses comandos simplesmente falham nesse cenário. É para isso que serve o próximo passo.

## 1.3 Gerar chave SSH e conectar ao GitHub

```bash
ssh-keygen -t ed25519
cat ~/.ssh/id_ed25519.pub
```

Copie a saída e adicione em GitHub → Settings → SSH and GPG keys → New SSH key.

Teste:

```bash
ssh -T git@github.com
```

Deve responder confirmando seu usuário do GitHub.

## 1.4 Iniciar o Docker manualmente (ambientes sem systemd)

Em vez de depender do systemd, use o script `docker-start` (ele já vem no repositório, em `scripts/docker-start.sh`, e o `install.sh` da Parte 2 o instala automaticamente em `/usr/local/bin/docker-start`). Ele verifica se o `dockerd` já está rodando e, se não estiver, sobe em background.

Depois de clonar o projeto (Parte 2) e rodar `./scripts/install.sh`, o comando para iniciar o Docker em qualquer sessão nova passa a ser:

```bash
sudo docker-start
```

## 1.5 Estrutura de pastas esperada

```
~/Projects/
    PyPAH/                  <- repositório clonado (contém scripts/, .env.dev, etc.)

~/Projects/Data_PyPAH/
    gold/
```

Recomendo clonar o projeto dentro do filesystem nativo da distro (`~/Projects/...`), não em `/mnt/c/...` — além de mais rápido, evita problemas de permissão e de final de linha (CRLF) em scripts `.sh`.

---

# Parte 2 — Preparação do projeto

## 2.1 Clonar o repositório

```bash
mkdir -p ~/Projects
cd ~/Projects

git clone https://github.com/repositorio-paineis-publicos/PyPAH.git
cd PyPAH

chmod +x scripts/*.sh
```

## 2.2 Instalar o ambiente

```bash
./scripts/install.sh
```

Esse script:
- cria `~/Data/PyPAH/bronze`, `silver`, `gold`
- instala o `docker-start` em `/usr/local/bin` e garante que o Docker Engine está rodando
- confirma o Docker Compose
- sobe o Portainer (se ainda não estiver rodando) em `http://localhost:9000`
- gera o `.env.dev` a partir de `.env.example.dev` (se ainda não existir) — **revise as variáveis antes de continuar**
- valida o `docker-compose` com esse `.env.dev`
- configura a entrada do crontab que roda `run_pipeline.sh` todo dia 20 de cada mês às 3h (idempotente — não duplica se já existir)

## 2.3 Configurar o Portainer

Na primeira vez que abrir `http://localhost:9000`:
1. Crie o usuário admin.
2. Selecione "Docker local" como ambiente.

## 2.4 Primeira checagem (pré-start)

```bash
./scripts/check_install.sh
```

Docker Engine, diretórios e Compose Config devem estar `OK`. Containers da API/Dashboard e os endpoints ainda vão aparecer `FALHOU` — normal, nada foi subido ainda.


## 2.5 Subir o ambiente

```bash
./scripts/start_dev.sh
```

Sobe Docker (via `docker-start`, se necessário), Portainer, API e Dashboard.

## 2.6 Segunda checagem (pós-start)

```bash
./scripts/check_install.sh
```

Agora tudo deve aparecer `OK`.

## 2.7 Rodar o pipeline manualmente

### Pelo script:
Será feito o download dos últimos 3 meses disponíveis no DataSUS
```bash
./scripts/run_pipeline.sh
```

### Direto pelo Compose (mais fácil de depurar, já que mostra a saída do container na hora):

Você escolhe qual o período você quer baixar, com data de início e fim.

```bash
docker compose --env-file .env.dev run --rm pypah-pipeline \
python -m Pipeline.pipeline_runner \
    --ano-inicio 2022 \
    --mes-inicio 1 \
    --ano-fim 2023 \
    --mes-fim 12
```
## 2.8 Iniciar o cron

```bash
sudo service cron start
sudo service cron status
```

Se quiser conferir ou editar a entrada manualmente:


```bash
crontab -e
```
Se desejar mudar o tempo de gatilho do cron, apenas falça alteração nos asteriscos
#### * * * * * -> Minuto/Hora/Dia do Mês/Mês/Dia da Semana
## 2.9 Testar se o cron está funcionando

Depois do horário agendado (dia 20, 3h por padrão), confira o log:

```bash
tail -f scripts/pipeline.log
```

Se aparecer a execução do pipeline ali, o cron está funcionando.

## 2.10 Acessar os serviços

| Serviço    | URL                              |
|------------|-----------------------------------|
| Dashboard  | http://localhost:8501             |
| API (docs) | http://localhost:8000/docs        |
| Portainer  | http://localhost:9000             |

## 2.11 Encerrar o ambiente

```bash
./scripts/stop_dev.sh
```

---

# Fluxo do dia a dia (depois da instalação inicial)

```bash
# 1. Abrir o Ubuntu (WSL) e garantir que o Docker está no ar
sudo docker-start

# 2. Entrar no projeto
cd ~/Projects/PyPAH

# 3. Subir tudo
./scripts/start_dev.sh

# 4. Trabalhar normalmente...

# 5. Ao final, se quiser encerrar
./scripts/stop_dev.sh
```

---

## Resumo — sequência completa (máquina limpa)

```bash
# Parte 1 — máquina
wsl --install
wsl -l -v

sudo apt update && sudo apt install -y git curl cron
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER    # feche e reabra o terminal depois

ssh-keygen -t ed25519
cat ~/.ssh/id_ed25519.pub        # adicionar no GitHub
ssh -T git@github.com

# Parte 2 — projeto
mkdir -p ~/Projects && cd ~/Projects
git clone https://github.com/repositorio-paineis-publicos/PyPAH.git
cd PyPAH
chmod +x scripts/*.sh

./scripts/install.sh
./scripts/check_install.sh       # pré-start

sudo service cron start

./scripts/start_dev.sh
./scripts/check_install.sh       # pós-start

./scripts/run_pipeline.sh        # opcional, teste manual
./scripts/stop_dev.sh            # ao final
```

## Scripts incluídos

```
scripts/
├── install.sh          # prepara diretórios, docker-start, Portainer, .env.dev, cron
├── check_install.sh    # valida Docker, containers, endpoints, cron e diretórios
├── docker-start.sh     # inicia o Docker Engine manualmente (ambientes sem systemd)
├── start_dev.sh        # sobe Docker/Portainer/API/Dashboard
├── stop_dev.sh         # derruba o ambiente
└── run_pipeline.sh     # roda o pipeline (manual ou via cron)
```

## Comandos após alteração

Caso realize alguma alteração nos scripts, env e etc, apenas rode:

```
chmod +x scripts/*.sh

./scripts/install.sh
./scripts/check_install.sh
./scripts/start_dev.sh
./scripts/check_install.sh
```