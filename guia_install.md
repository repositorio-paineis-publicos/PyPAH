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
~/Projeto_SUS/
    PyPAH/                  <- repositório clonado (contém scripts/, .env.dev, etc.)

~/Projeto_SUS/Data_PyPAH/
    gold/
```

Recomendo clonar o projeto dentro do filesystem nativo da distro (`~/Projects/...`), não em `/mnt/c/...` — além de mais rápido, evita problemas de permissão e de final de linha (CRLF) em scripts `.sh`.

---

# Parte 2 — Preparação do projeto
## Observação

### *A partir daqui repita tudo como está sendo mostrado, caso queira fazer diferente, será necessário mudar em outros scripts também.*

## 2.1 Clonar o repositório

```bash
mkdir -p ~/Projeto_SUS
cd ~/Projeto_SUS

git clone https://github.com/repositorio-paineis-publicos/PyPAH.git
cd PyPAH

chmod +x scripts/*.sh
```

## 2.2 Instalar o ambiente

```bash
./scripts/install.sh
```

Esse script:
- cria `~/Projeto_SUS/Data_PyPAH/ gold`
- instala o `docker-start` em `/usr/local/bin` e garante que o Docker Engine está rodando
- confirma o Docker Compose
- sobe o Portainer (se ainda não estiver rodando) em `http://localhost:9000`

- gera o `.env.dev` a partir de `.env.example.dev` (se ainda não existir)

## Amostragem da API (USE_SAMPLE / SAMPLE_ROWS)

A API pode servir o dataset completo ou uma amostra reduzida, dependendo do
ambiente. Isso é controlado por duas variáveis no `.env`:

- `USE_SAMPLE` (`true` / `false`) — se `true`, a API lê um arquivo de amostra
  em vez do `consolidated.parquet` completo. Em dev isso normalmente fica
  `false` (você quer ver os dados reais); em produção/demo, `true` deixa a
  API mais leve e rápida.
- `SAMPLE_ROWS` — quantas linhas tem a amostra (padrão: `10000`). Só tem
  efeito quando `USE_SAMPLE=true`, e só é usado pelo **pipeline** (é ele
  quem gera o arquivo de amostra, não a API).

A amostra não é sorteada do zero a cada request — ela é gerada **uma vez**,
pelo pipeline, de forma estratificada por Ano/Mês (garantindo que nenhum mês
fique de fora da amostra) e salva como `consolidated_sample_<SAMPLE_ROWS>.parquet`
junto do `consolidated.parquet` completo. A API só escolhe qual dos dois
arquivos ler, com base em `USE_SAMPLE` — ela não sabe nem precisa saber que
existe amostragem.

Se você mudar `SAMPLE_ROWS`, a próxima execução do pipeline (`./scripts/run_pipeline.sh`)
gera um novo arquivo de amostra com esse tamanho; o antigo não é apagado
automaticamente.

### Altere as variáveis antes de continuar, principalmente o caminho do Data_PyPAH

```
APP_ENV=development

STORAGE=local

# Local onde a pasta Data_PyPAH está localizada
PYPAH_DATA_ROOT=/home/seu_usuario/Projeto_SUS/Data_PyPAH   

# Controla se a API le o dataset completo ou uma amostra reduzida.
# Em dev, mantenha em false para trabalhar com os dados reais.
USE_SAMPLE=false

# Nome do serviço no docker-compose (rede interna do Docker)
API_URL=http://pypah-api:8000

GROQ_API_KEY=...
```
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

#### *Se for sua primeira execução do projeto e você não tiver dados baixados para alimentar a API/Dasboard, a API responderá que falhou, então apenas rode a pipeline(próximo passo do guia) e após o encerramento, a API e Dashboard já estarão funcionando alimentados, lembre de atualizar a página no navegador para evitar que a página desatualizada esteja na tela. Se roda o check de novo após a ingestão, a API responderá com `OK`, como todos os outros.*

## 2.7 Iniciar o cron

```
sudo service cron start
sudo service cron status
```

Se quiser conferir ou editar a entrada manualmente:


```bash
crontab -e
```
Se desejar mudar o tempo de gatilho do cron, apenas faça alteração nos asteriscos
#### * * * * * -> Minuto/Hora/Dia do Mês/Mês/Dia da Semana
## 2.8 Testar se o cron está funcionando

Depois do horário agendado (dia 20, 3h por padrão), confira o log:

```bash
tail -f scripts/pipeline.log
```

Se aparecer a execução do pipeline ali, o cron está funcionando.

## 2.9 Acessar os serviços

| Serviço    | URL                              |
|------------|-----------------------------------|
| Dashboard  | http://localhost:8501             |
| API (docs) | http://localhost:8000/docs        |
| Portainer  | http://localhost:9000             |

## 2.10 Encerrar o ambiente

```bash
./scripts/stop_dev.sh
```

---

# Parte 3 - Rodar o pipeline manualmente

## Pelo script:
Será feito o download dos últimos 3 meses disponíveis no DataSUS
```bash
./scripts/run_pipeline.sh
```

## Direto pelo Compose (mais fácil de depurar, já que mostra a saída do container na hora):

Você escolhe qual o período você quer baixar, com data de início e fim.

```bash
docker compose --env-file .env.dev run --rm pypah-pipeline \
python -m Pipeline.pipeline_runner \
    --ano-inicio 2022 \
    --mes-inicio 1 \

```
### Informações sobre os parâmtros
Se não for adicionado data de fim no docker compose, ele baixa da data de início até o último disponível, com o parâmetro de fim, ele encerra na data limite
```
--ano-fim 2023 \
--mes-fim 12
```
Se você quiser não fazer o download das tabelas dimensão, adicione como primeiro parâmetro do docker compose:

```
--skip-dims
```

Se quiser forçar a criação de outro consolidated, mesmo que não ocorra nenhum download novo
```
--force-consolidate
```
## Limpando Cache dos Containers
Após rodar o fluxo, rode esses dois comandos para que o cache dos containers seja resetado e após atualização manual, a API e o Dashboard não estejam consumindo dados antigos.
```
docker compose --env-file .env.dev restart pypah-api
docker compose --env-file .env.dev restart pypah-app
```

## Limpando o computador de todos os containers

Caso você queira apagar todos os volumes, imagens, containers e resetar o ambiente para como se fosse zerado, ou seja,use esses comandos para limpeza:

```
docker compose down --env-file .env.dev down -v
docker rm -f $(docker ps -aq) 2>/dev/null
docker rmi -f $(docker images -aq) 2>/dev/null
docker volume rm $(docker volume ls -q) 2>/dev/null
docker builder prune -af
```
Com isso, será removido a imagem do *pypah-api*, *pypah-app*, *pypah-pipeline* e o volume *portainer_data*, forçando tudo a ser reconstruído na próxima instalação. É o cenário mais próximo de uma máquina "limpa" sem reinstalar o Docker.

# Fluxo do dia a dia (depois da instalação inicial)

```bash
# 1. Abrir o Ubuntu (WSL) e garantir que o Docker está no ar
sudo docker-start

# 2. Entrar no projeto
cd ~/Projeto_SUS/PyPAH

# 3. Subir tudo
./scripts/start_dev.sh

# 4. Trabalhar normalmente...

# 5. Se tiver rodado a pipeline manualmente...
docker compose --env-file .env.dev restart pypah-api
docker compose --env-file .env.dev restart pypah-app

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
mkdir -p ~/Projeto_SUS && cd ~/Projeto_SUS
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

## Comandos interessantes
Sempre que for iniciar o projeto
```
./scripts/start_dev.sh
```
Caso queira fazer um check que tudo está operando normalmente:

```
./scripts/check_install.sh  
```

Caso realize uma nova ingestão, recomendo que recrie o container da API para que ela atualize os dados que ela está servindo, pois com o uso de cache, pode ser que os dados estejam desatualizados, então apenas rode esse comando para reiniciar o container com o cache limpo:

```
docker compose --env-file .env.dev restart pypah-api
```


Caso realize uma nova ingestão, recomendo que recrie o container da APP para que ela atualize os dados que ela está servindo, pois com o uso de cache, pode ser que os dados estejam desatualizados, então apenas rode esse comando para reiniciar o container com o cache limpo:

```
docker compose --env-file .env.dev restart pypah-app
```

### Aviso
Caso no seu uso aconteça algum problema do tipo:

```
WARN[0000] The "PYPAH_DATA_ROOT" variable is not set. Defaulting to a blank string. 
invalid spec: :/datasets: empty section between colons
```

O problema foi que ou você não definiu no *.env* (seja prod ou dev) o *PYPAH_DATA_ROOT* ou no comando que usou, não setou qual *.env* usou e isso você pode definir com:

```
#sete o nome do env que usar sem as aspas

--env-file ".env.usado" 
```

