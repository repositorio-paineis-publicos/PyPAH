"""
pipeline_runner.py
------------------
Orquestrador do pipeline incremental do PyPAH.

Fluxo de execucao:
  1. Lista particoes (ano/mes) existentes no R2.
  2. Verifica se consolidated.parquet existe no R2.
  3. Calcula meses novos disponiveis no FTP do DATASUS.
  4. Para cada mes novo: baixa .dbc -> converte -> trata -> agrega -> upload da particao.
  5. Se houve novos meses OU consolidated nao existe: gera e faz upload do consolidated.parquet.
  6. Atualiza tabelas dimensao (rotulos).

Variaveis de ambiente obrigatorias:
  R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, R2_BUCKET

Uso:
  # Modo incremental (so meses novos):
  python -m Pipeline.pipeline_runner

  # Carga historica:
  python -m Pipeline.pipeline_runner --ano-inicio 2018 --mes-inicio 1 --ano-fim 2024 --mes-fim 12

  # Forcar regeneracao do consolidated sem processar meses novos:
  python -m Pipeline.pipeline_runner --force-consolidate
"""

import os
import shutil
import argparse
import logging
from datetime import date
from pathlib import Path
from dateutil.relativedelta import relativedelta

import boto3
from botocore.config import Config
from dotenv import load_dotenv

from Pipeline.fun_sia import (
    baixar_dbc,
    conv_dbc_para_pqt,
    tratar_dados_sia,
    estab_ce_label,
    download_proc_label,
    col_interesse,
)
from Pipeline.gold import processar_gold_particionado, consolidar_gold_r2

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Constantes
# -----------------------------------------------------------------------------
load_dotenv()

GRUPO  = "PA"
ESTADO = "CE"

BASE_TMP      = Path("/tmp/pypah")
PASTA_DBC     = BASE_TMP / "dbc"
PASTA_BRONZE  = BASE_TMP / "bronze"
PASTA_SILVER  = BASE_TMP / "silver"
PASTA_ROTULOS = BASE_TMP / "rotulos"

R2_GOLD_PREFIX      = "gold"
R2_DIMS_PREFIX      = "dims"
R2_CONSOLIDATED_KEY = f"{R2_GOLD_PREFIX}/consolidated.parquet"

MESES_ATRASO_DATASUS = 2


# -----------------------------------------------------------------------------
# Helpers R2
# -----------------------------------------------------------------------------

def _s3_client():
    endpoint = os.environ["R2_ENDPOINT"]
    if not endpoint.startswith("http"):
        endpoint = f"https://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def listar_particoes_existentes(s3, bucket: str) -> set[tuple[int, int]]:
    """Retorna set de (ano, mes) das particoes ja presentes no R2."""
    existentes = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{R2_GOLD_PREFIX}/ano="):
        for obj in page.get("Contents", []):
            partes = obj["Key"].split("/")
            try:
                ano = int(partes[1].replace("ano=", ""))
                mes = int(partes[2].replace("mes=", ""))
                existentes.add((ano, mes))
            except (IndexError, ValueError):
                continue
    log.info(f"Particoes ja existentes no R2: {len(existentes)}")
    return existentes


def consolidated_existe(s3, bucket: str) -> bool:
    """Verifica se o consolidated.parquet ja existe no R2."""
    try:
        s3.head_object(Bucket=bucket, Key=R2_CONSOLIDATED_KEY)
        return True
    except Exception:
        return False


def calcular_meses_disponiveis(ano_inicio: int, mes_inicio: int) -> list[tuple[int, int]]:
    hoje   = date.today()
    limite = hoje - relativedelta(months=MESES_ATRASO_DATASUS)
    limite_tuple = (limite.year, limite.month)
    cursor = date(ano_inicio, mes_inicio, 1)
    meses  = []
    while (cursor.year, cursor.month) <= limite_tuple:
        meses.append((cursor.year, cursor.month))
        cursor += relativedelta(months=1)
    return meses


def fazer_upload_particao(s3, bucket: str, arquivo_local: Path, ano: int, mes: int):
    chave = f"{R2_GOLD_PREFIX}/ano={ano}/mes={mes:02d}/dados.parquet"
    log.info(f"Upload particao -> s3://{bucket}/{chave}")
    s3.upload_file(str(arquivo_local), bucket, chave)
    log.info("Upload da particao concluido.")


def fazer_upload_consolidated(s3, bucket: str, arquivo_local: Path):
    log.info(f"Upload consolidated -> s3://{bucket}/{R2_CONSOLIDATED_KEY}")
    s3.upload_file(str(arquivo_local), bucket, R2_CONSOLIDATED_KEY)
    log.info("Upload do consolidated concluido.")


def fazer_upload_dim(s3, bucket: str, arquivo_local: Path, nome_arquivo: str):
    chave = f"{R2_DIMS_PREFIX}/{nome_arquivo}"
    log.info(f"Upload dimensao -> s3://{bucket}/{chave}")
    s3.upload_file(str(arquivo_local), bucket, chave)


# -----------------------------------------------------------------------------
# Pipeline por mes
# -----------------------------------------------------------------------------

def processar_mes(s3, bucket: str, ano: int, mes: int) -> bool:
    """
    Executa o pipeline de um unico mes: bronze -> silver -> gold -> upload particao.

    Comportamento em caso de falha:
    - Se o silver ja existe em disco (de uma execucao anterior), pula download e conversao.
    - O silver so e apagado apos upload confirmado.
    - Gold parcial e sempre removido em caso de erro para evitar upload corrompido.
    """
    log.info("=" * 50)
    log.info(f"Processando {ano}/{mes:02d}...")
    log.info("=" * 50)

    pasta_dbc_mes    = PASTA_DBC    / f"{ano}{mes:02d}"
    pasta_bronze_mes = PASTA_BRONZE / f"{ano}{mes:02d}"
    pasta_silver_mes = PASTA_SILVER / f"{ano}{mes:02d}"
    pasta_gold_mes   = BASE_TMP / "gold" / f"{ano}{mes:02d}"

    arquivo_silver = pasta_silver_mes / "silver.parquet"
    arquivo_gold   = pasta_gold_mes   / "dados.parquet"

    try:
        # -- Etapas 1-3: Download + Conversao + Silver -------------------------
        # Pula se silver ja existe (retomada apos falha anterior no gold/upload)
        if arquivo_silver.exists():
            log.info("Silver ja existe em disco — pulando download e conversao.")
        else:
            for p in [pasta_dbc_mes, pasta_bronze_mes, pasta_silver_mes]:
                p.mkdir(parents=True, exist_ok=True)

            log.info("Etapa 1/4 — Download FTP DATASUS...")
            baixar_dbc(
                grupo=GRUPO, estado=ESTADO,
                anos=[ano], meses=[mes],
                destino=pasta_dbc_mes,
            )

            if not list(pasta_dbc_mes.glob("*.dbc")):
                log.warning(f"Nenhum .dbc encontrado para {ano}/{mes:02d}. Pulando.")
                return False

            log.info("Etapa 2/4 — Conversao DBC -> Bronze...")
            conv_dbc_para_pqt(
                pasta_origem=str(pasta_dbc_mes),
                pasta_destino=str(pasta_bronze_mes),
            )

            log.info("Etapa 3/4 — Tratamento Silver...")
            tratar_dados_sia(
                pasta=str(pasta_bronze_mes),
                colunas=col_interesse,
                arquivo_saida=str(arquivo_silver),
                verbose=True,
            )

            if not arquivo_silver.exists():
                log.error(f"Silver nao gerado para {ano}/{mes:02d}.")
                return False

            # DBC e bronze nao sao mais necessarios apos o silver
            for p in [pasta_dbc_mes, pasta_bronze_mes]:
                if p.exists():
                    shutil.rmtree(p)

        # -- Etapa 4: Agregacao Gold -------------------------------------------
        pasta_gold_mes.mkdir(parents=True, exist_ok=True)

        log.info("Etapa 4/4 — Agregacao Gold...")
        processar_gold_particionado(
            arquivo_silver=arquivo_silver,
            arquivo_saida=arquivo_gold,
        )

        if not arquivo_gold.exists():
            log.error(f"Gold nao gerado para {ano}/{mes:02d}.")
            return False

        # -- Upload particao ---------------------------------------------------
        fazer_upload_particao(s3, bucket, arquivo_gold, ano, mes)

        # So limpa apos upload confirmado
        for p in [pasta_silver_mes, pasta_gold_mes]:
            if p.exists():
                shutil.rmtree(p)

        log.info(f"Mes {ano}/{mes:02d} concluido com sucesso.")
        return True

    except Exception as e:
        log.error(f"Erro ao processar {ano}/{mes:02d}: {e}", exc_info=True)
        if arquivo_silver.exists():
            log.info(f"Silver preservado em {arquivo_silver} para retomada.")
        if pasta_gold_mes.exists():
            shutil.rmtree(pasta_gold_mes)
        return False


# -----------------------------------------------------------------------------
# Consolidacao
# -----------------------------------------------------------------------------

def gerar_consolidated(s3, bucket: str):
    """
    Le todas as particoes gold do R2, consolida em um unico parquet
    e faz upload sobrescrevendo o consolidated.parquet anterior.
    """
    log.info("Gerando consolidated.parquet...")

    pasta_tmp = BASE_TMP / "consolidated_tmp"
    pasta_tmp.mkdir(parents=True, exist_ok=True)
    arquivo_consolidated = pasta_tmp / "consolidated.parquet"

    try:
        endpoint = os.environ["R2_ENDPOINT"]
        if not endpoint.startswith("http"):
            endpoint = f"https://{endpoint}"

        consolidar_gold_r2(
            bucket=bucket,
            prefix=R2_GOLD_PREFIX,
            endpoint=endpoint,
            access_key=os.environ["R2_ACCESS_KEY_ID"],
            secret_key=os.environ["R2_SECRET_ACCESS_KEY"],
            arquivo_saida=arquivo_consolidated,
        )

        fazer_upload_consolidated(s3, bucket, arquivo_consolidated)

    finally:
        if pasta_tmp.exists():
            shutil.rmtree(pasta_tmp)


# -----------------------------------------------------------------------------
# Dimensoes
# -----------------------------------------------------------------------------

def atualizar_dimensoes(s3, bucket: str):
    log.info("Atualizando tabelas dimensao...")
    PASTA_ROTULOS.mkdir(parents=True, exist_ok=True)

    try:
        path_estab = estab_ce_label(destino=PASTA_ROTULOS)
        fazer_upload_dim(s3, bucket, Path(path_estab), "dim_estabelecimento_ce.parquet")
    except Exception as e:
        log.error(f"Erro ao atualizar dim_estabelecimento: {e}", exc_info=True)

    try:
        path_proc = download_proc_label(destino=PASTA_ROTULOS)
        fazer_upload_dim(s3, bucket, Path(path_proc), "dim_procedimento.parquet")
    except Exception as e:
        log.error(f"Erro ao atualizar dim_procedimento: {e}", exc_info=True)

    if PASTA_ROTULOS.exists():
        shutil.rmtree(PASTA_ROTULOS)


# -----------------------------------------------------------------------------
# Ponto de entrada
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Pipeline incremental PyPAH")
    parser.add_argument("--ano-inicio",       type=int, default=None)
    parser.add_argument("--mes-inicio",       type=int, default=None)
    parser.add_argument("--ano-fim",          type=int, default=None)
    parser.add_argument("--mes-fim",          type=int, default=None)
    parser.add_argument("--skip-dims",        action="store_true", help="Pula atualizacao das dimensoes")
    parser.add_argument("--force-consolidate",action="store_true", help="Regenera consolidated mesmo sem meses novos")
    args = parser.parse_args()

    bucket = os.environ["R2_BUCKET"]
    s3     = _s3_client()

    # -- Determinar meses a processar -----------------------------------------
    particoes_existentes = listar_particoes_existentes(s3, bucket)
    existe_consolidated  = consolidated_existe(s3, bucket)

    if args.ano_inicio and args.mes_inicio:
        ano_inicio, mes_inicio = args.ano_inicio, args.mes_inicio
        log.info(f"Modo carga historica: a partir de {ano_inicio}/{mes_inicio:02d}")
    else:
        if particoes_existentes:
            ultimo_ano, ultimo_mes = max(particoes_existentes)
            proximo = date(ultimo_ano, ultimo_mes, 1) + relativedelta(months=1)
            ano_inicio, mes_inicio = proximo.year, proximo.month
            log.info(f"Modo incremental: a partir de {ano_inicio}/{mes_inicio:02d}")
        else:
            ano_inicio, mes_inicio = 2018, 1
            log.info("Nenhuma particao existente. Iniciando desde 2018/01.")

    todos_os_meses  = calcular_meses_disponiveis(ano_inicio, mes_inicio)
    meses_pendentes = [(a, m) for a, m in todos_os_meses if (a, m) not in particoes_existentes]

    if args.ano_fim and args.mes_fim:
        limite = (args.ano_fim, args.mes_fim)
        meses_pendentes = [(a, m) for a, m in meses_pendentes if (a, m) <= limite]

    # -- Processar meses novos ------------------------------------------------
    sucessos = 0
    falhas   = 0

    if not meses_pendentes:
        log.info("Nenhum mes novo para processar.")
    else:
        log.info(f"Meses a processar: {len(meses_pendentes)}")
        for a, m in meses_pendentes:
            log.info(f"  -> {a}/{m:02d}")

        for ano, mes in meses_pendentes:
            ok = processar_mes(s3, bucket, ano, mes)
            if ok:
                sucessos += 1
            else:
                falhas += 1

        log.info(f"Processamento concluido. Sucessos: {sucessos} | Falhas: {falhas}")

    # -- Consolidated: gera apenas se necessario ------------------------------
    # Condicoes para gerar:
    #   1. Nao existe consolidated no R2, OU
    #   2. Houve ao menos 1 mes novo processado com sucesso, OU
    #   3. Flag --force-consolidate foi passada
    deve_consolidar = (
        not existe_consolidated
        or sucessos > 0
        or args.force_consolidate
    )

    if deve_consolidar:
        gerar_consolidated(s3, bucket)
    else:
        log.info("Consolidated ja esta atualizado. Nenhuma acao necessaria.")

    # -- Dimensoes ------------------------------------------------------------
    if not args.skip_dims and (sucessos > 0 or not existe_consolidated):
        atualizar_dimensoes(s3, bucket)
    elif args.skip_dims:
        log.info("Atualizacao de dimensoes puladas (--skip-dims).")
    else:
        log.info("Dimensoes ja estao atualizadas.")

    log.info("Pipeline encerrado.")


if __name__ == "__main__":
    main()
