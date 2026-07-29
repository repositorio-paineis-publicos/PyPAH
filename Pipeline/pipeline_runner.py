"""
pipeline_runner.py
------------------
Orquestrador do pipeline incremental do PyPAH.

Fluxo de execucao:
  1. Lista particoes (ano/mes) ja existentes (local ou R2, conforme STORAGE).
  2. Verifica se consolidated.parquet ja existe.
  3. Calcula meses novos disponiveis no FTP do DATASUS.
  4. Para cada mes novo: baixa .dbc -> converte -> trata -> agrega -> grava a particao.
  5. Se houve novos meses OU consolidated nao existe: gera e grava o consolidated.parquet.
  6. Atualiza tabelas dimensao (rotulos).

O destino de gravacao (filesystem local em DATA_ROOT ou bucket R2) e decidido
pelo modulo storage.py conforme a variavel de ambiente STORAGE — este arquivo
nao contem logica condicional de armazenamento.

Variaveis de ambiente:
  STORAGE=local -> requer DATA_ROOT
  STORAGE=r2    -> requer R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, R2_BUCKET

Uso:
  # Modo incremental (so meses novos):
  python -m Pipeline.pipeline_runner

  # Carga historica:
  python -m Pipeline.pipeline_runner --ano-inicio 2018 --mes-inicio 1 --ano-fim 2024 --mes-fim 12

  # Forcar regeneracao do consolidated sem processar meses novos:
  python -m Pipeline.pipeline_runner --force-consolidate
"""

import shutil
import argparse
import logging
from datetime import date
from pathlib import Path
from dateutil.relativedelta import relativedelta

from dotenv import load_dotenv

from Pipeline.fun_sia import (
    baixar_dbc,
    conv_dbc_para_pqt,
    tratar_dados_sia,
    estab_ce_label,
    download_proc_label,
    col_interesse,
)
from Pipeline.gold import processar_gold_particionado
import storage

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

MESES_ATRASO_DATASUS = 2
DIA_LANCAMENTO_DATASUS = 15  # dia do mês em que o DATASUS costuma publicar

def calcular_atraso_efetivo() -> int:
    """Se ainda não passamos do dia de lançamento neste mês, o mês que
    seria o 'limite' normal ainda não foi publicado — soma 1 mês de atraso."""
    if date.today().day < DIA_LANCAMENTO_DATASUS:
        return MESES_ATRASO_DATASUS + 1
    return MESES_ATRASO_DATASUS


def calcular_meses_disponiveis(ano_inicio: int, mes_inicio: int) -> list[tuple[int, int]]:
    hoje   = date.today()
    limite = hoje - relativedelta(months=calcular_atraso_efetivo())
    limite_tuple = (limite.year, limite.month)
    cursor = date(ano_inicio, mes_inicio, 1)
    meses  = []
    while (cursor.year, cursor.month) <= limite_tuple:
        meses.append((cursor.year, cursor.month))
        cursor += relativedelta(months=1)
    return meses


# -----------------------------------------------------------------------------
# Pipeline por mes
# -----------------------------------------------------------------------------

def processar_mes(ano: int, mes: int) -> bool:
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

        # -- Grava particao (local ou R2, conforme STORAGE) ---------------------
        storage.salvar_particao(arquivo_gold, ano, mes)

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

def gerar_consolidated():
    """
    Le todas as particoes gold (local ou R2, conforme STORAGE), consolida em
    um unico parquet e grava sobrescrevendo o consolidated.parquet anterior.
    """
    log.info("Gerando consolidated.parquet...")

    pasta_tmp = BASE_TMP / "consolidated_tmp"
    pasta_tmp.mkdir(parents=True, exist_ok=True)
    arquivo_consolidated = pasta_tmp / "consolidated.parquet"

    try:
        storage.consolidar_particoes(arquivo_consolidated)
        storage.salvar_consolidated(arquivo_consolidated)

    finally:
        if pasta_tmp.exists():
            shutil.rmtree(pasta_tmp)


# -----------------------------------------------------------------------------
# Dimensoes
# -----------------------------------------------------------------------------

def atualizar_dimensoes():
    log.info("Atualizando tabelas dimensao...")
    PASTA_ROTULOS.mkdir(parents=True, exist_ok=True)

    try:
        path_estab = estab_ce_label(destino=PASTA_ROTULOS)
        storage.salvar_dim(Path(path_estab), "dim_estabelecimento_ce.parquet")
    except Exception as e:
        log.error(f"Erro ao atualizar dim_estabelecimento: {e}", exc_info=True)

    try:
        path_proc = download_proc_label(destino=PASTA_ROTULOS)
        storage.salvar_dim(Path(path_proc), "dim_procedimento.parquet")
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

    # -- Determinar meses a processar -----------------------------------------
    particoes_existentes = storage.listar_particoes_existentes()
    existe_consolidated  = storage.consolidated_existe()
    log.info(f"Particoes ja existentes: {len(particoes_existentes)}")

    if args.ano_inicio and args.mes_inicio:
        ano_inicio, mes_inicio = args.ano_inicio, args.mes_inicio
        log.info(f"Modo carga historica: a partir de {ano_inicio}/{mes_inicio:02d}")
    else:
        if particoes_existentes:
            ultimo_ano, ultimo_mes = max(particoes_existentes)
            proximo = date(ultimo_ano, ultimo_mes, 1) + relativedelta(months=1)
            ano_inicio, mes_inicio = proximo.year, proximo.month
            log.info(f"Modo incremental: a partir de {ano_inicio}/{mes_inicio:02d}")
        # DEPOIS
        else:
            limite = date.today() - relativedelta(months=calcular_atraso_efetivo())
            inicio = limite - relativedelta(months=2)  # 3 meses incluindo o mais recente disponível
            ano_inicio, mes_inicio = inicio.year, inicio.month
            log.info(
                f"Nenhuma particao existente. Baixando os ultimos 3 meses "
                f"disponiveis, a partir de {ano_inicio}/{mes_inicio:02d}."
            )

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
            ok = processar_mes(ano, mes)
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
        gerar_consolidated()
    else:
        log.info("Consolidated ja esta atualizado. Nenhuma acao necessaria.")

    # -- Dimensoes ------------------------------------------------------------
    if not args.skip_dims and (sucessos > 0 or not existe_consolidated):
        atualizar_dimensoes()
    elif args.skip_dims:
        log.info("Atualizacao de dimensoes puladas (--skip-dims).")
    else:
        log.info("Dimensoes ja estao atualizadas.")

    log.info("Pipeline encerrado.")


if __name__ == "__main__":
    main()