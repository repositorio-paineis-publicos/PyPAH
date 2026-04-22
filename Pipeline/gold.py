"""
gold.py
-------
Funcoes de geracao da camada Gold.

processar_gold_particionado : agrega silver de um mes -> parquet gold local
consolidar_gold_r2          : le todas as particoes do R2 -> consolidated.parquet local
consolidar_gold_local       : utilitario para consolidar particoes locais (dev/debug)
"""

import duckdb
import logging
from pathlib import Path

log = logging.getLogger(__name__)


def processar_gold_particionado(
    arquivo_silver: str | Path,
    arquivo_saida: str | Path,
) -> Path:
    """
    Agrega o silver de um unico mes e salva como parquet gold local.

    Aplica o mesmo GROUP BY da tabela original gold_fact_qtd_val_TT:
    agrupa por unidade, ano, mes, data, municipio e procedimento,
    somando valores e quantidades produzidas/aprovadas.
    """
    arquivo_silver = str(arquivo_silver)
    arquivo_saida  = str(arquivo_saida)

    con = duckdb.connect()
    log.info(f"Agregando Gold a partir de: {arquivo_silver}")

    con.execute(f"""
        COPY (
            SELECT
                PA_CODUNI,
                Ano,
                Mes,
                data_ref,
                PA_MUNPCN,
                PA_PROC_ID,
                SUM(CAST(PA_VALPRO AS DOUBLE)) AS PA_VALPRO,
                SUM(CAST(PA_VALAPR AS DOUBLE)) AS PA_VALAPR,
                SUM(CAST(PA_QTDPRO AS BIGINT)) AS PA_QTDPRO,
                SUM(CAST(PA_QTDAPR AS BIGINT)) AS PA_QTDAPR
            FROM read_parquet('{arquivo_silver}')
            GROUP BY
                PA_CODUNI, Ano, Mes, data_ref, PA_MUNPCN, PA_PROC_ID
        ) TO '{arquivo_saida}' (FORMAT PARQUET, COMPRESSION 'snappy')
    """)

    con.close()
    log.info(f"Gold gerado em: {arquivo_saida}")
    return Path(arquivo_saida)


def consolidar_gold_r2(
    bucket: str,
    prefix: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
    arquivo_saida: str | Path,
) -> Path:
    """
    Le todas as particoes gold diretamente do R2 via DuckDB/httpfs
    e gera um unico consolidated.parquet local, pronto para upload.

    O DuckDB le as particoes em paralelo diretamente do R2,
    sem precisar baixa-las para disco primeiro.
    """
    arquivo_saida = str(arquivo_saida)
    glob_r2 = f"s3://{bucket}/{prefix}/ano=*/mes=*/dados.parquet"

    con = duckdb.connect()
    log.info("Configurando acesso ao R2 para consolidacao...")

    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='auto';
        SET s3_access_key_id='{access_key}';
        SET s3_secret_access_key='{secret_key}';
        SET s3_endpoint='{endpoint.replace('https://', '')}';
        SET s3_url_style='path';
    """)

    log.info(f"Consolidando todas as particoes de: {glob_r2}")

    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{glob_r2}', hive_partitioning=false)
            ORDER BY data_ref
        ) TO '{arquivo_saida}' (FORMAT PARQUET, COMPRESSION 'snappy')
    """)

    con.close()
    log.info(f"Consolidated gerado em: {arquivo_saida}")
    return Path(arquivo_saida)


def consolidar_gold_local(
    pasta_particoes: str | Path,
    arquivo_saida: str | Path,
) -> Path:
    """
    [Utilitario para desenvolvimento/debug]

    Consolida particoes gold locais em um unico parquet.
    Util para verificar dados sem precisar acessar o R2.
    """
    pasta_particoes = str(pasta_particoes)
    arquivo_saida   = str(arquivo_saida)

    con = duckdb.connect()
    log.info(f"Consolidando particoes locais de: {pasta_particoes}")

    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{pasta_particoes}/**/*.parquet', hive_partitioning=true)
            ORDER BY Ano, Mes, PA_MUNPCN, PA_CODUNI, PA_PROC_ID
        ) TO '{arquivo_saida}' (FORMAT PARQUET, COMPRESSION 'ztsd')
    """)

    con.close()
    log.info(f"Consolidacao local concluida: {arquivo_saida}")
    return Path(arquivo_saida)
