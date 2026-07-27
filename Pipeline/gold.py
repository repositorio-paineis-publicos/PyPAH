"""
gold.py
-------
Funcoes de geracao da camada Gold.

processar_gold_particionado : agrega silver de um mes -> parquet gold local

A consolidacao de particoes (local ou R2) foi movida para
storage.consolidar_particoes, que escolhe o backend via STORAGE, evitando
duas implementacoes quase identicas (uma por backend) vivendo aqui.
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