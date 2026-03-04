from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).resolve().parent.parent

DB_PATH = BASE_DIR / "db" / "db.duckdb"
SILVER_PATH = BASE_DIR / "dados_sia" / "Silver" / "**" / "arquivo_silver.parquet"

print("DB_PATH:", DB_PATH)
print("SILVER_PATH:", SILVER_PATH)

con = duckdb.connect(str(DB_PATH))

con.execute("""
CREATE OR REPLACE TABLE gold_fact_qtd_val_TT AS
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
FROM read_parquet(?)
GROUP BY
    PA_CODUNI,
    Ano,
    Mes,
    data_ref,
    PA_MUNPCN,
    PA_PROC_ID
""", [str(SILVER_PATH)])

con.close()
print("Gold criado com sucesso!")