"""
storage.py
----------
Abstracao minima de armazenamento para o PyPAH.

Permite alternar entre armazenamento local (ambiente de dev, fora do
Docker Desktop) e Cloudflare R2 (ambiente de producao atual) atraves
da variavel de ambiente STORAGE, sem espalhar `if STORAGE == ...`
pelo codigo de negocio da API e do Pipeline.

    STORAGE=local -> le/escreve arquivos em DATA_ROOT (filesystem local,
                      montado por fora do container)
    STORAGE=r2    -> le/escreve no bucket R2 configurado (comportamento
                      identico ao que existia antes desta mudanca)

Usado por: API/connection.py, API/routers/dados.py, Pipeline/pipeline_runner.py

boto3 so e importado dentro das funcoes de escrita (usadas apenas pelo
Pipeline), entao a API nao precisa dessa dependencia.
"""

import os
import shutil
from pathlib import Path


# -----------------------------------------------------------------
# Modo de armazenamento
# -----------------------------------------------------------------

def is_local() -> bool:
    return os.environ.get("STORAGE", "r2").lower() == "local"


def data_root() -> Path:
    return Path(os.environ.get("DATA_ROOT", "/datasets"))


# -----------------------------------------------------------------
# Caminhos de leitura — usados pela API (via DuckDB)
# -----------------------------------------------------------------

def gold_path() -> str:
    if is_local():
        return str(data_root() / "gold")
    return f"s3://{os.environ['R2_BUCKET']}/gold"


def dims_path() -> str:
    if is_local():
        return str(data_root() / "dims")
    return f"s3://{os.environ['R2_BUCKET']}/dims"


def consolidated_path(nome_arquivo: str = "consolidated.parquet") -> str:
    """
    nome_arquivo e resolvido por consolidated_name(), conforme USE_SAMPLE.
    """
    return f"{gold_path()}/{nome_arquivo}"

def consolidated_name() -> str:
    """
    Nome do arquivo consolidated a ser lido pela API, conforme USE_SAMPLE:
      USE_SAMPLE=true  -> amostra estratificada por mes (ambiente demo/prod leve)
      USE_SAMPLE=false -> dataset completo (padrao, usado em dev)
    """
    if os.environ.get("USE_SAMPLE", "false").lower() == "true":
        sample_rows = os.environ.get("SAMPLE_ROWS", "10000")
        return f"consolidated_sample_{sample_rows}.parquet"
    return "consolidated.parquet"

def configure_duckdb(con):
    """Habilita acesso ao R2 via httpfs quando STORAGE=r2. Sem efeito em modo local."""
    if is_local():
        return con
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='auto';
        SET s3_access_key_id='{os.environ["R2_ACCESS_KEY_ID"]}';
        SET s3_secret_access_key='{os.environ["R2_SECRET_ACCESS_KEY"]}';
        SET s3_endpoint='{os.environ["R2_ENDPOINT"]}';
        SET s3_url_style='path';
    """)
    return con


# -----------------------------------------------------------------
# Escrita — usado apenas pelo Pipeline (particoes, consolidated, dims)
# -----------------------------------------------------------------

def _s3_client():
    import boto3
    from botocore.config import Config

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


def listar_particoes_existentes() -> set[tuple[int, int]]:
    """Retorna o set de (ano, mes) ja presentes na camada gold."""
    if is_local():
        base = data_root() / "gold"
        existentes = set()
        for p in base.glob("ano=*/mes=*/dados.parquet"):
            try:
                ano = int(p.parent.parent.name.replace("ano=", ""))
                mes = int(p.parent.name.replace("mes=", ""))
                existentes.add((ano, mes))
            except ValueError:
                continue
        return existentes

    s3 = _s3_client()
    bucket = os.environ["R2_BUCKET"]
    existentes = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix="gold/ano="):
        for obj in page.get("Contents", []):
            partes = obj["Key"].split("/")
            try:
                ano = int(partes[1].replace("ano=", ""))
                mes = int(partes[2].replace("mes=", ""))
                existentes.add((ano, mes))
            except (IndexError, ValueError):
                continue
    return existentes


def consolidated_existe(nome_arquivo: str = "consolidated.parquet") -> bool:
    if is_local():
        return (data_root() / "gold" / nome_arquivo).exists()
    s3 = _s3_client()
    bucket = os.environ["R2_BUCKET"]
    try:
        s3.head_object(Bucket=bucket, Key=f"gold/{nome_arquivo}")
        return True
    except Exception:
        return False


def salvar_particao(arquivo_local: Path, ano: int, mes: int):
    """Grava (local) ou faz upload (R2) da particao gold de um mes."""
    if is_local():
        destino = data_root() / "gold" / f"ano={ano}" / f"mes={mes:02d}" / "dados.parquet"
        destino.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(str(arquivo_local), str(destino))
        return
    s3 = _s3_client()
    bucket = os.environ["R2_BUCKET"]
    chave = f"gold/ano={ano}/mes={mes:02d}/dados.parquet"
    s3.upload_file(str(arquivo_local), bucket, chave)


def salvar_consolidated(arquivo_local: Path, nome_arquivo: str = "consolidated.parquet"):
    if is_local():
        destino = data_root() / "gold" / nome_arquivo
        destino.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(str(arquivo_local), str(destino))
        return
    s3 = _s3_client()
    bucket = os.environ["R2_BUCKET"]
    s3.upload_file(str(arquivo_local), bucket, f"gold/{nome_arquivo}")

def gerar_sample_estratificado(arquivo_consolidated: Path, arquivo_saida: Path, sample_rows: int):
    """
    Gera uma amostra estratificada por Ano/Mes a partir do consolidated.parquet
    completo, garantindo que todos os meses apareçam na amostra (evita que um
    sample puramente aleatorio sub-represente um mes, municipio ou procedimento).
    """
    import duckdb

    con = duckdb.connect()
    con.execute(f"""
        COPY (
            SELECT * EXCLUDE (rn) FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY Ano, Mes ORDER BY random()
                    ) AS rn,
                    count(DISTINCT Ano || '-' || Mes) OVER () AS n_meses
                FROM read_parquet('{arquivo_consolidated}')
            )
            WHERE rn <= CEIL({sample_rows}.0 / n_meses)
        ) TO '{arquivo_saida}' (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    con.close()

def salvar_dim(arquivo_local: Path, nome_arquivo: str):
    if is_local():
        destino = data_root() / "dims" / nome_arquivo
        destino.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(str(arquivo_local), str(destino))
        return
    s3 = _s3_client()
    bucket = os.environ["R2_BUCKET"]
    s3.upload_file(str(arquivo_local), bucket, f"dims/{nome_arquivo}")


def consolidar_particoes(arquivo_saida: Path):
    """
    Le todas as particoes gold (local ou R2, conforme STORAGE) via DuckDB
    e gera um unico consolidated.parquet em arquivo_saida.

    Substitui as antigas `consolidar_gold_r2` / `consolidar_gold_local`
    de Pipeline/gold.py, que duplicavam a mesma logica por backend.
    """
    import duckdb

    con = duckdb.connect()
    configure_duckdb(con)

    if is_local():
        glob_path = str(data_root() / "gold" / "ano=*" / "mes=*" / "dados.parquet")
    else:
        bucket = os.environ["R2_BUCKET"]
        glob_path = f"s3://{bucket}/gold/ano=*/mes=*/dados.parquet"

    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{glob_path}', hive_partitioning=false)
            ORDER BY data_ref
        ) TO '{arquivo_saida}' (FORMAT PARQUET, COMPRESSION 'snappy')
    """)
    con.close()