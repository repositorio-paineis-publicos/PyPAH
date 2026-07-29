import os

from duckdb import HTTPException

from fastapi import APIRouter, Query
from typing import List, Optional
from API.connection import get_con
from API.cache import make_key, get_cached, set_cached
from storage import gold_path, dims_path, consolidated_path

from dotenv import load_dotenv
load_dotenv()

router = APIRouter()

GOLD = gold_path()
DIMS = dims_path()

CONSOLIDATED = consolidated_path("consolidated.parquet")

def gold_disponivel() -> bool:
    return os.path.exists(CONSOLIDATED)

@router.get("/anos")
def anos_disponiveis():
    key = make_key("anos", {})
    cached = get_cached(key)
    if cached is not None:
        return cached
    
    if not gold_disponivel():
        return []

    result = get_con().execute(f"""
        SELECT DISTINCT Ano
        FROM read_parquet('{CONSOLIDATED}')
        ORDER BY Ano
    """).df()["Ano"].tolist()

    set_cached(key, result)
    return result


@router.get("/meses")
def meses_disponiveis(anos: List[int] = Query(...)):
    key = make_key("meses", {"anos": sorted(anos)})
    cached = get_cached(key)
    if cached is not None:
        return cached

    if not gold_disponivel():
        return []

    anos_sql = ",".join(map(str, anos))

    result = get_con().execute(f"""
        SELECT Mes
        FROM read_parquet('{CONSOLIDATED}')
        WHERE Ano IN ({anos_sql})
        GROUP BY Mes
        ORDER BY MIN(data_ref)
    """).df()["Mes"].tolist()

    set_cached(key, result)
    return result


@router.get("/municipios")
def municipios_disponiveis():
    key = make_key("municipios", {})
    cached = get_cached(key)
    if cached is not None:
        return cached

    if not gold_disponivel():
        return []

    result = get_con().execute(f"""
        SELECT DISTINCT PA_MUNPCN
        FROM read_parquet('{CONSOLIDATED}')
        ORDER BY PA_MUNPCN
    """).df()["PA_MUNPCN"].tolist()

    set_cached(key, result)
    return result


@router.get("/estabelecimentos")
def estabelecimentos():
    key = make_key("estabelecimentos", {})
    cached = get_cached(key)
    if cached is not None:
        return cached

    result = get_con().execute(f"""
        SELECT PA_CODUNI, label_estabelecimento
        FROM read_parquet('{DIMS}/dim_estabelecimento_ce.parquet')
    """).df().to_dict(orient="records")

    set_cached(key, result)
    return result


@router.get("/procedimentos")
def procedimentos():
    key = make_key("procedimentos", {})
    cached = get_cached(key)
    if cached is not None:
        return cached

    result = get_con().execute(f"""
        SELECT PA_PROC_ID, label_procedimento
        FROM read_parquet('{DIMS}/dim_procedimento.parquet')
    """).df().to_dict(orient="records")

    set_cached(key, result)
    return result


@router.get("/dados")
def dados_filtrados(
    anos: Optional[List[int]] = Query(None),
    meses: Optional[List[str]] = Query(None),
    municipios: Optional[List[str]] = Query(None),
    pa_codunis: Optional[List[str]] = Query(None),
    pa_proc_ids: Optional[List[str]] = Query(None),
):
    params = {
        "anos": sorted(anos) if anos else [],
        "meses": sorted(meses) if meses else [],
        "municipios": sorted(municipios) if municipios else [],
        "pa_codunis": sorted(pa_codunis) if pa_codunis else [],
        "pa_proc_ids": sorted(pa_proc_ids) if pa_proc_ids else [],
    }

    key = make_key("dados", params)
    cached = get_cached(key)
    if cached is not None:
        return cached

    if not gold_disponivel():
        return []

    where = []

    if anos:
        anos_sql = ",".join(map(str, anos))
        where.append(f"Ano IN ({anos_sql})")

    if meses:
        meses_sql = ",".join(f"'{m}'" for m in meses)
        where.append(f"Mes IN ({meses_sql})")

    if municipios:
        municipios_sql = ",".join(f"'{m}'" for m in municipios)
        where.append(f"PA_MUNPCN IN ({municipios_sql})")

    if pa_codunis:
        codunis_sql = ",".join(f"'{c}'" for c in pa_codunis)
        where.append(f"PA_CODUNI IN ({codunis_sql})")

    if pa_proc_ids:
        proc_sql = ",".join(f"'{p}'" for p in pa_proc_ids)
        where.append(f"PA_PROC_ID IN ({proc_sql})")

    where_clause = ("WHERE " + " AND ".join(where)) if where else ""

    if not gold_disponivel():
        return []

    query = f"""
        SELECT
            data_ref,
            PA_MUNPCN,
            PA_CODUNI,
            PA_PROC_ID,
            Ano,
            Mes,
            SUM(PA_VALPRO) AS PA_VALPRO,
            SUM(PA_VALAPR) AS PA_VALAPR,
            SUM(PA_QTDPRO) AS PA_QTDPRO,
            SUM(PA_QTDAPR) AS PA_QTDAPR
        FROM (
            SELECT * FROM read_parquet('{CONSOLIDATED}')
            {where_clause}
            LIMIT 100000
            )
        GROUP BY data_ref, PA_MUNPCN, PA_CODUNI, PA_PROC_ID, Ano, Mes
        ORDER BY data_ref
    """



    result = get_con().execute(query).df().to_dict(orient="records")
    set_cached(key, result)
    return result