import os
import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb
from pathlib import Path

import requests

DATA_DIR = Path("/data")
DB_PATH = DATA_DIR / "pypah.duckdb"

URL = "https://github.com/monteirogmb/pypah-dataset/releases/download/gold-v1/pypah.duckdb"

DATA_DIR.mkdirs(exist_ok = True)

if not DB_PATH.exists()
    r = requests.get(URL)
    with open(DB_PATH, "wb") as f:
        f.write(r.content)

        
ROTULOS_URL = "https://github.com/monteirogmb/pypah-dataset/releases/download/td-v1"

@st.cache_resource
def get_con():
    return duckdb.connect(DB_PATH, read_only=True)

con = get_con()


@st.cache_data
def optimize_plotly(fig):
    fig.update_layout(
        hovermode="closest",
        transition_duration=0
    )
    return fig


@st.cache_data(show_spinner=True)
def anos_disponiveis():
    return (
        con.execute("SELECT DISTINCT Ano FROM gold_fact_qtd_val_3y ORDER BY Ano")
        .df()["Ano"]
        .tolist()
    )


@st.cache_data(show_spinner=True)
def meses_disponiveis_multi(anos):
    anos_sql = ",".join(map(str, anos))

    q = f"""
        SELECT Mes
        FROM gold_fact_qtd_val_3y
        WHERE Ano IN ({anos_sql})
        GROUP BY Mes
        ORDER BY MIN(data_ref)
    """

    return con.execute(q).df()["Mes"].tolist()


@st.cache_data(show_spinner=False)
def load_dim_estabelecimento():
    url = f"{ROTULOS_URL}/dim_estabelecimento_ce.parquet"

    df = pd.read_parquet(
        url,
        columns=["PA_CODUNI", "label_estabelecimento"]
    )

    return df

@st.cache_data(show_spinner=False)
def load_dim_procedimento():
    url = f"{ROTULOS_URL}/dim_procedimento.parquet"

    df = pd.read_parquet(
        url,
        columns=["PA_PROC_ID", "label_procedimento"]
    )

    return df


@st.cache_data(show_spinner=True)
def municipios_disponiveis():
    return (
        con.execute("SELECT DISTINCT PA_MUNPCN FROM gold_fact_qtd_val_3y")
        .df()["PA_MUNPCN"]
        .sort_values()
        .tolist()
    )


st.set_page_config(layout='wide')

st.title('PyPah Dashboard')
st.sidebar.title('Filtros')


## =========================
## Criação dos filtros
## =========================


df_dim_est = load_dim_estabelecimento()
df_dim_proc = load_dim_procedimento()


# Filtro de procedimentos

filtrar_proc = st.sidebar.checkbox(
    "Filtrar por procedimento",
    value=False
)
pa_proc_ids = None


if filtrar_proc:
    with st.spinner("Carregando procedimentos..."):
            opcoes_proc = df_dim_proc['label_procedimento'].sort_values().tolist()

    filtro_procedimentos = st.sidebar.multiselect(
        "Selecione os procedimentos",
        options=opcoes_proc
    )
    if filtro_procedimentos:
        map_proc_to_cod = dict(zip(df_dim_proc['label_procedimento'], df_dim_proc['PA_PROC_ID']))
        pa_proc_ids = [map_proc_to_cod[proc] for proc in filtro_procedimentos]


# Filtro de estabelecimentos

filtrar_estab = st.sidebar.checkbox(
    "Filtrar por estabelecimento",
    value=False
)
pa_codunis = None


if filtrar_estab:
    with st.spinner("Carregando estabelecimentos..."):
        opcoes_estab = df_dim_est['label_estabelecimento'].sort_values().tolist()
    filtro_estabelecimentos = st.sidebar.multiselect(
        "Selecione os estabelecimentos",
        options=opcoes_estab
    )
    if filtro_estabelecimentos:
        map_estab_to_cod = dict(zip(df_dim_est['label_estabelecimento'], df_dim_est['PA_CODUNI']))
        pa_codunis = [map_estab_to_cod[estab] for estab in filtro_estabelecimentos]


## Filtro de datas
# Filtro de anos
anos_disp = anos_disponiveis()

filtro_anos = False

if len(anos_disp) > 1:
    filtro_anos = st.sidebar.checkbox(
        "Filtrar por anos",
        value=False
    )



if filtro_anos:
    ano_sel = st.sidebar.slider(
        "Ano",
        min(anos_disp),
        max(anos_disp)
    )

    anos_sel = [ano_sel]
else:
    anos_sel = anos_disp

# Filtro de meses
meses = None

filtro_meses = st.sidebar.checkbox(
    "Filtrar por meses",
    value=False
)
if filtro_meses:
    with st.spinner("Carregando meses..."):
        meses_validos = meses_disponiveis_multi(anos=anos_sel)
    meses = st.sidebar.multiselect(
        "Meses",
        options=meses_validos,
        default=meses_validos
    )



# Filtro de municípios


filtro_municipio = st.sidebar.checkbox(
    "Filtrar por municípios",
    value=False
)

if filtro_municipio:
    with st.spinner("Carregando municípios..."):
        opcoes_mun = municipios_disponiveis()
    municipios = st.sidebar.multiselect(
        "Selecione os municípios",
        options = opcoes_mun,
        )
else:
    municipios = None

# Construção da query SQL com filtros


where = []

if anos_sel:
    where.append(f"ANO IN ({','.join(map(str, anos_sel))})")

if meses:
    meses_sql = ",".join(f"'{m}'" for m in meses)
    where.append(f"MES IN ({meses_sql})")

if pa_codunis:
    cods_sql = ",".join(f"'{c}'" for c in pa_codunis)
    where.append(f"PA_CODUNI IN ({cods_sql})")

if pa_proc_ids:
    procs_sql = ",".join(f"'{p}'" for p in pa_proc_ids)
    where.append(f"PA_PROC_ID IN ({procs_sql})")

if municipios:
    mun_sql = ",".join(f"'{m}'" for m in municipios)
    where.append(f"PA_MUNPCN IN ({mun_sql})")


query = """
SELECT * 
FROM gold_fact_qtd_val_3y
"""

if where:
    query += " WHERE " + " AND ".join(where)

df_filtro = con.execute(query).df()






MAPA_CORES = {
    "Produzido": "#1f4fd8",  # azul escuro
    "Aprovado": "#7aa6ff"    # azul claro
}

## =========================
## Tabelas gráficos de barra
## =========================

# Valores

valores_bar = (df_filtro.melt(
    id_vars='data_ref',
    value_vars=['PA_VALPRO', 'PA_VALAPR'],
    var_name='tipo',
    value_name='valor'
)
.groupby(['tipo', 'data_ref'], as_index=False)
.agg(valor=('valor', 'sum'))
).sort_values("tipo")

valores_bar['tipo'] = valores_bar['tipo'].map({
    'PA_VALPRO': 'Produzido',
    'PA_VALAPR': 'Aprovado'
})

# Quantidade

quant_bar = (df_filtro.melt(
    id_vars='data_ref',
    value_vars=['PA_QTDPRO', 'PA_QTDAPR'],
    var_name='tipo',
    value_name='quantidade'
)
.groupby(['data_ref', 'tipo'], as_index=False)
.agg(quantidade=('quantidade', 'sum'))
).sort_values(['tipo'])


quant_bar['tipo'] = quant_bar['tipo'].map({
    'PA_QTDPRO': 'Produzido',
    'PA_QTDAPR': 'Aprovado'
})

## =========================
## Tabelas gráficos de linha 
## =========================

# Valores

df_linha_val = (
    df_filtro
    .groupby("data_ref", as_index=False)
    .agg({
        "PA_VALPRO": "sum",
        "PA_VALAPR": "sum"
    })
)

df_linha_val_long = df_linha_val.melt(
    id_vars="data_ref",
    value_vars=["PA_VALPRO", "PA_VALAPR"],
    var_name="tipo",
    value_name="valor"
).sort_values("tipo")

df_linha_val_long["tipo"] = df_linha_val_long["tipo"].map({
    "PA_VALPRO": "Produzido",
    "PA_VALAPR": "Aprovado"
})


# Quantidade

df_linha_qtd = (
    df_filtro
    .groupby("data_ref", as_index=False)
    .agg({
        "PA_QTDPRO": "sum",
        "PA_QTDAPR": "sum"
    })
)

df_linha_qtd_long = df_linha_qtd.melt(
    id_vars="data_ref",
    value_vars=["PA_QTDPRO", "PA_QTDAPR"],
    var_name="tipo",
    value_name="quantidade"
).sort_values("tipo")

df_linha_qtd_long["tipo"] = df_linha_qtd_long["tipo"].map({
    "PA_QTDPRO": "Produzido",
    "PA_QTDAPR": "Aprovado"
})


## =========================
## Gráficos - Valores
## =========================

# Linha

fig_valores_lin = px.line(
    df_linha_val_long.sort_values("data_ref"),
    x="data_ref",
    y="valor",
    color="tipo",
    title="Valor Produzido por Mês/Ano",
    color_discrete_map=MAPA_CORES
)

fig_valores_lin.update_traces(
    mode="lines+markers",
    line=dict(width=2),            # linha um pouco mais fina
    marker=dict(
        size=8,                    # ponto bem visível
        symbol="circle",
        line=dict(width=1)         # borda no ponto
    ),
    hovertemplate=
        "Mês/Ano: %{x}<br>" +
        "%{fullData.name}: %{y:,.0f}<extra></extra>"
)

media_valor = df_linha_val_long["valor"].mean()

fig_valores_lin.add_hline(
    y=media_valor,
    line_dash="dash",
    annotation_text=f"Média: {media_valor:,.0f}",
    annotation_position="right"
)

fig_valores_lin = optimize_plotly(fig_valores_lin)

fig_valores_lin.update_layout(
    yaxis_title="Valor (R$)", xaxis_title="Mês/Ano",
    legend=dict(
        traceorder="normal"
    )
)

# Barra

fig_valores_bar = px.bar(
    valores_bar,
    x="data_ref",
    y="valor",
    color="tipo",
    barmode="group",
    title="Valor Produzido x Aprovado por Mês/Ano",
    color_discrete_map=MAPA_CORES
)

fig_valores_bar.update_traces(
    hovertemplate=
        "Mês/Ano: %{x}<br>" +
        "Valor: %{y:,.0f}<extra></extra>")

fig_valores_bar = optimize_plotly(fig_valores_bar)

fig_valores_bar.update_layout(
    yaxis_title="Valor (R$)", xaxis_title="Mês/Ano",
    legend=dict(
        traceorder="normal"
    )
)

## =========================
## Gráficos - Quantidade
## =========================

# Linha
fig_quant_lin = px.line(
    df_linha_qtd_long.sort_values("data_ref"),
    x="data_ref",
    y="quantidade",
    color="tipo",
    title="Quantidade Produzida por Mês/Ano",
    color_discrete_map=MAPA_CORES
)

fig_quant_lin.update_traces(
    mode="lines+markers",
    line=dict(width=2),            # linha um pouco mais fina
    marker=dict(
        size=8,                    # ponto bem visível
        symbol="circle",
        line=dict(width=1)         # borda no ponto
    ),
    hovertemplate=
        "Mês/Ano: %{x}<br>" +
        "%{fullData.name}: %{y:,.0f}<extra></extra>"
)


media_qtd = df_linha_qtd_long["quantidade"].mean()

fig_quant_lin.add_hline(
    y=media_qtd,
    line_dash="dash",
    annotation_text=f"Média: {media_qtd:,.0f}",
    annotation_position="right"
)

fig_quant_lin = optimize_plotly(fig_quant_lin)

fig_quant_lin.update_layout(
    yaxis_title="Quantidade", xaxis_title="Mês/Ano",
    legend=dict(
        traceorder="normal"
    )
)

# Barra

fig_quant_bar = px.bar(
    quant_bar,
    x="data_ref",
    y="quantidade",
    color="tipo",
    barmode="group",
    title="Quantidade Produzida x Aprovada por Mês/Ano",
    color_discrete_map=MAPA_CORES
)

fig_quant_bar.update_traces(
    hovertemplate=
        "Mês/Ano: %{x}<br>" +
        "Quantidade: %{y:,.0f}<extra></extra>"
)

fig_quant_bar = optimize_plotly(fig_quant_bar)

fig_quant_bar.update_layout(
    yaxis_title="Quantidade", xaxis_title="Mês/Ano",
    legend=dict(
        traceorder="normal"
    )
)

## =========================
## Visualização no Streamlit
## =========================

aba1, aba2, aba3 = st.tabs(['Valores', 'Quantidade', 'Tabela de dados'])

with aba1:
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_valores_lin, 
                        width='stretch',
                        config={"displayModeBar": False, "scrollZoom": False})
    with col2:
        st.plotly_chart(fig_valores_bar, 
                        width='stretch',
                        config={"displayModeBar": False, "scrollZoom": False})

with aba2:
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_quant_lin, 
                        width='stretch',
                        config={"displayModeBar": False, "scrollZoom": False})
    with col2:
        st.plotly_chart(fig_quant_bar, 
                        width='stretch',
                        config={"displayModeBar": False, "scrollZoom": False})
        
with aba3:
    st.write(df_filtro.shape)
    st.write(df_filtro.head(20))
