import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb

def optimize_plotly(fig):
    fig.update_layout(
        hovermode="closest",   # apenas o ponto
        transition_duration=0
    )
    return fig

@st.cache_resource
def get_con():
    con = duckdb.connect()
    con.execute("""
        CREATE OR REPLACE VIEW fact_qtd_val AS
        SELECT * FROM read_parquet(
            'C:/Projetos/PyPAH/dados_sia/facts_dash/fact_qtd_val.parquet'
        )
    """)
    return con



@st.cache_data
def filtrar_dados(df, hospital, ano, filtro_anos):
    df_f = df.copy()

    if hospital:
        df_f = df_f[df_f["PA_CODUNI"].isin(hospital)]


    if not filtro_anos:
        df_f = df_f[df_f["data_ref"].dt.year == ano]

    return df_f

def load_dim(tab_dim):
    return pd.read_parquet(tab_dim)

def anos_disponiveis(con):
    q = """
    SELECT DISTINCT Ano
    FROM fact_qtd_val
    ORDER BY Ano
    """
    return con.execute(q).df()["Ano"].tolist()

def meses_disponiveis_multi(con, anos):
    if not anos:
        return []

    anos_sql = ",".join(map(str, anos))

    q = f"""
    SELECT DISTINCT Mes
    FROM fact_qtd_val
    WHERE Ano IN ({anos_sql})
    GROUP BY Mes
    ORDER BY MIN(data_ref)
    """
    return con.execute(q).df()["Mes"].tolist()





st.set_page_config(layout='wide')

st.title('PyPah Dashboard')
st.sidebar.title('Filtros')


file = r"C:/Projetos/PyPAH/dados_sia/facts_dash/fact_qtd_val.parquet"

df = pd.read_parquet(file)


## =========================
## Criação dos filtros
## =========================


df_dim_est = load_dim(r"C:/Projetos/PyPAH/dados_sia/rotulos/dim_estabelecimento.parquet")
df_dim_proc = load_dim(r"C:/Projetos/PyPAH/dados_sia/rotulos/dim_procedimentos.parquet")

# Filtro de procedimentos
opcoes_proc = df_dim_proc['label_procedimento'].sort_values().tolist()

filtrar_proc = st.sidebar.checkbox(
    "Filtrar por procedimento",
    value=False
)
pa_proc_ids = None


if filtrar_proc:
    filtro_procedimentos = st.sidebar.multiselect(
        "Selecione os procedimentos",
        options=opcoes_proc
    )
    if filtro_procedimentos:
        map_proc_to_cod = dict(zip(df_dim_proc['label_procedimento'], df_dim_proc['PA_PROC_ID']))
        pa_proc_ids = [map_proc_to_cod[proc] for proc in filtro_procedimentos]


# Filtro de estabelecimentos
opcoes_estab = df_dim_est['label_estabelecimento'].sort_values().tolist()

filtrar_estab = st.sidebar.checkbox(
    "Filtrar por estabelecimento",
    value=False
)
pa_codunis = None


if filtrar_estab:
    filtro_estabelecimentos = st.sidebar.multiselect(
        "Selecione os estabelecimentos",
        options=opcoes_estab
    )
    if filtro_estabelecimentos:
        map_estab_to_cod = dict(zip(df_dim_est['label_estabelecimento'], df_dim_est['PA_CODUNI']))
        pa_codunis = [map_estab_to_cod[estab] for estab in filtro_estabelecimentos]







## Filtro de datas
# Filtro de anos
con = get_con()
anos_disp = anos_disponiveis(con)

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

meses_validos = meses_disponiveis_multi(
    con=con,
    anos=anos_sel
)


filtro_meses = st.sidebar.checkbox(
    "Filtrar por meses",
    value=False
)
if filtro_meses:
    meses = st.sidebar.multiselect(
        "Meses",
        options=meses_validos,
        default=meses_validos
    )




# Filtro de municípios
opcoes_mun = sorted(df["PA_MUNPCN"].unique().tolist())

filtro_municipio = st.sidebar.checkbox(
    "Filtrar por municípios",
    value=False
)

if filtro_municipio:
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
FROM fact_qtd_val
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
    st.write(df_filtro.head(20))