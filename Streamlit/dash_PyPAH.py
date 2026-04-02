import requests
import os
import streamlit as st
import pandas as pd
import plotly.express as px

API_URL = os.environ["API_URL"]


def _get(endpoint: str, params: dict = None):
    url = f"{API_URL}/api/{endpoint}"
    response = requests.get(url, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


@st.cache_data
def optimize_plotly(fig):
    fig.update_layout(
        hovermode="closest",
        transition_duration=0
    )
    return fig

@st.cache_data(ttl=3600)
def anos_disponiveis():
    return _get("anos")


@st.cache_data(ttl=3600)
def meses_disponiveis_multi(anos):
    return _get("meses", params={"anos": anos})


@st.cache_data(ttl=3600)
def municipios_disponiveis():
    return _get("municipios")


@st.cache_data(ttl=3600)
def load_dim_estabelecimento():
    return pd.DataFrame(_get("estabelecimentos"))


@st.cache_data(ttl=3600)
def load_dim_procedimento():
    return pd.DataFrame(_get("procedimentos"))


@st.cache_data(show_spinner=True)
def dados_filtrados(anos, meses, municipios, pa_codunis, pa_proc_ids):
    params = {}
    if anos:
        params["anos"] = anos
    if meses:
        params["meses"] = meses
    if municipios:
        params["municipios"] = municipios
    if pa_codunis:
        params["pa_codunis"] = pa_codunis
    if pa_proc_ids:
        params["pa_proc_ids"] = pa_proc_ids

    return pd.DataFrame(_get("dados", params=params))

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


df_filtro = dados_filtrados(
    anos=anos_sel,
    meses=meses,
    municipios=municipios,
    pa_codunis=pa_codunis,
    pa_proc_ids=pa_proc_ids
)






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
