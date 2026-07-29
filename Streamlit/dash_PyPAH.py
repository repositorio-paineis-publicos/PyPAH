import requests
import os
import streamlit as st
import pandas as pd
import plotly.express as px

from langchain_groq import ChatGroq
from langchain.prompts import PromptTemplate
from langchain.agents import create_react_agent, AgentExecutor
from langchain.memory import ConversationBufferMemory
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ferramentas import criar_ferramentas, DESCRICAO_COLUNAS

load_dotenv()
API_URL = os.environ["API_URL"]


# =============================================================================
# Helpers de API
# =============================================================================

def _get(endpoint: str, params: dict = None):
    url = f"{API_URL}/api/{endpoint}"
    response = requests.get(url, params=params, timeout=120)
    response.raise_for_status()
    return response.json()


@st.cache_data
def optimize_plotly(fig):
    fig.update_layout(hovermode="closest", transition_duration=0)
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


# =============================================================================
# Configuração da página
# =============================================================================

st.set_page_config(layout="wide")
st.title("PyPah Dashboard")
st.sidebar.title("Filtros")


# =============================================================================
# Dimensões
# =============================================================================

df_dim_est = load_dim_estabelecimento()
df_dim_proc = load_dim_procedimento()


# =============================================================================
# Filtros — Procedimentos
# =============================================================================

filtrar_proc = st.sidebar.checkbox("Filtrar por procedimento", value=False)
pa_proc_ids = None

if filtrar_proc:
    with st.spinner("Carregando procedimentos..."):
        opcoes_proc = df_dim_proc["label_procedimento"].sort_values().tolist()

    filtro_procedimentos = st.sidebar.multiselect("Selecione os procedimentos", options=opcoes_proc)
    if filtro_procedimentos:
        map_proc_to_cod = dict(zip(df_dim_proc["label_procedimento"], df_dim_proc["PA_PROC_ID"]))
        pa_proc_ids = [map_proc_to_cod[proc] for proc in filtro_procedimentos]


# =============================================================================
# Filtros — Estabelecimentos
# =============================================================================

filtrar_estab = st.sidebar.checkbox("Filtrar por estabelecimento", value=False)
pa_codunis = None

if filtrar_estab:
    with st.spinner("Carregando estabelecimentos..."):
        opcoes_estab = df_dim_est["label_estabelecimento"].sort_values().tolist()
    filtro_estabelecimentos = st.sidebar.multiselect("Selecione os estabelecimentos", options=opcoes_estab)
    if filtro_estabelecimentos:
        map_estab_to_cod = dict(zip(df_dim_est["label_estabelecimento"], df_dim_est["PA_CODUNI"]))
        pa_codunis = [map_estab_to_cod[estab] for estab in filtro_estabelecimentos]


# =============================================================================
# Filtros — Anos
# =============================================================================

anos_disp = anos_disponiveis()
if not anos_disp:
    st.warning(
        "Nenhum dado disponível. Execute a pipeline para carregar os dados."
    )
    st.stop()
    
filtro_anos = False

if len(anos_disp) > 1:
    filtro_anos = st.sidebar.checkbox("Filtrar por anos", value=False)

if filtro_anos:
    ano_sel = st.sidebar.slider("Ano", min(anos_disp), max(anos_disp))
    anos_sel = [ano_sel]
else:
    anos_sel = anos_disp


# =============================================================================
# Filtros — Meses
# =============================================================================

meses = None
filtro_meses = st.sidebar.checkbox("Filtrar por meses", value=False)

if filtro_meses:
    with st.spinner("Carregando meses..."):
        meses_validos = meses_disponiveis_multi(anos=anos_sel)
    meses = st.sidebar.multiselect("Meses", options=meses_validos, default=meses_validos)


# =============================================================================
# Filtros — Municípios
# =============================================================================

filtro_municipio = st.sidebar.checkbox("Filtrar por municípios", value=False)

if filtro_municipio:
    with st.spinner("Carregando municípios..."):
        opcoes_mun = municipios_disponiveis()
    municipios = st.sidebar.multiselect("Selecione os municípios", options=opcoes_mun)
else:
    municipios = None


# =============================================================================
# Carrega dados filtrados
# =============================================================================

df_filtro = dados_filtrados(
    anos=anos_sel,
    meses=meses,
    municipios=municipios,
    pa_codunis=pa_codunis,
    pa_proc_ids=pa_proc_ids,
)

df_filtro["data_ref"] = pd.to_datetime(df_filtro["data_ref"])


# =============================================================================
# Preparação das tabelas para gráficos
# (agora agrupa por data_ref para os gráficos, ignorando as colunas extras)
# =============================================================================

MAPA_CORES = {
    "Produzido": "#1f4fd8",
    "Aprovado": "#7aa6ff",
}

df_grafico = df_filtro.groupby("data_ref", as_index=False).agg(
    PA_VALPRO=("PA_VALPRO", "sum"),
    PA_VALAPR=("PA_VALAPR", "sum"),
    PA_QTDPRO=("PA_QTDPRO", "sum"),
    PA_QTDAPR=("PA_QTDAPR", "sum"),
)

# Barras — por ano
valores_bar = (
    df_grafico.copy()
    .assign(Ano=df_grafico["data_ref"].dt.year)
    .groupby("Ano", as_index=False)
    .agg(PA_VALPRO=("PA_VALPRO", "sum"), PA_VALAPR=("PA_VALAPR", "sum"))
    .melt(id_vars="Ano", value_vars=["PA_VALPRO", "PA_VALAPR"], var_name="tipo", value_name="valor")
    .replace({"PA_VALPRO": "Produzido", "PA_VALAPR": "Aprovado"})
    .sort_values("tipo")
)

quant_bar = (
    df_grafico.copy()
    .assign(Ano=df_grafico["data_ref"].dt.year)
    .groupby("Ano", as_index=False)
    .agg(PA_QTDPRO=("PA_QTDPRO", "sum"), PA_QTDAPR=("PA_QTDAPR", "sum"))
    .melt(id_vars="Ano", value_vars=["PA_QTDPRO", "PA_QTDAPR"], var_name="tipo", value_name="quantidade")
    .replace({"PA_QTDPRO": "Produzido", "PA_QTDAPR": "Aprovado"})
    .sort_values("tipo")
)

# Linhas — por mês
df_linha_val_long = df_grafico.melt(
    id_vars="data_ref",
    value_vars=["PA_VALPRO", "PA_VALAPR"],
    var_name="tipo", value_name="valor",
).replace({"PA_VALPRO": "Produzido", "PA_VALAPR": "Aprovado"}).sort_values(["tipo", "data_ref"]).reset_index(drop=True)

df_linha_qtd_long = df_grafico.melt(
    id_vars="data_ref",
    value_vars=["PA_QTDPRO", "PA_QTDAPR"],
    var_name="tipo", value_name="quantidade",
).replace({"PA_QTDPRO": "Produzido", "PA_QTDAPR": "Aprovado"}).sort_values(["tipo", "data_ref"]).reset_index(drop=True)


# =============================================================================
# Gráficos — Valores
# =============================================================================

fig_valores_lin = px.line(
    df_linha_val_long,
    x="data_ref", y="valor", color="tipo",
    title="Valor Produzido por Mês/Ano",
    color_discrete_map=MAPA_CORES,
)
fig_valores_lin.update_traces(
    mode="lines+markers",
    line=dict(width=2),
    marker=dict(size=8, symbol="circle", line=dict(width=1)),
    hovertemplate="Mês/Ano: %{x}<br>%{fullData.name}: %{y:,.0f}<extra></extra>",
)
fig_valores_lin.add_hline(
    y=df_linha_val_long["valor"].mean(),
    line_dash="dash",
    annotation_text=f"Média: {df_linha_val_long['valor'].mean():,.0f}",
    annotation_position="right",
)
fig_valores_lin = optimize_plotly(fig_valores_lin)
fig_valores_lin.update_layout(
    yaxis_title="Valor (R$)", xaxis_title="Mês/Ano",
    legend=dict(traceorder="normal"),
)

fig_valores_bar = px.bar(
    valores_bar, x="Ano", y="valor", color="tipo",
    barmode="group", title="Valor Produzido x Aprovado por Ano",
    color_discrete_map=MAPA_CORES,
)
fig_valores_bar.update_traces(hovertemplate="Ano: %{x}<br>Valor: %{y:,.0f}<extra></extra>")
fig_valores_bar = optimize_plotly(fig_valores_bar)
fig_valores_bar.update_layout(
    yaxis_title="Valor (R$)", xaxis_title="Ano",
    legend=dict(traceorder="normal"),
)


# =============================================================================
# Gráficos — Quantidade
# =============================================================================

fig_quant_lin = px.line(
    df_linha_qtd_long,
    x="data_ref", y="quantidade", color="tipo",
    title="Quantidade Produzida por Mês/Ano",
    color_discrete_map=MAPA_CORES,
)
fig_quant_lin.update_traces(
    mode="lines+markers",
    line=dict(width=2),
    marker=dict(size=8, symbol="circle", line=dict(width=1)),
    hovertemplate="Mês/Ano: %{x}<br>%{fullData.name}: %{y:,.0f}<extra></extra>",
)
fig_quant_lin.add_hline(
    y=df_linha_qtd_long["quantidade"].mean(),
    line_dash="dash",
    annotation_text=f"Média: {df_linha_qtd_long['quantidade'].mean():,.0f}",
    annotation_position="right",
)
fig_quant_lin = optimize_plotly(fig_quant_lin)
fig_quant_lin.update_layout(
    yaxis_title="Quantidade", xaxis_title="Mês/Ano",
    legend=dict(traceorder="normal"),
)

fig_quant_bar = px.bar(
    quant_bar, x="Ano", y="quantidade", color="tipo",
    barmode="group", title="Quantidade Produzida x Aprovada por Ano",
    color_discrete_map=MAPA_CORES,
)
fig_quant_bar.update_traces(hovertemplate="Ano: %{x}<br>Quantidade: %{y:,.0f}<extra></extra>")
fig_quant_bar = optimize_plotly(fig_quant_bar)
fig_quant_bar.update_layout(
    yaxis_title="Quantidade", xaxis_title="Ano",
    legend=dict(traceorder="normal"),
)


# =============================================================================
# Visualização no Streamlit — abas
# =============================================================================

aba1, aba2, aba3, aba4 = st.tabs(["Valores", "Quantidade", "Tabela de dados", "💬 Assistente IA"])

with aba1:
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_valores_lin, width="stretch", config={"displayModeBar": False, "scrollZoom": False})
    with col2:
        st.plotly_chart(fig_valores_bar, width="stretch", config={"displayModeBar": False, "scrollZoom": False})

with aba2:
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_quant_lin, width="stretch", config={"displayModeBar": False, "scrollZoom": False})
    with col2:
        st.plotly_chart(fig_quant_bar, width="stretch", config={"displayModeBar": False, "scrollZoom": False})

with aba3:
    st.write(df_filtro.shape)
    st.write(df_filtro.head(20))


# =============================================================================
# Aba 4 — Assistente IA com memória de conversa
# =============================================================================

with aba4:
    st.markdown("### 🤖 Assistente de análise de dados")
    st.info(
        "O assistente analisa os **dados atualmente filtrados** no dashboard. "
        "Ajuste os filtros na barra lateral e faça suas perguntas — ele lembra do contexto da conversa, "
        "então você pode fazer perguntas encadeadas como *'E no ano anterior?'* sem repetir o contexto."
    )

    # ------------------------------------------------------------------
    # Inicializa estado da sessão
    # ------------------------------------------------------------------

    if "historico_chat" not in st.session_state:
        st.session_state.historico_chat = []  # lista de {"role": ..., "content": ...}

    if "memoria_agente" not in st.session_state:
        st.session_state.memoria_agente = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=False,
        )

    # Reconstrói o agente sempre que o DataFrame muda (filtros alterados).
    # As dimensões são estáveis (ttl=3600), mas entram na chave por segurança.
    chave_df = (df_filtro.shape, tuple(df_filtro.columns.tolist()))

    if st.session_state.get("_chave_df_anterior") != chave_df:
        st.session_state._chave_df_anterior = chave_df
        # Recria ferramentas com o novo df e as dimensões; mantém a memória intacta
        st.session_state._tools = criar_ferramentas(df_filtro, df_dim_est, df_dim_proc)

        llm_agente = ChatGroq(
            api_key=os.getenv("GROQ_API_KEY"),
            model_name="llama-3.3-70b-versatile",
            temperature=0,
        )

        tool_names = ", ".join([t.name for t in st.session_state._tools])
        tools_desc = "\n".join([f"- {t.name}: {t.description}" for t in st.session_state._tools])

        prompt_agente = PromptTemplate(
            input_variables=["input", "agent_scratchpad", "tools", "tool_names", "chat_history"],
            template=f"""
Você é um assistente especializado em dados do SIA/SUS (Sistema de Informações Ambulatoriais do SUS).
Responda sempre em português. Seja objetivo e claro.

Contexto das colunas disponíveis:
{DESCRICAO_COLUNAS}

Histórico da conversa:
{{chat_history}}

Você tem acesso às seguintes ferramentas:
{{tools}}

Use o seguinte formato:

Question: a pergunta de entrada que você deve responder
Thought: você deve sempre pensar no que fazer
Action: a ação a ser tomada, deve ser uma de [{{tool_names}}]
Action Input: a entrada para a ação
Observation: o resultado da ação
... (este Thought/Action/Action Input/Observation pode se repetir N vezes)
Thought: Agora eu sei a resposta final
Final Answer: a resposta final para a pergunta de entrada original

Comece!

Question: {{input}}
Thought: {{agent_scratchpad}}""",
        )

        agente = create_react_agent(
            llm=llm_agente,
            tools=st.session_state._tools,
            prompt=prompt_agente,
        )

        st.session_state._orquestrador = AgentExecutor(
            agent=agente,
            tools=st.session_state._tools,
            memory=st.session_state.memoria_agente,
            verbose=True,
            handle_parsing_errors=True,
        )

    # ------------------------------------------------------------------
    # Ações rápidas
    # ------------------------------------------------------------------

    st.markdown("#### ⚡ Ações rápidas")
    col_btn1, col_btn2, col_btn3 = st.columns(3)

    with col_btn1:
        if st.button("📄 Informações gerais", key="btn_info"):
            with st.spinner("Gerando relatório... 🧞"):
                resposta = st.session_state._orquestrador.invoke(
                    {"input": "Quero um relatório com informações gerais sobre os dados"}
                )
            msg = resposta["output"]
            st.session_state.historico_chat.append({"role": "assistant", "content": msg, "acao_rapida": True})

    with col_btn2:
        if st.button("📊 Estatísticas descritivas", key="btn_estat"):
            with st.spinner("Gerando relatório... 🧞"):
                resposta = st.session_state._orquestrador.invoke(
                    {"input": "Quero um relatório de estatísticas descritivas dos dados"}
                )
            msg = resposta["output"]
            st.session_state.historico_chat.append({"role": "assistant", "content": msg, "acao_rapida": True})

    with col_btn3:
        if st.button("🗑️ Limpar conversa", key="btn_limpar"):
            st.session_state.historico_chat = []
            st.session_state.memoria_agente.clear()
            st.rerun()

    # ------------------------------------------------------------------
    # Histórico de mensagens
    # ------------------------------------------------------------------

    st.markdown("---")
    st.markdown("#### 💬 Conversa")

    for msg in st.session_state.historico_chat:
        role = msg["role"]
        with st.chat_message(role):
            st.markdown(msg["content"])

    # ------------------------------------------------------------------
    # Input do usuário
    # ------------------------------------------------------------------

    pergunta = st.chat_input("Faça uma pergunta sobre os dados (ex: Qual município teve maior produção?)")

    if pergunta:
        # Exibe a mensagem do usuário imediatamente
        st.session_state.historico_chat.append({"role": "user", "content": pergunta})
        with st.chat_message("user"):
            st.markdown(pergunta)

        # Processa com o agente
        with st.chat_message("assistant"):
            with st.spinner("Analisando... 🧞"):
                resposta = st.session_state._orquestrador.invoke({"input": pergunta})
            resposta_texto = resposta["output"]
            st.markdown(resposta_texto)

        st.session_state.historico_chat.append({"role": "assistant", "content": resposta_texto})