import os
import pandas as pd

from rapidfuzz import process, fuzz

from langchain_groq import ChatGroq
from langchain.tools import Tool
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser


# =============================================================================
# Descrição semântica das colunas — usada nos prompts das ferramentas
# =============================================================================

DESCRICAO_COLUNAS = """
- data_ref    → Data de referência (mês/ano) da produção ambulatorial
- PA_MUNPCN   → Código IBGE do município do paciente
- PA_CODUNI   → Código do estabelecimento de saúde (CNES) que realizou o procedimento
- PA_PROC_ID  → Código do procedimento ambulatorial realizado (tabela SIA/SUS)
- PA_VALPRO   → Valor produzido (R$): soma dos valores dos procedimentos realizados
- PA_VALAPR   → Valor aprovado (R$): soma dos valores aprovados pelo SUS para pagamento
- PA_QTDPRO   → Quantidade produzida: número de procedimentos realizados
- PA_QTDAPR   → Quantidade aprovada: número de procedimentos aprovados para pagamento
"""


def _get_llm():
    return ChatGroq(
        api_key=os.getenv("GROQ_API_KEY"),
        model_name="openai/gpt-oss-120b",
        temperature=0,
        model_kwargs={"reasoning_effort": "low"},
    )


# =============================================================================
# Helpers de enriquecimento com dimensões
# =============================================================================

def _enriquecer_df(
    df: pd.DataFrame,
    df_dim_est: pd.DataFrame,
    df_dim_proc: pd.DataFrame,
) -> pd.DataFrame:
    """
    Faz join do df de fatos com as dimensões de estabelecimento e procedimento,
    adicionando colunas legíveis de label ao lado dos códigos originais.
    O dashboard não usa essas colunas extras; elas existem apenas para o agente.
    """
    df = df.copy()

    # Join com dim_estabelecimento: PA_CODUNI → label_estabelecimento
    if "PA_CODUNI" in df.columns and not df_dim_est.empty:
        mapa_est = df_dim_est.set_index("PA_CODUNI")["label_estabelecimento"].to_dict()
        df["nome_estabelecimento"] = df["PA_CODUNI"].map(mapa_est).fillna(df["PA_CODUNI"])

    # Join com dim_procedimento: PA_PROC_ID → label_procedimento
    if "PA_PROC_ID" in df.columns and not df_dim_proc.empty:
        mapa_proc = df_dim_proc.set_index("PA_PROC_ID")["label_procedimento"].to_dict()
        df["nome_procedimento"] = df["PA_PROC_ID"].map(mapa_proc).fillna(df["PA_PROC_ID"])

    return df


def _top_com_label(
    df: pd.DataFrame,
    grupo_col: str,
    label_col: str,
    valor_col: str,
    n: int = 5,
) -> str:
    """
    Agrega df por grupo_col, soma valor_col e retorna os top-N
    exibindo label_col (nome legível) em vez do código bruto.
    Ex.: "Consulta em Clínica Médica (0301010072)  →  1.234.567"
    """
    if grupo_col not in df.columns or valor_col not in df.columns:
        return "(sem dados)"

    # Se a coluna de label existe, usa; senão cai de volta no código
    usar_label = label_col in df.columns

    agg = (
        df.groupby(grupo_col, as_index=False)
        .agg(
            **{valor_col: (valor_col, "sum")},
            **({label_col: (label_col, "first")} if usar_label else {}),
        )
        .sort_values(valor_col, ascending=False)
        .head(n)
    )

    linhas = []
    for _, row in agg.iterrows():
        if usar_label:
            descricao = f"{row[label_col]} ({row[grupo_col]})"
        else:
            descricao = str(row[grupo_col])
        linhas.append(f"  {descricao}  →  {row[valor_col]:,.2f}")

    return "\n".join(linhas) if linhas else "(sem dados)"


def _top_procedimentos_por_estabelecimento(
    df: pd.DataFrame,
    n_estab: int = 5,
    n_proc: int = 5,
) -> str:
    """
    Para cada um dos top-N estabelecimentos por PA_VALPRO, lista os top-N
    procedimentos (por PA_VALPRO) dentro daquele estabelecimento.
    Isso permite ao agente responder perguntas como:
    'Qual procedimento teve maior valor produzido no Hospital X?'
    """
    if "PA_CODUNI" not in df.columns or "PA_PROC_ID" not in df.columns:
        return "(sem dados)"

    usar_est_label = "nome_estabelecimento" in df.columns
    usar_proc_label = "nome_procedimento" in df.columns

    # Top estabelecimentos globais
    top_est = (
        df.groupby("PA_CODUNI")["PA_VALPRO"]
        .sum()
        .sort_values(ascending=False)
        .head(n_estab)
        .index.tolist()
    )

    blocos = []
    for cod_est in top_est:
        df_est = df[df["PA_CODUNI"] == cod_est]

        if usar_est_label:
            nome_est = df_est["nome_estabelecimento"].iloc[0]
            titulo = f"{nome_est} ({cod_est})"
        else:
            titulo = str(cod_est)

        total_est = df_est["PA_VALPRO"].sum()

        # Top procedimentos dentro deste estabelecimento
        top_proc = (
            df_est.groupby("PA_PROC_ID", as_index=False)
            .agg(
                PA_VALPRO=("PA_VALPRO", "sum"),
                PA_QTDPRO=("PA_QTDPRO", "sum"),
                **( {"nome_procedimento": ("nome_procedimento", "first")} if usar_proc_label else {} ),
            )
            .sort_values("PA_VALPRO", ascending=False)
            .head(n_proc)
        )

        linhas_proc = []
        for _, row in top_proc.iterrows():
            if usar_proc_label:
                desc_proc = f"{row['nome_procedimento']} ({row['PA_PROC_ID']})"
            else:
                desc_proc = str(row["PA_PROC_ID"])
            linhas_proc.append(
                f"    {desc_proc}  →  R$ {row['PA_VALPRO']:,.2f}  |  qtd: {row['PA_QTDPRO']:,.0f}"
            )

        bloco = (
            f"  Estabelecimento: {titulo}\n"
            f"  Total PA_VALPRO: R$ {total_est:,.2f}\n"
            f"  Top {n_proc} procedimentos por valor produzido:\n"
            + "\n".join(linhas_proc)
        )
        blocos.append(bloco)

    return "\n\n".join(blocos) if blocos else "(sem dados)"


# =============================================================================
# Ferramenta 1 — Informações gerais sobre os dados
# =============================================================================

def _informacoes_dados(
    pergunta: str,
    df: pd.DataFrame,
    df_dim_est: pd.DataFrame,
    df_dim_proc: pd.DataFrame,
) -> str:
    df_enr = _enriquecer_df(df, df_dim_est, df_dim_proc)
    shape = df_enr.shape
    colunas_tipos = df_enr.dtypes.to_string()

    # Lista de estabelecimentos e procedimentos presentes nos dados atuais
    estabelecimentos_presentes = ""
    if "nome_estabelecimento" in df_enr.columns:
        lista = (
            df_enr[["PA_CODUNI", "nome_estabelecimento"]]
            .drop_duplicates()
            .apply(lambda r: f"  {r['nome_estabelecimento']} ({r['PA_CODUNI']})", axis=1)
            .tolist()
        )
        estabelecimentos_presentes = "\n".join(lista[:20])  # limita a 20

    procedimentos_presentes = ""
    if "nome_procedimento" in df_enr.columns:
        lista = (
            df_enr[["PA_PROC_ID", "nome_procedimento"]]
            .drop_duplicates()
            .apply(lambda r: f"  {r['nome_procedimento']} ({r['PA_PROC_ID']})", axis=1)
            .tolist()
        )
        procedimentos_presentes = "\n".join(lista[:20])

    template = PromptTemplate(
        template="""
Você é um analista de dados do sistema de saúde público brasileiro (SIA/SUS).
Responda sempre em português.

Pergunta do usuário: {pergunta}

Informações do dataset atual (já filtrado conforme os filtros ativos no dashboard):

DIMENSÕES: {shape} (linhas x colunas)

COLUNAS E TIPOS:
{colunas_tipos}

DESCRIÇÃO SEMÂNTICA DAS COLUNAS:
{descricao_colunas}


ESTABELECIMENTOS PRESENTES NOS DADOS:
{estabelecimentos_presentes}

PROCEDIMENTOS PRESENTES NOS DADOS:
{procedimentos_presentes}

Com base nessas informações, escreva um resumo claro com:
1. Título: ## Relatório de informações gerais
2. Dimensão do dataset e o que cada linha representa
3. Descrição de cada coluna (nome, tipo e significado no contexto do SUS)
4. Quais estabelecimentos e procedimentos estão contemplados nos dados atuais
5. Sugestões de análises relevantes para esse contexto
        """,
        input_variables=[
            "pergunta", "shape", "colunas_tipos", "descricao_colunas",
            "estabelecimentos_presentes", "procedimentos_presentes",
        ],
    )

    cadeia = template | _get_llm() | StrOutputParser()
    return cadeia.invoke({
        "pergunta": pergunta,
        "shape": shape,
        "colunas_tipos": colunas_tipos,
        "descricao_colunas": DESCRICAO_COLUNAS,
        "estabelecimentos_presentes": estabelecimentos_presentes or "(não disponível)",
        "procedimentos_presentes": procedimentos_presentes or "(não disponível)",
    })


# =============================================================================
# Ferramenta 2 — Resumo estatístico descritivo
# =============================================================================

def _resumo_estatistico(
    pergunta: str,
    df: pd.DataFrame,
    df_dim_est: pd.DataFrame,
    df_dim_proc: pd.DataFrame,
) -> str:
    df_enr = _enriquecer_df(df, df_dim_est, df_dim_proc)
    resumo = df_enr[["PA_VALPRO", "PA_VALAPR", "PA_QTDPRO", "PA_QTDAPR"]].describe().transpose().to_string()

    # Contexto de escopo: quais estabelecimentos/procedimentos estão no recorte
    escopo = []
    if "nome_estabelecimento" in df_enr.columns:
        nomes = df_enr["nome_estabelecimento"].dropna().unique().tolist()
        escopo.append("Estabelecimentos: " + ", ".join(nomes[:10]))
    if "nome_procedimento" in df_enr.columns:
        nomes = df_enr["nome_procedimento"].dropna().unique().tolist()
        escopo.append("Procedimentos: " + ", ".join(nomes[:10]))

    escopo_texto = "\n".join(escopo) if escopo else "(filtros não aplicados)"

    template = PromptTemplate(
        template="""
Você é um analista de dados do sistema de saúde público brasileiro (SIA/SUS).
Responda sempre em português.

Pergunta do usuário: {pergunta}

ESCOPO DOS DADOS (filtros ativos no dashboard):
{escopo_texto}

Estatísticas descritivas das colunas numéricas:
{resumo}

DESCRIÇÃO DAS COLUNAS:
{descricao_colunas}

Elabore um relatório explicativo com:
1. Título: ## Relatório de estatísticas descritivas
2. Escopo dos dados analisados (quais estabelecimentos e procedimentos)
3. Visão geral dos valores de produção e aprovação ambulatorial
4. Um parágrafo para cada coluna numérica, comentando média, dispersão, mínimo e máximo
5. Identificação de possíveis outliers ou inconsistências (ex.: PA_VALAPR > PA_VALPRO)
6. Recomendações de próximos passos na análise
        """,
        input_variables=["pergunta", "escopo_texto", "resumo", "descricao_colunas"],
    )

    cadeia = template | _get_llm() | StrOutputParser()
    return cadeia.invoke({
        "pergunta": pergunta,
        "escopo_texto": escopo_texto,
        "resumo": resumo,
        "descricao_colunas": DESCRICAO_COLUNAS,
    })


# =============================================================================
# Resolução de entidade via fuzzy matching
# =============================================================================

def _selecionar_relevantes(pergunta: str, nomes: list, top_n: int) -> list:
    """
    Rankeia `nomes` (labels de procedimento/estabelecimento presentes no recorte
    atual) por relevância fuzzy em relação à `pergunta` e retorna os índices dos
    `top_n` mais relevantes.

    Substitui o corte arbitrário por posição (`.head(N)`) usado anteriormente para
    montar a amostra de exemplos enviada ao LLM: se o usuário menciona um
    estabelecimento ou procedimento específico (mesmo com nome parcial, abreviado
    ou digitado com pequenas variações), ele passa a aparecer na amostra em vez de
    depender de estar entre os primeiros N valores únicos do DataFrame.

    Não depende de nenhum modelo de embedding — usa apenas comparação lexical
    (rapidfuzz), suficiente para nomes de entidades curtos e estruturados como os
    do CNES/SIGTAP. Caso a pergunta não mencione nenhuma entidade específica, os
    scores ficam próximos entre si e o resultado se comporta como uma amostra
    qualquer (equivalente ao comportamento anterior).
    """
    if not nomes:
        return []

    matches = process.extract(
        pergunta,
        nomes,
        scorer=fuzz.token_set_ratio,
        limit=min(top_n, len(nomes)),
    )
    return [idx for _choice, _score, idx in matches]


# =============================================================================
# Ferramenta 3 — Execução de código pandas gerado pelo LLM
# =============================================================================

_PANDAS_SYSTEM_PROMPT = """
Você é um gerador de código Python/pandas para análise de dados do SIA/SUS.
Dado o schema do DataFrame e uma pergunta, escreva SOMENTE o código Python que calcula a resposta.

REGRAS OBRIGATÓRIAS:
1. O DataFrame já está disponível como `df` com as colunas descritas no schema.
2. As colunas de nomes JÁ ESTÃO PRONTAS no df — NÃO tente recriá-las:
   - `nome_procedimento`: nome legível do procedimento (ex: 'Consulta Médica em Atenção Especializada')
   - `nome_estabelecimento`: nome legível do estabelecimento (ex: 'Hospital Universitário Walter Cantídio')
   - `Ano`: ano extraído de data_ref (int)
   - `Mes`: mês extraído de data_ref (int)
   Se o schema indicar 'Nomes de procedimentos resolvidos: SIM', SEMPRE use `nome_procedimento`
   nas saídas — nunca exiba o código bruto PA_PROC_ID sozinho.
3. Sempre armazene o resultado final em uma variável chamada `resultado`.
4. `resultado` deve ser uma string formatada em Markdown com:
   a) Uma tabela Markdown listando TODOS os itens relevantes com nome e quantidade/valor
      (use tabela.to_markdown(index=False) — instale tabulate se necessário)
   b) Um parágrafo de conclusão destacando o maior valor
5. Em tabelas de resultado, SEMPRE inclua a coluna de nome (nome_procedimento ou
   nome_estabelecimento), não apenas códigos numéricos.
6. Quando a pergunta envolver 'consultas', filtre:
   df[df['nome_procedimento'].str.contains('Consulta', case=False, na=False)]
6b. Quando a pergunta mencionar um HOSPITAL/ESTABELECIMENTO específico pelo nome,
    filtre com correspondência parcial e sem diferenciar maiúsculas/minúsculas:
    df[df['nome_estabelecimento'].str.contains('TRECHO_DO_NOME', case=False, na=False)]
    Use apenas um trecho identificador do nome (ex: 'Walter Cantídio'), NÃO o nome inteiro.
6c. Para 'procedimento mais comum' (maior PA_QTDPRO) ou 'maior produção'
    (maior PA_VALPRO) dentro de um estabelecimento filtrado:
    df_filtrado.groupby('nome_procedimento', as_index=False)[['PA_QTDPRO','PA_VALPRO']].sum() \\
        .sort_values('PA_QTDPRO', ascending=False)
6d. Se a pergunta pedir 'produção ambulatorial' de um estabelecimento, retorne o
    total de PA_VALPRO (valor produzido) E o total de PA_QTDPRO (quantidade produzida)
    para aquele estabelecimento, junto com o detalhamento por procedimento.
7. NUNCA use print(). NUNCA use display(). Apenas atribua a `resultado`.
8. Trate erros com try/except e atribua a mensagem de erro a `resultado`.
9. Retorne APENAS o bloco de código Python puro, sem markdown, sem ```python, sem explicações.
"""

def _executar_pandas(
    pergunta: str,
    df: pd.DataFrame,
    df_dim_est: pd.DataFrame,
    df_dim_proc: pd.DataFrame,
) -> str:
    """
    Gera código pandas via LLM e executa no DataFrame enriquecido.
    Permite responder perguntas com filtros dinâmicos (por ano, procedimento,
    estabelecimento etc.) sem depender dos filtros do dashboard.
    """
    df_enr = _enriquecer_df(df, df_dim_est, df_dim_proc)

    # Garante colunas auxiliares de tempo
    if "data_ref" in df_enr.columns:
        df_enr["Ano"] = df_enr["data_ref"].dt.year
        df_enr["Mes"] = df_enr["data_ref"].dt.month

    # Verifica se o enriquecimento funcionou e monta amostras reais para o LLM
    tem_nome_proc = "nome_procedimento" in df_enr.columns
    tem_nome_est  = "nome_estabelecimento" in df_enr.columns

    proc_resolvidos = (
        tem_nome_proc
        and not df_enr["nome_procedimento"].dropna().empty
        and str(df_enr["nome_procedimento"].iloc[0]) != str(df_enr["PA_PROC_ID"].iloc[0])
    )
    est_resolvidos = (
        tem_nome_est
        and not df_enr["nome_estabelecimento"].dropna().empty
        and str(df_enr["nome_estabelecimento"].iloc[0]) != str(df_enr["PA_CODUNI"].iloc[0])
    )

    # Em vez de pegar arbitrariamente os primeiros N valores únicos, seleciona os
    # procedimentos/estabelecimentos presentes no recorte atual que são mais
    # relevantes (fuzzy) para a pergunta do usuário — ver `_selecionar_relevantes`.
    if tem_nome_proc:
        proc_unicos = (
            df_enr[["PA_PROC_ID", "nome_procedimento"]]
            .drop_duplicates("PA_PROC_ID")
            .reset_index(drop=True)
        )
        idxs_proc = _selecionar_relevantes(
            pergunta, proc_unicos["nome_procedimento"].tolist(), top_n=10
        )
        amostra_procs = [
            f"  {row.PA_PROC_ID} -> {row.nome_procedimento}"
            for row in proc_unicos.iloc[idxs_proc].itertuples()
        ]
    else:
        amostra_procs = []

    if tem_nome_est:
        est_unicos = (
            df_enr[["PA_CODUNI", "nome_estabelecimento"]]
            .drop_duplicates("PA_CODUNI")
            .reset_index(drop=True)
        )
        idxs_est = _selecionar_relevantes(
            pergunta, est_unicos["nome_estabelecimento"].tolist(), top_n=5
        )
        amostra_ests = [
            f"  {row.PA_CODUNI} -> {row.nome_estabelecimento}"
            for row in est_unicos.iloc[idxs_est].itertuples()
        ]
    else:
        amostra_ests = []

    schema = (
        "Colunas disponíveis: " + str(list(df_enr.columns)) + "\n"
        + "Anos disponíveis: " + str(sorted(df_enr["Ano"].unique().tolist()) if "Ano" in df_enr.columns else "N/A") + "\n"
        + "Nomes de procedimentos resolvidos: " + ("SIM - use a coluna nome_procedimento" if proc_resolvidos else "NAO - use PA_PROC_ID") + "\n"
        + "Nomes de estabelecimentos resolvidos: " + ("SIM - use a coluna nome_estabelecimento" if est_resolvidos else "NAO - use PA_CODUNI") + "\n"
        + "\nAmostra de procedimentos (codigo -> nome_procedimento):\n"
        + "\n".join(amostra_procs)
        + "\n\nAmostra de estabelecimentos (codigo -> nome_estabelecimento):\n"
        + "\n".join(amostra_ests)
        + "\n"
    )

    prompt = PromptTemplate(
        template="{system}\n\nSchema do DataFrame:\n{schema}\n\nPergunta: {pergunta}\n\nCódigo Python:",
        input_variables=["system", "schema", "pergunta"],
    )

    codigo_raw = (prompt | _get_llm() | StrOutputParser()).invoke({
        "system": _PANDAS_SYSTEM_PROMPT,
        "schema": schema,
        "pergunta": pergunta,
    })

    # Remove eventuais cercas de markdown que o LLM insista em colocar
    codigo = codigo_raw.strip()
    if codigo.startswith("```"):
        codigo = "\n".join(codigo.split("\n")[1:])
    if codigo.endswith("```"):
        codigo = "\n".join(codigo.split("\n")[:-1])
    codigo = codigo.strip()

    # Executa o código gerado num namespace isolado
    namespace = {"df": df_enr.copy(), "pd": pd}
    try:
        exec(codigo, namespace)  # noqa: S102
        resultado = namespace.get("resultado", "(código executado mas variável `resultado` não definida)")

        # Se resultado for um DataFrame vazio, evita to_markdown() quebrar e
        # dá um retorno explícito que o agente consegue interpretar.
        if isinstance(resultado, pd.DataFrame):
            if resultado.empty:
                resultado = (
                    "A consulta não retornou nenhuma linha. "
                    "Verifique se o nome do estabelecimento/procedimento usado no filtro "
                    "corresponde exatamente a um valor existente na coluna nome_estabelecimento "
                    "ou nome_procedimento (use correspondência parcial, sem diferenciar "
                    "maiúsculas/minúsculas e ignorando acentuação)."
                )
            else:
                try:
                    resultado = resultado.to_markdown(index=False)
                except ImportError:
                    resultado = resultado.to_string(index=False)

    except ImportError as exc:
        # Caso o código gerado tenha chamado to_markdown() sem tabulate instalado
        if "resultado" in namespace and isinstance(namespace["resultado"], pd.DataFrame):
            resultado = namespace["resultado"].to_string(index=False)
        else:
            resultado = f"Erro de dependência ao executar a análise: {exc}"
    except Exception as exc:
        resultado = (
            f"Não foi possível concluir a análise devido a um erro de execução: {exc}. "
            "Tente reformular a pergunta especificando o nome exato do estabelecimento "
            "ou procedimento conforme aparece nos dados."
        )

    if not str(resultado).strip():
        resultado = (
            "A consulta foi executada mas não produziu nenhum resultado. "
            "Verifique se o filtro (nome do estabelecimento, procedimento ou ano) "
            "corresponde a valores existentes nos dados."
        )

    return str(resultado)


# =============================================================================
# Ferramenta 4 — Consultas contextuais sobre o DataFrame (agregados estáticos)
# =============================================================================

_COLUNAS_NUMERICAS = ["PA_VALPRO", "PA_VALAPR", "PA_QTDPRO", "PA_QTDAPR"]
_COLUNAS_CATEGORICAS = ["PA_MUNPCN", "PA_CODUNI", "PA_PROC_ID", "data_ref"]


def _consulta_dados(
    pergunta: str,
    df: pd.DataFrame,
    df_dim_est: pd.DataFrame,
    df_dim_proc: pd.DataFrame,
) -> str:
    df_enr = _enriquecer_df(df, df_dim_est, df_dim_proc)

    # Totais gerais
    resumo_linhas = []
    for col in _COLUNAS_NUMERICAS:
        if col in df_enr.columns:
            resumo_linhas.append(
                f"{col} — soma: {df_enr[col].sum():,.2f} | "
                f"média: {df_enr[col].mean():,.2f} | "
                f"máx: {df_enr[col].max():,.2f} | "
                f"mín: {df_enr[col].min():,.2f}"
            )
    for col in _COLUNAS_CATEGORICAS:
        if col in df_enr.columns:
            resumo_linhas.append(f"{col} — {df_enr[col].nunique()} valores únicos")

    # Top 5 com labels legíveis
    top_municipios = _top_com_label(df_enr, "PA_MUNPCN", "PA_MUNPCN", "PA_VALPRO")

    top_estabelecimentos = _top_com_label(
        df_enr, "PA_CODUNI", "nome_estabelecimento", "PA_VALPRO"
    )

    top_procedimentos_qtd = _top_com_label(
        df_enr, "PA_PROC_ID", "nome_procedimento", "PA_QTDPRO"
    )

    top_procedimentos_val = _top_com_label(
        df_enr, "PA_PROC_ID", "nome_procedimento", "PA_VALPRO"
    )

    # Análise cruzada: top procedimentos dentro de cada estabelecimento
    procedimentos_por_estabelecimento = _top_procedimentos_por_estabelecimento(df_enr)

    # Evolução mensal
    evolucao = "(sem dados)"
    if "data_ref" in df_enr.columns and "PA_VALPRO" in df_enr.columns:
        evo = (
            df_enr.groupby("data_ref")[["PA_VALPRO", "PA_QTDPRO"]]
            .sum()
            .sort_index()
        )
        evolucao = evo.to_string()

    resumo_agregado = "\n".join(resumo_linhas)

    template = PromptTemplate(
        template="""
Você é um assistente especializado em análise de dados do SIA/SUS (Sistema de Informações Ambulatoriais do SUS).
Responda sempre em português, de forma clara e objetiva.

Pergunta do usuário: {pergunta}

DESCRIÇÃO DAS COLUNAS DO DATASET:
{descricao_colunas}

RESUMO AGREGADO DOS DADOS ATUAIS:
{resumo_agregado}

TOP 5 MUNICÍPIOS POR VALOR PRODUZIDO (PA_VALPRO):
{top_municipios}

TOP 5 ESTABELECIMENTOS POR VALOR PRODUZIDO (código → nome):
{top_estabelecimentos}

TOP 5 PROCEDIMENTOS POR QUANTIDADE PRODUZIDA (código → nome):
{top_procedimentos_qtd}

TOP 5 PROCEDIMENTOS POR VALOR PRODUZIDO (código → nome):
{top_procedimentos_val}

TOP PROCEDIMENTOS POR ESTABELECIMENTO (análise cruzada — quais procedimentos cada estabelecimento mais produziu):
{procedimentos_por_estabelecimento}

EVOLUÇÃO MENSAL (data_ref × PA_VALPRO e PA_QTDPRO):
{evolucao}

Com base nesses dados, responda à pergunta do usuário de forma direta e informativa,
usando os nomes dos estabelecimentos e procedimentos (não apenas os códigos).
A seção "TOP PROCEDIMENTOS POR ESTABELECIMENTO" contém o detalhamento por estabelecimento
e deve ser usada para responder perguntas como "qual procedimento teve maior valor no Hospital X?"
        """,
        input_variables=[
            "pergunta", "descricao_colunas", "resumo_agregado",
            "top_municipios", "top_estabelecimentos",
            "top_procedimentos_qtd", "top_procedimentos_val",
            "procedimentos_por_estabelecimento", "evolucao",
        ],
    )

    cadeia = template | _get_llm() | StrOutputParser()
    return cadeia.invoke({
        "pergunta": pergunta,
        "descricao_colunas": DESCRICAO_COLUNAS,
        "resumo_agregado": resumo_agregado,
        "top_municipios": top_municipios,
        "top_estabelecimentos": top_estabelecimentos,
        "top_procedimentos_qtd": top_procedimentos_qtd,
        "top_procedimentos_val": top_procedimentos_val,
        "procedimentos_por_estabelecimento": procedimentos_por_estabelecimento,
        "evolucao": evolucao,
    })


# =============================================================================
# Função pública — cria e retorna a lista de ferramentas para o agente
# =============================================================================

def criar_ferramentas(
    df: pd.DataFrame,
    df_dim_est: pd.DataFrame,
    df_dim_proc: pd.DataFrame,
) -> list:
    """
    Recebe o DataFrame de fatos (já filtrado) e as duas dimensões,
    e retorna a lista de Tools do LangChain para uso no agente ReAct.
    """

    ferramenta_pandas = Tool(
        name="executar_analise",
        func=lambda pergunta: _executar_pandas(pergunta, df, df_dim_est, df_dim_proc),
        description=(
            "Use esta ferramenta para QUALQUER pergunta analítica que envolva filtros, "
            "agrupamentos ou cálculos dinâmicos. Exemplos: "
            "'Liste a quantidade de consultas no ano de 2023', "
            "'Qual procedimento teve maior valor produzido no Hospital X?', "
            "'Quais os top 10 procedimentos por quantidade em 2024?', "
            "'Qual o total de PA_VALPRO por estabelecimento em março de 2023?', "
            "'Liste todos os procedimentos realizados no ano de ****'. "
            "Esta ferramenta executa código pandas real sobre os dados, podendo "
            "filtrar por ano, mês, estabelecimento, procedimento ou qualquer combinação. "
            "É a escolha correta para perguntas com filtros temporais ou por entidade específica."
        ),
        return_direct=False,
    )

    ferramenta_informacoes = Tool(
        name="informacoes_dados",
        func=lambda pergunta: _informacoes_dados(pergunta, df, df_dim_est, df_dim_proc),
        description=(
            "Use APENAS quando o usuário pedir um panorama geral do dataset: "
            "quais colunas existem, o que cada coluna significa, quantas linhas há. "
            "NÃO use para perguntas analíticas, rankings, valores, totais ou comparações. "
            "NÃO use se a pergunta mencionar um estabelecimento, procedimento, ano ou valor específico."
        ),
        return_direct=False,
    )

    ferramenta_estatisticas = Tool(
        name="resumo_estatistico",
        func=lambda pergunta: _resumo_estatistico(pergunta, df, df_dim_est, df_dim_proc),
        description=(
            "Use quando o usuário pedir estatísticas descritivas completas: "
            "média, desvio padrão, mínimo, máximo das colunas numéricas. "
            "NÃO use para perguntas sobre rankings, totais ou comparações entre entidades."
        ),
        return_direct=False,
    )

    ferramenta_consulta = Tool(
        name="consulta_dados",
        func=lambda pergunta: _consulta_dados(pergunta, df, df_dim_est, df_dim_proc),
        description=(
            "Use esta ferramenta para obter um panorama analítico geral dos dados atuais: "
            "totais globais, top 5 estabelecimentos, top 5 procedimentos por valor/quantidade, "
            "evolução mensal. NÃO use se a pergunta pede filtro por ano, mês, ou entidade específica — "
            "nesse caso use 'executar_analise'."
        ),
        return_direct=False,
    )

    return [ferramenta_pandas, ferramenta_informacoes, ferramenta_estatisticas, ferramenta_consulta]