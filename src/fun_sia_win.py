import os
import math
from ftplib import FTP


import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq



def baixar_dbc(grupo, estado, ano, meses, destino='../dados_sia/dados_dbc'):

    """
    Baixa arquivos .dbc do FTP do DATASUS por grupo, estado, ano e meses especificados e salva na pasta destino."""

    #Cria pasta destino se não existir
    os.makedirs(destino, exist_ok=True)
    
    # Conecta ao FTP do DATASUS
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd('/dissemin/publicos/SIASUS/200801_/Dados')
    
    # Baixa os arquivos especificados
    for mes in meses:
        nome_arquivo = f"{grupo}{estado}{str(ano)[-2:]}{mes:02d}.dbc"
        caminho_local = os.path.join(destino, nome_arquivo)
        print(f"Baixando {nome_arquivo}...")

        try:
            with open(caminho_local, 'wb') as f:
                ftp.retrbinary(f"RETR {nome_arquivo}", f.write)
            print(f"{nome_arquivo} salvo em {caminho_local}")
        except Exception as e:
            print(f"Erro com {nome_arquivo}: {e}")
    
    # Encerra a conexão FTP
    ftp.quit()
    


def tratar_dados_sia(
    pasta,
    colunas=None,
    hospitais=None,
    alvo_ram_mb=600,
    piloto=200_000,
    arquivo_saida="siasus_tratado.parquet",
    verbose=True
):
    
    """
    Docstring for carregar_dados_sia
    
    :param pasta: Pasta onde os arquivo .parquet estão armazenados
    :param colunas: Colunas a serem carregadas
    :param hospitais: Hospitais a serem filtrados
    :param alvo_ram_mb: Ram alvo em MB para cálculo do batch size
    :param piloto: Número de linhas para o batch piloto
    :param arquivo_saida: Caminho do arquivo de saída
    :param verbose: Se True, imprime informações de progresso
    """

    from tqdm import tqdm

    # Cria um Arrow Dataset a partir da pasta e varre a pasta procurando todos os arquivos .parquet
    dataset = ds.dataset(pasta, format="parquet")


    # Se houver hospitais para filtrar, cria o filtro
    filtro = None
    if hospitais:
        filtro = (ds.field("PA_CODUNI").isin(hospitais))

    # Scanner piloto que será usado para estimar o uso de memória por linha
    scanner_piloto = ds.Scanner.from_dataset(
        dataset,
        columns=colunas,
        filter=filtro,
        batch_size=piloto,
        use_threads=True
    )


    # Obtém um batch do scanner piloto
    batch_teste = next(scanner_piloto.to_batches())

    # Converte o batch para pandas para medir o uso de memória
    df_teste = batch_teste.to_pandas(ignore_metadata=True)

    # Calcula o uso de memória por linha
    uso_bytes = df_teste.memory_usage(deep=True).sum()
    uso_por_linha = uso_bytes / len(df_teste)

    # Calcula o batch size ideal com base na RAM alvo
    batch_size_ideal = int((alvo_ram_mb * 1024**2) / uso_por_linha)
    batch_size_ideal = max(batch_size_ideal, 20_000)

    # Imprime o batch size ideal se verbose estiver ativado
    if verbose:
        print(f"Batch size ideal ≈ {batch_size_ideal:,} linhas")

    # Inicializa o writer Parquet e a barra de progresso
    writer = None
    pbar = tqdm(desc="Processando batches", unit="batch") if verbose else None

    # Itera sobre os fragmentos(arquivos .parquet dentro das subpastas) do dataset
    for fragment in dataset.get_fragments():
        pasta_origem = os.path.basename(os.path.dirname(fragment.path))

        # Cria um scanner para o fragmento atual
        # Scanner sabe colunas, filtro e batch size, quais arquivos ler e leitura paralelizada
        scanner_frag = ds.Scanner.from_fragment(
            fragment,
            columns=colunas,
            filter=filtro,
            batch_size=batch_size_ideal,
            use_threads=True
        )


        # Itera sobre os batches do scanner do fragmento
        for batch in scanner_frag.to_batches():

            # Pula batch vazio (acontece quando o filtro remove todas as linhas do arquivo)
            if batch.num_rows == 0:
                continue
            
            # Converte o batch para pandas
            df = batch.to_pandas(ignore_metadata=True)

            # Remove colunas duplicadas, se houver
            df = df.loc[:, ~df.columns.duplicated()]

            # Adiciona coluna de arquivo de origem
            df["arquivo_origem"] = pasta_origem

            tabela = pa.Table.from_pandas(df, preserve_index=False)
            tabela = tabela.replace_schema_metadata(None)

            # Inicializa o ParquetWriter 
            if writer is None:
                schema_fixo = tabela.schema.remove_metadata()
                writer = pq.ParquetWriter(
                    arquivo_saida,
                    schema_fixo,
                    use_dictionary=True,
                    compression="snappy"
                )

            # Mapa para o ParquetWriter saber como salvar os dados e garantir consistência
            tabela = tabela.cast(schema_fixo)

            # Escreve a tabela no arquivo de saída
            writer.write_table(tabela)

            # Atualiza a barra de progresso
            if pbar is not None:
                pbar.update()


    # Fechar Writer/Barra de forma segura
    if writer is not None:
        writer.close()
    if pbar is not None:
        pbar.close()

    if verbose:
        print(f"\n Arquivo final salvo em: {arquivo_saida}")



def corrigir_colunas_PA(df):
    cols_quant = ['PA_QTDPRO', 'PA_QTDAPR']
    cols_val = ['PA_VALPRO', 'PA_VALAPR']

    df_novo = df[cols_quant + cols_val + ['arquivo_origem']
                     ]
    df_novo[cols_quant] = df_novo[cols_quant].fillna(0).astype(int)
    df_novo[cols_val] = df_novo[cols_val].fillna(0).astype(float)

    return df_novo



def somatorio_anual_PA(df):


  somas = {
    'PA_QTDPRO': df['PA_QTDPRO'].sum(),
    'PA_QTDAPR': df['PA_QTDAPR'].sum(),
    'PA_VALPRO': df['PA_VALPRO'].sum(),
    'PA_VALAPR': df['PA_VALAPR'].sum()
  }

  df_somas = pd.DataFrame({
    'Tipo': ['Quantidade', 'Valor'],
    'PRO': [somas['PA_QTDPRO'], somas['PA_VALPRO']],
    'APR': [somas['PA_QTDAPR'], somas['PA_VALAPR']]
  })

  return df_somas



def plot_PA(df, agrupamento='arquivo_origem', mostrar=['quantidade','valor']):


    """
    Plota comparação PRO vs APR com barras lado a lado e cores distintas.
    
    Parâmetros:
    - df: DataFrame com colunas PA_QTDPRO, PA_QTDAPR, PA_VALPRO, PA_VALAPR e coluna de agrupamento
    - agrupamento: coluna para agrupar ('arquivo_origem' = mês, 'ano', etc.)
    - mostrar: lista com 'quantidade', 'valor' ou ambos
    """


    import matplotlib.pyplot as plt
    import seaborn as sns
    import numpy as np


    # Agrupa os dados
    df_agrupado = df.groupby(agrupamento).sum(numeric_only=True).reset_index()
    categorias = df_agrupado[agrupamento]
    n_categorias = len(categorias)
    
    # Configura cores distintas
    cores = {
        'PRO_quantidade': '#1f77b4',  # azul escuro
        'APR_quantidade': '#ff7f0e',  # laranja
        'PRO_valor': '#2ca02c',       # verde
        'APR_valor': '#d62728',       # vermelho
    }
    
    # Configura posição das barras
    largura = 0.2  # largura de cada barra
    total_barras = len(mostrar) * 2  # PRO + APR para cada tipo
    offsets = np.linspace(-largura*total_barras/2 + largura/2, largura*total_barras/2 - largura/2, total_barras)
    
    fig, ax = plt.subplots(figsize=(12,6))
    
    barras = []
    idx = 0
    for tipo in mostrar:
        # PRO
        barras_pro = ax.bar(x= np.arange(n_categorias) + offsets[idx], 
                            height=df_agrupado[f'PA_QTDPRO' if tipo=='quantidade' else f'PA_VALPRO'],
                            width=largura, label=f'PRO ({tipo.capitalize()})', color=cores[f'PRO_{tipo}'])
        barras.append(barras_pro)
        idx += 1
        # APR
        barras_apr = ax.bar(x= np.arange(n_categorias) + offsets[idx], 
                            height=df_agrupado[f'PA_QTDAPR' if tipo=='quantidade' else f'PA_VALAPR'],
                            width=largura, label=f'APR ({tipo.capitalize()})', color=cores[f'APR_{tipo}'])
        barras.append(barras_apr)
        idx += 1
    
    # Adiciona valores acima das barras
    for grupo in barras:
        for barra in grupo:
            altura = barra.get_height()
            ax.text(barra.get_x() + barra.get_width()/2, altura + altura*0.01,
                    f'{altura:,.0f}', ha='center', va='bottom', fontsize=9)
    
    # Configurações do gráfico
    ax.set_xlabel('Categoria', fontsize=12)
    ax.set_ylabel('Soma', fontsize=12)
    ax.set_title(f'Comparação PRO vs APR por {agrupamento}', fontsize=14, weight='bold')
    ax.set_xticks(np.arange(n_categorias))
    ax.set_xticklabels(categorias, rotation=45, fontsize=11)
    ax.legend()
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    sns.despine()
    plt.tight_layout()
    plt.show()


                  # Move os arquivos gerados



def move_arquivo(arquivo, pasta_destino='dados_sia/dados_prontos'):
    import shutil
    from pathlib import Path

    os.makedirs(pasta_destino, exist_ok=True)

    caminho = Path(arquivo)

    destino_final = Path(pasta_destino) / caminho.name
    shutil.move(str(caminho), destino_final)
    print(f"{arquivo} movido para {pasta_destino} com sucesso.")

