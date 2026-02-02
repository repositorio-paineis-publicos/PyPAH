import os
import math
from ftplib import FTP
from dbfread import DBF
import zipfile
from pathlib import Path

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
            
            ## Adiciona coluna de datas

            df['data_ref'] = pd.to_datetime(
                '20' + df['arquivo_origem'].str[-4:-2] + '-' + df['arquivo_origem'].str[-2:],
                format='%Y-%m'
            )
            df['Ano'] = df['data_ref'].dt.year
            df['Mes'] = df['data_ref'].dt.month_name()

            # Adiciona coluna de UF

            df['UF'] = df['arquivo_origem'].str[2:4]

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



def move_arquivo(arquivo, pasta_destino='dados_sia/dados_prontos'):
    import shutil
    from pathlib import Path

    os.makedirs(pasta_destino, exist_ok=True)

    caminho = Path(arquivo)

    destino_final = Path(pasta_destino) / caminho.name
    shutil.move(str(caminho), destino_final)
    print(f"{arquivo} movido para {pasta_destino} com sucesso.")



def download_estab_label(
    nome_saida="dim_estabelecimento.parquet",
    destino=Path(r"C:\Projetos\PyPAH\dados_sia\rotulos")
):
    destino.mkdir(parents=True, exist_ok=True)

    zip_path = destino / "TAB_CNES.zip"
    extract_path = destino / "TAB_CNES"
    dbf_path = extract_path / "DBF" / "CADGERBR.dbf"
    output_path = destino / nome_saida

    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd("/dissemin/publicos/CNES/200508_/Auxiliar")

    with open(zip_path, "wb") as f:
        ftp.retrbinary("RETR TAB_CNES.zip", f.write)

    ftp.quit()

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_path)

    df_labels = pd.DataFrame(DBF(dbf_path, encoding="latin-1"))

    df_dim_estab = (
        df_labels
        .rename(columns={"CNES": "PA_CODUNI"})
        [["PA_CODUNI", "FANTASIA"]]
        .drop_duplicates()
        .assign(PA_CODUNI=lambda x: x["PA_CODUNI"].astype(str))
    )

    df_dim_estab["label_estabelecimento"] = df_dim_estab["PA_CODUNI"] + " - " + df_dim_estab["FANTASIA"]

    df_dim_estab.to_parquet(output_path, index=False)

    return output_path



def download_proc_label(
    nome_saida="dim_procedimentos.parquet",
    destino=Path(r"C:\Projetos\PyPAH\dados_sia\rotulos")
):
    destino.mkdir(parents=True, exist_ok=True)

    zip_path = destino / "TAB_SIA.zip"
    extract_path = destino / "TAB_SIA"
    dbf_path = extract_path / "DBF" / "TB_SIGTAW.dbf"
    output_path = destino / nome_saida

    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd("/dissemin/publicos/SIASUS/200801_/Auxiliar")

    with open(zip_path, "wb") as f:
        ftp.retrbinary("RETR TAB_SIA.zip", f.write)

    ftp.quit()

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_path)

    df_labels = pd.DataFrame(DBF(dbf_path, encoding="latin-1"))

    df_dim_estab = (
        df_labels
        .rename(columns={"IP_COD": "PA_PROC_ID"})
        [["PA_PROC_ID", "IP_DSCR"]]
        .drop_duplicates()
        .assign(PA_PROC_ID=lambda x: x["PA_PROC_ID"].astype(str))
    )

    df_dim_estab["label_procedimento"] = df_dim_estab["PA_PROC_ID"] + " - " + df_dim_estab["IP_DSCR"]

    df_dim_estab.to_parquet(output_path, index=False)

    return output_path



