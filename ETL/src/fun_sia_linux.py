import os
import shutil
from pathlib import Path


def conv_dbc_para_pqt(pasta_origem ='../dados_sia/dados_dbc', pasta_destino='dados_sia/dados_parquet'):
    """
    Converte arquivos .dbc para .parquet via PySUS 1.0.0
    e move os arquivos resultantes para uma pasta de destino.
    """

    from pysus.data.local import ParquetSet

    # Cria pastas se não existirem
    os.makedirs(pasta_origem, exist_ok=True)
    os.makedirs(pasta_destino, exist_ok=True)

    arquivos_dbc = [os.path.join(pasta_origem, arq) for arq in os.listdir(pasta_origem) if arq.endswith('.dbc')]
    if not arquivos_dbc:
        print("Nenhum arquivo .dbc encontrado.")
        return

    for arq in arquivos_dbc:
        try:
            print(f"Convertendo: {arq}")

            # Cria o ParquetSet → faz toda a conversão automática
            parquet_set = ParquetSet(arq)

            # Caminho do resultado (.parquet)
            caminho_parquet = Path(parquet_set.path)

            # Define o destino final
            nome_base = Path(arq).stem
            destino_final = Path(pasta_destino) / f"{nome_base}.parquet"

            # Move os arquivos gerados
            if caminho_parquet.is_dir():
                # PySUS salva múltiplos .parquet dentro de uma pasta
                destino_pasta = Path(pasta_destino) / nome_base
                os.makedirs(destino_pasta, exist_ok=True)
                for arquivo in caminho_parquet.glob("*.parquet"):
                    shutil.move(str(arquivo), destino_pasta / arquivo.name)
                try:
                    caminho_parquet.rmdir()
                except OSError:
                    pass
                print(f"{nome_base} convertido (vários arquivos .parquet)")
            else:
                shutil.move(str(caminho_parquet), destino_final)
                print(f"{nome_base} convertido (1 arquivo .parquet)")

        except Exception as e:
            print(f"Erro ao processar {arq}: {e}")


