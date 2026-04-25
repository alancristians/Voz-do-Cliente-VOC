import pandas as pd
import os
import glob

def normalizar_colunas(df):
    """
    Padroniza nomes de colunas: snake_case e remoção de caracteres especiais.
    """
    df.columns = [
        c.lower().replace(' ', '_').replace('.', '').replace('í', 'i')
        .replace('ã', 'a').replace('ç', 'c') for c in df.columns
    ]
    return df

def executar_silver():
    """
    Processa arquivos de notícias, ranking geral BCB e ranking de assuntos.
    """
    os.makedirs("data/silver", exist_ok=True)
    
    # 1. Notícias (Normalização básica)
    path_news = "data/bronze/noticias_bancos.parquet"
    if os.path.exists(path_news):
        df_n = pd.read_parquet(path_news).drop_duplicates(subset=['title'])
        df_n.columns = [c.lower().replace(' ', '_') for c in df_n.columns]
        if 'banco' in df_n.columns: df_n['bank'] = df_n['banco']
        df_n.to_parquet("data/silver/stg_noticias.parquet", index=False)

    # 2. Ranking Geral BCB (Métricas)
    arquivos_rank = glob.glob("data/bronze/reclamacoes_bcb_*.parquet")
    if arquivos_rank:
        df_r = pd.read_parquet(sorted(arquivos_rank)[-1])
        normalizar_colunas(df_r).to_parquet("data/silver/stg_bcb_ranking.parquet", index=False)

    # 3. Assuntos BCB (Qualitativo - NOVO)
    arquivos_assuntos = glob.glob("data/bronze/assuntos_bcb_*.parquet")
    if arquivos_assuntos:
        df_a = pd.read_parquet(sorted(arquivos_assuntos)[-1])
        normalizar_colunas(df_a).to_parquet("data/silver/stg_bcb_assuntos.parquet", index=False)

if __name__ == "__main__":
    executar_silver()