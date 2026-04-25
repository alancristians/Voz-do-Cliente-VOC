import pandas as pd
import os
import glob

def limpar_noticias(df):
    """
    Remove duplicatas e normaliza colunas do dataset de notícias.
    """
    if df is None or df.empty: return None
    df = df.drop_duplicates(subset=['title'])
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    if 'banco' in df.columns and 'bank' not in df.columns:
        df['bank'] = df['banco']
    return df

def executar_silver():
    """
    Executa a camada silver: limpeza de notícias e normalização do BCB.
    """
    os.makedirs("data/silver", exist_ok=True)
    
    # Processamento News
    news_path = "data/bronze/noticias_bancos.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        df_clean = limpar_noticias(df_news)
        if df_clean is not None:
            df_clean.to_parquet("data/silver/stg_noticias.parquet", index=False)

    # Processamento BCB
    arquivos_bcb = glob.glob("data/bronze/reclamacoes_bcb*.parquet")
    if arquivos_bcb:
        bcb_path = sorted(arquivos_bcb)[-1]
        df_bcb = pd.read_parquet(bcb_path)
        df_bcb.columns = [
            c.lower().replace(' ', '_').replace('.', '').replace('í', 'i')
            .replace('ã', 'a').replace('ç', 'c') for c in df_bcb.columns
        ]
        df_bcb.to_parquet("data/silver/stg_bcb.parquet", index=False)

if __name__ == "__main__":
    executar_silver()