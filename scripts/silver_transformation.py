import pandas as pd
import os
from datetime import datetime

def clean_news(df):
    """Limpa e padroniza os dados do Google News"""
    if df is None or df.empty: return None
    # Remove duplicatas de títulos
    df = df.drop_duplicates(subset=['title'])
    # Padroniza nomes de colunas
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    return df

def run_silver_transformation():
    print("💎 Iniciando Transformação Camada Prata...")
    os.makedirs("data/silver", exist_ok=True)
    
    # 1. Processando Notícias
    news_path = "data/bronze/noticias_bancos.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        df_news_clean = clean_news(df_news)
        df_news_clean.to_parquet("data/silver/stg_noticias.parquet", index=False)
        print(f"✅ Notícias processadas: {len(df_news_clean)} linhas.")

    # 2. Processando BCB (Exemplo de limpeza básica)
    bcb_path = "data/bronze/reclamacoes_bcb.parquet"
    if os.path.exists(bcb_path):
        df_bcb = pd.read_parquet(bcb_path)
        # Padroniza colunas para snake_case
        df_bcb.columns = [c.lower().replace(' ', '_').replace('.', '') for c in df_bcb.columns]
        df_bcb.to_parquet("data/silver/stg_bcb.parquet", index=False)
        print(f"✅ Dados BCB processados.")

    print("🚀 Camada Prata atualizada com sucesso!")

if __name__ == "__main__":
    run_silver_transformation()