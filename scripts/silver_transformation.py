import pandas as pd
import os
import glob
from datetime import datetime

def clean_news(df):
    """Limpa e padroniza os dados do Google News"""
    if df is None or df.empty: return None
    # Remove duplicatas de títulos para não sujar o dashboard
    df = df.drop_duplicates(subset=['title'])
    # Padroniza nomes de colunas para snake_case
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    return df

def run_silver_transformation():
    print("💎 Iniciando Transformação Camada Prata...")
    os.makedirs("data/silver", exist_ok=True)
    
    # 1. Processando Notícias (Vindo do Bronze)
    news_path = "data/bronze/noticias_bancos.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        df_news_clean = clean_news(df_news)
        df_news_clean.to_parquet("data/silver/stg_noticias.parquet", index=False)
        print(f"✅ Notícias processadas: {len(df_news_clean)} linhas.")

    # 2. Processando BCB de forma dinâmica
    # 🔍 Mesma lógica da Gold: busca o arquivo mais recente no Bronze
    arquivos_bcb = glob.glob("data/bronze/reclamacoes_bcb*.parquet")
    
    if arquivos_bcb:
        # Pega o arquivo do trimestre mais recente
        bcb_path = sorted(arquivos_bcb)[-1]
        print(f"📂 Aplicando limpeza no arquivo: {bcb_path}")
        
        df_bcb = pd.read_parquet(bcb_path)
        
        # Padroniza colunas (remove acentos, espaços e pontos)
        df_bcb.columns = [c.lower().replace(' ', '_').replace('.', '').replace('í', 'i').replace('ã', 'a') for c in df_bcb.columns]
        
        # Salva na Silver como um arquivo de staging estável
        df_bcb.to_parquet("data/silver/stg_bcb.parquet", index=False)
        print(f"✅ Dados BCB processados e salvos na Camada Prata.")
    else:
        print("⚠️ Nenhum arquivo BCB encontrado no Bronze para transformação.")

    print("🚀 Camada Prata atualizada com sucesso!")

if __name__ == "__main__":
    run_silver_transformation()