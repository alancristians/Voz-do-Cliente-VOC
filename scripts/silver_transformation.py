import pandas as pd
import os
import glob
from google import genai # Mudança aqui!
from datetime import datetime

# Configuração da Nova API (2026 Standard)
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def get_sentiment(text):
    """Nova forma de chamar o Gemini 1.5 Flash"""
    if not text or pd.isna(text): 
        return "Neutro"
    
    prompt = f"Analise o sentimento desta notícia bancária e responda apenas com UMA palavra (Positivo, Negativo ou Neutro): {text}"
    
    try:
        # Syntax nova: client.models.generate_content
        response = client.models.generate_content(
            model='gemini-1.5-flash',
            contents=prompt
        )
        sentiment = response.text.strip().capitalize()
        
        if "Positivo" in sentiment: return "Positivo"
        if "Negativo" in sentiment: return "Negativo"
        return "Neutro"
    except Exception as e:
        print(f"⚠️ Erro na API Gemini: {e}")
        return "Neutro"

def clean_news(df):
    if df is None or df.empty: return None
    df = df.drop_duplicates(subset=['title'])
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    return df

def run_silver_transformation():
    print("💎 Silver 2.1: Migrando para a nova SDK do Gemini...")
    os.makedirs("data/silver", exist_ok=True)
    
    news_path = "data/bronze/noticias_bancos.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        df_news_clean = clean_news(df_news)
        
        if df_news_clean is not None and not df_news_clean.empty:
            print(f"🤖 IA analisando sentimentos com a nova SDK...")
            df_news_clean['sentimento'] = df_news_clean['title'].apply(get_sentiment)
            df_news_clean.to_parquet("data/silver/stg_noticias.parquet", index=False)
            print(f"✅ Notícias processadas.")

    # Processamento BCB (Mantido igual, pois já estava bom)
    arquivos_bcb = glob.glob("data/bronze/reclamacoes_bcb*.parquet")
    if arquivos_bcb:
        bcb_path = sorted(arquivos_bcb)[-1]
        df_bcb = pd.read_parquet(bcb_path)
        df_bcb.columns = [c.lower().replace(' ', '_').replace('.', '').replace('í', 'i').replace('ã', 'a').replace('ç', 'c') for c in df_bcb.columns]
        df_bcb.to_parquet("data/silver/stg_bcb.parquet", index=False)
        print(f"✅ Dados BCB processados.")

    print("🚀 Camada Prata atualizada com sucesso!")

if __name__ == "__main__":
    run_silver_transformation()