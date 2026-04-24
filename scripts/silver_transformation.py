import pandas as pd
import os
import glob
import time # Importante para o delay
from google import genai # Mudança aqui!
from datetime import datetime

# Configuração da Nova API (2026 Standard)
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def get_sentiment(text):
    if not text or pd.isna(text): 
        return "Neutro"
    
    # Prompt otimizado para ser curto (economiza tokens)
    prompt = f"Sentimento (Positivo, Negativo, Neutro): {text}"
    
    # Tentativas de retry (Backoff simples)
    for attempt in range(3): 
        try:
            response = client.models.generate_content(
                model='gemini-2.0-flash',
                contents=prompt
            )
            
            if response and response.text:
                sentiment = response.text.strip().capitalize()
                # Pausa de 4 segundos entre requisições para respeitar o RPM do Free Tier
                time.sleep(4) 
                
                if "Positivo" in sentiment: return "Positivo"
                if "Negativo" in sentiment: return "Negativo"
            return "Neutro"
            
        except Exception as e:
            if "429" in str(e):
                print(f"⏳ Quota atingida. Aguardando 10s para re-tentativa {attempt+1}/3...")
                time.sleep(10) # Se der erro de quota, espera mais
            else:
                print(f"⚠️ Erro na API: {e}")
                return "Neutro"
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