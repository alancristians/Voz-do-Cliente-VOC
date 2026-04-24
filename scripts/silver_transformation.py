import pandas as pd
import os
import glob
import time
from google import genai
from datetime import datetime

# Configuração da Nova API (2026 Standard)
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def get_sentiment(text):
    if not text or pd.isna(text): 
        return "Neutro"
    
    prompt = f"Responda apenas Positivo, Negativo ou Neutro para esta notícia: {text}"
    
    for attempt in range(3): 
        try:
            response = client.models.generate_content(
                model='gemini-2.0-flash',
                contents=prompt
            )
            
            if response and response.text:
                sentiment = response.text.strip().capitalize()
                # 10 segundos de folga para o Google não nos bloquear (Free Tier Safety)
                time.sleep(10) 
                
                if "Positivo" in sentiment: return "Positivo"
                if "Negativo" in sentiment: return "Negativo"
                return "Neutro"
            
        except Exception as e:
            if "429" in str(e):
                wait_time = (attempt + 1) * 20
                print(f"⏳ Quota atingida. Banho de gelo de {wait_time}s...")
                time.sleep(wait_time)
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
    print("💎 Silver 2.4: Processamento Seletivo ativado...")
    os.makedirs("data/silver", exist_ok=True)
    
    news_path = "data/bronze/noticias_bancos.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        df_news_clean = clean_news(df_news)
        
        if df_news_clean is not None and not df_news_clean.empty:
            # 1. Criamos a coluna com valor padrão (Garante que todas as notícias apareçam no Dash)
            df_news_clean['sentimento'] = 'Neutro (Não analisado)'
            
            # 2. Limitamos a análise de IA apenas para as TOP 15 notícias (Mais recentes)
            # Isso evita o erro 429 e mantém o processo em menos de 5 minutos
            print(f"🤖 Analisando as 15 notícias mais recentes de um total de {len(df_news_clean)}...")
            
            indices_para_analisar = df_news_clean.index[:15]
            
            for i, idx in enumerate(indices_para_analisar):
                title = df_news_clean.at[idx, 'title']
                df_news_clean.at[idx, 'sentimento'] = get_sentiment(title)
                print(f"✅ Notícia {i+1}/15 analisada.")

            df_news_clean.to_parquet("data/silver/stg_noticias.parquet", index=False)
            print(f"🚀 Camada Prata: Notícias salvas com sucesso.")

    # Processamento BCB (Mantido igual)
    arquivos_bcb = glob.glob("data/bronze/reclamacoes_bcb*.parquet")
    if arquivos_bcb:
        bcb_path = sorted(arquivos_bcb)[-1]
        df_bcb = pd.read_parquet(bcb_path)
        df_bcb.columns = [c.lower().replace(' ', '_').replace('.', '').replace('í', 'i').replace('ã', 'a').replace('ç', 'c') for c in df_bcb.columns]
        df_bcb.to_parquet("data/silver/stg_bcb.parquet", index=False)
        print(f"✅ Dados BCB processados.")

if __name__ == "__main__":
    run_silver_transformation()