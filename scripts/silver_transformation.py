import pandas as pd
import os
import glob
import google.generativeai as genai
from datetime import datetime

# Configuração da IA (Pegando do Secret do GitHub)
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel('gemini-1.5-flash')

def get_sentiment(text):
    """Envia o título da notícia para o Gemini e retorna Positivo, Negativo ou Neutro."""
    if not text or pd.isna(text): 
        return "Neutro"
    
    # Prompt curto e direto para evitar que a IA 'alucine' ou escreva textos longos
    prompt = f"Analise o sentimento desta notícia bancária e responda apenas com UMA palavra (Positivo, Negativo ou Neutro): {text}"
    
    try:
        response = model.generate_content(prompt)
        sentiment = response.text.strip().capitalize()
        
        # Garante que a resposta seja uma das nossas categorias
        if "Positivo" in sentiment: return "Positivo"
        if "Negativo" in sentiment: return "Negativo"
        return "Neutro"
    except Exception as e:
        print(f"⚠️ Erro na API Gemini: {e}")
        return "Neutro"

def clean_news(df):
    """Limpa e padroniza os dados do Google News"""
    if df is None or df.empty: return None
    # Remove duplicatas de títulos ANTES da IA para economizar tokens
    df = df.drop_duplicates(subset=['title'])
    # Padroniza nomes de colunas para snake_case
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    return df

def run_silver_transformation():
    print("💎 Iniciando Transformação Camada Prata com IA...")
    os.makedirs("data/silver", exist_ok=True)
    
    # 1. Processando Notícias (Vindo do Bronze)
    news_path = "data/bronze/noticias_bancos.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        df_news_clean = clean_news(df_news)
        
        if df_news_clean is not None and not df_news_clean.empty:
            print(f"🤖 IA analisando sentimento de {len(df_news_clean)} notícias...")
            # Aplica a análise de sentimento no título
            df_news_clean['sentimento'] = df_news_clean['title'].apply(get_sentiment)
            
            df_news_clean.to_parquet("data/silver/stg_noticias.parquet", index=False)
            print(f"✅ Notícias processadas e analisadas.")

    # 2. Processando BCB de forma dinâmica
    arquivos_bcb = glob.glob("data/bronze/reclamacoes_bcb*.parquet")
    
    if arquivos_bcb:
        bcb_path = sorted(arquivos_bcb)[-1]
        print(f"📂 Aplicando limpeza no arquivo: {bcb_path}")
        
        df_bcb = pd.read_parquet(bcb_path)
        
        # Padroniza colunas (remove acentos e pontos)
        df_bcb.columns = [
            c.lower().replace(' ', '_').replace('.', '')
            .replace('í', 'i').replace('ã', 'a').replace('ç', 'c') 
            for c in df_bcb.columns
        ]
        
        df_bcb.to_parquet("data/silver/stg_bcb.parquet", index=False)
        print(f"✅ Dados BCB processados e salvos na Camada Prata.")
    else:
        print("⚠️ Nenhum arquivo BCB encontrado no Bronze.")

    print("🚀 Camada Prata atualizada com sucesso!")

if __name__ == "__main__":
    run_silver_transformation()