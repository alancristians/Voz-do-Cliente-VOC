import feedparser
import pandas as pd
from datetime import datetime
import os
import requests
from urllib.parse import quote
from bs4 import BeautifulSoup

def clean_html(html_text):
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    return soup.get_text()

def run_news_ingestion():
    print("📡 Iniciando coleta de notícias (Voz do Mercado)...")
    
    queries = ["Itaú", "Bradesco", "Nubank", "Santander", "Banco do Brasil"]
    all_news = []

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    }

    for bank in queries:
        bank_encoded = quote(bank)
        print(f"🔍 Buscando: {bank}")
        
        rss_url = f"https://news.google.com/rss/search?q={bank_encoded}+banco&hl=pt-BR&gl=BR&ceid=BR:pt-419"
        
        try:
            response = requests.get(rss_url, headers=headers, timeout=20)
            feed = feedparser.parse(response.content)
            
            if not feed.entries:
                continue

            for entry in feed.entries[:15]: # Aumentei para 15 para ter mais volume
                raw_summary = entry.summary if hasattr(entry, 'summary') else ""
                clean_summary = clean_html(raw_summary)

                all_news.append({
                    'bank': bank,
                    'title': entry.title,
                    'link': entry.link,
                    'published': entry.published,
                    'summary': clean_summary,
                    'source': entry.source.title if hasattr(entry, 'source') else 'Google News',
                    'extraction_at': datetime.now()
                })
        except Exception as e:
            print(f"⚠️ Erro em {bank}: {e}")
            continue

    if not all_news:
        return True 

    df = pd.DataFrame(all_news)
    
    # --- TRATAMENTO DE DATA E ORDENAÇÃO ---
    # Converte a coluna 'published' para datetime real
    df['published_dt'] = pd.to_datetime(df['published'], errors='coerce')
    
    # Ordena: Mais recentes primeiro (descendente)
    df = df.sort_values(by='published_dt', ascending=False)
    
    # Remove a coluna auxiliar para manter o parquet limpo (opcional)
    # df = df.drop(columns=['published_dt'])

    os.makedirs("data/bronze", exist_ok=True)
    df.to_parquet("data/bronze/noticias_bancos.parquet", index=False)
    print(f"✅ Sucesso! {len(df)} notícias ordenadas por data.")
    return True

if __name__ == "__main__":
    run_news_ingestion()