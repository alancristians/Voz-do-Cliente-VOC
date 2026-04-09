import feedparser
import pandas as pd
from datetime import datetime
import os
import sys
import requests
from urllib.parse import quote

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
        
        # URL simplificada (sem o 'quando:7d' que estava travando)
        rss_url = f"https://news.google.com/rss/search?q={bank_encoded}+banco&hl=pt-BR&gl=BR&ceid=BR:pt-419"
        
        try:
            response = requests.get(rss_url, headers=headers, timeout=20)
            feed = feedparser.parse(response.content)
            
            if not feed.entries:
                print(f"⚠️ Sem notícias recentes para {bank}.")
                continue

            for entry in feed.entries[:10]:
                all_news.append({
                    'bank': bank,
                    'title': entry.title,
                    'link': entry.link,
                    'published': entry.published,
                    'source': entry.source.title if hasattr(entry, 'source') else 'Google News',
                    'extraction_at': datetime.now()
                })
        except Exception as e:
            print(f"⚠️ Erro em {bank}: {e}")
            continue

    if not all_news:
        print("❌ Nenhuma notícia capturada. Mantendo pipeline vivo.")
        return True # Retorna True para não quebrar o GitHub Actions

    df = pd.DataFrame(all_news)
    os.makedirs("data/bronze", exist_ok=True)
    df.to_parquet("data/bronze/noticias_bancos.parquet", index=False)
    print(f"✅ Sucesso! {len(df)} notícias salvas.")
    return True

if __name__ == "__main__":
    run_news_ingestion()