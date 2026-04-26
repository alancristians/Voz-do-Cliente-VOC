import feedparser
import pandas as pd
from datetime import datetime
import os
import requests
from urllib.parse import quote
from bs4 import BeautifulSoup

def clean_html(html_text):
    if not html_text: return ""
    soup = BeautifulSoup(html_text, "html.parser")
    return soup.get_text()

def run_news_ingestion():
    print("📡 Iniciando coleta Voz do Mercado (Cenário 2026)...")
    
    # MAPEAMENTO SENIOR: Nome no Dash -> Termos de Busca no Google News
    # Adicionamos os novos bancos e refinamos as queries para maior volume
    queries = {
        "Itaú": "Itaú",
        "Bradesco": "Bradesco",
        "Nubank": "Nubank",
        "Santander": "Santander",
        "Banco do Brasil": "Banco do Brasil",
        "Caixa": "Caixa Econômica",
        "C6": "C6 Bank",
        "BTG Pactual": "BTG Pactual OR Banco Pan", # Busca conjunta conforme o ranking BCB
        "PicPay": "PicPay",
        "Inter": "Banco Inter",
        "Neon": "Banco Neon",
        "Mercado Pago": "Mercado Pago"
    }
    
    all_news = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    for bank_label, search_term in queries.items():
        # A query agora é dinâmica e foca em contexto bancário para evitar ruído
        rss_url = f"https://news.google.com/rss/search?q={quote(search_term)}+banco&hl=pt-BR&gl=BR&ceid=BR:pt-419"
        
        try:
            response = requests.get(rss_url, headers=headers, timeout=20)
            feed = feedparser.parse(response.content)
            
            for entry in feed.entries[:50]: # Limite de 10 notícias por banco (Bronze Layer)
                all_news.append({
                    'bank': bank_label, # Chave de ligação com as outras camadas
                    'title': entry.title,
                    'link': entry.link,
                    'published': entry.published,
                    'summary': clean_html(entry.summary if hasattr(entry, 'summary') else ""),
                    'source': entry.source.title if hasattr(entry, 'source') else 'Google News',
                    'extraction_at': datetime.now()
                })
        except Exception as e:
            print(f"⚠️ Erro ao processar notícias de {bank_label}: {e}")

    if all_news:
        df = pd.DataFrame(all_news)
        df['published_dt'] = pd.to_datetime(df['published'], errors='coerce')
        df = df.sort_values(by='published_dt', ascending=False)
        
        os.makedirs("data/bronze", exist_ok=True)
        # Salvamento em Parquet para performance na arquitetura Medalhão
        df.to_parquet("data/bronze/noticias_bancos.parquet", index=False)
        print(f"✅ Sucesso! {len(df)} notícias capturadas e salvas na Bronze.")
    return True

if __name__ == "__main__":
    run_news_ingestion()