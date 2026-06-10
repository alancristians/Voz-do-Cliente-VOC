import feedparser
import pandas as pd
from datetime import datetime
import os
import requests
from urllib.parse import quote
from bs4 import BeautifulSoup

def clean_html(html_text):
    """
    Remove tags HTML residuais do sumário da notícia utilizando o parser do BeautifulSoup.
    
    Args:
        html_text (str): Texto bruto contendo marcações HTML.
        
    Returns:
        str: Texto limpo e convertido para string pura.
    """
    if not html_text: 
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    return soup.get_text()

def run_news_ingestion():
    """
    Executa o pipeline de ingestão da camada Bronze para a Voz do Mercado.
    Consome o feed RSS do Google News de forma dinâmica por instituição,
    aplica a estratégia de append incremental e persiste os dados em formato Parquet.
    """
    print("📡 Iniciando coleta Voz do Mercado (Cenário 2026)...")
    
    # -------------------------------------------------------------------------
    # MAPEAMENTO DE ENTIDADES: Nome de Exibição -> Termo de Busca (Query)
    # Refinamento de busca utilizando operadores lógicos para maximizar a captura
    # -------------------------------------------------------------------------
    queries = {
        "Itaú": "Itaú",
        "Bradesco": "Bradesco",
        "Nubank": "Nubank",
        "Santander": "Santander",
        "Banco do Brasil": "Banco do Brasil",
        "Caixa": "Caixa Econômica",
        "C6": "C6 Bank",
        "BTG Pactual": "BTG Pactual OR Banco Pan",
        "PicPay": "PicPay",
        "Inter": "Banco Inter",
        "Neon": "Banco Neon",
        "Mercado Pago": "Mercado Pago"
    }
    
    all_news = []
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    # Iteração sobre a malha de instituições financeiras mapeadas
    for bank_label, search_term in queries.items():
        rss_url = f"https://news.google.com/rss/search?q={quote(search_term)}+banco&hl=pt-BR&gl=BR&ceid=BR:pt-419"
        
        try:
            response = requests.get(rss_url, headers=headers, timeout=20)
            feed = feedparser.parse(response.content)
            
            # Limitação de janela de corte (Top 40) por requisição diária para evitar throttling
            for entry in feed.entries[:40]: 
                all_news.append({
                    'bank': bank_label, 
                    'title': entry.title,
                    'link': entry.link,
                    'published': entry.published,
                    'summary': clean_html(entry.summary if hasattr(entry, 'summary') else ""),
                    'source': entry.source.title if hasattr(entry, 'source') else 'Google News',
                    'extraction_at': datetime.now()
                })
        except Exception as e:
            print(f"⚠️ Erro ao processar notícias de {bank_label}: {str(e)}")

    # -------------------------------------------------------------------------
    # CAMADA DE PERSISTÊNCIA: ESTRATÉGIA DE CARGA INCREMENTAL (UPSERT)
    # -------------------------------------------------------------------------
    if all_news:
        df_new = pd.DataFrame(all_news)
        df_new['published_dt'] = pd.to_datetime(df_new['published'], errors='coerce')
        
        target_path = "data/bronze/noticias_bancos.parquet"
        os.makedirs("data/bronze", exist_ok=True)
        
        # Consolidação histórica para evitar perda de dados (Data Loss) ao longo do mês
        if os.path.exists(target_path):
            print("📦 Histórico Bronze localizado. Iniciando concatenação incremental...")
            df_hist = pd.read_parquet(target_path)
            
            # Padronização de tipo do campo de data do histórico para o merge
            df_hist['published_dt'] = pd.to_datetime(df_hist['published'], errors='coerce')
            
            # Concatenação das partições em memória
            df_total = pd.concat([df_hist, df_new], ignore_index=True)
        else:
            print("🆕 Histórico não localizado. Inicializando nova tabela Bronze.")
            df_total = df_new
            
        # Governança de Chave Primária Lógica: Elimina duplicatas baseando-se no link único
        total_antes = len(df_total)
        df_total = df_total.drop_duplicates(subset=['link'], keep='first')
        total_depois = len(df_total)
        
        # Ordenação cronológica para otimização das próximas leituras (Silver Layer)
        df_total = df_total.sort_values(by='published_dt', ascending=False)
        
        # Escrita física otimizada em Parquet (Compressão Snappy nativa do Pandas)
        df_total.to_parquet(target_path, index=False)
        
        # Cálculo de métricas de engenharia para volumetria de logs
        novos_registros = total_depois - (total_antes - len(df_new))
        print(f"✅ Sucesso! Total em base: {total_depois} registros ({novos_registros} novas notícias adicionadas hoje).")
        
    return True

if __name__ == "__main__":
    run_news_ingestion()