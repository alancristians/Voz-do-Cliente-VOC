import pandas as pd
import os
import unicodedata

def remove_acentos(texto):
    """Normaliza o texto: remove acentos, espaços extras e deixa em minúsculo."""
    if not isinstance(texto, str): return ""
    nfkd_form = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)]).lower().strip()

def run_gold_analysis():
    print("🥇 Gerando Camada Ouro: Cruzamento de Alta Precisão 2025...")
    
    # Garantir diretório de saída
    os.makedirs("data/gold", exist_ok=True)
    
    bcb_path = "data/silver/stg_bcb.parquet"
    news_path = "data/silver/stg_noticias.parquet"
    
    if os.path.exists(bcb_path) and os.path.exists(news_path):
        df_bcb = pd.read_parquet(bcb_path)
        df_news = pd.read_parquet(news_path)
        
        # 1. Identificar colunas dinamicamente (BCB 2025)
        col_map = {c: remove_acentos(c) for c in df_bcb.columns}
        try:
            col_nome = [orig for orig, norm in col_map.items() if 'instituicao' in norm][0]
            col_indice = [orig for orig, norm in col_map.items() if 'indice' in norm][0]
        except IndexError:
            print("❌ Erro: Colunas esperadas não encontradas no arquivo Silver.")
            return

        # 2. Resumo de Notícias por Banco
        news_summary = df_news.groupby('bank').size().reset_index(name='qtd_noticias_recentes')
        
        # 3. Mapeamento de Sinônimos para Match de 2025
        # O BCB usa nomes como "ITAU UNIBANCO (conglomerado)" ou "NU (conglomerado)"
        synonyms = {
            "itau": "itau",
            "bradesco": "bradesco",
            "santander": "santander",
            "banco do brasil": "banco do brasil",
            "nubank": "nu ",
            "caixa": "caixa economica"
        }

        results = []
        for _, row_news in news_summary.iterrows():
            bank_raw = row_news['bank']
            bank_clean = remove_acentos(bank_raw)
            search_term = synonyms.get(bank_clean, bank_clean)