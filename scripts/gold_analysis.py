import pandas as pd
import os
import unicodedata

def normalizar_chave(texto):
    """Remove acentos e padroniza para busca, mas retorna a chave 'bonita'."""
    if not isinstance(texto, str): return ""
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower().strip()

def executar_gold():
    os.makedirs("data/gold", exist_ok=True)
    p_rank = "data/silver/stg_bcb_ranking.parquet"
    p_news = "data/silver/stg_noticias.parquet"
    
    if not os.path.exists(p_rank) or not os.path.exists(p_news): return

    df_rank = pd.read_parquet(p_rank)
    df_news = pd.read_parquet(p_news)
    
    # Normalizamos as notícias para bater com as chaves de busca
    df_news['bank_key'] = df_news['bank'].apply(normalizar_chave)
    news_counts = df_news.groupby('bank_key').size().to_dict()

    # Mapeamento: Chave de Busca -> Nome Oficial de Exibição
    # Isso elimina a necessidade de duplicatas no app.py
    bancos_alvo = {
        "itau": "Itaú", 
        "bradesco": "Bradesco", 
        "santander": "Santander",
        "banco do brasil": "Banco do Brasil", 
        "nubank": "Nubank", 
        "caixa": "Caixa",
        "c6": "C6", 
        "btg": "BTG Pactual", 
        "picpay": "PicPay", 
        "inter": "Inter"
    }

    gold_data = []
    c_inst = next(c for c in df_rank.columns if 'instituicao' in c)

    for key, nome_exibicao in bancos_alvo.items():
        # Busca flexível no ranking do BCB
        termo_busca = "nu " if key == "nubank" else key
        m_rank = df_rank[df_rank[c_inst].str.contains(termo_busca, case=False, na=False)].iloc[0:1]
        
        if not m_rank.empty:
            gold_data.append({
                'bank': nome_exibicao, # Aqui garantimos o nome único
                'qtd_noticias_recentes': news_counts.get(key, 0),
                'indice_bcb': m_rank.filter(like='indice').iloc[0,0],
                'total_clientes': m_rank.filter(like='clientes').iloc[0,0],
                'recl_procedentes': m_rank.filter(like='procedentes').iloc[0,0],
                'total_respondidas': m_rank.filter(like='respondidas').iloc[0,0],
                'principal_motivo': "Divergência Cadastral",
                'periodo': f"{df_rank['trimestre'].iloc[0]}T/{df_rank['ano'].iloc[0]}"
            })

    pd.DataFrame(gold_data).to_csv("data/gold/fact_finvoc_summary.csv", index=False)

if __name__ == "__main__":
    executar_gold()