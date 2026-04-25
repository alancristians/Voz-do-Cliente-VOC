import pandas as pd
import os
import unicodedata

def normalizar_chave(texto):
    """Padroniza strings para busca robusta."""
    if not isinstance(texto, str): return ""
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower().strip()

def executar_gold():
    """
    Consolida métricas de 2026 com busca flexível de notícias por banco.
    """
    os.makedirs("data/gold", exist_ok=True)
    p_rank = "data/silver/stg_bcb_ranking.parquet"
    p_news = "data/silver/stg_noticias.parquet"
    
    if not os.path.exists(p_rank) or not os.path.exists(p_news):
        print("⚠️ Aguardando arquivos da camada Silver...")
        return

    df_rank = pd.read_parquet(p_rank)
    df_news = pd.read_parquet(p_news)
    
    # Normalizamos a coluna de banco nas notícias para a busca
    df_news['bank_clean'] = df_news['bank'].apply(normalizar_chave)

    # Mapeamento: Chave de Busca -> Nome Oficial
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
            # NOVIDADE: Busca por sub-string nas notícias (Mais resiliente)
            filtro_news = df_news[df_news['bank_clean'].str.contains(key, na=False)]
            qtd_news = len(filtro_news)

            gold_data.append({
                'bank': nome_exibicao,
                'qtd_noticias_recentes': qtd_news,
                'indice_bcb': m_rank.filter(like='indice').iloc[0,0],
                'total_clientes': m_rank.filter(like='clientes').iloc[0,0],
                'recl_procedentes': m_rank.filter(like='procedentes').iloc[0,0],
                'total_respondidas': m_rank.filter(like='respondidas').iloc[0,0],
                'principal_motivo': "Irregularidade Operacional",
                'periodo': f"{df_rank['trimestre'].iloc[0]}T/{df_rank['ano'].iloc[0]}"
            })

    pd.DataFrame(gold_data).to_csv("data/gold/fact_finvoc_summary.csv", index=False)
    print("✅ Camada Gold gerada com sucesso.")

if __name__ == "__main__":
    executar_gold()