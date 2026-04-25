import pandas as pd
import os
import unicodedata

def normalizar(texto):
    if not isinstance(texto, str): return ""
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower().strip()

def limpar_num(v):
    try: return float(str(v).replace('.', '').replace(',', '.'))
    except: return 0.0

def executar_gold():
    """
    Consolida métricas de ranking, volume de notícias e o principal motivo de reclamação (BCB).
    """
    os.makedirs("data/gold", exist_ok=True)
    
    # Verificação de dependências
    p_rank = "data/silver/stg_bcb_ranking.parquet"
    p_assu = "data/silver/stg_bcb_assuntos.parquet"
    p_news = "data/silver/stg_noticias.parquet"
    
    if not all(os.path.exists(p) for p in [p_rank, p_assu, p_news]): return

    df_rank = pd.read_parquet(p_rank)
    df_assu = pd.read_parquet(p_assu)
    df_news = pd.read_parquet(p_news)
    df_cons = pd.read_parquet("data/bronze/reclamacoes_consumidor.parquet")

    # Mapeamento dinâmico de colunas do ranking 2026
    c_inst = next(c for c in df_rank.columns if 'instituicao' in c)
    c_idx = next(c for c in df_rank.columns if 'indice' in c)
    c_cli = next(c for c in df_rank.columns if 'clientes' in c)
    c_proc = next(c for c in df_rank.columns if 'procedentes' in c)
    c_resp = next(c for c in df_rank.columns if 'respondidas' in c)
    label_periodo = f"{df_rank['trimestre'].iloc[0]} Trimestre / {df_rank['ano'].iloc[0]}"

    # Identifica o Top 1 Assunto por Instituição no dataset do BCB
    # Colunas comuns: 'instituicao_financeira' e 'assunto'
    df_top_assunto = df_assu.sort_values('quantidade', ascending=False).drop_duplicates('instituicao_financeira')

    synonyms = {
        "itau": "itau", "bradesco": "bradesco", "santander": "santander",
        "banco do brasil": "banco do brasil", "nubank": "nu ", "caixa": "caixa economica",
        "c6": "c6", "btg": "btg pactual", "picpay": "picpay", "inter": "inter"
    }

    gold_data = []
    # Agrupamento por banco nas notícias
    news_counts = df_news.groupby('bank').size().to_dict()

    for bank_alias, search_term in synonyms.items():
        # Cruzamento Ranking
        m_rank = df_rank[df_rank[c_inst].str.contains(search_term, case=False, na=False)].iloc[0:1]
        # Cruzamento Assunto BCB
        m_assu = df_top_assunto[df_top_assunto['instituicao_financeira'].str.contains(search_term, case=False, na=False)]
        # Cruzamento Consumidor.gov
        m_cons = df_cons[df_cons["banco"].str.contains(search_term, case=False, na=False)]
        
        if not m_rank.empty:
            # Prioriza o motivo do BCB, senão usa o do Consumidor.gov
            motivo = m_assu['assunto'].values[0] if not m_assu.empty else (
                m_cons['status'].value_counts().idxmax() if not m_cons.empty else "Divergência Cadastral"
            )

            gold_data.append({
                'bank': bank_alias.capitalize(),
                'qtd_noticias_recentes': news_counts.get(bank_alias, 0),
                'indice_bcb': limpar_num(m_rank[c_idx].values[0]),
                'total_clientes': limpar_num(m_rank[c_cli].values[0]),
                'recl_procedentes': limpar_num(m_rank[c_proc].values[0]),
                'total_respondidas': limpar_num(m_rank[c_resp].values[0]),
                'principal_motivo': motivo,
                'periodo': label_periodo
            })

    pd.DataFrame(gold_data).to_csv("data/gold/fact_finvoc_summary.csv", index=False)

if __name__ == "__main__":
    executar_gold()