import pandas as pd
import os
import unicodedata

def normalizar_chave(texto):
    if not isinstance(texto, str): return ""
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower().strip()

def limpar_valor_bcb(v):
    if pd.isna(v) or v == "" or v == " ": return 0.0
    try:
        v_str = str(v).strip()
        if "," in v_str:
            v_str = v_str.replace('.', '').replace(',', '.')
        return float(v_str)
    except:
        return 0.0

def executar_gold():
    os.makedirs("data/gold", exist_ok=True)
    p_rank = "data/silver/stg_bcb_ranking.parquet"
    p_news = "data/silver/stg_noticias.parquet"
    
    if not os.path.exists(p_rank) or not os.path.exists(p_news): return

    df_rank = pd.read_parquet(p_rank)
    df_news = pd.read_parquet(p_news)
    df_news['bank_clean'] = df_news['bank'].apply(normalizar_chave)

    bancos_alvo = {
        "itau": "Itaú", "bradesco": "Bradesco", "santander": "Santander",
        "nubank": "Nubank", "caixa": "Caixa", "c6": "C6", 
        "btg": "BTG Pactual", "picpay": "PicPay", "inter": "Inter"
    }

    gold_data = []
    # Mapeamento dinâmico das colunas reais do arquivo de 2026
    c_inst = next(c for c in df_rank.columns if 'instituicao' in c.lower())
    c_idx = next(c for c in df_rank.columns if 'indice' in c.lower())
    c_cli = next(c for c in df_rank.columns if 'clientes' in c.lower())
    c_proc = next(c for c in df_rank.columns if 'procedentes' in c.lower())
    # LOCALIZAÇÃO DA COLUNA REAL DE RESPONDIDAS
    c_resp = next((c for c in df_rank.columns if 'respondidas' in c.lower()), None)
    
    for key, nome_exibicao in bancos_alvo.items():
        termo_busca = "nu " if key == "nubank" else key
        m_rank = df_rank[df_rank[c_inst].str.contains(termo_busca, case=False, na=False)].iloc[0:1]
        
        if not m_rank.empty:
            filtro_news = df_news[df_news['bank_clean'].str.contains(key, na=False)]
            
            # Se a coluna de respondidas existir, usa o dado real. Se não, fallback 1.3x.
            val_proc = limpar_valor_bcb(m_rank[c_proc].values[0])
            val_resp = limpar_valor_bcb(m_rank[c_resp].values[0]) if c_resp else (val_proc * 1.3)

            gold_data.append({
                'bank': nome_exibicao,
                'qtd_noticias_recentes': len(filtro_news),
                'indice_bcb': limpar_valor_bcb(m_rank[c_idx].values[0]),
                'total_clientes': limpar_valor_bcb(m_rank[c_cli].values[0]),
                'recl_procedentes': val_proc,
                'total_respondidas': val_resp,
                'periodo': f"{df_rank['trimestre'].iloc[0]}T/{df_rank['ano'].iloc[0]}"
            })

    pd.DataFrame(gold_data).to_csv("data/gold/fact_finvoc_summary.csv", index=False, decimal='.')

if __name__ == "__main__":
    executar_gold()