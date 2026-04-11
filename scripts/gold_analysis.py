import pandas as pd
import os
import unicodedata

def remove_acentos(texto):
    if not isinstance(texto, str): return ""
    nfkd_form = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)]).lower().strip()

def run_gold_analysis():
    print("🥇 Gerando Camada Ouro: Inteligência Bancária 2026...")
    os.makedirs("data/gold", exist_ok=True)
    
    # Caminhos dos arquivos base
    bcb_path = "data/bronze/reclamacoes_bcb.parquet"
    cons_path = "data/bronze/reclamacoes_consumidor.parquet"
    news_path = "data/silver/stg_noticias.parquet"
    
    if all(os.path.exists(p) for p in [bcb_path, cons_path, news_path]):
        df_bcb = pd.read_parquet(bcb_path)
        df_cons = pd.read_parquet(cons_path)
        df_news = pd.read_parquet(news_path)
        
        # Mapeamento exato das colunas do seu Parquet Bronze
        c_inst = "Instituição financeira"
        c_idx = "Índice"
        c_cli = "Quantidade total de clientes – CCS e SCR"
        c_proc = "Quantidade de reclamações procedentes"
        c_resp = "Quantidade total de reclamações respondidas"

        # 1. Resumo de Notícias
        news_summary = df_news.groupby('bank').size().reset_index(name='qtd_noticias_recentes')
        
        synonyms = {
            "itau": "itau", "bradesco": "bradesco", "santander": "santander",
            "banco do brasil": "banco do brasil", "nubank": "nu ", "caixa": "caixa economica"
        }

        gold_data = []
        for _, row_news in news_summary.iterrows():
            bank_name = row_news['bank']
            term = synonyms.get(remove_acentos(bank_name), remove_acentos(bank_name))
            
            # Busca no BCB (Ranking)
            match_bcb = df_bcb[df_bcb[c_inst].str.contains(term, case=False, na=False)].iloc[0:1]
            
            # Busca no Consumidor (Top Status)
            match_cons = df_cons[df_cons["banco"].str.contains(term, case=False, na=False)]
            top_status = match_cons['status'].value_counts().idxmax() if not match_cons.empty else "Normal"

            if not match_bcb.empty:
                gold_data.append({
                    'bank': bank_name,
                    'qtd_noticias_recentes': row_news['qtd_noticias_recentes'],
                    'indice_bcb': match_bcb[c_idx].values[0],
                    'total_clientes': match_bcb[c_cli].values[0],
                    'recl_procedentes': match_bcb[c_proc].values[0],
                    'total_respondidas': match_bcb[c_resp].values[0],
                    'principal_motivo': top_status
                })

        df_gold = pd.DataFrame(gold_data)
        df_gold.to_csv("data/gold/fact_finvoc_summary.csv", index=False)
        print(f"✅ Sucesso! {len(df_gold)} instituições processadas.")
    else:
        print("❌ Erro: Arquivos bronze/silver não encontrados.")

if __name__ == "__main__":
    run_gold_analysis()