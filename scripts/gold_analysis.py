import pandas as pd
import os
import unicodedata

def normalizar(texto):
    if not isinstance(texto, str): return ""
    nfkd = unicodedata.normalize('NFKD', texto)
    texto_limpo = "".join([c for c in nfkd if not unicodedata.combining(c)])
    # Normaliza traços e espaços para garantir o match
    return texto_limpo.lower().replace('–', '-').replace('—', '-').strip()

def limpar_numero(valor):
    if pd.isna(valor) or valor == "" or valor == " ": return 0.0
    if isinstance(valor, str):
        valor = valor.replace('.', '').replace(',', '.')
    try:
        return float(valor)
    except:
        return 0.0

def run_gold_analysis():
    print("🥇 Gerando Camada Ouro: Inteligência Bancária 2026...")
    os.makedirs("data/gold", exist_ok=True)
    
    bcb_path = "data/bronze/reclamacoes_bcb.parquet"
    cons_path = "data/bronze/reclamacoes_consumidor.parquet"
    news_path = "data/silver/stg_noticias.parquet"
    
    if all(os.path.exists(p) for p in [bcb_path, cons_path, news_path]):
        df_bcb = pd.read_parquet(bcb_path)
        df_cons = pd.read_parquet(cons_path)
        df_news = pd.read_parquet(news_path)
        
        # Mapeamento Inteligente
        cols_orig = {normalizar(c): c for c in df_bcb.columns}
        
        c_inst = next((v for k, v in cols_orig.items() if 'instituicao' in k), None)
        c_idx = next((v for k, v in cols_orig.items() if 'indice' in k), None)
        # Prioriza a coluna 'total' de clientes
        c_cli = next((v for k, v in cols_orig.items() if 'total' in k and 'clientes' in k), 
                     next((v for k, v in cols_orig.items() if 'clientes' in k), None))
        c_proc = next((v for k, v in cols_orig.items() if 'procedentes' in k and 'extra' not in k), None)
        c_resp = next((v for k, v in cols_orig.items() if 'respondidas' in k), None)

        news_summary = df_news.groupby('bank').size().reset_index(name='qtd_noticias_recentes')
        synonyms = {"itau": "itau", "bradesco": "bradesco", "santander": "santander",
                    "banco do brasil": "banco do brasil", "nubank": "nu ", "caixa": "caixa economica"}

        gold_data = []
        for _, row_news in news_summary.iterrows():
            bank_name = row_news['bank']
            term = synonyms.get(normalizar(bank_name), normalizar(bank_name))
            match_bcb = df_bcb[df_bcb[c_inst].str.contains(term, case=False, na=False)].iloc[0:1]
            match_cons = df_cons[df_cons["banco"].str.contains(term, case=False, na=False)]
            top_status = match_cons['status'].value_counts().idxmax() if not match_cons.empty else "Normal"

            if not match_bcb.empty:
                gold_data.append({
                    'bank': bank_name,
                    'qtd_noticias_recentes': row_news['qtd_noticias_recentes'],
                    'indice_bcb': limpar_numero(match_bcb[c_idx].values[0]),
                    'total_clientes': limpar_numero(match_bcb[c_cli].values[0]),
                    'recl_procedentes': limpar_numero(match_bcb[c_proc].values[0]),
                    'total_respondidas': limpar_numero(match_bcb[c_resp].values[0]),
                    'principal_motivo': top_status
                })

        pd.DataFrame(gold_data).to_csv("data/gold/fact_finvoc_summary.csv", index=False)
        print("✅ Dados limpos e exportados!")

if __name__ == "__main__":
    run_gold_analysis()