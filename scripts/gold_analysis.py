import pandas as pd
import os
import glob
import unicodedata

def normalizar(texto):
    """
    Normaliza strings para match de dados: remove acentos e padroniza caixa.
    """
    if not isinstance(texto, str): return ""
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower().strip()

def limpar_numero(valor):
    """
    Converte strings numéricas formatadas para float64.
    """
    if pd.isna(valor) or valor == "" or valor == " ": return 0.0
    if isinstance(valor, str):
        valor = valor.replace('.', '').replace(',', '.')
    try:
        return float(valor)
    except:
        return 0.0

def run_gold_analysis():
    """
    Gera a camada gold consolidando métricas do BCB, Consumidor.gov e volume de notícias.
    """
    os.makedirs("data/gold", exist_ok=True)
    
    arquivos_bcb = glob.glob("data/bronze/reclamacoes_bcb*.parquet")
    if not arquivos_bcb: return

    df_bcb = pd.read_parquet(sorted(arquivos_bcb)[-1])
    df_cons = pd.read_parquet("data/bronze/reclamacoes_consumidor.parquet")
    df_news = pd.read_parquet("data/silver/stg_noticias.parquet")
    
    news_summary = df_news.groupby('bank').size().reset_index(name='qtd_noticias')

    cols_orig = {normalizar(c): c for c in df_bcb.columns}
    c_inst = next((v for k, v in cols_orig.items() if 'instituicao' in k), None)
    c_idx = next((v for k, v in cols_orig.items() if 'indice' in k), None)
    c_cli = next((v for k, v in cols_orig.items() if 'clientes' in k), None)
    c_proc = next((v for k, v in cols_orig.items() if 'procedentes' in k), None)
    c_resp = next((v for k, v in cols_orig.items() if 'respondidas' in k), None)
    
    c_ano = next((v for k, v in cols_orig.items() if 'ano' in k), "2026")
    c_tri = next((v for k, v in cols_orig.items() if 'trimestre' in k), "1º")
    label_periodo = f"{df_bcb[c_tri].iloc[0]} Trimestre / {df_bcb[c_ano].iloc[0]}"

    synonyms = {
        "itau": "itau", 
        "bradesco": "bradesco", 
        "santander": "santander",
        "banco do brasil": "banco do brasil", 
        "nubank": "nu ", 
        "caixa": "caixa economica",
        "c6": "c6",
        "btg": "btg pactual",
        "picpay": "picpay",
        "inter": "inter",
        "neon": "neon",
        "mercado pago": "mercado pago",
        "pagseguro": "pagseguro"
    }

    gold_data = []
    for _, row_news in news_summary.iterrows():
        bank_name = row_news['bank']
        termo = synonyms.get(normalizar(bank_name), normalizar(bank_name))
        
        match_bcb = df_bcb[df_bcb[c_inst].str.contains(termo, case=False, na=False)].iloc[0:1]
        match_cons = df_cons[df_cons["banco"].str.contains(termo, case=False, na=False)]
        top_status = match_cons['status'].value_counts().idxmax() if not match_cons.empty else "Normal"

        if not match_bcb.empty:
            gold_data.append({
                'bank': bank_name,
                'qtd_noticias_recentes': row_news['qtd_noticias'],
                'indice_bcb': limpar_numero(match_bcb[c_idx].values[0]),
                'total_clientes': limpar_numero(match_bcb[c_cli].values[0]),
                'recl_procedentes': limpar_numero(match_bcb[c_proc].values[0]),
                'total_respondidas': limpar_numero(match_bcb[c_resp].values[0]),
                'principal_motivo': top_status,
                'periodo': label_periodo
            })

    pd.DataFrame(gold_data).to_csv("data/gold/fact_finvoc_summary.csv", index=False)

if __name__ == "__main__":
    run_gold_analysis()