import pandas as pd
import os
import glob
import re
import unicodedata

def normalizar_texto(texto):
    if not isinstance(texto, str): return ""
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower().strip()

def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        c.lower().strip()
        .replace(' ', '_')
        .replace('.', '')
        .replace('í', 'i')
        .replace('ã', 'a')
        .replace('ç', 'c')
        .replace('ó', 'o')
        .replace('ê', 'e') for c in df.columns
    ]
    df = df.loc[:, ~df.columns.duplicated()]
    return df

def extrair_ordem_arquivo(caminho_arquivo: str) -> tuple[int, int]:
    match = re.search(r'(\d{4})_(\d)T', caminho_arquivo)
    if match:
        return int(match.group(1)), int(match.group(2))
    return 0, 0

def sanitizar_serie_numerica(serie: pd.Series) -> pd.Series:
    s = serie.astype(str).str.strip()
    mascara_virgula = s.str.contains(',', na=False)
    s.loc[mascara_virgula] = s.loc[mascara_virgula].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    mascara_pontos_multiplos = s.str.count(r'\.') > 1
    s.loc[mascara_pontos_multiplos] = s.loc[mascara_pontos_multiplos].str.replace('.', '', regex=False)
    return pd.to_numeric(s, errors='coerce').fillna(0.0)

def processar_historico_dinamico():
    hist_path = "data/silver/hist_reclamacoes_bcb.csv"
    seed_path = "data/seeds/hist_bcb_seed.csv"
    
    mapa_nomes = {
        "ITAU (conglomerado)": "Itaú",
        "BRADESCO (conglomerado)": "Bradesco",
        "SANTANDER (conglomerado)": "Santander",
        "NU PAGAMENTOS (conglomerado)": "Nubank",
        "BB (conglomerado)": "Banco do Brasil",
        "CAIXA ECONÔMICA FEDERAL (conglomerado)": "Caixa",
        "CAIXA ECONMICA FEDERAL (conglomerado)": "Caixa",
        "BANCO C6 (conglomerado)": "C6",
        "BTG PACTUAL/BANCO PAN (conglomerado)": "BTG Pactual",
        "PICPAY (conglomerado)": "PicPay",
        "INTER (conglomerado)": "Inter",
        "MERCADO PAGO IP (conglomerado)": "Mercado Pago"
    }

    dfs_para_concatenar = []

    if os.path.exists(seed_path):
        try:
            df_seed = pd.read_csv(seed_path, sep=';', encoding='latin-1')
            dfs_para_concatenar.append(df_seed)
            print(f"📦 Seed carregada: {len(df_seed)} registros legados.")
        except Exception as e:
            print(f"⚠️ Erro ao carregar seed histórica: {e}")

    arquivos_bronze = glob.glob("data/bronze/reclamacoes_bcb_*.parquet")
    for arq in arquivos_bronze:
        try:
            df_b = pd.read_parquet(arq)
            df_b = df_b.loc[:, ~df_b.columns.duplicated()]

            c_inst = next((c for c in df_b.columns if 'instituicao' in normalizar_texto(c)), None)
            c_idx = next((c for c in df_b.columns if 'indice' in normalizar_texto(c)), None)
            c_cli = next((c for c in df_b.columns if 'clientes' in normalizar_texto(c)), None)

            if c_inst and c_idx and c_cli:
                df_part = df_b[[c_inst, c_idx, c_cli]].copy()
                df_part.columns = ['bank', 'indice_bcb', 'total_clientes']
                df_part['bank'] = df_part['bank'].str.strip().replace(mapa_nomes)
                
                df_part['indice_bcb'] = sanitizar_serie_numerica(df_part['indice_bcb'])
                df_part['total_clientes'] = sanitizar_serie_numerica(df_part['total_clientes'])
                
                ano, tri = extrair_ordem_arquivo(arq)
                if ano == 0:
                    ano = df_b['ano'].iloc[0] if 'ano' in df_b.columns else 2026
                    tri = df_b['trimestre'].iloc[0] if 'trimestre' in df_b.columns else 3
                    
                df_part['ano'] = int(ano)
                df_part['trimestre'] = int(tri)
                df_part['periodo'] = f"Q{tri}/{str(ano)[-2:]}"
                df_part['ordem_cronologica'] = int(f"{ano}{tri}")
                dfs_para_concatenar.append(df_part)
        except Exception as e:
            print(f"⚠️ Erro ao processar partição {arq}: {e}")

    if dfs_para_concatenar:
        df_consolidado = pd.concat(dfs_para_concatenar, ignore_index=True)
        df_consolidado['ordem_cronologica'] = pd.to_numeric(df_consolidado['ordem_cronologica'], errors='coerce').fillna(0).astype(int)
        df_consolidado = df_consolidado.drop_duplicates(subset=['bank', 'ordem_cronologica'], keep='last')
        df_consolidado = df_consolidado.sort_values(by=['ordem_cronologica', 'bank'])
        
        df_consolidado.to_csv(hist_path, index=False, sep=';', encoding='latin-1')
        print(f"✨ Histórico Silver dinâmico consolidado em {hist_path} ({len(df_consolidado)} registros).")

def executar_silver():
    print("🥈 Iniciando processamento da camada Silver (Arquitetura Limpa)...")
    os.makedirs("data/silver", exist_ok=True)
    
    # 1. Notícias com de-duplicacao inteligente por Titulo + Banco
    path_news = "data/bronze/noticias_bancos.parquet"
    if os.path.exists(path_news):
        df_n = pd.read_parquet(path_news)
        df_n.columns = [c.lower().strip().replace(' ', '_') for c in df_n.columns]
        if 'banco' in df_n.columns and 'bank' not in df_n.columns: 
            df_n['bank'] = df_n['banco']
            
        # Deduplica por titulo e banco para eliminar URLs com tokens dinamicos
        df_n = df_n.drop_duplicates(subset=['bank', 'title'], keep='first')
        df_n.to_parquet("data/silver/stg_noticias.parquet", index=False)
        print(f"✅ Silver: {len(df_n)} notícias únicas (de-duplicadas por título).")

    # 2. Ranking BCB
    arquivos_rank = glob.glob("data/bronze/reclamacoes_bcb_*.parquet")
    if arquivos_rank:
        ultimo_ranking = sorted(arquivos_rank, key=extrair_ordem_arquivo)[-1]
        df_r = pd.read_parquet(ultimo_ranking)
        df_r_norm = normalizar_colunas(df_r.copy())
        df_r_norm.to_parquet("data/silver/stg_bcb_ranking.parquet", index=False)
        print(f"✅ Silver: Último ranking BCB processado ({os.path.basename(ultimo_ranking)}).")

    # 3. Consolidação Histórica Dinâmica
    processar_historico_dinamico()

if __name__ == "__main__":
    executar_silver()