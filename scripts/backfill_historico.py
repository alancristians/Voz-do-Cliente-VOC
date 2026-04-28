import requests
import pandas as pd
from datetime import datetime
import os
from io import StringIO

def run_backfill_2025():
    print("⏳ Iniciando Backfill Histórico - Ano 2025 (Versão Blindada)...")
    
    ano = "2025"
    trimestres = ["1", "2", "3", "4"]
    tipo = "Bancos+e+financeiras"
    silver_path = "data/silver/hist_reclamacoes_bcb.parquet"
    
    os.makedirs("data/silver", exist_ok=True)
    all_quarters = []

    for tri in trimestres:
        url = f"https://www3.bcb.gov.br/rdrweb/rest/ext/ranking/arquivo?ano={ano}&periodicidade=TRIMESTRAL&periodo={tri}&tipo={tipo}"
        print(f"📥 Coletando {ano} - {tri}º Trimestre...")
        
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                content = response.content.decode('latin-1')
                df = pd.read_csv(StringIO(content), sep=';')
                
                if not df.empty:
                    df.columns = [col.strip() for col in df.columns]
                    
                    # --- MAPEAMENTO ANTI-DUPLICIDADE ---
                    mapping_rules = {
                        'bank': ['instituição', 'conglomerado'],
                        'indice_bcb': ['índice', 'indice'],
                        'total_clientes': ['clientes'],
                        'reclamacoes_procedentes': ['procedentes']
                    }
                    
                    new_cols = {}
                    targets_found = set()
                    
                    for col in df.columns:
                        col_lower = col.lower()
                        for target, keywords in mapping_rules.items():
                            if target not in targets_found and any(kw in col_lower for kw in keywords):
                                new_cols[col] = target
                                targets_found.add(target)
                                break 
                    
                    df = df.rename(columns=new_cols)
                    
                    # Garantir que temos o que precisamos
                    if 'bank' not in df.columns or 'indice_bcb' not in df.columns:
                        print(f"⚠️ Falha no mapeamento do Q{tri}. Colunas: {df.columns.tolist()}")
                        continue

                    # Limpeza de strings
                    df['bank'] = df['bank'].astype(str).str.strip()
                    
                    # --- CONVERSÃO NUMÉRICA SEGURA ---
                    for num_col in ['indice_bcb', 'total_clientes']:
                        if num_col in df.columns:
                            # Converte para string, limpa pontuação brasileira e vai para float
                            df[num_col] = (df[num_col].astype(str)
                                           .str.replace('.', '', regex=False)
                                           .str.replace(',', '.', regex=False)
                                           .str.replace('None', '0'))
                            df[num_col] = pd.to_numeric(df[num_col], errors='coerce').fillna(0)
                    
                    # Metadados
                    df['ano'] = int(ano)
                    df['trimestre'] = int(tri)
                    df['periodo'] = f"Q{tri}/25"
                    df['ordem_cronologica'] = int(f"{ano}{tri}")
                    
                    # Seleção final (usando apenas o que existe de fato)
                    available_cols = [c for c in ['bank', 'indice_bcb', 'total_clientes', 'ano', 'trimestre', 'periodo', 'ordem_cronologica'] if c in df.columns]
                    all_quarters.append(df[available_cols])
                    
                    print(f"✅ {ano} Q{tri} processado: {len(df)} registros.")
            else:
                print(f"❌ Erro HTTP {response.status_code}")
                
        except Exception as e:
            print(f"⚠️ Erro inesperado em {ano} Q{tri}: {e}")

    if all_quarters:
        df_final = pd.concat(all_quarters, ignore_index=True)
        # Mudando para CSV para facilitar sua inspeção manual
        csv_path = "data/silver/hist_reclamacoes_bcb.csv"
        df_final.to_csv(csv_path, index=False, sep=';', encoding='latin-1')
        print(f"\n✨ Sucesso! Arquivo salvo para inspeção em: {csv_path}")
    else:
        print("\n❌ Nenhum dado coletado.")

if __name__ == "__main__":
    run_backfill_2025()