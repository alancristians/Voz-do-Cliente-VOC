import requests
import pandas as pd
from datetime import datetime
import os
import sys
from io import StringIO

def baixar_dataset_bcb(ano, trimestre, tipo):
    """
    Realiza a requisição ao serviço RDR do BCB e retorna um DataFrame.
    """
    url = f"https://www3.bcb.gov.br/rdrweb/rest/ext/ranking/arquivo?ano={ano}&periodicidade=TRIMESTRAL&periodo={trimestre}&tipo={tipo}"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            content = response.content.decode('latin-1')
            df = pd.read_csv(StringIO(content), sep=';')
            if not df.empty:
                df['extraction_at'] = datetime.now()
                return df
        return None
    except Exception as e:
        print(f"⚠️ Erro ao acessar {tipo}: {e}")
        return None

def run_bcb_ingestion():
    """
    Orquestra a ingestão com fallback: se assuntos não existirem, não mata o processo.
    """
    print("📡 Consultando Dados Oficiais do BCB - 1º Tri de 2026...")
    ano = "2026"
    trimestre = "1"
    os.makedirs("data/bronze", exist_ok=True)
    
    sucesso_ranking = False

    # 1. Ingestão do Ranking (Obrigatório)
    df_ranking = baixar_dataset_bcb(ano, trimestre, "Bancos+e+financeiras")
    if df_ranking is not None:
        path_ranking = os.path.join("data/bronze", f"reclamacoes_bcb_{ano}_{trimestre}T.parquet")
        df_ranking.to_parquet(path_ranking, index=False)
        print(f"✅ Sucesso! {len(df_ranking)} instituições coletadas (Ranking).")
        sucesso_ranking = True

    # 2. Ingestão de Assuntos (Opcional - Se falhar, avisa mas não trava)
    df_assuntos = baixar_dataset_bcb(ano, trimestre, "Reclamações+por+assunto")
    if df_assuntos is not None:
        path_assuntos = os.path.join("data/bronze", f"assuntos_bcb_{ano}_{trimestre}T.parquet")
        df_assuntos.to_parquet(path_assuntos, index=False)
        print(f"✅ Sucesso! {len(df_assuntos)} registros de assuntos coletados.")
    else:
        print("ℹ️ Info: Arquivo de assuntos ainda não disponível no BCB. Usando fallback na Gold.")

    return sucesso_ranking # Retorna True se pelo menos o ranking principal baixou

if __name__ == "__main__":
    if not run_bcb_ingestion():
        print("❌ Falha crítica: Ranking Geral não encontrado.")
        sys.exit(1)