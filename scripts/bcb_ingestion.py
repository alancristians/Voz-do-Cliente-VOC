import requests
import pandas as pd
from datetime import datetime
import os
import sys

def run_bcb_ingestion():
    print("📡 Consultando Dados Oficiais do BCB (Foco em 2025/2026)...")
    
    # 🎯 CONFIGURAÇÃO DO PERÍODO
    # Como você viu que o 4º Tri já saiu, atualizamos os parâmetros.
    # No futuro, podemos automatizar essa escolha baseada na data atual.
    ano = "2025"
    trimestre = "4"
    
    url = f"https://www3.bcb.gov.br/rdrweb/rest/ext/ranking/arquivo?ano={ano}&periodicidade=TRIMESTRAL&periodo={trimestre}&tipo=Bancos+e+financeiras"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            from io import StringIO
            # O BCB usa latin-1 e separador ponto e vírgula
            content = response.content.decode('latin-1')
            df = pd.read_csv(StringIO(content), sep=';')
            
            if df.empty:
                print(f"⚠️ O arquivo do {trimestre}º Tri de {ano} retornou vazio.")
                return False
                
            # Adiciona timestamp de extração para auditoria (Boa prática de Data Eng)
            df['extraction_at'] = datetime.now()
            
            os.makedirs("data/bronze", exist_ok=True)
            
            # 💾 SALVAMENTO DINÂMICO
            # Salvando com o período no nome para a Gold Analysis identificar o mais novo
            filename = f"reclamacoes_bcb_{ano}_{trimestre}T.parquet"
            save_path = os.path.join("data/bronze", filename)
            
            df.to_parquet(save_path, index=False)
            print(f"✅ Sucesso! {len(df)} linhas coletadas do ranking de {trimestre}T/{ano}.")
            return True
        else:
            print(f"❌ Erro ao acessar BCB: Código {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Erro crítico na ingestão: {e}")
        return False

if __name__ == "__main__":
    if not run_bcb_ingestion():
        sys.exit(1)