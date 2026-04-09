import requests
import pandas as pd
from datetime import datetime
import os
import sys

def run_bcb_ingestion():
    print("📡 Consultando Dados Oficiais do BCB (Foco em 2025/2026)...")
    
    # URL da API do Portal de Dados Abertos filtrando pelo Ranking de Reclamações
    # Vamos tentar o link direto do arquivo mais recente de 2025 que é estável
    url = "https://www3.bcb.gov.br/rdrweb/rest/ext/ranking/arquivo?ano=2025&periodicidade=TRIMESTRAL&periodo=3&tipo=Bancos+e+financeiras"
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            # O BCB entrega um CSV separado por ponto e vírgula e com encoding latin-1
            from io import StringIO
            content = response.content.decode('latin-1')
            df = pd.read_csv(StringIO(content), sep=';')
            
            if df.empty:
                print("⚠️ Arquivo veio vazio.")
                return False
                
            df['extraction_at'] = datetime.now()
            
            os.makedirs("data/bronze", exist_ok=True)
            df.to_parquet("data/bronze/reclamacoes_bcb.parquet", index=False)
            print(f"✅ Sucesso! {len(df)} linhas coletadas do ranking de 2025.")
            return True
        else:
            print(f"❌ Erro ao acessar BCB: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Erro crítico: {e}")
        return False

if __name__ == "__main__":
    if not run_bcb_ingestion():
        sys.exit(1)