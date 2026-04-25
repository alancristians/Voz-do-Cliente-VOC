import pandas as pd
import requests
import os

def ingest_bcb_subjects(ano=2026, trimestre=1):
    """
    Consome a API OData do BCB para capturar os motivos mais frequentes.
    """
    print(f"🚀 Iniciando captura de assuntos do {trimestre}º Tri/{ano}...")
    
    url = "https://olinda.bcb.gov.br/olinda/servico/RankingReclamacoes/versao/v1/odata/ReclamacoesAgrupadasPorAssunto"
    params = {
        "@ano": ano,
        "@trimestre": trimestre,
        "$format": "json"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if 'value' in data and len(data['value']) > 0:
            df = pd.DataFrame(data['value'])
            
            os.makedirs("data/bronze", exist_ok=True)
            path = f"data/bronze/bcb_assuntos_{ano}_{trimestre}.parquet"
            df.to_parquet(path, index=False)
            print(f"✅ Sucesso! {len(df)} registros de assuntos salvos em Bronze.")
            return df
        else:
            print("⚠️ API retornou lista vazia para este período.")
            return None
            
    except Exception as e:
        print(f"❌ Erro na API do BCB: {e}")
        return None

if __name__ == "__main__":
    # Gatilho para a GitHub Action
    ingest_bcb_subjects(ano=2026, trimestre=1)