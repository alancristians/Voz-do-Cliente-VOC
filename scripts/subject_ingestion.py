import pandas as pd
import requests

def ingest_bcb_subjects(ano=2026, trimestre=1):
    """
    Consome a API OData do BCB para capturar os motivos mais frequentes.
    """
    url = "https://olinda.bcb.gov.br/olinda/servico/RankingReclamacoes/versao/v1/odata/ReclamacoesAgrupadasPorAssunto"
    params = {
        "@ano": ano,
        "@trimestre": trimestre,
        "$format": "json"
    }
    
    try:
        response = requests.get(url, params=params)
        data = response.json()
        df = pd.DataFrame(data['value'])
        
        # Salvando na Bronze para manter a rastreabilidade
        df.to_parquet(f"data/bronze/bcb_assuntos_{ano}_{trimestre}.parquet", index=False)
        return df
    except Exception as e:
        print(f"❌ Erro na API do BCB: {e}")
        return None