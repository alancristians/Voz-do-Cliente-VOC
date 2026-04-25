import pandas as pd
import requests
import os

def ingest_bcb_subjects(ano=2026, trimestre=1):
    print(f"🚀 Tentando captura de assuntos do {trimestre}º Tri/{ano}...")
    url = "https://olinda.bcb.gov.br/olinda/servico/RankingReclamacoes/versao/v1/odata/ReclamacoesAgrupadasPorAssunto"
    params = {"@ano": ano, "@trimestre": trimestre, "$format": "json"}
    
    try:
        response = requests.get(url, params=params, timeout=20)
        
        # Se 2026/1 falhar (Erro 400), tentamos o Fallback 2025/4
        if response.status_code != 200:
            print(f"⚠️ Dados de {ano}/{trimestre} indisponíveis (Status {response.status_code}).")
            print("🔄 Iniciando fallback para 2025/4...")
            params = {"@ano": 2025, "@trimestre": 4, "$format": "json"}
            response = requests.get(url, params=params, timeout=20)

        data = response.json()
        if 'value' in data and len(data['value']) > 0:
            df = pd.DataFrame(data['value'])
            os.makedirs("data/bronze", exist_ok=True)
            # Salvamos com o nome do período real para a Gold achar o mais recente
            periodo = f"{params['@ano']}_{params['@trimestre']}"
            path = f"data/bronze/bcb_assuntos_{periodo}.parquet"
            df.to_parquet(path, index=False)
            print(f"✅ Sucesso! {len(df)} registros salvos em: {path}")
        else:
            print("⚠️ API respondeu, mas não trouxe dados.")
            
    except Exception as e:
        print(f"❌ Erro crítico na ingestão: {e}")

if __name__ == "__main__":
    ingest_bcb_subjects()