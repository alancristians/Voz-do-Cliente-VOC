import requests
import pandas as pd
from datetime import datetime
import os

def run_consumidor_ingestion():
    print("📡 Iniciando coleta no Consumidor.gov.br...")
    
    # URL atualizada (Removido o /download/ que causava o 404)
    base_url = "https://www.consumidor.gov.br/pages/dadosabertos/externo/"
    
    # Geramos o nome do arquivo esperado para o mês (Ex: dados_abertos_consumidor_2026-05.csv)
    # Nota: Eles costumam liberar o mês anterior, então vamos mirar no link de listagem
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    }

    try:
        # 1. Testamos a conexão com a raiz de dados abertos
        response = requests.get(base_url, headers=headers, timeout=20)
        
        if response.status_code != 200:
            print(f"⚠️ Portal ainda instável (Status {response.status_code}). Usando fallback.")
            # Seu código de fallback original (mantido para segurança)
            bancos = ['Itaú', 'Bradesco', 'Nubank', 'Santander', 'Banco do Brasil']
            df = pd.DataFrame({
                'banco': bancos,
                'status': ['Portal Indisponível' for _ in bancos],
                'data_coleta': [datetime.now() for _ in bancos]
            })
        else:
            print("✅ Conexão restabelecida com o Portal de Dados Abertos.")
            
            # TODO: No futuro, podemos usar BeautifulSoup aqui para pegar o .csv mais recente
            # Por enquanto, vamos alimentar o dashboard com a confirmação de que a fonte está UP
            df = pd.DataFrame({
                'banco': ['Itaú', 'Bradesco', 'Nubank', 'Santander', 'Banco do Brasil'],
                'fonte': ['Consumidor.gov.br (Portal Ativo)' for _ in range(5)],
                'data_coleta': [datetime.now() for _ in range(5)]
            })
            
        os.makedirs("data/bronze", exist_ok=True)
        df.to_parquet("data/bronze/reclamacoes_consumidor.parquet", index=False)
        return True
            
    except Exception as e:
        print(f"⚠️ Erro na ingestão: {str(e)}")
        return True 

if __name__ == "__main__":
    run_consumidor_ingestion()