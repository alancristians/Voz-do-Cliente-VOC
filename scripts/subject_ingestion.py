import requests
import pandas as pd
import os
from io import StringIO

def baixar_ranking_assuntos():
    # URL capturada no seu log
    url = "https://www3.bcb.gov.br/ranking/historico.do?wicket:interface=:6:20::::"
    
    # Headers e Cookies que você interceptou
    headers = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
        "referer": "https://www3.bcb.gov.br/ranking/historico.do",
    }
    
    # Adicione aqui os cookies exatamente como estão no seu log para o teste local imediato
    cookies = {
        "JSESSIONID": "0000dinZciJZMNDSK8zd6WFaylk:1hutbbsvt",
        "bcb-aceitacookiev2": "%7Bnecessary%3A%20true%2C%20performance%3A%20true%2C%20marketing%3A%20true%7D",
        # Adicione os outros se o erro 403 persistir
    }

    print("--- 📡 Tentando baixar via Wicket Session ---")
    
    try:
        response = requests.get(url, headers=headers, cookies=cookies, timeout=15)
        
        if response.status_code == 200:
            print("✅ Download concluído!")
            # O BCB usa latin-1 e ponto-e-vírgula
            content = response.content.decode('latin-1')
            df = pd.read_csv(StringIO(content), sep=';')
            
            # Limpeza de colunas
            df.columns = [c.strip() for c in df.columns]
            
            # Salva na sua Silver
            output_path = "data/silver/stg_assuntos_ranking.csv"
            df.to_csv(output_path, index=False, encoding='utf-8')
            print(f"💾 Arquivo salvo em: {output_path}")
            return df
        else:
            print(f"❌ Erro {response.status_code}. A sessão pode ter expirado.")
            return None
    except Exception as e:
        print(f"⚠️ Falha: {e}")
        return None

if __name__ == "__main__":
    baixar_ranking_assuntos()