import requests
import pandas as pd
import os
from io import StringIO

def baixar_ranking_real_assuntos():
    """
    Realiza o download do Ranking de Assuntos do BCB utilizando a sessão 
    e interface capturadas via log de rede (Network).
    """
    # URL da Interface :17 capturada no seu log do Chrome (específica para o Ranking)
    url = "https://www3.bcb.gov.br/ranking/historico.do?wicket:interface=:6:17::::"
    
    # Headers extraídos do seu log para simular o comportamento do seu navegador
    headers = {
        "authority": "www3.bcb.gov.br",
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Mobile Safari/537.36",
        "referer": "https://www3.bcb.gov.br/ranking/historico.do",
        "accept-language": "pt-BR,pt;q=0.9,en;q=0.8",
        "priority": "u=0, i",
        "sec-fetch-dest": "iframe",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin"
    }
    
    # Cookies de sessão capturados (essenciais para autenticar o download no sistema Wicket)
    cookies = {
        "JSESSIONID": "0000dinZciJZMNDSK8zd6WFaylk:1hutbbsvt",
        "TS012b7e98": "012e4f88b38dde8bed308f80589108ea34f5af34701d785742fdf3944525092775a28a71adc557cc7bebc841460c101b76af86812559a10da19e9eec404f1d72212740a21c90e433e937a20a13e898922d682119e8a878a3cd3636a221380125605efb8fa2",
        "bcb-aceitacookiev2": "%7Bnecessary%3A%20true%2C%20performance%3A%20true%2C%20marketing%3A%20true%7D",
        "_ga": "GA1.1.326527551.1775755135",
        "_gid": "GA1.3.1631810994.1777433075"
    }

    print("--- 📡 Iniciando download do Ranking de Assuntos (:17) ---")
    
    try:
        # Request com timeout de 20s pois o servidor www3 costuma ser lento
        response = requests.get(url, headers=headers, cookies=cookies, timeout=20)
        
        if response.status_code == 200:
            print("✅ Conexão estabelecida com sucesso!")
            
            # O BCB usa encoding Latin-1 e separador ';' para CSVs de exportação
            content = response.content.decode('latin-1')
            
            # Lendo para o DataFrame
            df = pd.read_csv(StringIO(content), sep=';')
            
            # Data Cleaning: remove espaços em branco dos headers
            df.columns = [c.strip() for c in df.columns]
            
            # Garante que a pasta existe (Silver Layer)
            output_path = "data/silver/stg_assuntos_ranking.csv"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Salva em UTF-8 para evitar problemas de acentuação no Dashboard
            df.to_csv(output_path, index=False, encoding='utf-8')
            
            print(f"💾 Arquivo processado e salvo em: {output_path}")
            print(f"📊 Colunas detectadas: {df.columns.tolist()}")
            
            # Check rápido de volumetria
            col_qtd = [c for c in df.columns if 'Quantidade' in c or 'Total' in c]
            if col_qtd:
                print(f"🚀 Ingestão finalizada com {len(df)} registros.")
            else:
                print("⚠️ Atenção: O arquivo não parece conter colunas de quantidade numérica.")
            
            return True
            
        else:
            print(f"❌ Erro HTTP {response.status_code}.")
            print("Sua sessão (JSESSIONID) provavelmente expirou. Pegue uma nova no Chrome.")
            return False
            
    except Exception as e:
        print(f"⚠️ Falha catastrófica na ingestão: {e}")
        return False

# ESTE É O COMANDO QUE ESTAVA FALTANDO PARA O SCRIPT RODAR:
if __name__ == "__main__":
    baixar_ranking_real_assuntos()