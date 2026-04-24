import pandas as pd
import os
import glob
import time
from google import genai

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def run_silver_transformation():
    print("🚀 Silver 3.0: Processamento em LOTE (Batch Mode) iniciado...")
    os.makedirs("data/silver", exist_ok=True)
    
    news_path = "data/bronze/noticias_bancos.parquet"
    if not os.path.exists(news_path):
        print("⚠️ Arquivo bronze não encontrado.")
        return

    df = pd.read_parquet(news_path)
    # Limpeza básica de colunas
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    df = df.drop_duplicates(subset=['title'])
    
    if not df.empty:
        # Preparamos a lista de notícias formatada para a IA
        noticias_list = "\n".join([f"{i}: {title}" for i, title in enumerate(df['title'])])
        
        prompt = f"""
        Analise o sentimento das seguintes manchetes bancárias. 
        Responda APENAS no formato 'ÍNDICE: SENTIMENTO' (ex: 0: Positivo, 1: Negativo).
        Use apenas Positivo, Negativo ou Neutro.
        
        Notícias:
        {noticias_list}
        """
        
        try:
            print(f"🧠 Enviando {len(df)} notícias para análise em lote...")
            response = client.models.generate_content(
                model='gemini-2.0-flash',
                contents=prompt
            )
            
            # Parse das respostas
            respostas = response.text.strip().split('\n')
            sentimentos_map = {}
            for linha in respostas:
                if ':' in linha:
                    idx, sent = linha.split(':', 1)
                    sentimentos_map[int(idx.strip())] = sent.strip().capitalize()
            
            # Mapeamos de volta para o DataFrame
            df['sentimento'] = df.index.map(sentimentos_map).fillna('Neutro')
            print(f"✅ Sucesso! {len(sentimentos_map)} notícias classificadas.")
            
        except Exception as e:
            print(f"⚠️ Erro no processamento Batch: {e}")
            df['sentimento'] = 'Neutro'

        df.to_parquet("data/silver/stg_noticias.parquet", index=False)

    # Processamento BCB (Sempre rápido)
    arquivos_bcb = glob.glob("data/bronze/reclamacoes_bcb*.parquet")
    if arquivos_bcb:
        bcb_path = sorted(arquivos_bcb)[-1]
        df_bcb = pd.read_parquet(bcb_path)
        df_bcb.columns = [c.lower().replace(' ', '_').replace('.', '').replace('í', 'i').replace('ã', 'a').replace('ç', 'c') for c in df_bcb.columns]
        df_bcb.to_parquet("data/silver/stg_bcb.parquet", index=False)
        print(f"✅ Dados BCB processados.")

    print("🏆 Camada Prata FINALIZADA em tempo recorde!")

if __name__ == "__main__":
    run_silver_transformation()