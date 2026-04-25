import pandas as pd
import os
import glob
import time
from google import genai

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def run_silver_transformation():
    print("🚀 Silver 3.1: Tentativa FINAL com Gemini 1.5 Flash...")
    os.makedirs("data/silver", exist_ok=True)
    
    news_path = "data/bronze/noticias_bancos.parquet"
    if not os.path.exists(news_path): return

    df = pd.read_parquet(news_path)
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    df = df.drop_duplicates(subset=['title'])
    
    if not df.empty:
        # Pegamos apenas as 15 notícias mais recentes para o teste definitivo
        df_batch = df.head(15).copy()
        noticias_list = "\n".join([f"{i}: {title}" for i, title in enumerate(df_batch['title'])])
        
        prompt = f"""
        Analise o sentimento das seguintes manchetes bancárias. 
        Responda APENAS no formato 'ÍNDICE: SENTIMENTO'.
        Use apenas Positivo, Negativo ou Neutro.
        
        Notícias:
        {noticias_list}
        """
        
        try:
            print(f"🧠 Enviando lote de 15 notícias para o Gemini 1.5 Flash...")
            response = client.models.generate_content(
                model='gemini-1.5-flash', # CHAVEAMENTO PARA O MODELO ESTÁVEL
                contents=prompt
            )
            
            respostas = response.text.strip().split('\n')
            sentimentos_map = {}
            for linha in respostas:
                if ':' in linha:
                    try:
                        idx, sent = linha.split(':', 1)
                        sentimentos_map[int(idx.strip())] = sent.strip().capitalize()
                    except: continue
            
            df['sentimento'] = df.index.map(sentimentos_map).fillna('Neutro (Não analisado)')
            print(f"✅ Sucesso! {len(sentimentos_map)} notícias classificadas.")
            
        except Exception as e:
            print(f"⚠️ Falha final na IA: {e}")
            df['sentimento'] = 'Neutro'

        df.to_parquet("data/silver/stg_noticias.parquet", index=False)

    # Processamento BCB (Base para o nosso próximo upgrade)
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