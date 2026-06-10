import pandas as pd
import os
import glob

def normalizar_colunas(df):
    """
    Padroniza o schema do DataFrame: converte nomes de colunas para snake_case,
    remove espaços e caracteres especiais/acentuações.
    
    Args:
        df (pd.DataFrame): DataFrame bruto.
    Returns:
        pd.DataFrame: DataFrame com colunas normalizadas.
    """
    df.columns = [
        c.lower().strip()
        .replace(' ', '_')
        .replace('.', '')
        .replace('í', 'i')
        .replace('ã', 'a')
        .replace('ç', 'c')
        .replace('ó', 'o')
        .replace('ê', 'e') for c in df.columns
    ]
    return df

def executar_silver():
    """
    Orquestra a transformação e higienização da camada Silver (Staging).
    Processa dados de mídia e rankings do BCB aplicando regras de integridade.
    """
    print("🥈 Iniciando processamento da camada Silver...")
    os.makedirs("data/silver", exist_ok=True)
    
    # -------------------------------------------------------------------------
    # 1. PROCESSAMENTO DE MÍDIA (NOTÍCIAS)
    # -------------------------------------------------------------------------
    path_news = "data/bronze/noticias_bancos.parquet"
    if os.path.exists(path_news):
        # Leitura da Bronze Incremental
        df_n = pd.read_parquet(path_news)
        
        # Deduplicação baseada em chave primária natural estável (link)
        df_n = df_n.drop_duplicates(subset=['link'], keep='first')
        
        # Alinhamento de colunas padrão snake_case
        df_n.columns = [c.lower().strip().replace(' ', '_') for c in df_n.columns]
        
        # Garantia de existência do campo 'bank' (chave de junção/filtro)
        if 'banco' in df_n.columns and 'bank' not in df_n.columns: 
            df_n['bank'] = df_n['banco']
            
        df_n.to_parquet("data/silver/stg_noticias.parquet", index=False)
        print(f"✅ Camada Silver: Notícias processadas e de-duplicadas. Total: {len(df_n)} registros.")

    # -------------------------------------------------------------------------
    # 2. RANKING GERAL BCB (MÉTRICAS QUANTITATIVAS)
    # -------------------------------------------------------------------------
    arquivos_rank = glob.glob("data/bronze/reclamacoes_bcb_*.parquet")
    if arquivos_rank:
        ultimo_ranking = sorted(arquivos_rank)[-1]
        df_r = pd.read_parquet(ultimo_ranking)
        df_r = normalizar_colunas(df_r)
        df_r.to_parquet("data/silver/stg_bcb_ranking.parquet", index=False)
        print(f"✅ Camada Silver: Último ranking BCB processado ({os.path.basename(ultimo_ranking)}).")

    # -------------------------------------------------------------------------
    # 3. ASSUNTOS BCB (DADOS QUALITATIVOS)
    # ALINHAMENTO DE SCHEMA: Mantém compatibilidade controlada com as colunas do app.py
    # -------------------------------------------------------------------------
    arquivos_assuntos = glob.glob("data/bronze/assuntos_bcb_*.parquet")
    if arquivos_assuntos:
        ultimo_assuntos = sorted(arquivos_assuntos)[-1]
        df_a = pd.read_parquet(ultimo_assuntos)
        
        # IMPORTANTE: Para evitar quebra por KeyError no app.py (linhas 106-108),
        # salvamos o arquivo mantendo as colunas originais esperadas pela camada de apresentação,
        # ou aplicando um mapeamento explícito se necessário. Aqui preservamos o contrato do MVP.
        # Caso decida usar colunas normalizadas no app.py futuramente, altere a linha abaixo para usar normalizar_colunas(df_a)
        
        df_a.to_parquet("data/silver/stg_bcb_assuntos.csv", index=False) # Mantido em CSV/Parquet conforme fluxo original do app
        print(f"✅ Camada Silver: Dados qualitativos de assuntos preservados para o Dashboard.")

if __name__ == "__main__":
    executar_silver()