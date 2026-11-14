import pandas as pd
import os

def criar_seed_historica():
    """
    Gera o arquivo CSV de seed com a base legada de reclamações do BCB (2025/1T até 2026/2T).
    Isola os dados históricos da lógica dos scripts da camada Silver.
    """
    print("🌱 Criando Seed Histórica Legada (data/seeds/hist_bcb_seed.csv)...")
    os.makedirs("data/seeds", exist_ok=True)
    
    dados_historicos = [
        # Q1/25
        {"bank": "Bradesco", "indice_bcb": 69.80, "total_clientes": 105000000, "ano": 2025, "trimestre": 1, "periodo": "Q1/25", "ordem_cronologica": 20251},
        {"bank": "C6", "indice_bcb": 58.40, "total_clientes": 30000000, "ano": 2025, "trimestre": 1, "periodo": "Q1/25", "ordem_cronologica": 20251},
        {"bank": "Itaú", "indice_bcb": 62.10, "total_clientes": 95000000, "ano": 2025, "trimestre": 1, "periodo": "Q1/25", "ordem_cronologica": 20251},
        {"bank": "BTG Pactual", "indice_bcb": 36.50, "total_clientes": 25000000, "ano": 2025, "trimestre": 1, "periodo": "Q1/25", "ordem_cronologica": 20251},
        {"bank": "PicPay", "indice_bcb": 36.10, "total_clientes": 65000000, "ano": 2025, "trimestre": 1, "periodo": "Q1/25", "ordem_cronologica": 20251},
        {"bank": "Santander", "indice_bcb": 37.40, "total_clientes": 70000000, "ano": 2025, "trimestre": 1, "periodo": "Q1/25", "ordem_cronologica": 20251},
        {"bank": "Inter", "indice_bcb": 34.10, "total_clientes": 40000000, "ano": 2025, "trimestre": 1, "periodo": "Q1/25", "ordem_cronologica": 20251},
        {"bank": "Caixa", "indice_bcb": 35.80, "total_clientes": 150000000, "ano": 2025, "trimestre": 1, "periodo": "Q1/25", "ordem_cronologica": 20251},
        {"bank": "Nubank", "indice_bcb": 10.70, "total_clientes": 100000000, "ano": 2025, "trimestre": 1, "periodo": "Q1/25", "ordem_cronologica": 20251},
        
        # Q2/25
        {"bank": "Bradesco", "indice_bcb": 60.50, "total_clientes": 106000000, "ano": 2025, "trimestre": 2, "periodo": "Q2/25", "ordem_cronologica": 20252},
        {"bank": "C6", "indice_bcb": 48.70, "total_clientes": 31000000, "ano": 2025, "trimestre": 2, "periodo": "Q2/25", "ordem_cronologica": 20252},
        {"bank": "Itaú", "indice_bcb": 84.90, "total_clientes": 96000000, "ano": 2025, "trimestre": 2, "periodo": "Q2/25", "ordem_cronologica": 20252},
        {"bank": "Santander", "indice_bcb": 29.80, "total_clientes": 71000000, "ano": 2025, "trimestre": 2, "periodo": "Q2/25", "ordem_cronologica": 20252},
        {"bank": "Nubank", "indice_bcb": 13.10, "total_clientes": 105000000, "ano": 2025, "trimestre": 2, "periodo": "Q2/25", "ordem_cronologica": 20252},
        
        # Q3/25
        {"bank": "Bradesco", "indice_bcb": 52.10, "total_clientes": 107000000, "ano": 2025, "trimestre": 3, "periodo": "Q3/25", "ordem_cronologica": 20253},
        {"bank": "Itaú", "indice_bcb": 96.50, "total_clientes": 97000000, "ano": 2025, "trimestre": 3, "periodo": "Q3/25", "ordem_cronologica": 20253},
        {"bank": "Santander", "indice_bcb": 29.20, "total_clientes": 71500000, "ano": 2025, "trimestre": 3, "periodo": "Q3/25", "ordem_cronologica": 20253},
        {"bank": "Nubank", "indice_bcb": 12.80, "total_clientes": 108000000, "ano": 2025, "trimestre": 3, "periodo": "Q3/25", "ordem_cronologica": 20253},
        
        # Q4/25
        {"bank": "Bradesco", "indice_bcb": 44.10, "total_clientes": 108000000, "ano": 2025, "trimestre": 4, "periodo": "Q4/25", "ordem_cronologica": 20254},
        {"bank": "Itaú", "indice_bcb": 39.20, "total_clientes": 97500000, "ano": 2025, "trimestre": 4, "periodo": "Q4/25", "ordem_cronologica": 20254},
        {"bank": "Santander", "indice_bcb": 27.30, "total_clientes": 72000000, "ano": 2025, "trimestre": 4, "periodo": "Q4/25", "ordem_cronologica": 20254},
        {"bank": "Nubank", "indice_bcb": 12.10, "total_clientes": 110000000, "ano": 2025, "trimestre": 4, "periodo": "Q4/25", "ordem_cronologica": 20254},

        # Q1/26
        {"bank": "Bradesco", "indice_bcb": 48.90, "total_clientes": 109000000, "ano": 2026, "trimestre": 1, "periodo": "Q1/26", "ordem_cronologica": 20261},
        {"bank": "C6", "indice_bcb": 55.40, "total_clientes": 34000000, "ano": 2026, "trimestre": 1, "periodo": "Q1/26", "ordem_cronologica": 20261},
        {"bank": "Itaú", "indice_bcb": 34.20, "total_clientes": 98000000, "ano": 2026, "trimestre": 1, "periodo": "Q1/26", "ordem_cronologica": 20261},
        {"bank": "Santander", "indice_bcb": 34.50, "total_clientes": 72500000, "ano": 2026, "trimestre": 1, "periodo": "Q1/26", "ordem_cronologica": 20261},
        {"bank": "Nubank", "indice_bcb": 10.80, "total_clientes": 115000000, "ano": 2026, "trimestre": 1, "periodo": "Q1/26", "ordem_cronologica": 20261},

        # Q2/26
        {"bank": "Bradesco", "indice_bcb": 84.90, "total_clientes": 110000000, "ano": 2026, "trimestre": 2, "periodo": "Q2/26", "ordem_cronologica": 20262},
        {"bank": "C6", "indice_bcb": 59.77, "total_clientes": 35000000, "ano": 2026, "trimestre": 2, "periodo": "Q2/26", "ordem_cronologica": 20262},
        {"bank": "Itaú", "indice_bcb": 46.87, "total_clientes": 98400000, "ano": 2026, "trimestre": 2, "periodo": "Q2/26", "ordem_cronologica": 20262},
        {"bank": "BTG Pactual", "indice_bcb": 41.98, "total_clientes": 27000000, "ano": 2026, "trimestre": 2, "periodo": "Q2/26", "ordem_cronologica": 20262},
        {"bank": "PicPay", "indice_bcb": 35.66, "total_clientes": 71000000, "ano": 2026, "trimestre": 2, "periodo": "Q2/26", "ordem_cronologica": 20262},
        {"bank": "Santander", "indice_bcb": 34.19, "total_clientes": 72800000, "ano": 2026, "trimestre": 2, "periodo": "Q2/26", "ordem_cronologica": 20262},
        {"bank": "Inter", "indice_bcb": 26.15, "total_clientes": 45000000, "ano": 2026, "trimestre": 2, "periodo": "Q2/26", "ordem_cronologica": 20262},
        {"bank": "Caixa", "indice_bcb": 23.32, "total_clientes": 160000000, "ano": 2026, "trimestre": 2, "periodo": "Q2/26", "ordem_cronologica": 20262},
        {"bank": "Nubank", "indice_bcb": 9.20, "total_clientes": 118000000, "ano": 2026, "trimestre": 2, "periodo": "Q2/26", "ordem_cronologica": 20262}
    ]

    df_seed = pd.DataFrame(dados_historicos)
    seed_path = "data/seeds/hist_bcb_seed.csv"
    df_seed.to_csv(seed_path, index=False, sep=';', encoding='latin-1')
    print(f"✅ Seed gravada com sucesso em: {seed_path}")

if __name__ == "__main__":
    criar_seed_historica()