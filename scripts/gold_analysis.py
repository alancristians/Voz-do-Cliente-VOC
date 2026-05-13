import pandas as pd
import os
import unicodedata
import glob
import pytz
from datetime import datetime
from dotenv import load_dotenv
from groq import Groq

# --- CONFIGURAÇÕES INICIAIS ---
load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def normalizar_chave(texto):
    """Normaliza strings para busca e comparação de chaves."""
    if not isinstance(texto, str): return ""
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower().strip()

def limpar_valor_bcb(v):
    """Trata conversão de valores numéricos vindos do BCB (ponto/vírgula)."""
    if pd.isna(v) or v == "" or v == " ": return 0.0
    try:
        v_str = str(v).strip()
        if "," in v_str:
            v_str = v_str.replace('.', '').replace(',', '.')
        return float(v_str)
    except:
        return 0.0

def gerar_resumo_ia(banco, manchetes):
    """
    Interface com a API da Groq para sumarização de notícias.
    Implementa fail-safe para não interromper o pipeline em caso de erro na API.
    """
    if not manchetes:
        return "Sem notícias relevantes mapeadas para este banco no período."
    
    # Limita a 10 manchetes para otimização de tokens
    texto_input = "\n".join([f"- {m}" for m in manchetes[:10]])
    
    prompt = f"""
    Como analista de CX, resuma as notícias do banco {banco} em 3 tópicos curtos.
    Foque no sentimento do cliente e temas principais. Seja direto.
    
    Notícias:
    {texto_input}
    """
    
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=600
        )
        return completion.choices[0].message.content
    except Exception as e:
        print(f"⚠️ Alerta IA: Falha ao gerar insight para {banco}. Erro: {e}")
        return "Insight de IA temporariamente indisponível."

def salvar_timestamp():
    """Registra o carimbo de data/hora da última execução bem-sucedida."""
    os.makedirs("data/gold", exist_ok=True)
    sp_tz = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(sp_tz).strftime('%d/%m/%Y %H:%M')
    with open("data/gold/last_update.txt", "w") as f:
        f.write(agora)
    print(f"✅ Execução finalizada e timestamp salvo: {agora}")

def executar_gold():
    """
    Orquestrador da camada Gold: Consome Silver/Bronze, processa métricas BCB 
    e enriquece com Insights de IA.
    """
    os.makedirs("data/gold", exist_ok=True)
    
    # Definição de caminhos (Paths)
    p_rank = "data/silver/stg_bcb_ranking.parquet"
    p_news = "data/silver/stg_noticias.parquet"
    
    # Busca de Assuntos: Prioriza o CSV manual (bypass), senão busca Parquets na Bronze
    p_subjects_manual = "data/silver/stg_assuntos_ranking.csv"
    p_subjects_auto = sorted(glob.glob("data/bronze/bcb_assuntos_*.parquet"))
    
    # Validação de dependências críticas
    if not os.path.exists(p_rank) or not os.path.exists(p_news):
        print("❌ Erro: Arquivos base (Ranking ou Notícias) não encontrados.")
        return

    # Carga de dados
    df_rank = pd.read_parquet(p_rank)
    df_news = pd.read_parquet(p_news)
    
    if os.path.exists(p_subjects_manual):
        df_subjects = pd.read_csv(p_subjects_manual)
        print("📂 Carregando assuntos via Bypass (CSV Silver)")
    elif p_subjects_auto:
        df_subjects = pd.read_parquet(p_subjects_auto[-1])
        print(f"📂 Carregando assuntos via Bronze Parquet: {p_subjects_auto[-1]}")
    else:
        df_subjects = pd.DataFrame()
        print("⚠️ Aviso: Fonte de assuntos não localizada.")
    
    # Processamento de chaves para cruzamento
    df_news['bank_clean'] = df_news['bank'].apply(normalizar_chave)

    bancos_alvo = {
        "itau": "Itaú", "bradesco": "Bradesco", "santander": "Santander",
        "nubank": "Nubank", "caixa": "Caixa", "c6": "C6", 
        "btg": "BTG Pactual", "picpay": "PicPay", "inter": "Inter"
    }

    gold_data = []
    
    # Mapeamento dinâmico de colunas do BCB
    c_inst = next(c for c in df_rank.columns if 'instituicao' in c.lower())
    c_idx = next(c for c in df_rank.columns if 'indice' in c.lower())
    c_cli = next(c for c in df_rank.columns if 'clientes' in c.lower())
    c_proc = next(c for c in df_rank.columns if 'procedentes' in c.lower())
    c_resp = next((c for c in df_rank.columns if 'respondidas' in c.lower()), None)
    
    for key, nome_exibicao in bancos_alvo.items():
        print(f"📈 Analisando: {nome_exibicao}...")
        
        termo_busca = "nu " if key == "nubank" else key
        m_rank = df_rank[df_rank[c_inst].str.contains(termo_busca, case=False, na=False)].iloc[0:1]
        
        if not m_rank.empty:
            # 1. Extração do Motivo Top (BCB)
            motivo_top = "Não informado no período"
            if not df_subjects.empty:
                c_sub_inst = next((c for c in df_subjects.columns if 'instituicao' in c.lower()), None)
                c_assunto = next((c for c in df_subjects.columns if 'assunto' in c.lower()), None)
                
                if c_sub_inst and c_assunto:
                    m_subs = df_subjects[df_subjects[c_sub_inst].str.contains(termo_busca, case=False, na=False)]
                    if not m_subs.empty:
                        motivo_top = m_subs[c_assunto].iloc[0]

            # 2. Processamento de Notícias e IA
            filtro_news = df_news[df_news['bank_clean'].str.contains(key, na=False)]
            manchetes = filtro_news['title'].tolist()
            resumo_ia = gerar_resumo_ia(nome_exibicao, manchetes)

            # 3. Consolidação (Mantendo ordem e nomes das colunas originais)
            val_proc = limpar_valor_bcb(m_rank[c_proc].values[0])
            val_resp = limpar_valor_bcb(m_rank[c_resp].values[0]) if c_resp else (val_proc * 1.3)

            gold_data.append({
                'bank': nome_exibicao,
                'qtd_noticias_recentes': len(filtro_news),
                'indice_bcb': limpar_valor_bcb(m_rank[c_idx].values[0]),
                'total_clientes': limpar_valor_bcb(m_rank[c_cli].values[0]),
                'recl_procedentes': val_proc,
                'total_respondidas': val_resp,
                'principal_motivo': motivo_top,
                'periodo': f"{df_rank['trimestre'].iloc[0]}T/{df_rank['ano'].iloc[0]}",
                # UPGRADE: Adicionado ao final sem alterar as colunas base
                'resumo_insight_ia': resumo_ia 
            })

    # Exportação Final
    df_final = pd.DataFrame(gold_data)
    df_final.to_csv("data/gold/fact_finvoc_summary.csv", index=False, decimal='.')
    
    print(f"✅ Camada Ouro consolidada: {len(gold_data)} bancos processados.")
    salvar_timestamp()

if __name__ == "__main__":
    executar_gold()