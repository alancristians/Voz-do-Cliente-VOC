import pandas as pd
import os
import unicodedata
import glob
import pytz
from datetime import datetime
from dotenv import load_dotenv
from groq import Groq

load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))
MODELO_GROQ = "llama-3.1-8b-instant"

def normalizar_chave(texto):
    if not isinstance(texto, str): return ""
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower().strip()

def limpar_valor_bcb(v):
    if pd.isna(v) or v == "" or v == " ": return 0.0
    try:
        v_str = str(v).strip()
        if "," in v_str:
            v_str = v_str.replace('.', '').replace(',', '.')
        return float(v_str)
    except:
        return 0.0

def gerar_resumo_ia(banco, manchetes):
    if not manchetes:
        return "**Sem Fatos Relevantes**\nNenhuma menção crítica mapeada na mídia para esta instituição no mês vigente."
    
    texto_input = "\n".join([f"- {m}" for m in manchetes[:10]])
    prompt = f"""
    Atue como um Analista Sênior de Customer Experience (CX) e Inteligência de Mercado no setor financeiro.
    Sua tarefa é analisar as notícias recentes do banco {banco} e consolidar um resumo executivo de reputação.

    DIRETRIZES DE ANÁLISE:
    1. Identifique os principais movimentos (lançamentos, parcerias, sanções ou reclamações).
    2. Explique brevemente o contexto de cada ponto.

    REGRAS RÍGIDAS DE FORMATO (OBRIGATÓRIO):
    - Retorne EXATAMENTE 3 tópicos distintos.
    - Cada tópico DEVE começar com o título em negrito no formato exato: **Título Curto Aqui**
    - Logo abaixo do título, escreva o parágrafo explicativo da análise na mesma linha ou na linha seguinte.
    - Separe os 3 tópicos usando uma linha em branco entre eles.
    - NÃO inclua a palavra 'CONTEÚDO:' ou 'TÍTULO:'. Retorne apenas os blocos de texto limpos.

    Notícias para análise:
    {texto_input}
    """
    try:
        completion = client.chat.completions.create(
            model=MODELO_GROQ,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=600,
            timeout=15
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        return "**API Indisponível**\nInsight de IA temporariamente indisponível devido a instabilidade externa."

def salvar_timestamp():
    os.makedirs("data/gold", exist_ok=True)
    sp_tz = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(sp_tz).strftime('%d/%m/%Y %H:%M')
    with open("data/gold/last_update.txt", "w") as f:
        f.write(agora)

def executar_gold():
    print("🥇 Iniciando processamento da camada Gold...")
    os.makedirs("data/gold", exist_ok=True)
    os.makedirs("data/silver", exist_ok=True)
    
    p_rank = "data/silver/stg_bcb_ranking.parquet"
    p_news = "data/silver/stg_noticias.parquet"
    
    # Localização flexível do CSV de assuntos do BCB
    p_subjects = "stg_assuntos_ranking.csv"
    if not os.path.exists(p_subjects) and os.path.exists("data/silver/stg_assuntos_ranking.csv"):
        p_subjects = "data/silver/stg_assuntos_ranking.csv"

    if not os.path.exists(p_rank) or not os.path.exists(p_news):
        print("❌ Erro de Dependência: Arquivos base da camada Silver ausentes.")
        return

    df_rank = pd.read_parquet(p_rank)
    df_news = pd.read_parquet(p_news)
    
    df_subjects = pd.DataFrame()
    if os.path.exists(p_subjects):
        try:
            df_subjects = pd.read_csv(p_subjects, encoding='latin-1', sep=';', on_bad_lines='skip')
            df_subjects = df_subjects.loc[:, ~df_subjects.columns.str.contains('^Unnamed')]
            
            # Garante cópia sincronizada na pasta silver para o dashboard acessar perfeitamente
            df_subjects.to_csv("data/silver/stg_assuntos_ranking.csv", index=False, encoding='latin-1', sep=';')
            print("📂 Dados de assuntos carregados e sincronizados na Silver com sucesso.")
        except Exception as e:
            print(f"⚠️ Erro ao ler/sincronizar CSV de assuntos: {e}")

    df_news['published_dt'] = pd.to_datetime(df_news['published'], errors='coerce')
    limite_15d = datetime.now() - pd.Timedelta(days=15)
    df_news_filtered = df_news[df_news['published_dt'].dt.tz_localize(None) >= limite_15d].copy()
    df_news_filtered['bank_clean'] = df_news_filtered['bank'].apply(normalizar_chave)

    bancos_alvo = {
        "itau": "Itaú", "bradesco": "Bradesco", "santander": "Santander",
        "nubank": "Nubank", "caixa": "Caixa", "c6": "C6", 
        "btg": "BTG Pactual", "picpay": "PicPay", "inter": "Inter"
    }

    gold_data = []
    c_inst = next(c for c in df_rank.columns if 'instituicao' in c.lower())
    c_idx = next(c for c in df_rank.columns if 'indice' in c.lower())
    c_cli = next(c for c in df_rank.columns if 'clientes' in c.lower())
    c_proc = next(c for c in df_rank.columns if 'procedentes' in c.lower())
    c_resp = next((c for c in df_rank.columns if 'respondidas' in c.lower()), None)
    
    for key, nome_exibicao in bancos_alvo.items():
        termo_busca = "nu " if key == "nubank" else key
        m_rank = df_rank[df_rank[c_inst].str.contains(termo_busca, case=False, na=False)].iloc[0:1]
        
        if not m_rank.empty:
            motivo_top = "Não informado no período"
            if not df_subjects.empty:
                c_sub_inst = 'Instituição financeira'
                c_assunto = 'Irregularidade'
                c_qtd_assunto = 'Quantidade de reclamações procedentes'
                
                if c_sub_inst in df_subjects.columns and c_assunto in df_subjects.columns:
                    m_subs = df_subjects[df_subjects[c_sub_inst].str.contains(termo_busca, case=False, na=False)]
                    if not m_subs.empty:
                        if c_qtd_assunto in m_subs.columns:
                            m_subs = m_subs.sort_values(by=c_qtd_assunto, ascending=False)
                        motivo_top = m_subs[c_assunto].iloc[0]

            if key == "inter":
                filtro_news = df_news_filtered[df_news_filtered['bank_clean'] == "inter"]
            else:
                filtro_news = df_news_filtered[df_news_filtered['bank_clean'].str.contains(key, na=False)]
                
            manchetes = filtro_news['title'].tolist()
            resumo_ia = gerar_resumo_ia(nome_exibicao, manchetes)

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
                'resumo_insight_ia': resumo_ia 
            })

    df_final = pd.DataFrame(gold_data)
    df_final.to_csv("data/gold/fact_finvoc_summary.csv", index=False, decimal='.')
    print(f"✅ Camada Ouro executada com sucesso. {len(gold_data)} registros consolidados.")
    salvar_timestamp()

if __name__ == "__main__":
    executar_gold()