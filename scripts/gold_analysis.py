import pandas as pd
import os
import unicodedata
import glob
import pytz
from datetime import datetime
from dotenv import load_dotenv
from groq import Groq

# --- CONFIGURAÇÕES DE AMBIENTE E INSTANCIAÇÃO ---
load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def normalizar_chave(texto):
    """Normaliza strings removendo acentuações e padronizando caixa baixa para joins lógicos."""
    if not isinstance(texto, str): return ""
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower().strip()

def limpar_valor_bcb(v):
    """Trata anomalias e converte strings numéricas estruturadas do BCB para float estável."""
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
    Consome os inputs textuais selecionados e invoca a API do Groq Cloud Inference (Llama 3.3).
    Implementa isolamento de exceções (Fail-Safe) para resiliência do pipeline.
    """
    if not manchetes:
        return "🔹 Sem fatos relevantes ou menções críticas mapeadas na mídia para esta instituição no mês vigente."
    
    # Restrição física de amostragem para otimização de tokens (Janela de Contexto)
    texto_input = "\n".join([f"- {m}" for m in manchetes[:10]])
    
    # REFINAMENTO DE PROMPT: Foco em quebra de linha estruturada e profundidade analítica
    prompt = f"""
    Atue como um Analista Sênior de Customer Experience (CX) e Inteligência de Mercado no setor financeiro.
    Sua tarefa é analisar as notícias recentes do banco {banco} e consolidar um resumo executivo de reputação.

    DIRETRIZES DE ANÁLISE:
    1. Identifique os principais movimentos (ex: lançamentos, parcerias, sanções ou reclamações operacionais).
    2. Explique brevemente o contexto de cada ponto em vez de apenas listar o título da notícia.

    ESTRUTURA E REGRAS DE FORMATO (OBRIGATÓRIO):
    - Retorne EXATAMENTE 3 tópicos distintos.
    - Comece cada tópico OBRIGATORIAMENTE com o emoji '🔹 '.
    - Dê uma QUEBRA DE LINHA dupla (pressione Enter duas vezes) entre cada um dos tópicos para que eles não fiquem colados.
    - NÃO adicione saudações, introduções ou textos como "Aqui está o resumo". Retorne apenas os tópicos.

    Notícias para análise:
    {texto_input}
    """
    
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3, # Subimos levemente para ele contextualizar melhor
            max_tokens=600
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        print(f"⚠️ Alerta IA: Falha ao gerar insight preditivo para {banco}. Erro: {str(e)}")
        return "🔹 Insight de IA temporariamente indisponível devido a instabilidade na API externa."

def salvar_timestamp():
    """Garante a persistência do metadado de controle cronológico de execução (last_update)."""
    os.makedirs("data/gold", exist_ok=True)
    sp_tz = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(sp_tz).strftime('%d/%m/%Y %H:%M')
    with open("data/gold/last_update.txt", "w") as f:
        f.write(agora)
    print(f"✅ Execução finalizada e timestamp de governança salvo: {agora}")

def executar_gold():
    """
    Orquestra o fechamento da camada Gold (Data Mart). Consome as visões tratadas na Silver,
    aplica filtros de competência temporal e enriquece o modelo dimensional com IA Generativa.
    """
    print("🥇 Iniciando processamento e enriquecimento da camada Gold...")
    os.makedirs("data/gold", exist_ok=True)
    
    # Mapeamento físico dos Data Assets de entrada (Silver e Bronze)
    p_rank = "data/silver/stg_bcb_ranking.parquet"
    p_news = "data/silver/stg_noticias.parquet"
    
    # Correção do path para consistência com o script silver_transformation.py
    p_subjects_manual = "data/silver/stg_bcb_assuntos.csv"
    p_subjects_auto = sorted(glob.glob("data/bronze/assuntos_bcb_*.parquet"))
    
    # Cláusula de barreira para integridade referencial
    if not os.path.exists(p_rank) or not os.path.exists(p_news):
        print("❌ Erro de Dependência: Arquivos base da camada Silver ausentes. Abortando execução.")
        return

    # Ingestão das tabelas de Staging
    df_rank = pd.read_parquet(p_rank)
    df_news = pd.read_parquet(p_news)
    
    if os.path.exists(p_subjects_manual):
        df_subjects = pd.read_csv(p_subjects_manual)
        print("📂 Carregando dados qualitativos de reclamações via Silver Schema.")
    elif p_subjects_auto:
        df_subjects = pd.read_parquet(p_subjects_auto[-1])
        print(f"📂 Carregando dados qualitativos via partição Bronze: {p_subjects_auto[-1]}")
    else:
        df_subjects = pd.DataFrame()
        print("⚠️ Aviso: Fonte qualitativa de reclamações não localizada.")
    
    # -------------------------------------------------------------------------
    # REGRA DE NEGÓCIO: ISOLAMENTO TEMPORAL NA GOLD (15 DIAS MÓVEIS)
    # Garante que a volumetria da fato e o input da IA reflitam estritamente
    # a janela móvel de duas semanas configurada na camada de apresentação.
    # -------------------------------------------------------------------------
    df_news['published_dt'] = pd.to_datetime(df_news['published'], errors='coerce')
    limite_15d = datetime.now() - pd.Timedelta(days=15)
    
    df_news_filtered = df_news[df_news['published_dt'].dt.tz_localize(None) >= limite_15d].copy()
    df_news_filtered['bank_clean'] = df_news_filtered['bank'].apply(normalizar_chave)

    # Malha dimensional de instituições financeiras monitoradas
    bancos_alvo = {
        "itau": "Itaú", "bradesco": "Bradesco", "santander": "Santander",
        "nubank": "Nubank", "caixa": "Caixa", "c6": "C6", 
        "btg": "BTG Pactual", "picpay": "PicPay", "inter": "Inter"
    }

    gold_data = []
    
    # Mapeamento estrutural dinâmico de colunas do Banco Central (Agnóstico a mudanças de Schema)
    c_inst = next(c for c in df_rank.columns if 'instituicao' in c.lower())
    c_idx = next(c for c in df_rank.columns if 'indice' in c.lower())
    c_cli = next(c for c in df_rank.columns if 'clientes' in c.lower())
    c_proc = next(c for c in df_rank.columns if 'procedentes' in c.lower())
    c_resp = next((c for c in df_rank.columns if 'respondidas' in c.lower()), None)
    
    for key, nome_exibicao in bancos_alvo.items():
        print(f"📈 Consolidando indicadores: {nome_exibicao}...")
        
        termo_busca = "nu " if key == "nubank" else key
        m_rank = df_rank[df_rank[c_inst].str.contains(termo_busca, case=False, na=False)].iloc[0:1]
        
        if not m_rank.empty:
            # 1. Identificação do Principal Ofensor de CX (BCB)
            motivo_top = "Não informado no período"
            if not df_subjects.empty:
                c_sub_inst = next((c for c in df_subjects.columns if 'instituicao' in c.lower() or 'institui' in c.lower()), None)
                c_assunto = next((c for c in df_subjects.columns if 'assunto' in c.lower() or 'irregularidade' in c.lower()), None)
                
                if c_sub_inst and c_assunto:
                    m_subs = df_subjects[df_subjects[c_sub_inst].str.contains(termo_busca, case=False, na=False)]
                    if not m_subs.empty:
                        motivo_top = m_subs[c_assunto].iloc[0]

            # 2. Processamento Estrito de Notícias por Chave Única (Evita falsos positivos como 'internet')
            if key == "inter":
                filtro_news = df_news_filtered[df_news_filtered['bank_clean'] == "inter"]
            else:
                filtro_news = df_news_filtered[df_news_filtered['bank_clean'].str.contains(key, na=False)]
                
            manchetes = filtro_news['title'].tolist()
            
            # Enriquecimento analítico via IA
            resumo_ia = gerar_resumo_ia(nome_exibicao, manchetes)

            # 3. Consolidação Dimensional (Contrato de Schema da Fato)
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

    # Exportação física e estruturada da Tabela Fato (Gold Asset)
    df_final = pd.DataFrame(gold_data)
    df_final.to_csv("data/gold/fact_finvoc_summary.csv", index=False, decimal='.')
    
    print(f"✅ Camada Ouro executada com sucesso. {len(gold_data)} dimensões consolidadas.")
    salvar_timestamp()

if __name__ == "__main__":
    executar_gold()