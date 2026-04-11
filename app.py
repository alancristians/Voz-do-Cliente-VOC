import streamlit as st
import pandas as pd
import plotly.express as px
import os

# Configuração da página para ocupar a tela toda
st.set_page_config(page_title="Voz do Cliente - VOC 2026", layout="wide", page_icon="📊")

# Estilização básica para manter o padrão profissional
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric { background-color: #161b22; padding: 15px; border-radius: 10px; border: 1px solid #30363d; }
    </style>
    """, unsafe_allow_html=True)

st.title("📊 Voz do Cliente (VOC) & Sentimento de Mercado")
st.markdown("Monitoramento consolidado de reclamações e notícias dos principais bancos.")

# --- CARREGAMENTO DOS DADOS ---
# Caminhos das camadas Bronze/Gold do projeto
PATH_REC = "data/bronze/reclamacoes.parquet"
PATH_NEWS = "data/bronze/noticias_bancos.parquet"

def load_data(path):
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame()

df_rec = load_data(PATH_REC)
df_news = load_data(PATH_NEWS)

# --- SIDEBAR DE FILTROS ---
st.sidebar.header("Configurações")
bancos_disponiveis = ["Todos"]
if not df_rec.empty:
    bancos_disponiveis.extend(sorted(df_rec['banco'].unique()))

banco_sel = st.sidebar.selectbox("Escolha a Instituição", bancos_disponiveis)

# --- FILTRAGEM ---
df_rec_filt = df_rec if banco_sel == "Todos" else df_rec[df_rec['banco'] == banco_sel]
df_news_filt = df_news if banco_sel == "Todos" else df_news[df_news['bank'] == banco_sel]

# =========================================================
# SEÇÃO 1: DASHBOARD DE RECLAMAÇÕES (VOC ORIGINAL)
# =========================================================
st.subheader("📈 Análise de Reclamações")

if not df_rec_filt.empty:
    # Métricas Principais
    c1, c2, c3 = st.columns(3)
    c1.metric("Total de Reclamações", len(df_rec_filt))
    
    # Se houver coluna de índice de reclamação ou similar
    top_banco = df_rec['banco'].value_counts().idxmax()
    c2.metric("Líder de Reclamações", top_banco)
    
    # Gráfico de Volume por Banco (Plotly)
    fig_vol = px.bar(
        df_rec_filt['banco'].value_counts().reset_index(),
        x='banco', y='count',
        title="Volume de Reclamações por Banco",
        labels={'count': 'Quantidade', 'banco': 'Instituição'},
        color='count', color_continuous_scale='Reds'
    )
    fig_vol.update_layout(template="plotly_dark", plot_bgcolor='rgba(0,0,0,0)')
    st.plotly_chart(fig_vol, use_container_width=True)
else:
    st.warning("Aguardando carregamento da base de reclamações...")

st.divider()

# =========================================================
# SEÇÃO 2: NOTÍCIAS (SENTIMENTO DE MERCADO)
# =========================================================
st.subheader("📰 Notícias e Fatos Relevantes")

if not df_news_filt.empty:
    # Garantir ordenação por data
    if 'published_dt' in df_news_filt.columns:
        df_news_filt = df_news_filt.sort_values(by='published_dt', ascending=False)

    # Grid de Notícias
    for _, row in df_news_filt.iterrows():
        with st.container(border=True):
            col_t, col_d = st.columns([4, 1])
            col_t.markdown(f"### {row['title']}")
            
            # Data formatada com segurança
            data_news = row['published'] if 'published' in row else "Recent"
            col_d.write(f"⏱️ {data_news}")
            
            st.markdown(f"**Fonte:** {row.get('source', 'N/A')} | **Banco:** {row.get('bank', 'N/A')}")
            
            # Exibição do resumo com proteção contra KeyError
            conteudo = row.get('summary', "Resumo indisponível no momento.")
            st.write(conteudo)
            
            st.link_button("Abrir notícia completa", row['link'])
else:
    st.info("Nenhuma notícia recente encontrada para os critérios selecionados.")