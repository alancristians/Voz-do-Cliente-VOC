import streamlit as st
import pandas as pd
import plotly.express as px
import os

# Configuração da Página
st.set_page_config(page_title="Voz do Cliente - VOC 2026", layout="wide", page_icon="📊")

st.title("📊 Voz do Cliente (VOC) - Monitoramento Bancário")
st.markdown("Monitoramento de reclamações (BACEN/Reclame Aqui) e notícias de mercado em tempo real.")

# --- 1. CARREGAMENTO DE DADOS (SEGURO) ---
def load_data(path):
    if os.path.exists(path):
        return pd.read_parquet(path)
    return pd.DataFrame()

df_reclamacoes = load_data("data/bronze/reclamacoes.parquet") # Caminho padrão do seu VOC
df_news = load_data("data/bronze/noticias_bancos.parquet")

# --- 2. SIDEBAR DE FILTROS ---
st.sidebar.header("Filtros de Análise")
lista_bancos = ["Todos"]
if not df_news.empty:
    lista_bancos.extend(df_news['bank'].unique())
elif not df_reclamacoes.empty:
    lista_bancos.extend(df_reclamacoes['banco'].unique())

banco_selecionado = st.sidebar.selectbox("Selecione o Banco", lista_bancos)

# Filtragem dos DataFrames
if banco_selecionado != "Todos":
    df_news_filt = df_news[df_news['bank'] == banco_selecionado] if not df_news.empty else df_news
    df_rec_filt = df_reclamacoes[df_reclamacoes['banco'] == banco_selecionado] if not df_reclamacoes.empty else df_reclamacoes
else:
    df_news_filt = df_news
    df_rec_filt = df_reclamacoes

# --- 3. DASHBOARD DE RECLAMAÇÕES (O QUE VOCÊ TINHA ANTES) ---
st.subheader("📈 Voz do Cliente (Reclamações)")

col1, col2, col3 = st.columns(3)

if not df_rec_filt.empty:
    total_rec = len(df_rec_filt)
    col1.metric("Total de Reclamações", total_rec)
    
    # Gráfico de barras que você tinha antes
    fig_rec = px.bar(df_rec_filt['banco'].value_counts().reset_index(), 
                     x='banco', y='count', title="Volume de Reclamações por Instituição",
                     labels={'count': 'Qtd Reclamações', 'banco': 'Banco'},
                     color_discrete_sequence=['#FF4B4B'])
    st.plotly_chart(fig_rec, use_container_width=True)
else:
    st.warning("Dados de reclamações (VOC) não encontrados no repositório.")

st.divider()

# --- 4. SEÇÃO DE NOTÍCIAS (A FUNCIONALIDADE NOVA) ---
st.subheader("📰 Sentimento de Mercado (Últimas Notícias)")

if not df_news_filt.empty:
    # Ordenar por data (segurança extra)
    if 'published_dt' in df_news_filt.columns:
        df_news_filt = df_news_filt.sort_values(by='published_dt', ascending=False)

    # Exibição em Cards
    for _, row in df_news_filt.iterrows():
        with st.container(border=True):
            c1, c2 = st.columns([4, 1])
            c1.markdown(f"### {row['title']}")
            
            # Data amigável
            data_str = row['published'] if 'published' in row else "Data indisponível"
            c2.write(f"⏱️ {data_str}")
            
            st.markdown(f"**Fonte:** {row.get('source', 'N/A')} | **Banco:** {row.get('bank', 'N/A')}")
            
            # --- PROTEÇÃO CONTRA KEYERROR NO SUMMARY ---
            # Se a coluna 'summary' não existir no parquet antigo, ele mostra uma mensagem padrão
            resumo = row.get('summary', "Resumo ainda não disponível (aguardando próxima atualização do robô).")
            st.write(resumo)
            
            st.link_button("Ver notícia original", row['link'])
else:
    st.info("Nenhuma notícia coletada para o filtro selecionado.")