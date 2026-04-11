import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configuração e Estilo
st.set_page_config(page_title="FinVoC Dashboard", layout="wide", page_icon="📈")
st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🛡️ FinVoC: Inteligência de Mercado & Reclamações")

# 2. Carga de Dados
DATA_PATH = "data/gold/fact_finvoc_summary.csv"
NEWS_PATH = "data/bronze/noticias_bancos.parquet"

@st.cache_data
def load_data():
    if os.path.exists(DATA_PATH):
        df = pd.read_csv(DATA_PATH)
        # Cálculo da Taxa de Procedência
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = load_data()

if df is not None:
    # 3. Sidebar e Filtros
    bancos = st.sidebar.multiselect("Bancos:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. KPIs de Topo (Escala e Eficiência)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos", len(df_p))
    k2.metric("Total Notícias", int(df_p['qtd_noticias_recentes'].sum()))
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.1f}%")
    k4.metric("Total Clientes", f"{df_p['total_clientes'].sum()/1e6:.1f}M")

    st.divider()

    # 5. Gráficos de Escala e Eficiência
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📊 Escala (Volume de Clientes)")
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', hole=.4, template="plotly_dark")
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        st.subheader("🎯 Eficiência da Ouvidoria (%)")
        st.caption("Quanto menor a taxa, melhor o banco resolve os problemas internamente.")
        fig_bar = px.bar(df_p, x='bank', y='taxa_procedencia', color='bank', text_auto='.1f', template="plotly_dark")
        st.plotly_chart(fig_bar, use_container_width=True)

    # 6. Tabela de Motivos (Status) e Detalhes
    st.subheader("⚠️ Diagnóstico de Status e Reclamações")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], use_container_width=True, hide_index=True)

    # 7. Explorador de Notícias (No final da página)
    st.divider()
    st.subheader("🔍 Pesquisa Detalhada de Notícias")
    if os.path.exists(NEWS_PATH):
        df_news = pd.read_parquet(NEWS_PATH)
        search = st.text_input("Digite um termo para buscar nas notícias:")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Link")}, use_container_width=True, hide_index=True)

else:
    st.error("❌ Base de dados Gold não encontrada. Execute o script run_gold.py primeiro.")

st.sidebar.markdown("---")
st.sidebar.caption("FinVoC | Alan Cristian - Poli-USP")