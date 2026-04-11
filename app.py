import streamlit as st
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="FinVoC Dashboard", layout="wide", page_icon="📈")
st.markdown("<style>.stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }</style>", unsafe_allow_html=True)
st.title("🛡️ FinVoC: Inteligência de Mercado & Reclamações")

# Carga de Dados
GOLD_PATH = "data/gold/fact_finvoc_summary.csv"
NEWS_DETAIL_PATH = "data/bronze/noticias_bancos.parquet"

@st.cache_data
def load_data():
    if os.path.exists(GOLD_PATH):
        df = pd.read_csv(GOLD_PATH)
        # Garante que as colunas numéricas existam e sejam tratadas
        cols_calc = ['recl_procedentes', 'total_respondidas', 'total_clientes', 'indice_bcb']
        for col in cols_calc:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                df[col] = 0 # Cria coluna zerada se não existir na Gold
        
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = load_data()

if df is not None:
    bancos = st.sidebar.multiselect("Bancos:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Total Notícias", int(df_p['qtd_noticias_recentes'].sum()))
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.1f}%")
    k4.metric("Total Clientes", f"{df_p['total_clientes'].sum()/1e6:.1f}M")

    st.divider()

    # BLOCO 1: Gráficos Originais (O que você validou)
    st.subheader("📊 Performance: Notícias vs. Reclamações")
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.bar(df_p, x='bank', y='qtd_noticias_recentes', color='bank', template="plotly_dark", title="Volume de Notícias"), use_container_width=True)
    with c2:
        st.plotly_chart(px.line(df_p.sort_values('indice_bcb', ascending=False), x='bank', y='indice_bcb', markers=True, template="plotly_dark", title="Índice BCB (Menor é Melhor)"), use_container_width=True)

    # BLOCO 2: Novos Insights
    st.divider()
    st.subheader("🎯 Insights de Escala e Resolução")
    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(px.pie(df_p, values='total_clientes', names='bank', hole=.4, template="plotly_dark", title="Market Share (Clientes)"), use_container_width=True)
    with c4:
        st.plotly_chart(px.bar(df_p, x='bank', y='taxa_procedencia', color='bank', text_auto='.1f', template="plotly_dark", title="Taxa de Procedência (%)"), use_container_width=True)

    # BLOCO 3: Diagnóstico e Notícias (Filtro validado)
    st.subheader("⚠️ Diagnóstico de Operação")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("🔍 Explorador Detalhado de Conteúdo")
    if os.path.exists(NEWS_DETAIL_PATH):
        df_news = pd.read_parquet(NEWS_DETAIL_PATH)
        search = st.text_input("Pesquisar termo nas notícias:")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Link")}, use_container_width=True, hide_index=True)
else:
    st.error("❌ Base de dados não encontrada.")