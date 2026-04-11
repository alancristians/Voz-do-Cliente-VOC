import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configuração da Página
st.set_page_config(page_title="FinVoC Dashboard", layout="wide", page_icon="📈")

# Estilo CSS (Tema Escuro)
st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🛡️ FinVoC: Inteligência de Mercado & Reclamações")

# 2. Carga de Dados
GOLD_PATH = "data/gold/fact_finvoc_summary.csv"
NEWS_DETAIL_PATH = "data/bronze/noticias_bancos.parquet"

@st.cache_data
def load_gold():
    if os.path.exists(GOLD_PATH):
        df = pd.read_csv(GOLD_PATH)
        # Proteção contra erros de coluna
        if 'recl_procedentes' in df.columns and 'total_respondidas' in df.columns:
            df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        else:
            df['taxa_procedencia'] = 0
        return df
    return None

df = load_gold()

if df is not None:
    # 3. Sidebar
    bancos = st.sidebar.multiselect("Bancos:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. KPIs de Topo
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Total Notícias", int(df_p['qtd_noticias_recentes'].sum()))
    k3.metric("Eficiência Média", f"{df_p['taxa_procedencia'].mean():.1f}%")
    k4.metric("Market Share (Clientes)", f"{df_p['total_clientes'].sum()/1e6:.1f}M")

    st.divider()

    # 5. BLOCO ORIGINAL: Notícias e Índice BCB (O que já funcionava)
    st.subheader("📊 Performance: Notícias vs. Reclamações")
    c_orig1, c_orig2 = st.columns(2)
    with c_orig1:
        fig_bar = px.bar(df_p, x='bank', y='qtd_noticias_recentes', color='bank', template="plotly_dark", title="Volume de Notícias")
        st.plotly_chart(fig_bar, use_container_width=True)
    with c_orig2:
        df_ord = df_p.sort_values(by='indice_bcb', ascending=False)
        fig_line = px.line(df_ord, x='bank', y='indice_bcb', markers=True, template="plotly_dark", title="Índice BCB (Menor é Melhor)")
        st.plotly_chart(fig_line, use_container_width=True)

    st.divider()

    # 6. NOVO BLOCO: Escala e Eficiência
    st.subheader("🎯 Insights de Escala e Resolução")
    c_new1, c_new2 = st.columns(2)
    with c_new1:
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', hole=.4, template="plotly_dark", title="Market Share (Clientes)")
        st.plotly_chart(fig_pie, use_container_width=True)
    with c_new2:
        fig_eff = px.bar(df_p, x='bank', y='taxa_procedencia', color='bank', text_auto='.1f', template="plotly_dark", title="Taxa de Procedência (%)")
        st.plotly_chart(fig_eff, use_container_width=True)

    # 7. Diagnóstico de Status
    st.subheader("⚠️ Diagnóstico de Operação")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], use_container_width=True, hide_index=True)

    # 8. Explorador de Notícias (O que já funcionava)
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
    st.error("❌ Execute 'python scripts/gold_analysis.py' localmente primeiro.")

st.sidebar.markdown("---")
st.sidebar.caption("FinVoC | Alan Cristian - Poli-USP")