import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configuração de Página
st.set_page_config(page_title="FinVoC Dashboard", layout="wide", page_icon="📈")

# Estilo para cards de métricas
st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🛡️ FinVoC: Inteligência de Mercado & Reclamações")

# 2. Carga de Dados com Proteção de Colunas
GOLD_PATH = "data/gold/fact_finvoc_summary.csv"
NEWS_DETAIL_PATH = "data/bronze/noticias_bancos.parquet"

@st.cache_data
def load_data():
    if os.path.exists(GOLD_PATH):
        df = pd.read_csv(GOLD_PATH)
        # Converte para numérico e trata ausências para evitar erros de gráfico
        cols_numeric = ['recl_procedentes', 'total_respondidas', 'total_clientes', 'indice_bcb']
        for col in cols_numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            else:
                df[col] = 0
        
        # Cálculo de Eficiência (Taxa de Procedência)
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = load_data()

if df is not None:
    # 3. Sidebar
    bancos = st.sidebar.multiselect("Bancos:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. KPIs de Topo
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Total Notícias", int(df_p['qtd_noticias_recentes'].sum()))
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.1f}%")
    k4.metric("Market Share (Clientes)", f"{df_p['total_clientes'].sum()/1e6:.1f}M")

    st.divider()

    # 5. BLOCO ORIGINAL: Notícias e Índice BCB (Sintaxe atualizada)
    st.subheader("📊 Performance: Notícias vs. Reclamações")
    c1, c2 = st.columns(2)
    with c1:
        fig_bar = px.bar(df_p, x='bank', y='qtd_noticias_recentes', color='bank', template="plotly_dark", title="Volume de Notícias")
        st.plotly_chart(fig_bar, width="stretch")
    with c2:
        df_ord = df_p.sort_values(by='indice_bcb', ascending=False)
        fig_line = px.line(df_ord, x='bank', y='indice_bcb', markers=True, template="plotly_dark", title="Índice BCB (Menor é Melhor)")
        st.plotly_chart(fig_line, width="stretch")

    st.divider()

    # 6. BLOCO NOVOS INSIGHTS: Escala e Eficiência
    st.subheader("🎯 Insights de Escala e Resolução")
    c3, c4 = st.columns(2)
    with c3:
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', hole=.4, template="plotly_dark", title="Market Share (Clientes)")
        st.plotly_chart(fig_pie, width="stretch")
    with c4:
        fig_eff = px.bar(df_p, x='bank', y='taxa_procedencia', color='bank', text_auto='.1f', template="plotly_dark", title="Taxa de Procedência (%)")
        st.plotly_chart(fig_eff, width="stretch")

    # 7. Diagnóstico de Operação
    st.subheader("⚠️ Diagnóstico de Status")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], width="stretch", hide_index=True)

    # 8. Explorador de Notícias com Filtro Validado
    st.divider()
    st.subheader("🔍 Pesquisa Detalhada de Conteúdo")
    if os.path.exists(NEWS_DETAIL_PATH):
        df_news = pd.read_parquet(NEWS_DETAIL_PATH)
        search = st.text_input("Busca global nas notícias (termo, banco ou link):")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        
        st.dataframe(
            df_news, 
            column_config={"link": st.column_config.LinkColumn("Link Original")}, 
            width="stretch", 
            hide_index=True
        )
else:
    st.error("❌ Base de dados não encontrada. Verifique os arquivos Gold.")

st.sidebar.markdown("---")
st.sidebar.caption("FinVoC | Alan Cristian - Poli-USP")