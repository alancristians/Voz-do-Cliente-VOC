import streamlit as st
import pandas as pd
import plotly.express as px
import os

# Configuração de interface
st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")

st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🗣️ Voz do Cliente | Monitor de Reputação Bancária")
st.caption("Monitoramento de métricas oficiais (BCB/Consumidor.gov) e exposição mediática via Pipeline de Dados.")

# Identidade visual protegida
BANK_COLORS = {
    "Itau": "#EC7000", "Itaú": "#EC7000", "Bradesco": "#C8102E", 
    "Santander": "#FF0000", "Nubank": "#820AD1", "Banco do Brasil": "#F8D117", "Caixa": "#005CA9"
}

@st.cache_data
def load_data():
    path = "data/gold/fact_finvoc_summary.csv"
    if os.path.exists(path):
        df = pd.read_csv(path)
        for col in ['indice_bcb', 'total_clientes', 'recl_procedentes', 'total_respondidas']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = load_data()

if df is not None:
    periodo_ref = df['periodo'].iloc[0] if 'periodo' in df.columns else "Dados Recentes"
    st.info(f"📊 **Dados de Referência:** {periodo_ref}")
    
    bancos = st.sidebar.multiselect("Filtrar Instituições:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # KPIs principais
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()))
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%", help="Quanto menor, melhor a resolução interna.")
    k4.metric("Market Share (Clientes)", f"{df_p['total_clientes'].sum()/1e6:.1f}M")

    st.divider()

    # Gráficos de performance oficial
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.bar(df_p.sort_values('indice_bcb', ascending=False), 
                               x='bank', y='indice_bcb', color='bank', 
                               color_discrete_map=BANK_COLORS, template="plotly_dark", 
                               title="Índice de Reclamações (BCB)"), use_container_width=True)
    with c2:
        st.plotly_chart(px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                               color='bank', color_discrete_map=BANK_COLORS, 
                               template="plotly_dark", title="Volume de Clientes"), use_container_width=True)

    st.divider()

    # Matriz de diagnóstico
    st.subheader("⚠️ Matriz de Diagnóstico VOC")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], 
                 width="stretch", hide_index=True)

    # Explorador de Notícias estruturado
    st.divider()
    st.subheader("🔍 Explorador de Notícias")
    news_path = "data/silver/stg_noticias.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        search = st.text_input("Filtrar notícias (ex: Lucro, Pix, Fraude):")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Fonte")}, 
                     width="stretch", hide_index=True)
else:
    st.error("❌ Dados não encontrados.")

st.sidebar.markdown("---")
st.sidebar.caption("FinVoC 2.0 | Autor: Alan Cristian")