import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configuração de Layout
st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")

# Estilo para cards modernos
st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🛡️ Voz do Cliente | Monitor de Reputação Bancária")
st.caption("Solução analítica para monitoramento de marca, eficiência resolutiva e market share.")

# Cores oficiais das instituições
BANK_COLORS = {
    "Itau": "#EC7000", "Itaú": "#EC7000",
    "Bradesco": "#C8102E", "Santander": "#FF0000",
    "Nubank": "#820AD1", "Banco do Brasil": "#F8D117", "Caixa": "#005CA9"
}

# 2. Carga de Dados
GOLD_PATH = "data/gold/fact_finvoc_summary.csv"
NEWS_DETAIL_PATH = "data/bronze/noticias_bancos.parquet"

@st.cache_data
def load_data():
    if os.path.exists(GOLD_PATH):
        df = pd.read_csv(GOLD_PATH)
        for col in ['indice_bcb', 'total_clientes', 'recl_procedentes', 'total_respondidas']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = load_data()

if df is not None:
    # 3. Sidebar
    bancos = st.sidebar.multiselect("Filtrar Instituições:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. KPIs Estratégicos com Tooltips (Help)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()), 
              help="Volume total de menções na mídia nos últimos 7 dias.")
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%", 
              help="Taxa de Reclamações Procedentes (BCB). Quanto MENOR o %, melhor o banco resolve os problemas.")
    k4.metric("Total de Clientes", f"{df_p['total_clientes'].sum()/1e6:.1f}M", 
              help="Base total de clientes ativos registrados no Banco Central (CCS/SCR).")

    st.divider()

    # 5. Seção de Performance
    st.subheader("📊 Performance: Notícias vs. Reclamações", 
                 help="Cruzamento entre o volume de notícias (exposição) e o índice oficial de queixas (BCB).")
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.bar(df_p, x='bank', y='qtd_noticias_recentes', color='bank', 
                               color_discrete_map=BANK_COLORS, template="plotly_dark", title="Volume de Exposição na Mídia"), width="stretch")
    with c2:
        st.plotly_chart(px.line(df_p.sort_values('indice_bcb', ascending=False), x='bank', y='indice_bcb', 
                                markers=True, template="plotly_dark", title="Índice de Reclamações (BCB)"), width="stretch")

    st.divider()

    # 6. Escala vs. Eficiência
    st.subheader("🎯 Market Share e Eficácia Resolutiva", 
                 help="Mostra o tamanho do banco versus sua capacidade de resolver reclamações fundamentadas.")
    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                               color='bank', color_discrete_map=BANK_COLORS, 
                               template="plotly_dark", title="Market Share (Volume de Clientes)"), width="stretch")
    with c4:
        st.plotly_chart(px.bar(df_p, x='bank', y='taxa_procedencia', color='bank', 
                               color_discrete_map=BANK_COLORS, text_auto='.2f', 
                               template="plotly_dark", title="Taxa de Procedência (%)"), width="stretch")

    # 7. Diagnóstico Operacional
    st.subheader("⚠️ Matriz de Diagnóstico VOC", help="Visão analítica dos principais status de atendimento e eficiência.")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], 
                 width="stretch", hide_index=True)

    # 8. Explorador de Notícias
    st.divider()
    st.subheader("🔍 Explorador de Notícias (Dados Brutos)", help="Pesquisa detalhada em todas as notícias coletadas via Web Scraping.")
    if os.path.exists(NEWS_DETAIL_PATH):
        df_news = pd.read_parquet(NEWS_DETAIL_PATH)
        search = st.text_input("Busca por termo ou banco:")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Fonte da Notícia")}, 
                     width="stretch", hide_index=True)

else:
    st.error("❌ Erro ao carregar dados. Verifique a Camada Gold.")

st.sidebar.markdown("---")
st.sidebar.caption("Monitor de Reputação | Desenvolvido por Alan Cristian")