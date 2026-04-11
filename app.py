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
st.caption("Passe o mouse sobre os títulos e números para entender as métricas.")

# Cores das Marcas
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
    bancos = st.sidebar.multiselect("Bancos:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. KPIs de Topo com HELP (Tooltips)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Total Notícias", int(df_p['qtd_noticias_recentes'].sum()), 
              help="Contagem total de menções na mídia nos últimos 7 dias.")
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%", 
              help="Porcentagem de queixas que o BCB considerou procedentes. Quanto MENOR, mais eficiente o banco.")
    k4.metric("Market Share (Clientes)", f"{df_p['total_clientes'].sum()/1e6:.1f}M", 
              help="Volume total de clientes ativos registrados no CCS/SCR.")

    st.divider()

    # 5. Performance (Help nos Subheaders)
    st.subheader("📊 Performance: Notícias vs. Reclamações", 
                 help="Cruzamento entre o volume midiático e o índice oficial de queixas por 1 milhão de clientes.")
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.bar(df_p, x='bank', y='qtd_noticias_recentes', color='bank', 
                               color_discrete_map=BANK_COLORS, template="plotly_dark", title="Volume de Notícias"), width="stretch")
    with c2:
        st.plotly_chart(px.line(df_p.sort_values('indice_bcb', ascending=False), x='bank', y='indice_bcb', 
                                markers=True, template="plotly_dark", title="Índice BCB (Ranking de Insatisfação)"), width="stretch")

    st.divider()

    # 6. Insights de Escala e Resolução
    st.subheader("🎯 Insights de Escala e Resolução", 
                 help="Market Share em volume de clientes e a eficácia real das ouvidorias (Taxa de Procedência).")
    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                               color='bank', color_discrete_map=BANK_COLORS, 
                               template="plotly_dark", title="Market Share (Clientes)"), width="stretch")
    with c4:
        st.plotly_chart(px.bar(df_p, x='bank', y='taxa_procedencia', color='bank', 
                               color_discrete_map=BANK_COLORS, text_auto='.2f', 
                               template="plotly_dark", title="Taxa de Procedência (%)"), width="stretch")

    # 7. Tabela de Diagnóstico
    st.subheader("⚠️ Diagnóstico de Operação", 
                 help="Resumo analítico com o principal status identificado na camada bronze.")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], 
                 width="stretch", hide_index=True)

    # 8. Explorador de Notícias
    st.divider()
    st.subheader("🔍 Explorador Detalhado de Notícias", 
                 help="Busque por palavras-chave em todos os títulos e resumos das notícias coletadas.")
    if os.path.exists(NEWS_DETAIL_PATH):
        df_news = pd.read_parquet(NEWS_DETAIL_PATH)
        search = st.text_input("Digite um termo para filtrar as notícias:")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Link")}, 
                     width="stretch", hide_index=True)

else:
    st.error("❌ Dados não encontrados.")

st.sidebar.markdown("---")
st.sidebar.caption("FinVoC | Alan Cristian - Poli-USP")