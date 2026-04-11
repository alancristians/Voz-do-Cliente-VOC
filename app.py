import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configuração e Estilo
st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")
st.markdown("<style>.stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }</style>", unsafe_allow_html=True)

st.title("🛡️ Voz do Cliente | Monitor de Reputação Bancária")

# Cores Institucionais
BANK_COLORS = {"Itau": "#EC7000", "Itaú": "#EC7000", "Bradesco": "#C8102E", "Santander": "#FF0000", "Nubank": "#820AD1", "Banco do Brasil": "#F8D117", "Caixa": "#005CA9"}

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
    # --- BANNER DE PERÍODO DINÂMICO ---
    periodo_ref = df['periodo'].iloc[0] if 'periodo' in df.columns else "Dados Recentes"
    st.info(f"📊 **Dados de Referência:** {periodo_ref} (Fonte: Banco Central do Brasil)")
    
    bancos = st.sidebar.multiselect("Filtrar Bancos:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. KPIs de Topo (Hover)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()), help="Notícias coletadas nos últimos 7 dias.")
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%", help="Quanto menor, melhor a resolução interna.")
    k4.metric("Market Share (Clientes)", f"{df_p['total_clientes'].sum()/1e6:.1f}M", help="Base total de clientes (CCS/SCR).")

    st.divider()

    # 5. Gráficos de Performance
    st.subheader("📊 Performance: Notícias vs. Reclamações")
    c1, c2 = st.columns(2)
    with c1: st.plotly_chart(px.bar(df_p, x='bank', y='qtd_noticias_recentes', color='bank', color_discrete_map=BANK_COLORS, template="plotly_dark", title="Volume de Exposição"), width="stretch")
    with c2: st.plotly_chart(px.line(df_p.sort_values('indice_bcb', ascending=False), x='bank', y='indice_bcb', markers=True, template="plotly_dark", title="Índice BCB"), width="stretch")

    st.divider()

    # 6. Escala e Eficiência
    st.subheader("🎯 Market Share e Eficácia Resolutiva")
    c3, c4 = st.columns(2)
    with c3: st.plotly_chart(px.pie(df_p, values='total_clientes', names='bank', hole=.4, color='bank', color_discrete_map=BANK_COLORS, template="plotly_dark", title="Market Share (Clientes)"), width="stretch")
    with c4: st.plotly_chart(px.bar(df_p, x='bank', y='taxa_procedencia', color='bank', color_discrete_map=BANK_COLORS, text_auto='.2f', template="plotly_dark", title="Taxa de Procedência (%)"), width="stretch")

    # 7. Diagnóstico e Explorador
    st.subheader("⚠️ Matriz de Diagnóstico VOC")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], width="stretch", hide_index=True)

    st.divider()
    st.subheader("🔍 Explorador de Notícias (Dados Brutos)")
    if os.path.exists(NEWS_DETAIL_PATH):
        df_news = pd.read_parquet(NEWS_DETAIL_PATH)
        search = st.text_input("Filtrar notícias:")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Link")}, width="stretch", hide_index=True)
else:
    st.error("❌ Dados não encontrados.")

st.sidebar.markdown("---")
st.sidebar.caption("Monitor de Reputação | Desenvolvido por Alan Cristian")