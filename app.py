import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configuração de Página e Identidade
st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")

# Estilo para cards e métricas
st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🛡️ Voz do Cliente | Monitor de Reputação Bancária")
st.caption("Solução analítica para monitoramento de marca, eficiência resolutiva e market share.")

# Paleta de cores oficial das instituições
BANK_COLORS = {
    "Itau": "#EC7000", "Itaú": "#EC7000",
    "Bradesco": "#C8102E", "Santander": "#FF0000",
    "Nubank": "#820AD1", "Banco do Brasil": "#F8D117", "Caixa": "#005CA9"
}

# 2. Carregamento e Tratamento de Dados
GOLD_PATH = "data/gold/fact_finvoc_summary.csv"
NEWS_DETAIL_PATH = "data/bronze/noticias_bancos.parquet"

@st.cache_data
def load_data():
    if os.path.exists(GOLD_PATH):
        df = pd.read_csv(GOLD_PATH)
        for col in ['indice_bcb', 'total_clientes', 'recl_procedentes', 'total_respondidas']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        # Cálculo da Taxa de Procedência (Eficiência)
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = load_data()

if df is not None:
    # 3. Filtros Laterais
    bancos = st.sidebar.multiselect("Filtrar Instituições:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. Painel de Indicadores Chave (KPIs) com Tooltips
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Exposição Midiática", int(df_p['qtd_noticias_recentes'].sum()), 
              help="Volume total de notícias coletadas nos últimos 7 dias.")
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%", 
              help="Proporção de queixas procedentes. Quanto MENOR, melhor a resolução interna do banco.")
    k4.metric("Total de Clientes", f"{df_p['total_clientes'].sum()/1e6:.1f}M", 
              help="Soma total de clientes ativos (CCS/SCR) para contextualizar a escala.")

    st.divider()

    # 5. Análise de Exposição e Reputação
    st.subheader("📊 Performance: Notícias vs. Reclamações", 
                 help="Cruzamento entre o volume de notícias (sentimento externo) e o índice oficial do BCB.")
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.bar(df_p, x='bank', y='qtd_noticias_recentes', color='bank', 
                               color_discrete_map=BANK_COLORS, template="plotly_dark", title="Volume de Exposição"), width="stretch")
    with c2:
        st.plotly_chart(px.line(df_p.sort_values('indice_bcb', ascending=False), x='bank', y='indice_bcb', 
                                markers=True, template="plotly_dark", title="Índice de Insatisfação (BCB)"), width="stretch")

    st.divider()

    # 6. Escala e Eficiência Operacional
    st.subheader("🎯 Market Share e Eficácia de Resolução", 
                 help="Visão da dominância de mercado comparada à precisão resolutiva da Ouvidoria.")
    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                               color='bank', color_discrete_map=BANK_COLORS, 
                               template="plotly_dark", title="Market Share (Volume de Clientes)"), width="stretch")
    with c4:
        st.plotly_chart(px.bar(df_p, x='bank', y='taxa_procedencia', color='bank', 
                               color_discrete_map=BANK_COLORS, text_auto='.2f', 
                               template="plotly_dark", title="Taxa de Procedência (%)"), width="stretch")

    # 7. Matriz Diagnóstica
    st.subheader("⚠️ Diagnóstico de Operação VOC", help="Visão analítica resumida por instituição financeira.")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], 
                 width="stretch", hide_index=True)

    # 8. Explorador de Dados Brutos
    st.divider()
    st.subheader("🔍 News Explorer (Raw Data)", help="Pesquisa textual em todas as notícias processadas no pipeline.")
    if os.path.exists(NEWS_DETAIL_PATH):
        df_news = pd.read_parquet(NEWS_DETAIL_PATH)
        search = st.text_input("Filtrar notícias por palavra-chave:")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Fonte da Notícia")}, 
                     width="stretch", hide_index=True)

else:
    st.error("❌ Erro de carregamento: Execute o pipeline de dados primeiro.")

st.sidebar.markdown("---")
st.sidebar.caption("Monitor de Reputação | Alan Cristian")