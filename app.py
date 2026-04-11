import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configuração e Estilo
st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")
st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🛡️ Voz do Cliente | Monitor de Reputação Bancária")
st.caption("Inteligência de Dados e Monitoramento de Experiência do Cliente via Pipeline Automatizado.")

# Cores Oficiais
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
    # Banner de Período
    periodo_ref = df['periodo'].iloc[0] if 'periodo' in df.columns else "Dados Recentes"
    st.info(f"📊 **Dados de Referência:** {periodo_ref} (Fonte: Banco Central do Brasil)")
    
    bancos = st.sidebar.multiselect("Filtrar Instituições:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. KPIs com Tooltips (Help)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()), 
              help="Volume total de menções na mídia nos últimos 7 dias via Google News.")
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%", 
              help="Proporção de reclamações que o BCB considerou procedentes. Quanto MENOR, melhor o banco resolve os problemas internamente.")
    k4.metric("Market Share (Clientes)", f"{df_p['total_clientes'].sum()/1e6:.1f}M", 
              help="Volume total de clientes ativos registrados no Banco Central (CCS/SCR).")

    st.divider()

    # 5. Seção de Performance com HELP
    st.subheader("📊 Performance: Notícias vs. Reclamações", 
                 help="Cruzamento entre o volume de notícias (sentimento externo) e o índice oficial de queixas por 1 milhão de clientes.")
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.bar(df_p, x='bank', y='qtd_noticias_recentes', color='bank', color_discrete_map=BANK_COLORS, template="plotly_dark", title="Volume de Exposição na Mídia"), width="stretch")
    with c2:
        st.plotly_chart(px.line(df_p.sort_values('indice_bcb', ascending=False), x='bank', y='indice_bcb', markers=True, template="plotly_dark", title="Índice de Reclamações (BCB)"), width="stretch")

    st.divider()

    # 6. Escala e Eficiência com HELP
    st.subheader("🎯 Market Share e Eficácia de Resolução", 
                 help="Mostra o tamanho do banco (volume de clientes) comparado à sua capacidade real de resolver problemas (Taxa de Procedência).")
    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(px.pie(df_p, values='total_clientes', names='bank', hole=.4, color='bank', color_discrete_map=BANK_COLORS, template="plotly_dark", title="Volume de Clientes"), width="stretch")
    with c4:
        st.plotly_chart(px.bar(df_p, x='bank', y='taxa_procedencia', color='bank', color_discrete_map=BANK_COLORS, text_auto='.2f', template="plotly_dark", title="Taxa de Procedência (%)"), width="stretch")

    # 7. Matriz com HELP
    st.subheader("⚠️ Matriz de Diagnóstico VOC", 
                 help="Visão analítica dos principais status de atendimento e eficiência coletados via pipeline.")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], width="stretch", hide_index=True)

    # 8. Explorador com HELP
    st.divider()
    st.subheader("🔍 Explorador de Notícias (Dados Brutos)", 
                 help="Busca textual detalhada em todas as notícias coletadas via Web Scraping para validar o sentimento do mercado.")
    if os.path.exists(NEWS_DETAIL_PATH):
        df_news = pd.read_parquet(NEWS_DETAIL_PATH)
        search = st.text_input("Filtrar notícias por termo:")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Fonte")}, width="stretch", hide_index=True)

else:
    st.error("❌ Dados da Camada Ouro não encontrados.")

st.sidebar.markdown("---")
st.sidebar.caption("Monitor de Reputação | Desenvolvido por Alan Cristian")