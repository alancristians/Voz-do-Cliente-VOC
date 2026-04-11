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

# Definição das cores solicitadas (Hexadecimais)
BANK_COLORS = {
    "Itau": "#EC7000",      # Laranja Itaú
    "Itaú": "#EC7000",      
    "Bradesco": "#C8102E",   # Vermelho Bradesco (mais fechado)
    "Santander": "#FF0000",  # Vermelho Vivo Santander
    "Nubank": "#820AD1",     # Roxo Nubank
    "Banco do Brasil": "#F8D117", # Amarelo BB
    "Caixa": "#005CA9"       # Azul Caixa
}

# Glossário
with st.expander("ℹ️ Guia Técnico: Como interpretar os indicadores?"):
    st.markdown("""
    * **Média Eficiência:** Porcentagem de queixas procedentes. Quanto **menor**, melhor.
    * **Market Share:** Volume total de clientes ativos (CCS/SCR).
    """)

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
    bancos = st.sidebar.multiselect("Bancos:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Total Notícias", int(df_p['qtd_noticias_recentes'].sum()))
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%")
    k4.metric("Market Share", f"{df_p['total_clientes'].sum()/1e6:.1f}M")

    st.divider()

    # 4. Gráficos de Performance (Com cores travadas)
    st.subheader("📊 Performance: Notícias vs. Reclamações")
    c1, c2 = st.columns(2)
    with c1:
        fig_bar = px.bar(df_p, x='bank', y='qtd_noticias_recentes', 
                         color='bank', color_discrete_map=BANK_COLORS,
                         template="plotly_dark", title="Volume de Notícias")
        st.plotly_chart(fig_bar, width="stretch")
    with c2:
        # Gráfico de linha usa uma cor fixa ou pode ser mapeado se houver mais linhas
        fig_line = px.line(df_p.sort_values('indice_bcb', ascending=False), x='bank', y='indice_bcb', 
                           markers=True, template="plotly_dark", title="Índice BCB")
        fig_line.update_traces(line_color='#00ffcc') 
        st.plotly_chart(fig_line, width="stretch")

    st.divider()

    # 5. Novos Gráficos (Escala e Resolução)
    st.subheader("🎯 Insights de Escala e Resolução")
    c3, c4 = st.columns(2)
    with c3:
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', 
                         hole=.4, color='bank', color_discrete_map=BANK_COLORS,
                         template="plotly_dark", title="Market Share (Clientes)")
        st.plotly_chart(fig_pie, width="stretch")
    with c4:
        fig_eff = px.bar(df_p, x='bank', y='taxa_procedencia', 
                         color='bank', color_discrete_map=BANK_COLORS,
                         text_auto='.2f', template="plotly_dark", title="Taxa de Procedência (%)")
        st.plotly_chart(fig_eff, width="stretch")

    # 6. Diagnóstico e Explorador
    st.subheader("⚠️ Diagnóstico de Operação")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], width="stretch", hide_index=True)

    st.divider()
    st.subheader("🔍 Explorador Detalhado de Notícias")
    if os.path.exists(NEWS_DETAIL_PATH):
        df_news = pd.read_parquet(NEWS_DETAIL_PATH)
        search = st.text_input("Filtrar notícias por termo:")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Link")}, width="stretch", hide_index=True)

else:
    st.error("❌ Dados não encontrados.")

st.sidebar.markdown("---")
st.sidebar.caption("FinVoC | Alan Cristian - Poli-USP")