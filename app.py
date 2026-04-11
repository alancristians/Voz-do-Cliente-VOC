import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configuração da Página
st.set_page_config(
    page_title="FinVoC Dashboard", 
    layout="wide", 
    page_icon="📈"
)

# 2. Estilo CSS para os cards (Ajustado para o tema escuro do print)
st.markdown("""
    <style>
    .stMetric { 
        background-color: #1e1e1e; 
        padding: 15px; 
        border-radius: 10px; 
        border: 1px solid #333; 
    }
    [data-testid="stMetricValue"] { color: #ffffff !important; }
    [data-testid="stMetricLabel"] { color: #aaaaaa !important; }
    </style>
""", unsafe_allow_html=True)

st.title("🛡️ FinVoC: Voz do Cliente & Mercado")
st.markdown("Análise integrada de menções na mídia vs. indicadores oficiais do Banco Central.")

# 3. Caminho dos dados
DATA_PATH = "data/gold/fact_finvoc_summary.csv"

@st.cache_data
def load_data():
    if os.path.exists(DATA_PATH):
        df = pd.read_csv(DATA_PATH)
        # Identifica a coluna do índice do BCB
        idx_cols = [c for c in df.columns if 'indice' in c.lower() or 'índice' in c.lower()]
        if idx_cols:
            df = df.rename(columns={idx_cols[0]: 'indice_bcb'})
            df['indice_bcb'] = pd.to_numeric(df['indice_bcb'], errors='coerce').fillna(0)
        return df
    return None

df = load_data()

if df is not None:
    # 4. Sidebar com Filtros
    st.sidebar.header("⚙️ Configurações")
    bancos_disponiveis = df['bank'].unique()
    bancos = st.sidebar.multiselect(
        "Selecione os Bancos para comparar:", 
        options=bancos_disponiveis, 
        default=bancos_disponiveis
    )
    
    # Filtrando os dados
    df_plot = df[df['bank'].isin(bancos)]

    # 5. KPIs de Topo (Exatamente como no print)
    m1, m2, m3 = st.columns(3)
    m1.metric("Bancos Analisados", len(df_plot))
    m2.metric("Total de Notícias (Semana)", int(df_plot['qtd_noticias_recentes'].sum()))
    m3.metric("Média Índice BCB", f"{df_plot['indice_bcb'].mean():.2f}")

    st.divider()

    # 6. Gráficos Principais (Lado a Lado)
    col_esq, col_dir = st.columns(2)

    with col_esq:
        st.subheader("📰 Volume de Notícias (Google News)")
        fig_news = px.bar(
            df_plot, x='bank', y='qtd_noticias_recentes', 
            color='bank', text_auto=True,
            template="plotly_dark",
            labels={'bank': 'Banco', 'qtd_noticias_recentes': 'Nº de Notícias'}
        )
        st.plotly_chart(fig_news, use_container_width=True)

    with col_dir:
        st.subheader("📉 Índice de Reclamações (BCB 2025)")
        # Ordenação para manter o padrão visual do print
        df_ordenado = df_plot.sort_values(by='indice_bcb', ascending=False)
        fig_idx = px.line(
            df_ordenado, x='bank', y='indice_bcb', 
            markers=True, template="plotly_dark",
            labels={'bank': 'Banco', 'indice_bcb': 'Índice de Reclamações'}
        )
        st.plotly_chart(fig_idx, use_container_width=True)

    # 7. Tabela de Dados Brutos
    st.subheader("📄 Detalhamento da Camada Ouro")
    st.dataframe(df_plot, use_container_width=True)

    # 8. Glossário (Recuperado conforme solicitado)
    with st.expander("ℹ️ Entenda as Métricas (Glossário Técnico)"):
        st.markdown("""
        ### Como interpretar este Dashboard?
        
        **1. Volume de Notícias:**
        Ind