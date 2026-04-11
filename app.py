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

# 2. Estilo CSS para os cards (Tema Escuro preservado)
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
    
    df_plot = df[df['bank'].isin(bancos)]

    # 5. KPIs de Topo
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
            template="plotly_dark"
        )
        st.plotly_chart(fig_news, use_container_width=True)

    with col_dir:
        st.subheader("📉 Índice de Reclamações (BCB 2025)")
        df_ordenado = df_plot.sort_values(by='indice_bcb', ascending=False)
        fig_idx = px.line(
            df_ordenado, x='bank', y='indice_bcb', 
            markers=True, template="plotly_dark"
        )
        st.plotly_chart(fig_idx, use_container_width=True)

    st.divider()

    # 7. NOVA SEÇÃO: Busca e Conteúdo de Notícias
    st.subheader("🔍 Explorador de Conteúdo")
    search_term = st.text_input("Pesquise por banco, palavra-chave ou termo nas notícias:", "")

    # Filtro dinâmico: busca em todas as colunas de texto (bank e summary/title se existirem)
    if search_term:
        # Criamos uma máscara que verifica se o termo está em qualquer coluna de texto
        mask = df_plot.apply(lambda row: row.astype(str).str.contains(search_term, case=False).any(), axis=1)
        df_filtered = df_plot[mask]
    else:
        df_filtered = df_plot

    st.write(f"Exibindo {len(df_filtered)} registros encontrados:")
    st.dataframe(df_filtered, use_container_width=True)

    # 8. Glossário Técnico
    with st.expander("ℹ️ Entenda as Métricas (Glossário Técnico)"):
        st.markdown("""
        ### Como interpretar este Dashboard?
        
        **1. Volume de Notícias:**
        Indica a exposição midiática do banco nos últimos 7 dias.
        
        **2. Índice de Reclamações (BCB):**
        Métrica oficial do Banco Central (Reclamações / Clientes * 1M).
        * **Análise:** Quanto MENOR o índice, melhor a qualidade.
        """)

else:
    st.error(f"❌ Erro: Arquivo {DATA_PATH} não encontrado.")

st.sidebar.markdown("---")
st.sidebar.caption("Projeto FinVoC | Alan Cristian - Poli-USP")