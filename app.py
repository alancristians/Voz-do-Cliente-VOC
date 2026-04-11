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

# 3. Funções de Carga de Dados
DATA_PATH_GOLD = "data/gold/fact_finvoc_summary.csv"
DATA_PATH_NEWS = "data/bronze/noticias_bancos.parquet"

@st.cache_data
def load_gold_data():
    if os.path.exists(DATA_PATH_GOLD):
        df = pd.read_csv(DATA_PATH_GOLD)
        idx_cols = [c for c in df.columns if 'indice' in c.lower() or 'índice' in c.lower()]
        if idx_cols:
            df = df.rename(columns={idx_cols[0]: 'indice_bcb'})
            df['indice_bcb'] = pd.to_numeric(df['indice_bcb'], errors='coerce').fillna(0)
        return df
    return None

@st.cache_data
def load_news_data():
    if os.path.exists(DATA_PATH_NEWS):
        # Carrega o parquet com os detalhes das notícias
        return pd.read_parquet(DATA_PATH_NEWS)
    return None

df_gold = load_gold_data()
df_news = load_news_data()

if df_gold is not None:
    # 4. Sidebar com Filtros (Afeta apenas os gráficos)
    st.sidebar.header("⚙️ Configurações Dash")
    bancos_disponiveis = df_gold['bank'].unique()
    bancos_sel = st.sidebar.multiselect(
        "Filtrar Bancos nos Gráficos:", 
        options=bancos_disponiveis, 
        default=bancos_disponiveis
    )
    df_plot = df_gold[df_gold['bank'].isin(bancos_sel)]

    # 5. KPIs de Topo
    m1, m2, m3 = st.columns(3)
    m1.metric("Bancos Analisados", len(df_plot))
    m2.metric("Total de Notícias (Semana)", int(df_plot['qtd_noticias_recentes'].sum()))
    m3.metric("Média Índice BCB", f"{df_plot['indice_bcb'].mean():.2f}")

    st.divider()

    # 6. Gráficos Principais (Lado a Lado)
    col_esq, col_dir = st.columns(2)
    with col_esq:
        st.subheader("📰 Volume de Notícias")
        fig_news = px.bar(df_plot, x='bank', y='qtd_noticias_recentes', color='bank', template="plotly_dark")
        st.plotly_chart(fig_news, use_container_width=True)

    with col_dir:
        st.subheader("📉 Índice de Reclamações")
        df_ord = df_plot.sort_values(by='indice_bcb', ascending=False)
        fig_idx = px.line(df_ord, x='bank', y='indice_bcb', markers=True, template="plotly_dark")
        st.plotly_chart(fig_idx, use_container_width=True)

    # 7. SEÇÃO DE NOTÍCIAS (Independente e Compacta no final)
    st.divider()
    st.subheader("🔍 Explorador de Notícias Detalhado")
    
    if df_news is not None:
        # Busca específica para notícias
        search = st.text_input("Pesquisar termo nas notícias (ex: fraude, lucro, nome do banco):")
        
        # Ordenação por data (mais recentes primeiro) - assume coluna 'date' ou 'published'
        date_col = next((c for c in df_news.columns if 'date' in c or 'pub' in c), None)
        if date_col:
            df_news[date_col] = pd.to_datetime(df_news[date_col])
            df_news = df_news.sort_values(by=date_col, ascending=False)

        # Filtro de busca
        if search:
            df_news_filt = df_news[df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)]
        else:
            df_news_filt = df_news

        # Exibição compacta com link clicável
        st.dataframe(
            df_news_filt,
            column_config={
                "link": st.column_config.LinkColumn("Link Original"),
                "url": st.column_config.LinkColumn("URL"),
            },
            hide_index=True,
            use_container_width=True
        )
    else:
        st.info("ℹ️ Arquivo de detalhes das notícias (parquet) não encontrado para busca profunda.")

    # 8. Glossário Técnico
    with st.expander("ℹ️ Glossário"):
        st.markdown("""
        * **Volume**: Menções na mídia nos últimos 7 dias.
        * **Índice BCB**: Reclamações procedentes por 1 milhão de clientes. O **45.13 do Itaú** é a referência.
        """)

else:
    st.error(f"❌ Erro ao carregar dados em {DATA_PATH_GOLD}")

st.sidebar.markdown("---")
st.sidebar.caption("FinVoC | Alan Cristian - Poli-USP")