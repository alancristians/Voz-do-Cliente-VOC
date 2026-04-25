import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configurações de Interface e Estilo
st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")

# CSS para customização dos cards de métricas
st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🗣️ Voz do Cliente | Monitor de Reputação Bancária")
st.caption("Análise consolidada de dados oficiais (BCB/Consumidor.gov) e exposição mediática via Pipeline Medalhão.")

# 2. Identidade Visual Protegida (Cores Oficiais dos Bancos)
BANK_COLORS = {
    "Itau": "#EC7000", "Itaú": "#EC7000", 
    "Bradesco": "#C8102E", 
    "Santander": "#FF0000", 
    "Nubank": "#820AD1", 
    "Banco do Brasil": "#F8D117", 
    "Caixa": "#005CA9"
}

# 3. Carregamento e Tratamento de Dados
@st.cache_data
def carregar_dados():
    """
    Carrega o dataset da camada Gold, realiza conversão numérica e calcula KPIs de eficiência.
    """
    caminho_gold = "data/gold/fact_finvoc_summary.csv"
    if os.path.exists(caminho_gold):
        df = pd.read_csv(caminho_gold)
        # Conversão de colunas métricas para numérico
        cols_metricas = ['indice_bcb', 'total_clientes', 'recl_procedentes', 'total_respondidas']
        for col in cols_metricas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Cálculo da Taxa de Procedência (Eficácia de Resolução)
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = carregar_dados()

if df is not None:
    # Cabeçalho de referência temporal
    periodo_ref = df['periodo'].iloc[0] if 'periodo' in df.columns else "Dados Recentes"
    st.info(f"📊 **Dados de Referência:** {periodo_ref}")
    
    # Filtros na Barra Lateral
    st.sidebar.header("Configurações de Filtro")
    bancos = st.sidebar.multiselect("Filtrar Instituições:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. KPIs Principais (Resumo Executivo)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()), help="Volume total de notícias capturadas na camada Bronze.")
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%", help="Proporção de reclamações procedentes. Quanto menor, melhor a resolução interna.")
    k4.metric("Market Share (Clientes)", f"{df_p['total_clientes'].sum()/1e6:.1f}M", help="Total de clientes ativos conforme dados do Banco Central.")

    st.divider()

    # 5. Painel de Performance: Mídia vs Reclamações
    st.subheader("📈 Performance: Exposição mediática vs. Reclamações Oficiais")
    c1, c2 = st.columns(2)
    
    with c1:
        # Gráfico de Volume de Notícias (Exposição)
        fig_news = px.bar(df_p.sort_values('qtd_noticias_recentes', ascending=False), 
                          x='bank', y='qtd_noticias_recentes', color='bank',
                          color_discrete_map=BANK_COLORS, template="plotly_dark", 
                          title="Volume de Notícias na Mídia")
        st.plotly_chart(fig_news, use_container_width=True)
        
    with c2:
        # Gráfico de Índice de Reclamações do BCB
        fig_bcb = px.line(df_p.sort_values('indice_bcb', ascending=False), 
                          x='bank', y='indice_bcb', markers=True, 
                          template="plotly_dark", title="Índice de Reclamações (Banco Central)")
        st.plotly_chart(fig_bcb, use_container_width=True)

    st.divider()

    # 6. Painel de Escala e Eficiência
    st.subheader("🎯 Market Share e Eficácia de Resolução")
    c3, c4 = st.columns(2)
    
    with c3:
        # Gráfico de Distribuição de Clientes (Market Share)
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                         color='bank