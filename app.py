import streamlit as st
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")

st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🗣️ Voz do Cliente | Monitor de Reputação Bancária")
st.caption("Dados Oficiais (BCB/Consumidor.gov) | Arquitetura Medalhão.")

BANK_COLORS = {
    "Itau": "#EC7000", "Itaú": "#EC7000", "Bradesco": "#C8102E", 
    "Santander": "#FF0000", "Nubank": "#820AD1", "Banco do Brasil": "#F8D117", 
    "Caixa": "#005CA9", "C6": "#FFD700", "BTG": "#002F6C", "Picpay": "#21C25E", "Inter": "#FF7A00"
}

@st.cache_data
def carregar_dados():
    path = "data/gold/fact_finvoc_summary.csv"
    if os.path.exists(path):
        df = pd.read_csv(path)
        cols = ['indice_bcb', 'total_clientes', 'recl_procedentes', 'total_respondidas']
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = carregar_dados()

if df is not None:
    periodo_ref = df['periodo'].iloc[0] if 'periodo' in df.columns else "Dados Recentes"
    st.info(f"📊 **Dados de Referência:** {periodo_ref}")
    
    bancos = st.sidebar.multiselect("Filtrar Instituições:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()))
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%")
    k4.metric("Market Share (Clientes)", f"{df_p['total_clientes'].sum()/1e6:.1f}M")

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        fig_news = px.bar(df_p.sort_values('qtd_noticias_recentes', ascending=False), 
                          x='bank', y='qtd_noticias_recentes', color='bank',
                          color_discrete_map=BANK_COLORS, template="plotly_dark", 
                          title="Volume de Notícias")
        st.plotly_chart(fig_news, use_container_width=True)
        
    with c2:
        fig_bcb = px.line(df_p.sort_values('indice_bcb', ascending=False), 
                          x='bank', y='indice_bcb', markers=True, 
                          template="plotly_dark", title="Índice de Reclamações (BCB)")
        st.plotly_chart(fig_bcb, use_container_width=True)

    st.divider()

    c3, c4 = st.columns(2)
    with c3:
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                         color='bank', color_discrete_map=BANK_COLORS, 
                         template="plotly_dark", title="Market Share")
        st.plotly_chart(fig_pie, use_container_width=True)
        
    with c4:
        fig_proc = px.bar(df_p, x='bank', y='taxa_procedencia', 
                          color='bank', color_discrete_map=BANK_COLORS, 
                          text_auto='.2f', template="plotly_dark", 
                          title="Taxa de Procedência (%)")
        st.plotly_chart(fig_proc, use_container_width=True)

    st.subheader("⚠️ Matriz de Diagnóstico VOC")
    # Correção dos parâmetros de largura para evitar warnings
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], 
                 width=None, hide_index=True)

else:
    st.error("❌ Dados não encontrados.")