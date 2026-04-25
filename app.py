import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configurações de Interface e Estilo
st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")

st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🗣️ Voz do Cliente | Monitor de Reputação Bancária")
st.caption("Dados Oficiais (BCB/Consumidor.gov) | Arquitetura Medalhão.")

# 2. Identidade Visual Atualizada (C6 e BTG ajustados)
BANK_COLORS = {
    "Itau": "#EC7000", "Itaú": "#EC7000", 
    "Bradesco": "#C8102E", 
    "Santander": "#FF0000", 
    "Nubank": "#820AD1", 
    "Banco do Brasil": "#F8D117", 
    "Caixa": "#005CA9",
    "C6": "#1A1A1A",      # Cinza quase preto (Identidade C6)
    "Btg": "#00204D",     # Azul oficial (BTG Pactual)
    "Picpay": "#21C25E", 
    "Inter": "#FF7A00"
}

# 3. Carregamento de Dados
@st.cache_data
def carregar_dados():
    path = "data/gold/fact_finvoc_summary.csv"
    if os.path.exists(path):
        df = pd.read_csv(path)
        cols = ['indice_bcb', 'total_clientes', 'recl_procedentes', 'total_respondidas', 'qtd_noticias_recentes']
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = carregar_dados()

if df is not None:
    # Cabeçalho de Período
    periodo_ref = df['periodo'].iloc[0] if 'periodo' in df.columns else "Dados 2026"
    st.info(f"📊 **Dados de Referência:** {periodo_ref}")
    
    # Filtros
    bancos = st.sidebar.multiselect("Filtrar Instituições:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    # Garantindo que a métrica de exposição leia o valor correto
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()))
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%")
    k4.metric("Market Share (Clientes)", f"{df_p['total_clientes'].sum()/1e6:.1f}M")

    st.divider()

    # 5. Performance: Mídia vs Reclamações
    c1, c2 = st.columns(2)
    with c1:
        # Gráfico de Volume de Notícias (Restaurado)
        fig_news = px.bar(df_p.sort_values('qtd_noticias_recentes', ascending=False), 
                          x='bank', y='qtd_noticias_recentes', color='bank',
                          color_discrete_map=BANK_COLORS, template="plotly_dark", 
                          title="Volume de Notícias na Mídia")
        st.plotly_chart(fig_news, width='stretch')
        
    with c2:
        fig_bcb = px.line(df_p.sort_values('indice_bcb', ascending=False), 
                          x='bank', y='indice_bcb', markers=True, 
                          template="plotly_dark", title="Índice de Reclamações (BCB)")
        st.plotly_chart(fig_bcb, width='stretch')

    st.divider()

    # 6. Escala e Eficiência
    c3, c4 = st.columns(2)
    with c3:
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                         color='bank', color_discrete_map=BANK_COLORS, 
                         template="plotly_dark", title="Market Share")
        st.plotly_chart(fig_pie, width='stretch')
    with c4:
        fig_proc = px.bar(df_p, x='bank', y='taxa_procedencia', 
                          color='bank', color_discrete_map=BANK_COLORS, 
                          text_auto='.2f', template="plotly_dark", 
                          title="Taxa de Procedência (%)")
        st.plotly_chart(fig_proc, width='stretch')

    # 7. Matriz de Diagnóstico (Correção do erro de Width)
    st.subheader("⚠️ Matriz de Diagnóstico VOC")
    # Alterado width=None para width='stretch' para corrigir o StreamlitInvalidWidthError
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia']], 
                 width='stretch', hide_index=True)

    # 8. Explorador de Notícias (Restaurado)
    st.divider()
    st.subheader("🔍 Explorador de Notícias")
    news_path = "data/silver/stg_noticias.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        search = st.text_input("Pesquisar notícias (ex: Lucro, C6, Pix):")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Fonte")}, 
                     width='stretch', hide_index=True)

else:
    st.error("❌ Dados não encontrados. Verifique o pipeline.")

st.sidebar.markdown("---")
st.sidebar.caption("FinVoC 2.0 | Sistema de Monitoramento")