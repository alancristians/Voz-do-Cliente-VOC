import streamlit as st
import pandas as pd
import plotly.express as px
import os

st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")

BANK_COLORS = {
    "Itaú": "#EC7000", "Bradesco": "#C8102E", "Santander": "#FF0000", 
    "Nubank": "#820AD1", "Banco do Brasil": "#F8D117", "Caixa": "#005CA9", 
    "C6": "#353434", "BTG Pactual": "#001C3D", "PicPay": "#21C25E", "Inter": "#FF7A00"
}

@st.cache_data
def carregar_dados():
    path = "data/gold/fact_finvoc_summary.csv"
    if os.path.exists(path):
        df = pd.read_csv(path)
        for col in ['indice_bcb', 'total_clientes', 'qtd_noticias_recentes', 'recl_procedentes', 'total_respondidas']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = carregar_dados()

if df is not None:
    st.title("🗣️ Voz do Cliente | Monitor de Reputação Bancária")
    st.info(f"📊 **Dados de Referência:** {df['periodo'].iloc[0]}")
    
    bancos = st.sidebar.multiselect("Filtrar:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos", len(df_p))
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()))
    k3.metric("Média Índice BCB", f"{df_p['indice_bcb'].mean():.2f}")
    k4.metric("Total de Contas (BCB)", f"{df_p['total_clientes'].sum()/1e6:.1f}M")

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        fig_news = px.bar(df_p.sort_values('qtd_noticias_recentes'), 
                          y='bank', x='qtd_noticias_recentes', orientation='h',
                          color='bank', color_discrete_map=BANK_COLORS, 
                          template="plotly_dark", title="Volume de Notícias na Mídia")
        st.plotly_chart(fig_news, width='stretch')
        
    with c2:
        # Gráfico de Linha do Índice BCB (Agora com dados populados)
        fig_bcb = px.line(df_p.sort_values('indice_bcb', ascending=False), 
                          x='bank', y='indice_bcb', markers=True, 
                          template="plotly_dark", title="Índice de Reclamações (BCB)")
        st.plotly_chart(fig_bcb, width='stretch')

    st.divider()

    c3, c4 = st.columns(2)
    with c3:
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                         color='bank', color_discrete_map=BANK_COLORS, 
                         template="plotly_dark", title="Market Share (Contas Ativas)")
        st.plotly_chart(fig_pie, width='stretch')
    with c4:
        # ORDENAÇÃO: Do pior (maior taxa) para o melhor
        fig_proc = px.bar(df_p.sort_values('taxa_procedencia', ascending=False), 
                          x='bank', y='taxa_procedencia', 
                          color='bank', color_discrete_map=BANK_COLORS, 
                          text_auto='.2f', template="plotly_dark", 
                          title="Taxa de Procedência (%) - Ranking de Eficiência")
        st.plotly_chart(fig_proc, width='stretch')

    # Matriz com formatação profissional (M e %) e motivo oculto
    st.subheader("⚠️ Matriz de Diagnóstico VOC")
    st.dataframe(
        df_p[['bank', 'indice_bcb', 'taxa_procedencia', 'total_clientes']], 
        column_config={
            "bank": "Instituição",
            "indice_bcb": st.column_config.NumberColumn("Índice BCB", format="%.2f"),
            "taxa_procedencia": st.column_config.NumberColumn("Taxa Procedência", format="%.2f%%"),
            "total_clientes": st.column_config.NumberColumn("Total Clientes", format="%.1fM")
        },
        width='stretch', hide_index=True
    )

    st.divider()
    st.subheader("🔍 Explorador de Notícias")
    news_path = "data/silver/stg_noticias.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        search = st.text_input("Filtrar notícias:")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, width='stretch', hide_index=True)
else:
    st.error("❌ Dados não encontrados.")