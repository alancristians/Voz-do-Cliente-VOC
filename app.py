import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configurações de Interface
st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")

st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🗣️ Voz do Cliente | Monitor de Reputação Bancária")
st.caption("Engenharia de Dados (BCB/Consumidor.gov) | Arquitetura Medalhão.")

# 2. Identidade Visual (C6: Preto/Cinza | BTG: Azul Oficial)
BANK_COLORS = {
    "Itau": "#EC7000", "Itaú": "#EC7000", "Bradesco": "#C8102E", 
    "Santander": "#FF0000", "Nubank": "#820AD1", "Banco do Brasil": "#F8D117", 
    "Caixa": "#005CA9", 
    "C6": "#111111",      # Identidade Carbon C6
    "Btg": "#001C3D",     # Azul Marinho BTG Pactual
    "Picpay": "#21C25E", "Inter": "#FF7A00"
}

# 3. Tratamento de Dados com Resiliência Numérica
@st.cache_data
def carregar_dados():
    path = "data/gold/fact_finvoc_summary.csv"
    if os.path.exists(path):
        df = pd.read_csv(path)
        # Força conversão de todas as métricas para evitar 'zeros' nos gráficos
        cols = ['indice_bcb', 'total_clientes', 'recl_procedentes', 'total_respondidas', 'qtd_noticias_recentes']
        for col in cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = carregar_dados()

if df is not None:
    periodo_ref = df['periodo'].iloc[0] if 'periodo' in df.columns else "1º Tri / 2026"
    st.info(f"📊 **Dados de Referência:** {periodo_ref}")
    
    bancos = st.sidebar.multiselect("Filtrar Instituições:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. KPIs Principais
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()), help="Soma total de menções na mídia.")
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%")
    # Ajuste sugerido: Explicando o volume de contas vs população
    k4.metric("Total de Contas (BCB)", f"{df_p['total_clientes'].sum()/1e6:.1f}M", 
              help="Soma de relacionamentos bancários ativos. O valor supera a população pois cada cidadão possui, em média, 3 a 5 contas.")

    st.divider()

    # 5. Performance: Notícias vs Reclamações
    c1, c2 = st.columns(2)
    with c1:
        # Gráfico de Volume de Notícias (Barra)
        fig_news = px.bar(df_p.sort_values('qtd_noticias_recentes', ascending=False), 
                          x='bank', y='qtd_noticias_recentes', color='bank',
                          color_discrete_map=BANK_COLORS, template="plotly_dark", 
                          title="Volume de Notícias na Mídia")
        st.plotly_chart(fig_news, width='stretch')
        
    with c2:
        # Gráfico de Reclamações (Linha)
        fig_bcb = px.line(df_p.sort_values('indice_bcb', ascending=False), 
                          x='bank', y='indice_bcb', markers=True, 
                          template="plotly_dark", title="Índice de Reclamações (BCB)")
        st.plotly_chart(fig_bcb, width='stretch')

    st.divider()

    # 6. Escala e Eficácia
    c3, c4 = st.columns(2)
    with c3:
        # Market Share com nomes e quantidades absolutas
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                         color='bank', color_discrete_map=BANK_COLORS, 
                         template="plotly_dark", title="Market Share (Contas Ativas)")
        st.plotly_chart(fig_pie, width='stretch')
        
    with c4:
        fig_proc = px.bar(df_p, x='bank', y='taxa_procedencia', 
                          color='bank', color_discrete_map=BANK_COLORS, 
                          text_auto='.2f', template="plotly_dark", 
                          title="Taxa de Procedência (%)")
        st.plotly_chart(fig_proc, width='stretch')

    # 7. Matriz de Diagnóstico (Correção do Erro de Width)
    st.subheader("⚠️ Matriz de Diagnóstico VOC")
    # Corrigido width=None para width='stretch'
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia', 'total_clientes']], 
                 width='stretch', hide_index=True)

    # 8. Explorador de Notícias (Restaurado)
    st.divider()
    st.subheader("🔍 Explorador de Notícias")
    news_path = "data/silver/stg_noticias.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        search = st.text_input("Filtrar por termo (ex: Pix, Lucro, Fraude):")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Fonte")}, 
                     width='stretch', hide_index=True)

else:
    st.error("❌ Dados não encontrados.")

st.sidebar.markdown("---")
st.sidebar.caption("FinVoC 2.0 | Sistema de Monitoramento")