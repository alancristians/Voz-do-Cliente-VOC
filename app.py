import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configurações de Interface
st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")

# 2. CSS para os KPIs (Visual de "Caixas" restaurado)
st.markdown("""
    <style>
    div[data-testid="stMetric"] {
        background-color: #1e1e1e;
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #333;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.5);
    }
    </style>
""", unsafe_allow_html=True)

st.title("🗣️ Voz do Cliente | Monitor de Reputação Bancária")
st.caption("Engenharia de Dados (BCB) | Arquitetura Medalhão 2026.")

# 3. Identidade Visual Carbon C6 e Azul BTG
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
        # Garante tratamento de decimais e limpeza de dados
        cols = ['indice_bcb', 'total_clientes', 'recl_procedentes', 'total_respondidas', 'qtd_noticias_recentes']
        for col in cols:
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(',', '.')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = carregar_dados()

if df is not None:
    st.info(f"📊 **Dados de Referência:** {df['periodo'].iloc[0]}")
    
    # 4. SIDEBAR - Filtro e Rodapé (Alan Cristian 2026)
    st.sidebar.header("⚙️ Configurações")
    with st.sidebar.expander("🏦 Filtrar Instituições", expanded=True):
        todos_bancos = df['bank'].unique().tolist()
        
        if st.button("Limpar Seleção"):
            st.session_state["filtro_bancos"] = []
            st.rerun()

        selected_banks = st.multiselect(
            "Selecione os bancos:",
            options=todos_bancos,
            default=todos_bancos,
            key="filtro_bancos",
            label_visibility="collapsed"
        )
    
    df_p = df[df['bank'].isin(selected_banks)]

    # 5. KPIs de Topo com Tooltips
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p), help="Quantidade de bancos filtrados.")
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()), help="Total de menções capturadas no período.")
    k3.metric("Média Índice BCB", f"{df_p['indice_bcb'].mean():.2f}", help="Média do ranking de reclamações.")
    k4.metric("Total de Contas (BCB)", f"{df_p['total_clientes'].sum()/1e6:.1f}M", help="Soma de contas ativas no mercado.")

    st.divider()

    # 6. GRÁFICOS (Voz do Mercado e BCB)
    c1, c2 = st.columns(2)
    with c1:
        fig_news = px.bar(df_p.sort_values('qtd_noticias_recentes'), 
                          y='bank', x='qtd_noticias_recentes', orientation='h',
                          color='bank', color_discrete_map=BANK_COLORS, 
                          template="plotly_dark", title="Volume de Notícias na Mídia")
        st.plotly_chart(fig_news, width='stretch')
        
    with c2:
        fig_bcb = px.line(df_p.sort_values('indice_bcb', ascending=False), 
                          x='bank', y='indice_bcb', markers=True, 
                          template="plotly_dark", title="Índice de Reclamações (BCB)")
        st.plotly_chart(fig_bcb, width='stretch')

    st.divider()

    # 7. GRÁFICOS (Participação e Eficiência)
    c3, c4 = st.columns(2)
    with c3:
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                         color='bank', color_discrete_map=BANK_COLORS, 
                         template="plotly_dark", title="Market Share (Contas Ativas)")
        st.plotly_chart(fig_pie, width='stretch')
    with c4:
        fig_proc = px.bar(df_p.sort_values('taxa_procedencia', ascending=False), 
                          x='bank', y='taxa_procedencia', 
                          color='bank', color_discrete_map=BANK_COLORS, 
                          text_auto='.2f', template="plotly_dark", 
                          title="Taxa de Procedência (%) - Eficiência de Resolução")
        st.plotly_chart(fig_proc, width='stretch')

    # 8. MATRIZ DE DIAGNÓSTICO (RETORNO AO ORIGINAL)
    st.subheader("⚠️ Matriz de Diagnóstico VOC")
    df_matrix = df_p.copy()
    df_matrix['total_clientes_m'] = df_matrix['total_clientes'] / 1e6

    st.dataframe(
        df_matrix[['bank', 'indice_bcb', 'taxa_procedencia', 'total_clientes_m']], 
        column_config={
            "bank": "Instituição",
            "indice_bcb": st.column_config.NumberColumn("Índice BCB", format="%.2f", help="Índice oficial de reclamações."),
            "taxa_procedencia": st.column_config.NumberColumn("Taxa Procedência", format="%.2f%%", help="Reclamações procedentes/respondidas."),
            "total_clientes_m": st.column_config.NumberColumn("Total Clientes", format="%.2fM", help="Clientes em milhões.")
        },
        width='stretch', hide_index=True
    )

    # 9. EXPLORADOR DE NOTÍCIAS
    st.divider()
    st.subheader("🔍 Explorador de Notícias")
    news_path = "data/silver/stg_noticias.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        search = st.text_input("Filtrar notícias por termo:")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, width='stretch', hide_index=True)
else:
    st.error("❌ Dados não encontrados.")

# 10. RODAPÉ SIDEBAR (Selo Original)
st.sidebar.markdown("---")
st.sidebar.caption("🛡️ **FinVoC 2.0**")
st.sidebar.caption("Sistema de Monitoramento de Reputação")
st.sidebar.caption("Engenharia de Dados | Alan Cristian 2026")