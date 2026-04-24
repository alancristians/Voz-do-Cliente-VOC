import streamlit as st
import pandas as pd
import plotly.express as px
import os

# 1. Configuração e Estilo (Preservando seu layout favorito)
st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação 2.0", layout="wide", page_icon="🛡️")

st.markdown("""
    <style>
    .stMetric { background-color: #1e1e1e; padding: 15px; border-radius: 10px; border: 1px solid #333; }
    </style>
""", unsafe_allow_html=True)

st.title("🗣️ Voz do Cliente | Monitor de Reputação Bancária")
st.caption("Inteligência de Dados e Monitoramento de Experiência do Cliente via Pipeline Automatizado com IA Gemini.")

# --- DICIONÁRIOS DE CORES ---
# Cores de Sentimento (Para o gráfico de IA)
SENTIMENT_COLORS = {"Positivo": "#2ECC71", "Neutro": "#95A5A6", "Negativo": "#E74C3C"}

# Cores Oficiais (Sua Identidade Visual Protegida)
BANK_COLORS = {
    "Itau": "#EC7000", "Itaú": "#EC7000", 
    "Bradesco": "#C8102E", 
    "Santander": "#FF0000", 
    "Nubank": "#820AD1", 
    "Banco do Brasil": "#F8D117", 
    "Caixa": "#005CA9"
}

# 2. Caminhos de Dados
GOLD_PATH = "data/gold/fact_finvoc_summary.csv"
NEWS_DETAIL_PATH = "data/silver/stg_noticias.parquet" # Upgrade: agora busca da Silver para ler o sentimento

@st.cache_data
def load_data():
    if os.path.exists(GOLD_PATH):
        df = pd.read_csv(GOLD_PATH)
        # Resiliência para novas colunas de sentimento
        for s_col in ['sent_positivo', 'sent_neutro', 'sent_negativo']:
            if s_col not in df.columns: df[s_col] = 0
            
        for col in ['indice_bcb', 'total_clientes', 'recl_procedentes', 'total_respondidas']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # Cálculo da Taxa de Procedência (Eficácia)
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df
    return None

df = load_data()

if df is not None:
    # Banner de Período
    periodo_ref = df['periodo'].iloc[0] if 'periodo' in df.columns else "Dados Recentes"
    st.info(f"📊 **Dados de Referência:** {periodo_ref} | 🤖 Análise de Sentimento: **Gemini 1.5 Flash**")
    
    # Barra Lateral
    bancos = st.sidebar.multiselect("Filtrar Instituições:", options=df['bank'].unique(), default=df['bank'].unique())
    df_p = df[df['bank'].isin(bancos)]

    # 4. KPIs com Tooltips (Estilo original mantido)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p))
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()), 
              help="Volume total de notícias capturadas e analisadas pela IA nos últimos dias.")
    k3.metric("Média Eficiência", f"{df_p['taxa_procedencia'].mean():.2f}%", 
              help="Proporção de reclamações procedentes. Quanto MENOR, mais o banco resolve problemas internamente.")
    k4.metric("Market Share (Clientes)", f"{df_p['total_clientes'].sum()/1e6:.1f}M", 
              help="Volume total de clientes ativos registrados no Banco Central.")

    st.divider()

    # 5. Seção de Performance: Sentimento (IA) vs Reclamações (BCB)
    st.subheader("📊 Performance: Sentimento da Mídia vs. Reclamações", 
                  help="Cruzamento entre o clima externo (IA) e o índice oficial de queixas do BCB.")
    c1, c2 = st.columns(2)
    
    with c1:
        # UPGRADE: Gráfico de Barras Empilhadas com Semáforo de Sentimento
        df_sent = df_p.melt(id_vars=['bank'], value_vars=['sent_positivo', 'sent_neutro', 'sent_negativo'],
                            var_name='Sentimento', value_name='Qtd')
        df_sent['Sentimento'] = df_sent['Sentimento'].map({
            'sent_positivo': 'Positivo', 
            'sent_neutro': 'Neutro', 
            'sent_negativo': 'Negativo'
        })
        
        fig_sent = px.bar(df_sent, x='bank', y='Qtd', color='Sentimento', 
                          color_discrete_map=SENTIMENT_COLORS, barmode='stack',
                          template="plotly_dark", title="Clima da Mídia (IA Gemini)")
        st.plotly_chart(fig_sent, use_container_width=True)
        
    with c2:
        # MANTIDO: Gráfico de Reclamações
        st.plotly_chart(px.line(df_p.sort_values('indice_bcb', ascending=False), 
                                x='bank', y='indice_bcb', markers=True, 
                                template="plotly_dark", title="Índice de Reclamações (BCB)"), 
                        use_container_width=True)

    st.divider()

    # 6. Escala e Eficiência (AQUI AS CORES DOS BANCOS SÃO PROTAGONISTAS)
    st.subheader("🎯 Market Share e Eficácia de Resolução", 
                  help="O tamanho do banco comparado à sua capacidade real de resolver problemas.")
    c3, c4 = st.columns(2)
    with c3:
        st.plotly_chart(px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                               color='bank', color_discrete_map=BANK_COLORS, 
                               template="plotly_dark", title="Volume de Clientes"), 
                        use_container_width=True)
    with c4:
        st.plotly_chart(px.bar(df_p, x='bank', y='taxa_procedencia', 
                               color='bank', color_discrete_map=BANK_COLORS, 
                               text_auto='.2f', template="plotly_dark", 
                               title="Taxa de Procedência (%)"), 
                        use_container_width=True)

    # 7. Matriz de Diagnóstico VOC (Incluindo Alerta de Sentimento Negativo)
    st.subheader("⚠️ Matriz de Diagnóstico VOC")
    st.dataframe(df_p[['bank', 'principal_motivo', 'indice_bcb', 'taxa_procedencia', 'sent_negativo']], 
                 width="stretch", hide_index=True)

    # 8. Explorador de Notícias (Upgrade: Agora com a coluna de Sentimento da IA)
    st.divider()
    st.subheader("🔍 Explorador de Notícias com IA", 
                  help="Busca textual detalhada em todas as notícias classificadas pela IA.")
    if os.path.exists(NEWS_DETAIL_PATH):
        df_news = pd.read_parquet(NEWS_DETAIL_PATH)
        search = st.text_input("Filtrar por termo ou sentimento (ex: Negativo, Lucro, Fraude):")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(df_news, column_config={"link": st.column_config.LinkColumn("Fonte")}, 
                     width="stretch", hide_index=True)

else:
    st.error("❌ Dados da Camada Ouro não encontrados. Rode o pipeline no GitHub Actions.")

st.sidebar.markdown("---")
st.sidebar.caption("FinVoC 2.0 | Autor: Alan Cristian")