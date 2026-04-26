import streamlit as st
import pandas as pd
import plotly.express as px
import os
import pytz
from datetime import datetime

# 1. CONFIGURAÇÕES DE INTERFACE
# Definição do layout e metadados da página
st.set_page_config(page_title="Voz do Cliente | Monitor de Reputação", layout="wide", page_icon="🛡️")

# 2. ESTILIZAÇÃO CUSTOMIZADA (CSS)
# Customização de métricas e rodapé para alinhamento com a identidade visual do projeto
st.markdown("""
    <style>
    div[data-testid="stMetric"] {
        background-color: #1e1e1e;
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #333;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.5);
    }
    .main-footer {
        text-align: center;
        padding: 20px;
        color: #666;
        font-size: 14px;
        border-top: 1px solid #333;
        margin-top: 50px;
    }
    .main-footer a {
        color: #0077b5;
        text-decoration: none;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🗣️ Voz do Cliente | Monitor de Reputação Bancária")
st.caption('“Só existe um chefe: o cliente. Ele pode demitir todos na empresa gastando seu dinheiro em outro lugar.” – Sam Walton')

# 3. IDENTIDADE VISUAL E CORES INSTITUCIONAIS
# Mapeamento de cores para consistência visual entre gráficos
BANK_COLORS = {
    "Itaú": "#EC7000", "Bradesco": "#C8102E", "Santander": "#FF0000", 
    "Nubank": "#820AD1", "Banco do Brasil": "#F8D117", "Caixa": "#005CA9", 
    "C6": "#353434", "BTG Pactual": "#001C3D", "PicPay": "#21C25E", "Inter": "#FF7A00"
}

@st.cache_data
def carregar_dados():
    """
    Realiza a leitura dos dados processados nas camadas Silver e Gold.
    Aplica tratamento de tipos e conversão de fuso horário para a data de atualização.
    """
    path = "data/gold/fact_finvoc_summary.csv"
    news_path = "data/silver/stg_noticias.parquet"
    
    # Captura data de atualização do sistema via metadados do arquivo (Ajuste de Timezone UTC -> SP)
    last_update = "---"
    if os.path.exists(news_path):
        ts = os.path.getmtime(news_path)
        utc_dt = datetime.fromtimestamp(ts, tz=pytz.utc)
        sp_tz = pytz.timezone('America/Sao_Paulo')
        last_update = utc_dt.astimezone(sp_tz).strftime('%d/%m/%Y')

    if os.path.exists(path):
        df = pd.read_csv(path)
        
        # Data Cleaning: Padronização de tipos numéricos e tratamento de decimais
        cols = ['indice_bcb', 'total_clientes', 'recl_procedentes', 'total_respondidas', 'qtd_noticias_recentes']
        for col in cols:
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(',', '.')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        # Feature Engineering: Cálculo de taxa de eficiência de resolução
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
        return df, last_update
    
    return None, last_update

# Inicialização da carga de dados
df, data_atualizacao = carregar_dados()

if df is not None:
    # 4. DASHBOARD HEADER - Status do Pipeline
    st.info(f"📊 **Dados de Referência:** {df['periodo'].iloc[0]} | 🔄 **Atualização Diária:** Última carga em {data_atualizacao}")
    
    # 5. SIDEBAR - Governança de Filtros
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

    # 6. CAMADA DE MÉTRICAS (KPIs)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p), help="Quantidade de bancos no filtro atual.")
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()), help="Total de menções capturadas via Google News.")
    k3.metric("Média Índice BCB", f"{df_p['indice_bcb'].mean():.2f}", help="Média do ranking oficial de reclamações.")
    k4.metric("Total de Contas (BCB)", f"{df_p['total_clientes'].sum()/1e6:.1f}M", help="Volume total de clientes ativos.")

    st.divider()

    # 7. VISUALIZAÇÕES - Camada de Notícias e Reclamações
    c1, c2 = st.columns(2)
    with c1:
        news_path = "data/silver/stg_noticias.parquet"
        if os.path.exists(news_path):
            # 1. Carrega os dados detalhados da Silver
            df_raw_news = pd.read_parquet(news_path)
            
            # 2. Tratamento de data e filtro de 30 dias
            df_raw_news['published_dt'] = pd.to_datetime(df_raw_news['published'], errors='coerce')
            # Filtra apenas o que for dos últimos 30 dias em relação a hoje
            limite_30d = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=30)
            df_filtered = df_raw_news[df_raw_news['published_dt'] >= limite_30d.replace(tzinfo=None)]
            
            # 3. Faz a nova contagem por banco
            df_counts = df_filtered.groupby('bank').size().reset_index(name='vol_30d')
            
            # 4. Ordenação para o gráfico
            df_counts = df_counts.sort_values('vol_30d', ascending=True)
            
            # 5. Gera o gráfico com o volume real dos últimos 30 dias
            fig_news = px.bar(df_counts, 
                              y='bank', x='vol_30d', 
                              orientation='h',
                              color='bank', color_discrete_map=BANK_COLORS, 
                              text_auto=True,
                              template="plotly_dark", 
                              title="Volume de Notícias (Últimos 30 dias)")
            
            st.plotly_chart(fig_news, use_container_width=True)
        else:
            # Fallback caso o parquet não exista (usa o dado da Gold)
            fig_news = px.bar(df_p.sort_values('qtd_noticias_recentes'), 
                              y='bank', x='qtd_noticias_recentes', orientation='h',
                              color='bank', color_discrete_map=BANK_COLORS, 
                              template="plotly_dark", title="Volume de Notícias (Total)")
            st.plotly_chart(fig_news, use_container_width=True)
        
    with c2:
        fig_bcb = px.line(df_p.sort_values('indice_bcb', ascending=False), 
                          x='bank', y='indice_bcb', markers=True, 
                          template="plotly_dark", title="Índice de Reclamações (Ranking BCB)")
        st.plotly_chart(fig_bcb, use_container_width=True)

    st.divider()

    # 8. VISUALIZAÇÕES - Market Share e Eficiência Operacional
    c3, c4 = st.columns(2)
    with c3:
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', hole=.4, 
                         color='bank', color_discrete_map=BANK_COLORS, 
                         template="plotly_dark", title="Market Share (Contas Ativas)")
        st.plotly_chart(fig_pie, use_container_width=True)
    with c4:
        fig_proc = px.bar(df_p.sort_values('taxa_procedencia', ascending=False), 
                          x='bank', y='taxa_procedencia', 
                          color='bank', color_discrete_map=BANK_COLORS, 
                          text_auto='.2f', template="plotly_dark", 
                          title="Taxa de Procedência (%) - Eficiência de Resolução")
        st.plotly_chart(fig_proc, use_container_width=True)

    # 9. MATRIZ DE DIAGNÓSTICO (Tabela Fato)
    st.subheader(f"⚠️ Matriz de Diagnóstico VOC")
    df_matrix = df_p.copy()
    df_matrix['total_clientes_m'] = df_matrix['total_clientes'] / 1e6

    st.dataframe(
        df_matrix[['bank', 'indice_bcb', 'taxa_procedencia', 'total_clientes_m']], 
        column_config={
            "bank": "Instituição",
            "indice_bcb": st.column_config.NumberColumn("Índice BCB", format="%.2f"),
            "taxa_procedencia": st.column_config.NumberColumn("Taxa Procedência", format="%.2f%%"),
            "total_clientes_m": st.column_config.NumberColumn("Total Clientes", format="%.2fM")
        },
        width='stretch', hide_index=True
    )

    # 10. EXPLORADOR DE DADOS (Silver Layer)
    st.divider()
    st.subheader(f"🔍 Explorador de Notícias")
    news_path = "data/silver/stg_noticias.parquet"
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        search = st.text_input("Busca textual nas manchetes:", placeholder="Ex: C6 Bank, Reclamação, App...")
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        st.dataframe(
        df_news,
        column_config={
            "link": st.column_config.LinkColumn(
                "link", # Título da coluna
                help="Clique para abrir a notícia original",
                validate=r"^https?://"
            ),
            "title": "title",
            "published": "published"
        },
        width='stretch', 
        hide_index=True
    )
else:
    st.error("❌ Erro na carga dos dados das camadas Gold/Silver.")

# 11. FOOTER - Identidade Profissional
st.markdown(f"""
    <div class="main-footer">
        Desenvolvido e sustentado por <b>Alan Cristian</b> | 
        <a href="https://www.linkedin.com/in/alancristians/" target="_blank">LinkedIn</a><br>
        <i>"Feito é melhor que perfeito"</i>
    </div>
""", unsafe_allow_html=True)

# 12. SIDEBAR - Selo de Qualidade e Versão
st.sidebar.markdown("---")
st.sidebar.caption(f"📅 **Sincronização:** {data_atualizacao}")
st.sidebar.caption("Versão 1.3 | Alan Cristian | Engenharia de Dados (BCB)")