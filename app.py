import streamlit as st
import pandas as pd
import plotly.express as px
import os
import pytz
from datetime import datetime

# 1. CONFIGURAÇÕES DE INTERFACE
# Definição do layout e metadados da página
st.set_page_config(page_title="Voz do Cliente | Monitor de Experiência", layout="wide", page_icon="🛡️")

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

st.title("🗣️ Voz do Cliente | Monitor de Experiência Bancária")
st.caption('“Só existe um chefe: o cliente. Ele pode demitir todos na empresa gastando seu dinheiro em outro lugar.” – Sam Walton')

# 3. IDENTIDADE VISUAL E CORES INSTITUCIONAIS
# Mapeamento de cores para consistência visual entre gráficos
BANK_COLORS = {
    "Itaú": "#EC7000", 
    "Bradesco": "#C8102E", 
    "Santander": "#FF0000", 
    "Nubank": "#820AD1", 
    "Banco do Brasil": "#F8D117", 
    "Caixa": "#005CA9", 
    "C6": "#353434", 
    "BTG Pactual": "#001C3D", 
    "PicPay": "#21C25E", 
    "Inter": "#FF7A00",
    "Mercado Pago": "#00AEEF",  
    "Neon": "#00E5FF"           
}

@st.cache_data
def carregar_dados():
    """
    Realiza a leitura dos dados processados nas camadas Silver e Gold.
    Aplica tratamento de tipos e leitura do timestamp de atualização.
    """
    path = "data/gold/fact_finvoc_summary.csv"
    news_path = "data/silver/stg_noticias.parquet"
    hist_path = "data/silver/hist_reclamacoes_bcb.csv"
    last_update_path = "data/gold/last_update.txt"

    
    # AJUSTE: Captura data de atualização via arquivo de controle (TXT)
    last_update = "---"
    if os.path.exists(last_update_path):
        with open(last_update_path, "r") as f:
            last_update = f.read().strip()


    df = None
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
    
    # Ajuste para carregar o histórico com tradução de nomes (Necessário para o join com 2026)
    df_hist = pd.DataFrame()
    if os.path.exists(hist_path):
        df_hist = pd.read_csv(hist_path, sep=';', encoding='latin-1')
        df_hist['bank'] = df_hist['bank'].str.strip()
        
        # Mapa de nomes baseado no seu CSV (Garante que "ITAU (conglomerado)" case com "Itaú")
        mapa_nomes = {
            "ITAU (conglomerado)": "Itaú",
            "BRADESCO (conglomerado)": "Bradesco",
            "SANTANDER (conglomerado)": "Santander",
            "NU PAGAMENTOS (conglomerado)": "Nubank",
            "BB (conglomerado)": "Banco do Brasil",
            "CAIXA ECONMICA FEDERAL (conglomerado)": "Caixa",
            "BANCO C6 (conglomerado)": "C6",
            "BTG PACTUAL/BANCO PAN (conglomerado)": "BTG Pactual",
            "PICPAY (conglomerado)": "PicPay",
            "INTER (conglomerado)": "Inter",
            "MERCADO PAGO IP (conglomerado)": "Mercado Pago",
            "NEON PAGAMENTOS IP (conglomerado)": "Neon"
        }
        df_hist['bank'] = df_hist['bank'].replace(mapa_nomes)
        
    return df, last_update, df_hist

# Inicialização da carga de dados
df, data_atualizacao, df_hist = carregar_dados()

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
    k4.metric("Total de Contas (BCB)", f"{df_p['total_clientes'].sum()/1e6:.1f}M", help="Representa o total de CPFs e CNPJs com relacionamento ativo em cada instituição, conforme base de dados do Banco Central.")

    st.divider()

    # 7. VISUALIZAÇÕES - Camada de Notícias e Reclamações
    c1, c2 = st.columns(2)
    with c1:
        news_path = "data/silver/stg_noticias.parquet"
        st.subheader("Volume de Notícias na Mídia")
        st.caption("Dados referentes aos últimos 30 dias")
        
        if os.path.exists(news_path):
            df_raw_news = pd.read_parquet(news_path)
            df_raw_news = df_raw_news[df_raw_news['bank'].isin(selected_banks)]
            df_raw_news['published_dt'] = pd.to_datetime(df_raw_news['published'], errors='coerce')
            limite_30d = pd.Timestamp.now().replace(hour=0, minute=0, second=0, microsecond=0) - pd.Timedelta(days=30)
            df_filtered = df_raw_news[df_raw_news['published_dt'].dt.tz_localize(None) >= limite_30d]
            df_counts = df_filtered.groupby('bank').size().reset_index(name='vol_30d')
            
            fig_news = px.treemap(df_counts, path=['bank'], values='vol_30d', color='bank', 
                                  color_discrete_map=BANK_COLORS, template="plotly_dark")
            fig_news.update_layout(showlegend=True, legend_title_text="Instituição", margin=dict(t=10, l=0, r=0, b=0), paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
            fig_news.update_traces(textinfo="label+value", hovertemplate='<b>%{label}</b><br>Notícias: %{value}', textfont=dict(size=14, color="white"), marker=dict(line=dict(width=1, color='#333333')))
            st.plotly_chart(fig_news, use_container_width=True, config={'displayModeBar': 'hover', 'scrollZoom': False})
        else:
            fig_news = px.bar(df_p.sort_values('qtd_noticias_recentes'), y='bank', x='qtd_noticias_recentes', orientation='h', color='bank', color_discrete_map=BANK_COLORS, template="plotly_dark")
            fig_news.update_layout(margin=dict(t=10, l=0, r=0, b=0))
            st.plotly_chart(fig_news, use_container_width=True, config={'displayModeBar': 'hover', 'scrollZoom': False})
        
    with c2:
        df_sorted_bcb = df_p.sort_values('indice_bcb', ascending=False)
        st.subheader("Índice de Reclamações")
        st.caption("Ranking oficial do Banco Central (BCB)")

        # 1. Calculamos a média do setor
        media_setor = df_sorted_bcb['indice_bcb'].mean()

        # 2. Criamos o gráfico de dispersão corrigido
        # Removemos o text_auto e usamos 'text' para passar os valores
        fig_bcb = px.scatter(
            df_sorted_bcb, 
            x='bank', 
            y='indice_bcb', 
            color='bank', 
            color_discrete_map=BANK_COLORS,
            text=df_sorted_bcb['indice_bcb'].map('{:.2f}'.format), # Formata o texto aqui
            template="plotly_dark"
        )

        # 3. Adicionamos a Linha de Média
        fig_bcb.add_hline(
            y=media_setor, 
            line_dash="dash", 
            line_color="rgba(255, 255, 255, 0.5)", 
            annotation_text=f"Média: {media_setor:.2f}", 
            annotation_position="top right"
        )

        # 4. Ajustes de Estilo (Aqui ativamos a exibição do texto)
        fig_bcb.update_traces(
            mode='markers+text', # Força exibir o marcador E o texto
            marker=dict(size=14, line=dict(width=1, color='white')),
            textposition='top center'
        )
        
        fig_bcb.update_layout(
            xaxis_title="", 
            yaxis_title="Índice BCB", 
            showlegend=False,
            margin=dict(t=30, b=0, l=0, r=0),
            xaxis=dict(showgrid=True, gridcolor='#333'),
            yaxis=dict(showgrid=True, gridcolor='#333')
        )
        
        st.plotly_chart(fig_bcb, use_container_width=True, config={'displayModeBar': 'hover', 'scrollZoom': False})

    st.divider()

    # 8. VISUALIZAÇÕES - Market Share e Eficiência Operacional
    c3, c4 = st.columns(2)
    with c3:
        st.subheader("Market Share")
        st.caption("Distribuição por volume de contas ativas")
        fig_pie = px.pie(df_p, values='total_clientes', names='bank', hole=.4, color='bank', color_discrete_map=BANK_COLORS, template="plotly_dark")
        fig_pie.update_layout(margin=dict(t=20, b=20, l=0, r=0), showlegend=True)
        st.plotly_chart(fig_pie, use_container_width=True, config={'displayModeBar': 'hover', 'scrollZoom': False})
    with c4:
        df_proc_sorted = df_p.sort_values('taxa_procedencia', ascending=True)
        st.subheader("Taxa de Procedência (%)")
        st.caption("Menor valor indica melhor eficiência operacional")
        fig_proc = px.bar(df_proc_sorted, x='bank', y='taxa_procedencia', color='bank', color_discrete_map=BANK_COLORS, text_auto='.2f', template="plotly_dark")
        fig_proc.update_traces(textposition='outside')
        fig_proc.update_layout(showlegend=False, yaxis_title="Índice", xaxis_title="", margin=dict(t=20, b=0, l=0, r=0))
        st.plotly_chart(fig_proc, use_container_width=True, config={'displayModeBar': 'hover', 'scrollZoom': False})

    # --- NOVO BLOCO: ANÁLISE DE TENDÊNCIA HISTÓRICA (HOJE vs Q4/2025) ---
    with st.expander("📈 Evolução e Tendência Histórica (2025)", expanded=True):
        if not df_hist.empty:
            df_hist_filtered = df_hist[df_hist['bank'].isin(selected_banks)]
            if not df_hist_filtered.empty:
                # Métricas comparando Atual (2026) vs Ontem (Q4 2025)
                cols_met = st.columns(len(selected_banks[:4]))
                for i, b in enumerate(selected_banks[:4]):
                    with cols_met[i]:
                        # Valor 2026 (Hoje)
                        va = df_p[df_p['bank'] == b]['indice_bcb'].values[0]
                        
                        # Dados 2025 (Ontem)
                        bd = df_hist_filtered[df_hist_filtered['bank'] == b].sort_values('ordem_cronologica')
                        if not bd.empty:
                            vn = bd.iloc[-1]['indice_bcb'] # Pega o dado mais recente de 2025 (Q4)
                            delta_pct = ((va - vn) / vn * 100) if vn != 0 else 0
                            st.metric(label=b, value=f"{va:.2f}", delta=f"{delta_pct:.1f}% vs Q4", delta_color="inverse")
                
                st.write("---")
                # Gráfico de Linha Temporal
                fig_ev = px.line(df_hist_filtered.sort_values('ordem_cronologica'), x='periodo', y='indice_bcb', color='bank', markers=True, template="plotly_dark", color_discrete_map=BANK_COLORS)
                fig_ev.update_layout(margin=dict(t=20, b=0, l=0, r=0), xaxis_title="Trimestre", yaxis_title="Índice BCB")
                st.plotly_chart(fig_ev, use_container_width=True, config={'displayModeBar': 'hover'})
            else:
                st.info("⚠️ Selecione os bancos no filtro lateral para ver a evolução histórica.")
        else:
            st.warning("⚠️ Arquivo histórico de 2025 não encontrado.")

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
        use_container_width=True, hide_index=True
    )

    # 10. EXPLORADOR DE DADOS (Silver Layer)
    st.divider()
    st.subheader(f"🔍 Explorador de Notícias")
    news_path = "data/silver/stg_noticias.parquet"
    
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        df_news = df_news[df_news['bank'].isin(selected_banks)]
        search = st.text_input("Busca textual nas manchetes:", placeholder="Ex: C6 Bank, Reclamação, App...")
        
        if search:
            mask = df_news.apply(lambda r: r.astype(str).str.contains(search, case=False).any(), axis=1)
            df_news = df_news[mask]
        
        st.dataframe(
            df_news,
            column_config={
                "link": st.column_config.LinkColumn("link", help="Clique para abrir a notícia original", validate=r"^https?://"),
                "title": "title",
                "published": "published"
            },
            use_container_width=True, 
            hide_index=True
        )
        if not df_news.empty:
            st.caption(f"Exibindo as {len(df_news)} notícias mais relevantes/recentes.")
    else:
        st.error("❌ Erro na carga dos dados.")

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
st.sidebar.markdown("[🔗 Ranking de Reclamações (BCB)](https://www.bcb.gov.br/meubc/rankingreclamacoes)")
st.sidebar.caption("Alan Cristian | Engenharia de Dados (BCB)")