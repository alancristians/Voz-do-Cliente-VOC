import streamlit as st
import pandas as pd
import plotly.express as px
import os
import pytz
import re
from datetime import datetime

st.set_page_config(page_title="Voz do Cliente | Monitor de Experiência", layout="wide", page_icon="🛡️")

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

def carregar_dados():
    path = "data/gold/fact_finvoc_summary.csv"
    news_path = "data/silver/stg_noticias.parquet"
    hist_path = "data/silver/hist_reclamacoes_bcb.csv"
    last_update_path = "data/gold/last_update.txt"
    
    assuntos_path = "data/silver/stg_assuntos_ranking.csv"
    if not os.path.exists(assuntos_path):
        assuntos_path = "stg_assuntos_ranking.csv"

    last_update = "---"
    if os.path.exists(last_update_path):
        with open(last_update_path, "r") as f:
            last_update = f.read().strip()

    df = None
    if os.path.exists(path):
        df = pd.read_csv(path)
        cols = ['indice_bcb', 'total_clientes', 'recl_procedentes', 'total_respondidas', 'qtd_noticias_recentes']
        for col in cols:
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(',', '.')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        df['taxa_procedencia'] = (df['recl_procedentes'] / df['total_respondidas'] * 100).fillna(0)
    
    mapa_nomes = {
        "ITAU (conglomerado)": "Itaú",
        "BRADESCO (conglomerado)": "Bradesco",
        "SANTANDER (conglomerado)": "Santander",
        "NU PAGAMENTOS (conglomerado)": "Nubank",
        "BB (conglomerado)": "Banco do Brasil",
        "CAIXA ECONMICA FEDERAL (conglomerado)": "Caixa",
        "CAIXA ECONÔMICA FEDERAL (conglomerado)": "Caixa",
        "BANCO C6 (conglomerado)": "C6",
        "BTG PACTUAL/BANCO PAN (conglomerado)": "BTG Pactual",
        "PICPAY (conglomerado)": "PicPay",
        "INTER (conglomerado)": "Inter",
        "MERCADO PAGO IP (conglomerado)": "Mercado Pago",
        "NEON PAGAMENTOS IP (conglomerado)": "Neon"
    }

    df_hist = pd.DataFrame()
    if os.path.exists(hist_path):
        df_hist = pd.read_csv(hist_path, sep=';', encoding='latin-1')
        df_hist['bank'] = df_hist['bank'].str.strip().replace(mapa_nomes)
        if 'ordem_cronologica' in df_hist.columns:
            df_hist['ordem_cronologica'] = pd.to_numeric(df_hist['ordem_cronologica'], errors='coerce').fillna(0).astype(int)

    df_assuntos = pd.DataFrame()
    if os.path.exists(assuntos_path):
        try:
            df_assuntos = pd.read_csv(assuntos_path, sep=';', encoding='latin-1', on_bad_lines='skip')
        except Exception:
            try:
                df_assuntos = pd.read_csv(assuntos_path)
            except Exception:
                pass
            
        if not df_assuntos.empty:
            df_assuntos = df_assuntos.loc[:, ~df_assuntos.columns.str.contains('^Unnamed')]
            df_assuntos.columns = [c.strip() for c in df_assuntos.columns]
            if 'Instituição financeira' in df_assuntos.columns:
                df_assuntos['Instituição financeira'] = df_assuntos['Instituição financeira'].str.strip().replace(mapa_nomes)
            if 'Quantidade de reclamações procedentes' in df_assuntos.columns:
                df_assuntos['Quantidade de reclamações procedentes'] = pd.to_numeric(
                    df_assuntos['Quantidade de reclamações procedentes'], errors='coerce'
                ).fillna(0).astype(int)
        
    return df, last_update, df_hist, df_assuntos

df, data_atualizacao, df_hist, df_assuntos = carregar_dados()

if df is not None:
    st.info(f"📊 **Dados de Referência:** {df['periodo'].iloc[0]} | 🔄 **Atualização Diária:** Última carga em {data_atualizacao}")
    
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

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Bancos Analisados", len(df_p), help="Quantidade de bancos no filtro atual.")
    k2.metric("Exposição (Notícias)", int(df_p['qtd_noticias_recentes'].sum()), help="Total de menções capturadas via Google News.")
    k3.metric("Média Índice BCB", f"{df_p['indice_bcb'].mean():.2f}", help="Média do ranking oficial de reclamações.")
    k4.metric("Total de Contas (BCB)", f"{df_p['total_clientes'].sum()/1e6:.1f}M", help="Total de CPFs e CNPJs com relacionamento ativo.")

    st.divider()

    st.subheader("🎯 Detalhamento de Reclamações por Banco")
    st.caption("Filtro automático aplicado com base nas instituições selecionadas na barra lateral esquerda.")

    with st.expander("📊 **Ver Análise Comparativa de Mudanças (2T2026 vs. 3T2026)**", expanded=False):
        st.markdown("""

        #### **1. Zona de Alta Pressão**
        * 🔴 **Bradesco** | `Índice: 57,23` | `🔻 -32.6% vs Q2`  
          **Gargalo Principal:** *Operações de Crédito (Segurança & Legitimidade)* — 1.200 queixas procedentes.  
          *Diagnóstico:* Teve a maior redução absoluta do trimestre (-27,67 pts), mas permanece na 1ª posição devido à alta concentração de atritos em concessões de crédito.

        * 🖤 **C6 Bank** | `Índice: 52,31` | `🔻 -12.5% vs Q2`  
          **Gargalo Principal:** *Crédito Consignado (Informações Inadequadas)* — 636 queixas procedentes.  
          *Diagnóstico:* A transparência na contratação de consignado segue como o principal fator de desgaste na percepção da marca.

        * 🟠 **Itaú** | `Índice: 43,20` | `🔻 -7.8% vs Q2`  
          **Gargalo Principal:** *Cartões de Crédito (Segurança & Contestação)* — 900 queixas procedentes.  
          *Diagnóstico:* Mantém trajetória descendente no índice geral, concentrando fricção operacional em contestação e segurança no ecossistema de cartões.

        ---

        #### **2. Zona Intermediária**
        * 💙 **BTG Pactual** | `Índice: 40,60` | `🔻 -3.3% vs Q2`  
          **Gargalo Principal:** *SAC & Central de Relacionamento* — 207 queixas procedentes.  
          *Diagnóstico:* Perfil atípico no setor: o produto em si gera pouca queixa, mas o transbordo e resolutividade no atendimento de 1º nível são os gargalos.

        * 🟢 **PicPay** | `Índice: 35,51` | `➡️ Estável vs Q2`  
          **Gargalo Principal:** *Formalização de Crédito sem Título Adequado* — 540 queixas procedentes.  
          *Diagnóstico:* Estabilidade no índice médio, exigindo ajuste fino de governança na esteira de contratação de produtos de crédito.

        * 🌐 **Mercado Pago** | `Índice: 30,57` | `🆕 Estreia no Ranking`  
          **Gargalo Principal:** *Contas Pré-Pagas (Segurança & Bloqueios)* — 384 queixas procedentes.  
          *Diagnóstico:* Entrada direta na 6ª posição com índice controlado, concentrando queixas na segurança de contas de pagamento.

        ---

        #### **3. Zona de Maior Eficiência Relativa**
        * 🟡 **Banco do Brasil** | `Índice: 29,94` | `🔻 -14.9% vs Q2`  
          **Gargalo Principal:** *Cartões de Crédito* — 382 queixas procedentes. *(Recuo consistente para a 7ª posição geral)*.

        * 🔴 **Santander** | `Índice: 29,75` | `🔻 -13.0% vs Q2`  
          **Gargalo Principal:** *Transparência em Operações de Crédito* — 509 queixas procedentes. *(Evolução operacional contínua no trimestre)*.

        * 🟠 **Inter** | `Índice: 18,45` | `🔻 -29.4% vs Q2`  
          **Gargalo Principal:** *Cartões de Crédito* — 205 queixas procedentes. *(Destaque positivo de queda no volume de atritos entre os bancos digitais)*.

        * 🔵 **Caixa Econômica** | `Índice: 17,15` | `🔻 -26.5% vs Q2`  
          **Gargalo Principal:** *Serviços no Internet Banking* — 363 queixas procedentes. *(Excelente indicador em relação à base massiva de 160M+ contas)*.

        * 🟣 **Nubank** | `Índice: 10,68` | `🔺 +16.1% vs Q2`  
          **Gargalo Principal:** *Apontamentos no SCR / Registrato* — 210 queixas procedentes. *(Sustenta a menor taxa proporcional de reclamações entre os grandes players)*.
        """, unsafe_allow_html=True)

    if not df_assuntos.empty:
        col_inst = 'Instituição financeira'
        col_motivo = 'Irregularidade'
        col_qtd = 'Quantidade de reclamações procedentes'

        if col_inst in df_assuntos.columns and col_motivo in df_assuntos.columns and col_qtd in df_assuntos.columns:
            df_assuntos_f = df_assuntos[df_assuntos[col_inst].isin(selected_banks)]
            if df_assuntos_f.empty:
                df_assuntos_f = df_assuntos

            df_final_lista = df_assuntos_f.sort_values(by=col_qtd, ascending=False)
            max_val = int(df_assuntos[col_qtd].max()) if not df_assuntos.empty else 100
            if max_val <= 0: max_val = 100

            st.dataframe(
                df_final_lista[[col_inst, col_motivo, col_qtd]],
                column_config={
                    col_inst: st.column_config.TextColumn("Banco", width="small"),
                    col_motivo: st.column_config.TextColumn("Motivo da Reclamação", width="large"),
                    col_qtd: st.column_config.ProgressColumn(
                        "Volume",
                        help="Quantidade de reclamações procedentes",
                        format="%d",
                        min_value=0,
                        max_value=max_val
                    ),
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.error("❌ As colunas esperadas não foram encontradas no arquivo de assuntos.")
    else:
        st.warning("⚠️ Arquivo de assuntos não localizado ou vazio.")

    st.divider()

    c1, c2 = st.columns(2)
    with c1:
        news_path = "data/silver/stg_noticias.parquet"
        st.subheader("Volume de Notícias na Mídia")
        st.caption("Dados referentes aos últimos 7 dias móveis (de-duplicados por manchete)")
        
        if os.path.exists(news_path):
            df_raw_news = pd.read_parquet(news_path)
            df_raw_news = df_raw_news[df_raw_news['bank'].isin(selected_banks)]
            df_raw_news['published_dt'] = pd.to_datetime(df_raw_news['published'], errors='coerce', utc=True).dt.tz_convert(None)
            df_raw_news = df_raw_news.drop_duplicates(subset=['bank', 'title'], keep='first')
            
            limite_7d = pd.Timestamp.now().replace(hour=0, minute=0, second=0, microsecond=0) - pd.Timedelta(days=7)
            df_filtered_news = df_raw_news[df_raw_news['published_dt'] >= limite_7d]
            if df_filtered_news.empty:
                df_filtered_news = df_raw_news
            
            df_counts = df_filtered_news.groupby('bank').size().reset_index(name='vol_7d')
            
            fig_news = px.treemap(
                df_counts, 
                path=['bank'], 
                values='vol_7d', 
                color='bank', 
                color_discrete_map=BANK_COLORS, 
                template="plotly_dark"
            )
            
            fig_news.update_layout(
                showlegend=True, 
                legend_title_text="Instituição", 
                margin=dict(t=10, l=0, r=0, b=0), 
                paper_bgcolor='rgba(0,0,0,0)', 
                plot_bgcolor='rgba(0,0,0,0)'
            )
            fig_news.update_traces(
                textinfo="label+value", 
                hovertemplate='<b>%{label}</b><br>Notícias: %{value}', 
                textfont=dict(size=14, color="white"), 
                marker=dict(line=dict(width=1, color='#333333'))
            )
            st.plotly_chart(fig_news, use_container_width=True, config={'displayModeBar': 'hover', 'scrollZoom': False})
        else:
            fig_news = px.bar(
                df_p.sort_values('qtd_noticias_recentes'), 
                y='bank', 
                x='qtd_noticias_recentes', 
                orientation='h', 
                color='bank', 
                color_discrete_map=BANK_COLORS, 
                template="plotly_dark"
            )
            fig_news.update_layout(margin=dict(t=10, l=0, r=0, b=0))
            st.plotly_chart(fig_news, use_container_width=True, config={'displayModeBar': 'hover', 'scrollZoom': False})
        
    with c2:
        df_sorted_bcb = df_p.sort_values('indice_bcb', ascending=False)
        st.subheader("Índice de Reclamações")
        st.caption("Ranking oficial do Banco Central (BCB)")

        media_setor = df_sorted_bcb['indice_bcb'].mean()

        fig_bcb = px.scatter(
            df_sorted_bcb, 
            x='bank', 
            y='indice_bcb', 
            color='bank', 
            color_discrete_map=BANK_COLORS,
            text=df_sorted_bcb['indice_bcb'].map('{:.2f}'.format),
            template="plotly_dark"
        )

        fig_bcb.add_hline(
            y=media_setor, 
            line_dash="dash", 
            line_color="rgba(255, 255, 255, 0.5)", 
            annotation_text=f"Média: {media_setor:.2f}", 
            annotation_position="top right"
        )

        fig_bcb.update_traces(
            mode='markers+text',
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
        st.caption("Percentual de reclamações julgadas procedentes pelo BCB (menor = melhor)")
        fig_proc = px.bar(df_proc_sorted, x='bank', y='taxa_procedencia', color='bank', color_discrete_map=BANK_COLORS, text_auto='.2f', template="plotly_dark")
        fig_proc.update_traces(textposition='outside')
        fig_proc.update_layout(showlegend=False, yaxis_title="Procedência (%)", xaxis_title="", margin=dict(t=20, b=0, l=0, r=0))
        st.plotly_chart(fig_proc, use_container_width=True, config={'displayModeBar': 'hover', 'scrollZoom': False})

    with st.expander("📈 Evolução e Tendência Histórica (2025/2026)", expanded=True):
        if not df_hist.empty:
            df_hist_filtered = df_hist[df_hist['bank'].isin(selected_banks)].copy()
            if not df_hist_filtered.empty:
                df_hist_filtered = df_hist_filtered.sort_values('ordem_cronologica')
                periodos_ordenados = df_hist_filtered['periodo'].drop_duplicates().tolist()
                
                cols_met = st.columns(len(selected_banks[:4]))
                for i, b in enumerate(selected_banks[:4]):
                    with cols_met[i]:
                        va_rows = df_p[df_p['bank'] == b]['indice_bcb'].values
                        if len(va_rows) > 0:
                            va = va_rows[0]
                            bd = df_hist_filtered[df_hist_filtered['bank'] == b].sort_values('ordem_cronologica')
                            if not bd.empty:
                                bd_anteriores = bd[bd['ordem_cronologica'] < 20263]
                                vn = bd_anteriores.iloc[-1]['indice_bcb'] if not bd_anteriores.empty else bd.iloc[-1]['indice_bcb']
                                delta_pct = ((va - vn) / vn * 100) if vn != 0 else 0
                                st.metric(label=b, value=f"{va:.2f}", delta=f"{delta_pct:.1f}% vs Q2/26", delta_color="inverse")
                
                st.write("---")
                fig_ev = px.line(
                    df_hist_filtered, 
                    x='periodo', 
                    y='indice_bcb', 
                    color='bank', 
                    markers=True, 
                    template="plotly_dark", 
                    color_discrete_map=BANK_COLORS,
                    category_orders={"periodo": periodos_ordenados}
                )
                fig_ev.update_xaxes(categoryorder='array', categoryarray=periodos_ordenados)
                fig_ev.update_layout(margin=dict(t=20, b=0, l=0, r=0), xaxis_title="Trimestre", yaxis_title="Índice BCB")
                st.plotly_chart(fig_ev, use_container_width=True, config={'displayModeBar': 'hover'})
            else:
                st.info("⚠️ Selecione os bancos no filtro lateral para ver a evolução histórica.")
        else:
            st.warning("⚠️ Arquivo histórico de 2025/2026 não encontrado.")

    st.subheader("📊 Matriz de Diagnóstico")
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

    st.divider()

    if 'resumo_insight_ia' in df_p.columns and not df_p.empty:
        st.subheader("💡 Insights Estratégicos (IA)")
        focus_bank = st.selectbox(
            "Selecione um banco para ouvir a opinião da IA (GPT-OSS 120b):", 
            options=selected_banks, 
            key="focus_bank_ia"
        )
        
        if focus_bank:
            resumo = df_p[df_p['bank'] == focus_bank]['resumo_insight_ia'].values[0]
            
            st.markdown(f"**Análise da IA para {focus_bank}:**")
            
            if resumo and str(resumo).strip():
                texto = str(resumo).strip()
                texto = re.sub(r'(?:T[IÍ]TULO:|CONTEÚDO:)\s*', '', texto, flags=re.IGNORECASE)
                
                blocos_brutos = [b.strip() for b in re.split(r'\n\s*\n', texto) if b.strip()]
                
                for bloco in blocos_brutos:
                    linhas = [l.strip() for l in bloco.split('\n') if l.strip()]
                    
                    if len(linhas) >= 2:
                        titulo = re.sub(r'[\*\#\_]', '', linhas[0]).strip()
                        conteudo = "\n".join(linhas[1:])
                    elif len(linhas) == 1:
                        match_bold = re.match(r'^\*\*(.*?)\*\*\s*(.*)', linhas[0])
                        if match_bold:
                            titulo = match_bold.group(1).strip()
                            conteudo = match_bold.group(2).strip()
                        else:
                            titulo = "Destaque"
                            conteudo = linhas[0]
                    else:
                        continue

                    with st.container(border=True):
                        st.markdown(f"**🔹 {titulo}**")
                        st.write(conteudo)
            else:
                st.info("⚠️ Nenhum insight estratégico disponível para este banco.")
        st.divider()

    st.subheader("🔍 Explorador de Notícias")
    news_path = "data/silver/stg_noticias.parquet"
    
    if os.path.exists(news_path):
        df_news = pd.read_parquet(news_path)
        df_news = df_news[df_news['bank'].isin(selected_banks)]
        df_news = df_news.drop_duplicates(subset=['bank', 'title'], keep='first')
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
        st.error("❌ Erro na carga dos dados de notícias.")

st.markdown("""
    <div class="main-footer">
        Desenvolvido e sustentado por <b>Alan Cristian</b> | 
        <a href="https://www.linkedin.com/in/alancristians/" target="_blank">LinkedIn</a><br>
        <i>"Feito é melhor que perfeito"</i>
    </div>
""", unsafe_allow_html=True)

st.sidebar.markdown("---")
st.sidebar.caption(f"📅 **Sincronização:** {data_atualizacao}")
st.sidebar.markdown("[🔗 Ranking de Reclamações (BCB)](https://www.bcb.gov.br/meubc/rankingreclamacoes)")
st.sidebar.caption("Alan Cristian | Engenharia de Dados (BCB)")