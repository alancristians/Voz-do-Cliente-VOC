import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Voz do Cliente - VOC 2026", layout="wide")

st.title("📊 Voz do Cliente & Sentimento de Mercado")

file_path = "data/bronze/noticias_bancos.parquet"

if os.path.exists(file_path):
    df = pd.read_parquet(file_path)

    # Garantir ordenação no app (segunda camada de segurança)
    if 'published_dt' in df.columns:
        df = df.sort_values(by='published_dt', ascending=False)
    elif 'published' in df.columns:
        df['temp_date'] = pd.to_datetime(df['published'], errors='coerce')
        df = df.sort_values(by='temp_date', ascending=False)

    # --- MÉTRICAS ---
    st.subheader("📈 Volume de Notícias Recentes")
    contagem = df['bank'].value_counts()
    cols = st.columns(len(contagem))
    
    for i, (banco, qtd) in enumerate(contagem.items()):
        cols[i].metric(banco, qtd)

    st.divider()

    # --- FILTRO E LISTAGEM ---
    col_titulo, col_filtro = st.columns([2, 1])
    with col_titulo:
        st.subheader("📰 Conteúdo das Notícias (Mais recentes primeiro)")
    with col_filtro:
        banco_selecionado = st.selectbox("Filtrar por Instituição:", 
                                         ["Todos"] + list(df['bank'].unique()))
    
    df_display = df if banco_selecionado == "Todos" else df[df['bank'] == banco_selecionado]

    # Lista de Notícias
    for _, row in df_display.iterrows():
        with st.container(border=True):
            # Layout de linha para título e data
            c1, c2 = st.columns([4, 1])
            c1.markdown(f"### {row['title']}")
            c2.write(f"⏱️ {row['published']}")
            
            st.markdown(f"**Fonte:** {row['source']} | **Banco:** {row['bank']}")
            st.write(row['summary'])
            st.link_button("Ver notícia original", row['link'])

else:
    st.error("Dados não encontrados. Verifique se o GitHub Action rodou com sucesso!")