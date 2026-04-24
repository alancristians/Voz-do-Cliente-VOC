# 🗣️ FinVoC: Voz do Cliente & Mercado

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge.svg)](https://voz-do-cliente-voc-2026.streamlit.app/)

O **FinVoC** é uma plataforma de inteligência de dados que cruza a **exposição midiática** de grandes bancos brasileiros com os indicadores oficiais de satisfação do Banco Central do Brasil (BCB).

O projeto automatiza a coleta, o tratamento e a visualização de dados, permitindo uma análise 360º entre o que a mídia reporta e o que o regulador registra em tempo real.

## 🏗️ Arquitetura de Dados
O pipeline foi desenhado seguindo as melhores práticas de Engenharia de Dados (Arquitetura Medalhão):
* **Bronze (Raw):** Ingestão de dados brutos via Google News RSS e rankings do BCB.
* **Silver (Clean):** Limpeza, tipagem e reconciliação de nomes de instituições via *Fuzzy Matching*.
* **Gold (Business):** Tabelas fato consolidadas para consumo do dashboard, garantindo performance e clareza.

## 🛠️ Tecnologias Utilizadas
* **Linguagem:** Python 3.12
* **Processamento:** Pandas
* **Visualização:** Streamlit e Plotly
* **CI/CD:** GitHub Actions com agendamento diário (Cron: `0 0 * * *`)

## 📊 Como Interpretar os Indicadores
O dashboard utiliza o **Índice de Reclamações do BCB**:
* **Cálculo**: (Reclamações Procedentes / Total de Clientes) * 1.000.000.
* **Exemplo**: O índice de **45.13** do Itaú indica aprox. 45 reclamações para cada 1 milhão de clientes.
* **Regra**: Quanto menor o índice, melhor a eficiência operacional e satisfação do cliente.

---

## 👨‍💻 Contato e Autor
**Alan Cristian Oliveira Freire da Silva**
* 📧 **E-mail:** [alancristiansg@gmail.com](mailto:alancristiansg@gmail.com)
* 🔗 **LinkedIn:** [alancristians](https://www.linkedin.com/in/alancristians/)
* 🎓 Pós-graduando em Engenharia de Dados e Big Data - Poli-USP

---
🚀 *Curiosidade: Meu nome está em Marte, gravado no rover Perseverance da NASA: [Ver Certificado](https://mars.nasa.gov/layout/embed/send-your-name/mars2020/certificate/?cn=54438721578)*