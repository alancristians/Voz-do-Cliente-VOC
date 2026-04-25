# 🗣️ FinVoC: Voz do Cliente & Reputação Bancária

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge.svg)](https://voz-do-cliente-voc-2026.streamlit.app/)

O **FinVoC** (Financial Voice of Customer) é uma plataforma de inteligência de dados de alta performance que correlaciona a **exposição mediática** dos principais players do setor financeiro brasileiro com os indicadores oficiais de reclamações do Banco Central do Brasil (BCB).

O projeto evoluiu em **2026** para um modelo de processamento determinístico, eliminando gargalos de APIs externas e focando na profundidade dos dados estruturados, incluindo agora o detalhamento de **assuntos e irregularidades**.

## 🏗️ Arquitetura Medalhão (Data Pipeline)
O pipeline foi redesenhado para garantir **100% de disponibilidade** e latência zero:
* **Bronze (Raw):** Ingestão automatizada de RSS feeds (News) e datasets RDR do BCB (Ranking Geral e Ranking de Assuntos por Objeto).
* **Silver (Clean):** Normalização de esquemas, tratamento de caracteres especiais (UTF-8/Latin-1) e padronização de instituições financeiras.
* **Gold (Business):** Consolidação de métricas cruzadas, gerando uma visão 360º que integra volume de notícias, market share e eficiência de resolução.

## 🛠️ Tecnologias e Padrões
* **Linguagem:** Python 3.12
* **Engine de Dados:** Pandas & PyArrow (Processamento via Parquet para máxima performance).
* **Visualização:** Streamlit e Plotly (Dashboard interativo com identidade visual customizada).
* **CI/CD:** GitHub Actions com execução programada e persistência de dados no repositório.

## 📊 Metodologia e Indicadores
O monitor utiliza métricas oficiais para classificar as instituições:

1. **Índice de Reclamações (BCB):**
   $$\text{Índice} = \left( \frac{\text{Reclamações Procedentes}}{\text{Total de Clientes}} \right) \times 1.000.000$$

2. **Taxa de Procedência (Eficácia):**
   $$\text{Taxa \%} = \left( \frac{\text{Reclamações Procedentes}}{\text{Total Respondidas}} \right) \times 100$$
   *Indica a capacidade do banco em resolver conflitos sem a necessidade de intervenção do regulador.*

3. **Top Assuntos (VOC Qualitativo):** Identificação automática do principal motivo de insatisfação (ex: Pix, Cartão de Crédito, Atendimento) por instituição.

---

## 👨‍💻 Autor
**Alan Cristian Oliveira Freire da Silva**
* 🎓 Pós-graduando em Engenharia de Dados e Big Data - **PECE Poli-USP**
* 🔗 [LinkedIn](https://www.linkedin.com/in/alancristians/) | 📧 [E-mail](mailto:alancristiansg@gmail.com)

---
🚀 *Factum: Meu nome está em Marte no rover Perseverance da NASA (2020).*