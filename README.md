# 🛡️ FinVoC 2.0 - Monitor de Reputação Bancária

**FinVoC (Financial Voice of Customer)** é um ecossistema de Engenharia de Dados desenvolvido para monitorar a reputação das principais instituições financeiras no Brasil. O projeto utiliza uma **Arquitetura Medalhão (Bronze, Silver e Gold)** para processar dados oficiais do Banco Central e menções na mídia em tempo real.

---

## 🌐 Acesse o Dashboard
👉 **[Clique aqui para visualizar o FinVoC 2.0 no Streamlit](https://seu-app.streamlit.app)** *(Substitua o link acima pela sua URL oficial)*

---

## 🚀 Funcionalidades Atuais
* **KPIs de Desempenho:** Visão consolidada de bancos analisados, exposição na mídia, média de índice de reclamações e total de contas ativas.
* **Voz do Mercado:** Monitoramento dinâmico via Google News para capturar a exposição de cada marca.
* **Market Share (BCB):** Comparativo de volume de clientes entre os grandes bancos e os principais players digitais (C6, Nubank, Inter, etc).
* **Matriz de Diagnóstico:** Tabela técnica para análise de eficiência (Taxa de Procedência) e Índice BCB por instituição.
* **Explorador de Notícias:** Ferramenta de busca integrada para análise qualitativa de menções recentes.

## 🏗️ Arquitetura de Dados (Medallion)
O pipeline é automatizado via **GitHub Actions** e segue o fluxo:

1.  **🥉 Camada Bronze:** Coleta de dados brutos do Ranking de Reclamações (BCB) e Google News (Parquet).
2.  **🥈 Camada Silver:** Limpeza, padronização de nomes de instituições e tratamento de tipagem de dados.
3.  **🥇 Camada Gold:** Enriquecimento e cruzamento de bases para geração do arquivo `fact_finvoc_summary.csv` que alimenta o dashboard.

## 🛠️ Stack Tecnológica
* **Linguagem:** Python 3.11
* **Dashboard:** Streamlit
* **Processamento:** Pandas
* **Visualização:** Plotly Express
* **Automação:** GitHub Actions (Workflow CI/CD)

## 📊 Status do Projeto (Abril/2026)
* **Dados de Referência:** 1º Trimestre de 2026.
* **Instituições Monitoradas:** Itaú, Bradesco, Santander, Banco do Brasil, Caixa, Nubank, C6 Bank, BTG Pactual, Inter e PicPay.

---
**Desenvolvido por:** Alan Cristian Oliveira Freire da Silva  
**Projeto Pessoal de Engenharia de Dados** 