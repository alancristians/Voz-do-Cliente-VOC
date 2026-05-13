# 🛡️ FinVoC 2.0 - Voz do Cliente | Monitor Inteligente de Experiência Bancária

**FinVoC (Financial Voice of Customer)** é um ecossistema de Engenharia de Dados de ponta a ponta, projetado para monitorar e analisar a reputação das principais instituições financeiras do Brasil. O projeto combina dados oficiais, notícias de mercado e **Inteligência Artificial Generativa** para transformar dados brutos em insights estratégicos para o setor bancário.

---

## 🌐 Dashboard em Tempo Real
👉 **[Acesse o Monitor FinVoC 2.0 no Streamlit](https://voz-do-cliente-voc-2026.streamlit.app/)**

---

## ✨ Novidades da Versão 2.0
* **🧠 Inteligência Artificial (LLM):** Integração com **Llama 3.3 (via Groq API)** para análise automatizada de sentimentos e geração de resumos estratégicos sobre o cenário de cada instituição.
* **🤖 Automação Total (ETL):** Pipeline 100% automatizado via **GitHub Actions**, com execuções diárias (Cron) para garantir dados sempre atualizados e integridade dos arquivos.
* **📊 Camada Gold Avançada:** Enriquecimento de dados cruzando o Ranking de Reclamações do BCB com o volume de exposição na mídia (Google News).

## 🚀 Principais Funcionalidades
* **Resumo Executivo (IA):** Cards inteligentes gerados por LLM que sintetizam o "humor" do mercado para cada player.
* **KPIs de Desempenho:** Monitoramento de Índice de Reclamações, Taxa de Procedência e Market Share de contas ativas.
* **Voz do Mercado:** Ingestão dinâmica de notícias focada em menções recentes e tendências de reputação.
* **Matriz de Diagnóstico:** Comparativo técnico de eficiência entre Bancos Tradicionais e Neobanks (C6, Nubank, Inter, PicPay, etc).

## 🏗️ Arquitetura de Dados (Medallion)
O fluxo de dados segue rigorosamente as melhores práticas de arquitetura de dados moderna:

1.  **🥉 Camada Bronze:** Ingestão de dados brutos do Ranking BCB, Google News e metadados do Consumidor.gov em formato Parquet/CSV.
2.  **🥈 Camada Silver:** Limpeza profunda, padronização de nomes de instituições (Fuzzy Matching) e tratamento de tipagem de dados.
3.  **🥇 Camada Gold:** Cruzamento de bases, enriquecimento via **IA Generativa** e consolidação na `fact_finvoc_summary.csv`, otimizada para consumo analítico.

## 🛠️ Stack Tecnológica
* **Linguagem:** Python 3.11+
* **LLM/IA:** Llama 3.3 (Groq Cloud Inference)
* **Automação/CI-CD:** GitHub Actions
* **Processamento:** Pandas & PyArrow
* **Visualização:** Streamlit & Plotly Express
* **Armazenamento:** Arquitetura Medalhão (Versionada via Git)

## 📊 Status e Referência
* **Dados de Referência:** 1º Trimestre de 2026 (Dados oficiais BCB).
* **Instituições Monitoradas:** Itaú, Bradesco, Santander, Banco do Brasil, Caixa, Nubank, C6 Bank, BTG Pactual, Inter e PicPay.

---
**Desenvolvido por:** [Alan Cristian Oliveira Freire da Silva](https://github.com/alancristians)  
*Projeto Pessoal de Engenharia de Dados*