# 🚀 Data Ingestion & Analytics Pipeline — Portfólio de Engenharia de Dados

[![Python 3.14](https://img.shields.io/badge/Python-3.14-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Google Cloud](https://img.shields.io/badge/Google_Cloud-BigQuery-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![dbt](https://img.shields.io/badge/dbt-BigQuery-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://getdbt.com)
[![Pydantic](https://img.shields.io/badge/Pydantic-v2-E92063?style=for-the-badge&logo=pydantic&logoColor=white)](https://pydantic.dev)
[![Cost Guard](https://img.shields.io/badge/Cost_Guard-Always_Free_Circuit_Breaker-00A86B?style=for-the-badge)](https://cloud.google.com/free)

---

## 1. 📌 Visão Geral & Objetivo

Este projeto foi desenvolvido como **demonstração prática de Engenharia de Dados moderna de ponta a ponta**. 

O pipeline integra dados externos da **BrasilAPI (Feriados Nacionais)**, valida sua integridade contratual via **Pydantic**, persiste-os de forma idempotente no **Google BigQuery** (Dataset `pipeline_ingestao`, região `southamerica-east1`) e modela regras de negócio através do **dbt**, cruzando o calendário com dados de vendas para identificar **oportunidades comerciais e períodos promocionais no varejo**.

### 🛡️ Diferencial Arquitetural: Cost & Usage Guard
A arquitetura conta com uma camada nativa e preventiva de controle de consumo e custos para o BigQuery:
* **Circuit Breaker Preventivo:** Garante que a aplicação opere estritamente dentro da franquia gratuita do Google Cloud (*Always Free*: 1 TiB/mês), bloqueando preventivamente qualquer consulta que ameace ultrapassar o teto operacional de **50% (512 GiB)**.
* **Dry Run Obrigatório:** Simula o custo de bytes escaneados antes de disparar o job real (**0 bytes faturados na estimativa**).
* **Trava Nativa `maximum_bytes_billed`:** Proteção no hardware do BigQuery contra varreduras acidentais.
* **Cálculo Acumulado a Custo Zero:** Monitora o consumo acumulado do mês através da API de Jobs do Google Cloud sem realizar queries pagas no `INFORMATION_SCHEMA`.

---

## 2. 🏗️ Arquitetura da Solução

```text
                 ┌──────────────────────────┐
                 │        BrasilAPI         │  (REST API Externa)
                 │    Feriados Nacionais    │
                 └────────────┬─────────────┘
                              │ HTTP GET (com retries e backoff)
                              ▼
                 ┌──────────────────────────┐
                 │     Python Ingestion     │  • Contratos de Dados com Pydantic v2
                 │        (src/ingestion)   │  • Geração de chave natural determinística SHA-256
                 └────────────┬─────────────┘
                              │
                              ▼
                 ┌──────────────────────────┐
                 │   Cost & Usage Guard     │  • Dry Run preventivo (0 bytes cobrados)
                 │   (src/utils/cost_guard) │  • Circuit Breaker (Teto operacional: 50%)
                 │                          │  • maximum_bytes_billed ativo
                 └────────────┬─────────────┘
                              │ Batch Load Job (100% gratuito no Free Tier)
                              ▼
                 ┌──────────────────────────┐
                 │  BigQuery RAW Pessoal    │  • Projeto: projetoportifolio-492813
                 │   (pipeline_ingestao)    │  • Região: southamerica-east1 (São Paulo)
                 └────────────┬─────────────┘
                              │
     ┌────────────────────────┴────────────────────────┐
     │                     dbt                         │
     │                                                 │
     │  ┌───────────────────────────────────────────┐  │
     │  │ STAGING (stg_feriados, stg_sales)         │  │ Normalização, casts & deduplicação
     │  └─────────────────────┬─────────────────────┘  │
     │                        │                        │
     │  ┌─────────────────────▼─────────────────────┐  │
     │  │ INTERMEDIATE (int_calendario_comercial)   │  │ Série temporal + proximidade de feriados
     │  └─────────────────────┬─────────────────────┘  │
     │                        │                        │
     │  ┌─────────────────────▼─────────────────────┐  │
     │  │ MARTS                                     │  │
     │  │ • dim_calendario                          │  │ Dimensão analítica completa
     │  │ • fct_sales                               │  │ Fato transacional contextualizado
     │  │ • mart_oportunidades_comerciais           │  │ Scoring (ALTA/MÉDIA/BAIXA) & Recomendações
     │  └───────────────────────────────────────────┘  │
     └────────────────────────┬────────────────────────┘
                              │
                              ▼
                 ┌──────────────────────────┐
                 │   BI & Decisão de Negócio │
                 │ (Looker / Power BI / SQL)│
                 └──────────────────────────┘
```

---

## 3. 🔒 Segurança & Autenticação (Zero Credentials no Git)

Em total conformidade com as regras de segurança:
- **Nenhuma chave privada, token ou Service Account JSON é commitada ou criada no projeto.**
- A autenticação utiliza o padrão do Google Cloud: **Application Default Credentials (ADC)** vinculado à conta Google pessoal.
- O repositório versiona apenas [.env.example](file:///.env.example).
- O arquivo `.env` e os dados locais de histórico estão protegidos no [.gitignore](file:///.gitignore).

---

## 4. 💻 Estrutura do Repositório

```text
data-ingestion-pipeline/
├── .venv/                       # Ambiente virtual Python isolado
├── src/
│   ├── config/
│   │   └── settings.py          # Configurações validadas via Pydantic
│   ├── ingestion/
│   │   ├── clients/
│   │   │   └── brasilapi_client.py # Cliente HTTP resiliente (retries e timeouts)
│   │   ├── validators/
│   │   │   └── holiday_validator.py# Validação contratual de schema (Pydantic)
│   │   ├── loaders/
│   │   │   └── bigquery_loader.py  # Carga idempotente no BigQuery via ADC
│   │   └── pipeline.py          # Orquestrador da ingestão com telemetria
│   └── utils/
│       ├── logger.py            # Logging estruturado e métricas
│       └── cost_guard.py        # Cost & Usage Guard com Circuit Breaker
├── dbt/
│   ├── models/
│   │   ├── staging/             # stg_feriados.sql, stg_sales.sql
│   │   ├── intermediate/        # int_calendario_comercial.sql
│   │   └── marts/               # dim_calendario.sql, fct_sales.sql, mart_oportunidades_comerciais.sql
│   ├── seeds/
│   │   └── raw_sales.csv        # Dataset de vendas demonstrativo
│   ├── tests/
│   │   └── assert_positive_sales_revenue.sql # Teste singular de negócio
│   ├── dbt_project.yml
│   └── profiles.yml
├── tests/
│   ├── test_bigquery_connection.py # Teste de autenticação ADC e identificação de projeto
│   ├── test_bigquery_read_write.py # Teste de escrita e leitura controlada no BigQuery
│   ├── test_brasilapi_ingestion.py # Testes unitários da extração e validação
│   └── test_cost_guard.py          # Testes do Circuit Breaker e níveis de alerta
├── main.py                      # CLI central do pipeline
├── requirements.txt             # Dependências Python
├── .env.example                 # Exemplo de configuração
├── .gitignore                   # Proteção de credenciais e caches
└── README.md
```

---

## 5. 🚀 Como Executar Localmente

### 1. Clonar e Ativar o Ambiente Virtual
```powershell
git clone https://github.com/DantasRobson/data-ingestion-pipeline.git
cd data-ingestion-pipeline

# Criar e ativar o .venv
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Instalar dependências
pip install -r requirements.txt
```

### 2. Autenticação no Google Cloud (ADC Pessoal)
```powershell
gcloud auth login
gcloud config set project projetoportifolio-492813
gcloud auth application-default login
```

### 3. Configuração do `.env`
Copie o template e preencha as variáveis de ambiente:
```powershell
Copy-Item .env.example .env
```
Conteúdo do [.env](file:///.env):
```env
GCP_PROJECT_ID=projetoportifolio-492813
BQ_LOCATION=southamerica-east1
BQ_DATASET=pipeline_ingestao
```

### 4. Executar os Testes Automatizados
```powershell
pytest tests/ -v
```

### 5. Executar o Pipeline
```powershell
# Modo de simulação (Dry Run - sem escrita no BigQuery):
python main.py --dry-run

# Execução completa com carga no BigQuery pessoal:
python main.py
```

---

## 6. 📊 Painel do Cost & Usage Guard em Operação

Ao término de cada operação no BigQuery, o guardião emite o relatório em tempo real:

```text
================================================================================
🛡️ BIGQUERY COST & USAGE GUARD — RELATÓRIO DE CONSUMO
Projeto Verificado         : projetoportifolio-492813
Mês de Referência          : 2026-10
Última Operação            : leitura_teste_conectividade (Job: ca866e5a-fbad-4718)
Bytes Processados (Job)    : 87 bytes (0.00 MB)
Bytes Faturados (Job)      : 10,485,760 bytes (10.00 MB)
Custo Estimado da Query    : US$ 0.0001 (Isento na franquia Always Free)
--------------------------------------------------------------------------------
Consumo Acumulado no Mês   : 0.01 GiB de 1024.0 GiB
Uso da Franquia Always Free: 0.00% (Restante: 100.00%)
Teto Operacional Seguro    : 512.0 GiB (50.0% da franquia)
Uso do Teto Operacional    : 0.00%
Saldo Seguro Restante      : 511.99 GiB
Nível de Alerta Atual      : NORMAL
Status do Circuit Breaker  : DESARMADO (OPERAÇÃO SEGURA)
================================================================================
```

---

## 👨‍💻 Autor

**Robson Dantas de Melo**  
- [GitHub](https://github.com/DantasRobson)  
- [LinkedIn](https://linkedin.com)
