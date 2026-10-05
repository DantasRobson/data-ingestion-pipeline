# 🚀 Data Ingestion & Analytics Pipeline — Portfólio de Engenharia de Dados

[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Google Cloud](https://img.shields.io/badge/Google_Cloud-BigQuery-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![dbt](https://img.shields.io/badge/dbt-Core-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://getdbt.com)
[![Pydantic](https://img.shields.io/badge/Pydantic-v2-E92063?style=for-the-badge&logo=pydantic&logoColor=white)](https://pydantic.dev)
[![Architecture](https://img.shields.io/badge/Medallion-RAW%20%7C%20Staging%20%7C%20Intermediate%20%7C%20Marts-00A86B?style=for-the-badge)](https://cloud.google.com)

---

## 1. 📌 Visão Geral & Objetivo

Este projeto foi desenvolvido como **demonstração prática de Engenharia de Dados de ponta a ponta**. 

O objetivo principal vai além do simples consumo de endpoints REST: trata-se da implementação de uma **pipeline moderna, modular e orientada a contratos**, integrando dados brutos da web, persistindo-os de maneira idempotente no **Google BigQuery** e transformando-os através do **dbt** em modelos analíticos de negócio prontos para consumo por ferramentas de **BI (Power BI, Looker Studio, Tableau)**.

### Caso de Negócio: Oportunidades Comerciais & Calendário
Feriados e datas especiais alteram sazonalmente o comportamento de consumo e a demanda do varejo. O projeto responde a perguntas estratégicas:
- Quais produtos sofrem aceleração de vendas nos dias anteriores a datas comemorativas?
- Com quantos dias de antecedência (D-7 a D-3) campanhas promocionais devem ser iniciadas?
- Quais categorias apresentam oportunidade comercial **ALTA**, **MÉDIA** ou **BAIXA**?

---

## 2. 🏗️ Arquitetura da Solução

A esteira adota o padrão em camadas desacopladas (Arquitetura Medalhão / Modern Data Stack):

```text
                 ┌──────────────────────────┐
                 │        BrasilAPI         │  (REST API Externa)
                 │    Feriados Nacionais    │
                 └────────────┬─────────────┘
                              │ HTTP GET
                              ▼
                 ┌──────────────────────────┐
                 │     Python Ingestion     │  • Cliente HTTP com Retries & Backoff
                 │        (src/ingestion)   │  • Validação Contratual via Pydantic
                 │                          │  • Métricas & Observabilidade (RF-004)
                 └────────────┬─────────────┘
                              │ Idempotent MERGE (hash date+name+type)
                              ▼
                 ┌──────────────────────────┐
                 │  BigQuery RAW (portfolio)│  • Tabela: feriados (Particionada)
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
     │  │ • mart_oportunidades_comerciais           │  │ Regras de negócio, scoring & recomendações
     │  └───────────────────────────────────────────┘  │
     └────────────────────────┬────────────────────────┘
                              │
                              ▼
                 ┌──────────────────────────┐
                 │   BI & Decisão Estratégica│
                 │ (Looker / Power BI / SQL)│
                 └──────────────────────────┘
```

---

## 3. 🛡️ Segurança & Autenticação (Zero Credentials no Git)

Em conformidade com as melhores práticas de segurança de dados:
- **Nenhuma chave privada, token ou Service Account JSON é commitada no Git.**
- O repositório versiona apenas [.env.example](file:///.env.example).
- O arquivo `.env` está explicitamente protegido no [.gitignore](file:///.gitignore).

### Autenticação no Google Cloud
A pipeline utiliza o mecanismo padrão de autenticação do Google Cloud (Application Default Credentials):
```bash
gcloud auth application-default login
```
Desta forma, os clientes da Google Cloud SDK e dbt herdam a autenticação do seu ambiente local sem a necessidade de expor arquivos sensíveis.

---

## 4. 🗄️ Estrutura de Datasets no BigQuery

O ambiente é particionado em datasets dedicados por responsabilidade analítica:

| Dataset | Finalidade | Principais Objetos |
| :--- | :--- | :--- |
| `portfolio_raw` | Armazenamento dos dados brutos recebidos da fonte | `feriados` (particionada por ano/data, clusterizada por tipo/nome) |
| `portfolio_staging` | Views de limpeza estrutural e normalização de tipos | `stg_feriados`, `stg_sales` |
| `portfolio_intermediate` | Cruzamentos intermediários e cálculos temporais | `int_calendario_comercial` |
| `portfolio_marts` | Modelos analíticos finais prontos para dashboards | `dim_calendario`, `fct_sales`, `mart_oportunidades_comerciais` |

---

## 5. 💻 Estrutura do Código

```text
data-ingestion-pipeline/
├── src/
│   ├── config/
│   │   └── settings.py          # Leitura validada de variáveis de ambiente (Pydantic)
│   ├── ingestion/
│   │   ├── clients/
│   │   │   └── brasilapi_client.py # Cliente HTTP resiliente (retries e timeouts)
│   │   ├── validators/
│   │   │   └── holiday_validator.py# Validação de schema e geração de hash SHA-256
│   │   ├── loaders/
│   │   │   └── bigquery_loader.py  # Carga atômica e idempotente (MERGE SQL)
│   │   └── pipeline.py          # Orquestrador da ingestão com telemetria
│   └── utils/
│       └── logger.py            # Logging estruturado e métricas de observabilidade
├── dbt/
│   ├── models/
│   │   ├── staging/             # stg_feriados.sql, stg_sales.sql
│   │   ├── intermediate/        # int_calendario_comercial.sql
│   │   └── marts/               # dim_calendario.sql, fct_sales.sql, mart_oportunidades_comerciais.sql
│   ├── seeds/
│   │   └── raw_sales.csv        # Dataset sintético de vendas para o case comercial
│   ├── tests/
│   │   └── assert_positive_sales_revenue.sql # Teste singular de integridade de faturamento
│   ├── dbt_project.yml
│   └── profiles.yml
├── tests/
│   └── test_brasilapi_ingestion.py # Testes unitários com pytest
├── main.py                      # CLI central para execução ponta a ponta
├── requirements.txt             # Dependências declaradas
├── .env.example                 # Template de variáveis de ambiente
└── README.md
```

---

## 6. 🚀 Como Executar

### Pré-requisitos
- Python 3.10 ou superior instalado
- Google Cloud SDK (`gcloud`) instalado e autenticado
- Um projeto GCP ativo com faturamento configurado

### 1. Clonar o repositório e preparar o ambiente
```bash
git clone https://github.com/RobsonDantas/data-ingestion-pipeline.git
cd data-ingestion-pipeline
python -m venv .venv
source .venv/bin/activate  # No Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configurar o arquivo `.env`
Copie o arquivo de exemplo:
```bash
cp .env.example .env
```
Preencha o `GCP_PROJECT_ID` com o ID do seu projeto no GCP:
```env
GCP_PROJECT_ID=meu-projeto-gcp
GCP_LOCATION=US
BIGQUERY_DATASET_RAW=portfolio_raw
BIGQUERY_DATASET_STAGING=portfolio_staging
BIGQUERY_DATASET_INTERMEDIATE=portfolio_intermediate
BIGQUERY_DATASET_MARTS=portfolio_marts
```

### 3. Autenticar no GCP
```bash
gcloud auth application-default login
```

### 4. Executar a Pipeline de Ingestão

**Modo de teste rápido (Dry Run — sem gravação no BigQuery):**
```bash
python main.py --dry-run
```

**Modo completo (Ingestão + Carga Idempotente no BigQuery):**
```bash
python main.py
```

**Executar Ingestão + Transformações dbt em comando único:**
```bash
python main.py --run-dbt
```

---

## 7. 🔄 Transformações dbt & Modelagem

Para rodar diretamente os comandos dbt:
```bash
cd dbt
# Carrega seed sintético de vendas
dbt seed

# Constrói e testa todos os modelos
dbt build

# Roda apenas os testes de Data Quality
dbt test
```

### Regra de Negócio Implementada (`mart_oportunidades_comerciais`)
O modelo calcula a média de vendas de cada categoria em dias comuns versus a média registrada na janela pré-feriado (D-7 a D-1). A regra classifica a oportunidade conforme a fórmula:

$$\text{Crescimento \%} = \frac{\bar{X}_{\text{pré-feriado}} - \bar{X}_{\text{comum}}}{\bar{X}_{\text{comum}}} \times 100$$

- **ALTA**: Crescimento $\ge 30\%$ $\rightarrow$ *"Antecipar campanhas promocionais em D-7 e reforçar estoque."*
- **MÉDIA**: Crescimento entre $15\%$ e $29.9\%$ $\rightarrow$ *"Disparar comunicações em D-5 com foco em conversão."*
- **BAIXA**: Crescimento $< 15\%$ $\rightarrow$ *"Manter estratégia padrão sem custo extraordinário de mídia."*

---

## 8. 🧪 Data Quality & Testes

A integridade dos dados é validada em duas etapas:
1. **Camada de Ingestão Python**: Validação de contratos com Pydantic (`HolidayRawItem`), rejeitando formatos inválidos de data ou estruturas corrompidas.
2. **Camada dbt**:
   - `unique` e `not_null` nas chaves primárias (`holiday_id`, `date`, `sale_id`).
   - `relationships` garantindo integridade referencial entre `fct_sales` e `dim_calendario`.
   - `accepted_values` validando domínios (`opportunity_score` $\in$ `['ALTA', 'MEDIA', 'BAIXA']`).
   - `assert_positive_sales_revenue.sql`: garante `revenue >= 0` e `quantity > 0`.

Para rodar a suíte de testes unitários:
```bash
pytest tests/ -v
```

---

## 9. 📈 Observabilidade & Telemetria

Toda execução registra logs formatados e gera métricas padronizadas:
```text
2026-10-05 11:27:45 [INFO] [data_pipeline] ============================================================
2026-10-05 11:27:45 [INFO] [data_pipeline] 📊 RESUMO DE EXECUÇÃO: brasilapi_holidays
2026-10-05 11:27:45 [INFO] [data_pipeline] ID da Execução    : 20261005_142744
2026-10-05 11:27:45 [INFO] [data_pipeline] Status Final      : SUCCESS
2026-10-05 11:27:45 [INFO] [data_pipeline] Registros Obtidos : 42
2026-10-05 11:27:45 [INFO] [data_pipeline] Registros Carreg. : 42
2026-10-05 11:27:45 [INFO] [data_pipeline] Duração (segundos): 0.11s
2026-10-05 11:27:45 [INFO] [data_pipeline] ============================================================
```

---

## 10. 🔮 Próximos Passos & Extensões

- [ ] Ingestão de API meteorológica (Clima) para cruzamento de impacto climático nas vendas.
- [ ] Orquestração via Apache Airflow ou GitHub Actions com agendamento diário.
- [ ] Dashboard analítico interativo no Looker Studio conectado diretamente ao `portfolio_marts`.

---

## 👨‍💻 Autor

**Robson Dantas de Melo**  
- [LinkedIn](https://linkedin.com)  
- [GitHub](https://github.com/RobsonDantas)
