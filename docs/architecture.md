# 🏛️ Arquitetura Detalhada — Data Ingestion Pipeline

## 1. Princípios Arquiteturais

A solução foi concebida sob os seguintes pilares:

1. **Separação Rígida de Responsabilidades**:
   - **Ingestão (Python)**: Responsável unicamente pela comunicação HTTP com fontes externas, validação contratual de esquemas e persistência atômica/idempotente na camada RAW.
   - **Armazenamento (BigQuery)**: Repositório central de dados particionado em camadas lógicas (RAW, STAGING, INTERMEDIATE, MARTS).
   - **Transformação & Modelagem (dbt)**: Padronização, limpeza, regras de negócio e testes de qualidade de dados executados diretamente na computação distribuída do BigQuery.
2. **Idempotência Garantida**: Nenhuma reexecução da pipeline gera duplicação de dados, assegurada por hashing determinístico (`SHA-256(date_name_type)`) e comandos SQL `MERGE`.
3. **Segurança por Padrão**: Credenciais não são persistidas no código nem no repositório Git. Utiliza-se *Google Application Default Credentials (ADC)*.
4. **Extensibilidade**: A inclusão de novas fontes (ex: APIs de clima, bancos transacionais) requer apenas a criação de um novo client em `src/ingestion/clients/` sem necessidade de alterar o núcleo do orquestrador.

---

## 2. Camadas do Data Warehouse

```text
+---------------------------------------------------------------------------------------+
| CAMADA RAW (portfolio_raw)                                                            |
| • Dados inalterados da fonte com metadados técnicos (holiday_id, source, ingested_at).|
| • Particionamento anual por date, clusterização por type e name.                      |
+-------------------------------------------+-------------------------------------------+
                                            |
                                            v
+---------------------------------------------------------------------------------------+
| CAMADA STAGING (portfolio_staging - Views dbt)                                        |
| • stg_feriados: tipagem explícita (DATE, TIMESTAMP), strings normalizadas, deduplicação|
| • stg_sales: dados transacionais padronizados.                                        |
+-------------------------------------------+-------------------------------------------+
                                            |
                                            v
+---------------------------------------------------------------------------------------+
| CAMADA INTERMEDIATE (portfolio_intermediate - Tabelas dbt)                            |
| • int_calendario_comercial: série temporal diária (2024-2026), identificação de dias   |
|   úteis, fins de semana, proximidade de feriados (days_to_holiday, is_pre_holiday_window)|
+-------------------------------------------+-------------------------------------------+
                                            |
                                            v
+---------------------------------------------------------------------------------------+
| CAMADA MARTS (portfolio_marts - Tabelas dbt)                                          |
| • dim_calendario: dimensão tempo para BI.                                             |
| • fct_sales: fato transacional enriquecido com contexto de varejo/feriados.           |
| • mart_oportunidades_comerciais: métricas comparativas e classificação de campanhas.  |
+---------------------------------------------------------------------------------------+
```
