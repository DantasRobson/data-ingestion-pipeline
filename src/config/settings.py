"""
Módulo de configuração centralizada do Data Ingestion Pipeline.
Carrega variáveis de ambiente de forma segura e validada via Pydantic (ADC nativo).
"""

from typing import List
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # GCP & BigQuery Pessoal (ADC)
    gcp_project_id: str = Field(default="projetoportifolio-492813", alias="GCP_PROJECT_ID")
    gcp_location: str = Field(default="southamerica-east1", alias="BQ_LOCATION")
    bq_dataset: str = Field(default="pipeline_ingestao", alias="BQ_DATASET")

    # Datasets analíticos por camada
    bigquery_dataset_raw: str = Field(default="portfolio_raw", alias="BIGQUERY_DATASET_RAW")
    bigquery_dataset_staging: str = Field(default="portfolio_staging", alias="BIGQUERY_DATASET_STAGING")
    bigquery_dataset_intermediate: str = Field(default="portfolio_intermediate", alias="BIGQUERY_DATASET_INTERMEDIATE")
    bigquery_dataset_marts: str = Field(default="portfolio_marts", alias="BIGQUERY_DATASET_MARTS")

    # BigQuery Cost & Usage Guard (Always Free: 1 TiB/mês)
    bq_free_tier_bytes: int = Field(default=1099511627776, alias="BQ_FREE_TIER_BYTES")  # 1 TiB
    bq_operational_limit_pct: float = Field(default=50.0, alias="BQ_OPERATIONAL_LIMIT_PCT")  # Teto 50%
    bq_max_bytes_per_query: int = Field(default=10737418240, alias="BQ_MAX_BYTES_PER_QUERY")  # 10 GiB/query

    # BrasilAPI
    brasilapi_base_url: str = Field(default="https://brasilapi.com.br/api", alias="BRASILAPI_BASE_URL")
    ingestion_years: str = Field(default="2024,2025,2026", alias="INGESTION_YEARS")

    # Observabilidade & Logs
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @property
    def parsed_ingestion_years(self) -> List[int]:
        """Converte a string separada por vírgula em lista de inteiros."""
        return [int(y.strip()) for y in self.ingestion_years.split(",") if y.strip()]


settings = Settings()
