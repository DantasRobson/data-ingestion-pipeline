"""
Módulo de configuração centralizada do Data Ingestion Pipeline.
Carrega variáveis de ambiente de forma segura e validada via Pydantic.
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

    # GCP & BigQuery
    gcp_project_id: str = Field(default="myralisdev", alias="GCP_PROJECT_ID")
    gcp_location: str = Field(default="US", alias="GCP_LOCATION")
    bigquery_dataset_raw: str = Field(default="portfolio_raw", alias="BIGQUERY_DATASET_RAW")
    bigquery_dataset_staging: str = Field(default="portfolio_staging", alias="BIGQUERY_DATASET_STAGING")
    bigquery_dataset_intermediate: str = Field(default="portfolio_intermediate", alias="BIGQUERY_DATASET_INTERMEDIATE")
    bigquery_dataset_marts: str = Field(default="portfolio_marts", alias="BIGQUERY_DATASET_MARTS")
    google_application_credentials: str | None = Field(default=None, alias="GOOGLE_APPLICATION_CREDENTIALS")

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
