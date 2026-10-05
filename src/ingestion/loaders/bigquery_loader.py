"""
Loader para o Google BigQuery com garantia de idempotência e deduplicação (RF-005).
Carrega registros na camada RAW (portfolio_raw.feriados) utilizando ADC (gcloud auth).
"""

from typing import List, Optional
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from src.config.settings import settings
from src.ingestion.validators.holiday_validator import HolidayIngestionRecord
from src.utils.cost_guard import BigQueryCostGuard
from src.utils.logger import logger


class BigQueryLoader:
    """Responsável por carregar e sincronizar dados com a camada RAW no BigQuery."""

    def __init__(
        self,
        project_id: Optional[str] = None,
        dataset_name: Optional[str] = None,
        table_name: str = "feriados",
        location: Optional[str] = None
    ):
        self.project_id = project_id or settings.gcp_project_id
        self.dataset_name = dataset_name or settings.bigquery_dataset_raw
        self.table_name = table_name
        self.location = location or settings.gcp_location
        self.full_table_id = f"{self.project_id}.{self.dataset_name}.{self.table_name}"
        self._client: Optional[bigquery.Client] = None

    def get_client(self) -> bigquery.Client:
        """Inicializa ou retorna o client do BigQuery utilizando ADC (Application Default Credentials)."""
        if self._client is not None:
            return self._client

        try:
            self._client = bigquery.Client(project=self.project_id)
            return self._client
        except DefaultCredentialsError as exc:
            logger.error(
                "Credenciais GCP não encontradas! Execute 'gcloud auth application-default login' "
                f"para autenticar sua conta pessoal no projeto {self.project_id}."
            )
            raise exc

    def ensure_dataset_and_table(self) -> None:
        """Cria o dataset e a tabela RAW com o schema correto caso não existam."""
        client = self.get_client()

        # 1. Dataset
        dataset_ref = bigquery.DatasetReference(self.project_id, self.dataset_name)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = self.location
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset garantido: {self.dataset_name} (Location: {self.location})")

        # 2. Schema da tabela RAW (Seção 8 do PRD)
        schema = [
            bigquery.SchemaField("holiday_id", "STRING", mode="REQUIRED", description="Hash único para deduplicação"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED", description="Data do feriado"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED", description="Nome do feriado"),
            bigquery.SchemaField("type", "STRING", mode="REQUIRED", description="Tipo do feriado (national, etc)"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED", description="Origem dos dados (ex: BrasilAPI)"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED", description="Data/hora de ingestão UTC"),
        ]

        table_ref = client.dataset(self.dataset_name).table(self.table_name)
        table = bigquery.Table(table_ref, schema=schema)
        table.clustering_fields = ["type", "name"]
        client.create_table(table, exists_ok=True)
        logger.info(f"Tabela garantida: {self.full_table_id}")

    def load_holidays_idempotent(self, records: List[HolidayIngestionRecord]) -> int:
        """
        Carrega feriados garantindo idempotência e sem duplicações (RF-005).
        Utiliza Batch Load Job com desduplicação determinística em memória e carga WRITE_TRUNCATE.
        Compatível 100% com BigQuery Free Tier/Sandbox (Batch Loads gratuitos, sem dependência de DML).
        Retorna o número de registros persistidos.
        """
        if not records:
            logger.info("Nenhum registro para carregar.")
            return 0

        client = self.get_client()
        self.ensure_dataset_and_table()
        cost_guard = BigQueryCostGuard()

        table = client.get_table(self.full_table_id)
        existing_records_dict = {}

        if table.num_rows and table.num_rows > 0:
            logger.info(f"Lendo {table.num_rows} registros existentes para deduplicação idempotente...")
            read_sql = f"SELECT holiday_id, date, name, type, source, ingested_at FROM `{self.full_table_id}`"
            results, metric = cost_guard.execute_guarded_query(
                client=client,
                query=read_sql,
                step_name="read_existing_holidays"
            )
            for row in results:
                existing_records_dict[row["holiday_id"]] = {
                    "holiday_id": row["holiday_id"],
                    "date": str(row["date"]),
                    "name": row["name"],
                    "type": row["type"],
                    "source": row["source"],
                    "ingested_at": row["ingested_at"].isoformat() if hasattr(row["ingested_at"], "isoformat") else str(row["ingested_at"])
                }

        # Mescla com os novos registros (atualiza ou insere por holiday_id)
        for r in records:
            existing_records_dict[r.holiday_id] = {
                "holiday_id": r.holiday_id,
                "date": r.date,
                "name": r.name,
                "type": r.type,
                "source": r.source,
                "ingested_at": r.ingested_at.isoformat()
            }

        rows_to_load = list(existing_records_dict.values())
        logger.info(f"Carregando {len(rows_to_load)} registros desduplicados via Batch Load Job (gratuito e idempotente)...")

        schema = [
            bigquery.SchemaField("holiday_id", "STRING", mode="REQUIRED", description="Hash único para deduplicação"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED", description="Data do feriado"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED", description="Nome do feriado"),
            bigquery.SchemaField("type", "STRING", mode="REQUIRED", description="Tipo do feriado (national, etc)"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED", description="Origem dos dados (ex: BrasilAPI)"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED", description="Data/hora de ingestão UTC"),
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        load_job = client.load_table_from_json(rows_to_load, self.full_table_id, job_config=job_config)
        load_job.result()

        cost_guard.print_usage_report(client)
        logger.info(f"Carga RAW concluída com sucesso. {len(rows_to_load)} registros sincronizados em {self.full_table_id}.")
        return len(rows_to_load)
