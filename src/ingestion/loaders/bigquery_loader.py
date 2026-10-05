"""
Loader para o Google BigQuery com garantia de idempotência e deduplicação (RF-005).
Carrega registros na camada RAW (portfolio_raw.feriados) utilizando ADC (gcloud auth).
"""

from typing import List, Optional
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from src.config.settings import settings
from src.ingestion.validators.holiday_validator import HolidayIngestionRecord
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
        """Inicializa ou retorna o client do BigQuery com tratamento de credenciais padrão."""
        if self._client is not None:
            return self._client

        try:
            if settings.google_application_credentials:
                self._client = bigquery.Client.from_service_account_json(
                    settings.google_application_credentials,
                    project=self.project_id
                )
            else:
                self._client = bigquery.Client(project=self.project_id)
            return self._client
        except DefaultCredentialsError as exc:
            logger.error(
                "Credenciais GCP não encontradas! Autentique-se via 'gcloud auth application-default login' "
                "ou configure GCP_PROJECT_ID no .env."
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
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.YEAR,
            field="date"
        )
        table.clustering_fields = ["type", "name"]
        client.create_table(table, exists_ok=True)
        logger.info(f"Tabela garantida: {self.full_table_id}")

    def load_holidays_idempotent(self, records: List[HolidayIngestionRecord]) -> int:
        """
        Carrega feriados garantindo idempotência e sem duplicações (RF-005).
        Utiliza MERGE via tabela temporária de staging no BigQuery.
        Retorna o número de registros processados.
        """
        if not records:
            logger.info("Nenhum registro para carregar.")
            return 0

        client = self.get_client()
        self.ensure_dataset_and_table()

        rows_to_insert = [
            {
                "holiday_id": r.holiday_id,
                "date": r.date,
                "name": r.name,
                "type": r.type,
                "source": r.source,
                "ingested_at": r.ingested_at.isoformat()
            }
            for r in records
        ]

        temp_table_name = f"_temp_stg_{self.table_name}_{records[0].ingested_at.strftime('%Y%m%d%H%M%S')}"
        temp_table_id = f"{self.project_id}.{self.dataset_name}.{temp_table_name}"

        logger.info(f"Criando tabela temporária de carga: {temp_table_id}")
        schema = [
            bigquery.SchemaField("holiday_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        temp_table = bigquery.Table(temp_table_id, schema=schema)
        temp_table.expires = None
        client.create_table(temp_table, exists_ok=True)

        try:
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            )
            load_job = client.load_table_from_json(rows_to_insert, temp_table_id, job_config=job_config)
            load_job.result()

            merge_sql = f"""
            MERGE `{self.full_table_id}` T
            USING `{temp_table_id}` S
            ON T.holiday_id = S.holiday_id
            WHEN MATCHED THEN
                UPDATE SET
                    T.date = S.date,
                    T.name = S.name,
                    T.type = S.type,
                    T.source = S.source,
                    T.ingested_at = S.ingested_at
            WHEN NOT MATCHED THEN
                INSERT (holiday_id, date, name, type, source, ingested_at)
                VALUES (S.holiday_id, S.date, S.name, S.type, S.source, S.ingested_at);
            """

            logger.info("Executando MERGE idempotente no BigQuery...")
            query_job = client.query(merge_sql)
            query_job.result()

            logger.info(f"MERGE concluído com sucesso. {len(records)} registros sincronizados.")
            return len(records)

        finally:
            client.delete_table(temp_table_id, not_found_ok=True)
            logger.info(f"Tabela temporária {temp_table_name} removida.")
