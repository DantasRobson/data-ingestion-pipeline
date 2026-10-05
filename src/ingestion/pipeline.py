"""
Pipeline principal de ingestão de dados.
Executa a extração da BrasilAPI, validação com Pydantic e carga idempotente no BigQuery.
Suporta execução via: python -m src.ingestion.pipeline
"""

import argparse
import sys
from typing import List, Optional
from src.config.settings import settings
from src.ingestion.clients.brasilapi_client import BrasilAPIClient
from src.ingestion.loaders.bigquery_loader import BigQueryLoader
from src.ingestion.validators.holiday_validator import validate_and_parse_holidays
from src.utils.logger import ExecutionMetrics, logger


def run_pipeline(dry_run: bool = False, custom_years: Optional[List[int]] = None) -> ExecutionMetrics:
    """Executa o ciclo completo de ingestão da BrasilAPI para o BigQuery RAW."""
    metrics = ExecutionMetrics(pipeline_name="brasilapi_holidays")
    logger.info(f"Iniciando pipeline {metrics.pipeline_name} (Execution ID: {metrics.execution_id})")

    years = custom_years or settings.parsed_ingestion_years
    logger.info(f"Anos configurados para extração: {years}")

    try:
        # 1. Extração via BrasilAPI
        client = BrasilAPIClient()
        raw_holidays = client.fetch_multiple_years(years)
        records_received = len(raw_holidays)
        logger.info(f"Extração concluída: {records_received} registros recebidos da BrasilAPI.")

        if records_received == 0:
            logger.warning("Nenhum dado retornado pela API. Encerrando pipeline.")
            metrics.finish(status="SUCCESS", records_received=0, records_loaded=0)
            metrics.print_summary()
            return metrics

        # 2. Validação e enriquecimento de schema
        logger.info("Validando integridade dos dados e gerando chaves determinísticas...")
        validated_records = validate_and_parse_holidays(raw_holidays, source="BrasilAPI")
        logger.info(f"Validação bem-sucedida: {len(validated_records)} registros íntegros.")

        # 3. Carga no BigQuery (ou simulação dry-run)
        if dry_run:
            logger.info("MODO DRY-RUN ATIVO: Carga no BigQuery ignorada. Dados validados com sucesso:")
            for sample in validated_records[:3]:
                logger.info(f"   Amostra -> Data: {sample.date} | Nome: {sample.name} | Tipo: {sample.type}")
            records_loaded = len(validated_records)
        else:
            logger.info("Iniciando carga idempotente na camada RAW do BigQuery...")
            loader = BigQueryLoader()
            records_loaded = loader.load_holidays_idempotent(validated_records)

        metrics.finish(
            status="SUCCESS",
            records_received=records_received,
            records_loaded=records_loaded
        )

    except Exception as exc:
        logger.exception(f"Falha durante a execução da pipeline: {exc}")
        metrics.finish(
            status="FAILED",
            records_received=locals().get("records_received", 0),
            records_loaded=0,
            error=str(exc)
        )

    metrics.print_summary()
    return metrics


def main():
    parser = argparse.ArgumentParser(description="Pipeline de Ingestão de Feriados - BrasilAPI para BigQuery")
    parser.add_argument("--dry-run", action="store_true", help="Executa extração e validação sem persistir no BigQuery")
    parser.add_argument("--years", type=str, default=None, help="Lista de anos separados por vírgula (ex: 2024,2025,2026)")
    args = parser.parse_args()

    custom_years = [int(y.strip()) for y in args.years.split(",") if y.strip()] if args.years else None
    metrics = run_pipeline(dry_run=args.dry_run, custom_years=custom_years)

    if metrics.status != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
