#!/usr/bin/env python3
"""
===============================================================================
DATA INGESTION PIPELINE - ORQUESTRADOR PRINCIPAL (CLI)
===============================================================================
Executa o ciclo completo de Engenharia de Dados:
1. Extração da BrasilAPI (Feriados Nacionais)
2. Validação e Schema Contratual via Pydantic
3. Carga Idempotente no BigQuery RAW (com deduplicação determinística)
4. (Opcional) Execução das transformações dbt (Staging -> Intermediate -> Marts)

Comandos:
    $ python main.py
    $ python main.py --dry-run
    $ python main.py --years 2024,2025,2026
    $ python main.py --run-dbt
===============================================================================
"""

import argparse
import subprocess
import sys
from pathlib import Path
from src.ingestion.pipeline import run_pipeline
from src.utils.logger import logger


def run_dbt_command(command: str = "build") -> bool:
    """Executa comando dbt apontando para o diretório dbt/."""
    dbt_dir = Path(__file__).resolve().parent / "dbt"
    full_cmd = [sys.executable, "-m", "dbt.cli.main", command]

    logger.info(f"Executando dbt: {' '.join(full_cmd)} (cwd: {dbt_dir})")
    try:
        result = subprocess.run(full_cmd, cwd=str(dbt_dir), check=True)
        return result.returncode == 0
    except subprocess.CalledProcessError as err:
        logger.error(f"Erro na execução do dbt {command}: código de retorno {err.returncode}")
        return False
    except Exception as exc:
        logger.error(f"Falha ao disparar processo dbt: {exc}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Orquestrador do Data Ingestion Pipeline")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Executa extração e validação sem persistir no BigQuery"
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        help="Lista de anos a serem ingeridos (ex: 2024,2025,2026)"
    )
    parser.add_argument(
        "--run-dbt",
        action="store_true",
        help="Dispara automaticamente 'dbt build' após a ingestão"
    )

    args = parser.parse_args()

    custom_years = [int(y.strip()) for y in args.years.split(",") if y.strip()] if args.years else None

    # 1. Execução da Ingestão Python
    logger.info(">>> ETAPA 1/2: INGESTÃO E CARGA RAW <<<")
    metrics = run_pipeline(dry_run=args.dry_run, custom_years=custom_years)

    if metrics.status != "SUCCESS":
        logger.error(f"Pipeline de ingestão finalizou com status: {metrics.status}")
        sys.exit(1)

    # 2. Execução das Transformações dbt (se solicitado)
    if args.run_dbt:
        logger.info(">>> ETAPA 2/2: TRANSFORMAÇÃO DBT (STAGING -> INTERMEDIATE -> MARTS) <<<")
        success = run_dbt_command("build")
        if not success:
            sys.exit(1)

    logger.info("Processo concluído com êxito.")


if __name__ == "__main__":
    main()
