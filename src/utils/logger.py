"""
Módulo de observabilidade e logging estruturado.
Garante rastreabilidade, formatação limpa e métricas de execução para a esteira.
"""

import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from src.config.settings import settings


def setup_logger(name: str = "data_pipeline") -> logging.Logger:
    """Configura logger formatado e padronizado com suporte seguro a utf-8."""
    logger = logging.getLogger(name)
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logger.setLevel(level)

    if not logger.handlers:
        stream = sys.stdout
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

        handler = logging.StreamHandler(stream)
        handler.setLevel(level)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


logger = setup_logger()


class ExecutionMetrics:
    """Registra métricas de execução e observabilidade da pipeline (RF-004 e Seção 22)."""

    def __init__(self, pipeline_name: str, execution_id: Optional[str] = None):
        self.pipeline_name = pipeline_name
        self.execution_id = execution_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.start_time: datetime = datetime.now(timezone.utc)
        self.end_time: Optional[datetime] = None
        self.status: str = "RUNNING"
        self.records_received: int = 0
        self.records_loaded: int = 0
        self.error_message: Optional[str] = None

    def finish(self, status: str, records_received: int = 0, records_loaded: int = 0, error: Optional[str] = None):
        self.end_time = datetime.now(timezone.utc)
        self.status = status
        self.records_received = records_received
        self.records_loaded = records_loaded
        self.error_message = error

    @property
    def duration_seconds(self) -> float:
        end = self.end_time or datetime.now(timezone.utc)
        return round((end - self.start_time).total_seconds(), 2)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline": self.pipeline_name,
            "execution_id": self.execution_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status,
            "records_received": self.records_received,
            "records_loaded": self.records_loaded,
            "duration_seconds": self.duration_seconds,
            "error_message": self.error_message
        }

    def print_summary(self):
        """Exibe resumo visual amigável no console."""
        metrics = self.to_dict()
        separator = "=" * 60
        logger.info(separator)
        logger.info(f"📊 RESUMO DE EXECUÇÃO: {self.pipeline_name}")
        logger.info(f"ID da Execução    : {metrics['execution_id']}")
        logger.info(f"Status Final      : {metrics['status']}")
        logger.info(f"Registros Obtidos : {metrics['records_received']}")
        logger.info(f"Registros Carreg. : {metrics['records_loaded']}")
        logger.info(f"Duração (segundos): {metrics['duration_seconds']}s")
        if self.error_message:
            logger.error(f"Erro Registrado   : {self.error_message}")
        logger.info(separator)
