"""
Módulo Cost & Usage Guard para o Google BigQuery.
Implementa mecanismos preventivos (Dry Run, Circuit Breaker, maximum_bytes_billed),
cálculo de consumo acumulado mensal a CUSTO ZERO (via API de Jobs) e auditoria.
"""

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json
import os
from typing import Any, Dict, List, Optional, Tuple
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig
from src.config.settings import settings
from src.utils.logger import logger


class CostGuardError(Exception):
    """Exceção base do Cost Guard."""
    pass


class CircuitBreakerError(CostGuardError):
    """Lançada quando uma query ameaça ultrapassar o teto operacional seguro do mês."""
    pass


class QueryTooLargeError(CostGuardError):
    """Lançada quando uma consulta individual excede o limite máximo por query."""
    pass


@dataclass
class JobExecutionMetric:
    execution_id: str
    step_name: str
    job_id: str
    project_id: str
    start_time: str
    end_time: str
    bytes_processed: int
    bytes_billed: int
    estimated_cost_usd: float
    status: str
    alert_level: str


class BigQueryCostGuard:
    """Guardião de custos e consumo preventivo para o BigQuery pessoal."""

    def __init__(
        self,
        free_tier_bytes: Optional[int] = None,
        operational_limit_pct: Optional[float] = None,
        max_bytes_per_query: Optional[int] = None,
        history_file: str = "data/cost_guard_history.json"
    ):
        self.free_tier_bytes = free_tier_bytes or settings.bq_free_tier_bytes
        self.operational_limit_pct = operational_limit_pct or settings.bq_operational_limit_pct
        self.max_bytes_per_query = max_bytes_per_query or settings.bq_max_bytes_per_query
        self.history_file = history_file
        self.cost_per_tib_usd = 6.25  # Taxa padrão on-demand do BigQuery

        # Teto operacional em bytes (ex: 50% de 1 TiB = 512 GiB)
        self.operational_ceiling_bytes = int(self.free_tier_bytes * (self.operational_limit_pct / 100.0))

    def _ensure_project_safety(self, project_id: str) -> None:
        """Bloqueia imediatamente qualquer tentativa de uso de projetos corporativos."""
        if "myralis" in project_id.lower():
            raise RuntimeError(
                f"VIOLAÇÃO CRÍTICA DE SEGURANÇA: Tentativa de operação no projeto corporativo '{project_id}'! "
                "O Cost Guard bloqueou o processo imediatamente."
            )

    def get_monthly_billed_bytes(self, client: bigquery.Client) -> int:
        """
        Calcula o total de bytes faturados no mês corrente utilizando a API REST de Jobs.
        CUSTO DESTA CONSULTA: ZERO BYTES (Chamada de gerenciamento de metadados, não executa query).
        """
        self._ensure_project_safety(client.project)
        now = datetime.now(timezone.utc)
        start_of_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)

        total_billed = 0
        try:
            # Lista apenas os jobs concluídos neste projeto no mês corrente
            for job in client.list_jobs(project=client.project, min_creation_time=start_of_month, state_filter="DONE"):
                if hasattr(job, "total_bytes_billed") and job.total_bytes_billed:
                    total_billed += job.total_bytes_billed
        except Exception as exc:
            logger.warning(f"Não foi possível consultar jobs via API ({exc}). Utilizando fallback do histórico local.")
            total_billed = self._get_local_monthly_billed_bytes(start_of_month)

        return total_billed

    def _get_local_monthly_billed_bytes(self, start_of_month: datetime) -> int:
        """Fallback local lendo o histórico persistido em disco."""
        if not os.path.exists(self.history_file):
            return 0
        try:
            with open(self.history_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            total = 0
            for item in data:
                ts = datetime.fromisoformat(item["start_time"])
                if ts >= start_of_month and item.get("status") == "SUCCESS":
                    total += item.get("bytes_billed", 0)
            return total
        except Exception:
            return 0

    def dry_run_estimate(self, client: bigquery.Client, query: str, job_config: Optional[QueryJobConfig] = None) -> int:
        """
        Executa estimativa prévia (Dry Run) para obter bytes que serão processados sem custos.
        CUSTO: ZERO BYTES.
        """
        self._ensure_project_safety(client.project)
        config = job_config or QueryJobConfig()
        config.dry_run = True
        config.use_query_cache = False

        dry_job = client.query(query, job_config=config)
        return dry_job.total_bytes_processed or 0

    def get_alert_level(self, total_bytes: int) -> str:
        """Calcula o nível de alerta com base no percentual da franquia Always Free (1 TiB)."""
        pct = (total_bytes / self.free_tier_bytes) * 100.0
        if pct >= self.operational_limit_pct:
            return "BLOQUEIO_OPERACIONAL"
        elif pct >= 40.0:
            return "ALERTA_CRITICO"
        elif pct >= 25.0:
            return "ALERTA"
        elif pct >= 10.0:
            return "ACOMPANHAMENTO"
        return "NORMAL"

    def execute_guarded_query(
        self,
        client: bigquery.Client,
        query: str,
        step_name: str = "query",
        execution_id: Optional[str] = None,
        job_config: Optional[QueryJobConfig] = None
    ) -> Tuple[Any, JobExecutionMetric]:
        """
        Executa uma consulta com proteção preventiva total:
        1. Validação do projeto pessoal
        2. Estimativa de bytes via Dry Run (0 bytes cobrados)
        3. Validação contra limite individual por query
        4. Verificação do Circuit Breaker contra o teto mensal (50%)
        5. Ativação de maximum_bytes_billed no job real
        6. Coleta dos metadados reais pós-execução e persistência em auditoria
        """
        self._ensure_project_safety(client.project)
        exec_id = execution_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        # 1. Estimativa prévia
        logger.info(f"[{step_name}] Executando Dry Run preventivo no BigQuery...")
        estimated_bytes = self.dry_run_estimate(client, query)
        logger.info(f"[{step_name}] Estimativa Dry Run: {estimated_bytes:,} bytes ({estimated_bytes / (1024**2):.2f} MB).")

        # 2. Verificação de limite individual
        if estimated_bytes > self.max_bytes_per_query:
            raise QueryTooLargeError(
                f"Consulta cancelada! A estimativa de {estimated_bytes} bytes excede o limite individual por query "
                f"de {self.max_bytes_per_query} bytes."
            )

        # 3. Verificação do Circuit Breaker no consumo acumulado do mês
        current_month_billed = self.get_monthly_billed_bytes(client)
        projected_total = current_month_billed + estimated_bytes

        if projected_total > self.operational_ceiling_bytes:
            raise CircuitBreakerError(
                f"🚨 CIRCUIT BREAKER ACIONADO! A consulta de {estimated_bytes / (1024**3):.2f} GB faria o consumo do mês "
                f"atingir {projected_total / (1024**3):.2f} GB, ultrapassando o teto operacional seguro de "
                f"{self.operational_ceiling_bytes / (1024**3):.2f} GB ({self.operational_limit_pct}% da franquia). "
                "Execução cancelada antes de despachar a query."
            )

        # 4. Configura trava nativa de segurança no job real
        real_config = job_config or QueryJobConfig()
        real_config.dry_run = False
        real_config.maximum_bytes_billed = self.max_bytes_per_query

        # 5. Execução real
        start_dt = datetime.now(timezone.utc)
        query_job = client.query(query, job_config=real_config)
        results = query_job.result()
        end_dt = datetime.now(timezone.utc)

        # 6. Coleta de metadados reais
        bytes_processed = query_job.total_bytes_processed or 0
        bytes_billed = query_job.total_bytes_billed or 0

        # Custo teórico (US$ 0,00 se dentro da franquia gratuita)
        tib_billed = bytes_billed / (1024**4)
        estimated_cost = round(tib_billed * self.cost_per_tib_usd, 4)

        new_total_month = current_month_billed + bytes_billed
        alert_level = self.get_alert_level(new_total_month)

        metric = JobExecutionMetric(
            execution_id=exec_id,
            step_name=step_name,
            job_id=query_job.job_id,
            project_id=query_job.project,
            start_time=start_dt.isoformat(),
            end_time=end_dt.isoformat(),
            bytes_processed=bytes_processed,
            bytes_billed=bytes_billed,
            estimated_cost_usd=estimated_cost,
            status="SUCCESS",
            alert_level=alert_level
        )

        self._record_history(metric)
        return results, metric

    def _record_history(self, metric: JobExecutionMetric) -> None:
        """Registra a métrica no histórico local de auditoria de forma econômica."""
        os.makedirs(os.path.dirname(self.history_file) or ".", exist_ok=True)
        history = []
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except Exception:
                history = []

        history.append(asdict(metric))
        # Mantém histórico leve (últimos 500 registros)
        if len(history) > 500:
            history = history[-500:]

        with open(self.history_file, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)

    def get_usage_summary(self, client: bigquery.Client) -> Dict[str, Any]:
        """Calcula o resumo completo de consumo vs franquia Always Free e teto operacional."""
        month_bytes = self.get_monthly_billed_bytes(client)
        pct_free_tier_used = round((month_bytes / self.free_tier_bytes) * 100.0, 4)
        pct_free_tier_remaining = round(max(0.0, 100.0 - pct_free_tier_used), 4)

        bytes_remaining_ceiling = max(0, self.operational_ceiling_bytes - month_bytes)
        pct_ceiling_used = round((month_bytes / self.operational_ceiling_bytes) * 100.0, 2) if self.operational_ceiling_bytes > 0 else 0.0

        alert_level = self.get_alert_level(month_bytes)

        return {
            "project_id": client.project,
            "month": datetime.now(timezone.utc).strftime("%Y-%m"),
            "bytes_used_month": month_bytes,
            "gib_used_month": round(month_bytes / (1024**3), 3),
            "free_tier_total_gib": round(self.free_tier_bytes / (1024**3), 1),
            "pct_free_tier_used": pct_free_tier_used,
            "pct_free_tier_remaining": pct_free_tier_remaining,
            "operational_ceiling_gib": round(self.operational_ceiling_bytes / (1024**3), 1),
            "bytes_remaining_to_ceiling": bytes_remaining_ceiling,
            "gib_remaining_to_ceiling": round(bytes_remaining_ceiling / (1024**3), 3),
            "pct_ceiling_used": pct_ceiling_used,
            "alert_level": alert_level,
            "circuit_breaker_active": month_bytes >= self.operational_ceiling_bytes
        }

    def print_usage_report(self, client: bigquery.Client, last_metric: Optional[JobExecutionMetric] = None) -> None:
        """Exibe o painel de auditoria no terminal ao final da execução."""
        summary = self.get_usage_summary(client)
        sep = "=" * 80
        logger.info(sep)
        logger.info("🛡️ BIGQUERY COST & USAGE GUARD — RELATÓRIO DE CONSUMO")
        logger.info(f"Projeto Verificado         : {summary['project_id']}")
        logger.info(f"Mês de Referência          : {summary['month']}")

        if last_metric:
            logger.info(f"Última Operação            : {last_metric.step_name} (Job: {last_metric.job_id})")
            logger.info(f"Bytes Processados (Job)    : {last_metric.bytes_processed:,} bytes ({last_metric.bytes_processed / (1024**2):.2f} MB)")
            logger.info(f"Bytes Faturados (Job)      : {last_metric.bytes_billed:,} bytes ({last_metric.bytes_billed / (1024**2):.2f} MB)")
            logger.info(f"Custo Estimado da Query    : US$ {last_metric.estimated_cost_usd:.4f} (Isento na franquia Always Free)")

        logger.info("-" * 80)
        logger.info(f"Consumo Acumulado no Mês   : {summary['gib_used_month']} GiB de {summary['free_tier_total_gib']} GiB")
        logger.info(f"Uso da Franquia Always Free: {summary['pct_free_tier_used']:.2f}% (Restante: {summary['pct_free_tier_remaining']:.2f}%)")
        logger.info(f"Teto Operacional Seguro    : {summary['operational_ceiling_gib']} GiB ({self.operational_limit_pct}% da franquia)")
        logger.info(f"Uso do Teto Operacional    : {summary['pct_ceiling_used']:.2f}%")
        logger.info(f"Saldo Seguro Restante      : {summary['gib_remaining_to_ceiling']} GiB")
        logger.info(f"Nível de Alerta Atual      : {summary['alert_level']}")
        logger.info(f"Status do Circuit Breaker  : {'ARMADO (BLOQUEIO ATIVO)' if summary['circuit_breaker_active'] else 'DESARMADO (OPERAÇÃO SEGURA)'}")
        logger.info(sep)
