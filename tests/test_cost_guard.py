"""
Testes unitários e de integração para o BigQuery Cost & Usage Guard.
Valida níveis de alerta, circuit breaker preventivo e telemetria real via ADC.
"""

import os
import sys
import pytest

# Garante acesso aos módulos em src/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from google.cloud import bigquery
from src.config.settings import settings
from src.utils.cost_guard import (
    BigQueryCostGuard,
    CircuitBreakerError,
    QueryTooLargeError,
    JobExecutionMetric
)


def test_alert_levels_logic():
    """Valida o cálculo exato dos 4 níveis de alerta e do teto de 50%."""
    free_tier = 1000  # 1000 bytes para teste proporcional
    guard = BigQueryCostGuard(
        free_tier_bytes=free_tier,
        operational_limit_pct=50.0,
        history_file="data/test_history.json"
    )

    # < 10%
    assert guard.get_alert_level(50) == "NORMAL"
    # >= 10% e < 25%
    assert guard.get_alert_level(100) == "ACOMPANHAMENTO"
    assert guard.get_alert_level(200) == "ACOMPANHAMENTO"
    # >= 25% e < 40%
    assert guard.get_alert_level(250) == "ALERTA"
    assert guard.get_alert_level(350) == "ALERTA"
    # >= 40% e < 50%
    assert guard.get_alert_level(400) == "ALERTA_CRITICO"
    assert guard.get_alert_level(499) == "ALERTA_CRITICO"
    # >= 50% (Teto Operacional)
    assert guard.get_alert_level(500) == "BLOQUEIO_OPERACIONAL"
    assert guard.get_alert_level(600) == "BLOQUEIO_OPERACIONAL"


def test_circuit_breaker_trigger():
    """Valida se o Circuit Breaker impede a execução quando o teto for ultrapassado."""
    guard = BigQueryCostGuard(
        free_tier_bytes=1000,
        operational_limit_pct=50.0,  # Teto: 500 bytes
        max_bytes_per_query=1000,
        history_file="data/test_history.json"
    )

    # Simula mock do cliente
    class MockClient:
        project = "projetoportifolio-492813"
        def list_jobs(self, *args, **kwargs):
            return []

    client = MockClient()

    # Sobrescreve dry_run_estimate para simular uma query de 600 bytes
    guard.dry_run_estimate = lambda c, q, config=None: 600
    guard.get_monthly_billed_bytes = lambda c: 0

    with pytest.raises(CircuitBreakerError) as exc_info:
        guard.execute_guarded_query(client, "SELECT * FROM large_table", step_name="test_step")

    assert "CIRCUIT BREAKER ACIONADO" in str(exc_info.value)


def test_query_too_large_trigger():
    """Valida se uma consulta individual maior que o limite configurado é bloqueada."""
    guard = BigQueryCostGuard(
        free_tier_bytes=10000,
        operational_limit_pct=50.0,
        max_bytes_per_query=200,
        history_file="data/test_history.json"
    )

    class MockClient:
        project = "projetoportifolio-492813"

    client = MockClient()
    guard.dry_run_estimate = lambda c, q, config=None: 500

    with pytest.raises(QueryTooLargeError) as exc_info:
        guard.execute_guarded_query(client, "SELECT 1", step_name="test_step")

    assert "excede o limite individual" in str(exc_info.value)


def test_real_bigquery_guarded_execution():
    """Testa a execução real no BigQuery pessoal com Dry Run, medição de bytes e relatório."""
    assert settings.gcp_project_id == "projetoportifolio-492813"
    client = bigquery.Client(project=settings.gcp_project_id)
    guard = BigQueryCostGuard(history_file="data/cost_guard_history.json")

    query = "SELECT 1 AS teste_guard, CURRENT_TIMESTAMP() AS executado_em"

    # 1. Executa com proteção total
    results, metric = guard.execute_guarded_query(
        client=client,
        query=query,
        step_name="teste_autenticacao_guard",
        execution_id="test_suite_run"
    )

    # 2. Valida métricas retornadas
    assert metric.project_id == "projetoportifolio-492813"
    assert metric.status == "SUCCESS"
    assert metric.job_id is not None
    assert metric.bytes_processed >= 0
    assert metric.bytes_billed >= 0

    # 3. Valida resultado da query
    rows = list(results)
    assert len(rows) == 1
    assert rows[0]["teste_guard"] == 1

    # 4. Gera e exibe o resumo completo
    guard.print_usage_report(client, last_metric=metric)
    summary = guard.get_usage_summary(client)

    assert summary["project_id"] == "projetoportifolio-492813"
    assert summary["pct_ceiling_used"] <= 50.0
    assert summary["circuit_breaker_active"] is False
