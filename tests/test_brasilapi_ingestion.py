"""
Testes unitários para o módulo de ingestão da BrasilAPI (Feriados).
Valida contratos de dados, regras de integridade e resiliência (RF-001 a RF-005).
"""

from unittest.mock import MagicMock, patch
import pytest
from src.ingestion.clients.brasilapi_client import BrasilAPIClient, BrasilAPIClientError
from src.ingestion.pipeline import run_pipeline
from src.ingestion.validators.holiday_validator import (
    HolidayIngestionRecord,
    HolidayRawItem,
    validate_and_parse_holidays
)


def test_holiday_validator_valid():
    """Valida conversão e parsing com dados válidos."""
    data = [
        {"date": "2026-01-01", "name": "Confraternização Universal", "type": "national"},
        {"date": "2026-12-25", "name": "Natal", "type": "national"}
    ]
    records = validate_and_parse_holidays(data)
    assert len(records) == 2
    assert records[0].date == "2026-01-01"
    assert records[0].name == "Confraternização Universal"
    assert records[0].type == "national"
    assert records[0].source == "BrasilAPI"
    assert len(records[0].holiday_id) == 64  # SHA-256


def test_holiday_validator_invalid_date():
    """Garante erro de validação para datas em formato incorreto."""
    with pytest.raises(Exception):
        HolidayRawItem(date="01/01/2026", name="Ano Novo", type="national")


def test_holiday_validator_deterministic_hash():
    """Garante que a mesma combinação date + name + type gere sempre o mesmo hash para deduplicação (RF-005)."""
    raw1 = HolidayRawItem(date="2026-04-21", name="Tiradentes", type="national")
    raw2 = HolidayRawItem(date="2026-04-21", name="Tiradentes", type="national")

    rec1 = HolidayIngestionRecord.create(raw1)
    rec2 = HolidayIngestionRecord.create(raw2)

    assert rec1.holiday_id == rec2.holiday_id


@patch("requests.Session.get")
def test_brasilapi_client_success(mock_get):
    """Testa requisição bem-sucedida da BrasilAPI."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"date": "2026-01-01", "name": "Confraternização Universal", "type": "national"}
    ]
    mock_get.return_value = mock_response

    client = BrasilAPIClient()
    holidays = client.fetch_holidays_by_year(2026)

    assert len(holidays) == 1
    assert holidays[0]["name"] == "Confraternização Universal"


@patch("requests.Session.get")
def test_brasilapi_client_http_error(mock_get):
    """Valida tratamento e lançamento de erro quando a API falha (RF-003)."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_response.reason = "Internal Server Error"
    mock_get.return_value = mock_response

    client = BrasilAPIClient(max_retries=1)
    with pytest.raises(BrasilAPIClientError):
        client.fetch_holidays_by_year(2026)


@patch("src.ingestion.clients.brasilapi_client.BrasilAPIClient.fetch_multiple_years")
def test_run_pipeline_dry_run(mock_fetch):
    """Testa execução da pipeline em modo dry-run."""
    mock_fetch.return_value = [
        {"date": "2026-05-01", "name": "Dia do Trabalho", "type": "national"}
    ]
    metrics = run_pipeline(dry_run=True, custom_years=[2026])

    assert metrics.status == "SUCCESS"
    assert metrics.records_received == 1
    assert metrics.records_loaded == 1
    assert metrics.error_message is None
