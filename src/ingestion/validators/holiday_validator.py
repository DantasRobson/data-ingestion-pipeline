"""
Módulo de validação de dados para a fonte de feriados (BrasilAPI).
Implementa validação estrita com Pydantic (RF-002).
"""

from datetime import date, datetime, timezone
import hashlib
from typing import Any, Dict, List
from pydantic import BaseModel, Field, field_validator


class HolidayRawItem(BaseModel):
    """Modelo de validação do JSON bruto retornado pela BrasilAPI."""
    date: str = Field(..., description="Data do feriado no formato YYYY-MM-DD")
    name: str = Field(..., description="Nome do feriado nacional")
    type: str = Field(..., description="Tipo do feriado (ex: national)")

    @field_validator("date")
    @classmethod
    def validate_date_format(cls, value: str) -> str:
        try:
            parsed = date.fromisoformat(value)
            return parsed.isoformat()
        except ValueError:
            raise ValueError(f"Formato de data inválido: '{value}'. Esperado YYYY-MM-DD.")

    @field_validator("name")
    @classmethod
    def validate_name_not_empty(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("O campo 'name' não pode ser vazio.")
        return stripped

    @field_validator("type")
    @classmethod
    def validate_type_not_empty(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("O campo 'type' não pode ser vazio.")
        return stripped.lower()


class HolidayIngestionRecord(BaseModel):
    """Modelo final para persistência na camada RAW do BigQuery com metadados (RF-004 e RF-005)."""
    holiday_id: str = Field(..., description="Hash único natural para deduplicação (date + name + type)")
    date: str
    name: str
    type: str
    source: str = Field(default="BrasilAPI")
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def create(cls, raw: HolidayRawItem, source: str = "BrasilAPI") -> "HolidayIngestionRecord":
        """Gera chave determinística date + name + type para controle de unicidade."""
        raw_key = f"{raw.date}_{raw.name.lower()}_{raw.type.lower()}"
        holiday_id = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
        return cls(
            holiday_id=holiday_id,
            date=raw.date,
            name=raw.name,
            type=raw.type,
            source=source,
            ingested_at=datetime.now(timezone.utc)
        )


def validate_and_parse_holidays(raw_data: List[Dict[str, Any]], source: str = "BrasilAPI") -> List[HolidayIngestionRecord]:
    """
    Valida a lista de dicionários brutos recebida da API externa.
    Retorna registros validados e enriquecidos com metadados de ingestão.
    """
    if not isinstance(raw_data, list):
        raise ValueError(f"Estrutura inesperada recebida da API: esperava list, obteve {type(raw_data).__name__}")

    records: List[HolidayIngestionRecord] = []
    for item in raw_data:
        raw_item = HolidayRawItem(**item)
        record = HolidayIngestionRecord.create(raw_item, source=source)
        records.append(record)

    return records
