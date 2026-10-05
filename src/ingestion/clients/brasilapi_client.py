"""
Cliente HTTP para consumo da BrasilAPI (Feriados Nacionais).
Atende aos requisitos RF-001, RF-002 e RF-003 com resiliência, retries e logs padronizados.
"""

import time
from typing import Any, Dict, List, Optional
import requests
from requests.exceptions import RequestException
from src.config.settings import settings
from src.utils.logger import logger


class BrasilAPIClientError(Exception):
    """Exceção customizada para erros na integração com a BrasilAPI."""
    pass


class BrasilAPIClient:
    """Cliente para a API de Feriados Nacionais da BrasilAPI."""

    def __init__(self, base_url: Optional[str] = None, timeout: int = 15, max_retries: int = 3):
        self.base_url = (base_url or settings.brasilapi_base_url).rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "DataIngestionPipeline/1.0 (Portfolio Case)",
            "Accept": "application/json"
        })

    def fetch_holidays_by_year(self, year: int) -> List[Dict[str, Any]]:
        """
        Consulta feriados nacionais para um determinado ano.
        URL: {base_url}/feriados/v1/{year}
        """
        endpoint = f"/feriados/v1/{year}"
        url = f"{self.base_url}{endpoint}"

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Consultando BrasilAPI: ano {year} (tentativa {attempt}/{self.max_retries}) -> {url}")
                response = self.session.get(url, timeout=self.timeout)

                # RF-003: Tratamento e log claro de erros HTTP
                if response.status_code != 200:
                    error_msg = (
                        f"\nERROR\n"
                        f"Source: BrasilAPI\n"
                        f"Endpoint: {endpoint}\n"
                        f"Status: {response.status_code}\n"
                        f"Message: {response.text.strip() or response.reason}"
                    )
                    logger.error(error_msg)

                    # Se for erro transitório de servidor (5xx) ou rate limit (429), tenta novamente com backoff
                    if response.status_code in [429, 500, 502, 503, 504] and attempt < self.max_retries:
                        sleep_time = attempt * 2
                        logger.warning(f"Aguardando {sleep_time}s antes de tentar novamente...")
                        time.sleep(sleep_time)
                        continue

                    raise BrasilAPIClientError(f"Falha na requisição para {endpoint} com status {response.status_code}: {response.text}")

                # RF-002: Validação da estrutura JSON
                data = response.json()
                if not isinstance(data, list):
                    raise BrasilAPIClientError(f"Esperava retorno em formato lista para {endpoint}, obteve {type(data).__name__}")

                logger.info(f"Sucesso: {len(data)} feriados recebidos para o ano {year}.")
                return data

            except RequestException as exc:
                logger.error(f"Erro de conexão na tentativa {attempt} para {url}: {exc}")
                if attempt < self.max_retries:
                    time.sleep(attempt * 2)
                    continue
                raise BrasilAPIClientError(f"Falha de conexão com a BrasilAPI após {self.max_retries} tentativas: {exc}") from exc

        return []

    def fetch_multiple_years(self, years: List[int]) -> List[Dict[str, Any]]:
        """Busca feriados para múltiplos anos e agrega o resultado."""
        all_holidays: List[Dict[str, Any]] = []
        for year in years:
            holidays = self.fetch_holidays_by_year(year)
            all_holidays.extend(holidays)
        return all_holidays
