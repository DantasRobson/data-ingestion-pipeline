"""
Teste de autenticação e identificação de projeto no Google BigQuery (Etapas 12 e 13).
Valida conexão via Application Default Credentials (ADC) no projeto pessoal projetoportifolio-492813.
"""

import sys
import os

# Garante acesso aos módulos em src/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from google.cloud import bigquery
from src.config.settings import settings


def test_bigquery_authentication():
    # 1. Verificação crítica de isolamento
    if "myralis" in settings.gcp_project_id.lower():
        raise RuntimeError(
            f"VIOLAÇÃO DE SEGURANÇA: Tentativa de executar query no projeto corporativo '{settings.gcp_project_id}'! "
            "A execução foi abortada imediatamente."
        )

    expected_project = "projetoportifolio-492813"
    if settings.gcp_project_id != expected_project:
        raise ValueError(
            f"Projeto configurado '{settings.gcp_project_id}' difere do projeto pessoal esperado '{expected_project}'."
        )

    print(f"Iniciando cliente BigQuery para o projeto: {settings.gcp_project_id}...")
    client = bigquery.Client(project=settings.gcp_project_id)

    # 2. Validação do cliente
    assert client.project == expected_project, f"Cliente BigQuery apontando para {client.project}!"

    # 3. Execução da query de teste controlada (Etapa 12 do PRD)
    query = """
    SELECT
        1 AS teste,
        CURRENT_TIMESTAMP() AS executado_em
    """
    print("Executando query de teste no BigQuery...")
    query_job = client.query(query)
    results = list(query_job.result())

    # 4. Validação do resultado da query
    assert len(results) == 1, "Esperava exatamente 1 linha de resultado."
    row = results[0]
    teste_val = row["teste"]
    executado_em = row["executado_em"]

    print("\n=== RESULTADO DA EXECUCAO NO BIGQUERY ===")
    print(f"Projeto do Job  : {query_job.project}")
    print(f"ID do Job       : {query_job.job_id}")
    print(f"Status do Job   : {query_job.state}")
    print(f"Coluna 'teste'  : {teste_val}")
    print(f"Executado em    : {executado_em}")
    print("=========================================\n")

    assert teste_val == 1, f"Valor de teste incorreto: {teste_val}"
    assert query_job.project == expected_project, f"Job executado em projeto incorreto: {query_job.project}"
    print("Autenticacao e Identificacao de Projeto concluidas com 100% de SUCESSO!")


if __name__ == "__main__":
    try:
        test_bigquery_authentication()
    except Exception as exc:
        print(f"FALHA NO TESTE: {exc}", file=sys.stderr)
        sys.exit(1)
