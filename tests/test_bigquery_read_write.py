"""
Teste controlado de leitura e escrita no BigQuery pessoal (Etapas 14 e 15).
Valida escrita e leitura na tabela _teste_conectividade no dataset pipeline_ingestao.
"""

from datetime import datetime, timezone
import os
import sys
from google.cloud import bigquery
import pandas as pd

# Garante acesso aos módulos em src/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config.settings import settings
from src.utils.cost_guard import BigQueryCostGuard


def run_controlled_write_and_read_test():
    # 1. Segurança estrita de projeto
    if "myralis" in settings.gcp_project_id.lower():
        raise RuntimeError("VIOLAÇÃO DE SEGURANÇA: Tentativa de operação no projeto corporativo abortada!")

    expected_project = "projetoportifolio-492813"
    assert settings.gcp_project_id == expected_project, f"Projeto incorreto: {settings.gcp_project_id}"

    client = bigquery.Client(project=settings.gcp_project_id)
    guard = BigQueryCostGuard()

    dataset_name = "pipeline_ingestao"
    table_name = "_teste_conectividade"
    table_id = f"{settings.gcp_project_id}.{dataset_name}.{table_name}"

    print(f"1. Garantindo existência da tabela de teste: {table_id}...")
    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("mensagem", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("criado_em", "TIMESTAMP", mode="REQUIRED"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    print(f"   Tabela pronta: {table.full_table_id} (Location: {table.location})")

    # 2. Escrita controlada em lote (Batch Load - 100% gratuito e compatível com Free Tier)
    test_id = f"teste_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    rows_to_insert = [
        {
            "id": test_id,
            "mensagem": "Teste controlado de escrita - Pipeline Ingestao Pessoal",
            "criado_em": datetime.now(timezone.utc).isoformat()
        }
    ]

    print(f"2. Inserindo registro com id='{test_id}' via Batch Load Job (load_table_from_json)...")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    load_job = client.load_table_from_json(rows_to_insert, table_id, job_config=job_config)
    load_job.result()  # Aguarda conclusão do job
    print("   Registro inserido via Batch Load com 100% de sucesso!")

    # 3. Leitura confirmatória via Cost & Usage Guard
    print("3. Executando leitura confirmatória com o Cost Guard...")
    read_query = f"""
    SELECT
        id,
        mensagem,
        criado_em
    FROM `{table_id}`
    WHERE id = '{test_id}'
    """

    results, metric = guard.execute_guarded_query(
        client=client,
        query=read_query,
        step_name="leitura_teste_conectividade",
        execution_id="valida_escrita_001"
    )

    df = results.to_dataframe()

    print("\n=== RESULTADO DA CONFIRMAÇÃO DE ESCRITA E LEITURA ===")
    print("Quantidade de registros lidos :", len(df))
    print("Colunas retornadas            :", list(df.columns))
    print("ID verificado                 :", df["id"].iloc[0])
    print("Mensagem gravada              :", df["mensagem"].iloc[0])
    print("Data/Hora gravada             :", df["criado_em"].iloc[0])
    print("=====================================================\n")

    assert len(df) == 1, "Esperava exatamente 1 registro gravado."
    assert df["id"].iloc[0] == test_id, "ID lido diferente do ID gravado!"

    # 4. Exibe relatório operacional do Guard
    guard.print_usage_report(client, last_metric=metric)
    print("TESTE DE ESCRITA E LEITURA CONCLUÍDO COM 100% DE SUCESSO!")


if __name__ == "__main__":
    try:
        run_controlled_write_and_read_test()
    except Exception as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        sys.exit(1)
