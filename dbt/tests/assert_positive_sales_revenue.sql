-- Teste de negócio (RF-021): Garante que nenhuma venda possua receita negativa ou quantidade <= 0
select
    sale_id,
    revenue,
    quantity
from {{ ref('stg_sales') }}
where revenue < 0 or quantity <= 0
