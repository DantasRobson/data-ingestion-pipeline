{{
    config(
        materialized='table'
    )
}}

with sales_facts as (

    select * from {{ ref('fct_sales') }}

),

-- Média de receita diária em dias comuns por categoria
baseline_sales as (

    select
        product_category,
        round(avg(revenue), 2) as avg_revenue_normal,
        round(avg(quantity), 2) as avg_quantity_normal
    from sales_facts
    where sales_period_context = 'DIA_COMUM'
    group by product_category

),

-- Média de receita diária no período pré-feriado (janela de até 7 dias antes do feriado)
pre_holiday_sales as (

    select
        s.holiday_name,
        s.product_category,
        min(s.sale_date) as first_sale_window,
        max(s.sale_date) as last_sale_window,
        round(avg(s.revenue), 2) as avg_revenue_before_holiday,
        round(avg(s.quantity), 2) as avg_quantity_before_holiday,
        sum(s.revenue) as total_revenue_pre_holiday,
        count(distinct s.sale_id) as total_transactions_pre_holiday
    from sales_facts s
    where s.sales_period_context = 'PRE_FERIADO'
      and s.holiday_name != 'Dia Comum'
    group by s.holiday_name, s.product_category

),

growth_calculation as (

    select
        p.holiday_name,
        p.product_category,
        coalesce(b.avg_revenue_normal, 0) as avg_sales_normal,
        p.avg_revenue_before_holiday as avg_sales_before_holiday,
        p.total_revenue_pre_holiday,
        p.total_transactions_pre_holiday,

        -- Percentual de crescimento de receita pré-feriado vs baseline normal
        round(
            case
                when coalesce(b.avg_revenue_normal, 0) > 0 
                then ((p.avg_revenue_before_holiday - b.avg_revenue_normal) / b.avg_revenue_normal) * 100
                else 0
            end, 2
        ) as sales_growth_pct

    from pre_holiday_sales p
    left join baseline_sales b
        on p.product_category = b.product_category

)

select
    holiday_name,
    product_category,
    avg_sales_normal,
    avg_sales_before_holiday,
    total_revenue_pre_holiday,
    total_transactions_pre_holiday,
    sales_growth_pct,

    -- RF-019: Regra de Negócio para Classificação de Oportunidade
    case
        when sales_growth_pct >= 30.0 then 'ALTA'
        when sales_growth_pct >= 15.0 then 'MEDIA'
        else 'BAIXA'
    end as opportunity_score,

    -- Recomendações acionáveis para times de Vendas, Marketing e BI
    case
        when sales_growth_pct >= 30.0 then 
            concat('Oportunidade ALTA para ', product_category, ': Antecipar campanhas promocionais em D-7 e reforçar estoque.')
        when sales_growth_pct >= 15.0 then 
            concat('Oportunidade MÉDIA para ', product_category, ': Disparar comunicações e ofertas em D-5 com foco em conversão.')
        else 
            concat('Demanda regular para ', product_category, ': Manter estratégia padrão sem custo extraordinário de mídia.')
    end as recommendation
from growth_calculation
