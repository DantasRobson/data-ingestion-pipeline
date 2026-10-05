# 💼 Regras de Negócio — Oportunidades Comerciais

## 1. Contexto

Feriados nacionais alteram padrões de consumo no varejo. Certas categorias apresentam forte antecipação de compras (ex: Bebidas e Alimentos no Carnaval ou Natal; Eletrônicos e Presentes no Dia das Mães e Black Friday).

## 2. Janelas Temporais Analisadas

Para cada transação de venda e para cada data do calendário:
- **Janela Pré-Feriado (`PRE_FERIADO`)**: Dias que antecedem o feriado em uma janela de 1 a 7 dias (`days_to_holiday BETWEEN 1 AND 7`).
- **Durante o Feriado (`DURANTE_FERIADO`)**: A data exata do feriado nacional (`is_holiday = TRUE`).
- **Janela Pós-Feriado (`POS_FERIADO`)**: Período de 1 a 3 dias imediatamente subsequente ao feriado (`days_after_holiday BETWEEN 1 AND 3`).
- **Dia Comum (`DIA_COMUM`)**: Dias normais sem proximidade de feriados.

---

## 3. Classificação de Oportunidades (Opportunity Score)

No modelo `mart_oportunidades_comerciais`, o crescimento percentual de vendas é calculado por:

```sql
sales_growth_pct = ((avg_sales_before_holiday - avg_sales_normal) / avg_sales_normal) * 100
```

### Critérios de Decisão:
| Faixa de Crescimento | Score de Oportunidade | Ação Recomendada |
| :--- | :--- | :--- |
| $\ge 30\%$ | **ALTA** | *"Antecipar campanhas promocionais em D-7 e reforçar estoque."* |
| $15\% \le \text{Crescimento} < 30\%$ | **MÉDIA** | *"Disparar comunicações em D-5 com foco em conversão."* |
| $< 15\%$ | **BAIXA** | *"Manter estratégia padrão sem custo extraordinário de mídia."* |
