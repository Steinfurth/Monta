WITH revenue_per_segment AS (
  SELECT 
    cpo_segment, 
    count(distinct cp_id) as cp_sum_segment, 
    sum(roaming_revenue_eur) as roaming_rev_eur,
    sum(roaming_revenue_eur) / count(distinct cp_id) as roaming_rev_per_cp,
    avg(cp_age_days) as avg_cp_age,
    sum(roaming_revenue_eur) / count(distinct cp_id) / avg(cp_age_days) as roaming_rev_per_cp_per_day, 
    sum(roaming_revenue_eur) / count(distinct charge_id) as roaming_rev_per_session,
    sum(price_eur)/count(distinct charge_id) as price_per_session_eur,
    sum(cost_eur)/count(distinct charge_id) as cost_per_session_eur,
    avg(gross_profit_margin_cpo) as avg_gross_profit_margin_cpo,
    avg(gross_profit_cpo) as avg_gross_profit_per_session_eur
  FROM {{ ref('revenue_per_charge') }}
  GROUP BY cpo_segment
)

SELECT *
FROM revenue_per_segment
ORDER BY cp_sum_segment DESC