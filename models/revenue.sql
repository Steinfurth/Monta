WITH revenue AS (
  SELECT 
    DATE(date) as charge_date, 
    DATE(date_trunc(date,month)) as charge_month, 
    operator_id, 
    operator_name, 
    country, 
    cp_id,
    DATE_DIFF(date(max(date) over ()), date(min(date) over (partition by cp_id)),DAY) as cp_age_days,
    charge_id, 
    price_eur, 
    cost_eur,
    price_eur - cost_eur AS gross_profit_cpo,
    (price_eur - cost_eur) / price_eur AS gross_profit_margin_cpo,
    price_eur * 0.10 as roaming_revenue_eur
  FROM {{ ref('data_analytics_test_data') }}
  WHERE price_eur != 0 -- filtering out entries without price assigned
)

SELECT *
FROM revenue