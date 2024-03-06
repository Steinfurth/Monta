WITH dataset AS (SELECT * FROM `monta-415820.Monta_dataset.data_analytics_test`),

revenue AS (
  SELECT 
    charge_id, -- primary key
    DATE(date) AS charge_date, 
    DATE(date_trunc(date,month)) AS charge_month, 
    operator_id, 
    operator_name,
    COUNT(DISTINCT cp_id) OVER (PARTITION BY operator_id) AS cp_count, 
    country, 
    cp_id,
    price_eur, 
    cost_eur,
    price_eur - cost_eur AS gross_profit_cpo,
    (price_eur - cost_eur) / price_eur AS gross_profit_margin_cpo,
    price_eur * 0.10 AS roaming_revenue_eur -- assuming Monta's roaming revenue is 10% of the charge price
  FROM dataset
  WHERE price_eur != 0 -- filtering out entries without price assigned
),

-- segmented CPOs into small, medium and large according to count of charging points
revenue_segments AS (
  SELECT 
    *,
    CASE
          WHEN cp_count >= 100 THEN "large (100+ cp)"
          WHEN cp_count < 100 AND cp_count >= 10 THEN "medium (10-99 cp)"
          WHEN cp_count < 10 THEN "small (0-9 cp)"
    END AS cpo_segment
    FROM revenue
)

SELECT *
FROM revenue_segments