WITH dataset AS (SELECT * FROM `monta-415820.Monta_dataset.data_analytics_test`),

revenue AS (
  SELECT 
    DATE(date) as charge_date, 
    DATE(date_trunc(date,month)) as charge_month, 
    operator_id, 
    operator_name,
    COUNT(distinct cp_id) over (partition by operator_id) as cp_count, 
    country, 
    cp_id,
    DATE_DIFF(date(max(date) over ()), date(min(date) over (partition by cp_id)),DAY) as cp_age_days, -- calculating the age of each charging point (last date - first roaming charge date)
    charge_id, 
    price_eur, 
    cost_eur,
    price_eur - cost_eur AS gross_profit_cpo,
    (price_eur - cost_eur) / price_eur AS gross_profit_margin_cpo,
    price_eur * 0.10 as roaming_revenue_eur -- assuming Monta's revenue through roaming is 10% of the charge price
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