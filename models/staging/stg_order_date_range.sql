SELECT asof_date
FROM (
SELECT 
    GENERATE_DATE_ARRAY(
    (SELECT MIN(DATE(created_at)) FROM `bigquery-public-data.thelook_ecommerce.orders`),
    (SELECT MAX(DATE(created_at)) FROM `bigquery-public-data.thelook_ecommerce.orders`)
    ) AS asof_dates
),
UNNEST(asof_dates) AS asof_date
