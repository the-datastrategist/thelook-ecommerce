WITH

date_range AS (
  -- Select the past 30 days from the latest created_at date
  SELECT asof_date
  FROM (
  SELECT 
      GENERATE_DATE_ARRAY(
      (SELECT DATE_SUB(MAX(DATE(created_at)), INTERVAL 29 DAY) FROM `bigquery-public-data.thelook_ecommerce.inventory_items`),
      (SELECT MAX(DATE(created_at)) FROM `bigquery-public-data.thelook_ecommerce.inventory_items`)
      ) AS asof_dates
  ),
  UNNEST(asof_dates) AS asof_date
)


SELECT  
  asof_date,
  product_id,
  product_sku,
  product_category,
  product_name,
  product_brand,
  product_department,
  
  -- total historical metrics
  APPROX_COUNT_DISTINCT(DATE(created_at)) AS days,
  SUM(1) AS items_total,
  SUM(CASE WHEN sold_at IS NULL THEN 1 ELSE 0 END) AS items_available_total,
  SUM(CASE WHEN sold_at IS NOT NULL THEN 1 ELSE 0 END) AS items_sold_total,
  SUM(cost) AS cost_total,
  SUM(product_retail_price) AS sales_total,
  AVG(DATETIME_DIFF(DATETIME(sold_at), DATETIME(created_at), HOUR)) AS avg_hours_to_sale_total,

  -- 1 day metrics (asof_date)
  SUM(CASE WHEN asof_date = DATE(created_at) THEN 1 ELSE 0 END) AS items_1d,
  SUM(CASE WHEN asof_date = DATE(created_at) AND sold_at IS NULL THEN 1 ELSE 0 END) AS items_available_1d,
  SUM(CASE WHEN asof_date = DATE(created_at) AND sold_at IS NOT NULL THEN 1 ELSE 0 END) AS items_sold_1d,
  SUM(CASE WHEN asof_date = DATE(created_at) THEN cost ELSE NULL END) AS cost_1d,
  SUM(CASE WHEN asof_date = DATE(created_at) THEN product_retail_price ELSE NULL END) AS sales_1d,
  AVG(CASE WHEN asof_date = DATE(created_at) THEN DATETIME_DIFF(DATETIME(sold_at), DATETIME(created_at), HOUR) ELSE NULL END) AS avg_hours_to_sale_1d,

  -- 3 day metrics (incl asof_date)
  SUM(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 2 THEN 1 ELSE 0 END) AS items_3d,
  SUM(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 2 AND sold_at IS NULL THEN 1 ELSE 0 END) AS items_available_3d,
  SUM(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 2 AND sold_at IS NOT NULL THEN 1 ELSE 0 END) AS items_sold_3d,
  SUM(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 2 THEN cost ELSE NULL END) AS cost_3d,
  SUM(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 2 THEN product_retail_price ELSE NULL END) AS sales_3d,
  AVG(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 2 THEN DATETIME_DIFF(DATETIME(sold_at), DATETIME(created_at), HOUR) ELSE NULL END) AS avg_hours_to_sale_3d,

  -- 7 day metrics (incl asof_date)
  SUM(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS items_7d,
  SUM(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 6 AND sold_at IS NULL THEN 1 ELSE 0 END) AS items_available_7d,
  SUM(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 6 AND sold_at IS NOT NULL THEN 1 ELSE 0 END) AS items_sold_7d,
  SUM(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 6 THEN cost ELSE NULL END) AS cost_7d,
  SUM(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 6 THEN product_retail_price ELSE NULL END) AS sales_7d,
  AVG(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 6 THEN DATETIME_DIFF(DATETIME(sold_at), DATETIME(created_at), HOUR) ELSE NULL END) AS avg_hours_to_sale_7d,

FROM `bigquery-public-data.thelook_ecommerce.inventory_items` 
-- Include all inventory items created prior to the asof_date
JOIN date_range ON asof_date >= DATE(created_at)
GROUP BY 1,2,3,4,5,6,7
