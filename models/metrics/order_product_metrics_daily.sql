WITH

date_range AS (
  SELECT * 
  FROM {{ ref('stg_order_date_range') }}
),

product_metrics_1d AS (
  SELECT 
    asof_date,
    product_id,

    -- 1D metrics (asof_date = created_date)
    SUM(CASE WHEN asof_date = DATE(created_at) THEN sale_price ELSE 0 END) AS sales_1d,
    APPROX_COUNT_DISTINCT(CASE WHEN asof_date = DATE(created_at) THEN user_id ELSE NULL END) AS users_1d,
    APPROX_COUNT_DISTINCT(CASE WHEN asof_date = DATE(created_at) THEN order_id ELSE NULL END) AS orders_1d,
    APPROX_COUNT_DISTINCT(CASE WHEN asof_date = DATE(created_at) THEN inventory_item_id ELSE NULL END) AS items_1d,

    -- 7D metrics
    SUM(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 8 THEN sale_price ELSE 0 END) AS sales_7d,
    APPROX_COUNT_DISTINCT(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 8 THEN user_id ELSE NULL END) AS users_7d,
    APPROX_COUNT_DISTINCT(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 8 THEN order_id ELSE NULL END) AS orders_7d,
    APPROX_COUNT_DISTINCT(CASE WHEN DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 8 THEN inventory_item_id ELSE NULL END) AS items_7d,

    -- 28D metrics (entire window)
    SUM(sale_price) AS sales_28d,
    APPROX_COUNT_DISTINCT(user_id) AS users_28d,
    APPROX_COUNT_DISTINCT(order_id) AS orders_28d,
    APPROX_COUNT_DISTINCT(inventory_item_id) AS items_28d,

  FROM {{ ref("stg_order_items") }}
  JOIN date_range
  ON DATE_DIFF(asof_date, DATE(created_at), DAY) BETWEEN 0 AND 29
  GROUP BY 1,2
)

SELECT
  prod.category,
  prod.name,
  prod.brand,
  prod.department,
  prod.sku,
  metric.*,
FROM product_metrics_1d metric
JOIN {{ ref("stg_products") }} prod
ON metric.product_id = prod.id
