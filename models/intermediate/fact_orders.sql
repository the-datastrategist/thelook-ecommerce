{{
  config(
    materialized = "table",
    cluster_by = ["user_id", "order_id"],
  )
}}

-- Aggregate from order_items to order
select
  user_id,
  order_id,
  order_date,
  session_date,
  session_id,
  session_started_at,
  session_ended_at,
  session_browser,
  session_traffic_source,
  acquisition_traffic_source,
  age,
  gender,
  city,
  country,
  latitude,
  longitude,
  purchase_events,
  first_order_date,
  order_status,
  order_created_at,
  order_shipped_at,
  order_returned_at,
  order_items,
  hours_until_shipped,
  days_until_returned,

  -- Get days, weeks, months from users' first order
  date_diff(order_date, first_order_date, day) as days_since_first_order,
  date_diff(order_date, first_order_date, week) as weeks_since_first_order,
  date_diff(order_date, first_order_date, month) as months_since_first_order,

  -- Product measures
  -- Aggregated to order level
  count(distinct product_id) as products,
  count(distinct category) as categories,
  count(distinct brand) as brands,
  sum(sale_price) as revenue,
  sum(cost) as cost,
  sum(profit) as profit,
  sum(discount) as discount

from {{ ref("fact_order_items") }}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
