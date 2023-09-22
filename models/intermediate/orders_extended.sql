{{
  config(
    materialized = "table",
    cluster_by = ["user_id", "order_id"],
  )
}}

select
    user_id,
    order_id,
    order_date,
    user_created_at,
    first_order_date,
    date_diff(order_date, first_order_date, day) as days_since_first_order,
    date_diff(order_date, first_order_date, week) as weeks_since_first_order,
    date_diff(order_date, first_order_date, month) as months_since_first_order,

    -- User demographic info
    -- Use max() to ensure now duplicate user/order records
    max(traffic_source) as traffic_source,
    max(age) as age,
    max(gender) as gender,
    max(country) as country,

    -- Order attributes and measures
    -- Use max() to ensure now duplicate user/order records
    max(order_status) as order_status,
    max(shipped_at) as shipped_at,
    max(returned_at) as returned_at,
    max(order_items) as order_items,
    max(hours_until_shipped) as hours_until_shipped,
    max(days_until_returned) as days_until_returned,

    -- Product measures
    -- Aggregated to order level
    count(distinct product_id) as products,
    count(distinct category) as categories,
    count(distinct brand) as brands,
    sum(sale_price) as revenue,
    sum(cost) as cost,
    sum(profit) as profit,
    sum(discount) as discount

from {{ ref("order_items_extended") }}
group by 1, 2, 3, 4, 5
