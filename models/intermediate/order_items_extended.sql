{{
  config(
    materialized = "table",
    cluster_by = ["user_id", "order_id"],
  )
}}

with

    users as (
        -- Get user information.
        select
            id as user_id,
            age,
            gender,
            city,
            country,
            latitude,
            longitude,
            traffic_source,
            created_at as user_created_at
        from {{ ref("stg_users") }}
    ),

    orders as (
        -- Get order information.
        -- Order and product statuses are the same.
        -- WARNING: order_items cannot be summed to a higher level.
        select
            user_id,
            order_id,
            u.user_created_at,
            date(min(o.created_at) over (partition by user_id)) as first_order_date,
            date(o.created_at) as order_date,
            o.status as order_status,
            o.shipped_at,
            o.returned_at,
            num_of_item as order_items,
            date_diff(shipped_at, created_at, hour) as hours_until_shipped,
            date_diff(returned_at, shipped_at, day) as days_until_returned,
        from {{ ref("stg_orders") }} o
        join users u using (user_id)
    ),

    order_items as (
        select
            user_id,
            order_id,
            product_id,
            sku,
            category,
            brand,
            sale_price,
            cost,
            retail_price,
            retail_price - sale_price as discount,
            sale_price - cost as profit
        from {{ ref("stg_order_items") }} oi
        left join {{ ref("stg_products") }} p on oi.product_id = p.id
    )

select o.*, oi.* except (user_id, order_id), u.* except (user_id, user_created_at)
from orders o
join order_items oi using (user_id, order_id)
join users u using (user_id)
