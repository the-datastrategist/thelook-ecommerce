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
            traffic_source  as acquisition_traffic_source,
            created_at      as user_created_at
        from {{ ref("stg_users") }}
    ),

    user_sessions as (
        -- Get sessions that include a purchase event.
        -- Provides session timestamps and traffic source.
        -- Deduplicate to a single user/date
        select
            user_id,
            date(min(created_at)) as session_date,
            min(session_id)       as session_id,
            min(created_at)       as session_started_at,
            max(created_at)       as session_ended_at,
            max(browser)          as session_browser,
            max(traffic_source)   as session_traffic_source,
            sum(if(event_type = 'purchase', 1, 0)) as purchase_events
        from {{ ref("stg_events") }}
        group by 1
        having purchase_events > 0
    ),

    orders as (
        -- Get order information.
        -- Order and product statuses are the same.
        -- WARNING: order_items cannot be summed to a higher level.
        select
            o.user_id,
            order_id,
            us.* except (user_id),
            date(min(o.created_at) over (partition by o.user_id)) as first_order_date,
            date(o.created_at)  as order_date,
            o.status            as order_status,
            o.created_at        as order_created_at,
            o.shipped_at        as order_shipped_at,
            o.returned_at       as order_returned_at,
            num_of_item         as order_items,
            date_diff(shipped_at, created_at, hour) as hours_until_shipped,
            date_diff(returned_at, shipped_at, day) as days_until_returned,
        from {{ ref("stg_orders") }} o
        left join user_sessions us 
            on o.user_id = us.user_id 
            and date(o.created_at) = session_date
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
        from {{ ref("stg_order_items") }}  oi
        left join {{ ref("stg_products") }} p on oi.product_id = p.id
    )

select
    o.*,
    oi.* except (user_id, order_id),
    u.* except (user_id, user_created_at)
from orders o
join order_items oi 
  on o.user_id = oi.user_id
  and o.order_id = oi.order_id
join users u on o.user_id = u.user_id
