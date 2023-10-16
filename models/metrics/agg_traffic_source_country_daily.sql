{{
    config(
        materialized="table",
    )
}}

{% set order_statuses = [
    "shipped",
    "complete",
    "processing",
    "cancelled",
    "returned",
] %}


select
    order_date,
    acquisition_traffic_source,
    session_traffic_source,
    country,

    count(distinct user_id) as users,
    count(distinct order_id) as orders,
    sum(revenue) as revenue,
    sum(cost) as cost,
    sum(profit) as profit,
    sum(discount) as discount,

    sum(order_items) as total_order_items,
    sum(days_since_first_order) as total_days_since_first_order,
    sum(weeks_since_first_order) as total_weeks_since_first_order,
    sum(months_since_first_order) as total_months_since_first_order,
    sum(hours_until_shipped) as total_hours_until_shipped,
    sum(days_until_returned) as total_days_until_returned,
    sum(products) as total_products,
    sum(categories) as total_categories,
    sum(brands) as total_brands,

    {% for order_status in order_statuses %}
        count(
            distinct if(lower(order_status) = '{{order_status}}', user_id, null)
        ) as users_{{ order_status }},
        count(
            distinct if(lower(order_status) = '{{order_status}}', order_id, null)
        ) as orders_{{ order_status }},
        sum(
            if(lower(order_status) = '{{order_status}}', revenue, 0)
        ) as revenue_{{ order_status }},
        sum(
            if(lower(order_status) = '{{order_status}}', cost, 0)
        ) as cost_{{ order_status }},
        sum(
            if(lower(order_status) = '{{order_status}}', profit, 0)
        ) as profit_{{ order_status }},
    {% endfor %}

from {{ ref("fact_orders") }}
group by 1, 2, 3, 4
