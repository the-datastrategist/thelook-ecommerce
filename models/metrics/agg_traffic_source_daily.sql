{{
    config(
        materialized="table",
    )
}}

{% set event_types = ["product", "department", "cart", "purchase"] %}

{% set order_statuses = [
    "shipped",
    "complete",
    "processing",
    "cancelled",
    "returned",
] %}


with

    events_daily as (
        -- Aggregate session metrics by traffic source
        select
            date(created_at) as session_date,
            traffic_source as session_traffic_source,
            count(distinct session_id) as sessions,
            count(distinct coalesce(cast(user_id as string), ip_address)) as visitors,

            {% for event_type in event_types %}
                count(
                    distinct if(lower(event_type) = '{{event_type}}', session_id, null)
                ) as sessions_with_{{ event_type }},
                count(
                    distinct if(
                        lower(event_type) = '{{event_type}}',
                        coalesce(cast(user_id as string), ip_address),
                        null
                    )
                ) as visitors_with_{{ event_type }},
            {% endfor %}
        from {{ ref("stg_events") }}
        group by 1, 2
    ),

    orders_daily as (
        select
            order_date,
            acquisition_traffic_source,
            session_traffic_source,

            count(distinct user_id) as users,
            count(distinct order_id) as orders,
            sum(revenue) as revenue,
            sum(cost) as cost,
            sum(profit) as profit,
            sum(discount) as discount,

            {% for order_status in order_statuses %}
                count(
                    distinct if(lower(order_status) = '{{order_status}}', user_id, null)
                ) as users_with_{{ order_status }},
                count(
                    distinct if(
                        lower(order_status) = '{{order_status}}', order_id, null
                    )
                ) as orders_with_{{ order_status }},
            {% endfor %}

            sum(order_items) as total_order_items,
            sum(days_since_first_order) as total_days_since_first_order,
            sum(weeks_since_first_order) as total_weeks_since_first_order,
            sum(months_since_first_order) as total_months_since_first_order,
            sum(hours_until_shipped) as total_hours_until_shipped,
            sum(days_until_returned) as total_days_until_returned,
            sum(products) as total_products,
            sum(categories) as total_categories,
            sum(brands) as total_brands,
        from {{ ref("fact_orders") }}
        group by 1, 2, 3
    )

select
    session_date as asof_date,
    acquisition_traffic_source,
    ed.session_traffic_source,
    od.* except (order_date, acquisition_traffic_source, session_traffic_source),
    ed.* except (session_date, session_traffic_source),
from events_daily ed
left join
    orders_daily od
    on ed.session_date = od.order_date
    and ed.session_traffic_source = od.session_traffic_source
