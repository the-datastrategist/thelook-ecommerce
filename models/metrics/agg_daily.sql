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


{% set event_types = [
    "product",
    "department",
    "cart",
    "purchase",
] %}


{% set traffic_sources = [
    "email",
    "adwords",
    "youtube",
    "facebook",
    "organic",
] %}

with

    events_daily as (
        -- Aggregate session metrics by traffic source
        select
            date(created_at) as session_date,

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

            {% for traffic_source in traffic_sources %}
                count(
                    distinct if(
                        lower(traffic_source) = '{{traffic_source}}', session_id, null
                    )
                ) as sessions_from_{{ traffic_source }},
                count(
                    distinct if(
                        lower(traffic_source) = '{{traffic_source}}',
                        coalesce(cast(user_id as string), ip_address),
                        null
                    )
                ) as visitors_from_{{ traffic_source }},
            {% endfor %}

        from {{ ref("stg_events") }}
        group by 1
    ),

    orders_daily as (
        select
            order_date,

            count(distinct user_id) as users,
            count(distinct order_id) as orders,
            sum(
                date_diff(session_ended_at, session_started_at, second)
            ) as session_with_order_time_sec,
            avg(
                date_diff(session_ended_at, session_started_at, second)
            ) as avg_session_with_order_time_sec,
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

            -- First-time vs Repeat metrics
            count(distinct if(order_date = first_order_date, user_id, null))  as users_first_time,
            count(distinct if(order_date = first_order_date, order_id, null)) as orders_first_time,
            sum(if(order_date = first_order_date, revenue, 0))                as revenue_first_time,

            count(distinct if(order_date > first_order_date, user_id, null))  as users_repeat,
            count(distinct if(order_date > first_order_date, order_id, null)) as orders_repeat,
            sum(if(order_date > first_order_date, revenue, 0))                as revenue_repeat,

            -- Metrics by order status
            {% for order_status in order_statuses %}
                count(
                    distinct if(lower(order_status) = '{{order_status}}', user_id, null)
                ) as users_{{ order_status }},
                count(
                    distinct if(
                        lower(order_status) = '{{order_status}}', order_id, null
                    )
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
        group by 1
    )

select session_date as asof_date, od.* except (order_date), ed.* except (session_date),
from events_daily ed
left join orders_daily od on ed.session_date = od.order_date
