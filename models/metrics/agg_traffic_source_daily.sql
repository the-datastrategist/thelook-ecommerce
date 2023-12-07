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

    event_source_daily as (
        -- Aggregate session metrics by traffic source
        select
            session_date,
            session_traffic_source,

            -- Aggregate metrics
            count(distinct session_id)  as sessions,
            count(distinct visitor_id)  as visitors,
            sum(sequences)              as total_sequences,
            sum(session_time_sec)       as total_session_time_sec,
            sum(time_to_cart_sec)       as total_time_to_cart_sec,
            sum(time_to_purchase_sec)   as total_time_to_purchase_sec,
            sum(sequences_to_cart)      as total_sequences_to_cart,
            sum(sequences_to_purchase)  as total_sequences_to_purchase,

            avg(sequences)              as avg_sequences,
            avg(session_time_sec)       as avg_session_time_sec,
            avg(time_to_cart_sec)       as avg_time_to_cart_sec,
            avg(time_to_purchase_sec)   as avg_time_to_purchase_sec,
            avg(sequences_to_cart)      as avg_sequences_to_cart,
            avg(sequences_to_purchase)  as avg_sequences_to_purchase,

            -- Sessions & visitors by event type
            {% for event_type in event_types %}
            count(distinct if(events_with_{{ event_type }} > 0, session_id, null)) as sessions_with_{{ event_type }},
            count(distinct if(events_with_{{ event_type }} > 0, visitor_id, null)) as visitors_with_{{ event_type }},
            {% endfor %}

        from {{ ref("fact_sessions") }}
        group by 1, 2
    ),

    order_source_daily as (
        select
            order_date,
            acquisition_traffic_source,

            -- Aggregate metrics
            count(distinct user_id) as users,
            count(distinct order_id) as orders,
            sum(revenue) as revenue,
            sum(cost) as cost,
            sum(profit) as profit,
            sum(discount) as discount,

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
        group by 1, 2
    ),

    source_date_range as (
        -- Get a unique list of dates and traffic_sources
        select distinct asof_date, traffic_source
        from (
            select distinct
                session_date as asof_date,
                session_traffic_source as traffic_source
            from event_source_daily
            
            union distinct

            select distinct
                order_date as asof_date,
                acquisition_traffic_source as traffic_source,
            from order_source_daily
        )
    )

select
    ed.session_date as asof_date,
    coalesce(acquisition_traffic_source, session_traffic_source) as traffic_source,
    od.* except (order_date, acquisition_traffic_source),
    ed.* except (session_date, session_traffic_source),
from source_date_range dr 
left join order_source_daily od
    on dr.asof_date = od.order_date
    and dr.traffic_source = od.acquisition_traffic_source
left join event_source_daily ed
    on dr.asof_date = ed.session_date
    and dr.traffic_source = ed.session_traffic_source
