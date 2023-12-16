{{
    config(
        materialized="table",
        cluster_by=["session_date"],
    )
}}

{% set event_types = [
    "product",
    "department",
    "cart",
    "purchase",
    "cancel",
] %}

select
    session_id,
    visitor_id,
    user_id,
    min(event_date) as session_date,
    session_traffic_source,
    city,
    state,
    postal_code,
    max(sequence_number) as sequences,
    max(datetime_diff(created_at, session_started_at, second)) as session_time_sec,
    min(
        case
            when lower(event_type) = 'cart'
            then datetime_diff(created_at, session_started_at, second)
            else null
        end
    ) as time_to_cart_sec,
    min(
        case
            when lower(event_type) = 'purchase'
            then datetime_diff(created_at, session_started_at, second)
            else null
        end
    ) as time_to_purchase_sec,

    min(
        case when lower(event_type) = 'cart' then sequence_number else null end
    ) as sequences_to_cart,
    min(
        case when lower(event_type) = 'purchase' then sequence_number else null end
    ) as sequences_to_purchase,

    -- Event counts by type
    {% for event_type in event_types %}
        sum(
            if(lower(event_type) = '{{event_type}}', 1, 0)
        ) as events_with_{{ event_type }},
    {% endfor %}

from {{ ref("stg_event") }} e
group by 1, 2, 3, 5, 6, 7, 8
