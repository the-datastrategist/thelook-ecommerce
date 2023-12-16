select
    id as event_id,
    sha1(coalesce(cast(user_id as string), session_id)) as visitor_id,
    date(created_at) as event_date,
    traffic_source as session_traffic_source,
    e.* except (id, traffic_source),
    min(created_at) over (partition by session_id) as session_started_at
from `bigquery-public-data.thelook_ecommerce.events` e
